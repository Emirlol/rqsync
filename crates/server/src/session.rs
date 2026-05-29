use std::{
	collections::HashSet,
	fmt::{
		Display,
		Formatter,
	},
	path::PathBuf,
	sync::Arc,
};

use anyhow::{
	Context,
	bail,
	ensure,
};
use bytes::Bytes;
use lib::{
	ArchivedClientMessage,
	ClientMessage,
	Compression,
	MAX_CONTROL_FRAME_SIZE,
	PacketHandler,
	ServerMessage,
};
use quinn::{
	RecvStream,
	SendStream,
};
use rkyv::rancor;
use tokio::{
	sync::Mutex,
	task::JoinSet,
};

mod manifest;
mod progress;
mod receive;
mod resume;

use progress::{
	ReceiveProgress,
	log_receiving_start,
};
pub use receive::ReceiveState;
use receive::SharedReceiveState;
use resume::METADATA_FILE as RESUME_METADATA_FILE;

pub struct ServerSession {
	conn: quinn::Connection,
	control: Option<(SendStream, RecvStream)>,
	root_dir: PathBuf,
	state: ServerState,
}

impl PacketHandler for ServerSession {}

impl ServerSession {
	pub fn new(conn: quinn::Connection, root_dir: PathBuf) -> Self {
		Self {
			conn,
			control: None,
			root_dir,
			state: ServerState::Handshaking,
		}
	}

	pub fn connection(&self) -> &quinn::Connection {
		&self.conn
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		self.accept_control_stream().await?;
		self.handshake().await?;
		self.register_transfers().await?;
		self.finish_and_reconcile().await?;
		let _ = self.conn.closed().await;

		Ok(())
	}

	async fn handshake(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ServerState::Handshaking));

		match self.read_control().await? {
			ClientMessage::Hello { version } => {
				if version != lib::PROTOCOL_VERSION {
					bail!("Unsupported protocol version {version}, expected {}", lib::PROTOCOL_VERSION);
				}
				self.state = ServerState::Idle;
				self.write_control(ServerMessage::HelloAck { version }).await?;
				Ok(())
			}
			message => bail!("Unexpected message in handshake: {message:?}"),
		}
	}

	async fn register_transfers(&mut self) -> anyhow::Result<()> {
		ensure!(matches!(self.state, ServerState::Idle));

		let frame = self.read_control_frame().await?;
		let message = Self::access_message(&frame)?;

		match message {
			ArchivedClientMessage::RegisterTransfers { files, chunk_size, compression } => {
				let chunk_size = chunk_size.to_native();
				let (mut transfers, rejected) = manifest::create_incoming_transfers(files.iter(), chunk_size, compression.into(), self.root_dir.clone());
				let resume_path = self.resume_metadata_path();
				let received_chunks = resume::apply_metadata(&resume_path, &mut transfers, chunk_size).await?;

				let message = if transfers.is_empty() {
					self.state = ServerState::Idle;
					ServerMessage::AllTransfersRejected { rejected }
				} else {
					let accepted = transfers.keys().copied().collect::<HashSet<_>>();
					let receive_progress = ReceiveProgress::shared(&transfers);
					log_receiving_start(transfers.len(), Compression::from(compression), &receive_progress);
					let receive_state = Arc::new(Mutex::new(ReceiveState::new(transfers, resume_path)));
					let mut streams = JoinSet::new();
					for _ in 0..lib::DATA_STREAM_COUNT {
						let conn = self.conn.clone();
						let receive_state = receive_state.clone();
						let receive_progress = receive_progress.clone();

						streams.spawn(async move {
							let recv = conn.accept_uni().await.context("Failed to accept uni stream")?;
							receive::receive_data_chunks(recv, receive_state, receive_progress).await
						});
					}

					self.state = ServerState::ReceivingFiles { streams, receive_state };
					ServerMessage::TransfersRegistered { accepted, rejected, received_chunks }
				};

				self.write_control(message).await?;

				Ok(())
			}
			message => bail!("Unexpected message in register transfers: {message:?}"),
		}
	}

	async fn finish_and_reconcile(&mut self) -> anyhow::Result<()> {
		let receive_state = {
			let ServerState::ReceivingFiles { ref mut streams, ref receive_state } = self.state else {
				bail!("Unexpected state in finish and reconcile: {}", self.state);
			};

			while let Some(result) = streams.join_next().await {
				if let Err(err) = result? {
					receive::persist_resume_metadata_on_disconnect(receive_state).await?;
					return Err(err);
				}
			}

			receive_state.clone()
		};

		match self.read_control().await {
			Ok(ClientMessage::TransferFinished) => {}
			Ok(message) => {
				receive::persist_resume_metadata_on_disconnect(&receive_state).await?;
				bail!("Unexpected message in finish and reconcile: {message:?}");
			}
			Err(err) => {
				receive::persist_resume_metadata_on_disconnect(&receive_state).await?;
				return Err(err);
			}
		}

		self.state = ServerState::Idle;
		let message = ServerMessage::TransferComplete;
		self.write_control(message).await?;
		self.finish_control().await?;
		resume::remove_metadata(&self.resume_metadata_path()).await?;

		Ok(())
	}

	async fn read_control_frame(&mut self) -> anyhow::Result<Bytes> {
		let Some((_, ref mut recv)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let frame = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		Ok(frame)
	}

	async fn read_control(&mut self) -> anyhow::Result<ClientMessage> {
		let Some((_, ref mut recv)) = self.control else { bail!("Control stream not opened yet") };
		let frame = Self::read_frame(recv, MAX_CONTROL_FRAME_SIZE).await?;
		rkyv::from_bytes::<ClientMessage, rancor::Error>(&frame).context("Failed to deserialize control message")
	}

	async fn write_control(&mut self, message: ServerMessage) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		let message = rkyv::to_bytes::<rancor::Error>(&message).context("Failed to serialize control message")?;
		Self::write_frame(send, &message).await?;
		Ok(())
	}

	async fn finish_control(&mut self) -> anyhow::Result<()> {
		let Some((ref mut send, _)) = self.control else {
			return Err(anyhow::anyhow!("No control stream available"));
		};
		send.finish().context("Failed to finish control stream")?;
		Ok(())
	}

	async fn accept_control_stream(&mut self) -> anyhow::Result<()> {
		let (send, recv) = self.conn.accept_bi().await.context("Failed to accept bidirectional stream")?;
		self.control = Some((send, recv));
		Ok(())
	}

	fn resume_metadata_path(&self) -> PathBuf {
		self.root_dir.join(RESUME_METADATA_FILE)
	}

	#[inline]
	fn access_message(bytes: &[u8]) -> Result<&ArchivedClientMessage, rancor::Error> {
		rkyv::access(bytes)
	}
}

pub enum ServerState {
	Handshaking,
	Idle,
	RegisteringTransfers,
	ReceivingFiles {
		streams: JoinSet<anyhow::Result<()>>,
		receive_state: SharedReceiveState,
	},
	ReportingMissingChunks,
}

impl Display for ServerState {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			ServerState::Handshaking => write!(f, "Handshaking"),
			ServerState::Idle => write!(f, "Idle"),
			ServerState::RegisteringTransfers => write!(f, "RegisteringTransfers"),
			ServerState::ReceivingFiles { .. } => write!(f, "ReceivingFiles"),
			ServerState::ReportingMissingChunks => write!(f, "ReportingMissingChunks"),
		}
	}
}

#[cfg(test)]
mod tests;

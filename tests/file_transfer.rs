use std::{
	fs,
	io::Read,
	net::{
		IpAddr,
		Ipv6Addr,
		SocketAddr,
		UdpSocket,
	},
	path::PathBuf,
	sync::Arc,
	time::{
		Duration,
		SystemTime,
		UNIX_EPOCH,
	},
};

use client::ClientConfig;
use lib::{
	ClientMessage,
	Compression,
	DuplicateStrategy,
	FileManifestEntry,
	MAX_CONTROL_FRAME_SIZE,
	PacketHandler,
	ServerMessage,
};
use quinn::{
	crypto::rustls::QuicClientConfig,
	rustls,
};
use rkyv::rancor;
use server::ServerConfig;

fn test_dir(name: &str) -> PathBuf {
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
	let path = std::env::temp_dir().join(format!("speedtest-e2e-{name}-{}-{nanos}", std::process::id()));
	fs::create_dir_all(&path).unwrap();
	path
}

fn unused_local_addr() -> SocketAddr {
	let socket = UdpSocket::bind(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0)).unwrap();
	socket.local_addr().unwrap()
}

struct TestPacketHandler;

impl PacketHandler for TestPacketHandler {}

fn client_network_config() -> anyhow::Result<quinn::ClientConfig> {
	let crypto = rustls::ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(lib::cert::SkipServerVerification::new())
		.with_no_client_auth();

	let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
	config.transport_config(Arc::new(lib::transport_config()));
	Ok(config)
}

async fn send_first_chunk_then_disconnect(address: SocketAddr, source_file: PathBuf) -> anyhow::Result<()> {
	let bind_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
	let mut endpoint = quinn::Endpoint::client(bind_addr)?;
	endpoint.set_default_client_config(client_network_config()?);
	let conn = endpoint.connect(address, "localhost")?.await?;
	let (mut control_send, mut control_recv) = conn.open_bi().await?;

	let hello = rkyv::to_bytes::<rancor::Error>(&ClientMessage::Hello { version: lib::PROTOCOL_VERSION })?;
	TestPacketHandler::write_frame(&mut control_send, &hello).await?;
	let hello_ack = TestPacketHandler::read_frame(&mut control_recv, MAX_CONTROL_FRAME_SIZE).await?;
	assert!(matches!(rkyv::from_bytes::<ServerMessage, rancor::Error>(&hello_ack)?, ServerMessage::HelloAck { .. }));

	let file_size = fs::metadata(&source_file)?.len();
	let register = ClientMessage::RegisterTransfers {
		files: vec![FileManifestEntry {
			id: 0,
			uncompressed_size: file_size,
			rel_path: vec!["data.bin".to_owned()],
		}],
		chunk_size: client::session::CHUNK_SIZE,
		compression: Compression::None,
	};
	let register = rkyv::to_bytes::<rancor::Error>(&register)?;
	TestPacketHandler::write_frame(&mut control_send, &register).await?;
	let registered = TestPacketHandler::read_frame(&mut control_recv, MAX_CONTROL_FRAME_SIZE).await?;
	assert!(matches!(rkyv::from_bytes::<ServerMessage, rancor::Error>(&registered)?, ServerMessage::TransfersRegistered { .. }));

	let mut source = fs::File::open(&source_file)?;
	let mut bytes = vec![0; client::session::CHUNK_SIZE as usize];
	source.read_exact(&mut bytes)?;
	let chunk = ClientMessage::Chunk {
		file_id: 0,
		offset: 0,
		bytes: bytes.into(),
	};
	let chunk = rkyv::to_bytes::<rancor::Error>(&chunk)?;
	let mut data = conn.open_uni().await?;
	TestPacketHandler::write_frame(&mut data, &chunk).await?;
	data.finish()?;

	tokio::time::sleep(Duration::from_millis(100)).await;
	conn.close(0u32.into(), b"interrupted");
	endpoint.wait_idle().await;
	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_sends_files_to_server() {
	let root = test_dir("client-sends-files");
	let source_root = root.join("source");
	let receive_root = root.join("received");
	let selected_dir = source_root.join("payload");
	fs::create_dir_all(selected_dir.join("nested")).unwrap();
	fs::create_dir_all(&receive_root).unwrap();

	let top_level_file = source_root.join("top.txt");
	let nested_file = selected_dir.join("nested").join("bottom.bin");
	fs::write(&top_level_file, b"top level contents").unwrap();
	fs::write(&nested_file, b"nested contents").unwrap();

	let address = unused_local_addr();
	let server = tokio::spawn(server::run_server(ServerConfig {
		address,
		root_dir: receive_root.clone(),
	}));

	tokio::time::sleep(std::time::Duration::from_millis(100)).await;

	let client = client::run_client(ClientConfig {
		server_addr: address,
		files: vec![top_level_file, selected_dir],
		duplicate_strategy: DuplicateStrategy::Reject,
		compression: Compression::LZ4,
	});

	tokio::time::timeout(std::time::Duration::from_secs(10), client).await.expect("client timed out").unwrap();

	server.abort();
	let _ = server.await;

	assert_eq!(fs::read(receive_root.join("top.txt")).unwrap(), b"top level contents");
	assert_eq!(fs::read(receive_root.join("payload").join("nested").join("bottom.bin")).unwrap(), b"nested contents");

	fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_transfer_resumes_with_different_compression() {
	let root = test_dir("resume-transfer");
	let source_root = root.join("source");
	let receive_root = root.join("received");
	fs::create_dir_all(&source_root).unwrap();
	fs::create_dir_all(&receive_root).unwrap();

	let source_file = source_root.join("data.bin");
	let mut payload = vec![0u8; client::session::CHUNK_SIZE as usize + 17];
	for (index, byte) in payload.iter_mut().enumerate() {
		*byte = (index % 251) as u8;
	}
	fs::write(&source_file, &payload).unwrap();

	let address = unused_local_addr();
	let server = tokio::spawn(server::run_server(ServerConfig {
		address,
		root_dir: receive_root.clone(),
	}));

	tokio::time::sleep(Duration::from_millis(100)).await;

	send_first_chunk_then_disconnect(address, source_file.clone()).await.unwrap();
	let resume_file = receive_root.join(".speedtest-resume.rkyv");
	for _ in 0..20 {
		if resume_file.exists() {
			break;
		}
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(resume_file.exists());

	let client = client::run_client(ClientConfig {
		server_addr: address,
		files: vec![source_file],
		duplicate_strategy: DuplicateStrategy::Reject,
		compression: Compression::LZ4,
	});

	tokio::time::timeout(Duration::from_secs(10), client).await.expect("client timed out").unwrap();

	server.abort();
	let _ = server.await;

	assert_eq!(fs::read(receive_root.join("data.bin")).unwrap(), payload);
	assert!(!resume_file.exists());

	fs::remove_dir_all(root).unwrap();
}

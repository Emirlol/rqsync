pub mod session;

use std::{
	net::SocketAddr,
	path::PathBuf,
	sync::Arc,
};

use bit_set::BitSet;
use quinn::{
	Endpoint,
	rustls::pki_types::{
		CertificateDer,
		PrivatePkcs8KeyDer,
	},
};
use tracing::{
	error,
	info,
};

use crate::session::ServerSession;

pub struct ServerConfig {
	pub address: SocketAddr,
	pub root_dir: PathBuf,
}

pub async fn run_server(config: ServerConfig) -> anyhow::Result<()> {
	let ServerConfig { address, root_dir } = config;
	let endpoint = Endpoint::server(server_network_config()?, address)?;
	info!("Listening on {}...", endpoint.local_addr()?);

	while let Some(incoming) = endpoint.accept().await {
		let root_dir_clone = root_dir.clone();
		tokio::spawn(async move {
			match incoming.await {
				Ok(conn) => {
					let mut session = ServerSession::new(conn, root_dir_clone);
					match session.run().await {
						Ok(()) => info!("Connection with {} closed", session.connection().remote_address()),
						Err(e) => error!("Connection error with {}: {e}", session.connection().remote_address()),
					}
				}
				Err(e) => error!("Accept error: {e}"),
			}
		});
	}

	Ok(())
}

fn server_network_config() -> anyhow::Result<quinn::ServerConfig> {
	let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
	let cert_der = CertificateDer::from(cert.cert);
	let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

	let mut config = quinn::ServerConfig::with_single_cert(vec![cert_der], key_der.into())?;
	*Arc::get_mut(&mut config.transport).expect("transport config is not shared yet") = lib::transport_config();
	Ok(config)
}

struct IncomingTransfer {
	path: PathBuf,
	rel_path: Vec<String>,
	file_size: u64,
	chunk_size: u64,
	compression: lib::Compression,
	received_bytes: u64,
	completed: bool,
	// Lazily populated on first chunk
	#[cfg(test)]
	file: Option<tokio::fs::File>,
	received: BitSet<u64>,
}

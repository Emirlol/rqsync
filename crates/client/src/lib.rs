pub mod session;

use std::{
	net::{
		IpAddr,
		Ipv6Addr,
		SocketAddr,
	},
	path::PathBuf,
	sync::Arc,
	time::Duration,
};

use anyhow::Context;
use lib::{
	Compression,
	DuplicateStrategy,
};
use quinn::{
	crypto::rustls::QuicClientConfig,
	rustls,
	Endpoint,
};

use crate::session::ClientSession;

pub struct ClientConfig {
	pub server_addr: SocketAddr,
	pub files: Vec<PathBuf>,
	pub duplicate_strategy: DuplicateStrategy,
	pub compression: Compression,
}

pub async fn run_client(config: ClientConfig) -> anyhow::Result<()> {
	let ClientConfig {
		server_addr,
		files,
		duplicate_strategy,
		compression,
	} = config;
	let bind_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
	let mut endpoint = Endpoint::client(bind_addr)?;
	endpoint.set_default_client_config(client_network_config()?);

	let conn = tokio::time::timeout(Duration::from_secs(5), endpoint.connect(server_addr, "localhost")?)
		.await?
		.context("Connection timed out")?;

	let mut session = ClientSession::new(conn, files, duplicate_strategy, compression)?;
	session.run().await?;

	Ok(())
}

fn client_network_config() -> anyhow::Result<quinn::ClientConfig> {
	let crypto = rustls::ClientConfig::builder()
		.dangerous()
		.with_custom_certificate_verifier(lib::cert::SkipServerVerification::new())
		.with_no_client_auth();

	let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
	config.transport_config(Arc::new(lib::transport_config()));
	Ok(config)
}

pub(crate) struct ChunkJob {
	id: u32,
	source_path: PathBuf,
	offset: u64,
	uncompressed_len: u64,
}

#[derive(Clone)]
pub(crate) struct TransferCandidate {
	source_path: PathBuf,
	rel_path: Vec<String>,
	uncompressed_size: u64,
}

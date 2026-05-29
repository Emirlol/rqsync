use std::{
	fs,
	net::{
		Ipv6Addr,
		SocketAddr,
		UdpSocket,
	},
	path::PathBuf,
	time::{
		SystemTime,
		UNIX_EPOCH,
	},
};

use client::ClientConfig;
use lib::{
	Compression,
	DuplicateStrategy,
};
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

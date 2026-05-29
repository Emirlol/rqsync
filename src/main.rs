pub mod cli;

use anyhow::Result;
use clap::Parser;
use client::ClientConfig;
use server::ServerConfig;

use crate::cli::{
	Cli,
	Commands,
};

/// Planned flow:
/// Sender:
/// - Try to connect
/// - Select files to send via popup
/// - Collect all file metadata to be sent and assign ids
/// - Register all files with the server first
/// - Start iterating on files and splitting them into chunks
/// Receiver:
/// - Wait for connections
/// - Get a list of files to be received
/// - Check the transfer metadata file to see if the same exact transfer was already completed
/// - Compare files on local with the files to be received, let the server know which files are missing
/// - Start receiving files and writing them to disc as soon as they are recevied & decompressed
/// - Maintain a metadata file of the last transfers in case the transfer is halted, that will allow for continuing or deleting the leftovers from an unsuccessful transfer
/// - Continue building the metadata file as more files are received, and also continue receiving files in general lol

fn main() -> Result<()> {
	tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

	let cli = Cli::parse();
	let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
	rt.block_on(async {
		let signal = tokio::signal::ctrl_c();
		let app = async {
			match cli.command {
				Commands::Rx { address, root_dir } => {
					let config = ServerConfig { address, root_dir };
					server::run_server(config).await
				}
				Commands::Tx {
					server_addr,
					files,
					duplicate_strategy,
					compression,
				} => {
					let config = ClientConfig {
						server_addr,
						files,
						duplicate_strategy,
						compression,
					};
					client::run_client(config).await
				}
			}
		};
		tokio::select! {
			result = app => result,
			_ = signal => Ok(())
		}
	})?;

	Ok(())
}

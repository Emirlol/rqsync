use std::{
	net::SocketAddr,
	path::PathBuf,
};

use clap::{
	Parser,
	Subcommand,
};
use lib::{
	Compression,
	DuplicateStrategy,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
	#[command(subcommand)]
	pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
	/// Runs in receiver mode
	Rx {
		/// The address to listen on.
		address: SocketAddr,
		/// Root file directory to put received files in.
		#[arg(short, long, value_name = "DIR", default_value = ".")]
		root_dir: PathBuf,
	},
	/// Runs in sender mode
	Tx {
		/// The address of the remote server to connect to.
		server_addr: SocketAddr,
		/// The files to send
		files: Vec<PathBuf>,
		/// How duplicate files should be handled
		#[arg(short, long, value_enum, default_value_t = DuplicateStrategy::Reject)]
		duplicate_strategy: DuplicateStrategy,
		/// Compress chunks before sending them
		#[arg(short, long, value_enum, default_value_t = Compression::LZ4)]
		compression: Compression,
	},
}

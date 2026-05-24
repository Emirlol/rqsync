use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

const BUFFER_SIZE: usize = 128 * 1024; // 128KB buffer

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage:");
        println!("  Receiver: speedtest rx <addr> (e.g., :9999)");
        println!("  Sender:   speedtest tx <addr> <duration_secs> (e.g., localhost:9999 10)");
        return Ok(());
    }

    let mode = &args[1];
    let addr = &args[2];

    match mode.as_str() {
        "rx" => {
            run_receiver(addr)
        }
        "tx" => {
            let duration = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
            run_sender(addr, duration)
        }
        _ => {
            println!("Invalid mode. Use 'rx' or 'tx'.");
            Ok(())
        }
    }
}

fn run_receiver(addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    println!("Receiver listening on {}...", addr);

    for stream in listener.incoming() {
        let mut stream = stream?;
        println!("Client connected. Measuring...");

        let mut buffer = [0u8; BUFFER_SIZE];
        let mut total_bytes = 0u64;
        let start = Instant::now();

        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break, // Connection closed
                Ok(n) => total_bytes += n as u64,
                Err(e) => {
                    eprintln!("Read error: {}", e);
                    break;
                }
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        let mbytes = total_bytes as f64 / (1024.0 * 1024.0);
        println!(
            "Received: {:.2} MB | Average Speed: {:.2} MB/s",
            mbytes,
            mbytes / elapsed
        );
    }
    Ok(())
}

fn run_sender(addr: &str, duration_secs: u64) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr)?;
    let buffer = [0u8; BUFFER_SIZE];
    let start = Instant::now();
    let duration = Duration::from_secs(duration_secs);

    println!("Transmitting to {} for {}s...", addr, duration_secs);

    while start.elapsed() < duration {
        // We ignore the result to keep the loop as tight as possible, 
        // though in a real app you'd handle errors.
        if let Err(_) = stream.write_all(&buffer) {
            break;
        }
    }
    
    println!("Done.");
    Ok(())
}
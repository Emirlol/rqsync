use std::env;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use quinn::rustls::{self, DigitallySignedStruct, SignatureScheme};
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt};
use tokio::task::JoinSet;

const BUFFER_SIZE: usize = 1024 * 1024;
const FLOW_WINDOW: u64 = 512 * 1024 * 1024;
const DEFAULT_STREAMS: usize = 8;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage:");
        println!("  Receiver: speedtest rx <addr> (e.g., :9999)");
        println!("  Sender:   speedtest tx <addr> <duration_secs> [streams]");
        return Ok(());
    }

    match args[1].as_str() {
        "rx" => run_receiver(&args[2]).await,
        "tx" => {
            let duration = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
            let streams = args
                .get(4)
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_STREAMS);
            run_sender(&args[2], duration, streams).await
        }
        _ => {
            println!("Invalid mode. Use 'rx' or 'tx'.");
            Ok(())
        }
    }
}

async fn run_receiver(addr: &str) -> Result<()> {
    let endpoint = Endpoint::server(server_config()?, normalize_bind_addr(addr)?)?;
    println!("QUIC receiver listening on {}...", endpoint.local_addr()?);

    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_connection(conn).await {
                        eprintln!("Connection error: {e}");
                    }
                }
                Err(e) => eprintln!("Accept error: {e}"),
            }
        });
    }

    Ok(())
}

async fn handle_connection(conn: quinn::Connection) -> Result<()> {
    println!("Client connected from {}. Measuring...", conn.remote_address());

    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut streams = JoinSet::new();

    while let Ok(mut recv) = conn.accept_uni().await {
        let mut stream_kind = [0u8; 1];
        if recv.read_exact(&mut stream_kind).await.is_err() || stream_kind[0] == 0 {
            break;
        }

        let total = Arc::clone(&total);
        streams.spawn(async move { drain_stream(recv, total).await });
    }

    while let Some(res) = streams.join_next().await {
        let _ = res?;
    }

    let bytes = total.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64().max(f64::EPSILON);
    print_rate("Received", bytes, elapsed);
    Ok(())
}

async fn drain_stream(mut recv: quinn::RecvStream, total: Arc<AtomicU64>) -> Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut pending = 0u64;

    while let Ok(Some(n)) = recv.read(&mut buffer).await {
        pending += n as u64;
        if pending >= 16 * 1024 * 1024 {
            total.fetch_add(pending, Ordering::Relaxed);
            pending = 0;
        }
    }

    if pending != 0 {
        total.fetch_add(pending, Ordering::Relaxed);
    }
    Ok(())
}

async fn run_sender(addr: &str, duration_secs: u64, streams: usize) -> Result<()> {
    let streams = streams.max(1);
    let remote = resolve_addr(addr)?;
    let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_config()?);

    println!(
        "Transmitting to {} for {}s over {} QUIC streams...",
        remote, duration_secs, streams
    );

    let conn = tokio::time::timeout(Duration::from_secs(5), endpoint.connect(remote, "localhost")?)
        .await
        .map_err(|_| format!("timed out connecting to {remote}"))??;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
    let started = Instant::now();
    let total = Arc::new(AtomicU64::new(0));
    let payload = Arc::new(vec![0u8; BUFFER_SIZE]);
    let mut tasks = JoinSet::new();

    for _ in 0..streams {
        let conn = conn.clone();
        let total = Arc::clone(&total);
        let payload = Arc::clone(&payload);
        tasks.spawn(async move { pump_stream(conn, deadline, payload, total).await });
    }

    while let Some(res) = tasks.join_next().await {
        res??;
    }

    match tokio::time::timeout(Duration::from_secs(1), conn.open_uni()).await {
        Ok(Ok(mut control)) => {
            tokio::time::timeout(Duration::from_secs(1), control.write_all(&[0])).await??;
            control.finish()?;
        }
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let bytes = total.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64().max(f64::EPSILON);
    print_rate("Sent", bytes, elapsed);
    Ok(())
}

async fn pump_stream(
    conn: quinn::Connection,
    deadline: tokio::time::Instant,
    payload: Arc<Vec<u8>>,
    total: Arc<AtomicU64>,
) -> Result<()> {
    let mut send = match tokio::time::timeout_at(deadline, conn.open_uni()).await {
        Ok(Ok(send)) => send,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Ok(()),
    };
    match tokio::time::timeout_at(deadline, send.write_all(&[1])).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Ok(()),
    }
    let mut pending = 0u64;

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, send.write_all(&payload)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => break,
        }
        pending += payload.len() as u64;
        if pending >= 16 * 1024 * 1024 {
            total.fetch_add(pending, Ordering::Relaxed);
            pending = 0;
        }
    }

    if pending != 0 {
        total.fetch_add(pending, Ordering::Relaxed);
    }
    send.finish()?;
    let _ = tokio::time::timeout(Duration::from_secs(5), send.stopped()).await;
    Ok(())
}

fn server_config() -> Result<ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut config = ServerConfig::with_single_cert(vec![cert_der], key_der.into())?;
    *Arc::get_mut(&mut config.transport).expect("transport config is not shared yet") =
        transport_config();
    Ok(config)
}

fn client_config() -> Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    config.transport_config(Arc::new(transport_config()));
    Ok(config)
}

fn transport_config() -> TransportConfig {
    let mut transport = TransportConfig::default();
    transport
        .max_concurrent_uni_streams(VarInt::from_u32(1024))
        .max_concurrent_bidi_streams(VarInt::from_u32(0))
        .stream_receive_window(VarInt::from_u32(FLOW_WINDOW as u32))
        .receive_window(VarInt::from_u32(FLOW_WINDOW as u32))
        .send_window(FLOW_WINDOW)
        .send_fairness(false)
        .keep_alive_interval(Some(Duration::from_secs(1)));
    transport
}

fn resolve_addr(addr: &str) -> Result<SocketAddr> {
    addr.to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("could not resolve address: {addr}").into())
}

fn normalize_bind_addr(addr: &str) -> Result<SocketAddr> {
    if let Some(port) = addr.strip_prefix(':') {
        format!("[::]:{port}").parse().map_err(Into::into)
    } else {
        addr.parse().map_err(Into::into)
    }
}

fn print_rate(label: &str, bytes: u64, elapsed: f64) {
    let mib = bytes as f64 / (1024.0 * 1024.0);
    let gibit = bytes as f64 * 8.0 / (1024.0 * 1024.0 * 1024.0);
    println!(
        "{}: {:.2} MiB | Average Speed: {:.2} MiB/s ({:.2} Gibit/s)",
        label,
        mib,
        mib / elapsed,
        gibit / elapsed
    );
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

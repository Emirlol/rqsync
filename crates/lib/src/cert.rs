use std::sync::Arc;

use quinn::{
	rustls,
	rustls::{
		DigitallySignedStruct,
		SignatureScheme,
		client::danger::{
			HandshakeSignatureValid,
			ServerCertVerified,
			ServerCertVerifier,
		},
		crypto::CryptoProvider,
		pki_types::{
			CertificateDer,
			ServerName,
			UnixTime,
		},
	},
};

#[derive(Debug)]
pub struct SkipServerVerification(Arc<CryptoProvider>);

impl SkipServerVerification {
	pub fn new() -> Arc<Self> {
		Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
	}
}

impl ServerCertVerifier for SkipServerVerification {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, rustls::Error> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
	}

	fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.0.signature_verification_algorithms.supported_schemes()
	}
}

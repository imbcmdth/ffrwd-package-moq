//! The wasm32-wasip2 connect path: quinn over quinn-wasi, raw QUIC.
//!
//! MoQ needs no WebTransport layer when the relay speaks it over raw
//! QUIC - the moq ALPNs negotiate the protocol directly, the way
//! moq-native dials a `moqt://` URL. The quinn connection is wrapped in
//! [`web_transport_quinn::Session::raw`], which satisfies the transport
//! trait `moq_net` builds on without an h3 CONNECT exchange.
//!
//! wasi has no OS trust store. A relay whose certificate is handed over
//! explicitly is trusted for exactly that certificate; without one, the
//! webpki roots baked into the module stand in for the missing store,
//! which is what a public relay's certificate chains to.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;

/// How many concurrent streams either direction of a session may have
/// open. One MoQ group is one stream, so this is how much of a backlog
/// a relay can hand over at once; moq-native's own default, for the
/// same reason.
const MAX_STREAMS: u32 = 1024;

/// How often a quiet session proves it is alive, and how long a session
/// goes without hearing from the relay before it counts as gone; see
/// [`connect`].
const KEEP_ALIVE: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where and whom to dial: the relay's address, its TLS name, and what
/// to trust for it.
pub struct Relay {
	/// The relay's UDP address.
	pub addr: SocketAddr,
	/// The TLS server name (SNI and certificate verification).
	pub server_name: String,
	/// The relay's own certificate, DER - a private relay's trust
	/// root. `None` trusts the webpki roots instead.
	pub cert_der: Option<Vec<u8>>,
}

/// A connected MoQ session and what keeps it alive.
pub struct Connected {
	/// The QUIC endpoint; hold it for the session's lifetime and call
	/// [`quinn::Endpoint::wait_idle`] after dropping the session for a
	/// clean close.
	pub endpoint: quinn::Endpoint,
	/// The MoQ session handle.
	pub session: moq_net::Session,
	/// The session's protocol work; spawn or poll it.
	pub driver: moq_net::Driver,
}

/// Dials `relay` and performs the MoQ handshake for `client`.
///
/// Must run inside a tokio current-thread runtime (quinn-wasi's
/// requirement). The negotiated version is whatever the ALPN settles on,
/// newest moq-lite first.
pub async fn connect(
	relay: &Relay,
	client: moq_net::Client,
) -> Result<Connected, Box<dyn std::error::Error>> {
	let mut roots = RootCertStore::empty();
	match &relay.cert_der {
		Some(der) => {
			roots.add(CertificateDer::from(der.clone()))?;
		}
		None => {
			roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
		}
	}

	let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
		rustls::crypto::ring::default_provider(),
	))
	.with_protocol_versions(&[&rustls::version::TLS13])?
	.with_root_certificates(roots)
	.with_no_client_auth();
	tls.alpn_protocols = moq_net::Versions::default()
		.alpns()
		.iter()
		.map(|alpn| alpn.as_bytes().to_vec())
		.collect();

	let mut config = quinn::ClientConfig::new(Arc::new(
		quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
	));
	// No MTU probing: wasi reports no maximum, oversized sends just fail,
	// and the QUIC floor of 1200 always fits.
	let mut transport = quinn::TransportConfig::default();
	transport.mtu_discovery_config(None);
	// How many streams the relay may have open toward this session at
	// once. MoQ opens ONE PER GROUP, and a subscriber asking for a
	// backlog is served hundreds of them at once: quinn's default of 100
	// makes the relay queue the rest in `open_uni`, where a group can
	// wait tens of seconds for credit that the wire had to spare. That
	// is not reordering a reader can hold for - it is the relay handing
	// over a group long after everything around it - so the window is
	// raised to what moq-native uses for the same reason.
	transport.max_concurrent_uni_streams(MAX_STREAMS.into());
	transport.max_concurrent_bidi_streams(MAX_STREAMS.into());
	// A relay that goes away without closing the connection - a process
	// gone, a network gone - is found out only when nothing has come back
	// for the idle timeout, and quinn's own is 30s of a live stream lost
	// before anything tries again. A keep-alive holds a quiet session open
	// meanwhile, so a broadcast with nothing to send is not taken for dead.
	transport.keep_alive_interval(Some(KEEP_ALIVE));
	transport.max_idle_timeout(Some(
		IDLE_TIMEOUT
			.try_into()
			.expect("the idle timeout fits QUIC's"),
	));
	config.transport_config(Arc::new(transport));

	let endpoint = quinn_wasi::endpoint(
		"0.0.0.0:0".parse().expect("literal address"),
		quinn::EndpointConfig::default(),
		None,
	)?;

	let connection = endpoint
		.connect_with(config, relay.addr, &relay.server_name)?
		.await?;

	let session = web_transport_quinn::Session::raw(connection);
	let (session, driver) = client.connect(session).await?;

	Ok(Connected {
		endpoint,
		session,
		driver,
	})
}

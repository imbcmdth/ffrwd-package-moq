//! quinn's pluggable I/O floor on wasm32-wasip2.
//!
//! quinn abstracts its environment behind two traits: [`quinn::Runtime`]
//! (task spawning and timers) and [`quinn::AsyncUdpSocket`] (datagram I/O).
//! This crate implements both over the standard wasi interfaces, so a
//! wasip2 component can run a full QUIC endpoint on any host that grants
//! it network access — wasmtime with `-S inherit-network`, or the ffrwd
//! sidecar under its `-net` switch.
//!
//! - [`WasiRuntime`] spawns onto the ambient tokio current-thread runtime
//!   and times with `tokio::time`, whose clock is the wasi monotonic clock.
//! - [`WasiUdpSocket`] wraps a `wasi:sockets/udp` socket in its
//!   unconnected mode (`stream(none)`), so one socket serves many peers
//!   and every outgoing datagram names its destination — what QUIC needs.
//! - A process-wide [`reactor`](crate::reactor) bridges wasi pollables to
//!   tokio wakers: readiness has no callback on wasi, only pollables, and
//!   tokio's parker knows nothing of them. The reactor is a task that
//!   sweeps armed pollables with the non-blocking `ready()` and sleeps
//!   1ms between sweeps, so tokio's own timers stay exact while socket
//!   readiness is observed within a millisecond. A deeper integration
//!   (parking the runtime inside `wasi:io/poll.poll` itself) would remove
//!   that millisecond; it needs a park hook tokio does not expose.
//!
//! Backpressure follows the wasi contract: `check-send` names how many
//! datagrams may be sent right now, `try_send` refuses with `WouldBlock`
//! when the budget is zero, and quinn then waits on the send pollable via
//! its `UdpPoller` before retrying.
//!
//! ```ignore
//! let rt = tokio::runtime::Builder::new_current_thread()
//!     .enable_time()
//!     .build()?;
//! rt.block_on(async {
//!     let endpoint = quinn_wasi::endpoint(
//!         "127.0.0.1:0".parse().unwrap(),
//!         quinn::EndpointConfig::default(),
//!         None,
//!     )?;
//!     let conn = endpoint.connect_with(client_cfg, server, "localhost")?.await?;
//!     // ...
//! });
//! ```
//!
//! Certificate roots: wasi has no OS trust store, so
//! `rustls-platform-verifier` and `rustls-native-certs` cannot apply.
//! Bring roots explicitly — `webpki-roots` for the public web, or a
//! directly trusted certificate for a closed deployment.

mod addr;
mod error;
pub mod reactor;
mod runtime;
mod socket;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

pub use runtime::WasiRuntime;
pub use socket::WasiUdpSocket;

/// Builds a [`quinn::Endpoint`] on a wasi UDP socket bound to `bind`.
///
/// Must be called inside a tokio current-thread runtime; the endpoint
/// driver and the reactor are spawned onto it.
pub fn endpoint(
    bind: SocketAddr,
    config: quinn::EndpointConfig,
    server_config: Option<quinn::ServerConfig>,
) -> io::Result<quinn::Endpoint> {
    let socket = WasiUdpSocket::bind(bind)?;
    quinn::Endpoint::new_with_abstract_socket(config, server_config, socket, Arc::new(WasiRuntime))
}

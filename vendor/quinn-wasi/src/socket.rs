//! [`quinn::AsyncUdpSocket`] over `wasi:sockets/udp`.

use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use wasi::sockets::instance_network::instance_network;
use wasi::sockets::network::{ErrorCode, IpAddressFamily};
use wasi::sockets::udp::{
    IncomingDatagramStream, OutgoingDatagram, OutgoingDatagramStream, UdpSocket,
};
use wasi::sockets::udp_create_socket::create_udp_socket;

use crate::reactor::{Key, Reactor};
use crate::{addr, error};

/// A wasi UDP socket in unconnected mode: one socket, many peers, every
/// outgoing datagram naming its destination.
pub struct WasiUdpSocket {
    // Streams before their socket, so they drop first.
    incoming: IncomingDatagramStream,
    outgoing: OutgoingDatagramStream,
    _socket: UdpSocket,
    local: SocketAddr,
    reactor: Arc<Reactor>,
    recv_key: Key,
    send_key: Key,
}

impl WasiUdpSocket {
    /// Binds to `bind` and takes the socket's single datagram stream pair.
    ///
    /// Must be called inside a tokio current-thread runtime — the shared
    /// reactor spawns onto it on first use.
    pub fn bind(bind: SocketAddr) -> io::Result<Arc<Self>> {
        let family = match bind {
            SocketAddr::V4(_) => IpAddressFamily::Ipv4,
            SocketAddr::V6(_) => IpAddressFamily::Ipv6,
        };
        let socket = create_udp_socket(family).map_err(error::map)?;

        // Two-phase bind; would-block waits on the socket's pollable.
        let network = instance_network();
        socket
            .start_bind(&network, addr::to_wasi(bind))
            .map_err(error::map)?;
        loop {
            match socket.finish_bind() {
                Err(ErrorCode::WouldBlock) => socket.subscribe().block(),
                other => break other.map_err(error::map)?,
            }
        }
        let local = addr::from_wasi(socket.local_address().map_err(error::map)?);

        // The one stream pair, unconnected. Taking another pair would
        // invalidate this one, so it is taken exactly once, here.
        let (incoming, outgoing) = socket.stream(None).map_err(error::map)?;

        let reactor = Reactor::global();
        let recv_key = reactor.register(incoming.subscribe());
        let send_key = reactor.register(outgoing.subscribe());

        Ok(Arc::new(Self {
            incoming,
            outgoing,
            _socket: socket,
            local,
            reactor,
            recv_key,
            send_key,
        }))
    }
}

impl Drop for WasiUdpSocket {
    fn drop(&mut self) {
        self.reactor.remove(self.recv_key);
        self.reactor.remove(self.send_key);
    }
}

impl AsyncUdpSocket for WasiUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(SendPoller { socket: self })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        // ECN and source-address hints have no wasi equivalent; dropped.
        error::budget(self.outgoing.check_send())?;
        let datagram = OutgoingDatagram {
            data: transmit.contents.to_vec(),
            remote_address: Some(addr::to_wasi(transmit.destination)),
        };
        let sent = self.outgoing.send(&[datagram]).map_err(error::map)?;
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send accepted no datagram",
            ));
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let max = bufs.len().min(meta.len());
        let datagrams = match self.incoming.receive(max as u64) {
            Ok(datagrams) => datagrams,
            Err(code) => return Poll::Ready(Err(error::map(code))),
        };
        if datagrams.is_empty() {
            self.reactor.arm(self.recv_key, cx.waker());
            return Poll::Pending;
        }
        for (i, datagram) in datagrams.iter().enumerate() {
            let len = datagram.data.len().min(bufs[i].len());
            bufs[i][..len].copy_from_slice(&datagram.data[..len]);
            meta[i] = RecvMeta {
                addr: addr::from_wasi(datagram.remote_address),
                len,
                stride: len,
                ecn: None,
                dst_ip: None,
            };
        }
        Poll::Ready(Ok(datagrams.len()))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
}

impl std::fmt::Debug for WasiUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasiUdpSocket")
            .field("local", &self.local)
            .finish()
    }
}

/// Waits for send budget on behalf of one task.
struct SendPoller {
    socket: Arc<WasiUdpSocket>,
}

impl UdpPoller for SendPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        match self.socket.outgoing.check_send() {
            Ok(0) => {
                self.socket.reactor.arm(self.socket.send_key, cx.waker());
                Poll::Pending
            }
            Ok(_) => Poll::Ready(Ok(())),
            Err(code) => Poll::Ready(Err(error::map(code))),
        }
    }
}

impl std::fmt::Debug for SendPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendPoller").finish()
    }
}

//! [`quinn::Runtime`] over the ambient tokio current-thread runtime.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime};

/// quinn runtime for wasip2: tokio tasks, tokio timers.
///
/// The timer clock is `tokio::time`, which on wasip2 rests on the wasi
/// monotonic clock through `std::time::Instant`.
#[derive(Debug, Default, Clone, Copy)]
pub struct WasiRuntime;

impl Runtime for WasiRuntime {
    fn new_timer(&self, i: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(WasiTimer {
            sleep: tokio::time::sleep_until(tokio::time::Instant::from_std(i)),
        })
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }

    fn wrap_udp_socket(&self, _t: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        // std sockets have no wasi:sockets resource to recover.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no std sockets on wasi; bind with quinn_wasi::WasiUdpSocket",
        ))
    }
}

pin_project_lite::pin_project! {
    struct WasiTimer {
        #[pin]
        sleep: tokio::time::Sleep,
    }
}

impl AsyncTimer for WasiTimer {
    fn reset(self: Pin<&mut Self>, i: Instant) {
        self.project()
            .sleep
            .reset(tokio::time::Instant::from_std(i));
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        // Named form: with quinn's runtime-tokio feature on (the echo
        // test's native side), Sleep also implements AsyncTimer.
        Future::poll(self.project().sleep, cx)
    }
}

impl std::fmt::Debug for WasiTimer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasiTimer")
            .field("deadline", &self.sleep.deadline())
            .finish()
    }
}

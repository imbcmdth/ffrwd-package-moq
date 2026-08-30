# quinn-wasi

quinn's pluggable I/O floor on standard wasi:sockets. A wasm32-wasip2
component gets a full QUIC endpoint — handshake, streams, datagrams —
on any host that grants it network access: wasmtime with
`-S inherit-network`, or the ffrwd sidecar under `-net`. No invented
host interface; the imports are `wasi:sockets`, `wasi:io` and
`wasi:clocks`, so the component is portable to any wasip2 host.

## What it provides

- `WasiRuntime` — `quinn::Runtime` over the ambient tokio
  current-thread runtime; timers are `tokio::time`, whose clock is the
  wasi monotonic clock.
- `WasiUdpSocket` — `quinn::AsyncUdpSocket` over a `wasi:sockets/udp`
  socket in unconnected mode (`stream(none)`): one socket serves many
  peers and each outgoing datagram names its destination.
- `endpoint(bind, config, server_config)` — binds a socket and builds
  a `quinn::Endpoint` on both, via
  `Endpoint::new_with_abstract_socket`. Call it inside a tokio
  current-thread runtime.

Readiness is bridged by a reactor task: wasi exposes readiness only as
pollables, and tokio's parker cannot wait on them, so sockets arm their
pollables with wakers and the reactor sweeps the armed set with the
non-blocking `ready()`, sleeping 1ms between sweeps through
`tokio::time`. Tokio timers stay exact; socket readiness is seen at
most a millisecond late. Parking the runtime inside
`wasi:io/poll.poll` itself would remove that millisecond, but needs a
park hook tokio does not expose. Pollables are level-triggered, so the
sweep cannot lose events.

## Backpressure

The wasi outgoing stream names its send budget: `check-send` returns
how many datagrams may go out right now (~16 observed under wasmtime).
`try_send` refuses with `WouldBlock` on a zero budget; quinn then waits
on the socket's `UdpPoller`, whose `poll_writable` re-checks the budget
and otherwise arms the outgoing pollable. Oversized datagrams fail
per-send with `datagram-too-large` (mapped to `InvalidInput`); wasi has
no MTU getter, so turn quinn's MTU discovery off
(`TransportConfig::mtu_discovery_config(None)`) and let the QUIC floor
of 1200 carry everything.

## Certificates

wasi has no OS trust store; `rustls-platform-verifier` and
`rustls-native-certs` cannot apply. Bring roots explicitly:
`webpki-roots` for the public web, or a directly trusted certificate
for a closed deployment (what the echo test does).

TLS backend: `rustls-ring`. ring's C compiles for wasm32-wasip2 under
a wasm-capable clang — point `CC_wasm32_wasip2` at wasi-sdk's clang
(the echo test derives it from `WASI_SDK_PATH`). A zero-C build would
need a pure-Rust rustls provider wired through quinn's
`EndpointConfig::new(Arc<dyn HmacKey>)` seam; not done here.

## quinn-udp patch status

quinn-udp 0.5.15 does not build for wasm32-wasip2: its fallback path
calls `recv_from_vectored`, which socket2 lacks on wasi, and returns a
byte count where `()` is expected. `../vendor/quinn-udp` is the release
with the two-line fix (marked `ffrwd patch`), applied through
`[patch.crates-io]` by this crate and echo-guest. Upstreaming the fix
retires the vendored copy. The driver itself never touches that code —
it exists so the quinn build closes.

## Tests

- `cargo test --lib` — host-run unit tests: address conversions, error
  mapping, the send-budget rule.
- `cargo test --test echo` — the end-to-end proof. Builds
  `../echo-guest` for wasm32-wasip2, starts a native quinn endpoint on
  localhost with a fresh self-signed certificate, runs the guest under
  `wasmtime run -S inherit-network`, and asserts the transcript:
  handshake, a 16 KiB bidirectional echo, clean close. Needs `wasmtime`
  on PATH (or `WASMTIME`), and `CC_wasm32_wasip2` or `WASI_SDK_PATH`
  for ring.

## Not done

- ECN — wasi datagrams carry no ECN bits; quinn's are dropped on send
  and reported absent on receive.
- GSO/GRO — one datagram per transmit and per receive
  (`max_transmit_segments` = 1).
- Connection migration / `src_ip` — the socket has one local address;
  source-address hints are ignored.
- MTU discovery — no MTU getter in wasi; keep it off (above).
- The reactor's 1ms sweep — replaceable by a poll-based park once the
  runtime exposes one.

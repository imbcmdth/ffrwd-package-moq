//! A packet sink that publishes MoQ: encoded h264 packets in, a live
//! broadcast on a relay out.
//!
//! The stream's out-of-band SPS/PPS become an init segment, published
//! on its own track so a late subscriber always reads the decoder
//! config before the media. Each group of pictures becomes one
//! `moof`+`mdat` fragment - one MoQ frame - in a MoQ group of its own,
//! rotated where the encoder put its keyframes. The first publish is
//! held until the media track gains a consumer: a MoQ subscription
//! starts at the latest group, so anything published into a
//! subscriberless broadcast would be gone before it could be watched.
//!
//! One row leaves per published group - packet count, fragment bytes,
//! pts range - and the final call adds a summary.
//!
//! The QUIC session makes progress only inside `init` and `process`
//! calls: the host's packet cadence is the driver's clock. A steady
//! feed keeps it live, and the final call drains the wire before the
//! session closes.

// The module is wasm32-wasip2 alone: its transport rides wasi:sockets.
// A native build of the workspace compiles this crate to nothing, so
// `cargo test` on the host never chases the wasi-only dependencies.
#[cfg(target_os = "wasi")]
mod module;

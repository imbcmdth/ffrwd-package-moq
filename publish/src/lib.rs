//! A packet sink that publishes MoQ: encoded h264 and aac packets in, a
//! live broadcast on a relay out.
//!
//! The stream's out-of-band header - h264's SPS/PPS, aac's
//! AudioSpecificConfig - becomes an init segment, carried inside the
//! rendition's catalog entry as hang's `cmaf` container declares, so a
//! subscriber reads the decoder config with the catalog. Each group
//! becomes one `moof`+`mdat` fragment - one MoQ frame - in a MoQ group
//! of its own, rotated where the encoder put its keyframes, or on a
//! target duration for audio, which has none. The query's audio is
//! optional: a query naming only video publishes only video. The first
//! publish is
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
mod doh;
#[cfg(target_os = "wasi")]
mod module;

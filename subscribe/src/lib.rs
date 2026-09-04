//! A packet source that subscribes to MoQ: a live broadcast in,
//! encoded h264 and aac packets out.
//!
//! The relation IS the broadcast. `probe` connects, reads
//! `catalog.json` - hang's shape, each rendition's init segment
//! base64 inside its `cmaf` container - and reports one track per
//! rendition without pulling any media: the coded stream out of the
//! init segment (the `avcC`'s parameter sets as Annex-B extradata, the
//! `esds`'s AudioSpecificConfig), the geometry out of the catalog, and
//! the rendition's own name and codec string beside it. A video
//! rendition and the audio rendition named for it are one relation
//! row, so a broadcast this package published reads back as the
//! relation it was published from. Nothing bounds a broadcast, so the
//! catalog says so.
//!
//! `open` reads the same catalog and subscribes to every rendition's
//! media track at the live edge. Each frame is one fmp4 fragment,
//! demuxed back to the samples it was built from - `tfdt` for the
//! decode time, the `trun` for sizes, durations, composition offsets
//! and sync flags - and handed over as packets in the track's own
//! timescale. A subscription begins at the group in flight, so a video
//! track's first packets may sit past a keyframe: those are absorbed
//! rather than passed on as a group no decoder can start at.
//!
//! The QUIC session makes progress only inside `probe`, `open` and
//! `next` calls: the host's pull cadence is the driver's clock.

// The module is wasm32-wasip2 alone: its transport rides wasi:sockets.
// A native build of the workspace compiles this crate to nothing, so
// `cargo test` on the host never chases the wasi-only dependencies.
#[cfg(target_os = "wasi")]
mod module;

//! A source node that subscribes to MoQ: a live broadcast in, encoded
//! h264 and aac packets and JSON messages out.
//!
//! The relation IS the broadcast. `shape` connects, reads
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
//! `init` reads the same catalog and subscribes to the media track of
//! each rendition the query reads, at the live edge. Each frame is one fmp4 fragment,
//! demuxed back to the samples it was built from - `tfdt` for the
//! decode time, the `trun` for sizes, durations, composition offsets
//! and sync flags - and handed over as packets in the track's own
//! timescale. A subscription begins at the group in flight, so a video
//! track's first packets may sit past a keyframe: those are absorbed
//! rather than passed on as a group no decoder can start at.
//!
//! A rendition in hang's `legacy` container, which is what libmoq and
//! the moq-dev OBS plugin publish, has no init segment: each frame is a
//! varint pts in microseconds and then the sample, an Annex B access
//! unit or a raw AAC frame, handed on in microsecond ticks with its dts
//! at its pts. See `moq_core::legacy`.
//!
//! A data rendition in the catalog's `data` section is a track too, of
//! JSON messages: each group's one frame is a message in hang's `legacy`
//! framing, handed on as it arrived at the pts the frame carries, in the
//! timescale the catalog names. It rides on the first relation row,
//! beside the media.
//!
//! The node keeps its own time. The QUIC session makes progress only
//! inside its calls, and a tick blocks until something is ready.

// The module is wasm32-wasip2 alone: its transport rides wasi:sockets.
// A native build of the workspace compiles this crate to nothing, so
// `cargo test` on the host never chases the wasi-only dependencies.
#[cfg(target_os = "wasi")]
mod module;

#[cfg(target_os = "wasi")]
ffrwd_node::export!(module::Subscribe);

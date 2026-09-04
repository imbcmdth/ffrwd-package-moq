//! MoQ for ffrwd: the transport, the group discipline, and the fmp4
//! packaging a packet sink writes and a packet source reads back.
//!
//! - [`mux`] builds fragmented MP4 from encoded packets: an init
//!   segment from the stream's out-of-band header - h264's SPS/PPS into
//!   an `avc1`, AAC's AudioSpecificConfig into an `mp4a` - then one
//!   `moof`+`mdat` fragment per sample, each marked where it starts a
//!   group: at keyframe packets for video and on a target duration for
//!   audio, which has none.
//! - [`avc`] is the h264 byte-level knowledge under it: Annex-B NAL
//!   cutting, AVCC length-prefix framing, and the `avcC` record.
//! - [`catalog`] is the document a broadcast describes itself with - in
//!   the hang media layer's shape, each rendition's init segment inside
//!   - and the names its renditions publish under.
//! - [`subscribe`] is the receiving side before the media: the
//!   subscription shape, the catalog read, and [`subscribe::FrameStream`]
//!   pulling groups and frames off a track in arrival order.
//! - [`fmp4`] cuts a fragmented-MP4 byte stream back into segments -
//!   the parse-side mirror of [`mux`], used by tests to read what the
//!   muxer and ffmpeg each wrote.
//! - [`demux`] reads those segments back into the encoded packets they
//!   were built from, which is what a packet source hands an edge.
//! - [`relay`] dials: the URL, the name lookup, the trust and the token
//!   a publisher and a subscriber both need. [`wasi::connect`]
//!   (wasm32-wasip2 only) is the socket under it, quinn over quinn-wasi
//!   wrapped as a raw-QUIC MoQ session.
//! - [`dns`] is the wire codec under [`doh`], the DNS-over-HTTPS relay
//!   lookup: encode a question, read the answer's addresses.

pub mod avc;
pub mod catalog;
pub mod demux;
pub mod dns;
pub mod fmp4;
pub mod mux;
pub mod relay;
pub mod subscribe;

#[cfg(target_os = "wasi")]
pub mod doh;
#[cfg(target_os = "wasi")]
pub mod wasi;

use std::fmt;

/// Errors from the publish/subscribe cores.
#[derive(Debug)]
pub enum Error {
	/// The underlying MoQ session or model refused the operation.
	Moq(moq_net::Error),
	/// A frame timestamp does not fit the track's timescale.
	Time(moq_net::TimeOverflow),
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Moq(err) => write!(f, "moq: {err}"),
			Error::Time(err) => write!(f, "timestamp: {err}"),
		}
	}
}

impl std::error::Error for Error {}

impl From<moq_net::Error> for Error {
	fn from(err: moq_net::Error) -> Self {
		Error::Moq(err)
	}
}

impl From<moq_net::TimeOverflow> for Error {
	fn from(err: moq_net::TimeOverflow) -> Self {
		Error::Time(err)
	}
}

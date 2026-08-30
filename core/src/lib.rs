//! MoQ publishing for ffrwd: the transport, the group discipline, and
//! the fmp4 packaging a packet sink needs.
//!
//! - [`mux`] builds fragmented MP4 from encoded h264 packets: an init
//!   segment from the stream's out-of-band SPS/PPS, then one
//!   `moof`+`mdat` fragment per group of pictures, rotated at keyframe
//!   packets.
//! - [`avc`] is the h264 byte-level knowledge under it: Annex-B NAL
//!   cutting, AVCC length-prefix framing, and the `avcC` record.
//! - [`subscribe::FrameStream`] pulls groups and frames off a track
//!   subscription in arrival order (the test harness's receiving side).
//! - [`fmp4`] cuts a fragmented-MP4 byte stream back into segments -
//!   the parse-side mirror of [`mux`], used by tests to read what the
//!   muxer and ffmpeg each wrote.
//! - [`wasi::connect`] (wasm32-wasip2 only) dials a relay over
//!   quinn-wasi and hands back a raw-QUIC MoQ session.

pub mod avc;
pub mod fmp4;
pub mod mux;
pub mod subscribe;

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

//! MoQ for ffrwd: the transport, the group discipline, and the catalog
//! a packet sink publishes and a packet source reads back.
//!
//! The fmp4 packaging under it is not here. Boxes, sample tables, the
//! muxer and the scanner are
//! [`ffrwd_bmff`](https://github.com/imbcmdth/ffrwd-bmff); NAL units,
//! the `avcC` record and the RFC 6381 codec string are
//! [`ffrwd_nal`](https://github.com/imbcmdth/ffrwd-nal). What stays in
//! this crate is what MoQ decides rather than what a container says.
//!
//! - [`group`] is that decision: where a group opens, at keyframes for
//!   video and on a target duration for audio, which has none.
//! - [`order`] is the same decision on the way in: a subscription
//!   hands a track's groups over in arrival order, and [`order::Queue`]
//!   is what puts them back in sequence, what it waits for before it
//!   gives up on one, and what it counts while it does.
//! - [`catalog`] is the document a broadcast describes itself with - in
//!   the hang media layer's shape, each rendition's init segment inside
//!   - and the names its renditions publish under.
//! - [`message`] is a data track's frame: a message's pts ahead of its
//!   bytes, the way hang's `legacy` container frames a sample.
//! - [`subscribe`] is the receiving side before the media: the
//!   subscription shape, the catalog read, and [`subscribe::FrameStream`]
//!   pulling groups and frames off a track in arrival order.
//! - [`relay`] dials: the URL, the name lookup, the trust and the token
//!   a publisher and a subscriber both need. [`wasi::connect`]
//!   (wasm32-wasip2 only) is the socket under it, quinn over quinn-wasi
//!   wrapped as a raw-QUIC MoQ session.
//! - [`dns`] is the wire codec under [`doh`], the DNS-over-HTTPS relay
//!   lookup: encode a question, read the answer's addresses.

pub mod catalog;
pub mod dns;
pub mod group;
pub mod message;
pub mod order;
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

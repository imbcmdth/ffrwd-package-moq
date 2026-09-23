//! The subscribe core: MoQ groups in, frames out.
//!
//! [`FrameStream`] wraps a track subscriber and yields one
//! `(timestamp, payload)` frame at a time. Groups are taken in arrival
//! order and read to completion; the stream ends when the publisher
//! finishes the track.
//!
//! Beside it, what any subscriber does before the media: where a
//! reader joins a broadcast already running ([`Start`]) and the one
//! document off [`crate::catalog::TRACK`] that says what a broadcast
//! carries ([`read_catalog`]).

use std::time::Duration;

use bytes::Bytes;

use crate::Error;

/// How long a group that is no longer the latest is waited for rather
/// than skipped, on the wire and in this package's own hold.
///
/// This is moq-net's `latency_max`, and the number matters because the
/// value it replaces is zero: "The maximum age of a non-latest group
/// before it is skipped. `Duration::ZERO` skips immediately (e.g. group
/// 8 arriving means group 7 is skipped); a larger value tolerates that
/// much reordering before giving up on the older group." A subscription
/// left at its default therefore loses a group the moment a newer one
/// exists, which is what a player wanting the smallest delay takes and
/// the opposite of what a reader feeding a pipeline wants. Every
/// subscription this package opens asks for 30 seconds instead.
///
/// It is a REQUEST. The publisher's own track window caps it - this
/// package publishes with the same 30 seconds - and a relay's
/// `--cache-duration` caps that in turn.
pub const BACKLOG: Duration = Duration::from_secs(30);

/// Where a reader joins a broadcast that is already running.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Start {
	/// At the live edge, which is the default: the publisher serves from
	/// its newest group and the reader starts at the first group it can
	/// decode from. A node reading a live broadcast is then one group
	/// behind it rather than a retention window behind it.
	#[default]
	Live,
	/// At the oldest group the publisher still holds, which on a
	/// broadcast that has been running is the live edge less its
	/// retention window.
	///
	/// What this is for is a publisher running AHEAD of real time - a
	/// file poured into a relay as fast as it will take it - and a
	/// reader that wants every frame of it rather than the newest ones.
	/// It is the capture tool, and it costs the whole backlog's delay
	/// before the first packet comes out.
	Backlog,
}

impl Start {
	/// The name a query spells this with.
	pub fn parse(text: &str) -> Result<Self, String> {
		match text {
			"live" => Ok(Start::Live),
			"backlog" => Ok(Start::Backlog),
			other => Err(format!(
				"start is 'live' (join at the live edge) or 'backlog' (take what the relay still \
				 holds), and not '{other}'"
			)),
		}
	}

	/// The subscription this asks the publisher for.
	pub fn subscription(self) -> moq_net::track::Subscription {
		match self {
			Start::Live => live_edge(),
			Start::Backlog => from_start(),
		}
	}
}

/// Every group the publisher still holds, in order: a `group_start` of
/// 0 asks to be served from the oldest sequence there can be, and what
/// arrives is whatever of it is still in the cache.
pub fn from_start() -> moq_net::track::Subscription {
	live_edge().with_group_start(0)
}

/// From the publisher's newest group instead, which is what leaving
/// `group_start` unset means: "First group the publisher should
/// deliver, or `None` to start at the latest group."
///
/// What it does NOT mean is that the newest group keeps being jumped
/// to. moq-net's publisher fixes the cursor once, at the group that was
/// latest when the subscription was accepted - `start_group.or_else(||
/// track.latest())` - and from there serves every group in arrival
/// order off that one cursor. A reader that falls behind is not skipped
/// ahead; it loses a group only if the publisher's cache evicts one
/// before the serving loop reaches it, and that is [`BACKLOG`] of the
/// group going untouched. So a live node that lags catches up, which is
/// what a node should do, and a node lagging by more than the window
/// has a problem no transport setting papers over.
pub fn live_edge() -> moq_net::track::Subscription {
	moq_net::track::Subscription::default()
		.with_ordered(true)
		.with_latency_max(BACKLOG)
}

/// How long to wait before asking a broadcast for its catalog again,
/// and how many times. A publisher republishes the same document in a
/// fresh group every few seconds, so a group that aged out from under
/// the read is followed by another carrying the same thing.
const CATALOG_RETRY: Duration = Duration::from_millis(250);
const CATALOG_TRIES: u32 = 40;

/// The catalog document a broadcast describes itself with, read off
/// its own track: the LATEST group, since the newest document wins.
pub async fn read_catalog(
	broadcast: &moq_net::broadcast::Consumer,
) -> Result<String, Box<dyn std::error::Error>> {
	let mut last = String::new();
	for _ in 0..CATALOG_TRIES {
		let track = broadcast.track(crate::catalog::TRACK)?;
		let subscription = moq_net::track::Subscription::default()
			.with_ordered(true)
			.with_latency_max(BACKLOG);
		let mut stream = FrameStream::new(track.subscribe(subscription).await?);
		match stream.next().await {
			Ok(Some(frame)) => return Ok(String::from_utf8(frame.payload.to_vec())?),
			Ok(None) => return Err("the catalog track finished without a document".into()),
			Err(err) => last = err.to_string(),
		}
		tokio::time::sleep(CATALOG_RETRY).await;
	}
	Err(format!("the catalog track kept dropping its groups (last: {last})").into())
}

/// One frame as received, with its position in the group hierarchy.
#[derive(Clone, Debug)]
pub struct Received {
	/// The group's per-track sequence number.
	pub group: u64,
	/// The frame's index within its group.
	pub frame_in_group: u64,
	/// Presentation timestamp in microseconds.
	pub timestamp_us: u64,
	/// The frame payload.
	pub payload: Bytes,
}

/// Yields frames from a track subscription, group by group.
pub struct FrameStream {
	subscriber: moq_net::track::Subscriber,
	current: Option<(moq_net::group::Consumer, u64)>,
	/// Called when a group is read to completion: `(sequence, frames)`.
	on_group: Option<Box<dyn FnMut(u64, u64) + Send>>,
}

impl FrameStream {
	/// Wraps a subscriber; frames come from [`Self::next`].
	pub fn new(subscriber: moq_net::track::Subscriber) -> Self {
		Self {
			subscriber,
			current: None,
			on_group: None,
		}
	}

	/// Report each completed group through `f(sequence, frames)`.
	pub fn on_group(mut self, f: impl FnMut(u64, u64) + Send + 'static) -> Self {
		self.on_group = Some(Box::new(f));
		self
	}

	/// The next frame, or `None` once the track is finished.
	pub async fn next(&mut self) -> Result<Option<Received>, Error> {
		loop {
			if let Some((group, index)) = self.current.as_mut() {
				if let Some(frame) = group.read_frame().await? {
					let timestamp_us = frame
						.timestamp
						.convert(moq_net::Timescale::MICRO)?
						.value();
					let received = Received {
						group: group.sequence,
						frame_in_group: *index,
						timestamp_us,
						payload: frame.payload,
					};
					*index += 1;
					return Ok(Some(received));
				}
				let (group, frames) = self.current.take().expect("current group");
				if let Some(on_group) = self.on_group.as_mut() {
					on_group(group.sequence, frames);
				}
			}

			match self.subscriber.recv_group().await? {
				Some(group) => self.current = Some((group, 0)),
				None => return Ok(None),
			}
		}
	}
}

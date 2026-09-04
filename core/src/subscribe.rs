//! The subscribe core: MoQ groups in, frames out.
//!
//! [`FrameStream`] wraps a track subscriber and yields one
//! `(timestamp, payload)` frame at a time. Groups are taken in arrival
//! order and read to completion; the stream ends when the publisher
//! finishes the track.
//!
//! Beside it, what any subscriber does before the media: the
//! subscription a reader asks for ([`from_start`]) and the one
//! document off [`crate::catalog::TRACK`] that says what a broadcast
//! carries ([`read_catalog`]).

use std::time::Duration;

use bytes::Bytes;

use crate::Error;

/// How long a backlogged group is waited for rather than skipped. The
/// default drops a non-latest group the moment a newer one exists,
/// which is the opposite of what a reader feeding a pipeline wants.
pub const BACKLOG: Duration = Duration::from_secs(30);

/// Every group the publisher still holds, in order, tolerating
/// backlog: a reader joins at the oldest group still cached, which on
/// a broadcast that has been running is the live edge less its
/// retention window.
///
/// The alternative - the latest group alone, which is what a default
/// subscription asks for - is what a player wanting the smallest delay
/// takes, and it is not what a reader feeding a pipeline wants: the
/// default skips a group the moment a newer one exists, so a publisher
/// running ahead of real time loses most of what it sent.
pub fn from_start() -> moq_net::track::Subscription {
	live_edge().with_group_start(0)
}

/// The same, from the latest group instead: what a relay that will not
/// serve a backlog leaves, and all a broadcast running for hours has
/// near its edge anyway.
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

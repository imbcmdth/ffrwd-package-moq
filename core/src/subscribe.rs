//! The subscribe core: MoQ groups in, frames out.
//!
//! Wraps a track subscriber and yields one `(timestamp, payload)` frame
//! at a time — the exact shape a future sidecar sink binding will drain.
//! Groups are taken in arrival order and read to completion; the stream
//! ends when the publisher finishes the track.

use bytes::Bytes;

use crate::Error;

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

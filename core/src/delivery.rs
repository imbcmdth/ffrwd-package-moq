//! A data track's messages leave one at a time: the next group on the
//! track opens only once the one before it has been delivered.
//!
//! A relay built on the `serve` model of moq-transport (the IETF-draft
//! stack in cloudflare/moq-rs) keeps no more than the LATEST group of a
//! track. A new group replaces it, and each downstream subscriber is
//! handed whichever group is latest when its forwarding task next
//! wakes, so a group superseded before that wake is never forwarded to
//! anyone and cannot be fetched either. Two messages written in one
//! host call are two groups a few microseconds apart on the wire, and
//! Cloudflare's draft-16 relay lost the first of such a pair five times
//! in seven, with a fetch for it answered `not found`. Media
//! never meets this: a video group is a GOP and an audio group a fifth
//! of a second. A data track has no such floor, one message being one
//! group, so this is the floor it gets.
//!
//! Delivered means read to its end by every session serving it and
//! acknowledged: moq-net's serving task holds a consumer of the group
//! until the peer has acknowledged the stream's last byte, so the group
//! falling out of use is the relay having all of it. [`InFlight::settle`]
//! waits for that, bounded by [`DELIVER_MAX`], and then keeps the next
//! group at least [`SPACING`] behind the last one's finish, which is the
//! relay's own time to hand the group on before a newer one replaces it.
//! A track nobody is subscribed to sends nothing, and waits for nothing.
//!
//! The cost is a round trip to the relay per message, and only for a
//! message that follows the one before it closer than that.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::time::Instant;

/// The longest a group is waited on, from when it was finished. Past it
/// the next group goes out regardless: a session that has stalled, or a
/// relay a very long way off, must not stop the track.
pub const DELIVER_MAX: Duration = Duration::from_millis(250);

/// How long a finished group may go untaken by any serving task before
/// it is taken as not being served at all. A subscribed session takes a
/// group the first time it runs after the group was written, so this is
/// spent on a session that has gone, or on a group taken and delivered
/// between two looks that neither saw.
pub const TAKE_MAX: Duration = Duration::from_millis(50);

/// The least time between a group's finish and the next group's start on
/// the same track.
pub const SPACING: Duration = Duration::from_millis(10);

/// One turn of the wait for a group to be taken: long enough for a timer
/// or a socket to come back.
const TURN: Duration = Duration::from_millis(1);

/// A data track's last group, finished and still on its way.
pub struct InFlight {
	group: moq_net::group::Producer,
	finished: Instant,
	/// Whether a serving task has been seen holding it. A group nobody
	/// holds is either not taken yet or already delivered, and this is
	/// what tells the two apart.
	held: bool,
}

/// What a group's consumers say about it right now.
enum Use {
	/// A serving task holds it.
	Held,
	/// Nobody does.
	Free,
	/// It was aborted: nothing more will leave for it.
	Gone,
}

fn in_use(group: &moq_net::group::Producer) -> Use {
	let mut unused = pin!(group.unused());
	match unused.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
		Poll::Pending => Use::Held,
		Poll::Ready(Ok(())) => Use::Free,
		Poll::Ready(Err(_)) => Use::Gone,
	}
}

impl InFlight {
	/// A group just finished.
	pub fn new(group: moq_net::group::Producer) -> Self {
		Self {
			group,
			finished: Instant::now(),
			held: false,
		}
	}

	/// Notes whether a serving task holds the group, without waiting. A
	/// call looks between its own turns of the session, so a group that
	/// was taken and delivered before [`Self::settle`] runs is known to
	/// have been, and is not waited on again.
	pub fn watch(&mut self) {
		if matches!(in_use(&self.group), Use::Held) {
			self.held = true;
		}
	}

	/// Until the group has been delivered and [`SPACING`] has passed since
	/// its finish, or [`DELIVER_MAX`] has; at once for a track with no
	/// subscriber. The session has to run for any of it to happen, so the
	/// caller awaits this on the executor the session is driven on.
	pub async fn settle(mut self, subscribed: bool) {
		if !subscribed {
			return;
		}
		let deadline = self.finished + DELIVER_MAX;
		let taken_by = self.finished + TAKE_MAX;
		loop {
			match in_use(&self.group) {
				Use::Gone => return,
				Use::Held => {
					self.held = true;
					let _ = tokio::time::timeout_at(deadline, self.group.unused()).await;
					break;
				}
				Use::Free if self.held => break,
				Use::Free => {
					if Instant::now() >= taken_by {
						break;
					}
					tokio::task::yield_now().await;
					tokio::time::sleep(TURN).await;
				}
			}
		}
		tokio::time::sleep_until(self.finished + SPACING).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// What keeps the group's track alive while a test runs.
	type Keep = (
		moq_net::origin::Producer,
		moq_net::broadcast::Producer,
		moq_net::track::Producer,
	);

	/// A finished one-frame group on a track of its own.
	fn group() -> (Keep, moq_net::group::Producer) {
		let origin = moq_net::Origin::random().produce();
		let mut broadcast = origin
			.create_broadcast("test", moq_net::broadcast::Route::announced())
			.expect("a broadcast");
		let mut track = broadcast
			.create_track("data", moq_net::track::Info::default())
			.expect("a track");
		let mut group = track.append_group().expect("a group");
		let at = moq_net::Timestamp::from_micros(0).expect("a time");
		group
			.write_frame(at, bytes::Bytes::from_static(b"{}"))
			.expect("a frame");
		group.finish().expect("finished");
		((origin, broadcast, track), group)
	}

	fn millis(since: Instant) -> u128 {
		since.elapsed().as_millis()
	}

	#[tokio::test]
	async fn nothing_is_waited_on_without_a_subscriber() {
		let (_keep, group) = group();
		let _held = group.consume();
		let began = Instant::now();
		InFlight::new(group).settle(false).await;
		assert!(millis(began) < 5, "{}ms", millis(began));
	}

	#[tokio::test]
	async fn the_next_group_waits_for_the_last_to_be_let_go() {
		let (_keep, group) = group();
		let held = group.consume();
		let began = Instant::now();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_millis(60)).await;
			drop(held);
		});
		InFlight::new(group).settle(true).await;
		let took = millis(began);
		assert!((60..DELIVER_MAX.as_millis()).contains(&took), "{took}ms");
	}

	#[tokio::test]
	async fn a_group_delivered_already_costs_only_the_spacing() {
		let (_keep, group) = group();
		let held = group.consume();
		let mut flight = InFlight::new(group);
		flight.watch();
		drop(held);
		let began = Instant::now();
		flight.settle(true).await;
		let took = millis(began);
		assert!(took >= SPACING.as_millis() - 1 && took < TAKE_MAX.as_millis(), "{took}ms");
	}

	#[tokio::test]
	async fn a_group_nobody_takes_is_given_up_on_soon() {
		let (_keep, group) = group();
		let began = Instant::now();
		InFlight::new(group).settle(true).await;
		let took = millis(began);
		assert!(took >= TAKE_MAX.as_millis() - 1 && took < DELIVER_MAX.as_millis(), "{took}ms");
	}

	#[tokio::test]
	async fn a_group_held_for_good_is_waited_on_no_longer_than_the_bound() {
		let (_keep, group) = group();
		let _held = group.consume();
		let began = Instant::now();
		InFlight::new(group).settle(true).await;
		let took = millis(began);
		assert!(
			took >= DELIVER_MAX.as_millis() - 1 && took < DELIVER_MAX.as_millis() + 100,
			"{took}ms"
		);
	}
}

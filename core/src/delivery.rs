//! A track's groups leave one at a time: the next group on a track goes
//! onto the wire only once the one before it has been delivered, and
//! until then it waits in the track's queue, never in the host's call.
//!
//! A relay built on the `serve` model of moq-transport (the IETF-draft
//! stack in cloudflare/moq-rs) keeps no more than the LATEST group of a
//! track. A new group replaces it, and each downstream subscriber is
//! handed whichever group is latest when its forwarding task next
//! wakes, so a group superseded before that wake is never forwarded to
//! anyone and cannot be fetched either. Two messages written in one
//! host call are two groups a few microseconds apart on the wire, and
//! Cloudflare's draft-16 relay lost the first of such a pair five times
//! in seven, with a fetch for it answered `not found`. Media meets the
//! same thing more rarely: a burst of audio groups after a stalled call,
//! or one-frame audio groups, lost whole groups on that relay, and one
//! audio group went exactly where a data pair had held the call.
//!
//! Delivered means read to its end by every session serving it and
//! acknowledged: moq-net's serving task holds a consumer of the group
//! until the peer has acknowledged the stream's last byte, so the group
//! falling out of use is the relay having all of it. [`Pacer`] looks for
//! that without waiting, whenever the caller gives it a turn, and lets
//! the next group go once it has seen it and [`SPACING`] has passed since
//! the last group's finish, which is the relay's own time to hand the
//! group on before a newer one replaces it. A track nobody is subscribed
//! to sends nothing, and holds nothing back.
//!
//! The cost is a round trip to the relay between two groups of a track,
//! paid by the second group alone and only when it follows the first
//! closer than that. A video group is a GOP and an audio group a fifth of
//! a second, each far longer than a round trip, so on a normal link a
//! media group whose predecessor was finished when it opened is held for
//! about one round trip and then carries on live. A group that follows an
//! acknowledged one goes out at once.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;

/// The longest a group is held behind the one before it, from that
/// one's finish. Past it the group goes out whether or not the one
/// before it was acknowledged.
///
/// What that trades: on a relay that keeps only a track's newest group,
/// a group let go early can overtake the one before it and lose it. But
/// a session that has stalled, or a relay a very long way off, must not
/// stop the track, and every millisecond a group is held is a
/// millisecond the track runs behind real time until the queue catches
/// up. A quarter of a second is ten round trips to a public relay, and
/// no more than a player's jitter buffer absorbs.
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

/// The most groups a track holds back at once. A group queued past it
/// sends the oldest on at once, delivered or not, as [`DELIVER_MAX`]
/// does: a track cutting groups faster than the relay acknowledges them
/// (one frame of audio a group, against a round trip longer than a
/// frame) must not fall further behind real time with every group.
/// Media at the default grouping and a data track's pairs hold one or
/// two; eight is a burst of messages after a stall, at a round trip
/// apiece.
pub const QUEUE_MAX: usize = 8;

/// A track's last group on the wire, finished and still on its way.
struct InFlight {
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
	/// A group just finished, looked at once: a serving task that has
	/// been streaming it holds it still.
	fn new(group: moq_net::group::Producer, now: Instant) -> Self {
		let mut flight = Self {
			group,
			finished: now,
			held: false,
		};
		flight.watch();
		flight
	}

	/// Notes whether a serving task holds the group. Looking between the
	/// caller's own turns of the session is what lets a group taken and
	/// delivered before the next look be known to have gone, rather than
	/// taken for one nobody took.
	fn watch(&mut self) {
		if matches!(in_use(&self.group), Use::Held) {
			self.held = true;
		}
	}

	/// Whether the next group may go, at `now`: `Some(true)` once this
	/// one has been delivered (or is not being served) and [`SPACING`]
	/// has passed since its finish, `Some(false)` once [`DELIVER_MAX`]
	/// has passed without that, and `None` while neither has.
	fn settled(&mut self, now: Instant) -> Option<bool> {
		let delivered = match in_use(&self.group) {
			Use::Gone => return Some(true),
			Use::Held => {
				self.held = true;
				false
			}
			Use::Free => self.held || now >= self.finished + TAKE_MAX,
		};
		if delivered && now >= self.finished + SPACING {
			Some(true)
		} else if now >= self.finished + DELIVER_MAX {
			Some(false)
		} else {
			None
		}
	}
}

/// A group cut and not yet on the wire: its frames so far, and whether
/// it has been finished.
struct Held {
	frames: Vec<(moq_net::Timestamp, Bytes)>,
	closed: bool,
	since: Instant,
}

/// What a track held back, over a window or over the run.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Stats {
	/// The most groups held back at once.
	pub queue_max: usize,
	/// The longest a group was held before it went on the wire.
	pub wait_max: Duration,
	/// Groups let go before the one ahead of them was known delivered,
	/// by [`DELIVER_MAX`] or [`QUEUE_MAX`]: each one a group a relay that
	/// keeps only the newest could have lost.
	pub unpaced: u64,
}

impl Stats {
	fn note(&mut self, queued: usize, waited: Option<Duration>, unpaced: bool) {
		self.queue_max = self.queue_max.max(queued);
		if let Some(waited) = waited {
			self.wait_max = self.wait_max.max(waited);
		}
		self.unpaced += u64::from(unpaced);
	}
}

/// One track's way onto the wire: groups are cut here as the group
/// discipline says, and each goes out once the one before it is
/// delivered.
///
/// Nothing here waits. A group that may not go yet is held with its
/// frames, and every call to [`Pacer::release`] lets out what has become
/// free since, oldest first; frames written meanwhile join the newest
/// group, held or not. The caller gives it those turns from wherever the
/// session is running: the rest of a host call, and the next one.
#[derive(Default)]
pub struct Pacer {
	queue: VecDeque<Held>,
	/// The group on the wire that frames are still being written into.
	/// Only ever set with the queue empty.
	open: Option<moq_net::group::Producer>,
	/// The last group on the wire to be finished, until the next one goes.
	last: Option<InFlight>,
	appended: u64,
	closed: u64,
	window: Stats,
	run: Stats,
}

impl Pacer {
	pub fn new() -> Self {
		Self::default()
	}

	/// Cuts a new group: the one before it is finished, and the new one
	/// goes onto the wire at once if nothing is ahead of it, or joins the
	/// queue if something is.
	pub fn open(
		&mut self,
		track: &mut moq_net::track::Producer,
		subscribed: bool,
		now: Instant,
	) -> moq_net::Result<()> {
		self.close(now)?;
		self.queue.push_back(Held {
			frames: Vec::new(),
			closed: false,
			since: now,
		});
		self.release(track, subscribed, now).map(|_| ())
	}

	/// One frame into the newest group, on the wire or held.
	pub fn write(&mut self, timestamp: moq_net::Timestamp, data: Bytes) -> moq_net::Result<()> {
		match (self.queue.back_mut(), self.open.as_mut()) {
			(Some(held), _) => {
				held.frames.push((timestamp, data));
				Ok(())
			}
			(None, Some(open)) => open.write_frame(timestamp, data),
			(None, None) => panic!("a frame written with no group open"),
		}
	}

	/// Finishes the newest group: on the wire now, or when it leaves the
	/// queue. Nothing happens if it is finished already.
	pub fn close(&mut self, now: Instant) -> moq_net::Result<()> {
		if let Some(held) = self.queue.back_mut() {
			held.closed = true;
		} else if let Some(mut group) = self.open.take() {
			group.finish()?;
			self.closed += 1;
			self.last = Some(InFlight::new(group, now));
		}
		Ok(())
	}

	/// Puts on the wire every held group that may go at `now`, oldest
	/// first, and says how many went. A group goes once the one before it
	/// has settled (see [`DELIVER_MAX`]), at once when nobody is
	/// subscribed, and regardless when the queue is over [`QUEUE_MAX`].
	pub fn release(
		&mut self,
		track: &mut moq_net::track::Producer,
		subscribed: bool,
		now: Instant,
	) -> moq_net::Result<usize> {
		let mut released = 0;
		while !self.queue.is_empty() {
			let settled = match self.last.as_mut() {
				None => Some(true),
				Some(_) if !subscribed => Some(true),
				Some(last) => last.settled(now),
			};
			let paced = match settled {
				Some(paced) => paced,
				None if self.queue.len() > QUEUE_MAX => false,
				None => break,
			};
			let held = self.queue.pop_front().expect("not empty");
			let waited = now.saturating_duration_since(held.since);
			self.window.note(0, Some(waited), !paced);
			self.run.note(0, Some(waited), !paced);
			let mut group = track.append_group()?;
			self.appended += 1;
			for (timestamp, data) in held.frames {
				group.write_frame(timestamp, data)?;
			}
			self.last = None;
			if held.closed {
				group.finish()?;
				self.closed += 1;
				self.last = Some(InFlight::new(group, now));
			} else {
				self.open = Some(group);
			}
			released += 1;
		}
		let queued = self.queue.len();
		self.window.note(queued, None, false);
		self.run.note(queued, None, false);
		Ok(released)
	}

	/// Notes whether a serving task holds the last group, between two of
	/// the caller's turns of the session. See [`InFlight::watch`].
	pub fn watch(&mut self) {
		if let Some(last) = self.last.as_mut() {
			last.watch();
		}
	}

	/// Whether the last group on the wire is still held by a serving task:
	/// the next group cut would wait for it. An acknowledgement that has
	/// reached the socket reaches the serving task only as the session
	/// runs, so a caller that has left the session still for a while gives
	/// it turns while this says so, before cutting.
	pub fn unsettled(&self) -> bool {
		self.last
			.as_ref()
			.is_some_and(|last| matches!(in_use(&last.group), Use::Held))
	}

	/// Groups held back right now.
	pub fn queued(&self) -> usize {
		self.queue.len()
	}

	/// Groups put on the wire, and groups finished there.
	pub fn appended(&self) -> u64 {
		self.appended
	}

	pub fn closed(&self) -> u64 {
		self.closed
	}

	/// What was held back since [`Self::next_window`] last ran.
	pub fn window(&self) -> Stats {
		self.window
	}

	/// What was held back over the whole run.
	pub fn run(&self) -> Stats {
		self.run
	}

	/// Starts a new window's counts.
	pub fn next_window(&mut self) {
		self.window = Stats::default();
	}
}

#[cfg(test)]
impl Pacer {
	/// A consumer of the newest group on the wire, as a serving task
	/// takes one: it holds the group until dropped.
	fn serve(&self) -> Option<moq_net::group::Consumer> {
		self.open
			.as_ref()
			.or(self.last.as_ref().map(|last| &last.group))
			.map(moq_net::group::Producer::consume)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// What keeps a track alive while a test runs.
	struct Track {
		_origin: moq_net::origin::Producer,
		_broadcast: moq_net::broadcast::Producer,
		track: moq_net::track::Producer,
	}

	fn track() -> Track {
		let origin = moq_net::Origin::random().produce();
		let mut broadcast = origin
			.create_broadcast("test", moq_net::broadcast::Route::announced())
			.expect("a broadcast");
		let track = broadcast
			.create_track("data", moq_net::track::Info::default())
			.expect("a track");
		Track {
			_origin: origin,
			_broadcast: broadcast,
			track,
		}
	}

	fn at(micros: u64) -> moq_net::Timestamp {
		moq_net::Timestamp::from_micros(micros).expect("a time")
	}

	fn ms(millis: u64) -> Duration {
		Duration::from_millis(millis)
	}

	/// One whole group of one frame, cut, written and finished.
	fn message(pacer: &mut Pacer, track: &mut Track, body: u64, now: Instant) {
		pacer.open(&mut track.track, true, now).expect("open");
		pacer
			.write(at(body), Bytes::from(body.to_string()))
			.expect("write");
		pacer.close(now).expect("close");
	}

	/// The frames of the newest group on the wire, read and let go.
	async fn newest(pacer: &Pacer) -> Vec<String> {
		let mut group = pacer.serve().expect("a group on the wire");
		let mut frames = Vec::new();
		while let Ok(Ok(Some(frame))) = tokio::time::timeout(ms(20), group.read_frame()).await {
			frames.push(String::from_utf8(frame.payload.to_vec()).expect("utf-8"));
		}
		frames
	}

	#[tokio::test]
	async fn a_group_with_nothing_ahead_goes_at_once() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		assert_eq!(pacer.queued(), 0);
		assert_eq!((pacer.appended(), pacer.closed()), (1, 1));
		assert_eq!(newest(&pacer).await, ["1"]);
		assert_eq!(pacer.run().queue_max, 0, "nothing was held");
	}

	#[tokio::test]
	async fn a_group_behind_an_undelivered_one_waits_in_the_queue_not_the_call() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		let held = pacer.serve().expect("the first group");
		pacer.watch();
		// The second and third are cut at once and nothing blocks: they
		// are held, in order.
		message(&mut pacer, &mut track, 2, now);
		message(&mut pacer, &mut track, 3, now);
		assert_eq!(pacer.queued(), 2);
		assert_eq!(pacer.release(&mut track.track, true, now + ms(30)).expect("release"), 0);
		// Delivered: the next goes, and only the next, since the third
		// waits in turn for the second.
		drop(held);
		let later = now + ms(40);
		assert_eq!(pacer.release(&mut track.track, true, later).expect("release"), 1);
		let second = pacer.serve().expect("the second group");
		pacer.watch();
		assert_eq!(pacer.release(&mut track.track, true, later + ms(5)).expect("release"), 0);
		drop(second);
		let later = later + ms(10);
		assert_eq!(pacer.release(&mut track.track, true, later).expect("release"), 1);
		assert_eq!(pacer.queued(), 0);
		assert_eq!(newest(&pacer).await, ["3"]);
		assert_eq!(pacer.appended(), 3);
		let stats = pacer.run();
		assert_eq!(stats.queue_max, 2);
		assert_eq!(stats.wait_max, ms(50));
		assert_eq!(stats.unpaced, 0);
	}

	#[tokio::test]
	async fn the_queue_goes_out_in_the_order_it_was_cut() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 0, now);
		let mut held = pacer.serve();
		pacer.watch();
		for body in 1..=4 {
			message(&mut pacer, &mut track, body, now);
		}
		let mut order = Vec::new();
		let mut clock = now;
		while pacer.queued() > 0 {
			drop(held.take());
			clock += SPACING;
			assert_eq!(pacer.release(&mut track.track, true, clock).expect("release"), 1);
			order.extend(newest(&pacer).await);
			held = pacer.serve();
			pacer.watch();
		}
		assert_eq!(order, ["1", "2", "3", "4"]);
	}

	#[tokio::test]
	async fn a_delivered_group_still_keeps_the_next_the_spacing_behind() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		let held = pacer.serve();
		pacer.watch();
		drop(held);
		message(&mut pacer, &mut track, 2, now + ms(1));
		assert_eq!(pacer.queued(), 1, "held for the spacing");
		assert_eq!(pacer.release(&mut track.track, true, now + SPACING).expect("release"), 1);
	}

	#[tokio::test]
	async fn a_group_whose_predecessor_was_delivered_long_ago_goes_at_once() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		let held = pacer.serve().expect("the first group");
		pacer.watch();
		drop(held);
		message(&mut pacer, &mut track, 2, now + ms(500));
		assert_eq!(pacer.queued(), 0);
		assert_eq!(pacer.run().queue_max, 0);
		assert_eq!(pacer.run().wait_max, Duration::ZERO);
	}

	#[tokio::test]
	async fn a_group_held_for_good_goes_at_the_cap() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		let _held = pacer.serve().expect("the first group");
		pacer.watch();
		message(&mut pacer, &mut track, 2, now);
		let short = now + DELIVER_MAX - ms(1);
		assert_eq!(pacer.release(&mut track.track, true, short).expect("release"), 0);
		assert_eq!(pacer.release(&mut track.track, true, now + DELIVER_MAX).expect("release"), 1);
		assert_eq!(pacer.run().unpaced, 1);
		assert_eq!(pacer.run().wait_max, DELIVER_MAX);
	}

	#[tokio::test]
	async fn a_group_nobody_takes_is_given_up_on_soon() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		message(&mut pacer, &mut track, 2, now);
		let short = now + TAKE_MAX - ms(1);
		assert_eq!(pacer.release(&mut track.track, true, short).expect("release"), 0);
		assert_eq!(pacer.release(&mut track.track, true, now + TAKE_MAX).expect("release"), 1);
		assert_eq!(pacer.run().unpaced, 0, "not served is not overtaken");
	}

	#[tokio::test]
	async fn nothing_is_held_without_a_subscriber() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		let mut held = Vec::new();
		for body in 1..=3 {
			pacer.open(&mut track.track, false, now).expect("open");
			pacer.write(at(body), Bytes::from(body.to_string())).expect("write");
			pacer.close(now).expect("close");
			held.push(pacer.serve());
		}
		assert_eq!(pacer.queued(), 0);
		assert_eq!(pacer.appended(), 3);
	}

	#[tokio::test]
	async fn the_queue_is_bounded() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 0, now);
		let _held = pacer.serve().expect("the first group");
		pacer.watch();
		for body in 1..=QUEUE_MAX as u64 {
			message(&mut pacer, &mut track, body, now);
		}
		assert_eq!(pacer.queued(), QUEUE_MAX);
		assert_eq!(pacer.appended(), 1);
		// One more sends the oldest on, undelivered as the one before it is.
		message(&mut pacer, &mut track, 99, now);
		assert_eq!(pacer.queued(), QUEUE_MAX);
		assert_eq!(pacer.appended(), 2);
		assert_eq!(newest(&pacer).await, ["1"]);
		assert_eq!(pacer.run().queue_max, QUEUE_MAX);
		assert_eq!(pacer.run().unpaced, 1);
	}

	#[tokio::test]
	async fn a_held_group_gathers_its_frames_and_carries_on_live() {
		// A media group cut behind one still on its way: its first frames
		// wait with it, and once it goes the rest are written straight in.
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		pacer.open(&mut track.track, true, now).expect("open");
		pacer.write(at(0), Bytes::from_static(b"a0")).expect("write");
		let held = pacer.serve().expect("the first group");
		pacer.write(at(1), Bytes::from_static(b"a1")).expect("write");
		pacer.open(&mut track.track, true, now).expect("the next group");
		pacer.write(at(2), Bytes::from_static(b"b0")).expect("write");
		pacer.write(at(3), Bytes::from_static(b"b1")).expect("write");
		assert_eq!((pacer.queued(), pacer.appended(), pacer.closed()), (1, 1, 1));
		drop(held);
		assert_eq!(pacer.release(&mut track.track, true, now + ms(20)).expect("release"), 1);
		assert_eq!((pacer.queued(), pacer.appended(), pacer.closed()), (0, 2, 1));
		pacer.write(at(4), Bytes::from_static(b"b2")).expect("write");
		pacer.close(now + ms(30)).expect("close");
		assert_eq!(pacer.closed(), 2);
		assert_eq!(newest(&pacer).await, ["b0", "b1", "b2"]);
	}

	#[tokio::test]
	async fn a_window_counts_afresh() {
		let mut track = track();
		let mut pacer = Pacer::new();
		let now = Instant::now();
		message(&mut pacer, &mut track, 1, now);
		let _held = pacer.serve().expect("the first group");
		pacer.watch();
		message(&mut pacer, &mut track, 2, now);
		assert_eq!(pacer.window().queue_max, 1);
		pacer.next_window();
		assert_eq!(pacer.window(), Stats::default());
		assert_eq!(pacer.run().queue_max, 1);
	}
}

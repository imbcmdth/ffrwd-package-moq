//! Putting one track's groups back in order, and saying what that cost.
//!
//! A MoQ subscription is not a stream of groups in sequence. Groups
//! travel over parallel QUIC streams, a relay serves its cache in its
//! own arrival order, and the send order on the wire is the newest
//! group first, so a reader that asks a broadcast already running for
//! its backlog is handed the whole of it in something close to
//! reverse. A reader joining at the live edge is handed one group and
//! then the next, which is a far shorter way to fall out of order but
//! no guarantee of being in it. What comes out of this package is
//! packets in DECODE ORDER - `ffrwd:av` says a pad's packets are in
//! decode order, and the stream-copy muxers a query writes through
//! (nut, matroska, mp4) refuse a timestamp that goes backwards - so the
//! reordering has to be absorbed here.
//!
//! [`Queue`] is that hold: groups in as they land, groups out in
//! sequence, and a count of everything it could not do. It decides
//! nothing about media on its own - [`crate::group`] is where a group
//! begins and ends, and the demuxing is the caller's - but a live join
//! has to land where a decoder can start, so the caller says of each
//! group whether it can be decoded from and the queue does the rest.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde::Serialize;

/// What this module reports, as rows on its own stderr.
///
/// A packet source has no row channel in `ffrwd:av`: `next` hands back
/// packets and nothing else. So these go out as `subscribe: row <json>`
/// lines instead, one object to a line. `kind` says which of the four
/// shapes it is: a TRACK row every [`REPORT_EVERY`] and once more when
/// the track ends (`final`), a HOLE row for every hole given up on, a
/// LATE row for every group that arrived below the cursor and could not
/// be used, and a SKIPPED row for every group a live join stepped over
/// on its way to one a decoder can start at.
pub const ROWS_SCHEMA: &str = r#"{"type":"object","properties":{"kind":{"type":"string","enum":["track","hole","late","skipped"]},"track":{"type":"string"},"received":{"type":"integer"},"delivered":{"type":"integer"},"repeated":{"type":"integer"},"bytes":{"type":"integer"},"holes_opened":{"type":"integer"},"holes_filled":{"type":"integer"},"holes_abandoned_gone":{"type":"integer"},"holes_abandoned_budget":{"type":"integer"},"holes_abandoned_restart":{"type":"integer"},"holes_abandoned_end":{"type":"integer"},"dropped_late":{"type":"integer"},"skipped_join":{"type":"integer"},"fetches":{"type":"integer"},"fetches_refused":{"type":"integer"},"hold_max_groups":{"type":"integer"},"hold_max_bytes":{"type":"integer"},"reorder_max":{"type":"integer"},"first":{"type":["integer","null"]},"last":{"type":["integer","null"]},"final":{"type":"boolean"},"from":{"type":"integer"},"to":{"type":"integer"},"reason":{"type":"string","enum":["gone","budget","restart","end"]},"held_groups":{"type":"integer"},"held_bytes":{"type":"integer"},"group":{"type":"integer"},"cursor":{"type":"integer"}},"additionalProperties":false}"#;

/// How long a hole in a track's group sequence is held open for the
/// group that would fill it, and how much one track holds meanwhile.
///
/// Groups travel over parallel streams and a relay sends the newest one
/// first, so arrival order is nothing like sequence order: a reader
/// ASKING for the backlog takes the whole of it at once, hundreds of
/// groups deep, and the group it is waiting for may be the last one on
/// the wire. That is the worst of it and what these numbers are sized
/// for; a reader at the live edge, which is the default, has a group or
/// two to absorb. The hold therefore waits as long as the
/// relay could still serve that group - the subscription's own latency
/// window, [`crate::subscribe::BACKLOG`] - and holds as much as the
/// window can be worth in bytes rather than a count of groups. 64 MiB
/// is 30 seconds of about 17 Mbit/s, which is past any rung this
/// package publishes; audio is three orders of magnitude under it.
///
/// What the window is NOT is a deadline for giving up. Replaying a long
/// backlog takes far longer than any one group is worth waiting for,
/// and the group that fills the hole is often the last one on the wire:
/// a relay serves its cache in its own arrival order, and that order is
/// what the publisher's newest-first send made of it, so dozens of
/// groups can land tens of seconds behind the ones around them with the
/// track silent in between. This package measured exactly that against
/// a local relay; see `tests/live_backlog.py`.
///
/// So when the window runs out the relay is ASKED for the group by
/// sequence ([`fetch_group`]) instead of being guessed about. It has
/// the group and serves it, and the hole closes; or it refuses, and the
/// refusal is what lets the cursor step over the hole. A hole is given
/// up on only on that refusal, on the byte budget, on a resubscribe
/// (which starts at the live edge, so what is missing is gone), or at
/// the end of the track.
///
/// Every one of those is a LAST RESORT, not a routine: a hole given up
/// on is counted and reported, never dropped in silence. See
/// [`Counters`].
pub const HOLD_WAIT: Duration = crate::subscribe::BACKLOG;
pub const HOLD_BYTES: u64 = 64 << 20;

/// How long a reader that has just joined a BACKLOG waits for a lower
/// group sequence before it fixes its cursor.
///
/// A subscription asking for the backlog is served in the relay's own
/// arrival order with the newest group sent first, so the first group
/// to arrive is rarely the lowest one. A cursor fixed there would put
/// the whole backlog BELOW itself, and every group of it would then be
/// a late arrival. So the first groups are held instead, and the cursor
/// is fixed at the lowest sequence that has stopped falling: each new
/// low restarts the wait, and a relay still handing over older groups
/// keeps it open.
///
/// It is a backlog idea and nothing else. A reader joining at the live
/// edge has no lower sequence to wait for - the publisher serves from
/// its newest group and there is nothing under it - so settling would
/// be two seconds of delay bought for nothing; see
/// [`crate::subscribe::Start::Live`].
pub const JOIN_SETTLE: Duration = Duration::from_secs(2);

/// How often a track says what it has done, and how many incident rows
/// one track spells out before it only counts them.
pub const REPORT_EVERY: Duration = Duration::from_secs(5);
const ROW_CAP: u64 = 256;

/// What one track's hold is bounded by.
#[derive(Clone, Copy)]
pub struct Hold {
	/// How long a hole stays open for the group that would fill it.
	wait: Duration,
	/// How much one track holds meanwhile.
	bytes: u64,
	/// How long a joining reader waits for a lower sequence.
	settle: Duration,
}

impl Hold {
	/// How long a hole stands open before the relay is asked for the
	/// group outright, which is also how long that answer is waited for.
	pub fn wait(&self) -> Duration {
		self.wait
	}

	/// The hold a run asked for: the window in milliseconds, the budget
	/// in MiB, and the join's settling time in milliseconds.
	pub fn new(hold_ms: u64, hold_mib: u64, join_ms: u64) -> Self {
		Self {
			wait: Duration::from_millis(hold_ms),
			bytes: hold_mib.max(1) << 20,
			settle: Duration::from_millis(join_ms),
		}
	}
}

impl Default for Hold {
	fn default() -> Self {
		Self {
			wait: HOLD_WAIT,
			bytes: HOLD_BYTES,
			settle: JOIN_SETTLE,
		}
	}
}

/// What one track did with what the relay handed it.
///
/// Every group that arrives is accounted for: delivered in sequence,
/// held and then delivered, or - when the hold ran out of rope - named
/// in a row of its own before it is given up on. `received` is always
/// `delivered` plus `dropped_late` plus `skipped_join` plus `repeated`
/// plus what the hold still has.
#[derive(Default, Serialize)]
pub struct Counters {
	/// Groups the relay handed over, and groups handed on in sequence.
	received: u64,
	delivered: u64,
	/// Groups the wire repeated, which a resubscribe can do.
	repeated: u64,
	/// Payload bytes received, frames alone.
	bytes: u64,
	/// Holes in the sequence: opened, filled by the group that was
	/// missing, and given up on - because the relay refused to serve
	/// the group when asked for it outright, because the budget ran
	/// out, because the track was taken up again on a fresh
	/// subscription, or because the track ended with the hole open.
	holes_opened: u64,
	holes_filled: u64,
	holes_abandoned_gone: u64,
	holes_abandoned_budget: u64,
	holes_abandoned_restart: u64,
	holes_abandoned_end: u64,
	/// Groups that arrived below the cursor, which no consumer taking
	/// packets in decode order can be handed.
	dropped_late: u64,
	/// Groups held at a live join that began before the group the reader
	/// started at: a video group whose first frame is not a keyframe is
	/// a group no decoder can begin in, so the cursor steps over it. It
	/// is not a loss - nothing had started yet - but it is not silent
	/// either. Always 0 on a backlog join, which starts at the lowest
	/// sequence it is given.
	skipped_join: u64,
	/// Groups asked for outright when a hole had stood open too long,
	/// and the ones the relay would not serve.
	fetches: u64,
	fetches_refused: u64,
	/// The deepest the hold ever got, in groups and in bytes.
	hold_max_groups: u64,
	hold_max_bytes: u64,
	/// The widest the hold ever had to stretch in sequence, which is the
	/// reordering the wire asked it to absorb.
	reorder_max: u64,
	/// The first and last sequence delivered; null until one is.
	first: Option<u64>,
	last: Option<u64>,
}

/// One track's group sequence: what has arrived, what is held for the
/// group before it, and what goes out next.
///
/// This is the part of a reader the backlog is about, and it is kept
/// apart from the demuxing so it can be driven on its own; see the
/// tests at the foot of this file.
pub struct Queue {
	/// The track's name in the broadcast's catalog, which is what its
	/// rows are read back against.
	pub name: String,
	/// Where this reader joined, which is what the cursor is fixed by:
	/// the lowest sequence the relay hands over, or the first group a
	/// decoder can start at.
	start: crate::subscribe::Start,
	/// Completed groups waiting for the one before them, and what they
	/// weigh; see [`HOLD_BYTES`].
	pending: BTreeMap<u64, Vec<Bytes>>,
	pending_bytes: u64,
	/// The next group in sequence; None until the cursor is fixed.
	cursor: Option<u64>,
	/// The lowest sequence held while the cursor is still unfixed, and
	/// when it last fell; see [`JOIN_SETTLE`]. Backlog only.
	join_low: Option<(u64, Instant)>,
	/// The lowest sequence held that a decoder can start at, while the
	/// cursor is still unfixed. Live only: this is the cursor, as soon
	/// as there is one, and nothing under it is waited for.
	join_ready: Option<u64>,
	/// When the hole at the head of `pending` opened; see [`HOLD_WAIT`].
	gap_since: Option<Instant>,
	/// The group the relay has been asked for outright, and the answer
	/// when it says no. A hole that has stood open for the whole window
	/// is not guessed about: the relay is asked for the group by
	/// sequence, and only a refusal - it has that group or it does not
	/// - gives the hold leave to step over it.
	pub fetching: Option<u64>,
	gone: Option<u64>,
	/// Groups a fetch already filled a hole with. The subscription
	/// usually hands the same group over later, which is a repeat
	/// rather than a group that came too late to use.
	fetched: BTreeSet<u64>,
	/// Whether the track was taken up again on a fresh subscription,
	/// which is a hole nothing will fill: the new subscription starts
	/// at the live edge and the groups between are gone.
	pub restarted: bool,
	/// What this track has done, and when it last said so.
	pub counters: Counters,
	pub reported: Option<Instant>,
	rows: u64,
}

/// One track's totals, as a row. `final` marks the one that ends it.
#[derive(Serialize)]
struct TrackRow<'a> {
	kind: &'static str,
	track: &'a str,
	#[serde(flatten)]
	counters: &'a Counters,
	#[serde(rename = "final")]
	last: bool,
}

/// A hole given up on, named before the cursor steps over it. `from`
/// and `to` are both inclusive, and `reason` says what ran out.
#[derive(Serialize)]
struct HoleRow<'a> {
	kind: &'static str,
	track: &'a str,
	from: u64,
	to: u64,
	reason: &'static str,
	held_groups: u64,
	held_bytes: u64,
}

/// A group that arrived after its place in the sequence had passed, or
/// one a live join stepped over on its way to a group a decoder can
/// start at. `cursor` is where the reader was, or is about to be.
#[derive(Serialize)]
struct LateRow<'a> {
	kind: &'static str,
	track: &'a str,
	group: u64,
	cursor: u64,
}

/// One row on stderr, which is where a packet source's rows go; see
/// [`ROWS_SCHEMA`].
fn row(body: &impl Serialize) {
	eprintln!(
		"subscribe: row {}",
		serde_json::to_string(body).expect("a row serializes")
	);
}

impl Queue {
	pub fn new(name: String, start: crate::subscribe::Start) -> Self {
		Self {
			name,
			start,
			pending: BTreeMap::new(),
			pending_bytes: 0,
			cursor: None,
			join_low: None,
			join_ready: None,
			gap_since: None,
			fetching: None,
			gone: None,
			fetched: BTreeSet::new(),
			restarted: false,
			counters: Counters::default(),
			reported: None,
			rows: 0,
		}
	}

	/// Files one group the relay handed over.
	///
	/// A group below the cursor cannot be filed: its place in the
	/// sequence has passed, and what this module hands the host is
	/// packets in decode order - `ffrwd:av` says a pad's packets are in
	/// decode order, and the stream-copy muxers downstream (nut,
	/// matroska, mp4) refuse a timestamp that goes backwards. So it is
	/// counted and named in a row of its own rather than dropped in
	/// silence. The hold is what keeps that from happening; see
	/// [`HOLD_WAIT`] and [`JOIN_SETTLE`].
	///
	/// `decodable` says whether this group can be decoded from its own
	/// start - a video group whose first frame is a keyframe, an audio
	/// group at all - which is the only thing about media this queue is
	/// told, and it is told it because a live join has to land on such a
	/// group. It is read at the join and ignored afterwards.
	pub fn push(&mut self, sequence: u64, frames: Vec<Bytes>, decodable: bool) {
		let bytes: u64 = frames.iter().map(|frame| frame.len() as u64).sum();
		self.counters.received += 1;
		self.counters.bytes += bytes;
		if let Some(next) = self.cursor {
			if sequence < next {
				// A group a fetch already filled the hole with: the
				// subscription hands it over too, in its own time.
				if self.fetched.remove(&sequence) {
					self.counters.repeated += 1;
					return;
				}
				self.counters.dropped_late += 1;
				self.report_late(sequence, next);
				return;
			}
		}
		if self.pending.contains_key(&sequence) {
			// The same group twice, which a resubscribe can do. The one
			// already held is the one that goes out.
			self.counters.repeated += 1;
			return;
		}
		if self.fetching == Some(sequence) {
			// The answer to a fetch: remember it, so the subscription's
			// own copy is read as the repeat it is. One per hole, and
			// the oldest goes when there have been many.
			self.fetching = None;
			self.fetched.insert(sequence);
			while self.fetched.len() > ROW_CAP as usize {
				self.fetched.pop_first();
			}
		}
		self.pending.insert(sequence, frames);
		self.pending_bytes += bytes;
		if self.cursor.is_none() {
			match self.start {
				// A lower sequence than any yet: the relay is still handing
				// over the older end of the backlog, so the join waits on.
				crate::subscribe::Start::Backlog => {
					if self.join_low.is_none_or(|(low, _)| sequence < low) {
						self.join_low = Some((sequence, Instant::now()));
					}
				}
				// The lowest group a decoder can start at. There is no
				// waiting: the publisher serves a live subscription from its
				// newest group, so nothing older is coming and the first
				// group that can be started at is where this reader starts.
				crate::subscribe::Start::Live => {
					if decodable && self.join_ready.is_none_or(|low| sequence < low) {
						self.join_ready = Some(sequence);
					}
				}
			}
		}
		self.counters.hold_max_groups = self
			.counters
			.hold_max_groups
			.max(self.pending.len() as u64);
		self.counters.hold_max_bytes = self.counters.hold_max_bytes.max(self.pending_bytes);
		// How far apart in sequence the hold ever had to stretch, which
		// is the reordering the wire asked it to absorb.
		if let (Some((&low, _)), Some((&high, _))) =
			(self.pending.iter().next(), self.pending.iter().next_back())
		{
			self.counters.reorder_max = self.counters.reorder_max.max(high - low);
		}
	}

	/// The next group to hand on, or None while the hold is waiting.
	///
	/// `last` takes what is left whatever the sequence says: nothing
	/// more is coming. A sequence pushed onto `ask` is one the relay is
	/// to be asked for outright, the hole having stood open for the
	/// whole window.
	pub fn take(&mut self, hold: Hold, last: bool, ask: &mut Vec<u64>) -> Option<(u64, Vec<Bytes>)> {
		if self.cursor.is_none() && self.start == crate::subscribe::Start::Live {
			// A live join starts at the first group a decoder can start
			// at, and lets go of whatever is held below it: those groups
			// begin mid-picture, and what leaves here is packets a decoder
			// can be handed. Nothing had started, so nothing is lost - but
			// it is counted all the same.
			match self.join_ready {
				Some(low) => self.step_over_to(low),
				// Nothing to start at yet. At the end of the track there
				// will not be, so what is held goes out as it stands.
				None if !last => return None,
				None => {}
			}
		}
		let Some((&oldest, _)) = self.pending.iter().next() else {
			self.gap_since = None;
			return None;
		};
		let ready = match self.cursor {
			// The join. On a live join `oldest` is already the group this
			// reader starts at, so there is nothing to wait for. On a
			// backlog join the cursor is fixed at the lowest sequence the
			// relay hands over, not at the first group that happens to
			// arrive: see [`JOIN_SETTLE`]. Group 0 is the lowest there
			// can be, and a budget already spent has to start somewhere.
			None if self.start == crate::subscribe::Start::Live => true,
			None => match self.join_low {
				Some((low, since)) => {
					last || low == 0
						|| since.elapsed() >= hold.settle
						|| self.pending_bytes >= hold.bytes
				}
				None => true,
			},
			Some(next) if oldest == next => {
				if self.gap_since.take().is_some() {
					self.counters.holes_filled += 1;
					self.restarted = false;
					self.fetching = None;
					self.gone = None;
				}
				true
			}
			// A hole at the cursor. It is the oldest one there is, since
			// everything before it has gone out, and so the one least
			// likely to be filled: when the hold runs out of rope this
			// is what it gives up.
			Some(next) => {
				let since = match self.gap_since {
					Some(since) => since,
					None => {
						self.counters.holes_opened += 1;
						let now = Instant::now();
						self.gap_since = Some(now);
						now
					}
				};
				let budget = self.pending_bytes >= hold.bytes;
				// The relay was asked for this group outright and would
				// not serve it, which is the only answer that settles it.
				let gone = self.gone == Some(next);
				let reason = match (last, self.restarted, budget, gone) {
					(true, _, _, _) => "end",
					(_, true, _, _) => "restart",
					(_, _, true, _) => "budget",
					(_, _, _, true) => "gone",
					_ => "",
				};
				if reason.is_empty() {
					// Not yet. Once the hole has stood open for the whole
					// window, ask: a relay that still has the group serves
					// it, and one that does not says so.
					if self.fetching.is_none() && since.elapsed() >= hold.wait {
						self.fetching = Some(next);
						self.counters.fetches += 1;
						ask.push(next);
					}
					false
				} else {
					match reason {
						"budget" => self.counters.holes_abandoned_budget += 1,
						"restart" => self.counters.holes_abandoned_restart += 1,
						"end" => self.counters.holes_abandoned_end += 1,
						_ => self.counters.holes_abandoned_gone += 1,
					}
					self.report_hole(next, oldest - 1, reason);
					self.gap_since = None;
					self.restarted = false;
					self.fetching = None;
					self.gone = None;
					true
				}
			}
		};
		if !ready {
			return None;
		}
		let frames = self.pending.remove(&oldest).expect("just found");
		self.pending_bytes -= frames.iter().map(|frame| frame.len() as u64).sum::<u64>();
		self.cursor = Some(oldest + 1);
		self.join_low = None;
		self.join_ready = None;
		self.counters.delivered += 1;
		self.counters.first.get_or_insert(oldest);
		self.counters.last = Some(oldest);
		Some((oldest, frames))
	}

	/// Lets go of every held group below `low`, which is where a live
	/// join fixes its cursor. Each one is counted and named.
	fn step_over_to(&mut self, low: u64) {
		while let Some((&oldest, _)) = self.pending.iter().next() {
			if oldest >= low {
				break;
			}
			let frames = self.pending.remove(&oldest).expect("just found");
			self.pending_bytes -= frames.iter().map(|frame| frame.len() as u64).sum::<u64>();
			self.counters.skipped_join += 1;
			self.report_skipped(oldest, low);
		}
	}

	/// The relay would not serve the group a hole was waiting for.
	pub fn refused(&mut self, sequence: u64, why: &str) {
		self.counters.fetches_refused += 1;
		self.fetching = None;
		self.gone = Some(sequence);
		eprintln!(
			"subscribe: track '{}': the relay would not serve group {sequence}: {why}",
			self.name
		);
	}

	/// This track's totals so far.
	pub fn report(&mut self, last: bool) {
		self.reported = Some(Instant::now());
		row(&TrackRow {
			kind: "track",
			track: &self.name,
			counters: &self.counters,
			last,
		});
	}

	/// Whether this track's incident rows are still being spelled out;
	/// past [`ROW_CAP`] they are only counted.
	fn spell(&mut self) -> bool {
		self.rows += 1;
		self.rows <= ROW_CAP
	}

	fn report_hole(&mut self, from: u64, to: u64, reason: &'static str) {
		if !self.spell() {
			return;
		}
		row(&HoleRow {
			kind: "hole",
			track: &self.name,
			from,
			to,
			reason,
			held_groups: self.pending.len() as u64,
			held_bytes: self.pending_bytes,
		});
	}

	fn report_late(&mut self, group: u64, cursor: u64) {
		self.report_passed("late", group, cursor);
	}

	fn report_skipped(&mut self, group: u64, cursor: u64) {
		self.report_passed("skipped", group, cursor);
	}

	fn report_passed(&mut self, kind: &'static str, group: u64, cursor: u64) {
		if !self.spell() {
			return;
		}
		row(&LateRow {
			kind,
			track: &self.name,
			group,
			cursor,
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::subscribe::Start;

	/// A hold that waits: nothing is given up, and a join that has just
	/// seen a lower sequence keeps waiting for a lower one still.
	fn patient() -> Hold {
		Hold {
			wait: Duration::from_secs(30),
			bytes: 64 << 20,
			settle: Duration::from_secs(30),
		}
	}

	/// The same hold with its clocks already run out.
	fn now() -> Hold {
		Hold {
			settle: Duration::ZERO,
			wait: Duration::ZERO,
			..patient()
		}
	}

	fn group(sequence: u64) -> Vec<Bytes> {
		vec![Bytes::from(vec![sequence as u8; 16])]
	}

	/// Every group the queue will hand on, in the order it hands them.
	fn drain(queue: &mut Queue, hold: Hold, last: bool) -> Vec<u64> {
		let mut ask = Vec::new();
		let mut out = Vec::new();
		while let Some((sequence, _)) = queue.take(hold, last, &mut ask) {
			out.push(sequence);
		}
		out
	}

	#[test]
	fn a_reversed_backlog_goes_out_in_order() {
		// What a relay serving a backlog looks like: the newest group
		// first, the one the cursor must start at last.
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		for sequence in (1..200).rev() {
			queue.push(sequence, group(sequence), true);
			// Nothing goes out while the join is still seeing lower
			// sequences arrive, however long it is willing to wait.
			assert!(drain(&mut queue, patient(), false).is_empty());
		}
		// Group 0 is the lowest sequence there can be, so the join has
		// nothing left to settle once it lands.
		queue.push(0, group(0), true);
		let out = drain(&mut queue, patient(), false);
		assert_eq!(out, (0..200).collect::<Vec<_>>(), "the backlog came back reordered");
		assert_eq!(queue.counters.delivered, 200);
		assert_eq!(queue.counters.dropped_late, 0);
		assert_eq!(queue.counters.first, Some(0));
		assert_eq!(queue.counters.reorder_max, 199);
		assert_eq!(queue.counters.hold_max_groups, 200);
	}

	#[test]
	fn the_cursor_starts_at_the_lowest_sequence_and_not_the_first_to_arrive() {
		// 0.6.3 fixed the cursor at whatever arrived first, which put the
		// whole backlog below it.
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(40, group(40), true);
		assert!(drain(&mut queue, patient(), false).is_empty(), "the join settled at once");
		queue.push(38, group(38), true);
		queue.push(39, group(39), true);
		assert_eq!(drain(&mut queue, now(), false), vec![38, 39, 40]);
		assert_eq!(queue.counters.dropped_late, 0);
	}

	#[test]
	fn a_group_below_the_cursor_is_counted_rather_than_dropped_in_silence() {
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(10, group(10), true);
		assert_eq!(drain(&mut queue, now(), false), vec![10]);
		// Its place in the sequence has passed: no stream-copy consumer
		// can be handed it, so it is counted and named.
		queue.push(9, group(9), true);
		assert_eq!(queue.counters.dropped_late, 1);
		assert_eq!(queue.counters.delivered, 1);
		assert_eq!(queue.counters.received, 2);
	}

	#[test]
	fn a_hole_asks_the_relay_before_the_cursor_steps_over_it() {
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(0, group(0), true);
		assert_eq!(drain(&mut queue, now(), false), vec![0]);
		queue.push(2, group(2), true);

		// The window has run out, so the relay is asked for group 1
		// rather than guessed about, and nothing goes out meanwhile.
		let mut ask = Vec::new();
		assert!(queue.take(now(), false, &mut ask).is_none());
		assert_eq!(ask, vec![1]);
		assert_eq!(queue.counters.fetches, 1);
		assert_eq!(queue.counters.holes_opened, 1);
		// Asked once, not once a call.
		ask.clear();
		assert!(queue.take(now(), false, &mut ask).is_none());
		assert!(ask.is_empty());

		// The relay has it: the hole closes and nothing was given up.
		queue.push(1, group(1), true);
		assert_eq!(drain(&mut queue, now(), false), vec![1, 2]);
		assert_eq!(queue.counters.holes_filled, 1);
		assert_eq!(queue.counters.holes_abandoned_gone, 0);
		assert_eq!(queue.counters.dropped_late, 0);

		// And the subscription's own copy, arriving later, is a repeat
		// rather than a group that came too late.
		queue.push(1, group(1), true);
		assert_eq!(queue.counters.repeated, 1);
		assert_eq!(queue.counters.dropped_late, 0);
	}

	#[test]
	fn a_hole_the_relay_refuses_is_the_only_hole_given_up_on_time() {
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(0, group(0), true);
		assert_eq!(drain(&mut queue, now(), false), vec![0]);
		queue.push(3, group(3), true);

		let mut ask = Vec::new();
		assert!(queue.take(now(), false, &mut ask).is_none());
		assert_eq!(ask, vec![1]);
		// A refusal is the answer that settles it: the cursor steps over
		// the whole hole, 1 and 2 alike, and says so.
		queue.refused(1, "not found");
		assert_eq!(drain(&mut queue, now(), false), vec![3]);
		assert_eq!(queue.counters.holes_abandoned_gone, 1);
		assert_eq!(queue.counters.fetches_refused, 1);
		assert_eq!(queue.counters.delivered, 2);
	}

	#[test]
	fn the_budget_is_what_bounds_the_hold() {
		let small = Hold {
			bytes: 64,
			..patient()
		};
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(0, group(0), true);
		assert_eq!(drain(&mut queue, now(), false), vec![0]);
		// A hole at 1, and groups piling up behind it: four groups of
		// sixteen bytes is the whole budget.
		for sequence in 2..6 {
			queue.push(sequence, group(sequence), true);
		}
		assert_eq!(drain(&mut queue, small, false), vec![2, 3, 4, 5]);
		assert_eq!(queue.counters.holes_abandoned_budget, 1);
		assert_eq!(queue.counters.holes_abandoned_gone, 0);
	}

	#[test]
	fn the_end_of_a_track_takes_what_is_left_in_sequence() {
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(5, group(5), true);
		queue.push(7, group(7), true);
		// Nothing more is coming: what is held goes out in order, and
		// the hole it steps over is named.
		assert_eq!(drain(&mut queue, patient(), true), vec![5, 7]);
		assert_eq!(queue.counters.holes_abandoned_end, 1);
		assert_eq!(queue.counters.delivered, 2);
	}

	#[test]
	fn a_resubscribe_does_not_leave_the_hold_waiting_out_its_window() {
		let mut queue = Queue::new("a0".into(), Start::Backlog);
		queue.push(0, group(0), true);
		assert_eq!(drain(&mut queue, now(), false), vec![0]);
		// The wire broke and the track was taken up at the live edge:
		// the groups in between are gone, and the hold is told so.
		queue.restarted = true;
		queue.push(90, group(90), true);
		assert_eq!(drain(&mut queue, patient(), false), vec![90]);
		assert_eq!(queue.counters.holes_abandoned_restart, 1);
	}

	#[test]
	fn a_live_join_starts_at_the_first_group_and_waits_for_nothing() {
		// The publisher serves a live subscription from its newest group,
		// so there is nothing older to settle for: the most patient hold
		// there is still hands the first group straight on.
		let mut queue = Queue::new("a0".into(), Start::Live);
		queue.push(412, group(412), true);
		assert_eq!(drain(&mut queue, patient(), false), vec![412]);
		assert_eq!(queue.counters.first, Some(412));
		queue.push(413, group(413), true);
		assert_eq!(drain(&mut queue, patient(), false), vec![413]);
		assert_eq!(queue.counters.dropped_late, 0);
		assert_eq!(queue.counters.skipped_join, 0);
	}

	#[test]
	fn a_live_join_lands_on_a_group_a_decoder_can_start_at() {
		// A video group whose first frame is not a keyframe is a group no
		// decoder can begin in. The join steps over it and says so, and
		// what it starts at is the keyframe group beside it.
		let mut queue = Queue::new("v0".into(), Start::Live);
		queue.push(70, group(70), false);
		assert!(
			drain(&mut queue, patient(), false).is_empty(),
			"a live join started where a decoder cannot"
		);
		queue.push(71, group(71), true);
		assert_eq!(drain(&mut queue, patient(), false), vec![71]);
		assert_eq!(queue.counters.skipped_join, 1);
		assert_eq!(queue.counters.dropped_late, 0);
		assert_eq!(queue.counters.delivered, 1);
	}

	#[test]
	fn a_live_join_keeps_the_hold_it_always_had() {
		// Past the join a live reader is the same reader: a hole waits,
		// the relay is asked, and nothing is handed on out of sequence.
		let mut queue = Queue::new("a0".into(), Start::Live);
		queue.push(10, group(10), true);
		assert_eq!(drain(&mut queue, now(), false), vec![10]);
		queue.push(12, group(12), true);
		let mut ask = Vec::new();
		assert!(queue.take(now(), false, &mut ask).is_none());
		assert_eq!(ask, vec![11]);
		queue.push(11, group(11), true);
		assert_eq!(drain(&mut queue, now(), false), vec![11, 12]);
		assert_eq!(queue.counters.holes_filled, 1);
		assert_eq!(queue.counters.dropped_late, 0);
	}

	#[test]
	fn a_live_join_with_nothing_to_start_at_still_ends() {
		// The track finished before a group a decoder can start at ever
		// arrived. What is held goes out rather than the reader hanging
		// on a join that will never come.
		let mut queue = Queue::new("v0".into(), Start::Live);
		queue.push(3, group(3), false);
		assert!(drain(&mut queue, patient(), false).is_empty());
		assert_eq!(drain(&mut queue, patient(), true), vec![3]);
	}
}

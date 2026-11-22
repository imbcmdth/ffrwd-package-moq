//! Following a broadcast across a publisher that started again.
//!
//! A program that publishes a broadcast is sometimes replaced: its
//! process is lost and a new one is started, or its run ends and the
//! next run of the same query begins. The new publisher publishes the
//! same broadcast name with the same settings, and its timestamps and
//! group numbers start over. A reader that carries on across it hands
//! packets to muxers that refuse a timestamp going back, so the new
//! media has to be moved to follow the old.
//!
//! [`Timeline`] is that move, for every track a reader hands on. When
//! the tracks are taken up on a publisher that started again, it holds
//! what arrives for a moment, and until each media track has a packet,
//! and then gives every track one shift, in microseconds, so audio and
//! video keep the alignment the publisher gave them. The shift maps the
//! new publisher's clock onto the wall clock the way the old one's was, a
//! track's pace after the gap set against the same track's pace before
//! it: the held packet that waited least to arrive ends as far after
//! where its track's pace had the output, in media time, as it arrived
//! after the last packet before the gap. So the gap is as long as the
//! stream was away.
//!
//! When it is not known whether the publisher started again, the first
//! media packet says: one behind where its track had got belongs to a
//! publisher that did, and anything else carries on at once, as a
//! publisher whose own session was replaced does. Whatever the decision,
//! a media track never goes back: a packet that would is dropped and
//! counted, and the track starts again at a sync sample past its last
//! packet.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// How long the packets of a publisher that started again are held at
/// least, and at most.
///
/// A live join hands each track the group in progress from its first
/// frame, the oldest first, and the frames the publisher makes after
/// that one at a time as it makes them. Held for [`SETTLE_LEAST`], what
/// the tracks brought in reaches past that burst to frames that left the
/// publisher a moment ago, which is what the new publisher's clock is
/// read off. Past that the packets wait for the first packet of every
/// other media track, for up to [`SETTLE`] after the first media packet
/// came. Messages wait for the media while the reader has media tracks,
/// for up to [`SETTLE`] after the first of them; with no media by then
/// the shift is read off the messages. A media track whose first packet
/// comes after the shift is fixed gets the same shift.
pub const SETTLE_LEAST: Duration = Duration::from_millis(300);
pub const SETTLE: Duration = Duration::from_secs(2);

/// How far back the pace of the packets before a gap is read, and how
/// many packets that keeps at most.
const RECENT: Duration = Duration::from_secs(2);
const RECENT_MAX: usize = 1024;

/// What showed that the publisher started again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seen {
	/// The publisher finished its tracks, and a broadcast of the same
	/// name came after: a publisher that finished does not come back.
	Finished,
	/// The relay announced the broadcast again, from another publisher.
	Replaced,
	/// A track came back with its groups numbered below where this
	/// reader had got.
	Renumbered,
	/// A track's first media packet came back with a decode time at or
	/// below the last one handed on.
	Rewound,
}

impl Seen {
	/// The name a row spells it with.
	pub fn as_str(self) -> &'static str {
		match self {
			Seen::Finished => "finished",
			Seen::Replaced => "replaced",
			Seen::Renumbered => "renumbered",
			Seen::Rewound => "rewound",
		}
	}
}

/// A packet's times, in its track's ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
	pub pts: i64,
	pub dts: i64,
	/// 0 where the packet did not say.
	pub duration: i64,
}

impl Stamp {
	fn shifted(self, ticks: i64) -> Self {
		Self {
			pts: self.pts + ticks,
			dts: self.dts + ticks,
			duration: self.duration,
		}
	}
}

/// A restart taken up: what showed it, how long the stream was away and
/// the shift every track's packets now get.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Restart {
	pub seen: Seen,
	/// How long the stream was away, as the output shows it: from the end
	/// of the last packet before the gap to the newest of the first
	/// packets after it.
	pub away: Duration,
	/// Added to every timestamp of the new publisher, in microseconds.
	pub shift_us: i64,
}

/// One track's clock as its packets leave.
struct Clock {
	timescale: u32,
	/// Whether it is a media track. A media track's first packet is
	/// waited for and decides; a data track may carry no message for
	/// minutes, and two messages may share a pts.
	media: bool,
	/// The last packet handed on, in output ticks.
	last: Option<Stamp>,
	/// While set, a packet goes out only past this decode time and at a
	/// sync sample: the track starts again where a decoder can.
	floor: Option<i64>,
	/// Packets dropped because they would have gone back.
	behind: u64,
	/// Whether a frame of the tracks' new subscription has reached this
	/// track since they were last taken up. Until one has, what it lets go
	/// of is what it held of the publisher before.
	taken_up: bool,
}

/// One packet held: its times, whether a decoder can start at it, what
/// rides with it, and when it arrived.
type Kept<T> = (Stamp, bool, T, Instant);

/// The tracks were taken up again and their packets are held.
struct Held<T> {
	/// What shows the publisher started again; None while it is not
	/// known whether its clock carried on.
	seen: Option<Seen>,
	packets: Vec<Vec<Kept<T>>>,
}

/// Every track's timestamps on the way out; see the module's own words.
///
/// `T` is whatever rides with a packet's times, which this keeps and
/// hands back untouched. What may go out is taken with
/// [`Self::take_ready`], and each restart decided with
/// [`Self::take_restarts`].
pub struct Timeline<T> {
	clocks: Vec<Clock>,
	/// Added to every timestamp, in microseconds: 0 until a publisher
	/// starts again.
	shift_us: i64,
	/// When the last packet went out.
	last_at: Option<Instant>,
	/// Each packet of the last [`RECENT`]: its track, when it went out,
	/// and where its end put the output, in microseconds.
	recent: VecDeque<(usize, Instant, i64)>,
	held: Option<Held<T>>,
	ready: Vec<(usize, Stamp, T)>,
	restarts: Vec<Restart>,
}

fn ticks(us: i64, timescale: u32) -> i64 {
	ffrwd_bmff::time::rescale(us, 1_000_000, u64::from(timescale))
}

fn micros(ticks: i64, timescale: u32) -> i64 {
	ffrwd_bmff::time::rescale(ticks, u64::from(timescale), 1_000_000)
}

impl<T> Timeline<T> {
	/// One clock per track, in the reader's order: its timescale, and
	/// whether it is a media track.
	pub fn new(tracks: impl IntoIterator<Item = (u32, bool)>) -> Self {
		Self {
			clocks: tracks
				.into_iter()
				.map(|(timescale, media)| Clock {
					timescale: timescale.max(1),
					media,
					last: None,
					floor: None,
					behind: 0,
					taken_up: true,
				})
				.collect(),
			shift_us: 0,
			last_at: None,
			recent: VecDeque::new(),
			held: None,
			ready: Vec::new(),
			restarts: Vec::new(),
		}
	}

	/// The shift every track's packets get now, in microseconds.
	pub fn shift_us(&self) -> i64 {
		self.shift_us
	}

	/// How many packets of `track` were dropped because they would have
	/// gone out at or before its last one.
	pub fn behind(&self, track: usize) -> u64 {
		self.clocks[track].behind
	}

	/// What may go out, in the order it may, its times shifted.
	pub fn take_ready(&mut self) -> Vec<(usize, Stamp, T)> {
		std::mem::take(&mut self.ready)
	}

	/// Every restart decided since the last call, oldest first.
	pub fn take_restarts(&mut self) -> Vec<Restart> {
		std::mem::take(&mut self.restarts)
	}

	/// Whether a track's first packet decides: a media track's, or any
	/// track's where the reader has no media.
	fn decides(&self, track: usize) -> bool {
		self.clocks[track].media || self.clocks.iter().all(|clock| !clock.media)
	}

	/// The tracks were taken up again, on a new session or on a broadcast
	/// the relay announced again. `seen` is what shows the publisher
	/// started again; None says it is not known, and the first media
	/// packet decides. A publisher known to have carried on needs no
	/// call at all.
	///
	/// Whatever an earlier resume still holds belongs to a publisher that
	/// has gone since, and goes out first, decided on what it has. An
	/// earlier resume that holds nothing yet is the same gap: what it saw
	/// stands, since that is what became of the publisher the output last
	/// came from.
	pub fn resume(&mut self, seen: Option<Seen>) {
		for clock in &mut self.clocks {
			clock.taken_up = false;
		}
		if let Some(held) = self.held.as_mut() {
			if held.packets.iter().all(Vec::is_empty) {
				held.seen = held.seen.or(seen);
				return;
			}
		}
		self.conclude();
		self.held = Some(Held {
			seen,
			packets: (0..self.clocks.len()).map(|_| Vec::new()).collect(),
		});
	}

	/// A track taken up again came back numbering its groups below where
	/// the reader had got. Heard while the tracks are being taken up; once
	/// the decision is made it stands, and a packet that would go back on
	/// its track is dropped instead.
	pub fn renumbered(&mut self) {
		if let Some(held) = self.held.as_mut() {
			held.seen.get_or_insert(Seen::Renumbered);
		}
	}

	/// A frame of the tracks' new subscription reached `track`: what it
	/// lets go of from here on is the publisher's it was taken up on.
	pub fn new_frame(&mut self, track: usize) {
		self.clocks[track].taken_up = true;
	}

	/// One packet of `track` as demuxed, in the publisher's own ticks,
	/// which arrived at `now`; `sync` is whether a decoder can start at
	/// it. While the tracks are being taken up it is held, unless no frame
	/// of the new subscription has reached its track yet: then it is what
	/// the track held of the publisher before, a group kept behind a hole,
	/// and goes out as one let out late, deciding nothing.
	pub fn push(&mut self, track: usize, stamp: Stamp, sync: bool, payload: T, now: Instant) {
		if self.held.is_none() {
			self.emit(track, stamp, sync, payload, Some(now));
			return;
		}
		if !self.clocks[track].taken_up {
			self.emit(track, stamp, sync, payload, None);
			return;
		}
		let decides = self.decides(track);
		let back = self.goes_back(track, stamp);
		let held = self.held.as_mut().expect("just looked");
		held.packets[track].push((stamp, sync, payload, now));
		if held.seen.is_none() && decides {
			// Not known whether the clock carried on: the first packet that
			// decides says. Behind where its track had got, the publisher
			// started again; otherwise it carried on, and what is held goes
			// out at once.
			if back {
				held.seen = Some(Seen::Rewound);
			} else {
				self.conclude();
			}
		}
	}

	/// A packet of the publisher that went, let out at a restart from
	/// behind a hole it never filled: it goes out at once under the shift
	/// it was made under, and moves neither when the last packet went nor
	/// the pace the next shift is read from.
	pub fn push_late(&mut self, track: usize, stamp: Stamp, sync: bool, payload: T) {
		self.emit(track, stamp, sync, payload, None);
	}

	/// Whether a packet would land behind where its track had got: at or
	/// before its last decode time for media, before it for messages,
	/// which may share a pts.
	fn goes_back(&self, track: usize, stamp: Stamp) -> bool {
		let clock = &self.clocks[track];
		let Some(last) = clock.last else {
			return false;
		};
		let landed = stamp.dts + ticks(self.shift_us, clock.timescale);
		match clock.media {
			true => landed <= last.dts,
			false => landed < last.dts,
		}
	}

	/// One packet out, shifted; `at` is when it arrived, None for one let
	/// out late.
	fn emit(&mut self, track: usize, stamp: Stamp, sync: bool, payload: T, at: Option<Instant>) {
		let clock = &mut self.clocks[track];
		let stamp = stamp.shifted(ticks(self.shift_us, clock.timescale));
		if clock.media {
			// A media track never goes back, and what follows a packet
			// dropped for it starts at a sync sample.
			if let (None, Some(last)) = (clock.floor, clock.last) {
				if stamp.dts <= last.dts {
					clock.floor = Some(last.dts);
				}
			}
			if let Some(floor) = clock.floor {
				if stamp.dts <= floor || !sync {
					clock.behind += 1;
					return;
				}
				clock.floor = None;
			}
		}
		clock.last = Some(stamp);
		if let Some(at) = at {
			let end = micros(stamp.dts + stamp.duration, clock.timescale);
			self.recent.push_back((track, at, end));
			while self.recent.len() > RECENT_MAX
				|| self
					.recent
					.front()
					.is_some_and(|(_, first, _)| at.saturating_duration_since(*first) > RECENT)
			{
				self.recent.pop_front();
			}
			self.last_at = Some(self.last_at.map_or(at, |last| last.max(at)));
		}
		self.ready.push((track, stamp, payload));
	}

	/// Lets go of what a restart holds: once `least` has passed since the
	/// first media packet came and every media track has one, or once
	/// `most` has passed since it came. Messages that came with no media
	/// wait for it, for `most` after the first of them. A `most` of zero
	/// lets go of everything at once, decided on what there is, as at the
	/// end of a run. See [`SETTLE_LEAST`] and [`SETTLE`].
	pub fn settle(&mut self, now: Instant, least: Duration, most: Duration) {
		let Some(held) = self.held.as_ref() else {
			return;
		};
		if !most.is_zero() {
			let first = |deciding: bool| {
				(0..self.clocks.len())
					.filter(|&track| !deciding || self.decides(track))
					.filter_map(|track| held.packets[track].first().map(|(_, _, _, at)| *at))
					.min()
			};
			let Some(any) = first(false) else {
				return;
			};
			match first(true) {
				Some(media) => {
					let every = self
						.clocks
						.iter()
						.zip(&held.packets)
						.all(|(clock, packets)| !clock.media || !packets.is_empty());
					let waited = now.saturating_duration_since(media);
					if waited < most && (!every || waited < least) {
						return;
					}
				}
				None if now.saturating_duration_since(any) < most => return,
				None => {}
			}
		}
		self.conclude();
	}

	/// Lets go of everything held: a restart gets its shift from what is
	/// there, and a clock not known to have started again carries on.
	/// With nothing held there is nothing to shift, and no restart to say.
	fn conclude(&mut self) {
		let Some(held) = self.held.take() else {
			return;
		};
		let any = held.packets.iter().any(|packets| !packets.is_empty());
		if let (Some(seen), true) = (held.seen, any) {
			if let Some(restart) = self.restart(seen, &held) {
				self.restarts.push(restart);
			}
		}
		for (track, packets) in held.packets.into_iter().enumerate() {
			for (stamp, sync, payload, at) in packets {
				self.emit(track, stamp, sync, payload, Some(at));
			}
		}
	}

	/// The new shift for a publisher that started again. None when
	/// nothing went out before the gap: there is nothing to follow, the
	/// shift stays, and there is no gap to say.
	fn restart(&mut self, seen: Seen, held: &Held<T>) -> Option<Restart> {
		let end = self
			.clocks
			.iter()
			.filter_map(|clock| {
				let last = clock.last?;
				Some(micros(last.dts + last.duration, clock.timescale))
			})
			.max()?;
		// The tracks that decide, of those that brought something: the media
		// tracks, or the messages where no media came.
		let brought: Vec<usize> = (0..self.clocks.len())
			.filter(|&track| !held.packets[track].is_empty())
			.collect();
		let media: Vec<usize> = brought
			.iter()
			.copied()
			.filter(|&track| self.decides(track))
			.collect();
		let deciding = match media.is_empty() {
			true => brought,
			false => media,
		};
		// Each track's pace before the gap against its packets after it,
		// and the one whose packets waited least decides: a live join hands
		// a track the group in progress from its first frame, so the newest
		// frame of that burst, or one made after it, is the one nearest the
		// publisher's live edge. A track's own pace is the median over its
		// packets of the last [`RECENT`] of where each one's end put the
		// output, carried forward to when the last packet went out. Its own,
		// since a picture can run a decoder's delay behind the sound beside
		// it; a median, since a publisher whose input ends can hand the last
		// of that delay on at once. A track with no pace of its own is read
		// against where the output had got on any track.
		let decided = self.last_at.and_then(|last_at| {
			let estimates = deciding.iter().filter_map(|&track| {
				let clock = &self.clocks[track];
				let lag = held.packets[track]
					.iter()
					.map(|(stamp, _, _, at)| {
						at.saturating_duration_since(last_at).as_micros() as i64
							- micros(stamp.dts + stamp.duration, clock.timescale)
					})
					.min()?;
				let mut paced: Vec<i64> = self
					.recent
					.iter()
					.filter(|(index, _, _)| *index == track)
					.map(|(_, at, end)| {
						end + last_at.saturating_duration_since(*at).as_micros() as i64
					})
					.collect();
				paced.sort_unstable();
				Some((lag, paced.get(paced.len() / 2).copied()))
			});
			let estimates: Vec<(i64, Option<i64>)> = estimates.collect();
			let own = estimates
				.iter()
				.filter_map(|(lag, paced)| Some((*lag, (*paced)?)))
				.min_by_key(|(lag, _)| *lag);
			own.or_else(|| {
				let lag = estimates.iter().map(|(lag, _)| *lag).min()?;
				Some((lag, end))
			})
		});
		// How long the stream was away is read, once the shift is known,
		// off the newest of the first packets after the gap.
		let newest = deciding
			.iter()
			.filter_map(|&track| {
				let (stamp, _, _, _) = held.packets[track].first()?;
				Some(micros(stamp.dts, self.clocks[track].timescale))
			})
			.max();
		// Without a wall clock to read the old pace by, which only packets
		// let out late leave, the shift is only what keeps every track
		// ahead of its last packet.
		let mut shift_us = match decided {
			Some((lag, paced)) => paced + lag,
			None => self.shift_us,
		};
		// Every media track's first packet lands after its own last one,
		// even when the stream was away for less than the newest first
		// packet is ahead of the oldest.
		for _ in 0..self.clocks.len() {
			let mut short = 0i64;
			for (clock, packets) in self.clocks.iter().zip(&held.packets) {
				if !clock.media {
					continue;
				}
				if let (Some(last), Some((stamp, _, _, _))) = (clock.last, packets.first()) {
					let landed = stamp.dts + ticks(shift_us, clock.timescale);
					if landed <= last.dts {
						let missing = i128::from(last.dts + 1 - landed);
						let timescale = i128::from(clock.timescale);
						let us = (missing * 1_000_000 + timescale - 1) / timescale;
						short = short.max(us as i64);
					}
				}
			}
			if short == 0 {
				break;
			}
			shift_us += short;
		}
		self.shift_us = shift_us;
		// As the output now shows it: from the end of the last packet
		// before the gap to the newest of the first packets after it.
		let away = match newest {
			Some(first_us) => Duration::from_micros((first_us + shift_us - end).max(0) as u64),
			None => Duration::ZERO,
		};
		Some(Restart {
			seen,
			away,
			shift_us,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const VIDEO: i64 = 90_000;
	const AUDIO: i64 = 48_000;
	/// One frame of each, in microseconds.
	const PICTURE_US: i64 = 33_333;
	const SOUND_US: i64 = 21_333;

	fn stamp(dts: i64, duration: i64) -> Stamp {
		Stamp {
			pts: dts,
			dts,
			duration,
		}
	}

	/// A video track at 90 kHz and an audio track at 48 kHz, which have
	/// gone out up to `seconds` of their own clock, the last packets
	/// handed on at `at`.
	fn ran(seconds: i64, at: Instant) -> Timeline<u32> {
		let mut timeline = Timeline::new([(VIDEO as u32, true), (AUDIO as u32, true)]);
		timeline.push(0, stamp(seconds * VIDEO - 3000, 3000), true, 0, at);
		timeline.push(1, stamp(seconds * AUDIO - 1024, 1024), true, 0, at);
		assert_eq!(
			timeline.take_ready().len(),
			2,
			"nothing is held before a resume"
		);
		timeline
	}

	/// The same with a data track beside them, its last message at 19.5 s.
	fn ran_with_data(at: Instant) -> Timeline<u32> {
		let mut timeline = Timeline::new([
			(VIDEO as u32, true),
			(AUDIO as u32, true),
			(1_000_000, false),
		]);
		timeline.push(0, stamp(20 * VIDEO - 3000, 3000), true, 0, at);
		timeline.push(1, stamp(20 * AUDIO - 1024, 1024), true, 0, at);
		timeline.push(2, stamp(19_500_000, 0), true, 0, at);
		assert_eq!(timeline.take_ready().len(), 3);
		timeline
	}

	/// A resume, and a frame of the new subscription on every track, which
	/// is what the packets that follow it are.
	fn resumed(timeline: &mut Timeline<u32>, seen: Option<Seen>) {
		timeline.resume(seen);
		for track in 0..timeline.clocks.len() {
			timeline.new_frame(track);
		}
	}

	/// What a settle at `now` lets go of, and the restart it decides.
	fn settled(
		timeline: &mut Timeline<u32>,
		now: Instant,
		least: Duration,
	) -> (Vec<(usize, Stamp, u32)>, Option<Restart>) {
		timeline.settle(now, least, SETTLE);
		(timeline.take_ready(), timeline.take_restarts().pop())
	}

	fn find(out: &[(usize, Stamp, u32)], track: usize) -> Stamp {
		out.iter()
			.find(|(index, _, _)| *index == track)
			.expect("a packet of the track")
			.1
	}

	#[test]
	fn a_publisher_that_started_again_follows_on_after_the_time_it_was_away() {
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Finished));
		let back = before + Duration::from_secs(7);
		// The new publisher's clock started again: one second in, its
		// newest audio frame; a GOP before that, the keyframe video joins
		// at, which arrived with it.
		timeline.push(0, stamp(0, 3000), true, 1, back);
		let (out, restart) = settled(&mut timeline, back, Duration::ZERO);
		assert!(
			out.is_empty() && restart.is_none(),
			"held until every media track has a packet"
		);
		timeline.push(1, stamp(AUDIO, 1024), true, 2, back);
		let (out, restart) = settled(&mut timeline, back, Duration::ZERO);
		let restart = restart.expect("a restart");
		assert_eq!(restart.seen, Seen::Finished);
		// The audio frame waited least: it ends seven seconds after the
		// last packet before the gap ended, as it arrived seven seconds
		// after that packet did.
		assert_eq!(
			restart.shift_us,
			20_000_000 + 7_000_000 - 1_000_000 - SOUND_US
		);
		let video = find(&out, 0);
		let audio = find(&out, 1);
		assert_eq!(audio.dts + audio.duration, 27 * AUDIO);
		assert_eq!(
			restart.away,
			Duration::from_micros(7_000_000 - SOUND_US as u64)
		);
		// One shift for both: the second between them is the second the
		// publisher put between them.
		assert_eq!(
			micros(audio.dts, AUDIO as u32) - micros(video.dts, VIDEO as u32),
			1_000_000
		);
		// And what follows gets it too, with nothing held.
		timeline.push(1, stamp(AUDIO + 1024, 1024), true, 3, back);
		assert_eq!(timeline.take_ready()[0].1.dts, 27 * AUDIO);
	}

	#[test]
	fn the_packet_that_waited_least_sets_the_clock() {
		// A live join hands the audio group in progress from its first
		// frame: four frames at once, the newest just made. The newest is
		// the publisher's live edge, and it is what the shift follows.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_secs(5);
		timeline.push(0, stamp(3 * VIDEO, 3000), true, 1, back);
		for frame in 0..4 {
			timeline.push(1, stamp(3 * AUDIO + frame * 1024, 1024), true, 2, back);
		}
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		let newest_end = micros(3 * AUDIO + 4 * 1024, AUDIO as u32);
		assert_eq!(
			restart.expect("a restart").shift_us,
			20_000_000 + 5_000_000 - newest_end
		);
	}

	#[test]
	fn a_resume_holds_until_it_has_seen_the_live_edge() {
		// A live join hands the audio group in progress from its first
		// frame, made a while ago, and the frames the publisher makes after
		// it one at a time. Held for the least wait, the clock is read off
		// one of those.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_secs(5);
		timeline.push(0, stamp(0, 3000), true, 1, back);
		timeline.push(1, stamp(0, 1024), true, 2, back);
		let (out, restart) = settled(&mut timeline, back, SETTLE_LEAST);
		assert!(
			out.is_empty() && restart.is_none(),
			"let go before the least wait"
		);
		// A frame 200 ms of media on, made just now, 50 ms after the burst.
		let live = back + Duration::from_millis(50);
		timeline.push(1, stamp(9600, 1024), true, 3, live);
		let (_, restart) = settled(&mut timeline, back + SETTLE_LEAST, SETTLE_LEAST);
		let live_end = micros(9600 + 1024, AUDIO as u32);
		assert_eq!(
			restart.expect("a restart").shift_us,
			20_000_000 + 5_050_000 - live_end
		);
	}

	#[test]
	fn a_picture_behind_its_sound_is_set_against_its_own_pace() {
		// A picture at 60 fps runs a decoder's delay, a third of a second,
		// behind the sound on both sides of the gap, and outnumbers it in
		// the packets before the gap. Each track is read against its own
		// pace, so the delay is no part of the gap.
		let start = Instant::now();
		let mut timeline = Timeline::new([(VIDEO as u32, true), (AUDIO as u32, true)]);
		let delay = Duration::from_millis(333);
		for frame in 0..120u64 {
			let at = start + Duration::from_micros(frame * 16_667) + delay;
			timeline.push(0, stamp(frame as i64 * 1500, 1500), true, 0, at);
		}
		for frame in 0..94u64 {
			let at = start + Duration::from_micros(frame * 21_333);
			timeline.push(1, stamp(frame as i64 * 1024, 1024), true, 0, at);
		}
		timeline.take_ready();
		resumed(&mut timeline, Some(Seen::Finished));
		// The new publisher's first frames, each as late as its track's own
		// were: its first sound five seconds after the old sound's last.
		let back = start + Duration::from_micros(93 * 21_333) + Duration::from_secs(5);
		timeline.push(1, stamp(0, 1024), true, 1, back);
		timeline.push(0, stamp(0, 1500), true, 2, back + delay);
		let (out, restart) = settled(&mut timeline, back + delay, Duration::ZERO);
		let audio = find(&out, 1);
		let old_end = micros(94 * 1024, AUDIO as u32);
		let gap = micros(audio.dts, AUDIO as u32) - old_end;
		assert!(
			(gap - (5_000_000 - SOUND_US)).abs() < 1_000,
			"a gap of {gap} us: {restart:?}"
		);
	}

	#[test]
	fn a_tail_handed_on_at_once_does_not_lengthen_the_gap() {
		// A publisher whose input ends empties its decoder: its last third
		// of a second goes out at once, ahead of the pace the rest kept.
		// The gap is read from where that pace had the output.
		let start = Instant::now();
		let mut timeline = Timeline::new([(VIDEO as u32, true)]);
		for frame in 0..60u64 {
			let at = start + Duration::from_micros(frame * 33_333);
			timeline.push(0, stamp(frame as i64 * 3000, 3000), true, 0, at);
		}
		let last = start + Duration::from_micros(59 * 33_333);
		for frame in 60..70 {
			timeline.push(0, stamp(frame * 3000, 3000), frame == 60, 0, last);
		}
		timeline.take_ready();
		resumed(&mut timeline, Some(Seen::Finished));
		let back = last + Duration::from_secs(5);
		timeline.push(0, stamp(0, 3000), true, 1, back);
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		// The paced frames had the output at two seconds when the last of
		// them went out, and the new picture follows that by five.
		let paced = 2_000_000 + 5_000_000 - PICTURE_US;
		let shift = restart.expect("a restart").shift_us;
		assert!(
			(shift - paced).abs() < 1_000,
			"a shift of {shift} where the pace says {paced}"
		);
	}

	#[test]
	fn a_publisher_that_carried_on_goes_out_at_once_with_its_timestamps() {
		// Not known whether it carried on: its first media packet is ahead
		// of where its track had got, so it did, and nothing waits, the
		// sound not for the picture's keyframe either.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, None);
		let back = before + Duration::from_secs(5);
		timeline.push(1, stamp(25 * AUDIO, 1024), true, 1, back);
		let out = timeline.take_ready();
		assert_eq!(out.len(), 1, "the sound went out at once");
		assert_eq!(out[0].1.dts, 25 * AUDIO);
		timeline.push(0, stamp(25 * VIDEO, 3000), true, 2, back);
		assert_eq!(timeline.take_ready()[0].1.dts, 25 * VIDEO);
		assert!(timeline.take_restarts().is_empty());
		assert_eq!(timeline.shift_us(), 0);
	}

	#[test]
	fn a_clock_that_went_back_is_a_restart_whatever_else_was_seen() {
		// Nothing said the publisher was another one, and its groups were
		// numbered on, but its first media packet is behind the last one
		// out.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, None);
		let back = before + Duration::from_secs(4);
		timeline.push(0, stamp(3 * VIDEO, 3000), true, 1, back);
		assert!(
			timeline.take_ready().is_empty(),
			"held from the packet that said so"
		);
		timeline.push(1, stamp(3 * AUDIO, 1024), true, 2, back);
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		let restart = restart.expect("a restart");
		assert_eq!(restart.seen, Seen::Rewound);
		assert_eq!(
			restart.shift_us,
			20_000_000 + 4_000_000 - 3_000_000 - PICTURE_US
		);
	}

	#[test]
	fn a_shift_survives_the_next_carry_on_and_is_replaced_at_the_next_restart() {
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_secs(2);
		timeline.push(0, stamp(0, 3000), true, 1, back);
		timeline.push(1, stamp(0, 1024), true, 2, back);
		let (_, first) = settled(&mut timeline, back, Duration::ZERO);
		let first = first.expect("a restart");
		assert_eq!(first.shift_us, 20_000_000 + 2_000_000 - PICTURE_US);
		// The same publisher's session is replaced: its clock carried on,
		// and the shift stays.
		resumed(&mut timeline, None);
		let later = back + Duration::from_secs(10);
		timeline.push(0, stamp(10 * VIDEO, 3000), true, 3, later);
		timeline.push(1, stamp(10 * AUDIO, 1024), true, 4, later);
		let out = timeline.take_ready();
		assert_eq!(
			micros(find(&out, 0).dts, VIDEO as u32),
			10_000_000 + first.shift_us
		);
		// Then it starts again itself: the shift is worked out afresh from
		// where the output is, the end of that last picture.
		let end = micros(find(&out, 0).dts + 3000, VIDEO as u32);
		resumed(&mut timeline, Some(Seen::Finished));
		let again = later + Duration::from_secs(3);
		timeline.push(0, stamp(0, 3000), true, 5, again);
		timeline.push(1, stamp(0, 1024), true, 6, again);
		let (_, second) = settled(&mut timeline, again, Duration::ZERO);
		assert_eq!(
			second.expect("a restart").shift_us,
			end + 3_000_000 - PICTURE_US
		);
	}

	#[test]
	fn no_track_goes_back_when_the_stream_was_away_for_less_than_a_gop() {
		// The video joins a GOP two seconds old and the audio at its live
		// edge, after a gap of a fraction of a second: placing the audio
		// by the wall clock alone would put the video before its own last
		// packet.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_millis(300);
		timeline.push(0, stamp(10 * VIDEO, 3000), true, 1, back);
		timeline.push(1, stamp(12 * AUDIO, 1024), true, 2, back);
		let (out, _) = settled(&mut timeline, back, Duration::ZERO);
		let video = find(&out, 0);
		assert_eq!(
			video.dts,
			20 * VIDEO - 3000 + 1,
			"the video went back: {video:?}"
		);
	}

	#[test]
	fn a_track_that_comes_late_starts_again_at_a_sync_sample_past_its_last_packet() {
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Finished));
		let back = before + Duration::from_secs(5);
		timeline.push(1, stamp(AUDIO, 1024), true, 1, back);
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		assert!(restart.is_none());
		let (_, restart) = settled(&mut timeline, back + SETTLE, Duration::ZERO);
		let restart = restart.expect("the wait ran out");
		assert_eq!(
			restart.shift_us,
			20_000_000 + 5_000_000 - 1_000_000 - SOUND_US
		);
		// The video comes at last, from a keyframe older than anything the
		// shift could put after its own last packet: it is dropped, and so
		// is the picture after it, which no decoder could start at, until
		// a keyframe passes that point.
		timeline.push(0, stamp(-5 * VIDEO, 3000), true, 2, back + SETTLE);
		timeline.push(0, stamp(VIDEO, 3000), false, 3, back + SETTLE);
		assert!(timeline.take_ready().is_empty());
		assert_eq!(timeline.behind(0), 2);
		timeline.push(0, stamp(2 * VIDEO, 3000), true, 4, back + SETTLE);
		let out = timeline.take_ready();
		assert_eq!(
			micros(out[0].1.dts, VIDEO as u32),
			2_000_000 + restart.shift_us
		);
	}

	#[test]
	fn a_data_track_is_never_waited_for_and_follows_the_same_shift() {
		let before = Instant::now();
		let mut timeline = Timeline::new([(VIDEO as u32, true), (1_000_000, false)]);
		timeline.push(0, stamp(10 * VIDEO, 3000), true, 0, before);
		timeline.push(1, stamp(9_000_000, 0), true, 0, before);
		timeline.take_ready();
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_secs(1);
		timeline.push(0, stamp(0, 3000), true, 1, back);
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		let restart = restart.expect("a restart");
		assert_eq!(
			restart.shift_us,
			10_000_000 + PICTURE_US + 1_000_000 - PICTURE_US
		);
		timeline.push(1, stamp(500_000, 0), true, 2, back);
		assert_eq!(timeline.take_ready()[0].1.pts, 500_000 + restart.shift_us);
	}

	#[test]
	fn a_message_alone_decides_nothing_while_the_reader_has_media() {
		// Only a message of the new publisher comes at first. It is not
		// what the shift is read off: it waits, and the media that comes
		// within the hold sets the shift and goes out, the picture at its
		// keyframe.
		let before = Instant::now();
		let mut timeline = ran_with_data(before);
		resumed(&mut timeline, Some(Seen::Finished));
		let back = before + Duration::from_secs(5);
		timeline.push(2, stamp(400_000, 0), true, 1, back);
		let early = back + SETTLE - Duration::from_millis(1);
		let (out, restart) = settled(&mut timeline, early, SETTLE_LEAST);
		assert!(
			out.is_empty() && restart.is_none(),
			"a message alone let go: {restart:?}"
		);
		let media = back + Duration::from_secs(1);
		timeline.push(0, stamp(0, 3000), true, 2, media);
		timeline.push(1, stamp(0, 1024), true, 3, media);
		let (out, restart) = settled(&mut timeline, media + SETTLE_LEAST, SETTLE_LEAST);
		let restart = restart.expect("a restart");
		assert_eq!(restart.shift_us, 20_000_000 + 6_000_000 - PICTURE_US);
		assert_eq!(find(&out, 0).dts, ticks(restart.shift_us, VIDEO as u32));
		assert!(find(&out, 2).pts > 19_500_000, "the message went back");
		assert_eq!(timeline.behind(0), 0);
	}

	#[test]
	fn a_reader_of_messages_alone_reads_the_shift_off_them() {
		let before = Instant::now();
		let mut timeline = Timeline::new([(1_000_000, false)]);
		timeline.push(0, stamp(30_000_000, 0), true, 0, before);
		timeline.take_ready();
		resumed(&mut timeline, Some(Seen::Finished));
		let back = before + Duration::from_secs(5);
		timeline.push(0, stamp(1_000_000, 0), true, 1, back);
		let (out, restart) = settled(&mut timeline, back, Duration::ZERO);
		let restart = restart.expect("a restart");
		assert_eq!(restart.shift_us, 30_000_000 + 5_000_000 - 1_000_000);
		assert_eq!(out[0].1.pts, 35_000_000);
	}

	#[test]
	fn a_second_restart_inside_the_hold_lets_the_first_go_first() {
		// A was killed and B took the name; B's sound is held, its picture
		// not yet come, when B is lost too and C takes the name. B's packet
		// goes out under a shift of its own, and C follows it.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		timeline.push(1, stamp(0, 1024), true, 1, before + Duration::from_secs(2));
		resumed(&mut timeline, Some(Seen::Replaced));
		let restarts = timeline.take_restarts();
		assert_eq!(restarts.len(), 1, "B's restart is decided when C comes");
		let c = before + Duration::from_secs(4);
		timeline.push(1, stamp(0, 1024), true, 2, c);
		timeline.push(0, stamp(0, 3000), true, 2, c);
		timeline.settle(c, Duration::ZERO, SETTLE);
		assert_eq!(timeline.take_restarts().len(), 1);
		let audio: Vec<i64> = timeline
			.take_ready()
			.iter()
			.filter(|(track, _, _)| *track == 1)
			.map(|(_, stamp, _)| stamp.dts)
			.collect();
		assert_eq!(audio.len(), 2);
		assert!(audio[1] > audio[0], "audio went back: {audio:?}");
		// C's sound follows B's by the two seconds between their arrivals,
		// within the picture C's shift was read off.
		let apart = micros(audio[1] - audio[0], AUDIO as u32);
		assert!((apart - 2_000_000).abs() < PICTURE_US, "{apart} us apart");
	}

	#[test]
	fn a_track_renumbered_after_the_decision_opens_no_second_restart() {
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, None);
		let back = before + Duration::from_secs(5);
		timeline.push(1, stamp(25 * AUDIO, 1024), true, 1, back);
		assert_eq!(timeline.take_ready().len(), 1);
		timeline.renumbered();
		timeline.push(1, stamp(25 * AUDIO + 1024, 1024), true, 2, back);
		assert_eq!(timeline.take_ready().len(), 1, "a second hold opened");
		assert!(timeline.take_restarts().is_empty());
	}

	#[test]
	fn a_message_sharing_a_pts_across_a_carry_on_is_no_restart() {
		let before = Instant::now();
		let mut timeline = ran_with_data(before);
		resumed(&mut timeline, None);
		let back = before + Duration::from_secs(5);
		timeline.push(2, stamp(19_500_000, 0), true, 1, back);
		timeline.push(0, stamp(25 * VIDEO, 3000), true, 1, back);
		timeline.push(1, stamp(25 * AUDIO, 1024), true, 1, back);
		assert!(timeline.take_restarts().is_empty());
		assert_eq!(timeline.take_ready().len(), 3);
	}

	#[test]
	fn a_media_packet_that_would_go_back_waits_for_a_sync_sample_past_the_last() {
		// Whatever was decided, a picture behind the last one out is
		// dropped, and so is everything after it until a keyframe past the
		// last picture.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		timeline.push(0, stamp(19 * VIDEO, 3000), true, 1, before);
		timeline.push(0, stamp(20 * VIDEO, 3000), false, 2, before);
		timeline.push(0, stamp(21 * VIDEO, 3000), true, 3, before);
		let out = timeline.take_ready();
		assert_eq!(out.len(), 1);
		assert_eq!(out[0].1.dts, 21 * VIDEO);
		assert_eq!(timeline.behind(0), 2);
	}

	#[test]
	fn media_let_out_late_moves_nothing_the_next_shift_is_read_from() {
		// Groups of the publisher that went, held behind a hole it never
		// filled, go out at the restart. They bound where the new media may
		// land, but the gap is read from when the last packet came.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Replaced));
		timeline.push_late(1, stamp(20 * AUDIO, 1024), true, 1);
		assert_eq!(
			timeline.take_ready()[0].1.dts,
			20 * AUDIO,
			"under the shift it was made under"
		);
		let back = before + Duration::from_secs(5);
		timeline.push(0, stamp(0, 3000), true, 2, back);
		timeline.push(1, stamp(0, 1024), true, 3, back);
		let (_, restart) = settled(&mut timeline, back, Duration::ZERO);
		assert_eq!(
			restart.expect("a restart").shift_us,
			20_000_000 + 5_000_000 - PICTURE_US
		);
	}

	#[test]
	fn a_resume_that_held_nothing_is_the_same_gap_as_the_next() {
		// The tracks were taken up on a broadcast that handed over nothing
		// new, and then on the one that did: one restart, from the
		// publisher the output last came from.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		resumed(&mut timeline, Some(Seen::Finished));
		resumed(&mut timeline, Some(Seen::Replaced));
		let back = before + Duration::from_secs(9);
		timeline.push(0, stamp(0, 3000), true, 1, back);
		timeline.push(1, stamp(0, 1024), true, 2, back);
		timeline.settle(back, Duration::ZERO, SETTLE);
		let restarts = timeline.take_restarts();
		assert_eq!(restarts.len(), 1);
		assert_eq!(restarts[0].seen, Seen::Finished);
		// And the end of a run that held nothing says no restart.
		resumed(&mut timeline, Some(Seen::Finished));
		timeline.settle(back, Duration::ZERO, Duration::ZERO);
		assert!(timeline.take_restarts().is_empty());
	}

	#[test]
	fn packets_held_before_the_new_subscription_decide_nothing() {
		// Not known whether the publisher carried on. Before any frame of
		// the new subscription comes, a track lets go of a group of the old
		// publisher kept behind a hole: ahead of where the track had got,
		// it is no word that the clock carried on.
		let before = Instant::now();
		let mut timeline = ran(20, before);
		timeline.resume(None);
		let back = before + Duration::from_secs(5);
		timeline.push(1, stamp(20 * AUDIO, 1024), true, 1, back);
		assert_eq!(timeline.take_ready().len(), 1, "out as one let out late");
		// The new subscription's first frames number their groups from
		// nothing.
		timeline.new_frame(0);
		timeline.new_frame(1);
		timeline.renumbered();
		timeline.push(0, stamp(0, 3000), true, 2, back);
		timeline.push(1, stamp(0, 1024), true, 3, back);
		let (out, restart) = settled(&mut timeline, back + SETTLE_LEAST, SETTLE_LEAST);
		assert_eq!(restart.expect("a restart").seen, Seen::Renumbered);
		assert!(
			out.iter().any(|(track, _, _)| *track == 0),
			"the new picture was dropped"
		);
		assert_eq!(timeline.behind(0), 0);
	}

	#[test]
	fn messages_wait_for_the_media_for_the_hold_and_no_longer() {
		// Only a message of the new publisher came. It waits for the media
		// as long as the hold would, and with none by then the shift is
		// read off the message itself.
		let before = Instant::now();
		let mut timeline = ran_with_data(before);
		resumed(&mut timeline, Some(Seen::Finished));
		let back = before + Duration::from_secs(5);
		timeline.push(2, stamp(400_000, 0), true, 1, back);
		let early = back + SETTLE - Duration::from_millis(1);
		let (out, restart) = settled(&mut timeline, early, SETTLE_LEAST);
		assert!(out.is_empty() && restart.is_none());
		let (out, restart) = settled(&mut timeline, back + SETTLE, SETTLE_LEAST);
		let restart = restart.expect("a restart");
		assert_eq!(restart.shift_us, 19_500_000 + 5_000_000 - 400_000);
		assert_eq!(out[0].1.pts, 24_500_000);
		assert_eq!(restart.away, Duration::from_micros(4_500_000));
		// The media that comes after follows the same shift, ahead of the
		// old picture.
		timeline.push(0, stamp(0, 3000), true, 2, back + SETTLE);
		assert_eq!(
			timeline.take_ready()[0].1.dts,
			ticks(restart.shift_us, VIDEO as u32)
		);
	}

	#[test]
	fn a_hold_of_messages_ended_by_the_next_resume_is_read_off_them() {
		// B sent a message and no media before C took the name: B's
		// restart is read off its message, and the message lands ahead of
		// the last one of A's.
		let before = Instant::now();
		let mut timeline = ran_with_data(before);
		resumed(&mut timeline, Some(Seen::Replaced));
		timeline.push(
			2,
			stamp(400_000, 0),
			true,
			1,
			before + Duration::from_secs(2),
		);
		resumed(&mut timeline, Some(Seen::Replaced));
		let restarts = timeline.take_restarts();
		assert_eq!(restarts.len(), 1);
		assert_eq!(restarts[0].shift_us, 19_500_000 + 2_000_000 - 400_000);
		assert_eq!(restarts[0].away, Duration::from_micros(1_500_000));
		let out = timeline.take_ready();
		assert_eq!(out[0].1.pts, 21_500_000);
	}

	#[test]
	fn a_restart_with_nothing_out_before_it_says_nothing() {
		let before = Instant::now();
		let mut timeline = Timeline::new([(VIDEO as u32, true)]);
		resumed(&mut timeline, Some(Seen::Finished));
		timeline.push(0, stamp(0, 3000), true, 1, before);
		let (out, restart) = settled(&mut timeline, before, Duration::ZERO);
		assert!(restart.is_none(), "no gap to say: {restart:?}");
		assert_eq!(out[0].1.dts, 0);
	}
}

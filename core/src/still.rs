//! A still held while a publisher is away.
//!
//! A reader that copies a broadcast on to a destination that drops an idle
//! connection, an RTMP push to a streaming service for one, keeps handing
//! media on while the publisher it reads is away. Otherwise the destination
//! closes the connection meanwhile, and the next publisher's media starts a
//! new broadcast there. [`Stills`] says what goes out in the meantime, and
//! when. A picture gets its last sync sample again every [`INTERVAL`], under
//! new timestamps: the same bytes, which decode on their own and need no
//! encoder. Sound gets silent frames one after another with no hole between
//! them; [`silent_aac`] is the frame for AAC-LC. Data and text tracks get
//! nothing.
//!
//! Each track's stills carry on from the last packet the track handed on,
//! at the pace of the wall clock: a still is due once as much time has
//! passed since that packet went out as its decode time is past the
//! packet's. So a still is never behind its track, and the stills run in
//! real time. When the publisher's media goes on again, [`Stills::bridge`]
//! fills what is left between a track's last still and its next packet,
//! to within an interval for a picture and a frame for sound. Where that
//! next packet lands is the timeline's business; see
//! [`crate::restart::Timeline::still`].
//!
//! Timestamps here are the reader's output ticks, past whatever shift a
//! restart gave; the bytes that ride with a still are the reader's own.

use std::time::{Duration, Instant};

use crate::restart::Stamp;

/// How often a picture's still goes out.
pub const INTERVAL: Duration = Duration::from_millis(500);

/// How long a reader hears no media from its publisher before it holds a
/// still, when nothing has said yet that the publisher is away. A publisher
/// lost outright says nothing a reader can hear until the relay's idle
/// timeout; a live track's frames come a frame apart, and one that is late
/// by most of a second is still a late frame.
pub const QUIET: Duration = Duration::from_secs(1);

/// The samples an AAC frame carries.
pub const AAC_FRAME: u64 = 1024;

/// What a track's stills are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	/// A picture: its last sync sample again every [`INTERVAL`].
	Picture,
	/// Sound at `rate` samples a second: one silent frame of [`AAC_FRAME`]
	/// samples after another.
	Sound { rate: u32 },
	/// No stills: a data or a text track, or sound with no silent frame.
	Nothing,
}

/// One track's stills.
struct Track {
	kind: Kind,
	timescale: u32,
	/// The last packet of the publisher's handed on, in output ticks, and
	/// when it went out. The stills are paced from it.
	anchor: Option<(Stamp, Instant)>,
	/// A picture's last sync sample, once one has gone out: how far its
	/// presentation is past its decode time. Until one has, there is
	/// nothing to repeat.
	sync: Option<i64>,
	/// A picture's frame duration as its packets last said it, which each
	/// still carries.
	step: i64,
	/// Stills handed on since the anchor.
	made: u64,
}

fn ticks(us: i64, timescale: u32) -> i64 {
	ffrwd_bmff::time::rescale(us, 1_000_000, u64::from(timescale))
}

fn micros(ticks: i64, timescale: u32) -> i64 {
	ffrwd_bmff::time::rescale(ticks, u64::from(timescale), 1_000_000)
}

impl Track {
	/// The track's still `n` after its anchor, counting from 0, and when it
	/// is due.
	fn nth(&self, n: u64) -> Option<(Stamp, Instant)> {
		let (anchor, at) = self.anchor?;
		let stamp = match self.kind {
			Kind::Picture => {
				let offset = self.sync?;
				let after = INTERVAL.as_micros() as i64 * (n as i64 + 1);
				let dts = anchor.dts + ticks(after, self.timescale);
				Stamp {
					pts: dts + offset,
					dts,
					duration: self.step.max(1),
				}
			}
			Kind::Sound { rate } => {
				let samples = |count: u64| -> i64 {
					let scaled = i128::from(count) * i128::from(self.timescale);
					((scaled + i128::from(rate) / 2) / i128::from(rate)) as i64
				};
				// The silence starts where the last frame ended.
				let start = anchor.dts
					+ match anchor.duration {
						0 => samples(AAC_FRAME),
						duration => duration,
					};
				let dts = start + samples(n * AAC_FRAME);
				Stamp {
					pts: dts,
					dts,
					duration: start + samples((n + 1) * AAC_FRAME) - dts,
				}
			}
			Kind::Nothing => return None,
		};
		let since = micros(stamp.dts - anchor.dts, self.timescale).max(0);
		Some((stamp, at + Duration::from_micros(since as u64)))
	}
}

/// Every track's stills; see the module's own words.
pub struct Stills {
	tracks: Vec<Track>,
}

impl Stills {
	/// One track per track the reader hands on, in its order: its
	/// timescale, and what its stills are.
	pub fn new(tracks: impl IntoIterator<Item = (u32, Kind)>) -> Self {
		Self {
			tracks: tracks
				.into_iter()
				.map(|(timescale, kind)| Track {
					kind: match kind {
						Kind::Sound { rate: 0 } => Kind::Nothing,
						kind => kind,
					},
					timescale: timescale.max(1),
					anchor: None,
					sync: None,
					step: 0,
					made: 0,
				})
				.collect(),
		}
	}

	/// What `track`'s stills are.
	pub fn kind(&self, track: usize) -> Kind {
		self.tracks[track].kind
	}

	/// Whether the last packet `track` handed on was a still.
	pub fn after_still(&self, track: usize) -> bool {
		self.tracks[track].made > 0
	}

	/// A packet of the publisher's went out on `track` at `now`, in output
	/// ticks; `sync` is whether a decoder can start at it. The track's
	/// stills carry on from it.
	pub fn handed_on(&mut self, track: usize, stamp: Stamp, sync: bool, now: Instant) {
		let track = &mut self.tracks[track];
		if track.kind == Kind::Picture {
			let step = match (stamp.duration, track.anchor) {
				(0, Some((last, _))) => stamp.dts - last.dts,
				(duration, _) => duration,
			};
			if step > 0 {
				track.step = step;
			}
			if sync {
				track.sync = Some(stamp.pts - stamp.dts);
			}
		}
		track.anchor = Some((stamp, now));
		track.made = 0;
	}

	/// Every still due by `now`, each track's in order of decode time. Each
	/// one returned is counted as handed on.
	pub fn due(&mut self, now: Instant) -> Vec<(usize, Stamp)> {
		let mut out = Vec::new();
		for (index, track) in self.tracks.iter_mut().enumerate() {
			while let Some((stamp, due)) = track.nth(track.made) {
				if due > now {
					break;
				}
				out.push((index, stamp));
				track.made += 1;
			}
		}
		out
	}

	/// When the next still of any track is due.
	pub fn next(&self) -> Option<Instant> {
		self.tracks
			.iter()
			.filter_map(|track| Some(track.nth(track.made)?.1))
			.min()
	}

	/// The stills that go between `track`'s last still and a packet of the
	/// publisher's landing at decode time `next`: a picture's every
	/// [`INTERVAL`] that ends before it, a sound's silent frames up to where
	/// the next frame starts. Nothing when the track's last packet was the
	/// publisher's own. Each one returned is counted as handed on.
	pub fn bridge(&mut self, track: usize, next: i64) -> Vec<Stamp> {
		let track = &mut self.tracks[track];
		let mut out = Vec::new();
		if track.made == 0 {
			return out;
		}
		while let Some((stamp, _)) = track.nth(track.made) {
			if stamp.dts + stamp.duration > next {
				break;
			}
			out.push(stamp);
			track.made += 1;
		}
		out
	}
}

/// A silent raw AAC frame for the AudioSpecificConfig `config`, or why
/// there is none.
///
/// The frame is one channel element, a single channel's for mono and a
/// channel pair's for stereo, whose one long window has no scale factor
/// bands at all, followed by the end element. With no bands there are no
/// spectral coefficients, so it decodes to [`AAC_FRAME`] samples of zero a
/// channel at any sample rate. That holds for AAC-LC with frames of 1024
/// samples, mono or stereo; anything else (HE-AAC, 960-sample frames, more
/// channels, a program config element) has none.
pub fn silent_aac(config: &[u8]) -> Result<&'static [u8], String> {
	// A single channel element, instance 0; a global gain of 160; a long
	// window, a KBD shape, no bands, no prediction; no pulse, TNS or gain
	// control data; the end element.
	const MONO: &[u8] = &[0x01, 0x40, 0x20, 0x07];
	// A channel pair element, instance 0, sharing that window; no M/S; each
	// channel a global gain of 160 and nothing else; the end element.
	const STEREO: &[u8] = &[0x21, 0x10, 0x05, 0x00, 0xA0, 0x1C];

	let mut bits = Bits::new(config);
	let mut object = bits.read(5)?;
	if object == 31 {
		object = 32 + bits.read(6)?;
	}
	if bits.read(4)? == 15 {
		bits.read(24)?;
	}
	let channels = bits.read(4)?;
	if object != 2 {
		return Err(format!(
			"its AudioSpecificConfig names audio object type {object}, not AAC-LC (2)"
		));
	}
	if bits.read(1)? == 1 {
		return Err("its AAC frames are 960 samples long, not 1024".to_string());
	}
	if bits.read(1)? == 1 {
		bits.read(14)?;
	}
	bits.read(1)?;
	// An SBR extension signalled behind the LC config is HE-AAC, which a
	// decoder told so would read past an LC frame's end.
	if bits.left() >= 16 && bits.read(11)? == 0x2b7 && bits.read(5)? == 5 && bits.read(1)? == 1 {
		return Err("its AudioSpecificConfig signals SBR (HE-AAC)".to_string());
	}
	match channels {
		1 => Ok(MONO),
		2 => Ok(STEREO),
		0 => Err("its channels are laid out in a program config element".to_string()),
		count => Err(format!(
			"it has channel configuration {count}, and a silent frame is made for mono and \
			 stereo"
		)),
	}
}

/// A bit reader over an AudioSpecificConfig, most significant bit first.
struct Bits<'a> {
	bytes: &'a [u8],
	at: usize,
}

impl<'a> Bits<'a> {
	fn new(bytes: &'a [u8]) -> Self {
		Self { bytes, at: 0 }
	}

	fn left(&self) -> usize {
		self.bytes.len() * 8 - self.at
	}

	fn read(&mut self, count: usize) -> Result<u32, String> {
		if count > self.left() {
			return Err("its AudioSpecificConfig is cut short".to_string());
		}
		let mut value = 0u32;
		for _ in 0..count {
			let bit = (self.bytes[self.at / 8] >> (7 - self.at % 8)) & 1;
			value = (value << 1) | u32::from(bit);
			self.at += 1;
		}
		Ok(value)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const VIDEO: u32 = 90_000;
	const AUDIO: u32 = 48_000;

	fn stamp(dts: i64, duration: i64) -> Stamp {
		Stamp {
			pts: dts,
			dts,
			duration,
		}
	}

	/// A picture at 90 kHz and its sound at 48 kHz, each with one packet
	/// handed on at `at`: the picture a sync sample at 10 s, the sound a
	/// frame ending at 10 s.
	fn running(at: Instant) -> Stills {
		let mut stills = Stills::new([
			(VIDEO, Kind::Picture),
			(AUDIO, Kind::Sound { rate: AUDIO }),
			(1_000_000, Kind::Nothing),
		]);
		stills.handed_on(0, stamp(10 * VIDEO as i64, 3000), true, at);
		stills.handed_on(1, stamp(10 * AUDIO as i64 - 1024, 1024), true, at);
		stills
	}

	#[test]
	fn a_picture_repeats_every_interval_and_the_sound_follows_on_without_a_hole() {
		let at = Instant::now();
		let mut stills = running(at);
		assert!(stills.due(at).is_empty(), "nothing is due at once");
		let due = stills.due(at + Duration::from_secs(1));
		let pictures: Vec<i64> = due
			.iter()
			.filter(|(track, _)| *track == 0)
			.map(|(_, stamp)| stamp.dts)
			.collect();
		assert_eq!(
			pictures,
			vec![10 * 90_000 + 45_000, 11 * 90_000],
			"a picture every 500 ms after the last"
		);
		let sound: Vec<Stamp> = due
			.iter()
			.filter(|(track, _)| *track == 1)
			.map(|(_, stamp)| *stamp)
			.collect();
		// Every frame that starts within the second, the last of them 46
		// frames after the one that went out.
		assert_eq!(sound.len(), 46);
		assert_eq!(sound[0].dts, 10 * 48_000);
		for (a, b) in sound.iter().zip(&sound[1..]) {
			assert_eq!(a.dts + a.duration, b.dts, "a hole in the sound");
			assert_eq!(a.duration, 1024);
		}
		assert!(
			due.iter().all(|(track, _)| *track != 2),
			"a data track gets nothing"
		);
		assert!(stills.after_still(0) && stills.after_still(1));
	}

	#[test]
	fn stills_run_at_the_pace_of_the_wall_clock() {
		let at = Instant::now();
		let mut stills = running(at);
		let mut count = 0;
		for tenth in 1..=100 {
			count += stills
				.due(at + Duration::from_millis(tenth * 100))
				.iter()
				.filter(|(track, _)| *track == 0)
				.count();
		}
		assert_eq!(count, 20, "ten seconds of pictures every 500 ms");
		// The sound's next frame, the 469th after the one that went out, is
		// what comes next.
		let next = stills.next().expect("a still to come");
		let sound = micros(469 * 1024, AUDIO) as u64;
		assert_eq!(next, at + Duration::from_micros(sound));
	}

	#[test]
	fn a_picture_with_no_sync_sample_out_has_nothing_to_repeat() {
		let at = Instant::now();
		let mut stills = Stills::new([(VIDEO, Kind::Picture)]);
		stills.handed_on(0, stamp(3000, 3000), false, at);
		assert!(stills.due(at + Duration::from_secs(5)).is_empty());
		stills.handed_on(0, stamp(6000, 3000), true, at);
		assert_eq!(stills.due(at + Duration::from_secs(1)).len(), 2);
	}

	#[test]
	fn a_picture_still_keeps_its_sync_samples_composition_offset_and_frame_duration() {
		let at = Instant::now();
		let mut stills = Stills::new([(1_000_000, Kind::Picture)]);
		// A legacy track, which states no durations: the step between two
		// packets stands for one.
		stills.handed_on(
			0,
			Stamp {
				pts: 66_667,
				dts: 0,
				duration: 0,
			},
			true,
			at,
		);
		stills.handed_on(0, stamp(33_333, 0), false, at);
		let due = stills.due(at + INTERVAL);
		assert_eq!(due.len(), 1);
		assert_eq!(due[0].1.dts, 533_333);
		assert_eq!(due[0].1.pts, 533_333 + 66_667);
		assert_eq!(due[0].1.duration, 33_333);
	}

	#[test]
	fn sound_at_a_rate_its_timescale_does_not_divide_stays_contiguous() {
		let at = Instant::now();
		let mut stills = Stills::new([(90_000, Kind::Sound { rate: 44_100 })]);
		stills.handed_on(0, stamp(0, 2090), true, at);
		let due = stills.due(at + Duration::from_secs(10));
		for (a, b) in due.iter().zip(&due[1..]) {
			assert_eq!(a.1.dts + a.1.duration, b.1.dts);
		}
		// After n frames the silence is as long as n frames, to the tick.
		let last = due.last().expect("frames").1;
		let frames = due.len() as i64;
		let exact = (frames * 1024 * 90_000 + 22_050) / 44_100;
		assert_eq!(last.dts + last.duration - 2090, exact);
	}

	#[test]
	fn the_bridge_fills_to_within_an_interval_and_a_frame() {
		let at = Instant::now();
		let mut stills = running(at);
		stills.due(at + Duration::from_secs(2));
		// The publisher's picture lands 1.7 s after the last still.
		let picture = stills.bridge(0, 10 * 90_000 + 4 * 45_000 + 153_000);
		assert_eq!(picture.len(), 3, "{picture:?}");
		let last = picture.last().expect("stills").dts;
		assert!(10 * 90_000 + 4 * 45_000 + 153_000 - last < 45_000 + 3000);
		let next = 13 * 48_000 + 500;
		let sound = stills.bridge(1, next);
		let end = sound.last().map(|s| s.dts + s.duration).expect("frames");
		assert!(
			end <= next && next - end < 1024,
			"a gap of {} ticks",
			next - end
		);
		// Once the publisher's own packet goes out there is nothing to bridge.
		stills.handed_on(1, stamp(next, 1024), true, at);
		assert!(stills.bridge(1, next + 5000).is_empty());
		assert!(!stills.after_still(1));
	}

	#[test]
	fn a_track_with_no_stills_out_bridges_nothing() {
		let at = Instant::now();
		let mut stills = running(at);
		assert!(stills.bridge(0, 20 * 90_000).is_empty());
		assert!(stills.bridge(1, 20 * 48_000).is_empty());
	}

	#[test]
	fn silent_frames_for_lc_mono_and_stereo_and_none_for_the_rest() {
		// LC, 48 kHz, stereo and mono.
		assert_eq!(silent_aac(&[0x11, 0x90]).expect("stereo").len(), 6);
		assert_eq!(silent_aac(&[0x11, 0x88]).expect("mono").len(), 4);
		// LC, 44.1 kHz stereo with the backward-compatible SBR signal off,
		// and with it on.
		assert!(silent_aac(&[0x12, 0x10, 0x56, 0xe5, 0x00]).is_ok());
		assert!(silent_aac(&[0x12, 0x10, 0x56, 0xe5, 0x80]).is_err());
		// HE-AAC signalled outright, and HE-AAC v2.
		assert!(silent_aac(&[0x2b, 0x92, 0x08, 0x00]).is_err());
		assert!(silent_aac(&[0xeb, 0x09, 0x88, 0x00]).is_err());
		// 5.1, 960-sample frames, a program config element, cut short.
		assert!(silent_aac(&[0x11, 0xb0]).is_err());
		assert!(silent_aac(&[0x11, 0x94]).is_err());
		assert!(silent_aac(&[0x11, 0x80]).is_err());
		assert!(silent_aac(&[0x11]).is_err());
		// An explicit rate, 24 bits of it after the escape index.
		assert!(silent_aac(&[0x17, 0x80, 0x5d, 0xc0, 0x10]).is_ok());
	}
}

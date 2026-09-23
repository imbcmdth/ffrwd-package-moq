//! MoQ groups: where one opens on the way out, and where a reader may
//! start on the way in. This package's own transport convention, in
//! both directions.
//!
//! A subscription begins at the LATEST group by default and no decoder
//! can start in the middle of one, so where the groups are cut is what
//! decides where a subscriber may join, and how far behind live it
//! lands when it does. That is a transport decision rather
//! than anything a container says, which is why it lives here and not
//! in the muxer: [`ffrwd_bmff::mux`] hands back one fragment per
//! sample and says nothing about what a group is.
//!
//! Video rotates where the encoder put its keyframes. Every keyframe
//! opens a group, so an all-intra stream makes every frame its own
//! group - which is the point, since a join can then land nowhere but
//! on a frame a decoder can start at. Audio has no keyframe to rotate
//! on, every AAC frame being a sync sample, so every frame is its own
//! group unless a run asks for fewer; see [`AUDIO_GROUP_MS`]. A group
//! therefore always begins on a whole AAC frame, which is what a
//! decoder needs, and does not align with any video group, which is a
//! later concern.
//!
//! The stream's first fragment always opens the first group.

/// How long an audio group runs before it closes, in milliseconds; 0
/// is one frame per group. The default is a fifth of a second.
///
/// A relay forwards a group as it arrives and a player reading at the
/// live edge holds what it has until the group it belongs to is whole,
/// so a group that spans a second is a second of audio arriving at
/// once, and the picture beside it runs ahead. A fifth of a second is
/// ten AAC frames at 48 kHz: short enough that a player's own jitter
/// buffer swallows it, and slow enough in GROUPS A SECOND to survive a
/// public relay under load, where a hundred milliseconds did not - see
/// the README.
///
/// 0 is what upstream hang's own publisher writes, and on a real relay
/// it is not yet sound: a player reading a broadcast published that
/// way skips about one audio group in four, in the shape of several
/// groups appended inside one host call. A subscription's latency
/// window defaults to zero - a group that is no longer the latest when
/// a reader reaches it is skipped rather than served - and several
/// groups finished back to back give a reader that chance. Nothing
/// here reproduces it against a local relay, so it is offered and not
/// recommended; see the README.
pub const AUDIO_GROUP_MS: u32 = 200;

/// What the open group is measured against.
enum Rule {
	/// A keyframe closes the group before it.
	Keyframe,
	/// Every fragment is a group of its own.
	Fragment,
	/// This many stream ticks past the group's first timestamp closes
	/// it.
	Elapsed(i64),
}

/// One track's group cursor: fed each fragment in the order it is
/// published, it says which of them opens a group.
pub struct Groups {
	rule: Rule,
	/// The open group's first presentation timestamp; None before the
	/// first fragment.
	start_pts: Option<i64>,
}

impl Groups {
	/// The discipline for a video track: a group at every keyframe.
	pub fn video() -> Self {
		Self {
			rule: Rule::Keyframe,
			start_pts: None,
		}
	}

	/// The discipline for an audio track: a group per frame for
	/// `group_ms` of 0 ([`AUDIO_GROUP_MS`]), else a group once that many
	/// milliseconds have elapsed, which the track's own time base counts
	/// in ticks. A duration shorter than one frame is a frame per group
	/// by another name, and reads as one.
	pub fn audio(time_base_num: i32, time_base_den: i32, group_ms: u32) -> Self {
		let rule = match group_ms {
			0 => Rule::Fragment,
			ms => {
				let ticks = i64::from(ms) * i64::from(time_base_den.max(1))
					/ (i64::from(time_base_num.max(1)) * 1000);
				Rule::Elapsed(ticks.max(1))
			}
		};
		Self {
			rule,
			start_pts: None,
		}
	}

	/// Whether this track's groups hold exactly one fragment, which is
	/// what lets a publisher finish a group in the call that wrote it
	/// rather than when the next fragment rotates it. A group nobody has
	/// finished is a group a reader waits on.
	pub fn one_fragment_each(&self) -> bool {
		matches!(self.rule, Rule::Fragment)
	}

	/// Whether the fragment described by `keyframe` and `pts` opens a
	/// group, and opens it if it does. Fragments are offered in the
	/// order they are published.
	pub fn starts_a_group(&mut self, keyframe: bool, pts: i64) -> bool {
		let opens = match (self.start_pts, &self.rule) {
			(None, _) => true,
			(Some(_), Rule::Keyframe) => keyframe,
			(Some(_), Rule::Fragment) => true,
			(Some(start), Rule::Elapsed(ticks)) => pts - start >= *ticks,
		};
		if opens {
			self.start_pts = Some(pts);
		}
		opens
	}
}

/// Where a reader may start, once it has joined a broadcast already
/// running.
///
/// A video track's first fragments may sit past the keyframe their
/// group opened with - a group served from part way through, a
/// publisher that cuts its groups somewhere else, a track taken up
/// again after the wire broke. Those samples are a picture no decoder
/// can begin in, so they are absorbed rather than passed on. Audio has
/// no such gate: every AAC frame can be decoded from.
///
/// It is the LAST gate, not the first. Which group a reader starts at
/// is [`crate::order::Queue`]'s, and on a live join it already picks
/// one whose first frame is a keyframe; see
/// [`crate::subscribe::Start`].
pub struct Join {
	started: bool,
}

impl Join {
	/// A video track, which waits for its first sync sample.
	pub fn video() -> Self {
		Self { started: false }
	}

	/// An audio track, which starts at whatever arrives first.
	pub fn audio() -> Self {
		Self { started: true }
	}

	/// The samples of one fragment that this package will hand on, in
	/// decode order.
	///
	/// A fragment that carries nothing for the track - a `moof` whose
	/// track fragments all belong to somebody else - holds no samples
	/// and is not a failure. It yields nothing and is skipped: what a
	/// reader counts is the group sequence, which such a fragment does
	/// not disturb.
	pub fn playable(
		&mut self,
		track: &ffrwd_bmff::track::Track,
		fragment: &[u8],
	) -> ffrwd_bmff::Result<Vec<ffrwd_bmff::track::Sample>> {
		let mut out = Vec::new();
		for sample in track.fragment_samples(fragment)? {
			if !self.started {
				if !sample.keyframe {
					continue;
				}
				self.started = true;
			}
			out.push(sample);
		}
		Ok(out)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use ffrwd_bmff::mux::{Muxer, Packet, Video};
	use ffrwd_bmff::track::Track;

	/// A constrained-baseline record, which is opaque to the muxer.
	const AVCC: &[u8] = &[
		1, 66, 0xc0, 30, 0xff, 0xe1, 0, 4, 0x67, 66, 0xc0, 30, 1, 0, 2, 0x68, 0xee,
	];

	fn muxer(track_id: u32) -> Muxer {
		Muxer::video(
			Video {
				kind: *b"avc1",
				width: 320,
				height: 180,
				config: AVCC.to_vec(),
			},
			1,
			30,
		)
		.expect("a muxer")
		.with_track_id(track_id)
	}

	/// One fragment apiece for `keyframes`, as the publish path emits
	/// them: one sample to a fragment.
	fn fragments(muxer: &mut Muxer, keyframes: &[bool]) -> Vec<Vec<u8>> {
		let mut out = Vec::new();
		for (index, keyframe) in keyframes.iter().enumerate() {
			out.extend(
				muxer
					.push(Packet {
						pts: index as i64,
						dts: Some(index as i64),
						duration: Some(1),
						keyframe: *keyframe,
						data: &[0, 0, 0, 2, 0x65, index as u8],
					})
					.expect("push")
					.into_iter()
					.map(|fragment| fragment.bytes),
			);
		}
		out.extend(
			muxer
				.finish()
				.expect("finish")
				.into_iter()
				.map(|fragment| fragment.bytes),
		);
		out
	}

	#[test]
	fn a_fragment_that_carries_nothing_for_the_track_is_skipped() {
		// A moof whose only traf names track 2, read by track 1: an
		// empty sample list, which is a fragment to step over rather
		// than a stream to give up on.
		let track = Track::from_init(&muxer(1).init_segment()).expect("an init segment");
		let theirs = fragments(&mut muxer(2), &[true, false]);
		let mut join = Join::video();
		for fragment in &theirs {
			assert!(
				join.playable(&track, fragment).expect("no failure").is_empty(),
				"somebody else's track fragment yielded samples"
			);
		}
		// And the track's own fragments still read, after the skip.
		let ours = fragments(&mut muxer(1), &[true]);
		assert_eq!(join.playable(&track, &ours[0]).expect("read").len(), 1);
	}

	#[test]
	fn a_video_reader_absorbs_what_it_cannot_start_at() {
		let track = Track::from_init(&muxer(1).init_segment()).expect("an init segment");
		let stream = fragments(&mut muxer(1), &[false, false, true, false]);
		let mut join = Join::video();
		let counts: Vec<usize> = stream
			.iter()
			.map(|fragment| join.playable(&track, fragment).expect("read").len())
			.collect();
		assert_eq!(counts, [0, 0, 1, 1], "the samples before the keyframe go");

		// An audio reader starts wherever it joined.
		let mut audio = Join::audio();
		let counts: Vec<usize> = stream
			.iter()
			.map(|fragment| audio.playable(&track, fragment).expect("read").len())
			.collect();
		assert_eq!(counts, [1, 1, 1, 1]);
	}

	#[test]
	fn a_video_group_opens_at_the_first_fragment_and_every_keyframe() {
		let mut groups = Groups::video();
		let starts: Vec<bool> = (0..4i64)
			.map(|pts| groups.starts_a_group(pts % 3 == 0, pts))
			.collect();
		assert_eq!(starts, [true, false, false, true]);
	}

	#[test]
	fn an_all_intra_video_stream_makes_every_frame_its_own_group() {
		// The shape that survives a mid-group join on a relay which
		// replays nothing: a subscriber can only ever join at a group
		// start, so every frame being one is the point.
		let mut groups = Groups::video();
		assert!((0..5i64).all(|pts| groups.starts_a_group(true, pts)));
	}

	#[test]
	fn the_default_audio_group_is_a_fifth_of_a_second() {
		// Ten AAC frames at 48 kHz, which is what the live loop counts.
		let mut groups = Groups::audio(1, 48000, AUDIO_GROUP_MS);
		let starts: Vec<usize> = (0..31i64)
			.filter(|index| groups.starts_a_group(true, index * 1024))
			.map(|index| index as usize)
			.collect();
		assert_eq!(starts, [0, 10, 20, 30]);
	}

	#[test]
	fn a_zero_duration_is_a_group_per_frame() {
		let mut groups = Groups::audio(1, 48000, 0);
		assert!((0..50i64).all(|index| groups.starts_a_group(true, index * 1024)));
	}

	#[test]
	fn an_audio_group_starts_past_the_target_duration() {
		// One tick per sample at 48 kHz, 1024 samples an AAC frame: the
		// 47th frame is the first whose timestamp stands a whole second
		// past the group's first, so it is the one that opens the next
		// group.
		let mut groups = Groups::audio(1, 48000, 1000);
		let starts: Vec<usize> = (0..49i64)
			.filter(|index| groups.starts_a_group(true, index * 1024))
			.map(|index| index as usize)
			.collect();
		assert_eq!(starts, [0, 47]);
		// A hundred milliseconds is five frames, which is the rung
		// between a frame per group and a second of them.
		let mut hundred = Groups::audio(1, 48000, 100);
		let starts: Vec<usize> = (0..21i64)
			.filter(|index| hundred.starts_a_group(true, index * 1024))
			.map(|index| index as usize)
			.collect();
		assert_eq!(starts, [0, 5, 10, 15, 20]);
	}

	#[test]
	fn an_audio_time_base_with_a_numerator_still_measures_a_second() {
		// A time base of 1001/48000 counts fewer ticks to the second,
		// and a degenerate one still closes a group eventually.
		let mut groups = Groups::audio(1001, 48000, 1000);
		assert!(groups.starts_a_group(true, 0));
		assert!(!groups.starts_a_group(true, 46));
		assert!(groups.starts_a_group(true, 48));
		let mut degenerate = Groups::audio(1_000_000, 1, 1000);
		assert!(degenerate.starts_a_group(true, 0));
		assert!(degenerate.starts_a_group(true, 1), "a group must still close");
		// A target shorter than a frame is a frame per group.
		let mut fine = Groups::audio(1, 48000, 1);
		assert!(fine.starts_a_group(true, 0));
		assert!(fine.starts_a_group(true, 1024));
	}
}

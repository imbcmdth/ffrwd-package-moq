//! A text rendition: cue rows in at publish, WebVTT on the wire, cue rows
//! out at subscribe.
//!
//! A cue row is what ffrwd's caption producers write and its caption
//! consumers read: `text`, `start_t` and `end_t`, seconds on the program's
//! timeline, the text WebVTT cue text. A streaming producer (parakeet's
//! `transcribe`) also writes `final`, false on each row of an utterance
//! that is still growing and true on the row that ends it, and a row may
//! name its speaker in `voice`.
//!
//! On the wire the rendition is a WebVTT track in fragmented MP4 (ISO/IEC
//! 14496-30, the text track CMAF carries): its init segment in the catalog,
//! and each MoQ group one fragment of one sample. A sample covers a span of
//! the timeline and carries every cue shown over it, so the samples tile
//! the timeline with no overlap, a cue that runs across several samples is
//! carried in each one, and a span with nothing shown is an empty sample.
//! See [`ffrwd_bmff::webvtt`].
//!
//! [`Cutter`] is the publisher's half. Only a finished cue belongs in a
//! subtitle track, since each growing row of an utterance would be another
//! cue over the same words, so it takes rows with no `final` and rows with
//! `final` true, and counts the growing ones it passes over. A cue is known
//! only once it has finished, which is after it started, so the track's
//! timeline is held [`HOLD_MS`] behind the newest time the publisher has
//! seen: a cue that arrives within that is placed at its own start, and one
//! that arrives later loses the part of it that has already gone out.
//! Samples are cut where a cue starts or ends and at least every
//! [`SAMPLE_MAX_MS`], so a reader that joins at the newest group has the
//! cues on screen from its first sample. A span with nothing shown that has
//! settled for longer than two samples' worth at once, after a jump in the
//! publisher's time, is one sample. A cue's text is made safe for a `.vtt`
//! made from the track by [`ffrwd_bmff::webvtt::cue_text`].
//!
//! [`Reassembly`] is the subscriber's half. It hands each cue on as a
//! streaming producer would: a row at every sample that carries it, the
//! same `start_t` each time and `end_t` the end of that sample, with
//! `final` false, and a last row with `final` true once a sample comes
//! without it. A consumer that reads streaming rows (cea608's `cues`,
//! compose's `cues`) has the text from the cue's first sample, and one
//! that wants whole cues reads the rows with `final` true, which are the
//! rows that went in.

use serde::{Deserialize, Serialize};

pub use ffrwd_bmff::webvtt::Cue;

/// The codec string of a WebVTT track in an ISO file, as DASH and Shaka
/// name it (`application/mp4; codecs="wvtt"`).
pub const CODEC: &str = "wvtt";

/// The `vttC` header the track's sample entry carries.
pub const HEADER: &[u8] = b"WEBVTT";

/// The track's ticks per second: milliseconds, which is what a WebVTT
/// timestamp counts in.
pub const TIMESCALE: u32 = 1000;

/// The longest one sample covers, in milliseconds, while the track keeps
/// pace with its publisher.
pub const SAMPLE_MAX_MS: i64 = 1000;

/// How far behind the newest time seen the text track's timeline is held,
/// in milliseconds, unless a publisher says otherwise. Measured against
/// ffrwd/parakeet's `transcribe` over read and synthesized speech: a final
/// row is written at most 1.28 s after its last word, which this holds
/// with room, and a cue written after its own start keeps 1.6 s or more of
/// screen time. See the README, "Text tracks".
pub const HOLD_MS: u64 = 3000;

/// What a row input of cue rows declares.
pub const ROW_SCHEMA: &str = r#"{"type":"object","properties":{"text":{"type":"string"},"start_t":{"type":"number"},"end_t":{"type":"number"},"final":{"type":"boolean"},"voice":{"type":"string"}},"required":["text","start_t","end_t"]}"#;

/// One cue row.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Row {
	pub text: String,
	pub start_t: f64,
	pub end_t: f64,
	/// A streaming producer's: false while the utterance grows.
	#[serde(default, rename = "final", skip_serializing_if = "Option::is_none")]
	pub finished: Option<bool>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub voice: Option<String>,
}

/// One sample for the track: its start and span in [`TIMESCALE`] ticks,
/// and the cues shown over it.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
	pub start: i64,
	pub duration: i64,
	pub cues: Vec<Cue>,
}

/// What became of one row handed to [`Cutter::row`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Taken {
	/// A finished cue, placed at its own start.
	Placed,
	/// A finished cue that came after part of it had gone out: placed from
	/// where the track had got.
	Clipped,
	/// A finished cue that ended before where the track had got.
	Late,
	/// A row of an utterance still growing, which a subtitle track does not
	/// carry.
	Growing,
	/// A cue that ends at or before it starts, whose times are not times, or
	/// whose text is nothing once its empty lines are gone.
	Empty,
}

/// A cue waiting to be written, in ticks.
#[derive(Clone, Debug)]
struct Pending {
	start: i64,
	end: i64,
	cue: Cue,
}

/// The publisher's half: rows in, samples out.
#[derive(Debug)]
pub struct Cutter {
	hold: i64,
	/// The newest time seen on any input, in ticks.
	now: Option<i64>,
	/// The first time seen, which the track starts at when no cue starts
	/// earlier.
	first: Option<i64>,
	/// Where the samples written so far end.
	written: Option<i64>,
	pending: Vec<Pending>,
	next_source: u32,
}

/// Seconds to ticks, rounded to the nearest; None for a time that is not
/// one.
fn ticks(seconds: f64) -> Option<i64> {
	let ticks = (seconds * f64::from(TIMESCALE)).round();
	(ticks.is_finite() && ticks.abs() < 9.0e15).then_some(ticks as i64)
}

/// A cue's payload: its text, inside a `<v>` span when it names a voice.
fn payload(text: &str, voice: Option<&str>) -> String {
	match voice {
		Some(voice) => format!("<v {}>{text}", escape(voice)),
		None => text.to_string(),
	}
}

/// A voice's name as a `<v>` annotation spells it: on one line, with `&`,
/// `<` and `>` escaped.
fn escape(text: &str) -> String {
	text.replace(['\r', '\n'], " ")
		.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
}

fn unescape(text: &str) -> String {
	text.replace("&lt;", "<")
		.replace("&gt;", ">")
		.replace("&amp;", "&")
}

/// A payload back to its text and voice: a payload that opens with a
/// `<v Name>` span over the whole of it, with no class on the tag, gives
/// the name back as the voice. Anything else is text as it stands.
fn voiced(payload: &str) -> (String, Option<String>) {
	let Some(rest) = payload.strip_prefix("<v ") else {
		return (payload.to_string(), None);
	};
	let Some(close) = rest.find('>') else {
		return (payload.to_string(), None);
	};
	let (name, text) = (&rest[..close], &rest[close + 1..]);
	let text = text.strip_suffix("</v>").unwrap_or(text);
	if name.trim().is_empty() || text.contains("<v") || text.contains("</v>") {
		return (payload.to_string(), None);
	}
	(text.to_string(), Some(unescape(name.trim())))
}

impl Cutter {
	pub fn new(hold_ms: u64) -> Self {
		Self {
			hold: i64::try_from(hold_ms).unwrap_or(i64::MAX / 4),
			now: None,
			first: None,
			written: None,
			pending: Vec::new(),
			next_source: 1,
		}
	}

	/// A time the publisher has seen on one of its inputs, in seconds on
	/// the program's timeline: a media packet's, or a row's own.
	pub fn heard(&mut self, seconds: f64) {
		let Some(at) = ticks(seconds) else {
			return;
		};
		self.now = Some(self.now.map_or(at, |now| now.max(at)));
		self.first.get_or_insert(at);
	}

	/// One row, taken or passed over.
	pub fn row(&mut self, row: &Row) -> Taken {
		if row.finished == Some(false) {
			return Taken::Growing;
		}
		let (Some(mut start), Some(end)) = (ticks(row.start_t), ticks(row.end_t)) else {
			return Taken::Empty;
		};
		let text = ffrwd_bmff::webvtt::cue_text(&row.text);
		if end <= start || text.is_empty() {
			return Taken::Empty;
		}
		let mut taken = Taken::Placed;
		if let Some(written) = self.written {
			if end <= written {
				return Taken::Late;
			}
			if start < written {
				start = written;
				taken = Taken::Clipped;
			}
		}
		let source = self.next_source;
		self.next_source = self.next_source.wrapping_add(1).max(1);
		self.pending.push(Pending {
			start,
			end,
			cue: Cue {
				source_id: Some(source),
				payload: payload(&text, row.voice.as_deref()),
				..Cue::default()
			},
		});
		taken
	}

	/// The samples whose span has settled: up to [`HOLD_MS`] behind the
	/// newest time seen, or with `last`, everything, to the end of the last
	/// cue.
	pub fn ready(&mut self, last: bool) -> Vec<Sample> {
		let Some(now) = self.now.or_else(|| self.pending.iter().map(|p| p.end).max()) else {
			return Vec::new();
		};
		let settled = match last {
			true => self.pending.iter().map(|p| p.end).fold(now, i64::max),
			false => now - self.hold,
		};
		let mut at = match self.written {
			Some(written) => written,
			None => {
				let earliest = self.pending.iter().map(|p| p.start).min();
				match (self.first, earliest) {
					(Some(first), Some(cue)) => first.min(cue),
					(Some(first), None) => first,
					(None, Some(cue)) => cue,
					(None, None) => return Vec::new(),
				}
			}
		};
		let step = SAMPLE_MAX_MS * i64::from(TIMESCALE) / 1000;
		let mut out = Vec::new();
		while at < settled {
			self.pending.retain(|p| p.end > at);
			let mut shown: Vec<&Pending> = self
				.pending
				.iter()
				.filter(|p| p.start <= at && p.end > at)
				.collect();
			let next = self
				.pending
				.iter()
				.flat_map(|p| [p.start, p.end])
				.filter(|&t| t > at)
				.min()
				.unwrap_or(i64::MAX);
			let quiet = next.min(settled);
			let end = if shown.is_empty() && quiet - at >= 2 * step {
				quiet
			} else {
				match next.min(at + step) {
					boundary if boundary <= settled => boundary,
					_ if last => settled,
					_ => break,
				}
			};
			shown.sort_by_key(|p| (p.start, p.cue.source_id));
			out.push(Sample {
				start: at,
				duration: end - at,
				cues: shown.into_iter().map(|p| p.cue.clone()).collect(),
			});
			at = end;
		}
		if !out.is_empty() {
			self.written = Some(at);
			self.pending.retain(|p| p.end > at);
		}
		out
	}
}

/// One row as the subscriber hands it on, its times in [`TIMESCALE`]
/// ticks: `pts` is when it applies, the start of the sample that carried
/// it for a growing row and the cue's end for its final one.
#[derive(Clone, Debug, PartialEq)]
pub struct Out {
	pub pts: i64,
	pub text: String,
	pub voice: Option<String>,
	pub start: i64,
	pub end: i64,
	pub finished: bool,
}

impl Out {
	/// The row's JSON, its times in seconds, moved by `shift` ticks.
	pub fn json(&self, shift: i64, timescale: u32) -> Vec<u8> {
		let seconds = |ticks: i64| (ticks + shift) as f64 / f64::from(timescale.max(1));
		serde_json::to_vec(&Row {
			text: self.text.clone(),
			start_t: seconds(self.start),
			end_t: seconds(self.end),
			finished: Some(self.finished),
			voice: self.voice.clone(),
		})
		.expect("a cue row serializes")
	}
}

/// A cue being followed across the samples that carry it.
#[derive(Clone, Debug)]
struct Open {
	cue: Cue,
	start: i64,
	end: i64,
}

impl Open {
	fn same(&self, cue: &Cue) -> bool {
		match (self.cue.source_id, cue.source_id) {
			(Some(was), Some(now)) => was == now && self.cue.payload == cue.payload,
			_ => self.cue.payload == cue.payload,
		}
	}

	fn row(&self, pts: i64, end: i64, finished: bool) -> Out {
		let (text, voice) = voiced(&self.cue.payload);
		Out {
			pts,
			text,
			voice,
			start: self.start,
			end,
			finished,
		}
	}
}

/// The subscriber's half: samples in, rows out.
#[derive(Debug, Default)]
pub struct Reassembly {
	open: Vec<Open>,
	/// Where the last sample read ended.
	end: Option<i64>,
}

impl Reassembly {
	/// One sample's rows: a final row for each cue it no longer carries, and
	/// a growing row for each it does. A sample that does not start where
	/// the last one ended, a join or a gap or a publisher that started
	/// again, ends every cue first, at the end of the sample it was last in.
	pub fn sample(&mut self, start: i64, duration: i64, cues: Vec<Cue>) -> Vec<Out> {
		let mut out = Vec::new();
		if self.end != Some(start) {
			out.extend(self.finish());
		}
		let end = start + duration.max(0);
		let mut open = Vec::with_capacity(cues.len());
		for cue in cues {
			let found = self.open.iter().position(|was| was.same(&cue));
			let mut followed = match found {
				Some(index) => self.open.remove(index),
				None => Open {
					cue,
					start,
					end,
				},
			};
			followed.end = end;
			open.push(followed);
		}
		for gone in std::mem::take(&mut self.open) {
			out.push(gone.row(gone.end, gone.end, true));
		}
		out.extend(open.iter().map(|cue| cue.row(start, end, false)));
		self.open = open;
		self.end = Some(end);
		out
	}

	/// A final row for every cue still open: the track ended, or what comes
	/// next is another publisher's or does not follow on.
	pub fn finish(&mut self) -> Vec<Out> {
		self.end = None;
		std::mem::take(&mut self.open)
			.into_iter()
			.map(|cue| cue.row(cue.end, cue.end, true))
			.collect()
	}
}

/// A row's JSON with its times moved by `seconds`, for a row that already
/// went out under one shift and is let out under another.
pub fn shifted(json: &[u8], seconds: f64) -> Result<Vec<u8>, String> {
	let mut row: Row = serde_json::from_slice(json).map_err(|err| format!("a cue row: {err}"))?;
	row.start_t += seconds;
	row.end_t += seconds;
	serde_json::to_vec(&row).map_err(|err| format!("a cue row: {err}"))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(text: &str, start_t: f64, end_t: f64) -> Row {
		Row {
			text: text.into(),
			start_t,
			end_t,
			finished: Some(true),
			voice: None,
		}
	}

	/// Every sample, with each cue's payload, as `(start, duration,
	/// payloads)`.
	fn shape(samples: &[Sample]) -> Vec<(i64, i64, Vec<String>)> {
		samples
			.iter()
			.map(|s| {
				(
					s.start,
					s.duration,
					s.cues.iter().map(|c| c.payload.clone()).collect(),
				)
			})
			.collect()
	}

	#[test]
	fn the_samples_tile_the_timeline_and_carry_each_cue_where_it_shows() {
		let mut cutter = Cutter::new(0);
		cutter.heard(0.0);
		assert_eq!(cutter.row(&row("a", 1.0, 3.0)), Taken::Placed);
		assert_eq!(cutter.row(&row("b", 2.0, 4.5)), Taken::Placed);
		cutter.heard(5.0);
		assert_eq!(
			shape(&cutter.ready(false)),
			vec![
				(0, 1000, vec![]),
				(1000, 1000, vec!["a".into()]),
				(2000, 1000, vec!["a".into(), "b".into()]),
				(3000, 1000, vec!["b".into()]),
				(4000, 500, vec!["b".into()]),
			]
		);
		// The span after the last cue is not settled until the next cue or a
		// whole sample's length has passed.
		cutter.heard(5.5);
		assert_eq!(shape(&cutter.ready(false)), vec![(4500, 1000, vec![])]);
	}

	#[test]
	fn a_quiet_track_keeping_pace_cuts_a_sample_a_second() {
		let mut cutter = Cutter::new(2000);
		let mut written = Vec::new();
		for tick in 0..=500 {
			cutter.heard(f64::from(tick) * 0.02);
			written.extend(cutter.ready(false));
		}
		assert_eq!(written.len(), 8);
		assert!(written.iter().all(|s| s.duration == 1000 && s.cues.is_empty()));
	}

	#[test]
	fn a_jump_in_time_over_nothing_shown_is_one_sample() {
		let mut cutter = Cutter::new(2000);
		cutter.heard(0.0);
		cutter.heard(2.5);
		assert_eq!(shape(&cutter.ready(false)), Vec::<(i64, i64, Vec<String>)>::new());
		// Twenty seconds arrive at once, a cue inside them: the quiet spans
		// either side go out whole, the cue a sample a second as ever.
		assert_eq!(cutter.row(&row("in the jump", 9.0, 11.5)), Taken::Placed);
		cutter.heard(22.5);
		assert_eq!(
			shape(&cutter.ready(false)),
			vec![
				(0, 9000, vec![]),
				(9000, 1000, vec!["in the jump".into()]),
				(10000, 1000, vec!["in the jump".into()]),
				(11000, 500, vec!["in the jump".into()]),
				(11500, 9000, vec![]),
			]
		);
		// And keeping pace again, a second at a time.
		cutter.heard(23.5);
		assert_eq!(shape(&cutter.ready(false)), vec![(20500, 1000, vec![])]);
	}

	#[test]
	fn the_timeline_waits_out_the_hold_and_a_late_cue_keeps_what_is_left() {
		let mut cutter = Cutter::new(2000);
		cutter.heard(0.0);
		cutter.heard(3.5);
		// Only what is two seconds behind the newest time has settled.
		assert_eq!(shape(&cutter.ready(false)), vec![(0, 1000, vec![])]);
		// A cue that started 2.5 s ago is placed at its start.
		assert_eq!(cutter.row(&row("in time", 1.0, 2.5)), Taken::Placed);
		cutter.heard(6.0);
		assert_eq!(
			shape(&cutter.ready(false)),
			vec![
				(1000, 1000, vec!["in time".into()]),
				(2000, 500, vec!["in time".into()]),
				(2500, 1000, vec![]),
			]
		);
		assert_eq!(cutter.row(&row("too late", 1.0, 3.5)), Taken::Late);
		assert_eq!(cutter.row(&row("partly", 3.0, 5.0)), Taken::Clipped);
		assert_eq!(
			shape(&cutter.ready(true)),
			vec![
				(3500, 1000, vec!["partly".into()]),
				(4500, 500, vec!["partly".into()]),
				(5000, 1000, vec![]),
			]
		);
	}

	#[test]
	fn a_growing_row_is_passed_over_and_its_final_row_is_the_cue() {
		let mut cutter = Cutter::new(0);
		let growing = Row {
			finished: Some(false),
			..row("he goes", 0.32, 0.4)
		};
		assert_eq!(cutter.row(&growing), Taken::Growing);
		let whole = Row {
			finished: None,
			..row("he goes for the flank", 0.32, 1.12)
		};
		assert_eq!(cutter.row(&whole), Taken::Placed);
		assert_eq!(cutter.row(&row("nothing", 2.0, 2.0)), Taken::Empty);
		assert_eq!(
			shape(&cutter.ready(true)),
			vec![(320, 800, vec!["he goes for the flank".into()])]
		);
	}

	#[test]
	fn text_a_vtt_file_could_not_hold_is_written_so_it_can() {
		let mut cutter = Cutter::new(0);
		cutter.heard(0.0);
		let rows = [
			row("first line\n\nsecond line", 1.0, 2.0),
			row("left --> right", 2.0, 3.0),
			row("\r\n \n", 3.0, 4.0),
			Row {
				voice: Some("Two\nLines".into()),
				..row("x", 4.0, 5.0)
			},
		];
		let taken: Vec<Taken> = rows.iter().map(|r| cutter.row(r)).collect();
		assert_eq!(
			taken,
			[Taken::Placed, Taken::Placed, Taken::Empty, Taken::Placed]
		);
		let payloads: Vec<String> = cutter
			.ready(true)
			.into_iter()
			.flat_map(|s| s.cues)
			.map(|c| c.payload)
			.collect();
		assert_eq!(
			payloads,
			["first line\nsecond line", "left --&gt; right", "<v Two Lines>x"]
		);
	}

	#[test]
	fn a_voice_rides_as_a_v_span_and_comes_back_as_the_voice() {
		assert_eq!(
			payload("he goes", Some("Mango & <Co>")),
			"<v Mango &amp; &lt;Co&gt;>he goes"
		);
		assert_eq!(
			voiced("<v Mango &amp; &lt;Co&gt;>he goes"),
			("he goes".into(), Some("Mango & <Co>".into()))
		);
		assert_eq!(
			voiced("<v Ludwig>what a read</v>"),
			("what a read".into(), Some("Ludwig".into()))
		);
		// A class on the tag, or a second voice, stays text.
		for kept in ["<v.loud Mango>hi", "<v A>hi <v B>there", "<i>hi</i>"] {
			assert_eq!(voiced(kept), (kept.into(), None));
		}
	}

	#[test]
	fn a_cue_across_samples_comes_back_once_with_its_own_times() {
		let mut cutter = Cutter::new(0);
		cutter.heard(0.0);
		let rows = vec![
			Row {
				voice: Some("Mango".into()),
				..row("he goes for the flank", 1.0, 3.0)
			},
			row("what a read", 2.0, 4.5),
		];
		for row in &rows {
			cutter.row(row);
		}
		let mut reader = Reassembly::default();
		let mut out = Vec::new();
		for sample in cutter.ready(true) {
			out.extend(reader.sample(sample.start, sample.duration, sample.cues));
		}
		out.extend(reader.finish());
		let finals: Vec<Row> = out
			.iter()
			.filter(|o| o.finished)
			.map(|o| serde_json::from_slice(&o.json(0, TIMESCALE)).unwrap())
			.collect();
		assert_eq!(finals, rows, "the rows that went in");
		// Each final row leaves at its cue's end; every row before it at the
		// start of a sample that carried the cue, with the cue's own start.
		assert!(out.windows(2).all(|pair| pair[0].pts <= pair[1].pts));
		let first = out.iter().find(|o| o.text == "what a read").unwrap();
		assert_eq!(
			(first.pts, first.start, first.end, first.finished),
			(2000, 2000, 3000, false)
		);
	}

	#[test]
	fn a_reader_that_joins_or_meets_a_gap_ends_what_it_had() {
		let mut reader = Reassembly::default();
		let cue = Cue {
			source_id: Some(4),
			payload: "late joiner".into(),
			..Cue::default()
		};
		// Joined part way through a cue: it starts where the reader did.
		let rows = reader.sample(7000, 1000, vec![cue.clone()]);
		assert_eq!(
			(rows[0].start, rows[0].end, rows[0].finished),
			(7000, 8000, false)
		);
		// A sample that does not follow on ends it where it was last seen.
		let rows = reader.sample(9500, 500, vec![cue]);
		assert_eq!(
			rows.iter()
				.map(|o| (o.pts, o.start, o.end, o.finished))
				.collect::<Vec<_>>(),
			vec![(8000, 7000, 8000, true), (9500, 9500, 10000, false)]
		);
	}

	#[test]
	fn a_shifted_row_moves_both_its_times() {
		let json = serde_json::to_vec(&row("x", 1.0, 2.0)).unwrap();
		let moved: Row = serde_json::from_slice(&shifted(&json, 10.0).unwrap()).unwrap();
		assert_eq!((moved.start_t, moved.end_t), (11.0, 12.0));
	}
}

//! Writes cue rows as the WebVTT track a text rendition carries, init
//! segment and then one fragment per sample, so another reader (ffprobe,
//! a DASH player) can be pointed at the same bytes publish puts on the
//! wire.
//!
//!     cargo run -p moq-core --example cues_mp4 < cues.ndjson > cues.mp4
//!
//! Each line of stdin is one cue row: `{"text":..,"start_t":..,"end_t":..}`,
//! with `final` and `voice` where it has them. Blank lines are skipped.

use std::io::{self, BufRead, Write};

use moq_core::text::{Cutter, Row, HEADER, TIMESCALE};

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let mut cutter = Cutter::new(0);
	for line in io::stdin().lock().lines() {
		let line = line?;
		if line.trim().is_empty() {
			continue;
		}
		let row: Row = serde_json::from_str(&line)?;
		cutter.heard(row.start_t);
		cutter.row(&row);
	}
	let mut muxer = ffrwd_bmff::mux::Muxer::text(
		ffrwd_bmff::mux::Text {
			config: HEADER.to_vec(),
		},
		1,
		TIMESCALE as i32,
	)?;
	let mut out = muxer.init_segment();
	for sample in cutter.ready(true) {
		let bytes = ffrwd_bmff::webvtt::sample(&sample.cues);
		for fragment in muxer.push(ffrwd_bmff::mux::Packet {
			pts: sample.start,
			dts: Some(sample.start),
			duration: Some(sample.duration),
			keyframe: true,
			data: &bytes,
		})? {
			out.extend_from_slice(&fragment.bytes);
		}
	}
	io::stdout().write_all(&out)?;
	Ok(())
}

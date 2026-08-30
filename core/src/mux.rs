//! Fragmented-MP4 construction from encoded h264 packets.
//!
//! The inverse of [`crate::fmp4`]: where that module cuts a muxed byte
//! stream into segments, this one builds the segments from the packets
//! an encoder emits. One [`Muxer`] serves one coded stream: its
//! [`init_segment`](Muxer::init_segment) is the `ftyp`+`moov` a decoder
//! needs before any sample - the `avcC` built from the stream's
//! out-of-band SPS/PPS - and each [`push`](Muxer::push) collects one
//! packet into the open group of pictures, closing the group into a
//! `moof`+`mdat` fragment when the next keyframe arrives. Samples are
//! stored AVCC-framed (4-byte NAL lengths), reframed from the Annex-B
//! bytes the packets carry.
//!
//! Timestamps: the media timescale is the stream time base's
//! denominator, so a tick value crosses unchanged when the numerator is
//! 1 and is scaled by it otherwise. A stream that reorders frames may
//! open with negative decode times; the whole decode timeline is
//! shifted up once so the first fragment starts at zero, and
//! presentation offsets carry the reordering per sample. The first
//! packets of such a stream may arrive with no decode time at all -
//! the wire does not settle it - and get one synthesized backwards from
//! the first settled one at the fragment's own step.

use crate::avc;

/// One encoded packet, as the edge hands it over.
pub struct Packet<'a> {
	/// Presentation timestamp in stream time-base ticks.
	pub pts: i64,
	/// Decode timestamp; absent for the first packets of a reordering
	/// stream.
	pub dts: Option<i64>,
	/// Whether decoding can start at this packet.
	pub keyframe: bool,
	/// The encoded bytes, Annex-B.
	pub data: &'a [u8],
}

/// One closed group of pictures: a `moof`+`mdat` fragment.
#[derive(Debug)]
pub struct Fragment {
	/// The fragment bytes, ready for the wire.
	pub bytes: Vec<u8>,
	/// The `mfhd` sequence number, from 1.
	pub sequence: u32,
	/// Whether the first sample is a sync sample.
	pub keyframe: bool,
	/// Samples in the fragment.
	pub samples: u64,
	/// Smallest presentation timestamp, in stream ticks as fed.
	pub pts_min: i64,
	/// Largest presentation timestamp, in stream ticks as fed.
	pub pts_max: i64,
}

struct Sample {
	data: Vec<u8>,
	pts: i64,
	dts: Option<i64>,
	keyframe: bool,
}

/// Builds an init segment once and fragments as packets arrive.
pub struct Muxer {
	timescale: u32,
	/// Ticks multiply by this on the way to media time (the time
	/// base's numerator).
	tick_scale: i64,
	width: u32,
	height: u32,
	avcc: Vec<u8>,
	sequence: u32,
	pending: Vec<Sample>,
	/// Added to every decode time; set when the first fragment closes.
	dts_shift: Option<i64>,
	/// The last computed sample duration, for the stream's final sample.
	last_duration: Option<i64>,
}

impl Muxer {
	/// A muxer for one coded stream: Annex-B SPS/PPS extradata, the
	/// declared frame size, and the stream time base.
	pub fn new(
		extradata: &[u8],
		width: u32,
		height: u32,
		time_base_num: i32,
		time_base_den: i32,
	) -> Result<Self, String> {
		if time_base_num <= 0 || time_base_den <= 0 {
			return Err(format!(
				"time base {time_base_num}/{time_base_den} is not positive"
			));
		}
		let (sps, pps) = avc::parse_parameter_sets(extradata);
		let avcc = avc::build_avcc(&sps, &pps)?;
		Ok(Self {
			timescale: time_base_den as u32,
			tick_scale: time_base_num as i64,
			width,
			height,
			avcc,
			sequence: 1,
			pending: Vec::new(),
			dts_shift: None,
			last_duration: None,
		})
	}

	/// The `avcC` payload built from the extradata, for inspection.
	pub fn avcc(&self) -> &[u8] {
		&self.avcc
	}

	/// The `ftyp`+`moov` a decoder reads before any fragment.
	pub fn init_segment(&self) -> Vec<u8> {
		let mut out = ftyp();
		out.extend_from_slice(&self.moov());
		out
	}

	/// Collects one packet; a keyframe closes the open group first, and
	/// the closed fragment comes back.
	pub fn push(&mut self, packet: Packet<'_>) -> Result<Option<Fragment>, String> {
		let data = avc::annexb_to_avcc(packet.data);
		if data.is_empty() {
			return Err(format!(
				"the packet at pts {} carries no Annex-B start code",
				packet.pts
			));
		}
		let mut closed = None;
		if packet.keyframe && !self.pending.is_empty() {
			closed = Some(self.close(packet.dts)?);
		}
		self.pending.push(Sample {
			data,
			pts: packet.pts,
			dts: packet.dts,
			keyframe: packet.keyframe,
		});
		Ok(closed)
	}

	/// Closes whatever group is open. The stream's last fragment, at
	/// end of input.
	pub fn finish(&mut self) -> Result<Option<Fragment>, String> {
		if self.pending.is_empty() {
			return Ok(None);
		}
		Ok(Some(self.close(None)?))
	}

	/// Closes the pending samples into one fragment. `next_dts` is the
	/// decode time of the packet after the fragment, which settles the
	/// last sample's duration; absent at end of input, where the last
	/// known duration stands in.
	fn close(&mut self, next_dts: Option<i64>) -> Result<Fragment, String> {
		let samples = std::mem::take(&mut self.pending);
		let count = samples.len();

		let dts = self.resolve_dts(&samples, next_dts)?;
		let mut durations = Vec::with_capacity(count);
		for i in 0..count {
			let next = match (dts.get(i + 1), next_dts) {
				(Some(following), _) => Some(*following),
				(None, Some(after)) => Some(after),
				(None, None) => None,
			};
			let duration = match next {
				Some(next) => {
					let step = next - dts[i];
					if step < 0 {
						return Err(format!(
							"decode time steps backwards at pts {}",
							samples[i].pts
						));
					}
					step
				}
				None => self.last_duration.unwrap_or(0),
			};
			durations.push(duration);
		}
		if let Some(last) = durations.iter().rev().find(|d| **d > 0) {
			self.last_duration = Some(*last);
		}

		let shift = *self
			.dts_shift
			.get_or_insert_with(|| if dts[0] < 0 { -dts[0] } else { 0 });
		let base_decode_time = dts[0] + shift;
		if base_decode_time < 0 {
			return Err(format!(
				"decode time {base_decode_time} below the stream's start",
			));
		}

		let mut entries = Vec::with_capacity(count);
		let mut mdat_len = 0usize;
		for (i, sample) in samples.iter().enumerate() {
			let cts = (sample.pts - dts[i]) * self.tick_scale;
			let cts: i32 = cts.try_into().map_err(|_| {
				format!("presentation offset {cts} at pts {} overflows", sample.pts)
			})?;
			let duration = durations[i] * self.tick_scale;
			let duration: u32 = duration
				.try_into()
				.map_err(|_| format!("duration {duration} at pts {} overflows", sample.pts))?;
			let flags: u32 = if sample.keyframe { 0x0200_0000 } else { 0x0101_0000 };
			entries.push(TrunEntry {
				duration,
				size: sample.data.len() as u32,
				flags,
				cts,
			});
			mdat_len += sample.data.len();
		}

		let moof = moof(
			self.sequence,
			(base_decode_time * self.tick_scale) as u64,
			&entries,
		);
		let mut bytes = moof;
		bytes.extend_from_slice(&((mdat_len + 8) as u32).to_be_bytes());
		bytes.extend_from_slice(b"mdat");
		for sample in &samples {
			bytes.extend_from_slice(&sample.data);
		}

		let fragment = Fragment {
			bytes,
			sequence: self.sequence,
			keyframe: samples[0].keyframe,
			samples: count as u64,
			pts_min: samples.iter().map(|s| s.pts).min().expect("samples"),
			pts_max: samples.iter().map(|s| s.pts).max().expect("samples"),
		};
		self.sequence += 1;
		Ok(fragment)
	}

	/// One decode time per sample: the settled ones as they came, an
	/// unsettled prefix synthesized backwards from the first settled
	/// one at the nearest known step.
	fn resolve_dts(&self, samples: &[Sample], next_dts: Option<i64>) -> Result<Vec<i64>, String> {
		let first_known = samples.iter().position(|s| s.dts.is_some());
		match first_known {
			None => {
				// No decode time settled anywhere: a stream that does
				// not reorder, where presentation order is decode order.
				Ok(samples.iter().map(|s| s.pts).collect())
			}
			Some(k) => {
				let known = samples[k].dts.expect("position found it");
				let step = samples
					.get(k + 1)
					.and_then(|s| s.dts)
					.map(|following| following - known)
					.or_else(|| next_dts.map(|after| after - known))
					.unwrap_or(0);
				let mut out = Vec::with_capacity(samples.len());
				for (i, sample) in samples.iter().enumerate() {
					match sample.dts {
						Some(dts) => out.push(dts),
						None if i < k => out.push(known - step * (k - i) as i64),
						None => {
							return Err(format!(
								"the packet at pts {} has no decode time after one settled",
								sample.pts
							))
						}
					}
				}
				Ok(out)
			}
		}
	}

	fn moov(&self) -> Vec<u8> {
		let stsd_child = avc1(self.width, self.height, &self.avcc);
		let stbl = boxed(
			b"stbl",
			[
				full_boxed(b"stsd", 0, 0, {
					let mut p = 4u32.to_be_bytes().to_vec();
					p[3] = 1; // entry_count 1
					p.extend_from_slice(&stsd_child);
					p
				}),
				full_boxed(b"stts", 0, 0, vec![0; 4]),
				full_boxed(b"stsc", 0, 0, vec![0; 4]),
				full_boxed(b"stsz", 0, 0, vec![0; 8]),
				full_boxed(b"stco", 0, 0, vec![0; 4]),
			]
			.concat(),
		);
		let dinf = boxed(
			b"dinf",
			full_boxed(b"dref", 0, 0, {
				let mut p = vec![0, 0, 0, 1]; // entry_count 1
				p.extend_from_slice(&full_boxed(b"url ", 0, 1, Vec::new()));
				p
			}),
		);
		let minf = boxed(
			b"minf",
			[full_boxed(b"vmhd", 0, 1, vec![0; 8]), dinf, stbl].concat(),
		);
		let hdlr = full_boxed(b"hdlr", 0, 0, {
			let mut p = vec![0; 4]; // pre_defined
			p.extend_from_slice(b"vide");
			p.extend_from_slice(&[0; 12]);
			p.extend_from_slice(b"VideoHandler\0");
			p
		});
		let mdhd = full_boxed(b"mdhd", 0, 0, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&self.timescale.to_be_bytes());
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&0x55c4u16.to_be_bytes()); // und
			p.extend_from_slice(&[0; 2]);
			p
		});
		let mdia = boxed(b"mdia", [mdhd, hdlr, minf].concat());
		let tkhd = full_boxed(b"tkhd", 0, 3, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&1u32.to_be_bytes()); // track_ID
			p.extend_from_slice(&[0; 4]); // reserved
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&[0; 8]); // reserved
			p.extend_from_slice(&[0; 8]); // layer, group, volume, reserved
			p.extend_from_slice(&MATRIX);
			p.extend_from_slice(&(self.width << 16).to_be_bytes());
			p.extend_from_slice(&(self.height << 16).to_be_bytes());
			p
		});
		let trak = boxed(b"trak", [tkhd, mdia].concat());
		let mvex = boxed(
			b"mvex",
			full_boxed(b"trex", 0, 0, {
				let mut p = 1u32.to_be_bytes().to_vec(); // track_ID
				p.extend_from_slice(&1u32.to_be_bytes()); // sample description
				p.extend_from_slice(&[0; 12]); // default duration, size, flags
				p
			}),
		);
		let mvhd = full_boxed(b"mvhd", 0, 0, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&1000u32.to_be_bytes()); // movie timescale
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate 1.0
			p.extend_from_slice(&0x0100u16.to_be_bytes()); // volume 1.0
			p.extend_from_slice(&[0; 10]); // reserved
			p.extend_from_slice(&MATRIX);
			p.extend_from_slice(&[0; 24]); // pre_defined
			p.extend_from_slice(&2u32.to_be_bytes()); // next_track_ID
			p
		});
		boxed(b"moov", [mvhd, trak, mvex].concat())
	}
}

/// Converts stream ticks to the microseconds a MoQ timestamp counts,
/// clamped at zero on the way into an unsigned wire field.
pub fn ticks_to_micros(ticks: i64, time_base_num: i32, time_base_den: i32) -> u64 {
	if ticks <= 0 || time_base_num <= 0 || time_base_den <= 0 {
		return 0;
	}
	(ticks as u128 * time_base_num as u128 * 1_000_000 / time_base_den as u128) as u64
}

/// The identity transformation matrix every mp4 header carries.
const MATRIX: [u8; 36] = {
	let mut m = [0u8; 36];
	m[1] = 0x01; // 0x00010000
	m[17] = 0x01;
	m[32] = 0x40; // 0x40000000
	m
};

struct TrunEntry {
	duration: u32,
	size: u32,
	flags: u32,
	cts: i32,
}

fn ftyp() -> Vec<u8> {
	let mut p = Vec::new();
	p.extend_from_slice(b"iso5"); // major brand
	p.extend_from_slice(&512u32.to_be_bytes()); // minor version
	p.extend_from_slice(b"iso5isomavc1mp41"); // compatible brands
	boxed(b"ftyp", p)
}

fn moof(sequence: u32, base_decode_time: u64, entries: &[TrunEntry]) -> Vec<u8> {
	// Sizes first, so the trun's data offset can point past the moof
	// into the mdat payload before either is assembled.
	let trun_size = 20 + entries.len() * 16;
	let traf_size = 8 + 16 + 20 + trun_size;
	let moof_size = 8 + 16 + traf_size;
	let data_offset = (moof_size + 8) as i32;

	let mfhd = full_boxed(b"mfhd", 0, 0, sequence.to_be_bytes().to_vec());
	let tfhd = full_boxed(b"tfhd", 0, 0x020000, 1u32.to_be_bytes().to_vec());
	let tfdt = full_boxed(b"tfdt", 1, 0, base_decode_time.to_be_bytes().to_vec());
	let trun = full_boxed(b"trun", 1, 0xf01, {
		let mut p = (entries.len() as u32).to_be_bytes().to_vec();
		p.extend_from_slice(&data_offset.to_be_bytes());
		for entry in entries {
			p.extend_from_slice(&entry.duration.to_be_bytes());
			p.extend_from_slice(&entry.size.to_be_bytes());
			p.extend_from_slice(&entry.flags.to_be_bytes());
			p.extend_from_slice(&entry.cts.to_be_bytes());
		}
		p
	});
	let traf = boxed(b"traf", [tfhd, tfdt, trun].concat());
	let built = boxed(b"moof", [mfhd, traf].concat());
	debug_assert_eq!(built.len(), moof_size);
	built
}

/// The avc1 sample entry, its `avcC` inside.
fn avc1(width: u32, height: u32, avcc: &[u8]) -> Vec<u8> {
	let mut p = Vec::new();
	p.extend_from_slice(&[0; 6]); // reserved
	p.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
	p.extend_from_slice(&[0; 16]); // pre_defined, reserved
	p.extend_from_slice(&(width as u16).to_be_bytes());
	p.extend_from_slice(&(height as u16).to_be_bytes());
	p.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // 72 dpi
	p.extend_from_slice(&0x0048_0000u32.to_be_bytes());
	p.extend_from_slice(&[0; 4]); // reserved
	p.extend_from_slice(&1u16.to_be_bytes()); // frame_count
	p.extend_from_slice(&[0; 32]); // compressorname, empty
	p.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
	p.extend_from_slice(&(-1i16).to_be_bytes()); // pre_defined
	p.extend_from_slice(&boxed(b"avcC", avcc.to_vec()));
	boxed(b"avc1", p)
}

fn boxed(kind: &[u8; 4], payload: Vec<u8>) -> Vec<u8> {
	let mut out = Vec::with_capacity(8 + payload.len());
	out.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
	out.extend_from_slice(kind);
	out.extend_from_slice(&payload);
	out
}

fn full_boxed(kind: &[u8; 4], version: u8, flags: u32, payload: Vec<u8>) -> Vec<u8> {
	let mut full = Vec::with_capacity(4 + payload.len());
	full.push(version);
	full.extend_from_slice(&flags.to_be_bytes()[1..]);
	full.extend_from_slice(&payload);
	boxed(kind, full)
}

#[cfg(test)]
mod tests {
	use super::*;

	// A syntactically plausible SPS/PPS pair, constrained-baseline so
	// the avcC needs no extension bytes.
	const EXTRADATA: &[u8] = &[
		0, 0, 0, 1, 0x67, 66, 0xc0, 30, 0xab, 0xcd, // SPS, profile 66
		0, 0, 0, 1, 0x68, 0xee, 0x06, 0xf2, // PPS
	];

	fn muxer() -> Muxer {
		Muxer::new(EXTRADATA, 320, 180, 1, 30).expect("a muxer")
	}

	fn packet(pts: i64, keyframe: bool) -> Vec<u8> {
		// One slice-looking NAL behind a start code; pts stamps it.
		vec![0, 0, 0, 1, if keyframe { 0x65 } else { 0x41 }, pts as u8]
	}

	#[test]
	fn a_keyframe_closes_the_group_before_it() {
		let mut mux = muxer();
		for pts in 0..3 {
			let data = packet(pts, pts == 0);
			let closed = mux
				.push(Packet {
					pts,
					dts: Some(pts),
					keyframe: pts == 0,
					data: &data,
				})
				.expect("push");
			assert!(closed.is_none());
		}
		let data = packet(3, true);
		let closed = mux
			.push(Packet {
				pts: 3,
				dts: Some(3),
				keyframe: true,
				data: &data,
			})
			.expect("push")
			.expect("the keyframe closed a fragment");
		assert_eq!(closed.sequence, 1);
		assert_eq!(closed.samples, 3);
		assert!(closed.keyframe);
		assert_eq!((closed.pts_min, closed.pts_max), (0, 2));

		let last = mux.finish().expect("finish").expect("the tail fragment");
		assert_eq!(last.sequence, 2);
		assert_eq!(last.samples, 1);
		assert!(mux.finish().expect("finish").is_none());
	}

	#[test]
	fn an_unsettled_decode_prefix_is_synthesized_backwards() {
		let mut mux = muxer();
		// Decode times settle at the third packet, as a reordering
		// stream's do; the synthesized prefix must keep the step.
		let inputs = [(2i64, None), (0, None), (1, Some(0i64)), (3, Some(1))];
		for (pts, dts) in inputs {
			let data = packet(pts, pts == 2);
			mux.push(Packet {
				pts,
				dts,
				keyframe: pts == 2,
				data: &data,
			})
			.expect("push");
		}
		let fragment = mux.finish().expect("finish").expect("a fragment");
		assert_eq!(fragment.samples, 4);
		// Synthesized: dts -2, -1 behind the settled 0, 1 - so the
		// shift lifts the base decode time to zero exactly.
		// Past the tag: version(1) + flags(3), then the 64-bit time.
		let tfdt = &fragment.bytes[past_tag(&fragment.bytes, b"tfdt")..];
		assert_eq!(&tfdt[..12], &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
	}

	#[test]
	fn a_packet_without_start_codes_is_refused() {
		let mut mux = muxer();
		let err = mux
			.push(Packet {
				pts: 0,
				dts: Some(0),
				keyframe: true,
				data: &[1, 2, 3],
			})
			.expect_err("no start code");
		assert!(err.contains("start code"), "{err}");
	}

	#[test]
	fn ticks_scale_to_microseconds() {
		assert_eq!(ticks_to_micros(30, 1, 30), 1_000_000);
		assert_eq!(ticks_to_micros(2048, 1, 61440), 33_333);
		assert_eq!(ticks_to_micros(-5, 1, 30), 0);
	}

	/// Byte offset just past the first `kind` tag found.
	fn past_tag(bytes: &[u8], kind: &[u8; 4]) -> usize {
		bytes
			.windows(4)
			.position(|w| w == kind)
			.map(|p| p + 4)
			.expect("box present")
	}
}

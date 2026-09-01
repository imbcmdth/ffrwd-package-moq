//! The muxer pinned against real ffmpeg output.
//!
//! `tests/data/ref-frag.mp4` was generated once with ffmpeg (7.x,
//! libx264 High profile, B-frames on) and committed:
//!
//!     ffmpeg -f lavfi -i testsrc2=size=320x180:rate=30 -t 4 \
//!       -c:v libx264 -preset veryfast -g 30 -pix_fmt yuv420p \
//!       -f mp4 -movflags frag_keyframe+empty_moov+default_base_moof \
//!       ref-frag.mp4
//!
//! Everything asserted here derives from that reference, never from
//! the muxer under test: the encoded packets are read back out of the
//! reference's own fragments - sample bytes reframed to the Annex-B
//! the encoded edge carries, decode and presentation times off `tfdt`
//! and the `trun` entries, keyframes off the sample flags - and fed to
//! the muxer, whose output must then agree with the reference sample
//! by sample: one fragment apiece, group starts exactly at the
//! reference's own keyframe cuts, decode times, sizes, durations,
//! presentation offsets, sync flags, and the `mdat` payload byte for
//! byte. The `avcC` built from the SPS/PPS must equal the reference's
//! exactly. When ffprobe is on the PATH the muxer's own output must
//! also decode whole.

use moq_core::fmp4::{Scanner, Segment};
use moq_core::mux::{Muxer, Packet};

const REFERENCE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/ref-frag.mp4");
const NON_SYNC: u32 = 0x0001_0000;

#[test]
fn the_muxer_agrees_with_ffmpeg_fragment_by_fragment() {
	let reference = Reference::read();
	let extradata = annexb_extradata(&reference.avcc);

	let mut muxer = Muxer::video(
		&extradata,
		reference.width,
		reference.height,
		1,
		reference.timescale as i32,
	)
	.expect("a muxer from the reference's own parameter sets");

	assert_eq!(
		muxer.avcc().expect("a video muxer builds an avcC"),
		&reference.avcc[..],
		"the avcC built from the SPS/PPS is not the reference's"
	);

	let init = muxer.init_segment();
	let init_timescale = mdhd_timescale(&init);
	assert_eq!(init_timescale, reference.timescale, "init segment timescale");
	let (width, height) = avc1_dimensions(&init);
	assert_eq!((width, height), (reference.width, reference.height));
	assert!(
		find_box(&init, &[b"moov", b"mvex", b"trex"]).is_some(),
		"init segment carries no trex"
	);

	// The reference's packets, in decode order, fed back in.
	let mut ours = Vec::new();
	for sample in &reference.samples {
		let annexb = avcc_to_annexb(&sample.data);
		ours.extend(
			muxer
				.push(Packet {
					pts: sample.pts,
					dts: Some(sample.dts),
					// The NUT wire supplies no duration for a reordering
					// stream, and this reference has B-frames: the muxer's
					// lookahead is the path under test, byte-identically.
					duration: None,
					keyframe: sample.keyframe,
					data: &annexb,
				})
				.expect("push"),
		);
	}
	ours.extend(muxer.finish().expect("finish"));

	// One fragment per sample; everything asserted is still the
	// reference's own numbers, read at the sample level.
	assert_eq!(
		ours.len(),
		reference.samples.len(),
		"one fragment per reference sample"
	);
	// Group starts land exactly where the reference cut its fragments.
	let starts: Vec<usize> = ours
		.iter()
		.enumerate()
		.filter(|(_, fragment)| fragment.starts_group)
		.map(|(i, _)| i)
		.collect();
	let mut cuts = Vec::with_capacity(reference.fragments.len());
	let mut at = 0usize;
	for fragment in &reference.fragments {
		cuts.push(at);
		at += fragment.samples.len();
	}
	assert_eq!(starts, cuts, "group starts are the reference's fragment cuts");

	for (i, (mine, theirs)) in ours.iter().zip(&reference.samples).enumerate() {
		let parsed = parse_fragment(&mine.bytes);
		assert_eq!(mine.sequence, i as u32 + 1, "mfhd sequence");
		assert_eq!(parsed.sequence, mine.sequence, "mfhd sequence as written");
		assert_eq!(
			parsed.base_decode_time, theirs.dts as u64,
			"tfdt of sample {i}"
		);
		assert_eq!(parsed.samples.len(), 1, "sample count of fragment {i}");
		let entry = parsed.samples[0];
		assert_eq!(entry.size as usize, theirs.data.len(), "size of sample {i}");
		assert_eq!(entry.duration, theirs.duration, "duration of sample {i}");
		assert_eq!(
			entry.cts,
			theirs.pts - theirs.dts,
			"presentation offset of sample {i}"
		);
		assert_eq!(
			entry.flags & NON_SYNC == 0,
			theirs.keyframe,
			"sync flag of sample {i}"
		);
		assert_eq!(
			parsed.mdat, theirs.data,
			"mdat payload of sample {i} is not byte-identical"
		);
	}

	// The whole stream the muxer wrote, decodable when ffprobe is here.
	let mut stream = init;
	for fragment in &ours {
		stream.extend_from_slice(&fragment.bytes);
	}
	if let Some(frames) = ffprobe_frames(&stream) {
		assert_eq!(frames, 120, "the reference is 4s at 30fps");
	}
}

#[test]
fn the_reference_round_trips_through_the_scanner() {
	// The parse side proves the fixture is what this file assumes:
	// keyframe-aligned fragments, every first sample a sync sample.
	let reference = Reference::read();
	assert!(reference.fragments.len() >= 3, "the fixture is several GOPs");
	for fragment in &reference.fragments {
		let first = fragment.samples.first().expect("samples");
		assert!(
			first.flags & NON_SYNC == 0,
			"fragment {} does not open on a sync sample",
			fragment.sequence
		);
	}
}

/// What the committed reference holds, parsed with the same box
/// knowledge the package ships plus this file's own readers.
struct Reference {
	timescale: u32,
	width: u32,
	height: u32,
	avcc: Vec<u8>,
	fragments: Vec<ParsedFragment>,
	/// Every sample of every fragment, in decode order.
	samples: Vec<RefSample>,
}

struct RefSample {
	data: Vec<u8>,
	dts: i64,
	pts: i64,
	duration: u32,
	keyframe: bool,
}

struct ParsedFragment {
	sequence: u32,
	base_decode_time: u64,
	samples: Vec<ParsedSample>,
	mdat: Vec<u8>,
}

#[derive(Clone, Copy)]
struct ParsedSample {
	duration: u32,
	size: u32,
	flags: u32,
	cts: i64,
}

impl Reference {
	fn read() -> Self {
		let bytes = std::fs::read(REFERENCE).expect("the committed reference");
		let mut scanner = Scanner::new();
		scanner.push(&bytes).expect("scan the reference");
		scanner.finish().expect("the reference ends on a boundary");

		let mut init = None;
		let mut fragments = Vec::new();
		while let Some(segment) = scanner.poll() {
			match segment {
				Segment::Init(seg) => init = Some(seg.bytes.to_vec()),
				Segment::Fragment(frag) => fragments.push(parse_fragment(&frag.bytes)),
			}
		}
		let init = init.expect("the reference has an init segment");

		let timescale = mdhd_timescale(&init);
		let (width, height) = avc1_dimensions(&init);
		let avcc = find_box(
			&init,
			&[b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd"],
		)
		.map(|stsd| {
			let avc1 = child_box(&stsd[8..], b"avc1").expect("an avc1 sample entry");
			child_box(&avc1[78..], b"avcC")
				.expect("an avcC in the sample entry")
				.to_vec()
		})
		.expect("an stsd in the reference");

		// Decode order is fragment order; times chain across fragments.
		let mut samples = Vec::new();
		for fragment in &fragments {
			let mut dts = fragment.base_decode_time as i64;
			let mut offset = 0usize;
			for sample in &fragment.samples {
				let data = fragment.mdat[offset..offset + sample.size as usize].to_vec();
				offset += sample.size as usize;
				samples.push(RefSample {
					data,
					dts,
					pts: dts + sample.cts,
					duration: sample.duration,
					keyframe: sample.flags & NON_SYNC == 0,
				});
				dts += sample.duration as i64;
			}
		}

		Self {
			timescale,
			width,
			height,
			avcc,
			fragments,
			samples,
		}
	}
}

/// One `moof`+`mdat` fragment taken apart, sample table resolved per
/// the ISO precedence: trun first-sample flags, per-sample fields,
/// tfhd defaults.
fn parse_fragment(bytes: &[u8]) -> ParsedFragment {
	let moof = child_box(bytes, b"moof").expect("a moof");
	let mdat = child_box(bytes, b"mdat").expect("an mdat").to_vec();
	let mfhd = child_box(moof, b"mfhd").expect("an mfhd");
	let sequence = be32(mfhd, 4);
	let traf = child_box(moof, b"traf").expect("a traf");

	let tfhd = child_box(traf, b"tfhd").expect("a tfhd");
	let tfhd_flags = be32(tfhd, 0) & 0x00ff_ffff;
	let mut pos = 8usize; // version/flags + track_ID
	if tfhd_flags & 0x1 != 0 {
		pos += 8; // base_data_offset
	}
	if tfhd_flags & 0x2 != 0 {
		pos += 4; // sample_description_index
	}
	let default_duration = (tfhd_flags & 0x8 != 0).then(|| {
		let v = be32(tfhd, pos);
		pos += 4;
		v
	});
	let default_size = (tfhd_flags & 0x10 != 0).then(|| {
		let v = be32(tfhd, pos);
		pos += 4;
		v
	});
	let default_flags = (tfhd_flags & 0x20 != 0).then(|| be32(tfhd, pos));

	let tfdt = child_box(traf, b"tfdt").expect("a tfdt");
	let base_decode_time = match tfdt[0] {
		1 => u64::from_be_bytes(tfdt[4..12].try_into().expect("8 bytes")),
		_ => be32(tfdt, 4) as u64,
	};

	let trun = child_box(traf, b"trun").expect("a trun");
	let version = trun[0];
	let trun_flags = be32(trun, 0) & 0x00ff_ffff;
	let count = be32(trun, 4) as usize;
	let mut pos = 8usize;
	if trun_flags & 0x1 != 0 {
		pos += 4; // data_offset
	}
	let first_flags = (trun_flags & 0x4 != 0).then(|| {
		let v = be32(trun, pos);
		pos += 4;
		v
	});
	let mut samples = Vec::with_capacity(count);
	for i in 0..count {
		let duration = if trun_flags & 0x100 != 0 {
			let v = be32(trun, pos);
			pos += 4;
			v
		} else {
			default_duration.expect("a duration somewhere")
		};
		let size = if trun_flags & 0x200 != 0 {
			let v = be32(trun, pos);
			pos += 4;
			v
		} else {
			default_size.expect("a size somewhere")
		};
		let own_flags = if trun_flags & 0x400 != 0 {
			let v = be32(trun, pos);
			pos += 4;
			Some(v)
		} else {
			None
		};
		let flags = match (i, first_flags, own_flags) {
			(0, Some(first), _) => first,
			(_, _, Some(own)) => own,
			_ => default_flags.expect("sample flags somewhere"),
		};
		let cts = if trun_flags & 0x800 != 0 {
			let raw = be32(trun, pos);
			pos += 4;
			if version == 0 {
				raw as i64
			} else {
				raw as i32 as i64
			}
		} else {
			0
		};
		samples.push(ParsedSample {
			duration,
			size,
			flags,
			cts,
		});
	}

	ParsedFragment {
		sequence,
		base_decode_time,
		samples,
		mdat,
	}
}

/// The media timescale out of an init segment's mdhd.
fn mdhd_timescale(init: &[u8]) -> u32 {
	let mdhd = find_box(init, &[b"moov", b"trak", b"mdia", b"mdhd"]).expect("an mdhd");
	be32(mdhd, 12)
}

/// Width and height out of the avc1 sample entry.
fn avc1_dimensions(init: &[u8]) -> (u32, u32) {
	let stsd = find_box(init, &[b"moov", b"trak", b"mdia", b"minf", b"stbl", b"stsd"])
		.expect("an stsd");
	let avc1 = child_box(&stsd[8..], b"avc1").expect("an avc1");
	(be16(avc1, 24) as u32, be16(avc1, 26) as u32)
}

/// SPS/PPS out of an avcC payload, respelled as Annex-B extradata -
/// the shape the encoded edge hands a module.
fn annexb_extradata(avcc: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	let sps_count = (avcc[5] & 0x1f) as usize;
	let mut pos = 6usize;
	for _ in 0..sps_count {
		let len = be16(avcc, pos) as usize;
		pos += 2;
		out.extend_from_slice(&[0, 0, 0, 1]);
		out.extend_from_slice(&avcc[pos..pos + len]);
		pos += len;
	}
	let pps_count = avcc[pos] as usize;
	pos += 1;
	for _ in 0..pps_count {
		let len = be16(avcc, pos) as usize;
		pos += 2;
		out.extend_from_slice(&[0, 0, 0, 1]);
		out.extend_from_slice(&avcc[pos..pos + len]);
		pos += len;
	}
	out
}

/// One AVCC sample respelled as Annex-B, 4-byte start codes.
fn avcc_to_annexb(sample: &[u8]) -> Vec<u8> {
	let mut out = Vec::with_capacity(sample.len());
	let mut pos = 0usize;
	while pos + 4 <= sample.len() {
		let len = be32(sample, pos) as usize;
		pos += 4;
		out.extend_from_slice(&[0, 0, 0, 1]);
		out.extend_from_slice(&sample[pos..pos + len]);
		pos += len;
	}
	assert_eq!(pos, sample.len(), "sample lengths do not tile the sample");
	out
}

/// The payload of the first `kind` child at each step of `path`.
fn find_box<'a>(mut data: &'a [u8], path: &[&[u8; 4]]) -> Option<&'a [u8]> {
	for kind in path {
		data = child_box(data, kind)?;
	}
	Some(data)
}

/// The payload of the first `kind` box among `data`'s top-level boxes.
fn child_box<'a>(data: &'a [u8], kind: &[u8; 4]) -> Option<&'a [u8]> {
	let mut pos = 0usize;
	while pos + 8 <= data.len() {
		let size = be32(data, pos) as usize;
		if size < 8 || pos + size > data.len() {
			return None;
		}
		if &data[pos + 4..pos + 8] == kind {
			return Some(&data[pos + 8..pos + size]);
		}
		pos += size;
	}
	None
}

fn be32(data: &[u8], pos: usize) -> u32 {
	u32::from_be_bytes(data[pos..pos + 4].try_into().expect("4 bytes"))
}

fn be16(data: &[u8], pos: usize) -> u16 {
	u16::from_be_bytes(data[pos..pos + 2].try_into().expect("2 bytes"))
}

/// Frame count by ffprobe over the muxer's own output; `None` without
/// ffprobe on the PATH (the byte comparisons above still hold alone).
fn ffprobe_frames(stream: &[u8]) -> Option<u64> {
	let dir = std::env::temp_dir().join(format!("moq-mux-ref-{}", std::process::id()));
	std::fs::create_dir_all(&dir).ok()?;
	let path = dir.join("ours.mp4");
	std::fs::write(&path, stream).ok()?;
	let output = std::process::Command::new("ffprobe")
		.args(["-v", "error", "-count_frames", "-select_streams", "v:0"])
		.args(["-show_entries", "stream=nb_read_frames"])
		.args(["-of", "default=nw=1:nk=1"])
		.arg(&path)
		.output()
		.ok()?;
	std::fs::remove_dir_all(&dir).ok();
	assert!(
		output.status.success(),
		"ffprobe rejected the muxer's output: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

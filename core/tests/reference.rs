//! What this package publishes, pinned against real ffmpeg output.
//!
//! `tests/data/ref-frag.mp4` was generated once with ffmpeg (7.x,
//! libx264 High profile, B-frames on) and committed:
//!
//!     ffmpeg -f lavfi -i testsrc2=size=320x180:rate=30 -t 4 \
//!       -c:v libx264 -preset veryfast -g 30 -pix_fmt yuv420p \
//!       -f mp4 -movflags frag_keyframe+empty_moov+default_base_moof \
//!       ref-frag.mp4
//!
//! The box layer, the sample tables and the writer are
//! [`ffrwd_bmff`]'s, and their agreement with ffmpeg is asserted in
//! that crate against a fixture of its own. What is left here is what
//! this package decides: the `avcC` it builds out of the extradata a
//! wire hands it, the Annex-B reframing it does on the way into a
//! sample, and where it cuts its MoQ groups.
//!
//! Everything asserted derives from the reference, never from the code
//! under test: the encoded packets are read back out of the
//! reference's own fragments by this file's own hand-written parser,
//! reframed to the Annex-B an encoded edge carries, and fed to the
//! publishing path, whose fragments must then carry the reference's
//! own sample bytes back, byte for byte. When ffprobe is on the PATH
//! the whole published stream must also decode.

use ffrwd_bmff::mux::{Muxer, Packet, Video};
use ffrwd_bmff::scanner::{Scanner, Segment};
use ffrwd_nal::annexb::annexb_to_length_prefixed;
use ffrwd_nal::config::{avcc_length_size, build_avcc, parse_parameter_sets};
use moq_core::group::Groups;

const REFERENCE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/ref-frag.mp4");
const NON_SYNC: u32 = 0x0001_0000;

#[test]
fn the_record_this_package_publishes_is_ffmpegs_own_avcc() {
	// THE RECORD CHANGED BY ONE BYTE, and this is the assertion that
	// says which byte. ffmpeg's H.264 demuxer pads the SPS in its
	// Annex-B extradata with a `trailing_zero_8bits`, and the scanner
	// this package used to carry gave that byte to the SPS. So every
	// `avcC` published before this moved to ffrwd-nal carried a 27
	// byte SPS where ffmpeg's own MP4 muxer writes 26. The byte is
	// padding and no decoder reads it, but the record was not
	// ffmpeg's. It is now.
	let reference = Reference::read();
	let extradata = ffmpeg_extradata(&reference.avcc);
	let (sps, pps) = parse_parameter_sets(&extradata);

	assert_eq!(
		sps.len() + pps.len(),
		2,
		"the extradata carries one SPS and one PPS"
	);
	assert_eq!(
		sps[0].len(),
		26,
		"the pad between the sets belongs to the stream, not to the SPS"
	);

	let built = build_avcc(&sps, &pps).expect("an avcC from the reference's own sets");
	assert_eq!(
		built, reference.avcc,
		"the published record is not ffmpeg's own, byte for byte"
	);
	assert_eq!(avcc_length_size(&built), 4);

	// And the record as this package used to write it, which is the
	// same call with the pad still on the SPS.
	let padded = vec![[sps[0].clone(), vec![0]].concat()];
	let old = build_avcc(&padded, &pps).expect("the record as it was");
	assert_eq!(
		old.len(),
		built.len() + 1,
		"exactly one byte, the pad itself, has left the record"
	);
	assert_eq!(&old[6..8], &[0, 27], "the SPS length field as it was");
	assert_eq!(&built[6..8], &[0, 26], "and as ffmpeg spells it");
	assert_eq!(
		&old[8..8 + 27],
		&[&built[8..8 + 26], &[0u8][..]].concat()[..],
		"the byte that left is the trailing zero, and nothing else moved"
	);

	// Which is one byte off every init segment and every catalog entry
	// a broadcast of this stream publishes.
	assert_eq!(
		publisher(&reference, &built).init_segment().len() + 1,
		publisher(&reference, &old).init_segment().len(),
	);
}

#[test]
fn what_this_package_publishes_is_the_references_own_packets() {
	let reference = Reference::read();
	let extradata = ffmpeg_extradata(&reference.avcc);
	let (sps, pps) = parse_parameter_sets(&extradata);
	let avcc = build_avcc(&sps, &pps).expect("an avcC");
	let length_size = avcc_length_size(&avcc);
	let mut muxer = publisher(&reference, &avcc);
	let mut groups = Groups::video();

	let init = muxer.init_segment();

	// The reference's packets, in decode order, reframed to the
	// Annex-B an encoded edge hands a packet sink and fed back in the
	// way the publish module feeds them.
	let mut ours = Vec::new();
	for sample in &reference.samples {
		let annexb = avcc_to_annexb(&sample.data);
		let stored = annexb_to_length_prefixed(&annexb, length_size).expect("a stored sample");
		ours.extend(
			muxer
				.push(Packet {
					pts: sample.pts,
					dts: Some(sample.dts),
					// The NUT wire supplies no duration for a reordering
					// stream, and this reference has B-frames: the
					// lookahead is the path under test.
					duration: None,
					keyframe: sample.keyframe,
					data: &stored,
				})
				.expect("push"),
		);
	}
	ours.extend(muxer.finish().expect("finish"));

	assert_eq!(
		ours.len(),
		reference.samples.len(),
		"one fragment per reference sample"
	);

	// The group discipline: a group opens at every keyframe, which for
	// this stream is exactly where ffmpeg cut its own fragments.
	let starts: Vec<usize> = ours
		.iter()
		.enumerate()
		.filter(|(_, fragment)| groups.starts_a_group(fragment.keyframe, fragment.pts))
		.map(|(index, _)| index)
		.collect();
	let mut cuts = Vec::with_capacity(reference.fragments.len());
	let mut at = 0usize;
	for fragment in &reference.fragments {
		cuts.push(at);
		at += fragment.samples.len();
	}
	assert_eq!(starts, cuts, "group starts are the reference's fragment cuts");

	// And the bytes came back whole: Annex-B out of the reference,
	// length-prefixed into the sample, and the same bytes in the mdat.
	for (index, (mine, theirs)) in ours.iter().zip(&reference.samples).enumerate() {
		assert_eq!(mine.sequence, index as u32 + 1, "mfhd sequence");
		assert_eq!(mine.keyframe, theirs.keyframe, "sync flag of sample {index}");
		let parsed = parse_fragment(&mine.bytes);
		assert_eq!(parsed.samples.len(), 1, "sample count of fragment {index}");
		assert_eq!(
			parsed.mdat, theirs.data,
			"mdat payload of sample {index} is not byte-identical"
		);
	}

	// The whole stream the publish path wrote, decodable when ffprobe
	// is here to say so.
	let mut stream = init;
	for fragment in &ours {
		stream.extend_from_slice(&fragment.bytes);
	}
	if let Some(frames) = ffprobe_frames(&stream) {
		assert_eq!(frames, 120, "the reference is 4s at 30fps");
	}
}

/// The muxer the publish module builds for this stream, given a record
/// to carry into its sample entry.
fn publisher(reference: &Reference, avcc: &[u8]) -> Muxer {
	Muxer::video(
		Video {
			kind: *b"avc1",
			width: reference.width,
			height: reference.height,
			config: avcc.to_vec(),
		},
		1,
		reference.timescale as i32,
	)
	.expect("a muxer from the reference's own parameter sets")
}

/// The Annex-B extradata ffmpeg's H.264 demuxer hands out of band for
/// this stream: each parameter set behind a 4-byte start code, and the
/// `trailing_zero_8bits` of Annex B.1.1 that ffmpeg pads the SPS with.
///
/// Dumped out of the fixture by remuxing it to an elementary stream
/// and back into NUT, whose stream header carries
///
///     00 00 00 01 <SPS, 26 bytes> 00 00 00 00 01 <PPS, 4 bytes>
///
/// which is five bytes between the sets where a start code is four.
fn ffmpeg_extradata(avcc: &[u8]) -> Vec<u8> {
	let mut out = Vec::new();
	let sps_count = (avcc[5] & 0x1f) as usize;
	let mut pos = 6usize;
	for _ in 0..sps_count {
		let len = be16(avcc, pos) as usize;
		pos += 2;
		out.extend_from_slice(&[0, 0, 0, 1]);
		out.extend_from_slice(&avcc[pos..pos + len]);
		pos += len;
		out.push(0); // trailing_zero_8bits, as ffmpeg writes it
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

/// What the committed reference holds, taken apart by this file's own
/// readers.
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
	keyframe: bool,
}

struct ParsedFragment {
	samples: Vec<ParsedSample>,
	base_decode_time: u64,
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
				Segment::Init(seg) => init = Some(seg.bytes),
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
		samples,
		base_decode_time,
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

/// One AVCC sample respelled as Annex-B, 4-byte start codes: the shape
/// the encoded edge hands a packet sink.
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

/// Frame count by ffprobe over the published stream; `None` without
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
		"ffprobe rejected what the publish path wrote: {}",
		String::from_utf8_lossy(&output.stderr)
	);
	String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

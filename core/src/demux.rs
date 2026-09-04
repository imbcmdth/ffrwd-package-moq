//! Fragmented MP4 back to the encoded packets it was built from: the
//! inverse of [`crate::mux`], over the segments [`crate::fmp4`] cuts.
//!
//! One [`Track`] is read once from an init segment - the `moov`'s
//! media timescale, the sample entry's geometry, and the decoder
//! configuration inside it (`avcC` for h264, the `esds`'s
//! AudioSpecificConfig for AAC) - and then reads each `moof`+`mdat`
//! fragment into [`Sample`]s: `tfdt` gives the fragment's base decode
//! time, the `trun` gives each sample's size, duration, composition
//! offset and sync flag, and the `mdat` is cut by those sizes.
//! Timestamps come out in the track's own timescale, so a coded stream
//! whose time base is `1/timescale` carries them unchanged.
//!
//! Sample bytes come out exactly as the container stored them: AVCC
//! length-prefixed for h264 - [`crate::avc::avcc_to_annexb`] reframes
//! them for an edge that wants Annex-B - and raw AAC frames for audio.

use crate::avc;
use crate::fmp4::{box_payload, child_boxes, peek_box, Error};

/// What one track carries, past the timescale every track has.
#[derive(Clone, Debug, PartialEq)]
pub enum Media {
	Video {
		width: u32,
		height: u32,
		/// The `avcC` record, as the sample entry carried it.
		avcc: Vec<u8>,
	},
	Audio {
		sample_rate: u32,
		channels: u32,
		/// The AudioSpecificConfig out of the `esds`.
		asc: Vec<u8>,
	},
}

/// One sample, timed in the track's own timescale.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
	/// When the sample is decoded.
	pub dts: i64,
	/// When it is presented: `dts` plus the composition offset.
	pub pts: i64,
	/// How long it is presented.
	pub duration: i64,
	/// Whether decoding can start here.
	pub keyframe: bool,
	/// The stored bytes, as the `mdat` carried them.
	pub data: Vec<u8>,
}

/// The `trex` defaults a fragment falls back to.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Defaults {
	duration: u32,
	size: u32,
	flags: u32,
}

/// One track's init segment, read: what its fragments mean.
#[derive(Clone, Debug, PartialEq)]
pub struct Track {
	/// The unit sample times are counted in, per second.
	pub timescale: u32,
	pub media: Media,
	defaults: Defaults,
	/// The NAL length prefix the `avcC` declares; 0 for audio.
	length_size: usize,
}

/// The `sample_is_non_sync_sample` bit: clear on a sync sample.
const NON_SYNC: u32 = 0x0001_0000;

impl Track {
	/// Reads an init segment - `ftyp`+`moov`, the bytes a catalog's
	/// `cmaf` container carries.
	pub fn read(init: &[u8]) -> Result<Self, Error> {
		let moov = top_level(init, b"moov")?.ok_or_else(|| Error::Malformed {
			offset: 0,
			what: "the init segment carries no moov".into(),
		})?;
		let trak = find(moov, 0, b"trak")?.ok_or_else(|| Error::Malformed {
			offset: 0,
			what: "the moov carries no trak".into(),
		})?;
		let mdia = find(trak, 0, b"mdia")?.ok_or_else(|| Error::Malformed {
			offset: 0,
			what: "the trak carries no mdia".into(),
		})?;
		let mdhd = find(mdia, 0, b"mdhd")?.ok_or_else(|| Error::Malformed {
			offset: 0,
			what: "the mdia carries no mdhd".into(),
		})?;
		let timescale = mdhd_timescale(mdhd)?;

		let stsd = find(mdia, 0, b"minf")?
			.and_then(|minf| find(minf, 0, b"stbl").ok().flatten())
			.and_then(|stbl| find(stbl, 0, b"stsd").ok().flatten())
			.ok_or_else(|| Error::Malformed {
				offset: 0,
				what: "the track carries no sample description".into(),
			})?;
		// Full box: version/flags, entry_count, then the entries.
		let entry = stsd.get(8..).ok_or_else(|| Error::Truncated {
			offset: 0,
			what: "the stsd ends before its first entry".into(),
		})?;
		let header = peek_box(entry, 0)?.ok_or_else(|| Error::Truncated {
			offset: 0,
			what: "the stsd's entry is shorter than its header".into(),
		})?;
		let media = match &header.kind {
			b"avc1" | b"avc3" => sample_entry_video(entry)?,
			b"mp4a" => sample_entry_audio(entry)?,
			other => {
				return Err(Error::Unsupported {
					offset: 0,
					what: format!("the sample entry is '{}', not avc1 or mp4a", name(other)),
				})
			}
		};
		let length_size = match &media {
			Media::Video { avcc, .. } => avc::avcc_length_size(avcc),
			Media::Audio { .. } => 0,
		};
		let defaults = match find(moov, 0, b"mvex")?
			.map(|mvex| find(mvex, 0, b"trex"))
			.transpose()?
			.flatten()
		{
			Some(trex) => trex_defaults(trex),
			None => Defaults::default(),
		};
		Ok(Track {
			timescale,
			media,
			defaults,
			length_size,
		})
	}

	/// The NAL length prefix this track's samples are framed with; 0
	/// for a track whose samples carry no framing of their own.
	pub fn length_size(&self) -> usize {
		self.length_size
	}

	/// The samples of one `moof`+`mdat` fragment, in decode order.
	///
	/// `fragment` is a complete fragment as [`crate::fmp4::Fragment`]
	/// carries it: the `moof` first, the `mdat` its data belongs to
	/// after. Sample data offsets are read the way an ISO fragment
	/// spells them - a `trun`'s `data_offset` from the `moof`'s own
	/// first byte under `default-base-is-moof`, from the explicit
	/// `base_data_offset` otherwise.
	pub fn samples(&self, fragment: &[u8]) -> Result<Vec<Sample>, Error> {
		let moof_len = match peek_box(fragment, 0)? {
			Some(header) if &header.kind == b"moof" => header.size as usize,
			Some(header) => {
				return Err(Error::Malformed {
					offset: 0,
					what: format!("the fragment opens with '{}', not moof", name(&header.kind)),
				})
			}
			None => {
				return Err(Error::Truncated {
					offset: 0,
					what: "the fragment is shorter than a box header".into(),
				})
			}
		};
		let moof = &fragment[..moof_len];
		// The mdat payload and its position within the fragment, so a
		// data offset counted from the moof indexes it.
		let (mdat_at, mdat) = mdat_payload(fragment, moof_len)?;

		let mut trafs = Vec::new();
		for child in child_boxes(box_payload(moof, 0)?, 0)? {
			if &child.kind == b"traf" {
				trafs.push(child.payload);
			}
		}
		let traf = match trafs.len() {
			1 => trafs[0],
			0 => {
				return Err(Error::Malformed {
					offset: 0,
					what: "the moof carries no traf".into(),
				})
			}
			several => {
				return Err(Error::Unsupported {
					offset: 0,
					what: format!(
						"the moof carries {several} track fragments, and a MoQ track is one"
					),
				})
			}
		};
		self.traf_samples(traf, mdat, mdat_at)
	}

	fn traf_samples(&self, traf: &[u8], mdat: &[u8], mdat_at: usize) -> Result<Vec<Sample>, Error> {
		let mut tfhd = None;
		let mut base_decode_time = None;
		let mut truns = Vec::new();
		for child in child_boxes(traf, 0)? {
			match &child.kind {
				b"tfhd" => tfhd = Some(read_tfhd(child.payload)?),
				b"tfdt" => base_decode_time = Some(read_tfdt(child.payload)?),
				b"trun" => truns.push(child.payload),
				_ => {}
			}
		}
		let tfhd = tfhd.ok_or_else(|| Error::Malformed {
			offset: 0,
			what: "the traf carries no tfhd".into(),
		})?;
		// Without a tfdt the fragment states no decode time at all, and
		// a live subscriber has no earlier fragment to continue from.
		let mut dts = base_decode_time.ok_or_else(|| Error::Unsupported {
			offset: 0,
			what: "the fragment carries no tfdt, so its decode time is unstated".into(),
		})?;

		let mut out = Vec::new();
		for trun in truns {
			let entries = read_trun(trun, &tfhd, &self.defaults)?;
			// Sample data sits at the base offset the tfhd settled plus
			// the trun's own; both are counted from the moof's first byte
			// under default-base-is-moof, which is the only shape a
			// single-track fragment takes.
			let mut at = tfhd
				.base_data_offset
				.saturating_add(entries.data_offset)
				.checked_sub(mdat_at as i64)
				.and_then(|at| usize::try_from(at).ok())
				.ok_or_else(|| Error::Malformed {
					offset: 0,
					what: "the trun's sample data starts before the mdat".into(),
				})?;
			for entry in entries.samples {
				let data =
					mdat.get(at..at + entry.size as usize)
						.ok_or_else(|| Error::Malformed {
							offset: 0,
							what: format!(
								"a sample of {} bytes at {at} overruns the mdat's {}",
								entry.size,
								mdat.len()
							),
						})?;
				at += entry.size as usize;
				out.push(Sample {
					dts,
					pts: dts + i64::from(entry.composition_offset),
					duration: i64::from(entry.duration),
					keyframe: entry.flags & NON_SYNC == 0,
					data: data.to_vec(),
				});
				dts += i64::from(entry.duration);
			}
		}
		if out.is_empty() {
			return Err(Error::Malformed {
				offset: 0,
				what: "the fragment declares no samples".into(),
			});
		}
		Ok(out)
	}
}

/// What one `tfhd` settles for its track fragment.
struct Tfhd {
	base_data_offset: i64,
	default_duration: Option<u32>,
	default_size: Option<u32>,
	default_flags: Option<u32>,
}

/// One `trun`'s samples and where their data starts.
struct Trun {
	data_offset: i64,
	samples: Vec<Entry>,
}

struct Entry {
	duration: u32,
	size: u32,
	flags: u32,
	composition_offset: i32,
}

fn read_tfhd(tfhd: &[u8]) -> Result<Tfhd, Error> {
	let flags = full_flags(tfhd, "tfhd")?;
	let mut pos = 8usize; // version/flags + track_ID
					   // `default-base-is-moof` (0x020000) puts the base at the moof's own
					   // first byte, which is offset zero within a fragment.
	let base_data_offset = if flags & 0x1 != 0 {
		field(tfhd, &mut pos, 8, "tfhd")? as i64
	} else {
		0
	};
	if flags & 0x2 != 0 {
		field(tfhd, &mut pos, 4, "tfhd")?; // sample_description_index
	}
	let default_duration = if flags & 0x8 != 0 {
		Some(field(tfhd, &mut pos, 4, "tfhd")? as u32)
	} else {
		None
	};
	let default_size = if flags & 0x10 != 0 {
		Some(field(tfhd, &mut pos, 4, "tfhd")? as u32)
	} else {
		None
	};
	let default_flags = if flags & 0x20 != 0 {
		Some(field(tfhd, &mut pos, 4, "tfhd")? as u32)
	} else {
		None
	};
	Ok(Tfhd {
		base_data_offset,
		default_duration,
		default_size,
		default_flags,
	})
}

/// The `tfdt`'s base media decode time, either width.
fn read_tfdt(tfdt: &[u8]) -> Result<i64, Error> {
	let version = *tfdt.first().ok_or_else(|| Error::Truncated {
		offset: 0,
		what: "the tfdt is empty".into(),
	})?;
	let width = if version == 1 { 8 } else { 4 };
	let field = tfdt.get(4..4 + width).ok_or_else(|| Error::Truncated {
		offset: 0,
		what: "the tfdt ends inside its decode time".into(),
	})?;
	Ok(field
		.iter()
		.fold(0u64, |value, byte| (value << 8) | u64::from(*byte)) as i64)
}

fn read_trun(trun: &[u8], tfhd: &Tfhd, trex: &Defaults) -> Result<Trun, Error> {
	let flags = full_flags(trun, "trun")?;
	let count = trun
		.get(4..8)
		.map(|four| u32::from_be_bytes(four.try_into().expect("4 bytes")))
		.ok_or_else(|| Error::Truncated {
			offset: 0,
			what: "the trun ends before its sample count".into(),
		})?;
	let mut pos = 8usize;
	let data_offset = if flags & 0x1 != 0 {
		field(trun, &mut pos, 4, "trun")? as u32 as i32 as i64
	} else {
		0
	};
	let first_sample_flags = if flags & 0x4 != 0 {
		Some(field(trun, &mut pos, 4, "trun")? as u32)
	} else {
		None
	};

	let duration_default = tfhd.default_duration.unwrap_or(trex.duration);
	let size_default = tfhd.default_size.unwrap_or(trex.size);
	let flags_default = tfhd.default_flags.unwrap_or(trex.flags);
	let mut samples = Vec::with_capacity(count as usize);
	for index in 0..count {
		let duration = if flags & 0x100 != 0 {
			field(trun, &mut pos, 4, "trun")? as u32
		} else {
			duration_default
		};
		let size = if flags & 0x200 != 0 {
			field(trun, &mut pos, 4, "trun")? as u32
		} else {
			size_default
		};
		let sample_flags = if flags & 0x400 != 0 {
			field(trun, &mut pos, 4, "trun")? as u32
		} else {
			flags_default
		};
		// Version 0 counts the offset unsigned and version 1 signed;
		// the same bits either way, and only a reordering stream (which
		// is version 1) ever sets the sign bit.
		let composition_offset = if flags & 0x800 != 0 {
			field(trun, &mut pos, 4, "trun")? as u32 as i32
		} else {
			0
		};
		samples.push(Entry {
			duration,
			size,
			flags: match (index, first_sample_flags) {
				(0, Some(first)) => first,
				_ => sample_flags,
			},
			composition_offset,
		});
	}
	Ok(Trun {
		data_offset,
		samples,
	})
}

/// The `mdat` payload of a fragment, whichever box follows the `moof`,
/// and where within the fragment it starts.
fn mdat_payload(fragment: &[u8], from: usize) -> Result<(usize, &[u8]), Error> {
	let mut pos = from;
	while pos < fragment.len() {
		let rest = &fragment[pos..];
		let header = peek_box(rest, pos as u64)?.ok_or_else(|| Error::Truncated {
			offset: pos as u64,
			what: "the fragment ends inside a box header".into(),
		})?;
		let size = header.size as usize;
		if rest.len() < size {
			return Err(Error::Truncated {
				offset: pos as u64,
				what: format!("box '{}' overruns the fragment", name(&header.kind)),
			});
		}
		if &header.kind == b"mdat" {
			let header_len = if rest[0..4] == [0, 0, 0, 1] { 16 } else { 8 };
			return Ok((pos + header_len, &rest[header_len..size]));
		}
		pos += size;
	}
	Err(Error::Malformed {
		offset: from as u64,
		what: "the fragment carries no mdat".into(),
	})
}

fn sample_entry_video(entry: &[u8]) -> Result<Media, Error> {
	let payload = box_payload(entry, 0)?;
	// VisualSampleEntry: 8 bytes of SampleEntry, then 16 pre_defined
	// and reserved, then the frame size.
	let width = be16(payload, 24)?;
	let height = be16(payload, 26)?;
	let avcc = find(payload, 78, b"avcC")?.ok_or_else(|| Error::Unsupported {
		offset: 0,
		what: "the avc1 entry carries no avcC".into(),
	})?;
	Ok(Media::Video {
		width: u32::from(width),
		height: u32::from(height),
		avcc: avcc.to_vec(),
	})
}

fn sample_entry_audio(entry: &[u8]) -> Result<Media, Error> {
	let payload = box_payload(entry, 0)?;
	// AudioSampleEntry: 8 bytes of SampleEntry, 8 reserved, then the
	// channel count, the sample size, 4 more reserved, and the 16.16
	// sample rate.
	let channels = be16(payload, 16)?;
	let sample_rate = u32::from(be16(payload, 24)?);
	let esds = find(payload, 28, b"esds")?.ok_or_else(|| Error::Unsupported {
		offset: 0,
		what: "the mp4a entry carries no esds".into(),
	})?;
	let asc = esds_config(esds).ok_or_else(|| Error::Unsupported {
		offset: 0,
		what: "the esds carries no decoder specific info".into(),
	})?;
	Ok(Media::Audio {
		sample_rate,
		channels: u32::from(channels),
		asc,
	})
}

/// The AudioSpecificConfig inside an `esds` full box payload: the
/// DecoderSpecificInfo (tag 5) of the decoder config (tag 4) of the ES
/// descriptor (tag 3).
fn esds_config(esds: &[u8]) -> Option<Vec<u8>> {
	let mut at = esds.get(4..)?; // past version/flags
	loop {
		let (tag, payload, rest) = descriptor(at)?;
		match tag {
			0x03 => {
				// ES_ID, then a flags byte whose bits add fields before
				// the descriptors that follow.
				let flags = *payload.get(2)?;
				let mut pos = 3usize;
				if flags & 0x80 != 0 {
					pos += 2; // dependsOn_ES_ID
				}
				if flags & 0x40 != 0 {
					pos += 1 + usize::from(*payload.get(pos)?); // URL
				}
				if flags & 0x20 != 0 {
					pos += 2; // OCR_ES_Id
				}
				at = payload.get(pos..)?;
			}
			0x04 => at = payload.get(13..)?, // past the decoder config's fixed fields
			0x05 => return Some(payload.to_vec()),
			_ => at = rest,
		}
		if at.is_empty() {
			return None;
		}
	}
}

/// One MPEG-4 descriptor: its tag, its payload, and what follows it.
fn descriptor(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
	let tag = *data.first()?;
	let mut length = 0usize;
	let mut pos = 1usize;
	loop {
		let byte = *data.get(pos)?;
		pos += 1;
		length = (length << 7) | usize::from(byte & 0x7f);
		if byte & 0x80 == 0 {
			break;
		}
		if pos > 5 {
			return None;
		}
	}
	let payload = data.get(pos..pos + length)?;
	Some((tag, payload, &data[pos + length..]))
}

fn trex_defaults(trex: &[u8]) -> Defaults {
	// Full box: version/flags, track_ID, sample_description_index, then
	// the three defaults.
	let field = |at: usize| {
		trex.get(at..at + 4)
			.map(|four| u32::from_be_bytes(four.try_into().expect("4 bytes")))
			.unwrap_or(0)
	};
	Defaults {
		duration: field(12),
		size: field(16),
		flags: field(20),
	}
}

fn mdhd_timescale(mdhd: &[u8]) -> Result<u32, Error> {
	let version = *mdhd.first().ok_or_else(|| Error::Truncated {
		offset: 0,
		what: "the mdhd is empty".into(),
	})?;
	// Version 1 counts creation and modification in 64 bits apiece.
	let at = if version == 1 { 20 } else { 12 };
	let timescale = mdhd
		.get(at..at + 4)
		.map(|four| u32::from_be_bytes(four.try_into().expect("4 bytes")))
		.ok_or_else(|| Error::Truncated {
			offset: 0,
			what: "the mdhd ends before its timescale".into(),
		})?;
	if timescale == 0 {
		return Err(Error::Malformed {
			offset: 0,
			what: "the track's timescale is zero".into(),
		});
	}
	Ok(timescale)
}

/// The payload of the first top-level box of `kind` in a segment.
fn top_level<'a>(data: &'a [u8], kind: &[u8; 4]) -> Result<Option<&'a [u8]>, Error> {
	let mut pos = 0usize;
	while pos < data.len() {
		let rest = &data[pos..];
		let Some(header) = peek_box(rest, pos as u64)? else {
			return Ok(None);
		};
		let size = header.size as usize;
		if rest.len() < size {
			return Ok(None);
		}
		if &header.kind == kind {
			let header_len = if rest[0..4] == [0, 0, 0, 1] { 16 } else { 8 };
			return Ok(Some(&rest[header_len..size]));
		}
		pos += size;
	}
	Ok(None)
}

/// The payload of the first child box of `kind`, starting at `from`.
fn find<'a>(parent: &'a [u8], from: usize, kind: &[u8; 4]) -> Result<Option<&'a [u8]>, Error> {
	let Some(data) = parent.get(from..) else {
		return Ok(None);
	};
	for child in child_boxes(data, 0)? {
		if &child.kind == kind {
			return Ok(Some(child.payload));
		}
	}
	Ok(None)
}

/// One big-endian field of `width` bytes at `pos`, which advances past it.
fn field(data: &[u8], pos: &mut usize, width: usize, what: &str) -> Result<u64, Error> {
	let bytes = data
		.get(*pos..*pos + width)
		.ok_or_else(|| Error::Truncated {
			offset: *pos as u64,
			what: format!("the {what} ends inside its fields"),
		})?;
	*pos += width;
	Ok(bytes
		.iter()
		.fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
}

fn full_flags(data: &[u8], what: &str) -> Result<u32, Error> {
	let four = data.get(0..4).ok_or_else(|| Error::Truncated {
		offset: 0,
		what: format!("the {what} ends inside its version and flags"),
	})?;
	Ok(u32::from_be_bytes(four.try_into().expect("4 bytes")) & 0x00ff_ffff)
}

fn be16(data: &[u8], at: usize) -> Result<u16, Error> {
	data.get(at..at + 2)
		.map(|pair| u16::from_be_bytes(pair.try_into().expect("2 bytes")))
		.ok_or_else(|| Error::Truncated {
			offset: at as u64,
			what: "the sample entry ends inside its fields".into(),
		})
}

fn name(kind: &[u8; 4]) -> String {
	kind.iter()
		.map(|b| {
			if b.is_ascii_graphic() {
				*b as char
			} else {
				'.'
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::mux::{Muxer, Packet};

	// The parameter sets mux's own tests use: constrained baseline, so
	// the avcC needs no extension bytes.
	const EXTRADATA: &[u8] = &[
		0, 0, 0, 1, 0x67, 66, 0xc0, 30, 0xab, 0xcd, // SPS
		0, 0, 0, 1, 0x68, 0xee, 0x06, 0xf2, // PPS
	];

	/// The 5-byte AudioSpecificConfig ffmpeg writes for 48 kHz mono AAC-LC.
	const ASC: &[u8] = &[0x11, 0x88, 0x56, 0xe5, 0x00];

	fn annexb(pts: i64, keyframe: bool) -> Vec<u8> {
		vec![0, 0, 0, 1, if keyframe { 0x65 } else { 0x41 }, pts as u8]
	}

	/// Muxes `packets` and reads every fragment back.
	fn round_trip(
		mut muxer: Muxer,
		packets: Vec<(i64, i64, bool, Vec<u8>)>,
	) -> (Track, Vec<Sample>) {
		let track = Track::read(&muxer.init_segment()).expect("an init segment reads");
		let mut fragments = Vec::new();
		for (pts, dts, keyframe, data) in &packets {
			fragments.extend(
				muxer
					.push(Packet {
						pts: *pts,
						dts: Some(*dts),
						duration: None,
						keyframe: *keyframe,
						data,
					})
					.expect("push"),
			);
		}
		fragments.extend(muxer.finish().expect("finish"));
		let mut samples = Vec::new();
		for fragment in &fragments {
			samples.extend(track.samples(&fragment.bytes).expect("a fragment reads"));
		}
		(track, samples)
	}

	#[test]
	fn a_video_stream_survives_the_mux_and_the_demux() {
		let packets: Vec<(i64, i64, bool, Vec<u8>)> = (0..6i64)
			.map(|pts| (pts, pts, pts % 3 == 0, annexb(pts, pts % 3 == 0)))
			.collect();
		let muxer = Muxer::video(EXTRADATA, 320, 180, 1, 30).expect("a muxer");
		let (track, samples) = round_trip(muxer, packets.clone());

		assert_eq!(track.timescale, 30);
		assert_eq!(
			track.media,
			Media::Video {
				width: 320,
				height: 180,
				avcc: crate::avc::build_avcc(
					&crate::avc::parse_parameter_sets(EXTRADATA).0,
					&crate::avc::parse_parameter_sets(EXTRADATA).1
				)
				.expect("an avcC"),
			}
		);
		assert_eq!(samples.len(), packets.len());
		for (sample, (pts, dts, keyframe, data)) in samples.iter().zip(&packets) {
			assert_eq!((sample.pts, sample.dts), (*pts, *dts));
			assert_eq!(sample.keyframe, *keyframe);
			assert_eq!(sample.duration, 1);
			// h264 samples are stored AVCC-framed; reframed, they are
			// the Annex-B bytes that went in.
			assert_eq!(
				crate::avc::avcc_to_annexb(&sample.data, track.length_size()).expect("annex-b"),
				*data
			);
		}
	}

	#[test]
	fn an_audio_stream_survives_the_mux_and_the_demux() {
		// One tick per sample, an AAC frame every 1024.
		let packets: Vec<(i64, i64, bool, Vec<u8>)> = (0..4i64)
			.map(|index| {
				let pts = index * 1024;
				(pts, pts, true, vec![0x21, index as u8, 0x10, 0x04])
			})
			.collect();
		let muxer = Muxer::audio(ASC, 48000, 1, 1, 48000).expect("a muxer");
		let (track, samples) = round_trip(muxer, packets.clone());

		assert_eq!(track.timescale, 48000);
		assert_eq!(
			track.media,
			Media::Audio {
				sample_rate: 48000,
				channels: 1,
				asc: ASC.to_vec(),
			}
		);
		assert_eq!(samples.len(), packets.len());
		for (sample, (pts, _, _, data)) in samples.iter().zip(&packets) {
			assert_eq!(sample.pts, *pts);
			assert_eq!(sample.duration, 1024);
			// AAC frames are stored exactly as they arrived.
			assert_eq!(&sample.data, data);
			assert!(sample.keyframe, "every AAC frame is a sync sample");
		}
	}

	#[test]
	fn a_reordering_stream_keeps_its_composition_offsets() {
		// Presentation runs ahead of decode: an IPB order where the
		// second sample is presented last.
		let packets = vec![
			(0i64, 0i64, true, annexb(0, true)),
			(3, 1, false, annexb(3, false)),
			(1, 2, false, annexb(1, false)),
			(2, 3, false, annexb(2, false)),
		];
		let muxer = Muxer::video(EXTRADATA, 320, 180, 1, 30).expect("a muxer");
		let (_, samples) = round_trip(muxer, packets.clone());
		assert_eq!(
			samples.iter().map(|s| (s.dts, s.pts)).collect::<Vec<_>>(),
			vec![(0, 0), (1, 3), (2, 1), (3, 2)]
		);
	}

	#[test]
	fn a_time_base_with_a_numerator_scales_into_the_timescale() {
		// At 1001/30000 the muxer multiplies ticks by 1001 on the way
		// into media time, and the timescale it declares is 30000 - so
		// a coded stream at 1/30000 reads the samples back unchanged.
		let packets: Vec<(i64, i64, bool, Vec<u8>)> = (0..3i64)
			.map(|pts| (pts, pts, pts == 0, annexb(pts, pts == 0)))
			.collect();
		let muxer = Muxer::video(EXTRADATA, 320, 180, 1001, 30000).expect("a muxer");
		let (track, samples) = round_trip(muxer, packets);
		assert_eq!(track.timescale, 30000);
		assert_eq!(
			samples.iter().map(|s| s.dts).collect::<Vec<_>>(),
			vec![0, 1001, 2002]
		);
		assert!(samples.iter().all(|s| s.duration == 1001));
	}

	#[test]
	fn a_fragment_opening_on_anything_but_a_moof_is_refused() {
		let muxer = Muxer::video(EXTRADATA, 320, 180, 1, 30).expect("a muxer");
		let track = Track::read(&muxer.init_segment()).expect("an init segment reads");
		let err = track
			.samples(&[0, 0, 0, 8, b'f', b'r', b'e', b'e'])
			.expect_err("a refusal");
		assert!(err.to_string().contains("not moof"), "{err}");
	}

	#[test]
	fn an_init_segment_of_another_codec_is_refused() {
		// A moov whose sample entry is neither avc1 nor mp4a is named,
		// not silently read as one of them.
		let muxer = Muxer::video(EXTRADATA, 320, 180, 1, 30).expect("a muxer");
		let mut init = muxer.init_segment();
		// The last "avc1" is the sample entry's; the first is one of the
		// ftyp's compatible brands.
		let at = init
			.windows(4)
			.rposition(|four| four == b"avc1")
			.expect("the entry is there");
		init[at..at + 4].copy_from_slice(b"hev1");
		let err = Track::read(&init).expect_err("a refusal");
		assert!(err.to_string().contains("not avc1 or mp4a"), "{err}");
	}
}

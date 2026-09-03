//! The catalog a broadcast describes itself with, in the shape the hang
//! media layer reads (github.com/kixelated/moq, the `hang` crate).
//!
//! A subscriber that has only a broadcast path needs somewhere to read
//! what is on offer before it can name a track. hang's convention is a
//! `catalog.json` track carrying one JSON document, latest group wins:
//! a `video` and an `audio` section, each a map of rendition name to a
//! WebCodecs-style decoder config in camelCase, and each rendition
//! naming its container. Ours is `cmaf` - every MoQ frame one
//! `moof`+`mdat` fragment - whose init segment (`ftyp`+`moov`) rides
//! INSIDE the catalog entry as base64, not on a track of its own.
//!
//! Built by hand against hang 0.20.7's `catalog` module rather than by
//! depending on the crate: this side compiles to wasm32-wasip2 inside
//! the publish module, and the document is plain serde. The live test's
//! harness parses what this writes with hang's own parser, which is
//! what keeps the two from drifting.

use serde::Serialize;

/// The track a broadcast describes itself on: hang's `Catalog::DEFAULT_NAME`.
pub const TRACK: &str = "catalog.json";

/// The buffer depth recommended to AUDIO readers, in milliseconds.
/// Live capture hands the pipeline audio in bursts hundreds of
/// milliseconds wide; a reader holding this much plays through them.
/// Video gets no recommendation: it arrives smoothly, and a held-back
/// picture at the live edge is skipped rather than shown.
pub const AUDIO_JITTER_MS: u32 = 600;

/// One video rendition's entry, hang's `VideoConfig` subset.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoRendition {
	/// RFC 6381, e.g. `avc1.64001f`, read off the stream's own avcC.
	pub codec: String,
	/// The decoder configuration, hex: the avcC record. `avc1` carries
	/// its parameter sets out of band, and this is what says so to a
	/// reader that does not open the init segment.
	pub description: String,
	pub coded_width: u32,
	pub coded_height: u32,
	pub container: Container,
}

/// One audio rendition's entry, hang's `AudioConfig` subset.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioRendition {
	/// RFC 6381, e.g. `mp4a.40.2`, read off the AudioSpecificConfig.
	pub codec: String,
	/// The decoder configuration, hex: the AudioSpecificConfig itself.
	pub description: String,
	pub sample_rate: u32,
	pub number_of_channels: u32,
	/// Capture bursts become latency, not gaps.
	pub jitter: u32,
	pub container: Container,
}

/// The container a rendition's frames travel in: `cmaf`, the init
/// segment carried inside the entry.
#[derive(Debug, PartialEq)]
pub struct Container {
	/// The `ftyp`+`moov` a decoder reads before any fragment.
	pub init: Vec<u8>,
}

impl Serialize for Container {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		#[derive(Serialize)]
		struct Cmaf<'a> {
			kind: &'static str,
			init: &'a str,
		}
		Cmaf {
			kind: "cmaf",
			init: &base64(&self.init),
		}
		.serialize(serializer)
	}
}

/// One track of the catalog: its name, and the entry its kind calls for.
#[derive(Debug, PartialEq)]
pub enum Track {
	Video(String, VideoRendition),
	Audio(String, AudioRendition),
}

impl Track {
	/// A video track, described by the frames it carries, its decoder
	/// configuration (the avcC record) and the init segment.
	pub fn video(
		name: String,
		codec: String,
		config: &[u8],
		width: u32,
		height: u32,
		init: Vec<u8>,
	) -> Self {
		Track::Video(
			name,
			VideoRendition {
				codec,
				description: hex(config),
				coded_width: width,
				coded_height: height,
				container: Container { init },
			},
		)
	}

	/// An audio track, described the same way; the configuration is the
	/// AudioSpecificConfig.
	pub fn audio(
		name: String,
		codec: String,
		config: &[u8],
		sample_rate: u32,
		channels: u32,
		init: Vec<u8>,
	) -> Self {
		Track::Audio(
			name,
			AudioRendition {
				codec,
				description: hex(config),
				sample_rate,
				number_of_channels: channels,
				jitter: AUDIO_JITTER_MS,
				container: Container { init },
			},
		)
	}

	/// The track's name, whichever kind it is.
	pub fn name(&self) -> &str {
		match self {
			Track::Video(name, _) | Track::Audio(name, _) => name,
		}
	}
}

/// What a subscriber reads off [`TRACK`] to choose a rendition.
#[derive(Debug, PartialEq, Serialize)]
pub struct Catalog {
	video: Renditions<VideoRendition>,
	audio: Renditions<AudioRendition>,
}

#[derive(Debug, PartialEq, Serialize)]
struct Renditions<T> {
	// serde_json's map sorts keys, matching hang's own BTreeMap: the
	// document's order is alphabetical whatever order the tracks came in.
	renditions: std::collections::BTreeMap<String, T>,
}

impl Catalog {
	pub fn new(tracks: Vec<Track>) -> Self {
		let mut video = std::collections::BTreeMap::new();
		let mut audio = std::collections::BTreeMap::new();
		for track in tracks {
			match track {
				Track::Video(name, entry) => {
					video.insert(name, entry);
				}
				Track::Audio(name, entry) => {
					audio.insert(name, entry);
				}
			}
		}
		Catalog {
			video: Renditions { renditions: video },
			audio: Renditions { renditions: audio },
		}
	}

	/// The document as the bytes one MoQ frame carries.
	pub fn document(&self) -> Result<Vec<u8>, String> {
		serde_json::to_vec(self).map_err(|err| format!("catalog: {err}"))
	}
}

/// Lowercase hex, the coding the catalog's `description` field uses.
fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Standard base64 with padding, the alphabet hang's `serde_with` field
/// uses. Hand-rolled: the tree carries no base64 crate, and encoding is
/// twenty lines.
fn base64(bytes: &[u8]) -> String {
	const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
		let word = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
		for i in 0..4 {
			if i <= chunk.len() {
				out.push(ALPHABET[(word >> (18 - 6 * i)) as usize & 0x3f] as char);
			} else {
				out.push('=');
			}
		}
	}
	out
}

/// The track names a broadcast's renditions publish under.
///
/// ONE stream keeps the base name as written, so a broadcast that
/// carried a single track goes on carrying exactly that one. SEVERAL
/// name a rendition apiece under it, by the frame height each encodes -
/// which is what tells a subscriber which is which - and a height two of
/// them share takes its position too, to stay distinct.
pub fn track_names(base: &str, heights: &[u32]) -> Vec<String> {
	if heights.len() == 1 {
		return vec![base.to_string()];
	}
	heights
		.iter()
		.enumerate()
		.map(|(index, height)| {
			if heights.iter().filter(|other| *other == height).count() > 1 {
				format!("{base}.{height}p.{index}")
			} else {
				format!("{base}.{height}p")
			}
		})
		.collect()
}

/// What one sink pad carries, past its row and its rendition's name -
/// enough to name its MoQ track without depending on the wit types
/// `init` reads them off. A video pad also carries the frame height
/// the fallback naming reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowKind {
	Video { height: u32 },
	Audio,
}

/// One pad, in the order a packet sink's `init` receives it: which
/// relation row it belongs to, and what the row's rendition-meta said
/// its name was, if anything did.
#[derive(Debug, Clone)]
pub struct RowPad {
	pub row: u32,
	pub kind: RowKind,
	pub name: Option<String>,
}

/// Names a sink's tracks from the relation rows it was handed, in pad
/// order. A row is one rendition: a video pad and an audio pad
/// sharing a row are one muxed rendition, and either alone is its
/// own - so three pads on rows `[0, 0, 1]` are two renditions, one
/// muxed and one on its own.
///
/// A pad's own name - what its row's rendition-meta said, read off a
/// manifest or another broadcast's catalog - is used as written.
/// Without one, a video pad falls back to [`track_names`], computed
/// over only the unnamed video pads so a lone one still keeps the
/// plain `video_base`; an audio pad falls back to `audio_base`,
/// numbered past the first. MoQ track names are one flat namespace
/// per broadcast, so a muxed row's audio pad cannot repeat its video
/// pad's explicit name - it qualifies it with ".audio" instead.
pub fn track_names_for_rows(pads: &[RowPad], video_base: &str, audio_base: &str) -> Vec<String> {
	let mut row_has_video: std::collections::HashMap<u32, bool> = std::collections::HashMap::new();
	for pad in pads {
		if let RowKind::Video { .. } = pad.kind {
			row_has_video.insert(pad.row, true);
		}
	}

	let unnamed_heights: Vec<u32> = pads
		.iter()
		.filter_map(|pad| match (pad.kind, &pad.name) {
			(RowKind::Video { height }, None) => Some(height),
			_ => None,
		})
		.collect();
	let mut fallback_video_names = track_names(video_base, &unnamed_heights).into_iter();

	let mut unnamed_audio_ordinal = 0u32;
	pads
		.iter()
		.map(|pad| match pad.kind {
			RowKind::Video { .. } => match &pad.name {
				Some(name) => name.clone(),
				None => fallback_video_names
					.next()
					.expect("one fallback name per unnamed video pad"),
			},
			RowKind::Audio => match &pad.name {
				Some(name) if row_has_video.get(&pad.row).copied().unwrap_or(false) => {
					format!("{name}.audio")
				}
				Some(name) => name.clone(),
				None => {
					unnamed_audio_ordinal += 1;
					match unnamed_audio_ordinal {
						1 => audio_base.to_string(),
						nth => format!("{audio_base}.{}", nth - 1),
					}
				}
			},
		})
		.collect()
}

/// The RFC 6381 codec string an `avcC` record spells: the profile, the
/// constraint flags and the level, which are its bytes 1 through 3. The
/// fallback for a stream that names no profile or level of its own.
pub fn avc_codec(avcc: &[u8]) -> String {
	match avcc.get(1..4) {
		Some(triple) => format!("avc1.{:02x}{:02x}{:02x}", triple[0], triple[1], triple[2]),
		None => "avc1".to_string(),
	}
}

/// The same codec string from the stream's own profile and level, the
/// preferred source: a stream whose extradata is empty still names them.
/// The constraint flags are the one byte they do not carry, read off the
/// `avcC` where it has one and zero otherwise.
pub fn avc_codec_from(profile: i32, level: i32, avcc: &[u8]) -> String {
	let constraints = avcc.get(2).copied().unwrap_or(0);
	format!(
		"avc1.{:02x}{constraints:02x}{:02x}",
		profile as u8, level as u8
	)
}

/// The RFC 6381 codec string an AudioSpecificConfig spells: `mp4a.40`,
/// the object type indication MPEG-4 audio takes, then the audio object
/// type - the config's first five bits, or the escape's six more.
pub fn aac_codec(asc: &[u8]) -> String {
	let Some(&first) = asc.first() else {
		return "mp4a.40".to_string();
	};
	let object_type = match first >> 3 {
		31 => 32 + ((u32::from(first & 0x07) << 3) | u32::from(asc.get(1).copied().unwrap_or(0) >> 5)),
		short => u32::from(short),
	};
	format!("mp4a.40.{object_type}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn one_stream_keeps_the_track_name_as_written() {
		assert_eq!(track_names("video", &[720]), vec!["video"]);
	}

	#[test]
	fn several_streams_name_a_rendition_apiece_by_height() {
		assert_eq!(
			track_names("video", &[480, 360, 240]),
			vec!["video.480p", "video.360p", "video.240p"]
		);
	}

	#[test]
	fn renditions_of_one_height_stay_distinct() {
		assert_eq!(
			track_names("video", &[720, 720]),
			vec!["video.720p.0", "video.720p.1"]
		);
	}

	#[test]
	fn only_the_shared_height_takes_a_position() {
		assert_eq!(
			track_names("video", &[720, 720, 360]),
			vec!["video.720p.0", "video.720p.1", "video.360p"]
		);
	}

	#[test]
	fn rows_zero_zero_one_are_a_muxed_rendition_and_an_audio_only_one() {
		// Three pads: a video and an audio on row 0 (one muxed
		// rendition, unnamed - the old plain "video"/"audio" defaults),
		// and an audio alone on row 1 (its own rendition, numbered past
		// the first).
		let pads = vec![
			RowPad {
				row: 0,
				kind: RowKind::Video { height: 720 },
				name: None,
			},
			RowPad {
				row: 0,
				kind: RowKind::Audio,
				name: None,
			},
			RowPad {
				row: 1,
				kind: RowKind::Audio,
				name: None,
			},
		];
		assert_eq!(
			track_names_for_rows(&pads, "video", "audio"),
			vec!["video", "audio", "audio.1"]
		);
	}

	#[test]
	fn an_explicit_rendition_name_is_used_as_written() {
		// An audio-only row with a rendition name reads it straight off,
		// no numbering.
		let pads = vec![RowPad {
			row: 0,
			kind: RowKind::Audio,
			name: Some("commentary".to_string()),
		}];
		assert_eq!(track_names_for_rows(&pads, "video", "audio"), vec!["commentary"]);
	}

	#[test]
	fn a_muxed_rows_named_audio_pad_is_qualified_against_its_video_pad() {
		// Row 0 pairs a video and an audio pad under the same explicit
		// name: the video pad keeps it, the audio pad cannot repeat it
		// (MoQ track names are one flat namespace), so it is qualified.
		let pads = vec![
			RowPad {
				row: 0,
				kind: RowKind::Video { height: 1080 },
				name: Some("1080p".to_string()),
			},
			RowPad {
				row: 0,
				kind: RowKind::Audio,
				name: Some("1080p".to_string()),
			},
		];
		assert_eq!(
			track_names_for_rows(&pads, "video", "audio"),
			vec!["1080p", "1080p.audio"]
		);
	}

	#[test]
	fn unnamed_video_rows_still_fall_back_to_height_derived_names() {
		// Two unnamed video-only rows behave exactly as `track_names`
		// over their heights, undisturbed by an explicit-name row mixed
		// among them.
		let pads = vec![
			RowPad {
				row: 0,
				kind: RowKind::Video { height: 480 },
				name: None,
			},
			RowPad {
				row: 1,
				kind: RowKind::Video { height: 720 },
				name: Some("hd".to_string()),
			},
			RowPad {
				row: 2,
				kind: RowKind::Video { height: 240 },
				name: None,
			},
		];
		assert_eq!(
			track_names_for_rows(&pads, "video", "audio"),
			vec!["video.480p", "hd", "video.240p"]
		);
	}

	#[test]
	fn the_codec_string_is_the_avcc_profile_and_level() {
		// avcC: configurationVersion, profile 0x64, compat 0x00, level 0x1f.
		assert_eq!(avc_codec(&[1, 0x64, 0x00, 0x1f, 0xff]), "avc1.64001f");
	}

	#[test]
	fn an_unreadable_avcc_still_names_the_codec() {
		assert_eq!(avc_codec(&[1, 0x64]), "avc1");
	}

	#[test]
	fn the_streams_own_profile_and_level_agree_with_the_avcc() {
		// An intact avcC and the stream's numbers spell the same string;
		// the middle byte is the avcC's, the outer two the stream's.
		let avcc = [1u8, 0x64, 0x00, 0x1f, 0xff];
		assert_eq!(avc_codec_from(0x64, 0x1f, &avcc), avc_codec(&avcc));
		assert_eq!(avc_codec_from(0x64, 0x1f, &avcc), "avc1.64001f");
		// Constrained baseline: the constraint flags come off the avcC.
		assert_eq!(
			avc_codec_from(66, 30, &[1, 0x42, 0xc0, 0x1e]),
			"avc1.42c01e"
		);
	}

	#[test]
	fn without_an_avcc_the_constraint_flags_read_zero() {
		assert_eq!(avc_codec_from(0x64, 0x28, &[]), "avc1.640028");
	}

	#[test]
	fn the_codec_string_is_the_configs_audio_object_type() {
		// The 48 kHz mono AAC-LC config ffmpeg writes: object type 2 in
		// the first five bits, which is what ffprobe calls mp4a.40.2.
		assert_eq!(aac_codec(&[0x11, 0x88, 0x56, 0xe5, 0x00]), "mp4a.40.2");
		// 44.1 kHz stereo, the same object type.
		assert_eq!(aac_codec(&[0x12, 0x10, 0x56, 0xe5, 0x00]), "mp4a.40.2");
	}

	#[test]
	fn an_escaped_object_type_reads_past_the_first_five_bits() {
		// 31 in the first five bits escapes: the type is 32 plus the
		// six bits that follow. 0xf8 0x40 -> 32 + 2 = 34.
		assert_eq!(aac_codec(&[0xf8, 0x40]), "mp4a.40.34");
	}

	#[test]
	fn an_empty_config_still_names_the_codec() {
		assert_eq!(aac_codec(&[]), "mp4a.40");
	}

	#[test]
	fn base64_matches_the_known_vectors() {
		// hang's own container test: [0,1,2] carries as "AAEC".
		assert_eq!(base64(&[0, 1, 2]), "AAEC");
		// RFC 4648's vectors, padding included.
		assert_eq!(base64(b""), "");
		assert_eq!(base64(b"f"), "Zg==");
		assert_eq!(base64(b"fo"), "Zm8=");
		assert_eq!(base64(b"foo"), "Zm9v");
		assert_eq!(base64(b"foob"), "Zm9vYg==");
		assert_eq!(base64(b"fooba"), "Zm9vYmE=");
		assert_eq!(base64(b"foobar"), "Zm9vYmFy");
	}

	#[test]
	fn the_document_is_hangs_shape() {
		let catalog = Catalog::new(vec![
			Track::video(
				"video.480p".into(),
				"avc1.64001f".into(),
				&[1, 0x64, 0x00, 0x1f],
				854,
				480,
				vec![0, 1, 2],
			),
			Track::video(
				"video.240p".into(),
				"avc1.640015".into(),
				&[1, 0x64, 0x00, 0x15],
				426,
				240,
				vec![3],
			),
		]);
		let document = String::from_utf8(catalog.document().expect("serializes")).unwrap();
		// Renditions are a map sorted by name; every key is camelCase;
		// the description is hex; the init rides inside the entry as base64.
		assert_eq!(
			document,
			r#"{"video":{"renditions":{"video.240p":{"codec":"avc1.640015","description":"01640015","codedWidth":426,"codedHeight":240,"container":{"kind":"cmaf","init":"Aw=="}},"video.480p":{"codec":"avc1.64001f","description":"0164001f","codedWidth":854,"codedHeight":480,"container":{"kind":"cmaf","init":"AAEC"}}}},"audio":{"renditions":{}}}"#
		);
	}

	#[test]
	fn an_audio_rendition_says_its_rate_and_channels() {
		let catalog = Catalog::new(vec![Track::audio(
			"audio".into(),
			"mp4a.40.2".into(),
			&[0x11, 0x90],
			48000,
			2,
			vec![0, 1, 2],
		)]);
		let document = String::from_utf8(catalog.document().expect("serializes")).unwrap();
		assert_eq!(
			document,
			r#"{"video":{"renditions":{}},"audio":{"renditions":{"audio":{"codec":"mp4a.40.2","description":"1190","sampleRate":48000,"numberOfChannels":2,"jitter":600,"container":{"kind":"cmaf","init":"AAEC"}}}}}"#
		);
	}
}

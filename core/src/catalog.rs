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

/// The RFC 6381 codec string an `avcC` record spells: the profile, the
/// constraint flags and the level, which are its bytes 1 through 3.
pub fn avc_codec(avcc: &[u8]) -> String {
	match avcc.get(1..4) {
		Some(triple) => format!("avc1.{:02x}{:02x}{:02x}", triple[0], triple[1], triple[2]),
		None => "avc1".to_string(),
	}
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
	fn the_codec_string_is_the_avcc_profile_and_level() {
		// avcC: configurationVersion, profile 0x64, compat 0x00, level 0x1f.
		assert_eq!(avc_codec(&[1, 0x64, 0x00, 0x1f, 0xff]), "avc1.64001f");
	}

	#[test]
	fn an_unreadable_avcc_still_names_the_codec() {
		assert_eq!(avc_codec(&[1, 0x64]), "avc1");
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
			r#"{"video":{"renditions":{}},"audio":{"renditions":{"audio":{"codec":"mp4a.40.2","description":"1190","sampleRate":48000,"numberOfChannels":2,"container":{"kind":"cmaf","init":"AAEC"}}}}}"#
		);
	}
}

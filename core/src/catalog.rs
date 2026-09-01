//! The catalog a broadcast describes itself with, and the names its
//! tracks take.
//!
//! A subscriber that has only a broadcast path needs somewhere to read
//! what is on offer before it can name a track. moq-rs's convention is a
//! `catalog.json` track carrying one JSON document, latest group wins;
//! this is that document, plus the naming that decides what is in it.

use serde::Serialize;

/// The track a broadcast describes itself on.
pub const TRACK: &str = "catalog.json";

/// The document's shape, so a reader can tell it from a later one.
pub const VERSION: u32 = 1;

/// One track of the catalog: what it carries and where its init segment
/// is.
#[derive(Debug, PartialEq, Serialize)]
pub struct Track {
	pub name: String,
	pub init: String,
	pub kind: &'static str,
	/// RFC 6381, e.g. `avc1.64001f`, read off the stream's own avcC.
	pub codec: String,
	pub width: u32,
	pub height: u32,
}

/// What a subscriber reads off [`TRACK`] to choose a rendition.
#[derive(Debug, PartialEq, Serialize)]
pub struct Catalog {
	pub version: u32,
	pub tracks: Vec<Track>,
}

impl Catalog {
	pub fn new(tracks: Vec<Track>) -> Self {
		Catalog {
			version: VERSION,
			tracks,
		}
	}

	/// The document as the bytes one MoQ frame carries.
	pub fn document(&self) -> Result<Vec<u8>, String> {
		serde_json::to_vec(self).map_err(|err| format!("catalog: {err}"))
	}
}

/// The init segment's track, beside the media track it describes.
pub fn init_name(track: &str) -> String {
	format!("{track}.init")
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
	fn the_document_names_every_track_and_its_init() {
		let catalog = Catalog::new(vec![
			Track {
				name: "video.480p".into(),
				init: init_name("video.480p"),
				kind: "video",
				codec: "avc1.64001f".into(),
				width: 854,
				height: 480,
			},
			Track {
				name: "video.240p".into(),
				init: init_name("video.240p"),
				kind: "video",
				codec: "avc1.640015".into(),
				width: 426,
				height: 240,
			},
		]);
		let document = String::from_utf8(catalog.document().expect("serializes")).unwrap();
		assert_eq!(
			document,
			r#"{"version":1,"tracks":[{"name":"video.480p","init":"video.480p.init","kind":"video","codec":"avc1.64001f","width":854,"height":480},{"name":"video.240p","init":"video.240p.init","kind":"video","codec":"avc1.640015","width":426,"height":240}]}"#
		);
	}
}

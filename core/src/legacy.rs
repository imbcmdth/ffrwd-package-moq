//! Media in hang's `legacy` container: what libmoq, the library under
//! the moq-dev OBS plugin, publishes.
//!
//! Every frame is a QUIC varint of the sample's pts in microseconds and
//! then the sample itself, the same framing a data track's message takes
//! ([`crate::message`]): an access unit in Annex B for `avc3`, `hev1` and
//! their out-of-band twins, a temporal unit of OBUs for `av01`, a raw
//! frame for AAC. The catalog carries no init segment, and for the in-band
//! video codecs no description either: the parameter sets ride in the
//! keyframes.
//!
//! Nothing says which frames are keyframes, so it is read off the sample:
//! an IDR slice for H.264, an IRAP picture for HEVC, a sequence header for
//! AV1 (an encoder writes one ahead of every key frame). Nor is there a
//! decode time. A frame's pts is its dts as long as the pts keep rising,
//! which is what a stream without B-frames does; one that reorders has no
//! decode time this can recover, and [`Order`] says so.

/// A legacy rendition's codec, from the RFC 6381 string its catalog entry
/// spells.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
	H264,
	Hevc,
	Av1,
	Aac,
}

impl Codec {
	/// The codec a catalog's `codec` string names, or why this does not
	/// read it.
	pub fn of(codec: &str) -> Result<Self, String> {
		let family = codec.split('.').next().unwrap_or(codec);
		match family {
			"avc1" | "avc3" => Ok(Codec::H264),
			"hev1" | "hvc1" => Ok(Codec::Hevc),
			"av01" => Ok(Codec::Av1),
			"mp4a" if codec.starts_with("mp4a.40.") => Ok(Codec::Aac),
			"opus" | "Opus" | "flac" | "fLaC" => Err(format!(
				"'{codec}' audio is not something ffrwd's packet wire carries yet, which is h264, \
				 hevc and av1 video and aac audio; have the publisher send AAC"
			)),
			_ => Err(format!(
				"'{codec}' in the legacy container is not one this reads, which is h264 ('avc1', \
				 'avc3'), hevc ('hev1', 'hvc1'), av1 ('av01') and aac ('mp4a.40')"
			)),
		}
	}

	/// The codec name a coded stream is published under.
	pub fn name(self) -> &'static str {
		match self {
			Codec::H264 => "h264",
			Codec::Hevc => "hevc",
			Codec::Av1 => "av1",
			Codec::Aac => "aac",
		}
	}

	pub fn video(self) -> bool {
		self != Codec::Aac
	}
}

/// One frame taken apart: the sample's pts in microseconds, the sample,
/// and whether a decoder can start at it.
#[derive(Debug, PartialEq)]
pub struct Sample<'a> {
	pub pts_us: u64,
	pub data: &'a [u8],
	pub keyframe: bool,
}

/// A legacy frame back to its sample.
pub fn read(codec: Codec, frame: &[u8]) -> Result<Sample<'_>, String> {
	let (pts_us, data) = crate::message::decode(frame)?;
	if data.is_empty() {
		return Err(format!("a frame at {pts_us}us carries no sample"));
	}
	Ok(Sample {
		pts_us,
		data,
		keyframe: keyframe(codec, data),
	})
}

/// Whether a decoder can start at this sample.
pub fn keyframe(codec: Codec, sample: &[u8]) -> bool {
	let h26x = match codec {
		Codec::Aac => return true,
		Codec::Av1 => {
			return ffrwd_nal::obu::scan_obus(sample)
				.map(|obus| obus.iter().any(|obu| obu.kind == ffrwd_nal::obu::OBU_SEQUENCE_HEADER))
				.unwrap_or(false)
		}
		Codec::H264 => ffrwd_nal::Codec::H264,
		Codec::Hevc => ffrwd_nal::Codec::H265,
	};
	ffrwd_nal::annexb::split_nals(sample)
		.iter()
		.filter_map(|nal| h26x.nal_type(nal))
		.any(|kind| h26x.is_keyframe_type(kind))
}

/// An H.264 codec string's profile and level: `avc3.64001f` is profile
/// 100, level 31. The constraint byte between them is not wanted.
pub fn avc_profile_level(codec: &str) -> Option<(i32, i32)> {
	let hex = codec.split('.').nth(1)?;
	if hex.len() != 6 {
		return None;
	}
	let profile = i32::from_str_radix(&hex[0..2], 16).ok()?;
	let level = i32::from_str_radix(&hex[4..6], 16).ok()?;
	Some((profile, level))
}

/// A track's decode times, which the legacy container does not carry.
///
/// A sample's pts is its dts while the pts keep rising. A pts that goes
/// back is a reordered picture - a B-frame - whose decode time the frame
/// never said and nothing here can recover without guessing the reorder
/// depth, so it is refused by name rather than handed on out of order.
#[derive(Debug, Default)]
pub struct Order {
	last_pts_us: Option<u64>,
}

impl Order {
	/// The dts of the next sample in decode order, in microseconds.
	pub fn dts(&mut self, name: &str, pts_us: u64) -> Result<u64, String> {
		if let Some(last) = self.last_pts_us {
			if pts_us <= last {
				return Err(format!(
					"track '{name}' went back from {last}us to {pts_us}us: its encoder reorders \
					 pictures (B-frames), and the legacy container carries no decode time to \
					 read them by. Set the encoder's B-frames to 0"
				));
			}
		}
		self.last_pts_us = Some(pts_us);
		Ok(pts_us)
	}

	/// The track was taken up again: its next sample starts a new run.
	pub fn restart(&mut self) {
		self.last_pts_us = None;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The head of a keyframe OBS sent as `1.avc3`: the varint pts, an
	/// access unit delimiter, then the SPS; an IDR slice follows in the
	/// real frame, and is appended here.
	fn obs_keyframe() -> Vec<u8> {
		let mut frame = vec![0xa1, 0x16, 0x27, 0x60];
		frame.extend_from_slice(&[0, 0, 0, 1, 0x09, 0x10]);
		frame.extend_from_slice(&[0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1f, 0xac, 0xb4]);
		frame.extend_from_slice(&[0, 0, 0, 1, 0x65, 0x88, 0x84]);
		frame
	}

	#[test]
	fn an_obs_keyframe_reads_as_its_pts_and_an_idr() {
		let frame = obs_keyframe();
		let sample = read(Codec::H264, &frame).expect("reads");
		assert_eq!(sample.pts_us, 0x2116_2760);
		assert!(sample.keyframe);
		assert_eq!(sample.data, &frame[4..]);
	}

	#[test]
	fn a_predicted_picture_is_no_keyframe() {
		// OBS's second frame of the same group: AUD then a non-IDR slice.
		let frame = [0xa1, 0x16, 0xa9, 0x95, 0, 0, 0, 1, 0x09, 0x30, 0, 0, 0, 1, 0x41, 0x9a, 0x26];
		assert!(!read(Codec::H264, &frame).unwrap().keyframe);
	}

	#[test]
	fn hevc_irap_and_av1_sequence_headers_are_keyframes() {
		// An HEVC IDR_W_RADL (type 19) and a TRAIL_R (type 1).
		assert!(keyframe(Codec::Hevc, &[0, 0, 0, 1, 19 << 1, 1, 0xaf]));
		assert!(!keyframe(Codec::Hevc, &[0, 0, 0, 1, 1 << 1, 1, 0xaf]));
		// A temporal delimiter, then a sequence header, both with sizes.
		let key = [0x12, 0x00, 0x0a, 0x02, 0xaa, 0xbb];
		let delta = [0x12, 0x00, 0x32, 0x01, 0xcc];
		assert!(keyframe(Codec::Av1, &key));
		assert!(!keyframe(Codec::Av1, &delta));
	}

	#[test]
	fn every_aac_frame_is_a_keyframe() {
		// OBS's `0.aac`: the varint pts, then a raw frame.
		let frame = [0xa1, 0x39, 0x2a, 0xea, 0x21, 0x1a, 0xd4, 0x85];
		let sample = read(Codec::Aac, &frame).unwrap();
		assert_eq!(sample.pts_us, 0x2139_2aea);
		assert!(sample.keyframe);
	}

	#[test]
	fn codecs_are_read_off_the_catalog_string() {
		assert_eq!(Codec::of("avc3.64001f"), Ok(Codec::H264));
		assert_eq!(Codec::of("hev1.1.6.L93.B0"), Ok(Codec::Hevc));
		assert_eq!(Codec::of("av01.0.08M.08"), Ok(Codec::Av1));
		assert_eq!(Codec::of("mp4a.40.2"), Ok(Codec::Aac));
		assert!(Codec::of("opus").unwrap_err().contains("AAC"));
		assert!(Codec::of("vp09.00.10.08").unwrap_err().contains("'vp09.00.10.08'"));
	}

	#[test]
	fn a_codec_string_gives_profile_and_level() {
		assert_eq!(avc_profile_level("avc3.64001f"), Some((100, 31)));
		assert_eq!(avc_profile_level("avc1.42c01e"), Some((66, 30)));
		assert_eq!(avc_profile_level("avc3"), None);
	}

	#[test]
	fn a_rising_pts_is_its_own_dts_and_a_reorder_is_refused() {
		let mut order = Order::default();
		assert_eq!(order.dts("v", 100), Ok(100));
		assert_eq!(order.dts("v", 133), Ok(133));
		let err = order.dts("v", 120).unwrap_err();
		assert!(err.contains("B-frames") && err.contains("'v'"), "{err}");
		order.restart();
		assert_eq!(order.dts("v", 50), Ok(50));
	}

	#[test]
	fn a_frame_without_a_sample_is_refused() {
		assert!(read(Codec::Aac, &[0x05]).unwrap_err().contains("no sample"));
	}
}

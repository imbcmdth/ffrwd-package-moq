//! h264 byte-level knowledge: Annex-B NAL cutting, AVCC length-prefix
//! framing, and the `avcC` decoder configuration record.
//!
//! The encoded edge hands a packet sink Annex-B bytes - start-coded NAL
//! units, the out-of-band SPS/PPS the same way. MP4 wants the opposite
//! spelling: each NAL prefixed with its 4-byte length, and the SPS/PPS
//! gathered into an `avcC` record inside the sample entry. Pure
//! reframing both ways; no NAL payload is touched.

/// The NAL units of an Annex-B byte stream, start codes removed.
///
/// Both 3-byte (`00 00 01`) and 4-byte (`00 00 00 01`) start codes cut;
/// a zero byte directly before a 3-byte code belongs to the code, not
/// to the NAL before it. Bytes before the first start code are ignored,
/// as ffmpeg ignores them.
pub fn split_nals(annexb: &[u8]) -> Vec<&[u8]> {
	let mut nals = Vec::new();
	let mut start = None;
	let mut i = 0;
	while i + 2 < annexb.len() {
		if annexb[i] == 0 && annexb[i + 1] == 0 && annexb[i + 2] == 1 {
			if let Some(from) = start {
				let mut end = i;
				// A 4-byte start code's leading zero is the code's.
				if end > from && annexb[end - 1] == 0 {
					end -= 1;
				}
				nals.push(&annexb[from..end]);
			}
			start = Some(i + 3);
			i += 3;
		} else {
			i += 1;
		}
	}
	if let Some(from) = start {
		nals.push(&annexb[from..]);
	}
	nals
}

/// One Annex-B packet reframed as AVCC: each NAL as 4-byte big-endian
/// length plus payload. Empty when the packet holds no start code.
pub fn annexb_to_avcc(annexb: &[u8]) -> Vec<u8> {
	let nals = split_nals(annexb);
	let mut out = Vec::with_capacity(annexb.len());
	for nal in nals {
		out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
		out.extend_from_slice(nal);
	}
	out
}

/// The SPS and PPS NAL units of an Annex-B extradata blob, in order.
pub fn parse_parameter_sets(annexb: &[u8]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
	let mut sps = Vec::new();
	let mut pps = Vec::new();
	for nal in split_nals(annexb) {
		match nal.first().map(|b| b & 0x1f) {
			Some(7) => sps.push(nal.to_vec()),
			Some(8) => pps.push(nal.to_vec()),
			_ => {}
		}
	}
	(sps, pps)
}

/// Builds the `avcC` box payload (AVCDecoderConfigurationRecord) from
/// SPS and PPS NAL units, 4-byte lengths declared.
///
/// For the high profiles the record carries chroma format and bit
/// depths read out of the first SPS, the way ffmpeg's own writer does;
/// the baseline family leaves them off.
pub fn build_avcc(sps: &[Vec<u8>], pps: &[Vec<u8>]) -> Result<Vec<u8>, String> {
	let first = sps
		.first()
		.filter(|s| s.len() >= 4)
		.ok_or("extradata carries no SPS")?;
	if pps.is_empty() {
		return Err("extradata carries no PPS".into());
	}
	if sps.len() > 31 {
		return Err(format!("{} SPS do not fit an avcC", sps.len()));
	}

	let mut avcc = Vec::new();
	avcc.push(1); // configurationVersion
	avcc.extend_from_slice(&first[1..4]); // profile, compat, level
	avcc.push(0xfc | 3); // lengthSizeMinusOne = 3
	avcc.push(0xe0 | sps.len() as u8);
	for s in sps {
		avcc.extend_from_slice(&(s.len() as u16).to_be_bytes());
		avcc.extend_from_slice(s);
	}
	avcc.push(pps.len() as u8);
	for p in pps {
		avcc.extend_from_slice(&(p.len() as u16).to_be_bytes());
		avcc.extend_from_slice(p);
	}

	// The extension bytes, for profiles outside the baseline family.
	let profile = first[1];
	if !matches!(profile, 66 | 77 | 88) {
		let parsed = parse_sps_formats(first)
			.ok_or("SPS of a high profile too short to read its formats")?;
		avcc.push(0xfc | (parsed.chroma_format_idc & 0x3));
		avcc.push(0xf8 | (parsed.bit_depth_luma_minus8 & 0x7));
		avcc.push(0xf8 | (parsed.bit_depth_chroma_minus8 & 0x7));
		avcc.push(0); // numOfSequenceParameterSetExt
	}
	Ok(avcc)
}

/// The NAL length prefix an `avcC` declares, in bytes: its
/// `lengthSizeMinusOne` plus one. Four for everything ffmpeg writes;
/// a record too short to say is read as four.
pub fn avcc_length_size(avcc: &[u8]) -> usize {
	match avcc.get(4) {
		Some(byte) => usize::from(byte & 0x3) + 1,
		None => 4,
	}
}

/// The SPS and PPS an `avcC` record carries, back as the Annex-B
/// extradata a coded stream declares: each NAL behind a 4-byte start
/// code, the sets in the order the record listed them.
///
/// The inverse of [`build_avcc`]; what a source reading an mp4 init
/// segment hands an encoded edge that expects Annex-B.
pub fn avcc_to_annexb_extradata(avcc: &[u8]) -> Result<Vec<u8>, String> {
	if avcc.len() < 6 {
		return Err(format!("an avcC of {} bytes names no parameter sets", avcc.len()));
	}
	let mut out = Vec::with_capacity(avcc.len());
	let mut pos = 5usize;
	let mut counts = [usize::from(avcc[5] & 0x1f), 0];
	pos += 1;
	for set in 0..2 {
		if set == 1 {
			let count = *avcc.get(pos).ok_or("the avcC ends before its PPS count")?;
			counts[1] = usize::from(count);
			pos += 1;
		}
		for _ in 0..counts[set] {
			let length = avcc
				.get(pos..pos + 2)
				.map(|pair| usize::from(u16::from_be_bytes([pair[0], pair[1]])))
				.ok_or("the avcC ends inside a parameter set length")?;
			pos += 2;
			let nal = avcc
				.get(pos..pos + length)
				.ok_or("the avcC ends inside a parameter set")?;
			pos += length;
			out.extend_from_slice(&[0, 0, 0, 1]);
			out.extend_from_slice(nal);
		}
	}
	if out.is_empty() {
		return Err("the avcC carries no parameter sets".into());
	}
	Ok(out)
}

/// One AVCC-framed sample back as Annex-B: each length-prefixed NAL
/// behind a 4-byte start code. The inverse of [`annexb_to_avcc`].
pub fn avcc_to_annexb(sample: &[u8], length_size: usize) -> Result<Vec<u8>, String> {
	if !(1..=4).contains(&length_size) {
		return Err(format!("a NAL length of {length_size} bytes is not 1 to 4"));
	}
	let mut out = Vec::with_capacity(sample.len() + 8);
	let mut pos = 0usize;
	while pos < sample.len() {
		let header = sample
			.get(pos..pos + length_size)
			.ok_or_else(|| format!("the sample ends inside a NAL length at byte {pos}"))?;
		let length = header
			.iter()
			.fold(0usize, |value, byte| (value << 8) | usize::from(*byte));
		pos += length_size;
		let nal = sample
			.get(pos..pos + length)
			.ok_or_else(|| format!("a NAL of {length} bytes overruns the sample"))?;
		pos += length;
		out.extend_from_slice(&[0, 0, 0, 1]);
		out.extend_from_slice(nal);
	}
	Ok(out)
}

/// The formats an avcC extension carries, read out of an SPS.
struct SpsFormats {
	chroma_format_idc: u8,
	bit_depth_luma_minus8: u8,
	bit_depth_chroma_minus8: u8,
}

/// Reads chroma format and bit depths from a high-profile SPS NAL.
/// `None` when the SPS runs out before they are read.
fn parse_sps_formats(nal: &[u8]) -> Option<SpsFormats> {
	// RBSP: strip emulation-prevention bytes (00 00 03 -> 00 00).
	let mut rbsp = Vec::with_capacity(nal.len());
	let mut zeros = 0u32;
	for &b in &nal[1..] {
		if zeros >= 2 && b == 3 {
			zeros = 0;
			continue;
		}
		zeros = if b == 0 { zeros + 1 } else { 0 };
		rbsp.push(b);
	}

	let mut bits = BitReader::new(&rbsp);
	bits.skip(24)?; // profile_idc, constraint flags, level_idc
	bits.ue()?; // seq_parameter_set_id
	let chroma_format_idc = bits.ue()?;
	if chroma_format_idc == 3 {
		bits.skip(1)?; // separate_colour_plane_flag
	}
	let bit_depth_luma_minus8 = bits.ue()?;
	let bit_depth_chroma_minus8 = bits.ue()?;
	Some(SpsFormats {
		chroma_format_idc: chroma_format_idc as u8,
		bit_depth_luma_minus8: bit_depth_luma_minus8 as u8,
		bit_depth_chroma_minus8: bit_depth_chroma_minus8 as u8,
	})
}

/// A most-significant-bit-first reader over a byte slice.
struct BitReader<'a> {
	data: &'a [u8],
	pos: usize,
}

impl<'a> BitReader<'a> {
	fn new(data: &'a [u8]) -> Self {
		Self { data, pos: 0 }
	}

	fn bit(&mut self) -> Option<u32> {
		let byte = *self.data.get(self.pos / 8)?;
		let bit = (byte >> (7 - self.pos % 8)) & 1;
		self.pos += 1;
		Some(bit as u32)
	}

	fn skip(&mut self, n: usize) -> Option<()> {
		if self.pos + n > self.data.len() * 8 {
			return None;
		}
		self.pos += n;
		Some(())
	}

	/// One unsigned exp-Golomb value.
	fn ue(&mut self) -> Option<u32> {
		let mut zeros = 0;
		while self.bit()? == 0 {
			zeros += 1;
			if zeros > 31 {
				return None;
			}
		}
		let mut value = 0u32;
		for _ in 0..zeros {
			value = (value << 1) | self.bit()?;
		}
		Some((1u32 << zeros) - 1 + value)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn start_codes_of_both_lengths_cut() {
		let annexb = [
			0, 0, 0, 1, 0x67, 0xaa, // 4-byte code, SPS-ish
			0, 0, 1, 0x68, 0xbb, // 3-byte code
			0, 0, 0, 1, 0x65, 0xcc, 0xdd, // 4-byte code again
		];
		let nals = split_nals(&annexb);
		assert_eq!(nals, vec![&[0x67, 0xaa][..], &[0x68, 0xbb], &[0x65, 0xcc, 0xdd]]);
	}

	#[test]
	fn avcc_framing_prefixes_each_nal_with_its_length() {
		let annexb = [0, 0, 1, 0x06, 0x05, 0, 0, 0, 1, 0x65, 0x88];
		assert_eq!(
			annexb_to_avcc(&annexb),
			vec![0, 0, 0, 2, 0x06, 0x05, 0, 0, 0, 2, 0x65, 0x88]
		);
	}

	#[test]
	fn a_packet_without_start_codes_reframes_to_nothing() {
		assert!(annexb_to_avcc(&[1, 2, 3, 4]).is_empty());
	}

	#[test]
	fn exp_golomb_reads_the_first_values() {
		// 1 -> 0; 010 -> 1; 011 -> 2; 00100 -> 3.
		let mut bits = BitReader::new(&[0b1_010_011_0, 0b0100_0000]);
		assert_eq!(bits.ue(), Some(0));
		assert_eq!(bits.ue(), Some(1));
		assert_eq!(bits.ue(), Some(2));
		assert_eq!(bits.ue(), Some(3));
	}

	#[test]
	fn extradata_round_trips_through_the_avcc() {
		// The parameter sets go in Annex-B, come back out Annex-B, and
		// the record between them declares 4-byte NAL lengths.
		let annexb: &[u8] = &[
			0, 0, 0, 1, 0x67, 66, 0xc0, 30, 0xab, 0xcd, // SPS
			0, 0, 0, 1, 0x68, 0xee, 0x06, 0xf2, // PPS
		];
		let (sps, pps) = parse_parameter_sets(annexb);
		let avcc = build_avcc(&sps, &pps).expect("an avcC");
		assert_eq!(avcc_length_size(&avcc), 4);
		assert_eq!(avcc_to_annexb_extradata(&avcc).expect("extradata"), annexb);
	}

	#[test]
	fn a_high_profile_record_keeps_its_sets_past_the_extension_bytes() {
		// A High-profile avcC carries three extension bytes after the
		// PPS; the sets read back from before them either way.
		let annexb: &[u8] = &[
			0, 0, 0, 1, 0x67, 0x64, 0x00, 0x1f, 0xac, 0xd9, 0x40, 0x50, 0x05, 0xbb,
			0, 0, 0, 1, 0x68, 0xeb, 0xec, 0xb2, 0x2c,
		];
		let (sps, pps) = parse_parameter_sets(annexb);
		let avcc = build_avcc(&sps, &pps).expect("an avcC");
		assert_eq!(avcc_to_annexb_extradata(&avcc).expect("extradata"), annexb);
	}

	#[test]
	fn a_sample_round_trips_through_the_avcc_framing() {
		// Two NALs behind start codes, length-prefixed and back.
		let annexb: &[u8] = &[0, 0, 0, 1, 0x65, 1, 2, 3, 0, 0, 0, 1, 0x41, 9];
		let framed = annexb_to_avcc(annexb);
		assert_eq!(framed, vec![0, 0, 0, 4, 0x65, 1, 2, 3, 0, 0, 0, 2, 0x41, 9]);
		assert_eq!(avcc_to_annexb(&framed, 4).expect("annex-b"), annexb);
	}

	#[test]
	fn a_nal_running_past_the_sample_is_refused() {
		let err = avcc_to_annexb(&[0, 0, 0, 9, 1, 2], 4).expect_err("a refusal");
		assert!(err.contains("overruns"), "{err}");
	}

	#[test]
	fn an_avcc_too_short_to_name_a_set_is_refused() {
		let err = avcc_to_annexb_extradata(&[1, 0x64, 0, 0x1f]).expect_err("a refusal");
		assert!(err.contains("names no parameter sets"), "{err}");
	}
}

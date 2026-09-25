//! One message on a data track, framed the way hang's `legacy` container
//! frames a sample: a QUIC variable-length integer of the message's pts in
//! MICROSECONDS, then the message's own bytes. It is the one frame of its
//! group.
//!
//! The pts rides inside the frame because the frame's own timestamp does
//! not always arrive. moq-lite from draft 05 carries a frame timestamp in
//! the track's timescale, but the IETF drafts (moq-transport 16 and the
//! rest) send no timescale, and a subscriber that has none stamps each
//! frame with the time it ARRIVED. A media track survives that, its time
//! being inside its fmp4 fragments; a message would not. So a reader takes
//! the pts from the frame, never from the object's timestamp.
//!
//! The varint is moq-net's own ([`moq_net::VarInt`]), the code hang's
//! legacy container writes with, so a hang reader decodes these frames as
//! it decodes any legacy one.

use bytes::Buf;

/// The largest pts a frame can carry, in microseconds: a QUIC varint's
/// 62 bits, which is some 146 thousand years.
pub const MAX_PTS_US: u64 = (1 << 62) - 1;

/// One message as the frame a data track's group carries.
pub fn encode(pts_us: u64, message: &[u8]) -> Result<Vec<u8>, String> {
	let value = moq_net::VarInt::try_from(pts_us)
		.map_err(|_| format!("a message at {pts_us}us is past what a frame's varint holds"))?;
	let mut frame = Vec::with_capacity(8 + message.len());
	value
		.encode_quic(&mut frame)
		.map_err(|err| format!("a message's pts: {err}"))?;
	frame.extend_from_slice(message);
	Ok(frame)
}

/// A data track's frame back to the message's pts in microseconds and its
/// bytes, which are everything after the varint.
pub fn decode(frame: &[u8]) -> Result<(u64, &[u8]), String> {
	let mut reader = frame;
	let value = moq_net::VarInt::decode_quic(&mut reader)
		.map_err(|err| format!("a frame that opens with no pts ({} bytes): {err}", frame.len()))?;
	let rest = frame.len() - reader.remaining();
	Ok((value.into_inner(), &frame[rest..]))
}

#[cfg(test)]
mod tests {
	use super::*;

	const MESSAGE: &[u8] = br#"{"kind":"break","id":7,"start_pts":225.0}"#;

	#[test]
	fn a_frame_reads_back_as_the_pts_and_the_message() {
		// Each varint length, both sides of every boundary: 1, 2, 4 and
		// 8 bytes.
		for pts in [
			0u64,
			63,
			64,
			16_383,
			16_384,
			(1 << 30) - 1,
			1 << 30,
			18_623_457,
			MAX_PTS_US,
		] {
			let frame = encode(pts, MESSAGE).expect("encodes");
			assert_eq!(decode(&frame).expect("decodes"), (pts, MESSAGE), "pts {pts}");
		}
	}

	#[test]
	fn an_epoch_sized_pts_survives() {
		// A pts counted from the Unix epoch, as a wall-clock source writes
		// one: 2026-09-24 in microseconds, past anything 32 bits holds.
		let pts = 1_790_208_000_123_457u64;
		let frame = encode(pts, MESSAGE).expect("encodes");
		assert_eq!(frame.len(), 8 + MESSAGE.len(), "an 8 byte varint");
		assert_eq!(decode(&frame).expect("decodes"), (pts, MESSAGE));
	}

	#[test]
	fn the_varint_is_the_one_hang_writes() {
		// RFC 9000's own example, 494878333 in four bytes, then the bytes
		// untouched: nothing between the prefix and the message.
		let frame = encode(494_878_333, b"{}").expect("encodes");
		assert_eq!(frame, [0x9d, 0x7f, 0x3e, 0x7d, b'{', b'}']);
		// A message may be empty and still has its pts.
		assert_eq!(encode(37, b"").expect("encodes"), [0x25]);
		assert_eq!(decode(&[0x25]).expect("decodes"), (37, &b""[..]));
	}

	#[test]
	fn a_pts_past_the_varint_is_refused() {
		assert!(encode(MAX_PTS_US + 1, MESSAGE).is_err());
	}

	#[test]
	fn a_frame_cut_short_is_refused() {
		// An 8 byte varint with 3 of its bytes, and a frame with nothing.
		assert!(decode(&[0xc0, 0, 0]).is_err());
		assert!(decode(&[]).is_err());
	}
}

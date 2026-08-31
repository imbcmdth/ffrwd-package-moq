//! Minimal DNS wire codec for the DoH lookup: one question out, the
//! answer's addresses back. Compression pointers are honored when
//! parsing; TTLs are not read.

use std::net::IpAddr;

/// A-record query type.
pub const TYPE_A: u16 = 1;
/// AAAA-record query type.
pub const TYPE_AAAA: u16 = 28;

/// A name walk follows at most this many compression pointers.
const POINTER_LIMIT: usize = 16;

/// Encodes one query for `name`: id 0 (RFC 8484 asks for it, for
/// cacheability), recursion desired, one question, class IN.
pub fn encode_query(name: &str, qtype: u16) -> Result<Vec<u8>, String> {
	let name = name.strip_suffix('.').unwrap_or(name);
	if name.is_empty() || name.len() > 253 {
		return Err(format!("'{name}' is not a DNS name"));
	}
	let mut msg = Vec::with_capacity(18 + name.len());
	msg.extend_from_slice(&[0, 0, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
	for label in name.split('.') {
		if label.is_empty() || label.len() > 63 {
			return Err(format!("'{name}' is not a DNS name"));
		}
		msg.push(label.len() as u8);
		msg.extend_from_slice(label.as_bytes());
	}
	msg.push(0);
	msg.extend_from_slice(&qtype.to_be_bytes());
	msg.extend_from_slice(&1u16.to_be_bytes());
	Ok(msg)
}

/// The addresses in a response's answer section, in record order.
/// Records of other types (a CNAME in the chain) carry no address and
/// are skipped; NXDOMAIN reads as no addresses. Malformed input
/// errors, never panics.
pub fn parse_response(msg: &[u8]) -> Result<Vec<IpAddr>, String> {
	if msg.len() < 12 {
		return Err("dns response truncated".into());
	}
	let flags = u16::from_be_bytes([msg[2], msg[3]]);
	if flags & 0x8000 == 0 {
		return Err("dns message is not a response".into());
	}
	match flags & 0x000f {
		0 => {}
		3 => return Ok(Vec::new()),
		code => return Err(format!("dns error {code}")),
	}
	let questions = u16::from_be_bytes([msg[4], msg[5]]);
	let answers = u16::from_be_bytes([msg[6], msg[7]]);

	let mut cursor = Cursor { msg, at: 12 };
	for _ in 0..questions {
		cursor.skip_name()?;
		cursor.take(4)?;
	}
	let mut addrs = Vec::new();
	for _ in 0..answers {
		cursor.skip_name()?;
		let head = cursor.take(10)?;
		let rtype = u16::from_be_bytes([head[0], head[1]]);
		let rdlen = u16::from_be_bytes([head[8], head[9]]) as usize;
		let rdata = cursor.take(rdlen)?;
		match rtype {
			TYPE_A if rdlen == 4 => {
				addrs.push(IpAddr::from(<[u8; 4]>::try_from(rdata).expect("4 bytes")));
			}
			TYPE_AAAA if rdlen == 16 => {
				addrs.push(IpAddr::from(<[u8; 16]>::try_from(rdata).expect("16 bytes")));
			}
			_ => {}
		}
	}
	Ok(addrs)
}

/// base64url without padding: how an RFC 8484 GET carries the query.
pub fn base64url(bytes: &[u8]) -> String {
	const ALPHABET: &[u8; 64] =
		b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
	let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let word = u32::from_be_bytes([
			0,
			chunk[0],
			chunk.get(1).copied().unwrap_or(0),
			chunk.get(2).copied().unwrap_or(0),
		]);
		for slot in 0..=chunk.len() {
			let shift = 18 - 6 * slot;
			out.push(ALPHABET[(word >> shift) as usize & 63] as char);
		}
	}
	out
}

struct Cursor<'a> {
	msg: &'a [u8],
	at: usize,
}

impl<'a> Cursor<'a> {
	fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
		let end = self
			.at
			.checked_add(len)
			.filter(|&end| end <= self.msg.len())
			.ok_or("dns response truncated")?;
		let piece = &self.msg[self.at..end];
		self.at = end;
		Ok(piece)
	}

	/// Walks past one possibly-compressed name. The first pointer ends
	/// the in-place walk; a bounded jump count rules out pointer loops.
	fn skip_name(&mut self) -> Result<(), String> {
		let mut at = self.at;
		let mut jumps = 0;
		loop {
			let len = *self.msg.get(at).ok_or("dns response truncated")? as usize;
			if len & 0xc0 == 0xc0 {
				let low = *self.msg.get(at + 1).ok_or("dns response truncated")? as usize;
				if jumps == 0 {
					self.at = at + 2;
				}
				jumps += 1;
				if jumps > POINTER_LIMIT {
					return Err("dns name pointers loop".into());
				}
				at = (len & 0x3f) << 8 | low;
			} else if len & 0xc0 != 0 {
				return Err("dns label form unknown".into());
			} else if len == 0 {
				if jumps == 0 {
					self.at = at + 1;
				}
				return Ok(());
			} else {
				at += 1 + len;
				if at > self.msg.len() {
					return Err("dns response truncated".into());
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::{Ipv4Addr, Ipv6Addr};

	#[test]
	fn a_query_encodes_to_known_bytes() {
		let query = encode_query("example.com", TYPE_A).unwrap();
		#[rustfmt::skip]
		let expected: &[u8] = &[
			0x00, 0x00, // id 0
			0x01, 0x00, // recursion desired
			0x00, 0x01, // one question
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // no other sections
			7, b'e', b'x', b'a', b'm', b'p', b'l', b'e',
			3, b'c', b'o', b'm',
			0,
			0x00, 0x01, // type A
			0x00, 0x01, // class IN
		];
		assert_eq!(query, expected);
	}

	#[test]
	fn a_trailing_dot_and_aaaa_change_only_what_they_should() {
		let plain = encode_query("example.com", TYPE_AAAA).unwrap();
		let dotted = encode_query("example.com.", TYPE_AAAA).unwrap();
		assert_eq!(plain, dotted);
		assert_eq!(&plain[plain.len() - 4..], &[0x00, 0x1c, 0x00, 0x01]);
	}

	#[test]
	fn hostile_names_are_refused() {
		assert!(encode_query("", TYPE_A).is_err());
		assert!(encode_query("a..b", TYPE_A).is_err());
		assert!(encode_query(&"x".repeat(64), TYPE_A).is_err());
		assert!(encode_query(&"x.".repeat(200), TYPE_A).is_err());
	}

	/// A response to `example.com A`: a CNAME to `cdn.example.com`
	/// (its owner a pointer to the question name, its target partly
	/// compressed), then an A and an AAAA on the target, plus an
	/// unknown-type record that must be skipped.
	fn response_with_compression() -> Vec<u8> {
		let mut msg = vec![
			0x00, 0x00, // id
			0x81, 0x80, // response, recursion available
			0x00, 0x01, // one question
			0x00, 0x04, // four answers
			0x00, 0x00, 0x00, 0x00,
		];
		// question at offset 12: example.com A IN
		msg.extend_from_slice(&[7]);
		msg.extend_from_slice(b"example");
		msg.extend_from_slice(&[3]);
		msg.extend_from_slice(b"com");
		msg.extend_from_slice(&[0, 0x00, 0x01, 0x00, 0x01]);
		// CNAME: owner is a pointer to offset 12; rdata is
		// "cdn" + pointer to offset 12. The rdata starts at offset 41.
		msg.extend_from_slice(&[0xc0, 12]);
		msg.extend_from_slice(&[0x00, 0x05, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x06]);
		msg.extend_from_slice(&[3]);
		msg.extend_from_slice(b"cdn");
		msg.extend_from_slice(&[0xc0, 12]);
		// A on the CNAME target (pointer to offset 41): 192.0.2.7
		msg.extend_from_slice(&[0xc0, 41]);
		msg.extend_from_slice(&[0x00, 0x01, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x04]);
		msg.extend_from_slice(&[192, 0, 2, 7]);
		// AAAA on the same name: 2001:db8::7
		msg.extend_from_slice(&[0xc0, 41]);
		msg.extend_from_slice(&[0x00, 0x1c, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x10]);
		msg.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7]);
		// TXT-shaped stranger: skipped whole
		msg.extend_from_slice(&[0xc0, 41]);
		msg.extend_from_slice(&[0x00, 0x10, 0x00, 0x01, 0, 0, 0, 60, 0x00, 0x02]);
		msg.extend_from_slice(&[1, b'x']);
		msg
	}

	#[test]
	fn a_compressed_answer_yields_its_addresses() {
		let addrs = parse_response(&response_with_compression()).unwrap();
		assert_eq!(
			addrs,
			vec![
				IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7)),
				IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 7)),
			]
		);
	}

	#[test]
	fn nxdomain_reads_as_no_addresses() {
		let msg = [0, 0, 0x81, 0x83, 0, 0, 0, 0, 0, 0, 0, 0];
		assert_eq!(parse_response(&msg).unwrap(), Vec::<IpAddr>::new());
	}

	#[test]
	fn broken_responses_error_instead_of_panicking() {
		let good = response_with_compression();
		for cut in [0, 5, 13, 30, good.len() - 1] {
			assert!(parse_response(&good[..cut]).is_err(), "cut at {cut}");
		}
		// a query is not a response
		assert!(parse_response(&encode_query("example.com", TYPE_A).unwrap()).is_err());
		// a name pointer pointing at itself must not spin
		let mut looped = good.clone();
		looped[12] = 0xc0;
		looped[13] = 12;
		assert!(parse_response(&looped).is_err());
		// arbitrary garbage
		assert!(parse_response(&[0xff; 40]).is_err());
	}

	#[test]
	fn base64url_matches_known_vectors() {
		assert_eq!(base64url(b""), "");
		assert_eq!(base64url(b"f"), "Zg");
		assert_eq!(base64url(b"fo"), "Zm8");
		assert_eq!(base64url(b"foo"), "Zm9v");
		assert_eq!(base64url(b"foob"), "Zm9vYg");
		assert_eq!(base64url(&[0xfb, 0xff, 0xbf]), "-_-_");
	}
}

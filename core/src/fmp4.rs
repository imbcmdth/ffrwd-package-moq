//! Incremental fragmented-MP4 segmentation.
//!
//! Feeds on an arbitrary byte stream (a pipe, a file) and cuts it at
//! top-level box boundaries into the segments MoQ publishing wants:
//! one init segment — everything before the first `moof`, which must
//! include a `moov` — then one fragment per `moof` and the boxes
//! through its `mdat`. Each fragment's `moof` is parsed far enough to
//! read the fragment sequence number (`mfhd`) and whether the first
//! sample is a sync sample (`tfhd`/`trun` sample flags), so a caller
//! can rotate groups on real keyframes instead of assuming them.
//!
//! Trailing boxes after the last fragment — ffmpeg closes a fragmented
//! stream with `mfra` even into a pipe — are collected aside, not
//! emitted: a live subscriber has no use for a random-access index.
//!
//! Pure Rust, no I/O: [`Scanner::push`] takes bytes, [`Scanner::poll`]
//! yields segments, [`Scanner::finish`] checks the stream ended on a
//! clean boundary.

use std::collections::VecDeque;
use std::fmt;

use bytes::Bytes;

/// Refuse any single box larger than this: a stream claiming one is
/// far more likely corrupt than carrying a quarter-gigabyte fragment.
const MAX_BOX: u64 = 1 << 28;

/// The `sample_is_non_sync_sample` bit of ISO sample flags: clear on
/// a sync sample (a keyframe).
const NON_SYNC_FLAG: u32 = 0x0001_0000;

/// Errors from the segmenter. Every variant carries the absolute
/// stream offset where the problem sits.
#[derive(Debug)]
pub enum Error {
	/// The bytes at `offset` do not fit the expected box structure.
	Malformed { offset: u64, what: String },
	/// The stream ended inside a box or a fragment.
	Truncated { offset: u64, what: String },
	/// A well-formed shape this segmenter does not handle.
	Unsupported { offset: u64, what: String },
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Malformed { offset, what } => write!(f, "malformed at offset {offset}: {what}"),
			Error::Truncated { offset, what } => write!(f, "truncated at offset {offset}: {what}"),
			Error::Unsupported { offset, what } => {
				write!(f, "unsupported at offset {offset}: {what}")
			}
		}
	}
}

impl std::error::Error for Error {}

/// One cut of the stream.
#[derive(Clone, Debug)]
pub enum Segment {
	/// Everything before the first `moof`: `ftyp`, `moov`, and any
	/// boxes between them. A late subscriber needs exactly these bytes
	/// before any fragment.
	Init(Init),
	/// One `moof` and the boxes through its `mdat`.
	Fragment(Fragment),
}

/// The init segment.
#[derive(Clone, Debug)]
pub struct Init {
	/// The segment bytes, verbatim.
	pub bytes: Bytes,
	/// Absolute stream offset of the first byte.
	pub offset: u64,
}

/// One movie fragment.
#[derive(Clone, Debug)]
pub struct Fragment {
	/// The fragment bytes (`moof` through `mdat`), verbatim.
	pub bytes: Bytes,
	/// Absolute stream offset of the first byte.
	pub offset: u64,
	/// The `mfhd` sequence number, when the fragment carries one.
	pub sequence: Option<u32>,
	/// Whether the first sample is a sync sample. `None` when the
	/// sample flags live only in the `moov`'s `trex` defaults, which
	/// this segmenter does not track.
	pub keyframe: Option<bool>,
}

enum Phase {
	/// Accumulating boxes before the first `moof`.
	Init,
	/// Cutting `moof`..`mdat` fragments.
	Fragments,
	/// Accumulating trailing boxes after the last fragment — ffmpeg
	/// ends a fragmented stream with `mfra`, even into a pipe.
	Trailer,
}

/// The incremental segmenter.
pub struct Scanner {
	buf: Vec<u8>,
	/// Absolute stream offset of `buf[0]`.
	base: u64,
	phase: Phase,
	out: VecDeque<Segment>,
	init: Vec<u8>,
	saw_moov: bool,
	/// The fragment being collected; starts with its `moof`.
	frag: Vec<u8>,
	frag_start: u64,
	/// Length of the current fragment's `moof` box within `frag`.
	frag_moof_len: usize,
	trailer: Vec<u8>,
}

impl Scanner {
	pub fn new() -> Self {
		Self {
			buf: Vec::new(),
			base: 0,
			phase: Phase::Init,
			out: VecDeque::new(),
			init: Vec::new(),
			saw_moov: false,
			frag: Vec::new(),
			frag_start: 0,
			frag_moof_len: 0,
			trailer: Vec::new(),
		}
	}

	/// Feeds more stream bytes; complete segments queue for [`poll`].
	///
	/// [`poll`]: Self::poll
	pub fn push(&mut self, chunk: &[u8]) -> Result<(), Error> {
		self.buf.extend_from_slice(chunk);
		self.scan()
	}

	/// The next complete segment, if one is ready.
	pub fn poll(&mut self) -> Option<Segment> {
		self.out.pop_front()
	}

	/// Declares end of stream: the last byte pushed must have closed a
	/// fragment, with no partial box and no fragment in flight.
	pub fn finish(&self) -> Result<(), Error> {
		if !self.frag.is_empty() {
			return Err(Error::Truncated {
				offset: self.frag_start,
				what: format!(
					"stream ended inside a fragment ({} bytes collected, no mdat)",
					self.frag.len()
				),
			});
		}
		if !self.buf.is_empty() {
			return Err(Error::Truncated {
				offset: self.base,
				what: format!("stream ended inside a box ({} bytes pending)", self.buf.len()),
			});
		}
		if matches!(self.phase, Phase::Init) {
			return Err(Error::Truncated {
				offset: self.base,
				what: "stream ended before any movie fragment".into(),
			});
		}
		Ok(())
	}

	/// Cumulative count of bytes fully consumed into segments or the
	/// trailer (excludes bytes still buffered).
	pub fn consumed(&self) -> u64 {
		self.base
	}

	/// Cuts as many complete boxes off the head of `buf` as it holds.
	fn scan(&mut self) -> Result<(), Error> {
		loop {
			let Some(header) = peek_box(&self.buf, self.base)? else {
				return Ok(());
			};
			let size = header.size as usize;

			match self.phase {
				Phase::Init => {
					if &header.kind == b"moof" {
						if !self.saw_moov {
							return Err(Error::Malformed {
								offset: self.base,
								what: "moof before any moov".into(),
							});
						}
						let offset = self.base - self.init.len() as u64;
						let bytes = Bytes::from(std::mem::take(&mut self.init));
						self.out.push_back(Segment::Init(Init { bytes, offset }));
						self.phase = Phase::Fragments;
						// The moof itself is handled by the next pass.
						continue;
					}
					if self.buf.len() < size {
						return Ok(());
					}
					if &header.kind == b"moov" {
						self.saw_moov = true;
					}
					self.init.extend_from_slice(&self.buf[..size]);
					self.consume(size);
				}
				Phase::Fragments => {
					if self.frag.is_empty() && &header.kind != b"moof" {
						// The stream's trailer has begun (ffmpeg: mfra).
						self.phase = Phase::Trailer;
						continue;
					}
					if !self.frag.is_empty() && &header.kind == b"moof" {
						return Err(Error::Malformed {
							offset: self.base,
							what: "moof before the previous fragment's mdat".into(),
						});
					}
					if self.buf.len() < size {
						return Ok(());
					}
					if self.frag.is_empty() {
						self.frag_start = self.base;
						self.frag_moof_len = size;
					}
					self.frag.extend_from_slice(&self.buf[..size]);
					self.consume(size);
					if &header.kind == b"mdat" {
						let moof = &self.frag[..self.frag_moof_len];
						let summary = moof_summary(moof, self.frag_start)?;
						let fragment = Fragment {
							bytes: Bytes::from(std::mem::take(&mut self.frag)),
							offset: self.frag_start,
							sequence: summary.sequence,
							keyframe: summary.first_sample_keyframe,
						};
						self.frag_moof_len = 0;
						self.out.push_back(Segment::Fragment(fragment));
					}
				}
				Phase::Trailer => {
					if &header.kind == b"moof" {
						return Err(Error::Malformed {
							offset: self.base,
							what: "moof after the stream's trailer began".into(),
						});
					}
					if self.buf.len() < size {
						return Ok(());
					}
					self.trailer.extend_from_slice(&self.buf[..size]);
					self.consume(size);
				}
			}
		}
	}

	/// Trailing boxes after the last fragment, so far — `mfra` and kin.
	/// Not part of any segment: a live subscriber cannot use them.
	pub fn trailer(&self) -> &[u8] {
		&self.trailer
	}

	fn consume(&mut self, n: usize) {
		self.buf.drain(..n);
		self.base += n as u64;
	}
}

impl Default for Scanner {
	fn default() -> Self {
		Self::new()
	}
}

/// A box header peeked off the head of a buffer.
struct BoxHeader {
	/// Total box size including the header.
	size: u64,
	kind: [u8; 4],
}

/// Reads the header at the head of `buf`, or `None` if more bytes are
/// needed to know. `offset` is `buf[0]`'s absolute position, for errors.
fn peek_box(buf: &[u8], offset: u64) -> Result<Option<BoxHeader>, Error> {
	if buf.len() < 8 {
		return Ok(None);
	}
	let size32 = u32::from_be_bytes(buf[0..4].try_into().expect("4 bytes"));
	let kind: [u8; 4] = buf[4..8].try_into().expect("4 bytes");
	let (size, header_len) = match size32 {
		0 => {
			// "To end of file" only works when the file's end is known;
			// a live stream has none.
			return Err(Error::Unsupported {
				offset,
				what: format!("box '{}' extends to end of stream", kind_name(&kind)),
			});
		}
		1 => {
			if buf.len() < 16 {
				return Ok(None);
			}
			let size64 = u64::from_be_bytes(buf[8..16].try_into().expect("8 bytes"));
			(size64, 16u64)
		}
		n => (n as u64, 8u64),
	};
	if size < header_len {
		return Err(Error::Malformed {
			offset,
			what: format!("box '{}' size {size} below its header", kind_name(&kind)),
		});
	}
	if size > MAX_BOX {
		return Err(Error::Malformed {
			offset,
			what: format!("box '{}' claims {size} bytes", kind_name(&kind)),
		});
	}
	Ok(Some(BoxHeader { size, kind }))
}

fn kind_name(kind: &[u8; 4]) -> String {
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

/// What one `moof` says about its fragment.
#[derive(Clone, Copy, Debug)]
pub struct MoofSummary {
	/// The `mfhd` sequence number.
	pub sequence: Option<u32>,
	/// Whether the first sample of the first track fragment that
	/// declares sample flags is a sync sample. `None` when no `tfhd`
	/// default and no `trun` flags decide it (the `trex` default case).
	pub first_sample_keyframe: Option<bool>,
}

/// Parses a complete `moof` box (header included) for its summary.
///
/// The first-sample flags follow ISO precedence: the `trun`'s
/// `first_sample_flags` when present, else the first sample's own
/// per-sample flags, else the `tfhd` default. Only the first `traf`
/// that yields an answer is consulted — a single-track stream has
/// exactly one.
pub fn moof_summary(moof: &[u8], offset: u64) -> Result<MoofSummary, Error> {
	let payload = box_payload(moof, offset)?;
	let mut sequence = None;
	let mut keyframe = None;
	let mut children = BoxIter::new(payload, offset + (moof.len() - payload.len()) as u64);
	while let Some(child) = children.next()? {
		match &child.kind {
			b"mfhd" => {
				// Full box: version/flags, then sequence_number.
				if child.payload.len() >= 8 {
					sequence = Some(u32::from_be_bytes(
						child.payload[4..8].try_into().expect("4 bytes"),
					));
				}
			}
			b"traf" if keyframe.is_none() => {
				if let Some(flags) = traf_first_sample_flags(child.payload, child.offset)? {
					keyframe = Some(flags & NON_SYNC_FLAG == 0);
				}
			}
			_ => {}
		}
	}
	Ok(MoofSummary {
		sequence,
		first_sample_keyframe: keyframe,
	})
}

/// The first sample's flags within one `traf` payload, per precedence.
fn traf_first_sample_flags(traf: &[u8], offset: u64) -> Result<Option<u32>, Error> {
	let mut tfhd_default = None;
	let mut children = BoxIter::new(traf, offset);
	while let Some(child) = children.next()? {
		match &child.kind {
			b"tfhd" => {
				tfhd_default = tfhd_default_sample_flags(child.payload);
			}
			b"trun" => {
				if let Some(flags) = trun_first_sample_flags(child.payload) {
					return Ok(Some(flags));
				}
				// The first trun decides; without its own flags, the
				// tfhd default (or nothing) stands.
				return Ok(tfhd_default);
			}
			_ => {}
		}
	}
	Ok(tfhd_default)
}

/// `default_sample_flags` out of a `tfhd` payload, when the flag bit
/// says it is present.
fn tfhd_default_sample_flags(tfhd: &[u8]) -> Option<u32> {
	if tfhd.len() < 8 {
		return None;
	}
	let flags = u32::from_be_bytes(tfhd[0..4].try_into().expect("4 bytes")) & 0x00ff_ffff;
	// Fixed fields: track_ID. Optionals, in order, per flag bit.
	let mut pos = 8usize;
	if flags & 0x1 != 0 {
		pos += 8; // base_data_offset
	}
	if flags & 0x2 != 0 {
		pos += 4; // sample_description_index
	}
	if flags & 0x8 != 0 {
		pos += 4; // default_sample_duration
	}
	if flags & 0x10 != 0 {
		pos += 4; // default_sample_size
	}
	if flags & 0x20 != 0 {
		return tfhd
			.get(pos..pos + 4)
			.map(|b| u32::from_be_bytes(b.try_into().expect("4 bytes")));
	}
	None
}

/// The flags governing a `trun`'s first sample, when the trun itself
/// carries them: `first_sample_flags`, else the first per-sample flags.
fn trun_first_sample_flags(trun: &[u8]) -> Option<u32> {
	if trun.len() < 8 {
		return None;
	}
	let flags = u32::from_be_bytes(trun[0..4].try_into().expect("4 bytes")) & 0x00ff_ffff;
	let mut pos = 8usize; // version/flags + sample_count
	if flags & 0x1 != 0 {
		pos += 4; // data_offset
	}
	if flags & 0x4 != 0 {
		return trun
			.get(pos..pos + 4)
			.map(|b| u32::from_be_bytes(b.try_into().expect("4 bytes")));
	}
	if flags & 0x400 != 0 {
		let sample_count = u32::from_be_bytes(trun[4..8].try_into().expect("4 bytes"));
		if sample_count == 0 {
			return None;
		}
		// Per-sample fields before flags, in order.
		if flags & 0x100 != 0 {
			pos += 4; // sample_duration
		}
		if flags & 0x200 != 0 {
			pos += 4; // sample_size
		}
		return trun
			.get(pos..pos + 4)
			.map(|b| u32::from_be_bytes(b.try_into().expect("4 bytes")));
	}
	None
}

/// Strips a complete box's header, returning its payload.
fn box_payload(data: &[u8], offset: u64) -> Result<&[u8], Error> {
	let header = peek_box(data, offset)?.ok_or_else(|| Error::Truncated {
		offset,
		what: "box shorter than its header".into(),
	})?;
	if data.len() as u64 != header.size {
		return Err(Error::Malformed {
			offset,
			what: format!(
				"box '{}' size {} does not match its {} bytes",
				kind_name(&header.kind),
				header.size,
				data.len()
			),
		});
	}
	let header_len = if data[0..4] == [0, 0, 0, 1] { 16 } else { 8 };
	Ok(&data[header_len..])
}

/// Walks the child boxes of a parent payload.
struct BoxIter<'a> {
	data: &'a [u8],
	pos: usize,
	/// Absolute offset of `data[0]`, for errors.
	offset: u64,
}

struct ChildBox<'a> {
	kind: [u8; 4],
	payload: &'a [u8],
	/// Absolute offset of the payload's first byte.
	offset: u64,
}

impl<'a> BoxIter<'a> {
	fn new(data: &'a [u8], offset: u64) -> Self {
		Self { data, pos: 0, offset }
	}

	fn next(&mut self) -> Result<Option<ChildBox<'a>>, Error> {
		if self.pos == self.data.len() {
			return Ok(None);
		}
		let here = self.offset + self.pos as u64;
		let rest = &self.data[self.pos..];
		let header = peek_box(rest, here)?.ok_or_else(|| Error::Truncated {
			offset: here,
			what: "child box shorter than its header".into(),
		})?;
		let size = header.size as usize;
		if rest.len() < size {
			return Err(Error::Malformed {
				offset: here,
				what: format!(
					"child box '{}' overruns its parent",
					kind_name(&header.kind)
				),
			});
		}
		let header_len = if rest[0..4] == [0, 0, 0, 1] { 16 } else { 8 };
		let child = ChildBox {
			kind: header.kind,
			payload: &rest[header_len..size],
			offset: here + header_len as u64,
		};
		self.pos += size;
		Ok(Some(child))
	}
}

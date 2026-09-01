//! Fragmented-MP4 construction from encoded h264 and AAC packets.
//!
//! The inverse of [`crate::fmp4`]: where that module cuts a muxed byte
//! stream into segments, this one builds the segments from the packets
//! an encoder emits. One [`Muxer`] serves one coded stream: its
//! [`init_segment`](Muxer::init_segment) is the `ftyp`+`moov` a decoder
//! needs before any sample, and each [`push`](Muxer::push) emits one
//! `moof`+`mdat` fragment PER SAMPLE - the hang container convention,
//! where each MoQ frame is a complete fragment - so media leaves as it
//! is encoded rather than in group-sized bursts. A sample's duration is
//! the packet's own where the wire carried one, and such a packet leaves
//! on its own push; a duration-less packet falls back to the decode-time
//! step to the next one, so its push emits the packet BEFORE it, and
//! [`finish`](Muxer::finish) flushes such a tail at the last known
//! duration.
//!
//! The group discipline is unchanged, only marked instead of buffered:
//! each fragment says whether its sample [starts a
//! group](Fragment::starts_group), and the transport rotates its groups
//! there.
//!
//! [`Muxer::video`] packages h264: an `avc1` sample entry whose `avcC`
//! is built from the stream's out-of-band SPS/PPS, samples stored
//! AVCC-framed (4-byte NAL lengths) after reframing the Annex-B bytes
//! the packets carry, and a group that starts at every keyframe.
//!
//! [`Muxer::audio`] packages AAC: an `mp4a` sample entry whose `esds`
//! carries the stream's AudioSpecificConfig, samples stored exactly as
//! the packets carry them. Every AAC frame is a sync sample, so there
//! is no keyframe to rotate on and a group starts once a target
//! duration has elapsed instead - see [`AUDIO_GROUP_SECONDS`]. A group
//! therefore always begins on a whole AAC frame, which is what a
//! decoder needs; it does not align with any video group, which is a
//! later concern.
//!
//! Timestamps: the media timescale is the stream time base's
//! denominator, so a tick value crosses unchanged when the numerator is
//! 1 and is scaled by it otherwise. A stream that reorders frames may
//! open with negative decode times; the whole decode timeline is
//! shifted up once so the first fragment starts at zero, and
//! presentation offsets carry the reordering per sample. The first
//! packets of such a stream may arrive with no decode time at all -
//! the wire does not settle it - and are held until one settles, when
//! the prefix gets decode times synthesized backwards at the settled
//! step and flushes.

use crate::avc;

/// One encoded packet, as the edge hands it over.
pub struct Packet<'a> {
	/// Presentation timestamp in stream time-base ticks.
	pub pts: i64,
	/// Decode timestamp; absent for the first packets of a reordering
	/// stream.
	pub dts: Option<i64>,
	/// How long the packet is presented, in stream ticks, as the wire
	/// carried it; None where it carried none. A packet with one needs no
	/// lookahead: its fragment can leave before the next packet arrives.
	pub duration: Option<i64>,
	/// Whether decoding can start at this packet.
	pub keyframe: bool,
	/// The encoded bytes, Annex-B.
	pub data: &'a [u8],
}

/// One sample as a complete `moof`+`mdat` fragment.
#[derive(Debug)]
pub struct Fragment {
	/// The fragment bytes, ready for the wire.
	pub bytes: Vec<u8>,
	/// The `mfhd` sequence number, from 1.
	pub sequence: u32,
	/// Whether the sample is a sync sample.
	pub keyframe: bool,
	/// Whether the sample starts a group: a video keyframe, an audio
	/// frame past the group target, and always the stream's first.
	pub starts_group: bool,
	/// The sample's presentation timestamp, in stream ticks as fed.
	pub pts: i64,
}

struct Sample {
	data: Vec<u8>,
	pts: i64,
	dts: Option<i64>,
	/// The wire's own duration, where the packet carried one.
	duration: Option<i64>,
	keyframe: bool,
	starts_group: bool,
}

/// How long an audio group runs before it closes. One second, so a
/// group holds a whole number of AAC frames and lands near a video
/// group of the GOP lengths this publishes at; the frame boundary is
/// exact, the video boundary is not.
pub const AUDIO_GROUP_SECONDS: i64 = 1;

/// What a muxer packages, and the sample entry that calls for.
enum Kind {
	Video {
		width: u32,
		height: u32,
		avcc: Vec<u8>,
	},
	Audio {
		sample_rate: u32,
		channels: u32,
		/// The AudioSpecificConfig, as the stream header carried it.
		asc: Vec<u8>,
		/// How long a group runs, in stream ticks.
		group_ticks: i64,
	},
}

/// Builds an init segment once and fragments as packets arrive.
pub struct Muxer {
	timescale: u32,
	/// Ticks multiply by this on the way to media time (the time
	/// base's numerator).
	tick_scale: i64,
	kind: Kind,
	sequence: u32,
	/// Samples whose duration the next decode time has yet to settle.
	pending: Vec<Sample>,
	/// The open group's first presentation timestamp; None before the
	/// first push.
	group_start_pts: Option<i64>,
	/// Added to every decode time; set when the first fragment closes.
	dts_shift: Option<i64>,
	/// The last computed sample duration, for the stream's final sample.
	last_duration: Option<i64>,
}

impl Muxer {
	/// A muxer for one coded h264 stream: Annex-B SPS/PPS extradata, the
	/// declared frame size, and the stream time base.
	pub fn video(
		extradata: &[u8],
		width: u32,
		height: u32,
		time_base_num: i32,
		time_base_den: i32,
	) -> Result<Self, String> {
		let (sps, pps) = avc::parse_parameter_sets(extradata);
		let avcc = avc::build_avcc(&sps, &pps)?;
		Self::build(
			Kind::Video {
				width,
				height,
				avcc,
			},
			time_base_num,
			time_base_den,
		)
	}

	/// A muxer for one coded AAC stream: the AudioSpecificConfig the
	/// stream header carried, the samples it declares, and the stream
	/// time base - whose tick is one sample, so the group target is the
	/// rate itself.
	pub fn audio(
		extradata: &[u8],
		sample_rate: u32,
		channels: u32,
		time_base_num: i32,
		time_base_den: i32,
	) -> Result<Self, String> {
		if extradata.is_empty() {
			return Err(
				"the audio stream carries no AudioSpecificConfig, so no esds can be built".into(),
			);
		}
		if channels == 0 {
			return Err("the audio stream declares no channels".into());
		}
		let group_ticks = AUDIO_GROUP_SECONDS * time_base_den.max(1) as i64
			/ time_base_num.max(1) as i64;
		Self::build(
			Kind::Audio {
				sample_rate,
				channels,
				asc: extradata.to_vec(),
				group_ticks: group_ticks.max(1),
			},
			time_base_num,
			time_base_den,
		)
	}

	fn build(kind: Kind, time_base_num: i32, time_base_den: i32) -> Result<Self, String> {
		if time_base_num <= 0 || time_base_den <= 0 {
			return Err(format!(
				"time base {time_base_num}/{time_base_den} is not positive"
			));
		}
		Ok(Self {
			timescale: time_base_den as u32,
			tick_scale: time_base_num as i64,
			kind,
			sequence: 1,
			pending: Vec::new(),
			group_start_pts: None,
			dts_shift: None,
			last_duration: None,
		})
	}

	/// The `avcC` payload built from the extradata, for inspection.
	/// None for an audio muxer, which builds an `esds` instead.
	pub fn avcc(&self) -> Option<&[u8]> {
		match &self.kind {
			Kind::Video { avcc, .. } => Some(avcc),
			Kind::Audio { .. } => None,
		}
	}

	/// The decoder configuration a WebCodecs `description` carries: the
	/// `avcC` record for video, the AudioSpecificConfig for audio.
	pub fn decoder_config(&self) -> &[u8] {
		match &self.kind {
			Kind::Video { avcc, .. } => avcc,
			Kind::Audio { asc, .. } => asc,
		}
	}

	/// The `ftyp`+`moov` a decoder reads before any fragment.
	pub fn init_segment(&self) -> Vec<u8> {
		let mut out = ftyp();
		out.extend_from_slice(&self.moov());
		out
	}

	/// Collects one packet and returns every fragment it settles: this
	/// one, when it carries its own duration - else the packet BEFORE
	/// it, whose duration its decode time is - or several, where its
	/// decode time settles a held prefix - or none, while the stream's
	/// reorder delay keeps decode times unsettled.
	pub fn push(&mut self, packet: Packet<'_>) -> Result<Vec<Fragment>, String> {
		let (data, sync) = match &self.kind {
			Kind::Video { .. } => {
				let data = avc::annexb_to_avcc(packet.data);
				if data.is_empty() {
					return Err(format!(
						"the packet at pts {} carries no Annex-B start code",
						packet.pts
					));
				}
				(data, packet.keyframe)
			}
			// An AAC frame is stored as it arrived, and every one of
			// them can be decoded from.
			Kind::Audio { .. } => {
				if packet.data.is_empty() {
					return Err(format!("the packet at pts {} carries no bytes", packet.pts));
				}
				(packet.data.to_vec(), true)
			}
		};
		let starts_group = self.starts_a_group(&packet);
		if starts_group {
			self.group_start_pts = Some(packet.pts);
		}
		self.pending.push(Sample {
			data,
			pts: packet.pts,
			dts: packet.dts,
			duration: packet.duration.filter(|d| *d > 0),
			keyframe: sync,
			starts_group,
		});
		self.drain(false)
	}

	/// Whether `packet` opens a new group. Video rotates where the
	/// encoder put its keyframes; audio has none to rotate on and
	/// rotates on elapsed ticks. The stream's first packet opens the
	/// first group.
	fn starts_a_group(&self, packet: &Packet<'_>) -> bool {
		let Some(start) = self.group_start_pts else {
			return true;
		};
		match self.kind {
			// A keyframe opens a group, but not more often than once a
			// second: an all-intra stream (every frame an IDR, the shape
			// that survives a mid-group join on a relay that replays
			// nothing) would otherwise make every frame its own group.
			// Every keyframe opens a group. An all-intra stream makes every
			// frame its own group, which is the point: a subscriber can only
			// ever join at a group start, so no join lands mid-group.
			Kind::Video { .. } => packet.keyframe,
			Kind::Audio { group_ticks, .. } => packet.pts - start >= group_ticks,
		}
	}

	/// Flushes the samples still held: a duration-less last one, whose
	/// duration no following decode time will settle - its own wire
	/// duration or the last known one stands in - and any prefix a
	/// reorder delay kept unsettled to the end.
	pub fn finish(&mut self) -> Result<Vec<Fragment>, String> {
		self.drain(true)
	}

	/// Emits every pending sample whose decode time AND duration are
	/// settled. A sample's duration is the wire's own where its packet
	/// carried one; only a duration-less tail waits for the next decode
	/// time to settle it as a step. A flush emits everything, such a tail
	/// at the last known duration.
	fn drain(&mut self, flush: bool) -> Result<Vec<Fragment>, String> {
		let dts = self.resolved_dts(flush)?;
		// `dts` covers no pending sample or every one of them, so only
		// the final sample can lack a settled duration.
		let holds_tail =
			!flush && !dts.is_empty() && self.pending[dts.len() - 1].duration.is_none();
		let emit = dts.len() - usize::from(holds_tail);
		let mut out = Vec::with_capacity(emit);
		if emit == 0 {
			return Ok(out);
		}
		let samples: Vec<Sample> = self.pending.drain(..emit).collect();
		for (i, sample) in samples.into_iter().enumerate() {
			let duration = match (sample.duration, dts.get(i + 1)) {
				(Some(own), _) => own,
				(None, Some(next)) => {
					let step = next - dts[i];
					if step < 0 {
						return Err(format!(
							"decode time steps backwards at pts {}",
							sample.pts
						));
					}
					step
				}
				(None, None) => self.last_duration.unwrap_or(0),
			};
			if duration > 0 {
				self.last_duration = Some(duration);
			}
			out.push(self.emit(sample, dts[i], duration)?);
		}
		Ok(out)
	}

	/// One decode time per pending sample, for as many as are settled
	/// or synthesizable now: settled ones as they came, an unsettled
	/// prefix synthesized backwards from the first settled pair at its
	/// step. Empty while nothing has settled, or while a prefix waits
	/// for the pair; a flush resolves everything, presentation order
	/// standing in for a stream that never settled a decode time.
	fn resolved_dts(&self, flush: bool) -> Result<Vec<i64>, String> {
		let samples = &self.pending;
		if samples.is_empty() {
			return Ok(Vec::new());
		}
		let Some(k) = samples.iter().position(|s| s.dts.is_some()) else {
			// No decode time settled anywhere: at end of input this is
			// a stream that does not reorder, where presentation order
			// is decode order.
			return if flush {
				Ok(samples.iter().map(|s| s.pts).collect())
			} else {
				Ok(Vec::new())
			};
		};
		let known = samples[k].dts.expect("position found it");
		let step = samples.get(k + 1).and_then(|s| s.dts).map(|f| f - known);
		let step = match step {
			Some(step) => step,
			// No prefix to synthesize, so no step is needed.
			None if k == 0 => 0,
			None if flush => 0,
			// Hold the prefix until a second decode time sets the step.
			None => return Ok(Vec::new()),
		};
		let mut out = Vec::with_capacity(samples.len());
		for (i, sample) in samples.iter().enumerate() {
			match sample.dts {
				Some(dts) => out.push(dts),
				None if i < k => out.push(known - step * (k - i) as i64),
				None => {
					return Err(format!(
						"the packet at pts {} has no decode time after one settled",
						sample.pts
					))
				}
			}
		}
		Ok(out)
	}

	/// One sample into one `moof`+`mdat` fragment.
	fn emit(&mut self, sample: Sample, dts: i64, duration: i64) -> Result<Fragment, String> {
		let shift = *self
			.dts_shift
			.get_or_insert(if dts < 0 { -dts } else { 0 });
		let base_decode_time = dts + shift;
		if base_decode_time < 0 {
			return Err(format!(
				"decode time {base_decode_time} below the stream's start",
			));
		}

		let cts = (sample.pts - dts) * self.tick_scale;
		let cts: i32 = cts.try_into().map_err(|_| {
			format!("presentation offset {cts} at pts {} overflows", sample.pts)
		})?;
		let duration = duration * self.tick_scale;
		let duration: u32 = duration
			.try_into()
			.map_err(|_| format!("duration {duration} at pts {} overflows", sample.pts))?;
		let flags: u32 = if sample.keyframe { 0x0200_0000 } else { 0x0101_0000 };
		let entry = TrunEntry {
			duration,
			size: sample.data.len() as u32,
			flags,
			cts,
		};

		let mut bytes = moof(
			self.sequence,
			(base_decode_time * self.tick_scale) as u64,
			&entry,
		);
		bytes.extend_from_slice(&((sample.data.len() + 8) as u32).to_be_bytes());
		bytes.extend_from_slice(b"mdat");
		bytes.extend_from_slice(&sample.data);

		let fragment = Fragment {
			bytes,
			sequence: self.sequence,
			keyframe: sample.keyframe,
			starts_group: sample.starts_group,
			pts: sample.pts,
		};
		self.sequence += 1;
		Ok(fragment)
	}

	fn moov(&self) -> Vec<u8> {
		let (stsd_child, handler, handler_name, media_header) = match &self.kind {
			Kind::Video {
				width,
				height,
				avcc,
			} => (
				avc1(*width, *height, avcc),
				b"vide",
				&b"VideoHandler\0"[..],
				full_boxed(b"vmhd", 0, 1, vec![0; 8]),
			),
			Kind::Audio {
				sample_rate,
				channels,
				asc,
				..
			} => (
				mp4a(*sample_rate, *channels, asc),
				b"soun",
				&b"SoundHandler\0"[..],
				// balance and reserved, both zero.
				full_boxed(b"smhd", 0, 0, vec![0; 4]),
			),
		};
		let stbl = boxed(
			b"stbl",
			[
				full_boxed(b"stsd", 0, 0, {
					let mut p = 4u32.to_be_bytes().to_vec();
					p[3] = 1; // entry_count 1
					p.extend_from_slice(&stsd_child);
					p
				}),
				full_boxed(b"stts", 0, 0, vec![0; 4]),
				full_boxed(b"stsc", 0, 0, vec![0; 4]),
				full_boxed(b"stsz", 0, 0, vec![0; 8]),
				full_boxed(b"stco", 0, 0, vec![0; 4]),
			]
			.concat(),
		);
		let dinf = boxed(
			b"dinf",
			full_boxed(b"dref", 0, 0, {
				let mut p = vec![0, 0, 0, 1]; // entry_count 1
				p.extend_from_slice(&full_boxed(b"url ", 0, 1, Vec::new()));
				p
			}),
		);
		let minf = boxed(b"minf", [media_header, dinf, stbl].concat());
		let hdlr = full_boxed(b"hdlr", 0, 0, {
			let mut p = vec![0; 4]; // pre_defined
			p.extend_from_slice(handler);
			p.extend_from_slice(&[0; 12]);
			p.extend_from_slice(handler_name);
			p
		});
		let mdhd = full_boxed(b"mdhd", 0, 0, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&self.timescale.to_be_bytes());
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&0x55c4u16.to_be_bytes()); // und
			p.extend_from_slice(&[0; 2]);
			p
		});
		let mdia = boxed(b"mdia", [mdhd, hdlr, minf].concat());
		// A video track states its frame size and no volume; an audio
		// track states full volume and no frame size.
		let (volume, width, height) = match self.kind {
			Kind::Video { width, height, .. } => (0u16, width, height),
			Kind::Audio { .. } => (0x0100, 0, 0),
		};
		let tkhd = full_boxed(b"tkhd", 0, 3, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&1u32.to_be_bytes()); // track_ID
			p.extend_from_slice(&[0; 4]); // reserved
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&[0; 8]); // reserved
			p.extend_from_slice(&[0; 4]); // layer, alternate group
			p.extend_from_slice(&volume.to_be_bytes());
			p.extend_from_slice(&[0; 2]); // reserved
			p.extend_from_slice(&MATRIX);
			p.extend_from_slice(&(width << 16).to_be_bytes());
			p.extend_from_slice(&(height << 16).to_be_bytes());
			p
		});
		let trak = boxed(b"trak", [tkhd, mdia].concat());
		let mvex = boxed(
			b"mvex",
			full_boxed(b"trex", 0, 0, {
				let mut p = 1u32.to_be_bytes().to_vec(); // track_ID
				p.extend_from_slice(&1u32.to_be_bytes()); // sample description
				p.extend_from_slice(&[0; 12]); // default duration, size, flags
				p
			}),
		);
		let mvhd = full_boxed(b"mvhd", 0, 0, {
			let mut p = vec![0; 8]; // creation, modification
			p.extend_from_slice(&1000u32.to_be_bytes()); // movie timescale
			p.extend_from_slice(&[0; 4]); // duration: told by fragments
			p.extend_from_slice(&0x0001_0000u32.to_be_bytes()); // rate 1.0
			p.extend_from_slice(&0x0100u16.to_be_bytes()); // volume 1.0
			p.extend_from_slice(&[0; 10]); // reserved
			p.extend_from_slice(&MATRIX);
			p.extend_from_slice(&[0; 24]); // pre_defined
			p.extend_from_slice(&2u32.to_be_bytes()); // next_track_ID
			p
		});
		boxed(b"moov", [mvhd, trak, mvex].concat())
	}
}

/// Converts stream ticks to the microseconds a MoQ timestamp counts,
/// clamped at zero on the way into an unsigned wire field.
pub fn ticks_to_micros(ticks: i64, time_base_num: i32, time_base_den: i32) -> u64 {
	if ticks <= 0 || time_base_num <= 0 || time_base_den <= 0 {
		return 0;
	}
	(ticks as u128 * time_base_num as u128 * 1_000_000 / time_base_den as u128) as u64
}

/// The identity transformation matrix every mp4 header carries.
const MATRIX: [u8; 36] = {
	let mut m = [0u8; 36];
	m[1] = 0x01; // 0x00010000
	m[17] = 0x01;
	m[32] = 0x40; // 0x40000000
	m
};

struct TrunEntry {
	duration: u32,
	size: u32,
	flags: u32,
	cts: i32,
}

fn ftyp() -> Vec<u8> {
	let mut p = Vec::new();
	p.extend_from_slice(b"iso5"); // major brand
	p.extend_from_slice(&512u32.to_be_bytes()); // minor version
	p.extend_from_slice(b"iso5isomavc1mp41"); // compatible brands
	boxed(b"ftyp", p)
}

fn moof(sequence: u32, base_decode_time: u64, entry: &TrunEntry) -> Vec<u8> {
	// Sizes first, so the trun's data offset can point past the moof
	// into the mdat payload before either is assembled.
	let trun_size = 20 + 16;
	let traf_size = 8 + 16 + 20 + trun_size;
	let moof_size = 8 + 16 + traf_size;
	let data_offset = (moof_size + 8) as i32;

	let mfhd = full_boxed(b"mfhd", 0, 0, sequence.to_be_bytes().to_vec());
	let tfhd = full_boxed(b"tfhd", 0, 0x020000, 1u32.to_be_bytes().to_vec());
	let tfdt = full_boxed(b"tfdt", 1, 0, base_decode_time.to_be_bytes().to_vec());
	let trun = full_boxed(b"trun", 1, 0xf01, {
		let mut p = 1u32.to_be_bytes().to_vec();
		p.extend_from_slice(&data_offset.to_be_bytes());
		p.extend_from_slice(&entry.duration.to_be_bytes());
		p.extend_from_slice(&entry.size.to_be_bytes());
		p.extend_from_slice(&entry.flags.to_be_bytes());
		p.extend_from_slice(&entry.cts.to_be_bytes());
		p
	});
	let traf = boxed(b"traf", [tfhd, tfdt, trun].concat());
	let built = boxed(b"moof", [mfhd, traf].concat());
	debug_assert_eq!(built.len(), moof_size);
	built
}

/// The avc1 sample entry, its `avcC` inside.
fn avc1(width: u32, height: u32, avcc: &[u8]) -> Vec<u8> {
	let mut p = Vec::new();
	p.extend_from_slice(&[0; 6]); // reserved
	p.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
	p.extend_from_slice(&[0; 16]); // pre_defined, reserved
	p.extend_from_slice(&(width as u16).to_be_bytes());
	p.extend_from_slice(&(height as u16).to_be_bytes());
	p.extend_from_slice(&0x0048_0000u32.to_be_bytes()); // 72 dpi
	p.extend_from_slice(&0x0048_0000u32.to_be_bytes());
	p.extend_from_slice(&[0; 4]); // reserved
	p.extend_from_slice(&1u16.to_be_bytes()); // frame_count
	p.extend_from_slice(&[0; 32]); // compressorname, empty
	p.extend_from_slice(&0x0018u16.to_be_bytes()); // depth
	p.extend_from_slice(&(-1i16).to_be_bytes()); // pre_defined
	p.extend_from_slice(&boxed(b"avcC", avcc.to_vec()));
	boxed(b"avc1", p)
}

/// The mp4a sample entry, its `esds` inside.
fn mp4a(sample_rate: u32, channels: u32, asc: &[u8]) -> Vec<u8> {
	let mut p = Vec::new();
	p.extend_from_slice(&[0; 6]); // reserved
	p.extend_from_slice(&1u16.to_be_bytes()); // data_reference_index
	p.extend_from_slice(&[0; 8]); // version, revision, vendor
	p.extend_from_slice(&(channels.min(0xffff) as u16).to_be_bytes());
	p.extend_from_slice(&16u16.to_be_bytes()); // samplesize
	p.extend_from_slice(&[0; 4]); // pre_defined, reserved
	// 16.16 fixed point, so a rate past 65535 does not fit and is left
	// at zero - the esds below carries the real one either way.
	let rate = if sample_rate > u32::from(u16::MAX) {
		0
	} else {
		sample_rate << 16
	};
	p.extend_from_slice(&rate.to_be_bytes());
	p.extend_from_slice(&full_boxed(b"esds", 0, 0, esds(asc)));
	boxed(b"mp4a", p)
}

/// The `esds` payload: the ES descriptor holding a decoder config whose
/// specific info is the stream's own AudioSpecificConfig.
fn esds(asc: &[u8]) -> Vec<u8> {
	let specific = descriptor(0x05, asc.to_vec());
	let config = descriptor(0x04, {
		let mut p = vec![
			0x40, // MPEG-4 audio
			0x15, // audio stream, not upstream
			0, 0, 0, // buffer size
		];
		p.extend_from_slice(&0u32.to_be_bytes()); // max bitrate: unstated
		p.extend_from_slice(&0u32.to_be_bytes()); // average bitrate: unstated
		p.extend_from_slice(&specific);
		p
	});
	// SLConfigDescriptor, predefined 2: the timing an mp4 track carries.
	let sl = descriptor(0x06, vec![0x02]);
	descriptor(0x03, {
		let mut p = 0u16.to_be_bytes().to_vec(); // ES_ID
		p.push(0); // stream priority, no dependency, no URL
		p.extend_from_slice(&config);
		p.extend_from_slice(&sl);
		p
	})
}

/// One MPEG-4 descriptor: a tag, its length in the seven-bits-a-byte
/// coding descriptors use, then the payload.
fn descriptor(tag: u8, payload: Vec<u8>) -> Vec<u8> {
	let mut out = vec![tag];
	let mut length = payload.len();
	let mut coded = vec![(length & 0x7f) as u8];
	length >>= 7;
	while length > 0 {
		coded.push((length & 0x7f) as u8 | 0x80);
		length >>= 7;
	}
	coded.reverse();
	out.extend_from_slice(&coded);
	out.extend_from_slice(&payload);
	out
}

fn boxed(kind: &[u8; 4], payload: Vec<u8>) -> Vec<u8> {
	let mut out = Vec::with_capacity(8 + payload.len());
	out.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
	out.extend_from_slice(kind);
	out.extend_from_slice(&payload);
	out
}

fn full_boxed(kind: &[u8; 4], version: u8, flags: u32, payload: Vec<u8>) -> Vec<u8> {
	let mut full = Vec::with_capacity(4 + payload.len());
	full.push(version);
	full.extend_from_slice(&flags.to_be_bytes()[1..]);
	full.extend_from_slice(&payload);
	boxed(kind, full)
}

#[cfg(test)]
mod tests {
	use super::*;

	// A syntactically plausible SPS/PPS pair, constrained-baseline so
	// the avcC needs no extension bytes.
	const EXTRADATA: &[u8] = &[
		0, 0, 0, 1, 0x67, 66, 0xc0, 30, 0xab, 0xcd, // SPS, profile 66
		0, 0, 0, 1, 0x68, 0xee, 0x06, 0xf2, // PPS
	];

	fn muxer() -> Muxer {
		Muxer::video(EXTRADATA, 320, 180, 1, 30).expect("a muxer")
	}

	/// The 5-byte AudioSpecificConfig ffmpeg writes for 48 kHz mono
	/// AAC-LC, as the NUT stream header carries it.
	const ASC: &[u8] = &[0x11, 0x88, 0x56, 0xe5, 0x00];

	/// An audio muxer at the time base a NUT audio stream declares: one
	/// tick per sample.
	fn audio_muxer() -> Muxer {
		Muxer::audio(ASC, 48000, 1, 1, 48000).expect("a muxer")
	}

	/// One AAC frame's worth of packet: 1024 samples on, and bytes that
	/// cross unchanged.
	fn aac_frame(index: i64) -> Vec<u8> {
		vec![0x21, index as u8, 0x10, 0x04]
	}

	fn packet(pts: i64, keyframe: bool) -> Vec<u8> {
		// One slice-looking NAL behind a start code; pts stamps it.
		vec![0, 0, 0, 1, if keyframe { 0x65 } else { 0x41 }, pts as u8]
	}

	#[test]
	fn each_push_settles_the_sample_before_it() {
		let mut mux = muxer();
		let mut fragments = Vec::new();
		for pts in 0..4i64 {
			let keyframe = pts % 3 == 0;
			let data = packet(pts, keyframe);
			let emitted = mux
				.push(Packet {
					pts,
					dts: Some(pts),
					duration: None,
					keyframe,
					data: &data,
				})
				.expect("push");
			// The first push has nothing settled; each later one
			// settles exactly the packet before it.
			assert_eq!(emitted.len(), usize::from(pts > 0));
			fragments.extend(emitted);
		}
		fragments.extend(mux.finish().expect("finish"));
		assert!(mux.finish().expect("finish").is_empty());

		assert_eq!(fragments.len(), 4, "one fragment per sample");
		for (i, fragment) in fragments.iter().enumerate() {
			assert_eq!(fragment.sequence, i as u32 + 1);
			assert_eq!(fragment.pts, i as i64);
			assert_eq!(trun_sample_count(&fragment.bytes), 1);
		}
		// Groups start at the stream's first sample and at each
		// keyframe; the sync flag marks exactly the keyframes, in the
		// fragment and in its trun entry.
		let starts: Vec<bool> = fragments.iter().map(|f| f.starts_group).collect();
		assert_eq!(starts, [true, false, false, true]);
		let syncs: Vec<bool> = fragments.iter().map(|f| f.keyframe).collect();
		assert_eq!(syncs, [true, false, false, true]);
		for fragment in &fragments {
			let non_sync = trun_first_flags(&fragment.bytes) & 0x0001_0000 != 0;
			assert_eq!(non_sync, !fragment.keyframe, "trun sync flag");
		}
	}

	#[test]
	fn a_packet_with_its_own_duration_leaves_on_its_own_push() {
		let mut mux = muxer();
		for pts in 0..3i64 {
			let data = packet(pts, pts == 0);
			let emitted = mux
				.push(Packet {
					pts,
					dts: Some(pts),
					duration: Some(1),
					keyframe: pts == 0,
					data: &data,
				})
				.expect("push");
			// No lookahead: the wire settled the duration, so nothing
			// waits for the next packet.
			assert_eq!(emitted.len(), 1, "pts {pts}");
			assert_eq!(emitted[0].pts, pts);
			assert_eq!(trun_entry(&emitted[0].bytes).0, 1, "the wire's duration");
		}
		assert!(mux.finish().expect("finish").is_empty(), "nothing held");
	}

	#[test]
	fn the_final_sample_keeps_its_real_duration() {
		let mut mux = muxer();
		// Duration-less packets two ticks apart, then a final one whose
		// wire duration (5) disagrees with the last known step (2): its
		// fragment must keep the real one rather than the stand-in, and
		// carrying its own duration it leaves without waiting.
		let mut fragments = Vec::new();
		for pts in [0i64, 2] {
			let data = packet(pts, pts == 0);
			fragments.extend(
				mux.push(Packet {
					pts,
					dts: Some(pts),
					duration: None,
					keyframe: pts == 0,
					data: &data,
				})
				.expect("push"),
			);
		}
		let data = packet(4, false);
		fragments.extend(
			mux.push(Packet {
				pts: 4,
				dts: Some(4),
				duration: Some(5),
				keyframe: false,
				data: &data,
			})
			.expect("push"),
		);
		assert_eq!(
			fragments.iter().map(|f| f.pts).collect::<Vec<_>>(),
			[0, 2, 4],
			"the wire-settled packet needed no flush"
		);
		let durations: Vec<u32> = fragments.iter().map(|f| trun_entry(&f.bytes).0).collect();
		assert_eq!(durations, [2, 2, 5]);
		assert!(mux.finish().expect("finish").is_empty());
	}

	#[test]
	fn a_duration_less_packet_still_waits_for_its_successor() {
		let mut mux = muxer();
		let with = packet(0, true);
		let emitted = mux
			.push(Packet {
				pts: 0,
				dts: Some(0),
				duration: Some(1),
				keyframe: true,
				data: &with,
			})
			.expect("push");
		assert_eq!(emitted.len(), 1, "the wire-settled packet leaves");
		let without = packet(1, false);
		let held = mux
			.push(Packet {
				pts: 1,
				dts: Some(1),
				duration: None,
				keyframe: false,
				data: &without,
			})
			.expect("push");
		assert!(held.is_empty(), "the lookahead is still the fallback");
		let tail = mux.finish().expect("finish");
		assert_eq!(tail.len(), 1);
		assert_eq!(
			trun_entry(&tail[0].bytes).0,
			1,
			"the tail stands in at the last known duration"
		);
	}

	#[test]
	fn an_unsettled_decode_prefix_is_held_then_synthesized_backwards() {
		let mut mux = muxer();
		// Decode times settle at the third packet, as a reordering
		// stream's do; nothing may leave before the settled PAIR sets
		// the step, and the synthesized prefix must keep it.
		for (pts, dts) in [(2i64, None), (0, None), (1, Some(0i64))] {
			let data = packet(pts, pts == 2);
			let held = mux
				.push(Packet {
					pts,
					dts,
					duration: None,
					keyframe: pts == 2,
					data: &data,
				})
				.expect("push");
			assert!(held.is_empty(), "nothing may leave before the step is known");
		}
		let data = packet(3, false);
		let emitted = mux
			.push(Packet {
				pts: 3,
				dts: Some(1),
				duration: None,
				keyframe: false,
				data: &data,
			})
			.expect("push");
		// The settled pair 0,1 sets the step; the prefix resolves to
		// -2, -1 and three samples flush, the fourth still waiting on
		// its own duration.
		assert_eq!(emitted.iter().map(|f| f.pts).collect::<Vec<_>>(), [2, 0, 1]);
		// Synthesized: dts -2 behind the settled 0 - so the shift
		// lifts the first fragment's base decode time to zero exactly,
		// and the next decodes one tick later.
		// Past the tag: version(1) + flags(3), then the 64-bit time.
		let tfdt = past_tag(&emitted[0].bytes, b"tfdt");
		assert_eq!(&emitted[0].bytes[tfdt..tfdt + 12], &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		let tfdt = past_tag(&emitted[1].bytes, b"tfdt");
		assert_eq!(&emitted[1].bytes[tfdt..tfdt + 12], &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
		// The reorder still crosses as presentation offsets: the first
		// sample shows 4 ticks ahead of its decode time.
		let entry = trun_entry(&emitted[0].bytes);
		assert_eq!(entry.3, 4, "cts offset of the reordered sample");

		let tail = mux.finish().expect("finish");
		assert_eq!(tail.iter().map(|f| f.pts).collect::<Vec<_>>(), [3]);
	}

	#[test]
	fn a_packet_without_start_codes_is_refused() {
		let mut mux = muxer();
		let err = mux
			.push(Packet {
				pts: 0,
				dts: Some(0),
				duration: None,
				keyframe: true,
				data: &[1, 2, 3],
			})
			.expect_err("no start code");
		assert!(err.contains("start code"), "{err}");
	}

	#[test]
	fn an_audio_group_starts_past_the_target_duration() {
		let mut mux = audio_muxer();
		let mut fragments = Vec::new();
		for index in 0..49i64 {
			let data = aac_frame(index);
			fragments.extend(
				mux.push(Packet {
					pts: index * 1024,
					dts: Some(index * 1024),
					duration: None,
					keyframe: true,
					data: &data,
				})
				.expect("push"),
			);
		}
		fragments.extend(mux.finish().expect("finish"));
		assert_eq!(fragments.len(), 49, "one fragment per AAC frame");
		// 1024 samples a frame at 48 kHz: the 47th frame is the first
		// whose timestamp stands a whole second past the group's
		// first, so it is the one that starts the next group.
		let starts: Vec<usize> = fragments
			.iter()
			.enumerate()
			.filter(|(_, f)| f.starts_group)
			.map(|(i, _)| i)
			.collect();
		assert_eq!(starts, [0, 47]);
		assert!(
			fragments.iter().all(|f| f.keyframe),
			"every AAC frame is a sync sample"
		);
	}

	#[test]
	fn an_audio_packet_crosses_without_reframing() {
		let mut mux = audio_muxer();
		// Bytes that would be an Annex-B start code are still just
		// bytes here: audio is stored exactly as it arrived.
		let data = vec![0, 0, 0, 1, 0x65, 0x2a];
		mux.push(Packet {
			pts: 0,
			dts: Some(0),
			duration: None,
			keyframe: true,
			data: &data,
		})
		.expect("push");
		let fragments = mux.finish().expect("finish");
		let fragment = fragments.first().expect("a fragment");
		let mdat = past_tag(&fragment.bytes, b"mdat");
		assert_eq!(&fragment.bytes[mdat..], &data[..]);
	}

	#[test]
	fn an_empty_audio_packet_is_refused() {
		let mut mux = audio_muxer();
		let err = mux
			.push(Packet {
				pts: 0,
				dts: Some(0),
				duration: None,
				keyframe: true,
				data: &[],
			})
			.expect_err("no bytes");
		assert!(err.contains("no bytes"), "{err}");
	}

	#[test]
	fn an_audio_stream_without_a_config_has_no_esds_to_build() {
		let err = match Muxer::audio(&[], 48000, 2, 1, 48000) {
			Ok(_) => panic!("an audio muxer needs a config to build an esds from"),
			Err(err) => err,
		};
		assert!(err.contains("AudioSpecificConfig"), "{err}");
	}

	#[test]
	fn the_audio_init_segment_carries_an_mp4a_entry_around_the_config() {
		let init = audio_muxer().init_segment();
		assert!(audio_muxer().avcc().is_none(), "an audio muxer builds esds");
		let mp4a = past_tag(&init, b"mp4a");
		// Past the tag: 6 reserved, data_reference_index, 8 more
		// reserved, then the channel count and sample size.
		assert_eq!(&init[mp4a + 16..mp4a + 20], &[0, 1, 0, 16]);
		// The rate is 16.16 fixed point, so 48000 sits in the top half.
		assert_eq!(&init[mp4a + 24..mp4a + 28], &(48000u32 << 16).to_be_bytes());
		// The esds descriptor chain ends with the config verbatim.
		let esds = past_tag(&init, b"esds");
		let config = init[esds..]
			.windows(ASC.len())
			.position(|w| w == ASC)
			.expect("the config is in the esds");
		// Tag 0x05 and its length byte stand immediately before it.
		assert_eq!(
			&init[esds + config - 2..esds + config],
			&[0x05, ASC.len() as u8]
		);
		// The handler is sound, not video.
		assert!(init.windows(4).any(|w| w == b"soun"));
		assert!(init.windows(4).any(|w| w == b"smhd"));
		assert!(!init.windows(4).any(|w| w == b"vmhd"));
	}

	#[test]
	fn a_long_config_codes_its_descriptor_length_across_bytes() {
		// A payload past 127 bytes needs the continuation coding, and
		// the length must still read back as the payload's own.
		let long = vec![0x11u8; 200];
		let coded = descriptor(0x05, long.clone());
		assert_eq!(coded[0], 0x05);
		assert_eq!(&coded[1..3], &[0x81, 0x48]);
		assert_eq!(&coded[3..], &long[..]);
	}

	#[test]
	fn ticks_scale_to_microseconds() {
		assert_eq!(ticks_to_micros(30, 1, 30), 1_000_000);
		assert_eq!(ticks_to_micros(2048, 1, 61440), 33_333);
		assert_eq!(ticks_to_micros(-5, 1, 30), 0);
	}

	/// The trun's sample count.
	fn trun_sample_count(bytes: &[u8]) -> u32 {
		let payload = past_tag(bytes, b"trun") + 4; // version + flags
		u32::from_be_bytes(bytes[payload..payload + 4].try_into().expect("count"))
	}

	/// The trun's first entry flags.
	fn trun_first_flags(bytes: &[u8]) -> u32 {
		let (_, _, flags, _) = trun_entry(bytes);
		flags
	}

	/// The trun's first entry: duration, size, flags, cts.
	fn trun_entry(bytes: &[u8]) -> (u32, u32, u32, i32) {
		// Past the tag: version + flags, count, data offset.
		let entry = past_tag(bytes, b"trun") + 12;
		let field = |i: usize| {
			u32::from_be_bytes(bytes[entry + 4 * i..entry + 4 * i + 4].try_into().expect("field"))
		};
		(field(0), field(1), field(2), field(3) as i32)
	}

	/// Byte offset just past the first `kind` tag found.
	fn past_tag(bytes: &[u8], kind: &[u8; 4]) -> usize {
		bytes
			.windows(4)
			.position(|w| w == kind)
			.map(|p| p + 4)
			.expect("box present")
	}
}

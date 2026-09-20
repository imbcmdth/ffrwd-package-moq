wit_bindgen::generate!({
	path: "wit",
	world: "packet-sink-module",
});

use std::cell::RefCell;
use std::time::Duration;

use bytes::Bytes;
use exports::ffrwd::av::packet_sink::{
	Arity, Guest, InputStream, Meta, PacketSinkMeta, PadPackets, Processed,
};
use ffrwd::av::types::CodedFormat;
use serde::{Deserialize, Serialize};

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP, and a path the session opens under, which is where a relay's own address carries a token: https://relay.example/<JWT>"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none, and a token already in the relay URL needs none"},"audio_group_ms":{"type":"integer","minimum":0,"default":100,"description":"how long an audio group runs, in milliseconds; a tenth of a second is five AAC frames at 48kHz. 0 is one frame per group, which is what upstream hang writes and is experimental here: a player skips about one group in four"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// The base name a video track falls back to when its row's rendition
/// carries none, and the ladder holds only the one stream.
const DEFAULT_VIDEO_TRACK: &str = "video";

/// The base name an audio track falls back to under the same rule.
const DEFAULT_AUDIO_TRACK: &str = "audio";

/// One schema covers both row shapes: a group row carries `group`, the
/// trailing summary carries `groups`, and each leaves the other's
/// fields out. `pts_start`/`pts_end` are seconds of media time.
const ROWS_SCHEMA: &str = r#"{"type":"object","properties":{"track":{"type":"string"},"group":{"type":"integer"},"packets":{"type":"integer"},"bytes":{"type":"integer"},"pts_start":{"type":"number"},"pts_end":{"type":"number"},"tracks":{"type":"integer"},"groups":{"type":"integer"},"init_bytes":{"type":"integer"}},"additionalProperties":false}"#;

/// How long the session stays open after the last fragment, for the
/// wire to drain: there is no delivered signal for a subscription.
const DRAIN: Duration = Duration::from_secs(2);

/// How long a non-latest group is kept for a backlogged subscriber. The
/// default evicts after 5s, which is not enough for one that arrives
/// mid-broadcast and asks for the catalog.
const KEEP: Duration = Duration::from_secs(30);

/// How often the publisher re-checks for its first subscriber. Nothing is
/// published before one arrives: a subscription starts at the LATEST group,
/// so anything published earlier is simply gone.
const POLL: Duration = Duration::from_millis(20);

/// How long the first media is held for that first subscriber before it
/// goes out regardless. The hold keeps a file's start from being lost
/// to the latest-group rule, and the harness readers arrive within a
/// second - but an unwatched live publish must still flow: the pipes
/// feeding the module are bounded, and a stalled stage is killed. First
/// reader or this, whichever comes first.
const HOLD_MAX: Duration = Duration::from_secs(10);

/// How long a call keeps driving the session after it has written
/// something, and in what steps. The session makes progress only while
/// a host call is blocked on the executor, so a group finished at the
/// end of a call is bytes that have not left yet; these turns are what
/// puts them on the socket before the call returns. Small: a few
/// milliseconds against an AAC frame's 21ms, and nothing at all for a
/// call that wrote nothing.
const FLUSH_SLICE: Duration = Duration::from_millis(1);
const FLUSH_TURNS: usize = 3;

/// How often the catalog goes out again, the same snapshot in a fresh
/// group, while the broadcast lives. A relay that does not retain a
/// track's last closed group has nothing to hand a late joiner, whose
/// catalog.json subscription would otherwise wait forever. It is ~1KB;
/// the cost is nothing.
const CATALOG_REFRESH: Duration = Duration::from_secs(3);

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Params {
	relay: String,
	broadcast: String,
	#[serde(default)]
	cert: String,
	// The token rides the params JSON, which is visible on the sidecar
	// command line - scoped, expiring credentials only, the same caveat
	// the query text already carries.
	#[serde(default)]
	token: String,
	/// How long an audio group runs, in milliseconds; the default is a
	/// tenth of a second and 0 is one AAC frame per group, which is
	/// experimental. See [`moq_core::group::AUDIO_GROUP_MS`].
	#[serde(default = "default_audio_group_ms")]
	audio_group_ms: u32,
}

fn default_audio_group_ms() -> u32 {
	moq_core::group::AUDIO_GROUP_MS
}

/// One published group's row.
#[derive(Serialize)]
struct GroupRow {
	track: String,
	group: u64,
	packets: u64,
	bytes: u64,
	pts_start: f64,
	pts_end: f64,
}

/// The trailing summary, once per run over every track.
#[derive(Serialize)]
struct SummaryRow {
	tracks: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
	init_bytes: u64,
}

/// The tokio floor the session runs on. Split from [`Session`] so a
/// call can block on the executor while the work borrows the session.
struct Executor {
	runtime: tokio::runtime::Runtime,
	// The session driver is not Send on wasm, so it lives on a LocalSet
	// and runs whenever a call blocks here.
	local: tokio::task::LocalSet,
}

impl Executor {
	fn enter<T>(&self, work: impl std::future::Future<Output = T>) -> T {
		self.local.block_on(&self.runtime, work)
	}
}

/// What one track carries, past the fields every track has.
#[derive(Clone, Copy)]
enum Media {
	Video { width: u32, height: u32 },
	Audio { sample_rate: u32, channels: u32 },
}

/// One stream packaged before the relay is dialed, waiting for the MoQ
/// tracks it will publish on.
struct Built {
	muxer: ffrwd_bmff::mux::Muxer,
	codec: String,
	media: Media,
	time_base: (i32, i32),
	/// The NAL length prefix this track's `avcC` declares; 0 for audio,
	/// whose frames carry no framing of their own.
	length_size: usize,
}

/// One track: one encoded stream, its own fmp4 muxer, and the MoQ track
/// carrying its fragments. The init segment rides inside the catalog.
struct Rendition {
	name: String,
	codec: String,
	media: Media,
	track: Option<moq_net::track::Producer>,
	/// The open MoQ group, rotated where the group discipline says.
	group: Option<moq_net::group::Producer>,
	/// Which fragments open a group: this package's own convention, not
	/// anything the muxer decides.
	discipline: moq_core::group::Groups,
	muxer: ffrwd_bmff::mux::Muxer,
	time_base: (i32, i32),
	length_size: usize,
	init_bytes: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
	/// The open group's accumulators, for its row when it closes.
	group_packets: u64,
	group_bytes: u64,
	group_pts_min: i64,
	group_pts_max: i64,
}

impl Rendition {
	fn seconds(&self, ticks: i64) -> f64 {
		ticks as f64 * self.time_base.0 as f64 / self.time_base.1 as f64
	}

	/// One packet as the muxer stores it: an h264 packet reframed from
	/// the Annex-B the encoded edge carries into the length prefixes
	/// this track's `avcC` declares, an AAC frame exactly as it
	/// arrived. No NAL payload is touched either way.
	fn stored(&self, pts: i64, data: &[u8]) -> Result<Vec<u8>, String> {
		match self.media {
			Media::Video { .. } => {
				let framed =
					ffrwd_nal::annexb::annexb_to_length_prefixed(data, self.length_size)
						.map_err(|err| {
							format!("track '{}': the packet at pts {pts}: {err}", self.name)
						})?;
				if framed.is_empty() {
					return Err(format!(
						"track '{}': the packet at pts {pts} carries no Annex-B start code",
						self.name
					));
				}
				Ok(framed)
			}
			Media::Audio { .. } => Ok(data.to_vec()),
		}
	}

	/// Publishes one single-sample fragment as one MoQ frame in the
	/// open group, rotating the group where the discipline says one
	/// starts. A rotation closes the group before it, and a track whose
	/// groups hold one fragment apiece closes this one straight after
	/// writing it: a group is only forwarded once it is whole, so a
	/// finish that waits for the next fragment is a frame the reader
	/// waits on - and at the end of a batch, one it waits on until the
	/// batch after. Each close yields that group's row.
	fn publish_fragment(
		&mut self,
		fragment: ffrwd_bmff::mux::Fragment,
	) -> Result<Vec<String>, String> {
		let mut rows = Vec::new();
		if self.discipline.starts_a_group(fragment.keyframe, fragment.pts) {
			rows.extend(self.close_group()?);
			let track = self.track.as_mut().expect("track lives until last");
			self.group = Some(
				track
					.append_group()
					.map_err(|err| format!("moq group: {err}"))?,
			);
			self.group_packets = 0;
			self.group_bytes = 0;
			self.group_pts_min = fragment.pts;
			self.group_pts_max = fragment.pts;
		}
		let timestamp_us =
			ffrwd_bmff::time::ticks_to_micros(fragment.pts, self.time_base.0, self.time_base.1);
		let bytes_len = fragment.bytes.len() as u64;
		let group = self.group.as_mut().expect("a group start opened one");
		group
			.write_frame(
				moq_net::Timestamp::from_micros(timestamp_us)
					.map_err(|err| format!("moq timestamp: {err}"))?,
				Bytes::from(fragment.bytes),
			)
			.map_err(|err| format!("moq frame: {err}"))?;
		self.group_packets += 1;
		self.group_bytes += bytes_len;
		self.group_pts_min = self.group_pts_min.min(fragment.pts);
		self.group_pts_max = self.group_pts_max.max(fragment.pts);
		self.bytes += bytes_len;
		if self.discipline.one_fragment_each() {
			rows.extend(self.close_group()?);
		}
		Ok(rows)
	}

	/// Closes the open group, if any, and returns its row.
	fn close_group(&mut self) -> Result<Option<String>, String> {
		let Some(mut group) = self.group.take() else {
			return Ok(None);
		};
		group.finish().map_err(|err| format!("moq group: {err}"))?;
		let row = GroupRow {
			track: self.name.clone(),
			group: self.groups,
			packets: self.group_packets,
			bytes: self.group_bytes,
			pts_start: self.seconds(self.group_pts_min),
			pts_end: self.seconds(self.group_pts_max),
		};
		self.groups += 1;
		Ok(Some(
			serde_json::to_string(&row).expect("a group row serializes"),
		))
	}

	/// The catalog entry naming this track: its decoder configuration
	/// and its init segment inside.
	fn catalog_entry(&mut self) -> moq_core::catalog::Track {
		let init = self.muxer.init_segment();
		self.init_bytes = init.len() as u64;
		match self.media {
			Media::Video { width, height } => moq_core::catalog::Track::video(
				self.name.clone(),
				self.codec.clone(),
				self.muxer.config(),
				width,
				height,
				init,
			),
			Media::Audio {
				sample_rate,
				channels,
			} => moq_core::catalog::Track::audio(
				self.name.clone(),
				self.codec.clone(),
				self.muxer.config(),
				sample_rate,
				channels,
				init,
			),
		}
	}
}

/// One whole payload as one frame in a group of its own.
fn write_group(
	track: &mut moq_net::track::Producer,
	timestamp_us: u64,
	payload: Vec<u8>,
) -> Result<(), String> {
	let mut group = track
		.append_group()
		.map_err(|err| format!("moq group: {err}"))?;
	group
		.write_frame(
			moq_net::Timestamp::from_micros(timestamp_us)
				.map_err(|err| format!("moq timestamp: {err}"))?,
			Bytes::from(payload),
		)
		.map_err(|err| format!("moq frame: {err}"))?;
	group.finish().map_err(|err| format!("moq group: {err}"))
}

struct Session {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
	driver: Option<tokio::task::JoinHandle<()>>,
	broadcast: Option<moq_net::broadcast::Producer>,
	catalog: Option<moq_net::track::Producer>,
	renditions: Vec<Rendition>,
	params: Params,
	/// Set once a subscriber arrived and the init segments went out.
	started: bool,
	/// When the catalog last went out; see [`CATALOG_REFRESH`].
	catalog_sent: Option<std::time::Instant>,
}

struct State {
	executor: Executor,
	session: Session,
}

thread_local! {
	static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

fn parse_params(params: &str) -> Result<Params, String> {
	let params: Params = serde_json::from_str(params).map_err(|err| format!("params: {err}"))?;
	// The relay URL's path and the token argument are two spellings of
	// one field, so what the session will ask for is settled here, where
	// params are refused, rather than at the dial: a URL and a token that
	// disagree stop the run instead of publishing a broadcast nobody
	// scoped to the token can find.
	moq_core::relay::session_path(&params.relay, &params.token)?;
	Ok(params)
}

impl Session {
	/// Whether anything here has a subscriber at all, the catalog
	/// included. A subscription starts at the LATEST group, so nothing may
	/// be published before one exists: the relay subscribes upstream the
	/// moment a downstream subscriber wants a track, which is what
	/// surfaces here.
	fn wanted(&self) -> bool {
		let tracks = self
			.renditions
			.iter()
			.map(|rendition| rendition.track.as_ref())
			.chain(std::iter::once(self.catalog.as_ref()));
		tracks
			.flatten()
			.any(|track| track.subscription().is_some())
	}

	/// Whether any RENDITION has been asked for, which is what says a
	/// reader has got past the catalog and named the rung it wants.
	fn rendition_wanted(&self) -> bool {
		self.renditions.iter().any(|rendition| {
			rendition
				.track
				.as_ref()
				.is_some_and(|track| track.subscription().is_some())
		})
	}

	/// The catalog onto its own track, once, before any fragment. It
	/// carries every rendition's init segment, so nothing else has to go
	/// out before the media.
	fn publish_catalog(&mut self) -> Result<(), String> {
		let catalog = moq_core::catalog::Catalog::new(
			self.renditions
				.iter_mut()
				.map(Rendition::catalog_entry)
				.collect(),
		);
		let document = catalog.document()?;
		let track = self.catalog.as_mut().expect("catalog lives until last");
		write_group(track, 0, document)?;
		self.catalog_sent = Some(std::time::Instant::now());
		Ok(())
	}

	/// One host call's work: hold for the first subscriber, feed each
	/// pad's muxer, publish what closed, and on the final call drain and
	/// close the session.
	async fn drive(&mut self, pads: &[PadPackets], last: bool) -> Result<Processed, String> {
		let mut rows = Vec::new();
		let mut trailing = Vec::new();

		if !self.started {
			// The catalog goes out to the first reader of anything: it is
			// what a subscriber needs before it can name a rendition, so
			// holding it until a rendition is named would hold it forever.
			let deadline = tokio::time::Instant::now() + HOLD_MAX;
			while !self.wanted() && tokio::time::Instant::now() < deadline {
				tokio::time::sleep(POLL).await;
			}
			self.publish_catalog()?;
			// Then the media, once a rung has actually been asked for.
			//
			// A reader still choosing when this fires starts at the group
			// it arrives in, as a reader of any live broadcast does: a
			// subscription begins at the LATEST group, and there is no
			// rendezvous in the protocol to wait for one that has not
			// asked yet.
			while !self.rendition_wanted() && tokio::time::Instant::now() < deadline {
				tokio::time::sleep(POLL).await;
			}
			self.started = true;
		}
		// The same catalog again in a fresh group, for the late joiner
		// whose relay no longer holds the first one.
		if self
			.catalog_sent
			.is_some_and(|sent| sent.elapsed() >= CATALOG_REFRESH)
		{
			self.publish_catalog()?;
		}

		for (index, pad) in pads.iter().enumerate() {
			let Some(rendition) = self.renditions.get_mut(index) else {
				continue;
			};
			for packet in &pad.packets {
				let stored = rendition.stored(packet.pts, &packet.data)?;
				let fragments = rendition
					.muxer
					.push(ffrwd_bmff::mux::Packet {
						pts: packet.pts,
						dts: packet.dts,
						duration: packet.duration,
						// Every AAC frame can be decoded from, whatever
						// the wire said about it.
						keyframe: packet.keyframe
							|| matches!(rendition.media, Media::Audio { .. }),
						data: &stored,
					})
					.map_err(|err| {
						format!(
							"track '{}': the packet at pts {}: {err}",
							rendition.name, packet.pts
						)
					})?;
				rendition.packets += 1;
				for fragment in fragments {
					rows.extend(rendition.publish_fragment(fragment)?);
				}
			}
			// Let the driver move the frames onto the wire now, not
			// after the next call.
			tokio::task::yield_now().await;
		}

		// And drive it until the frames and the finishes written above
		// are actually handed to the socket. The QUIC session only runs
		// while a host call is blocked here, so a group written at the
		// end of a call would otherwise sit whole-but-unsent until the
		// next one - which for a group per frame is the difference
		// between a reader that plays it and a reader that gives up on
		// it. One yield lets the driver poll; the slices let its timers
		// and the socket's readiness catch up.
		if !rows.is_empty() {
			for _ in 0..FLUSH_TURNS {
				tokio::time::sleep(FLUSH_SLICE).await;
			}
		}

		if last {
			// Every track's last fragment goes out BEFORE any track is
			// finished: a subscriber stops at the finish, so a track told
			// it is over while its own tail is still queued loses that
			// tail. The yield is what lets the driver move them.
			for index in 0..self.renditions.len() {
				let flushed = self.renditions[index].muxer.finish().map_err(|err| {
					format!("track '{}': {err}", self.renditions[index].name)
				})?;
				for fragment in flushed {
					rows.extend(self.renditions[index].publish_fragment(fragment)?);
				}
				if let Some(row) = self.renditions[index].close_group()? {
					rows.push(row);
				}
			}
			tokio::time::sleep(DRAIN).await;
			for rendition in &mut self.renditions {
				if let Some(mut track) = rendition.track.take() {
					track.finish().map_err(|err| format!("moq track: {err}"))?;
				}
			}
			if let Some(mut catalog) = self.catalog.take() {
				catalog
					.finish()
					.map_err(|err| format!("moq track: {err}"))?;
			}
			// The finish still has to cross the wire; the session offers
			// no delivered signal, so hold it open briefly.
			tokio::time::sleep(DRAIN).await;
			drop(self.broadcast.take());
			drop(self.session.take());
			if let Some(driver) = self.driver.take() {
				let _ = driver.await;
			}
			self.endpoint.wait_idle().await;
			trailing.push(
				serde_json::to_string(&SummaryRow {
					tracks: self.renditions.len() as u64,
					groups: self.renditions.iter().map(|r| r.groups).sum(),
					packets: self.renditions.iter().map(|r| r.packets).sum(),
					bytes: self.renditions.iter().map(|r| r.bytes).sum(),
					init_bytes: self.renditions.iter().map(|r| r.init_bytes).sum(),
				})
				.expect("a summary row serializes"),
			);
		}

		Ok(Processed { rows, trailing })
	}
}

struct Publish;

impl Guest for Publish {
	fn describe() -> PacketSinkMeta {
		PacketSinkMeta {
			meta: Meta {
				name: "publish".to_string(),
				version: "0.5.0".to_string(),
				params_schema: PARAMS_SCHEMA.to_string(),
				rows_schema: ROWS_SCHEMA.to_string(),
				// No decoded payload ever arrives, so no format list fills in.
				pixel_formats: vec![],
				sample_formats: vec![],
				sample_rates: vec![],
				channel_counts: vec![],
				rows_language: vec![],
			},
			// The fmp4 packaging is codec-shaped: avcC from SPS/PPS for
			// video, esds from the AudioSpecificConfig for audio.
			video_codecs: vec!["h264".to_string()],
			audio_codecs: vec!["aac".to_string()],
			// One broadcast carries as many renditions as the query names,
			// and the audio it names beside them - or none, for a query
			// that has none.
			video: Arity::Many,
			audio: Arity::Any,
		}
	}

	fn init(streams: Vec<InputStream>, params: String) -> Result<(), String> {
		let params = parse_params(&params)?;
		let audio_group_ms = params.audio_group_ms;
		if streams.is_empty() {
			return Err("publish reads at least one stream".into());
		}
		// Every muxer is built before the session, so a stream this
		// module cannot package is refused before anything is dialed.
		let mut built = Vec::with_capacity(streams.len());
		for stream in &streams {
			let coded = &stream.coded;
			built.push(match &coded.format {
				CodedFormat::Video(video) => {
					if coded.codec != "h264" {
						return Err(format!(
							"publish packages h264 video, and this stream is {}",
							coded.codec
						));
					}
					// The avcC is built here, from the Annex-B SPS/PPS
					// the wire carried out of band, and handed to the
					// muxer as an opaque record: the container layer
					// reads no codec bytes.
					let (sps, pps) = ffrwd_nal::config::parse_parameter_sets(&coded.extradata);
					let avcc = ffrwd_nal::config::build_avcc(&sps, &pps).map_err(|err| {
						format!("the h264 stream's extradata builds no avcC: {err}")
					})?;
					// The stream's own profile and level name the codec;
					// parsing the avcC is the fallback for a wire that
					// does not say.
					let codec = match (coded.profile, coded.level) {
						(Some(profile), Some(level)) => ffrwd_nal::codec_string::avc_codec_from(
							profile as u8,
							level as u8,
							&avcc,
						),
						_ => ffrwd_nal::codec_string::avc_codec(&avcc),
					};
					let length_size = ffrwd_nal::config::avcc_length_size(&avcc);
					let muxer = ffrwd_bmff::mux::Muxer::video(
						ffrwd_bmff::mux::Video {
							kind: *b"avc1",
							width: video.width,
							height: video.height,
							config: avcc,
						},
						coded.time_base.num,
						coded.time_base.den,
					)
					.map_err(|err| {
						format!(
							"the h264 stream at time base {}/{} packages as no avc1 track: {err}",
							coded.time_base.num, coded.time_base.den
						)
					})?;
					Built {
						muxer,
						codec,
						media: Media::Video {
							width: video.width,
							height: video.height,
						},
						time_base: (coded.time_base.num, coded.time_base.den),
						length_size,
					}
				}
				CodedFormat::Audio(audio) => {
					if coded.codec != "aac" {
						return Err(format!(
							"publish packages aac audio, and this stream is {}",
							coded.codec
						));
					}
					let muxer = ffrwd_bmff::mux::Muxer::audio(
						ffrwd_bmff::mux::Audio {
							sample_rate: audio.sample_rate,
							channels: audio.channels,
							config: coded.extradata.clone(),
						},
						coded.time_base.num,
						coded.time_base.den,
					)
					.map_err(|err| {
						format!(
							"the aac stream at time base {}/{} packages as no mp4a track: {err}",
							coded.time_base.num, coded.time_base.den
						)
					})?;
					Built {
						muxer,
						// The AudioSpecificConfig crosses as extradata,
						// and is what names the codec.
						codec: moq_core::catalog::aac_codec(&coded.extradata),
						media: Media::Audio {
							sample_rate: audio.sample_rate,
							channels: audio.channels,
						},
						time_base: (coded.time_base.num, coded.time_base.den),
						length_size: 0,
					}
				}
			});
		}
		// Renditions come from the rows, not from argument names: every
		// stream carries the relation row it belongs to, and the row's
		// rendition-meta is what the source (a manifest, another moq
		// broadcast) said about it. A row with a video and an audio pad
		// is one muxed rendition; a video alone or an audio alone is its
		// own. The naming rule itself is a pure function in moq-core, so
		// it is unit-tested without a session or the wit types.
		let pads: Vec<moq_core::catalog::RowPad> = streams
			.iter()
			.zip(&built)
			.map(|(stream, b)| moq_core::catalog::RowPad {
				row: stream.row,
				kind: match b.media {
					Media::Video { height, .. } => moq_core::catalog::RowKind::Video { height },
					Media::Audio { .. } => moq_core::catalog::RowKind::Audio,
				},
				name: stream.rendition.name.clone(),
			})
			.collect();
		let names = moq_core::catalog::track_names_for_rows(
			&pads,
			DEFAULT_VIDEO_TRACK,
			DEFAULT_AUDIO_TRACK,
		);
		// What each track is worth when the session has more to send than
		// the wire takes. A relay reads every track of a broadcast on one
		// session and asks for them alike, so this tie-break is what keeps
		// a video keyframe from sitting in front of a sound.
		let priorities: Vec<u8> = built
			.iter()
			.map(|b| match b.media {
				Media::Video { .. } => moq_core::catalog::PRIORITY_VIDEO,
				Media::Audio { .. } => moq_core::catalog::PRIORITY_AUDIO,
			})
			.collect();

		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_time()
			.build()
			.map_err(|err| format!("runtime: {err}"))?;
		let local = tokio::task::LocalSet::new();
		let executor = Executor { runtime, local };

		// Everything moq-net touches runs on the runtime, model setup
		// included. The broadcast and its tracks exist before the
		// session: the session's driver announces whatever the origin
		// already carries.
		let (broadcast, catalog, tracks, connected) = executor.enter(async {
			let origin = moq_net::Origin::random().produce();
			let mut broadcast = origin
				.create_broadcast(
					params.broadcast.as_str(),
					moq_net::broadcast::Route::announced(),
				)
				.map_err(|err| format!("broadcast '{}': {err}", params.broadcast))?;
			// The default keep window evicts a non-latest group after
			// 5s; give a backlogged subscriber more rope.
			let info = moq_net::track::Info::default().with_latency_max(KEEP);
			let catalog_name = moq_core::catalog::TRACK;
			let catalog = broadcast
				.create_track(
					catalog_name,
					info.clone()
						.with_priority(moq_core::catalog::PRIORITY_CATALOG),
				)
				.map_err(|err| format!("track '{catalog_name}': {err}"))?;
			let mut tracks = Vec::with_capacity(names.len());
			for (name, priority) in names.iter().zip(&priorities) {
				// Audio also asks to be served in sequence order. A track's
				// groups are otherwise newest-first, which is right for
				// video - a late picture is worth less than the next one -
				// and wrong for sound, where a group skipped is a click and
				// the one behind it cannot stand in. moq-net carries this in
				// TRACK_INFO and a subscriber takes it as the default for
				// its own subscription, so it reaches the relay's re-serve
				// rather than only our own queue.
				let ordered = *priority == moq_core::catalog::PRIORITY_AUDIO;
				let track = broadcast
					.create_track(
						name.as_str(),
						info.clone().with_priority(*priority).with_ordered(ordered),
					)
					.map_err(|err| format!("track '{name}': {err}"))?;
				tracks.push(track);
			}
			let connected = moq_core::relay::dial(
				&params.relay,
				&params.cert,
				&params.token,
				moq_net::Client::new().with_publisher(origin.consume()),
			)
			.await?;
			Ok::<_, String>((broadcast, catalog, tracks, connected))
		})?;

		let renditions = built
			.into_iter()
			.zip(names)
			.zip(tracks)
			.map(|((built, name), track)| Rendition {
				name,
				codec: built.codec,
				media: built.media,
				track: Some(track),
				group: None,
				discipline: match built.media {
					Media::Video { .. } => moq_core::group::Groups::video(),
					Media::Audio { .. } => moq_core::group::Groups::audio(
						built.time_base.0,
						built.time_base.1,
						audio_group_ms,
					),
				},
				muxer: built.muxer,
				time_base: built.time_base,
				length_size: built.length_size,
				init_bytes: 0,
				groups: 0,
				packets: 0,
				bytes: 0,
				group_packets: 0,
				group_bytes: 0,
				group_pts_min: 0,
				group_pts_max: 0,
			})
			.collect();

		let driver = connected.driver;
		let driver = executor.local.spawn_local(async move {
			let _ = driver.await;
		});

		STATE.with(|s| {
			*s.borrow_mut() = Some(State {
				executor,
				session: Session {
					endpoint: connected.endpoint,
					session: Some(connected.session),
					driver: Some(driver),
					broadcast: Some(broadcast),
					catalog: Some(catalog),
					renditions,
					params,
					started: false,
					catalog_sent: None,
				},
			});
		});
		Ok(())
	}

	fn set_params(params: String) -> Result<(), String> {
		let asked = parse_params(&params)?;
		STATE.with(|s| {
			let holder = s.borrow();
			let state = holder.as_ref().ok_or("set-params before init")?;
			if asked != state.session.params {
				return Err(
					"publish cannot move to another relay, broadcast or track mid-stream".into(),
				);
			}
			Ok(())
		})
	}

	fn process(pads: Vec<PadPackets>, last: bool) -> Processed {
		STATE.with(|s| {
			let mut holder = s.borrow_mut();
			let state = holder.as_mut().expect("process called before init");

			// Nothing to publish and no close asked: stay off the network.
			if pads.iter().all(|pad| pad.packets.is_empty()) && !last {
				return Processed {
					rows: vec![],
					trailing: vec![],
				};
			}

			let State { executor, session } = state;
			match executor.enter(session.drive(&pads, last)) {
				Ok(processed) => {
					if last {
						*holder = None;
					}
					processed
				}
				// The call has no error channel; a failure mid-stream
				// can only stop the run, with the reason named.
				Err(err) => panic!("publish: {err}"),
			}
		})
	}
}

export!(Publish);

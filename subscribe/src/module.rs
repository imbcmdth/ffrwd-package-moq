use std::time::Duration;

use ffrwd_node::{
	Bound, CodedAudio, CodedFormat, CodedStream, CodedVideo, Init, Node, Out, Output, Packet,
	Rational, Shape, Tick,
};
use moq_core::order::{Hold, Queue};
use moq_core::subscribe::{Received, Start, Step};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::mpsc;

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP, and a path the session opens under, which is where a relay's own address carries a token: https://relay.example/<JWT>"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none, and a token already in the relay URL needs none"},"hold_ms":{"type":"integer","minimum":0,"description":"how long a hole in a track's group sequence is held open for the group that would fill it before the relay is asked for it, in milliseconds; left out, 1000 on a live join, which has given up the past already, and 30000 on a backlog join, the subscription's own latency window"},"hold_mib":{"type":"integer","minimum":1,"default":64,"description":"how much one track holds meanwhile, in MiB; 64 covers a 30s window up to about 17 Mbit/s"},"join_ms":{"type":"integer","minimum":0,"default":2000,"description":"how long a joining reader waits for a lower group sequence before it fixes its cursor on a backlog join, in milliseconds; 0 starts at the first group that arrives, and a live join never waits at all"},"reconnect_s":{"type":"integer","minimum":0,"default":60,"description":"how long a reader whose session the relay dropped keeps trying to open a new one and take its tracks up again at the live edge, in seconds; 0 ends the run on the first drop"},"start":{"type":"string","enum":["live","backlog"],"default":"live","description":"where to join a broadcast already running: 'live' at the publisher's newest group, starting at the first group a decoder can begin at - a keyframe group for video, any group for audio - or 'backlog' at the oldest group the relay still holds, which reads the whole cache before the first packet comes out"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// How long the relay gets to announce the broadcast, and then to hand
/// over its catalog, before the call gives up.
///
/// A compile-time probe wants a quick answer: a broadcast that is not
/// there is a query to fix, not something to wait on. A run wants the
/// opposite - it may well be started beside the publisher it reads, and
/// giving up on that is worse than waiting.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);

/// How many sessions a probe opens before a broadcast that was not
/// announced on any of them is refused. A fresh session's announce can
/// arrive late, or not at all, from a public relay that has the broadcast:
/// Cloudflare's refused three probes of one that a live reader joined
/// meanwhile, and a second try took. A broadcast that is not there is
/// still refused, within twice [`PROBE_TIMEOUT`].
const PROBE_DIALS: u32 = 2;

/// How long one wait for the wire lasts before the hold's own clocks
/// are looked at again. The hold releases on a deadline as well as on
/// an arrival, so a call that parked on the channel alone would sleep
/// through the deadline on a track that has gone quiet.
const TICK: Duration = Duration::from_millis(20);

/// How many times a track whose wire broke is taken up again, and how
/// long between tries. A relay serving a public network drops a stream
/// now and then; a live broadcast outlives that, and so should reading
/// it.
const TRACK_RETRIES: u32 = 5;
const TRACK_RETRY: Duration = Duration::from_millis(500);

/// How long a reader whose session ended waits between tries at a new
/// one: the first wait, and the longest the doubling reaches. A relay
/// that reset every session at once is taking them all back, and a try
/// a few seconds apart is what gets one in without hammering it.
const RECONNECT_FIRST: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// How long one pull keeps the session turning after it has something
/// to hand back, and how many idle turns end it early. The QUIC driver
/// runs only while a call is blocked on the executor, so a pull that
/// returned the instant anything arrived would leave the wire unread
/// between calls.
const DRIVE_SLICE: Duration = Duration::from_millis(1);
const DRIVE_TURNS: usize = 200;
const IDLE_TURNS: usize = 5;

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Params {
	relay: String,
	broadcast: String,
	#[serde(default)]
	cert: String,
	// The token rides the params JSON, which is visible on the sidecar
	// command line - scoped, expiring credentials only, the same caveat
	// the query text already carries.
	#[serde(default)]
	token: String,
	/// The hold, per track: see [`moq_core::order`]. A run whose relay
	/// keeps less history than this package asks for, or one that would
	/// rather stall for less time, says so here.
	/// Left out, it follows `start`; see [`hold_ms`].
	#[serde(default)]
	hold_ms: Option<u64>,
	#[serde(default = "default_hold_mib")]
	hold_mib: u64,
	#[serde(default = "default_join_ms")]
	join_ms: u64,
	/// Where this reader joins a broadcast already running: the live
	/// edge, which is the default, or the backlog; see
	/// [`moq_core::subscribe::Start`].
	#[serde(default = "default_start")]
	start: String,
	/// How long a dropped session is tried again for, in seconds; see
	/// [`Reader::reconnect`].
	#[serde(default = "default_reconnect_s")]
	reconnect_s: u64,
}

fn default_reconnect_s() -> u64 {
	60
}

/// How long a hole waits before the relay is asked for the group, when
/// the query did not say: a live join has given up the past on purpose,
/// so a group the relay never delivered is asked for after a second
/// rather than held for the whole backlog window with everything behind
/// it waiting too.
fn hold_ms(params: &Params, start: Start) -> u64 {
	params.hold_ms.unwrap_or(match start {
		Start::Live => moq_core::order::HOLD_WAIT_LIVE,
		Start::Backlog => moq_core::order::HOLD_WAIT,
	}
	.as_millis() as u64)
}

fn default_hold_mib() -> u64 {
	moq_core::order::HOLD_BYTES >> 20
}

fn default_join_ms() -> u64 {
	moq_core::order::JOIN_SETTLE.as_millis() as u64
}

fn default_start() -> String {
	"live".to_string()
}

fn check_params(params: &Params) -> Result<Start, String> {
	// The relay URL's path and the token argument are two spellings of
	// one field, so what the session will ask for is settled here, where
	// params are refused. `shape` reads these params at compile time, so
	// a URL and a token that disagree stop the query rather than the run.
	moq_core::relay::session_path(&params.relay, &params.token)?;
	// And so is a start nobody spells: a typo stops the query rather
	// than quietly reading the wrong end of the broadcast.
	Start::parse(&params.start)
}

/// One track as the catalog is read into it: its coded stream, its
/// relation row and what the catalog called it.
struct SourceTrack {
	coded: CodedStream,
	row: u32,
	rendition: RenditionMeta,
}

struct RenditionMeta {
	name: Option<String>,
	codecs: Option<String>,
}

/// Every track the catalog names, in its order.
struct Catalog {
	tracks: Vec<SourceTrack>,
}

/// One track's packets from one pull.
struct PadPackets {
	packets: Vec<Packet>,
}

/// The tokio floor the session runs on. Split from [`Reader`] so a call
/// can block on the executor while the work borrows the reader.
struct Executor {
	runtime: tokio::runtime::Runtime,
	// The session driver is not Send on wasm, so it lives on a LocalSet
	// and runs whenever a call blocks here.
	local: tokio::task::LocalSet,
}

impl Executor {
	fn new() -> Result<Self, String> {
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_time()
			.build()
			.map_err(|err| format!("runtime: {err}"))?;
		Ok(Executor {
			runtime,
			local: tokio::task::LocalSet::new(),
		})
	}

	fn enter<T>(&self, work: impl std::future::Future<Output = T>) -> T {
		self.local.block_on(&self.runtime, work)
	}
}

thread_local! {
	// One runtime for the instance's life: quinn-wasi binds its socket
	// reactor to the first runtime a guest runs, and `shape` and `init` both
	// dial, in that order, in one instance.
	static EXECUTOR: std::cell::OnceCell<Executor> = const { std::cell::OnceCell::new() };
}

/// `work` run on the instance's one runtime.
fn enter<T>(work: impl std::future::Future<Output = T>) -> Result<T, String> {
	EXECUTOR.with(|cell| {
		if cell.get().is_none() {
			let _ = cell.set(Executor::new()?);
		}
		Ok(cell.get().expect("just set").enter(work))
	})
}

/// What one reader task says about its track.
enum Wire {
	/// One complete group: its sequence and its frames, each frame a
	/// complete fmp4 fragment, or for a data track the one message.
	Group(usize, u64, Vec<Received>),
	/// The relay was asked for one group outright and would not serve
	/// it: it does not have it, so the hole waiting for it is a hole
	/// nothing will fill. Carries why.
	Gone(usize, u64, String),
	/// The track was taken up again on a fresh subscription, which
	/// starts at the live edge: whatever the hold is waiting for is not
	/// coming.
	Restarted(usize),
	/// A track's reader has finished: the publisher ended it. Which
	/// track it was does not matter - what the run waits for is all of
	/// them.
	Finished,
	/// A track could not be taken up again on this session, and why:
	/// the session itself is what has to be opened again.
	Lost(usize, String),
}

/// How a rendition's frames are taken apart.
enum Body {
	/// fmp4 fragments, read by the track the init segment describes.
	Media(ffrwd_bmff::track::Track),
	/// hang's `legacy` container: a varint pts in microseconds, then one
	/// sample of this codec. See [`moq_core::legacy`].
	Legacy(moq_core::legacy::Codec),
	/// One message per group: the frame is the message's pts in
	/// microseconds, then its bytes, handed on in `1/timescale` ticks.
	Data { timescale: u32 },
}

/// One rendition being read: how to take its fragments apart, where its
/// groups have got to, and whether its packets have started.
struct Rendition {
	body: Body,
	video: bool,
	/// The NAL length prefix this track's `avcC` declares; 0 for audio
	/// and data, whose frames carry no framing of their own, and for a
	/// legacy picture already in Annex B.
	length_size: usize,
	/// A legacy track's decode times, which its frames do not carry.
	order: moq_core::legacy::Order,
	/// Where this reader may start, and what a fragment carrying
	/// nothing for the track means.
	join: moq_core::group::Join,
	/// The group sequence this rendition's fragments arrive in.
	queue: Queue<Received>,
	/// Its catalog name, init segment and kind, which a broadcast opened
	/// again on a new session has to carry unchanged.
	name: String,
	init: Vec<u8>,
	kind: moq_core::catalog::Kind,
	/// Its codec string and decoder configuration: all a legacy
	/// rendition has in place of an init segment to be the same track by.
	codec: String,
	config: Vec<u8>,
	/// Set when the track was taken up on a new session and nothing has
	/// arrived on it yet: its first group says whether the publisher
	/// carried on or started again.
	resumed: bool,
}

impl Rendition {
	/// The track was taken up on a fresh subscription at the live edge:
	/// what its hold waits for is not coming, and a picture has to start
	/// again at a sync sample, since the frames it would have referred
	/// back to went with the groups in between.
	fn restart(&mut self) {
		self.queue.restarted = true;
		self.order.restart();
		if self.video {
			self.join = moq_core::group::Join::video();
		}
	}
}

/// Why taking the tracks up on a new session did not work: a broadcast
/// that is no longer the one this run describes, or something that may
/// well work on the next try.
enum Resume {
	Refused(String),
	Again(String),
}

/// Every track being read, and the session under them.
struct Reader {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
	/// The session's protocol task, which ends when the session does.
	driver: tokio::task::JoinHandle<Result<(), moq_net::Error>>,
	/// Why the session has to be opened again, when a track has said so.
	lost: Option<String>,
	/// What the session was opened with, to open it again.
	params: Params,
	renditions: Vec<Rendition>,
	frames: mpsc::UnboundedReceiver<Wire>,
	/// The broadcast, and a sender into the channel above: what a fetch
	/// for a missing group needs. Holding a sender is why the end of the
	/// run is counted rather than read off a closed channel.
	broadcast: moq_net::broadcast::Consumer,
	sender: mpsc::UnboundedSender<Wire>,
	/// Tracks whose reader has finished, and the groups a hole is
	/// waiting on an answer for.
	finished: usize,
	asking: Vec<(usize, u64)>,
	/// What every track's hold is bounded by.
	hold: Hold,
}

/// The reader the instance's runtime drives.
struct State {
	reader: Reader,
}

/// A dialed relay with the broadcast open: what both calls need
/// before they part ways.
struct Opened {
	endpoint: quinn::Endpoint,
	session: moq_net::Session,
	driver: tokio::task::JoinHandle<Result<(), moq_net::Error>>,
	broadcast: moq_net::broadcast::Consumer,
	renditions: Vec<moq_core::catalog::Rendition>,
}

/// Connects, waits for the broadcast to be announced and reads its
/// catalog. The session driver is spawned here and runs whenever a
/// call blocks on the executor. A broadcast not announced within `wait`
/// is asked for again on a new session, up to `dials` sessions in all.
async fn open_broadcast(params: &Params, wait: Duration, dials: u32) -> Result<Opened, String> {
	let path = params.broadcast.as_str();
	let mut dialed = 0;
	let (endpoint, session, driver, broadcast) = loop {
		dialed += 1;
		let origin = moq_net::Origin::random().produce();
		let moq_core::wasi::Connected {
			endpoint,
			session,
			driver,
		} = moq_core::relay::dial(
			&params.relay,
			&params.cert,
			&params.token,
			moq_net::Client::new().with_subscriber(origin.clone()),
		)
		.await?;
		let driver = tokio::task::spawn_local(driver);
		match tokio::time::timeout(wait, origin.consume().announced_broadcast(path)).await {
			Ok(Some(broadcast)) => break (endpoint, session, driver, broadcast),
			Ok(None) => return Err(format!("broadcast '{path}' is not on the relay")),
			Err(_) if dialed < dials => {
				eprintln!(
					"subscribe: broadcast '{path}' was not announced within {}s; asking on a \
					 new session",
					wait.as_secs()
				);
				driver.abort();
				drop(session);
				endpoint.wait_idle().await;
			}
			Err(_) => {
				return Err(format!(
					"broadcast '{path}' was not announced within {}s{}",
					wait.as_secs(),
					if dials > 1 {
						format!(", on each of {dials} sessions")
					} else {
						String::new()
					}
				))
			}
		}
	};

	let document = tokio::time::timeout(wait, moq_core::subscribe::read_catalog(&broadcast))
		.await
		.map_err(|_| {
			format!(
				"broadcast '{path}' sent no catalog within {}s",
				wait.as_secs()
			)
		})?
		.map_err(|err| format!("broadcast '{path}': {err}"))?;

	Ok(Opened {
		endpoint,
		session,
		driver,
		broadcast,
		renditions: moq_core::catalog::parse(&document)
			.map_err(|err| format!("broadcast '{path}': {err}"))?,
	})
}

/// One catalog data rendition as the track a source publishes: JSON
/// messages in the timescale the catalog names, and nothing else to say.
fn data_track(
	rendition: &moq_core::catalog::Rendition,
	timescale: u32,
) -> Result<SourceTrack, String> {
	if rendition.codec != "json" {
		return Err(format!(
			"rendition '{}' carries '{}' data, and this reads json, one JSON object a message",
			rendition.name, rendition.codec
		));
	}
	let time_base = Rational {
		num: 1,
		den: timescale as i32,
	};
	Ok(SourceTrack {
		coded: CodedStream {
			codec: rendition.codec.clone(),
			time_base,
			format: CodedFormat::Data,
			extradata: Vec::new(),
			profile: None,
			level: None,
		},
		row: rendition.row,
		rendition: RenditionMeta {
			name: Some(rendition.name.clone()),
			codecs: Some(rendition.codec.clone()),
		},
	})
}

/// One catalog rendition as the track a source publishes: its coded
/// stream read off the init segment, its geometry off the catalog. A
/// data rendition has no init segment, and is [`data_track`]'s.
fn source_track(
	rendition: &moq_core::catalog::Rendition,
) -> Result<(SourceTrack, Body), String> {
	if let moq_core::catalog::Kind::Data { timescale } = rendition.kind {
		return Ok((data_track(rendition, timescale)?, Body::Data { timescale }));
	}
	if rendition.packaging == moq_core::catalog::Packaging::Legacy {
		return legacy_track(rendition);
	}
	let track = ffrwd_bmff::track::Track::from_init(&rendition.init)
		.map_err(|err| format!("rendition '{}': init segment: {err}", rendition.name))?;
	let time_base = Rational {
		num: 1,
		den: track.timescale as i32,
	};
	let config = &track.entry.config;
	let (codec, format, extradata, profile, level) = match (&rendition.kind, &track.entry.kind)
	{
		(moq_core::catalog::Kind::Video { width, height }, b"avc1" | b"avc3") => {
			let extradata = ffrwd_nal::config::avcc_to_annexb_extradata(config)
				.map_err(|err| format!("rendition '{}': its avcC: {err}", rendition.name))?;
			let (profile, level) = match ffrwd_nal::sps::profile_level(config) {
				Some((profile, level)) => (Some(i32::from(profile)), Some(i32::from(level))),
				None => (None, None),
			};
			(
				"h264",
				CodedFormat::Video(CodedVideo {
					width: *width,
					height: *height,
					sample_aspect_ratio: None,
					color: None,
				}),
				extradata,
				profile,
				level,
			)
		}
		(
			moq_core::catalog::Kind::Audio {
				sample_rate,
				channels,
			},
			b"mp4a",
		) => (
			"aac",
			CodedFormat::Audio(CodedAudio {
				sample_rate: *sample_rate,
				channels: *channels,
				channel_layout: None,
			}),
			// A decoder takes the AudioSpecificConfig, and an `mp4a`
			// entry's record is the whole `esds` descriptor chain with
			// that config buried in it. The catalog spells the config
			// out, as it spells the rate and the channel count this
			// rendition is also read from.
			rendition.config.clone(),
			None,
			None,
		),
		(_, other) => {
			return Err(format!(
				"rendition '{}' is packaged as '{}', and this reads avc1 video and mp4a audio \
				 described as what they are",
				rendition.name,
				String::from_utf8_lossy(other)
			))
		}
	};
	Ok((
		SourceTrack {
			coded: CodedStream {
				codec: codec.to_string(),
				time_base,
				format,
				extradata,
				profile,
				level,
			},
			row: rendition.row,
			rendition: RenditionMeta {
				name: Some(rendition.name.clone()),
				codecs: Some(rendition.codec.clone()),
			},
		},
		Body::Media(track),
	))
}

/// One `legacy` rendition as the track a source publishes: what libmoq's
/// OBS output sends. There is no init segment, so everything comes off the
/// catalog entry: the codec off its string, the geometry off its fields,
/// the decoder configuration off its description where it has one. Its
/// pts are microseconds, so that is the time base.
///
/// A description is what says how a picture is framed, as it does for
/// WebCodecs: an `avc1` or `avc3` entry with an `avcC` carries its NAL
/// units length-prefixed, and is reframed into Annex B on the way through;
/// one without carries Annex B with its parameter sets in the keyframes.
/// An HEVC entry with an `hvcC` would be length-prefixed as well, and is
/// refused until something publishes one.
fn legacy_track(rendition: &moq_core::catalog::Rendition) -> Result<(SourceTrack, Body), String> {
	use moq_core::catalog::Kind;
	use moq_core::legacy::Codec;
	let name = &rendition.name;
	let codec = Codec::of(&rendition.codec).map_err(|err| format!("rendition '{name}': {err}"))?;
	let time_base = Rational {
		num: 1,
		den: 1_000_000,
	};
	let (format, extradata, profile, level) = match (rendition.kind, codec) {
		(Kind::Video { width, height }, Codec::H264 | Codec::Hevc | Codec::Av1) => {
			let extradata = match (codec, rendition.config.is_empty()) {
				(_, true) => Vec::new(),
				(Codec::H264, false) => ffrwd_nal::config::avcc_to_annexb_extradata(&rendition.config)
					.map_err(|err| format!("rendition '{name}': its avcC: {err}"))?,
				(Codec::Hevc, false) => {
					return Err(format!(
						"rendition '{name}' describes its HEVC with an hvcC, which frames its \
						 pictures length-prefixed, and this reads legacy HEVC in Annex B \
						 ('hev1' without a description)"
					))
				}
				// An av1C: the OBUs are the same with or without it.
				_ => rendition.config.clone(),
			};
			let (profile, level) = match codec {
				Codec::H264 => match moq_core::legacy::avc_profile_level(&rendition.codec) {
					Some((profile, level)) => (Some(profile), Some(level)),
					None => (None, None),
				},
				_ => (None, None),
			};
			(
				CodedFormat::Video(CodedVideo {
					width,
					height,
					sample_aspect_ratio: None,
					color: None,
				}),
				extradata,
				profile,
				level,
			)
		}
		(
			Kind::Audio {
				sample_rate,
				channels,
			},
			Codec::Aac,
		) => {
			if rendition.config.is_empty() {
				return Err(format!(
					"rendition '{name}' is raw AAC with no description, and a decoder needs \
					 the AudioSpecificConfig the description carries"
				));
			}
			(
				CodedFormat::Audio(CodedAudio {
					sample_rate,
					channels,
					channel_layout: None,
				}),
				rendition.config.clone(),
				None,
				None,
			)
		}
		(_, codec) => {
			return Err(format!(
				"rendition '{name}' is {} but filed in the catalog's other section",
				codec.name()
			))
		}
	};
	Ok((
		SourceTrack {
			coded: CodedStream {
				codec: codec.name().to_string(),
				time_base,
				format,
				extradata,
				profile,
				level,
			},
			row: rendition.row,
			rendition: RenditionMeta {
				name: Some(rendition.name.clone()),
				codecs: Some(rendition.codec.clone()),
			},
		},
		Body::Legacy(codec),
	))
}

/// The renditions `order` names as a catalog the host reads, and the demuxers
/// under it, in that order. `probe` passes every index and `open` the ones it
/// was told to pull; a track's `index` and `row` stay the catalog's own, so
/// the same rendition is the same track in both.
fn catalog_of(
	renditions: &[moq_core::catalog::Rendition],
	order: &[usize],
	start: Start,
) -> Result<(Catalog, Vec<Rendition>), String> {
	let mut tracks = Vec::with_capacity(order.len());
	let mut readers = Vec::with_capacity(order.len());
	for &index in order {
		let rendition = &renditions[index];
		let (track, body) = source_track(rendition)?;
		let video = matches!(rendition.kind, moq_core::catalog::Kind::Video { .. });
		let length_size = match (&body, video) {
			(Body::Media(demux), true) => ffrwd_nal::config::avcc_length_size(&demux.entry.config),
			// A legacy H.264 entry that describes itself is length-prefixed.
			(Body::Legacy(moq_core::legacy::Codec::H264), true) if !rendition.config.is_empty() => {
				ffrwd_nal::config::avcc_length_size(&rendition.config)
			}
			_ => 0,
		};
		tracks.push(track);
		readers.push(Rendition {
			body,
			video,
			length_size,
			join: match video {
				true => moq_core::group::Join::video(),
				false => moq_core::group::Join::audio(),
			},
			queue: Queue::new(rendition.name.clone(), start),
			name: rendition.name.clone(),
			init: rendition.init.to_vec(),
			kind: rendition.kind,
			codec: rendition.codec.clone(),
			config: rendition.config.clone(),
			order: moq_core::legacy::Order::default(),
			resumed: false,
		});
	}
	Ok((Catalog { tracks }, readers))
}

impl Reader {
	/// One pull: the packets that came in, a list per track in catalog
	/// order, or none once every track has finished and nothing is held.
	///
	/// A call blocks until something is ready and then keeps the session
	/// turning for a while: the reader tasks and the QUIC driver run
	/// only while a call is on the executor, so returning at the first
	/// sign of a frame would leave the wire unread between calls.
	async fn pull(&mut self) -> Result<Option<Vec<PadPackets>>, String> {
		let mut pads: Vec<Vec<Packet>> = (0..self.renditions.len()).map(|_| Vec::new()).collect();
		let mut produced = false;
		loop {
			while let Ok(wire) = self.frames.try_recv() {
				self.hold(wire)?;
			}
			// The session ended under the tracks, or one of them could not
			// be taken up again on it: open a new one before anything else.
			if self.driver.is_finished() || self.lost.is_some() {
				let why = match self.lost.take() {
					Some(why) => why,
					None => match (&mut self.driver).await {
						Ok(Ok(())) => "the relay closed the session".to_string(),
						Ok(Err(err)) => err.to_string(),
						Err(err) => err.to_string(),
					},
				};
				self.reconnect(why).await?;
				continue;
			}
			produced |= self.release(&mut pads, self.over())?;
			self.ask();
			self.report();
			if produced {
				break;
			}
			if self.over() {
				// Every track has finished and no answer is outstanding:
				// whatever was held has just gone out, and this is the
				// end of the run.
				for rendition in &mut self.renditions {
					rendition.queue.report(true);
				}
				return Ok(produced.then(|| into_pads(pads)));
			}
			// Nothing ready: wait for the wire, but only for a tick. The
			// hold releases on a deadline as well as on an arrival, and a
			// track that has gone quiet would otherwise sleep through it.
			match tokio::time::timeout(TICK, self.frames.recv()).await {
				Ok(Some(wire)) => self.hold(wire)?,
				// Nothing holds a sender but this reader, so the channel
				// closes only if it has been dropped out from under us.
				Ok(None) => return Ok(None),
				Err(_) => {}
			}
		}

		let mut idle = 0;
		for _ in 0..DRIVE_TURNS {
			tokio::time::sleep(DRIVE_SLICE).await;
			let mut arrived = false;
			while let Ok(wire) = self.frames.try_recv() {
				arrived = true;
				self.hold(wire)?;
			}
			self.release(&mut pads, false)?;
			self.ask();
			idle = if arrived { 0 } else { idle + 1 };
			if idle >= IDLE_TURNS {
				break;
			}
		}
		Ok(Some(into_pads(pads)))
	}

	/// Whether the run is over: every track's reader has finished and
	/// no track is still waiting for an answer about a missing group.
	fn over(&self) -> bool {
		self.finished >= self.renditions.len()
			&& self
				.renditions
				.iter()
				.all(|rendition| rendition.queue.fetching.is_none())
	}

	/// Asks the relay for the groups the holds have given up waiting
	/// for, one task apiece. They answer into the same channel the
	/// subscriptions do.
	fn ask(&mut self) {
		for (index, sequence) in self.asking.drain(..) {
			let name = self.renditions[index].queue.name.clone();
			tokio::task::spawn_local(fetch_group(
				index,
				self.broadcast.clone(),
				name,
				sequence,
				self.hold.wait(),
				self.sender.clone(),
			));
		}
	}

	/// What each track has done, every [`REPORT_EVERY`].
	fn report(&mut self) {
		for rendition in &mut self.renditions {
			if rendition
				.queue
				.reported
				.is_none_or(|sent| sent.elapsed() >= moq_core::order::REPORT_EVERY)
			{
				rendition.queue.report(false);
			}
		}
	}

	/// Files one completed group under its track, or fails the pull when
	/// a track did. What each kind of word means is [`Queue`]'s business.
	fn hold(&mut self, wire: Wire) -> Result<(), String> {
		match wire {
			Wire::Group(index, sequence, frames) => {
				let rendition = &mut self.renditions[index];
				// The first group on a new session. A publisher that carried
				// on counts on from where it was; one that started again
				// counts from nothing, and its clock went back with it,
				// which no reader downstream of this one can follow.
				if std::mem::take(&mut rendition.resumed) {
					if let Some(cursor) = rendition.queue.cursor() {
						if sequence + 1 < cursor {
							return Err(format!(
								"track '{}' came back on a new session at group {sequence}, \
								 below the {cursor} this reader had reached: its publisher \
								 started again, and its timestamps with it",
								rendition.name
							));
						}
					}
				}
				// Whether a decoder could start at this group, which is
				// what a live join lands on. The keyframe flag comes off
				// the fragment the publisher wrote rather than off a
				// convention about where groups are cut.
				let decodable = decodable(rendition, &frames);
				rendition.queue.push(sequence, frames, decodable)
			}
			Wire::Gone(index, sequence, why) => {
				self.renditions[index].queue.refused(sequence, &why)
			}
			Wire::Restarted(index) => self.renditions[index].restart(),
			Wire::Finished => self.finished += 1,
			Wire::Lost(index, err) => {
				let name = &self.renditions[index].queue.name;
				self.lost.get_or_insert(format!("track '{name}': {err}"));
			}
		}
		Ok(())
	}

	/// Opens a new session and takes every track up again at the live
	/// edge, trying for as long as `reconnect_s` allows.
	///
	/// A relay resets sessions now and then - every one at once, the
	/// same instant for two readers on two machines - and a live
	/// broadcast outlives that. So the reader dials again, waits for the
	/// broadcast to be announced, and reads its catalog: the tracks it
	/// pulls have to be there under the same names with the same init
	/// segments, or what comes next is not the stream this run was
	/// describing, and that stops it. Everything that fails before then
	/// is tried again, a doubling wait apart, until the window is spent.
	async fn reconnect(&mut self, why: String) -> Result<(), String> {
		let window = Duration::from_secs(self.params.reconnect_s);
		if window.is_zero() {
			return Err(why);
		}
		eprintln!("subscribe: the session ended: {why}; connecting again");
		let began = std::time::Instant::now();
		let mut wait = RECONNECT_FIRST;
		let mut attempts = 0u64;
		loop {
			attempts += 1;
			let left = window.saturating_sub(began.elapsed());
			let failure = match open_broadcast(&self.params, left.max(RECONNECT_FIRST), 1).await {
				Ok(opened) => match self.resume(opened).await {
					Ok(()) => {
						moq_core::order::report_reconnect(
							attempts,
							began.elapsed().as_millis() as u64,
							&why,
						);
						return Ok(());
					}
					Err(Resume::Refused(err)) => return Err(err),
					Err(Resume::Again(err)) => err,
				},
				Err(err) => err,
			};
			let left = window.saturating_sub(began.elapsed());
			if left.is_zero() {
				return Err(format!(
					"{why}; no new session took the tracks up within {}s ({attempts} tries, \
					 the last: {failure})",
					window.as_secs()
				));
			}
			eprintln!("subscribe: connecting again: {failure}; trying again");
			tokio::time::sleep(wait.min(left)).await;
			wait = (wait * 2).min(RECONNECT_MAX);
		}
	}

	/// Takes every track up on a freshly opened broadcast. The old
	/// session's readers answer into a channel nobody reads any more, so
	/// nothing of it reaches the holds; each hold is told its track
	/// restarted, which is the hole between the two sessions.
	async fn resume(&mut self, opened: Opened) -> Result<(), Resume> {
		for rendition in &self.renditions {
			let Some(now) = opened
				.renditions
				.iter()
				.find(|candidate| candidate.name == rendition.name)
			else {
				return Err(Resume::Refused(format!(
					"the broadcast came back on a new session without track '{}'",
					rendition.name
				)));
			};
			if now.init[..] != rendition.init[..] {
				return Err(Resume::Refused(format!(
					"the broadcast came back on a new session with another init segment for \
					 track '{}': its publisher started again with other settings",
					rendition.name
				)));
			}
			// Nor has a legacy one: it is the same track while its codec,
			// its decoder configuration and its geometry are.
			if matches!(rendition.body, Body::Legacy(_))
				&& (now.codec != rendition.codec
					|| now.config != rendition.config
					|| now.kind != rendition.kind)
			{
				return Err(Resume::Refused(format!(
					"the broadcast came back on a new session with track '{}' in another codec \
					 or at another size: its publisher started again with other settings",
					rendition.name
				)));
			}
			// A data track has no init segment; what it has to keep is
			// what it is and the timescale its pts count in.
			let data = matches!(rendition.kind, moq_core::catalog::Kind::Data { .. });
			if data && (now.kind != rendition.kind || now.codec != "json") {
				return Err(Resume::Refused(format!(
					"the broadcast came back on a new session with track '{}' carrying another \
					 kind of data or counting it in another timescale",
					rendition.name
				)));
			}
		}
		let (sender, frames) = mpsc::unbounded_channel::<Wire>();
		let mut opening = Vec::with_capacity(self.renditions.len());
		for rendition in &self.renditions {
			let subscribing = subscribe_track(&opened.broadcast, &rendition.name, Start::Live)
				.map_err(Resume::Again)?;
			opening.push((rendition.name.clone(), subscribing));
		}
		let mut subscribers = Vec::with_capacity(opening.len());
		for (name, subscribing) in opening {
			let subscriber = subscribing
				.await
				.map_err(|err| Resume::Again(format!("track '{name}': {err}")))?;
			subscribers.push((name, subscriber));
		}
		for (pad, (name, subscriber)) in subscribers.into_iter().enumerate() {
			tokio::task::spawn_local(read_track(
				pad,
				opened.broadcast.clone(),
				name,
				subscriber,
				sender.clone(),
			));
		}
		for rendition in &mut self.renditions {
			rendition.restart();
			rendition.queue.fetching = None;
			rendition.resumed = true;
		}
		self.driver.abort();
		self.endpoint = opened.endpoint;
		self.session = Some(opened.session);
		self.driver = opened.driver;
		self.broadcast = opened.broadcast;
		self.sender = sender;
		self.frames = frames;
		self.finished = 0;
		self.asking.clear();
		Ok(())
	}

	/// Every held group whose turn has come, onto its track's pad, in
	/// sequence. `last` releases what is left whatever the sequence says:
	/// nothing more is coming. True when packets went out.
	fn release(&mut self, pads: &mut [Vec<Packet>], last: bool) -> Result<bool, String> {
		let mut any = false;
		let hold = self.hold;
		let asking = &mut self.asking;
		let mut ask = Vec::new();
		for (index, (rendition, pad)) in self.renditions.iter_mut().zip(pads.iter_mut()).enumerate()
		{
			while let Some((_, frames)) = rendition.queue.take(hold, last, &mut ask) {
				for frame in frames {
					any |= match rendition.body {
						Body::Data { timescale } => take_message(timescale, index, frame, pad)?,
						Body::Media(_) => take_fragment(rendition, index, &frame.payload, pad)?,
						Body::Legacy(codec) => take_legacy(rendition, codec, index, frame, pad)?,
					};
				}
			}
			asking.extend(ask.drain(..).map(|sequence| (index, sequence)));
		}
		Ok(any)
	}
}

/// Whether a decoder can start at this group, which is what a live
/// join has to land on.
///
/// A MoQ group opens at a keyframe by this package's own convention and
/// by hang's, and a live join is exactly where trusting that would buy
/// a broken picture - so it is read off the packets instead: the first
/// sample the group carries for this track has to be a sync sample.
/// Audio has no such gate, every AAC frame being one, so a group with
/// any sample in it will do. A fragment carrying nothing for this track,
/// a `moof` whose track fragments are all somebody else's, says nothing
/// either way, and the next one is read. A message stands alone, so a
/// data group is always one to start at.
fn decodable(rendition: &Rendition, frames: &[Received]) -> bool {
	let track = match &rendition.body {
		Body::Media(track) => track,
		Body::Data { .. } => return !frames.is_empty(),
		Body::Legacy(codec) => {
			// The first frame with a sample in it decides, as the first
			// sample of a fragment does.
			return frames.iter().find_map(|frame| {
				match legacy_sample(rendition, *codec, &frame.payload) {
					Ok(Some((_, _, keyframe))) => Some(!rendition.video || keyframe),
					Ok(None) => None,
					Err(_) => Some(false),
				}
			}) == Some(true);
		}
	};
	for fragment in frames {
		let Ok(samples) = track.fragment_samples(&fragment.payload) else {
			return false;
		};
		if let Some(sample) = samples.first() {
			return !rendition.video || sample.keyframe;
		}
	}
	false
}

fn into_pads(pads: Vec<Vec<Packet>>) -> Vec<PadPackets> {
	pads.into_iter()
		.map(|packets| PadPackets { packets })
		.collect()
}

/// One fmp4 fragment's samples onto a pad. True when any went out; a
/// video track's samples before its first sync sample do not, since no
/// decoder can start in the middle of a picture.
///
/// [`moq_core::order::Queue`] has already chosen a group a decoder can
/// start at on a live join, and a backlog join starts at the oldest
/// group there is, so this gate is what catches whatever neither
/// covers: a resubscribe after the wire broke, and a publisher whose
/// groups do not open at keyframes.
///
/// A fragment that carries nothing for this track - a `moof` whose
/// track fragments are all somebody else's - holds no samples and is
/// simply skipped. The reader's cursor is the group sequence, which
/// such a fragment does not disturb.
fn take_fragment(
	rendition: &mut Rendition,
	index: usize,
	fragment: &[u8],
	pad: &mut Vec<Packet>,
) -> Result<bool, String> {
	let Body::Media(track) = &rendition.body else {
		unreachable!("a data track's frames are messages");
	};
	let samples = rendition
		.join
		.playable(track, fragment)
		.map_err(|err| format!("track {index}: {err}"))?;
	let mut any = false;
	for sample in samples {
		// The bytes are borrowed out of the fragment; an h264 sample is
		// reframed from the length prefixes the avcC declares into the
		// Annex-B an encoded edge expects, and an AAC frame is copied.
		let stored = ffrwd_bmff::fragment::sample_bytes(fragment, 0, &sample)
			.map_err(|err| format!("track {index}: {err}"))?;
		let data = if rendition.video {
			ffrwd_nal::annexb::length_prefixed_to_annexb(stored, rendition.length_size)
				.map_err(|err| format!("track {index}: {err}"))?
		} else {
			stored.to_vec()
		};
		pad.push(Packet {
			pts: sample.pts,
			dts: Some(sample.dts),
			duration: Some(sample.duration).filter(|ticks| *ticks > 0),
			keyframe: sample.keyframe,
			data,
		});
		any = true;
	}
	Ok(any)
}

/// A legacy frame taken apart: its pts in microseconds, the sample as an
/// encoded edge wants it (Annex B for a picture, reframed if the entry's
/// avcC said it was length-prefixed), and whether a decoder can start at
/// it. `None` for a frame that is a pts and nothing else, which libmoq
/// sends now and then on a video track and which has nothing to hand on.
fn legacy_sample(
	rendition: &Rendition,
	codec: moq_core::legacy::Codec,
	frame: &[u8],
) -> Result<Option<(u64, Vec<u8>, bool)>, String> {
	let (pts_us, sample) = moq_core::message::decode(frame)?;
	if sample.is_empty() {
		return Ok(None);
	}
	let data = match rendition.length_size {
		0 => sample.to_vec(),
		size => ffrwd_nal::annexb::length_prefixed_to_annexb(sample, size)
			.map_err(|err| err.to_string())?,
	};
	let keyframe = moq_core::legacy::keyframe(codec, &data);
	Ok(Some((pts_us, data, keyframe)))
}

/// One legacy frame onto a pad, at the pts the frame carries (never the
/// frame's own timestamp, which over an IETF draft is when it arrived).
/// A picture waits for its track's first keyframe, as a fragment's
/// samples do in [`take_fragment`]; its dts is its pts, and a picture
/// that goes back in time is refused by [`moq_core::legacy::Order`]. An
/// AAC frame lasts 1024 samples.
fn take_legacy(
	rendition: &mut Rendition,
	codec: moq_core::legacy::Codec,
	index: usize,
	frame: Received,
	pad: &mut Vec<Packet>,
) -> Result<bool, String> {
	let Some((pts_us, data, keyframe)) = legacy_sample(rendition, codec, &frame.payload)
		.map_err(|err| format!("track {index}, group {}: {err}", frame.group))?
	else {
		return Ok(false);
	};
	if rendition.video && !rendition.join.admit(keyframe) {
		return Ok(false);
	}
	let pts = i64::try_from(pts_us).map_err(|_| format!("track {index}: a pts of {pts_us}us"))?;
	let dts = rendition.order.dts(&rendition.name, pts_us)? as i64;
	let duration = match rendition.kind {
		moq_core::catalog::Kind::Audio { sample_rate, .. } if sample_rate > 0 => {
			Some(1024 * 1_000_000 / i64::from(sample_rate))
		}
		_ => None,
	};
	pad.push(Packet {
		pts,
		dts: Some(dts),
		duration,
		keyframe,
		data,
	});
	Ok(true)
}

/// One message onto a pad: the bytes after the frame's pts as they
/// arrived, and that pts in the track's own ticks. The pts is read out of
/// the frame and never off the frame's timestamp, which over an IETF draft
/// is the time the frame ARRIVED; see [`moq_core::message`]. Every message
/// is a keyframe and is decoded when it is presented, so dts is the pts.
fn take_message(
	timescale: u32,
	index: usize,
	frame: Received,
	pad: &mut Vec<Packet>,
) -> Result<bool, String> {
	let (pts_us, message) = moq_core::message::decode(&frame.payload)
		.map_err(|err| format!("track {index}, group {}: {err}", frame.group))?;
	let pts = ffrwd_bmff::time::rescale(
		i64::try_from(pts_us).unwrap_or(i64::MAX),
		1_000_000,
		u64::from(timescale),
	);
	pad.push(Packet {
		pts,
		dts: Some(pts),
		duration: None,
		keyframe: true,
		data: message.to_vec(),
	});
	Ok(true)
}

/// `subscribe` as the host runs it: the reader, and the output each of
/// its pads leaves on, with whether it carries messages.
pub(crate) struct Subscribe {
	state: Option<State>,
	ports: Vec<(String, bool)>,
}

/// The catalog's tracks as port names: each its rendition's name as far
/// as a port name allows, and unique.
fn ports(catalog: &Catalog) -> Vec<String> {
	let mut names: Vec<String> = Vec::with_capacity(catalog.tracks.len());
	for (index, track) in catalog.tracks.iter().enumerate() {
		let written = track.rendition.name.clone().unwrap_or_default();
		let mut name: String = written
			.chars()
			.map(|c| {
				if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
					c
				} else {
					'_'
				}
			})
			.collect();
		if name.is_empty() || names.contains(&name) {
			name = format!("{name}-{index}");
		}
		names.push(name);
	}
	names
}

/// The catalog's relation: a row per rendition row, with the name and
/// codecs of the media on it, and each track's index into it.
fn relation(catalog: &Catalog) -> (Vec<String>, Vec<u32>) {
	let mut rows: Vec<u32> = catalog.tracks.iter().map(|track| track.row).collect();
	rows.sort_unstable();
	rows.dedup();
	let relation = rows
		.iter()
		.map(|row| {
			let on: Vec<&SourceTrack> = catalog.tracks.iter().filter(|t| t.row == *row).collect();
			let media: Vec<&&SourceTrack> = on
				.iter()
				.filter(|t| !matches!(t.coded.format, CodedFormat::Data))
				.collect();
			let named = media.first().copied().or(on.first());
			let codecs: Vec<&str> = media
				.iter()
				.filter_map(|t| t.rendition.codecs.as_deref())
				.collect();
			serde_json::json!({
				"name": named.and_then(|t| t.rendition.name.clone()),
				"codecs": (!codecs.is_empty()).then(|| codecs.join(",")),
			})
			.to_string()
		})
		.collect();
	let index = catalog
		.tracks
		.iter()
		.map(|track| rows.iter().position(|row| *row == track.row).unwrap_or(0) as u32)
		.collect();
	(relation, index)
}

impl Node for Subscribe {
	const NAME: &'static str = "subscribe";
	const VERSION: &'static str = env!("CARGO_PKG_VERSION");
	const PARAMS_SCHEMA: &'static str = PARAMS_SCHEMA;
	const ROWS_SCHEMA: &'static str = moq_core::order::ROWS_SCHEMA;
	type Params = Params;

	/// Connects, reads the catalog and says what it holds without pulling
	/// any media: a packets output per media track, a data output per data
	/// track, and the relation rows they belong to. Nothing bounds a
	/// broadcast, so neither does the shape.
	fn shape(params: &Params, _: &Bound) -> ffrwd_node::Result<Shape> {
		let start = check_params(params)?;
		let catalog = enter(async {
			let opened = open_broadcast(params, PROBE_TIMEOUT, PROBE_DIALS).await?;
			let every = (0..opened.renditions.len()).collect::<Vec<_>>();
			let (catalog, _) = catalog_of(&opened.renditions, &every, start)?;
			drop(opened.broadcast);
			drop(opened.session);
			opened.endpoint.wait_idle().await;
			Ok::<_, String>(catalog)
		})??;
		let names = ports(&catalog);
		let (relation, rows) = relation(&catalog);
		let mut shape = Shape::new().self_clocked().bounded(false);
		for row in &relation {
			shape = shape.relation_row(row);
		}
		for ((track, name), row) in catalog.tracks.iter().zip(&names).zip(rows) {
			let output = match track.coded.format {
				CodedFormat::Data => Output::rows(name),
				_ => Output::packets(name).coded(track.coded.clone()),
			};
			shape = shape.output(output.time_base(track.coded.time_base).row(row));
		}
		Ok(shape)
	}

	/// Subscribes to the tracks the query latched onto, at the edge
	/// `start` names, and to nothing else.
	fn init(params: Params, init: &Init) -> ffrwd_node::Result<Subscribe> {
		let start = check_params(&params)?;
		let (ports, reader) = enter(async {
			let opened = open_broadcast(&params, OPEN_TIMEOUT, 1).await?;
			let every = (0..opened.renditions.len()).collect::<Vec<_>>();
			let (whole, _) = catalog_of(&opened.renditions, &every, start)?;
			let names = ports(&whole);
			let order: Vec<usize> = (0..names.len())
				.filter(|&i| init.latched(&names[i]))
				.collect();
			// One line per run naming what this source pulls, so a run can
			// be read back against the catalog it narrowed.
			eprintln!(
				"subscribe: pulling {} of {} rendition(s) at the {} edge: {}",
				order.len(),
				opened.renditions.len(),
				match start {
					Start::Live => "live",
					Start::Backlog => "oldest cached",
				},
				order
					.iter()
					.map(|&index| format!("{index}={}", opened.renditions[index].name))
					.collect::<Vec<_>>()
					.join(", ")
			);
			let (_, renditions) = catalog_of(&opened.renditions, &order, start)?;
			// Every track is REGISTERED before any is awaited, which is what
			// keeps a ladder's rungs together: a live subscription starts at
			// whatever group was the publisher's latest when it was
			// accepted, so subscribing a track and then waiting out a round
			// trip before subscribing the next would join each rung a round
			// trip further on.
			let (sender, frames) = mpsc::unbounded_channel::<Wire>();
			let mut opening = Vec::with_capacity(order.len());
			for &index in &order {
				let name = opened.renditions[index].name.clone();
				let subscribing = subscribe_track(&opened.broadcast, &name, start)?;
				opening.push((name, subscribing));
			}
			for (pad, (name, subscribing)) in opening.into_iter().enumerate() {
				let subscriber = subscribing
					.await
					.map_err(|err| format!("track '{name}': {err}"))?;
				tokio::task::spawn_local(read_track(
					pad,
					opened.broadcast.clone(),
					name,
					subscriber,
					sender.clone(),
				));
			}
			let ports: Vec<(String, bool)> = order
				.iter()
				.map(|&index| {
					let data = matches!(whole.tracks[index].coded.format, CodedFormat::Data);
					(names[index].clone(), data)
				})
				.collect();
			Ok::<_, String>((
				ports,
				Reader {
					endpoint: opened.endpoint,
					session: Some(opened.session),
					driver: opened.driver,
					lost: None,
					params: params.clone(),
					renditions,
					frames,
					broadcast: opened.broadcast,
					sender,
					finished: 0,
					asking: Vec::new(),
					hold: Hold::new(hold_ms(&params, start), params.hold_mib, params.join_ms),
				},
			))
		})??;
		Ok(Subscribe {
			state: Some(State { reader }),
			ports,
		})
	}

	/// Blocks until something is ready, then hands every track's packets
	/// on its port and the reader's rows on the run's. Once every track
	/// has finished it says so, and the session closes.
	fn process(&mut self, tick: &Tick, out: &mut Out) -> ffrwd_node::Result<()> {
		let Some(State { reader }) = self.state.as_mut() else {
			return Ok(());
		};
		let pulled = if tick.last() {
			None
		} else {
			enter(reader.pull())??
		};
		for row in moq_core::order::take_rows() {
			out.report(&RawValue::from_string(row)?)?;
		}
		let Some(pads) = pulled else {
			let mut done = self.state.take().expect("the reader is here");
			enter(async {
				drop(done.reader.session.take());
				done.reader.endpoint.wait_idle().await;
			})?;
			out.finish();
			return Ok(());
		};
		for ((port, data), pad) in self.ports.iter().zip(pads) {
			for packet in pad.packets {
				if *data {
					// A message's pts never goes back on its port, as a
					// publisher writing one track in order never sends.
					let pts = out
						.last(port)
						.map_or(packet.pts, |last| packet.pts.max(last));
					out.message(port, pts, packet.data)?;
				} else {
					out.packet(port, packet)?;
				}
			}
		}
		Ok(())
	}
}

/// Registers a subscription to one named track of a broadcast, at the
/// live edge or over the whole backlog the publisher still holds.
///
/// This does NOT wait: moq-net registers the subscription here and the
/// value handed back is what resolves once the track's info arrives.
/// Every track of a run is registered before any of them is awaited,
/// so they go out together and each one joins the same edge.
fn subscribe_track(
	broadcast: &moq_net::broadcast::Consumer,
	name: &str,
	start: Start,
) -> Result<impl std::future::Future<Output = Result<moq_net::track::Subscriber, moq_net::Error>>, String>
{
	Ok(broadcast
		.track(name)
		.map_err(|err| format!("track '{name}': {err}"))?
		.subscribe(start.subscription()))
}

/// One group asked for by sequence, outside any subscription.
///
/// What a hold does when a hole has stood open for its whole window:
/// rather than guess whether the group is still coming, ask the relay
/// for it. A relay that has it in its cache serves it here and the hole
/// closes; one that does not refuses, and the refusal - not a clock -
/// is what lets the cursor step over the hole. No answer inside the
/// window counts as a refusal, since the hold has to move eventually.
async fn fetch_group(
	index: usize,
	broadcast: moq_net::broadcast::Consumer,
	name: String,
	sequence: u64,
	wait: Duration,
	sender: mpsc::UnboundedSender<Wire>,
) {
	let asked = async {
		let track = broadcast
			.track(&name)
			.map_err(|err| format!("track '{name}': {err}"))?;
		let mut group = track
			.fetch_group(sequence, None)
			.await
			.map_err(|err| err.to_string())?;
		let mut frames = Vec::new();
		while let Some(frame) = group.read_frame().await.map_err(|err| err.to_string())? {
			frames.push(Received {
				group: sequence,
				frame_in_group: frames.len() as u64,
				timestamp_us: frame
					.timestamp
					.convert(moq_net::Timescale::MICRO)
					.map_err(|err| err.to_string())?
					.value(),
				payload: frame.payload,
			});
		}
		Ok::<_, String>(frames)
	};
	let wire = match tokio::time::timeout(wait, asked).await {
		Ok(Ok(frames)) => Wire::Group(index, sequence, frames),
		Ok(Err(err)) => Wire::Gone(index, sequence, err),
		Err(_) => Wire::Gone(
			index,
			sequence,
			format!("no answer within {}s", wait.as_secs()),
		),
	};
	let _ = sender.send(wire);
}

/// One track's groups onto the shared channel until it finishes, and
/// the word that it has.
///
/// Frames are gathered per group and the group goes out whole, the
/// moment the stream says it has been read to its end. That matters most
/// to a data track, whose next group may be a minute away: a message has
/// to go on when its own group ends, not when the next one begins.
/// The channel is unbounded on purpose - one track blocking on a full
/// queue would stop it reading its subscription, and the relay would
/// age its groups out while another track drained. Nothing accumulates
/// for long: these tasks only run while a call is blocked on the
/// executor, and that call returns as soon as anything is ready.
async fn read_track(
	index: usize,
	broadcast: moq_net::broadcast::Consumer,
	name: String,
	subscriber: moq_net::track::Subscriber,
	sender: mpsc::UnboundedSender<Wire>,
) {
	read_groups(index, broadcast, name, subscriber, &sender).await;
	// The reader holds no sender of its own once this returns, so the
	// end of the run is counted rather than read off a closed channel:
	// a fetch for a missing group answers on the same one.
	let _ = sender.send(Wire::Finished);
}

async fn read_groups(
	index: usize,
	broadcast: moq_net::broadcast::Consumer,
	name: String,
	subscriber: moq_net::track::Subscriber,
	sender: &mpsc::UnboundedSender<Wire>,
) {
	let mut stream = moq_core::subscribe::FrameStream::new(subscriber);
	let mut attempts = 0u32;
	loop {
		// The group being gathered is dropped on a resubscribe: only a
		// group read to its end goes out, and the sequence cursor
		// downstream drops whatever the new subscription repeats.
		let mut open: Option<(u64, Vec<Received>)> = None;
		let failure = loop {
			match stream.step().await {
				Ok(Some(Step::Frame(frame))) => match &mut open {
					Some((sequence, frames)) if *sequence == frame.group => frames.push(frame),
					_ => {
						// A group whose end was never read, which only a
						// stream that moved on without one can hand over.
						if let Some((sequence, frames)) = open.take() {
							if sender.send(Wire::Group(index, sequence, frames)).is_err() {
								return;
							}
						}
						open = Some((frame.group, vec![frame]));
					}
				},
				Ok(Some(Step::End(ended))) => {
					if let Some((sequence, frames)) = open.take_if(|(sequence, _)| *sequence == ended) {
						if sender.send(Wire::Group(index, sequence, frames)).is_err() {
							return;
						}
					}
				}
				// The publisher finished the track: its last group goes
				// out and this reader is done.
				Ok(None) => {
					if let Some((sequence, frames)) = open.take() {
						let _ = sender.send(Wire::Group(index, sequence, frames));
					}
					return;
				}
				Err(err) => break err.to_string(),
			}
		};

		// The wire broke under the track. A live broadcast outlives one
		// subscription, so the track is taken up again; one that will
		// not stay up on this session is the session's to mend.
		attempts += 1;
		if attempts > TRACK_RETRIES {
			let _ = sender.send(Wire::Lost(index, failure));
			return;
		}
		eprintln!("subscribe: track '{name}': {failure}; subscribing again");
		tokio::time::sleep(TRACK_RETRY).await;
		// Always at the live edge, whichever end this run joined at. A
		// backlog is asked for once: a relay that dropped the
		// subscription rather than serve it will do so again, and the
		// latest group is what it does have either way.
		let taken = match subscribe_track(&broadcast, &name, Start::Live) {
			Ok(subscribing) => subscribing.await.map_err(|err| err.to_string()),
			Err(err) => Err(err),
		};
		match taken {
			Ok(subscriber) => {
				// The new subscription starts at the live edge, so the
				// groups between are gone: the hold is told, rather than
				// left waiting out its whole window for one of them.
				if sender.send(Wire::Restarted(index)).is_err() {
					return;
				}
				stream = moq_core::subscribe::FrameStream::new(subscriber);
			}
			// The subscription could not even be asked for again, which
			// is what a session the relay has dropped looks like from
			// here.
			Err(err) => {
				let _ = sender.send(Wire::Lost(index, err));
				return;
			}
		}
	}
}


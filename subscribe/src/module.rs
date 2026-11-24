use std::time::{Duration, Instant};

use ffrwd_node::{
	Bound, CodedAudio, CodedFormat, CodedStream, CodedVideo, Init, Node, Out, Output, Packet,
	Rational, Shape, Tick,
};
use moq_core::order::{Hold, Queue};
use moq_core::restart::{Seen, Stamp, Timeline};
use moq_core::still::{self, Stills};
use moq_core::subscribe::{Received, Start, Step};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::mpsc;

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP, and a path the session opens under, which is where a relay's own address carries a token: https://relay.example/<JWT>"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none, and a token already in the relay URL needs none"},"hold_ms":{"type":"integer","minimum":0,"description":"how long a hole in a track's group sequence is held open for the group that would fill it before the relay is asked for it, in milliseconds; left out, 1000 on a live join, which has given up the past already, and 30000 on a backlog join, the subscription's own latency window"},"hold_mib":{"type":"integer","minimum":1,"default":64,"description":"how much one track holds meanwhile, in MiB; 64 covers a 30s window up to about 17 Mbit/s"},"join_ms":{"type":"integer","minimum":0,"default":2000,"description":"how long a joining reader waits for a lower group sequence before it fixes its cursor on a backlog join, in milliseconds; 0 starts at the first group that arrives, and a live join never waits at all"},"reconnect_s":{"type":"integer","minimum":0,"default":60,"description":"how long a reader whose session the relay dropped keeps trying to open a new one and take its tracks up again at the live edge, in seconds; 0 ends the run on the first drop"},"restart_s":{"type":"integer","minimum":0,"default":0,"description":"how long a reader whose publisher finished its tracks waits for a publisher of the same broadcast to take it up again, in seconds; a program that starts again finishes its tracks first when its run ends. 0 ends the run when the tracks finish, as at the end of a file. A broadcast the relay dropped without its tracks finishing is waited for as long as this or reconnect_s allows, whichever is longer"},"still":{"type":["integer","boolean"],"minimum":0,"maximum":1,"default":0,"description":"1 (or true) to go on handing media on while the publisher the reader reads is away, so that a destination which closes a connection that carries nothing, an RTMP push for one, never goes without: each picture's last keyframe again every 500 ms and silent AAC frames with no hole between them, from a second after the last media, or at once while the reader waits for a publisher it knows has gone, until the publisher's media is back or the wait ends. Data and text ports get an empty message every 500 ms, which says how far their timeline has got, until they hand on a row of their own. 0, the default, hands nothing on meanwhile"},"start":{"type":"string","enum":["live","backlog"],"default":"live","description":"where to join a broadcast already running: 'live' at the publisher's newest group, starting at the first group a decoder can begin at - a keyframe group for video, any group for audio - or 'backlog' at the oldest group the relay still holds, which reads the whole cache before the first packet comes out"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

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
	/// How long a reader whose publisher finished its tracks waits for one
	/// to take the broadcast up again, in seconds; see [`Reader::follow`].
	#[serde(default)]
	restart_s: u64,
	/// Whether the reader hands on a still while its publisher is away;
	/// see [`Holding`].
	#[serde(default, deserialize_with = "flag")]
	still: bool,
}

/// A switch written as 1 or 0, or as true or false. A query writes 1 or 0:
/// ffrwd's compiler takes literals and NULL among a source's arguments,
/// and `true` is neither to it.
fn flag<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
	#[derive(Deserialize)]
	#[serde(untagged)]
	enum Flag {
		Bool(bool),
		Number(f64),
	}
	match Flag::deserialize(deserializer)? {
		Flag::Bool(on) => Ok(on),
		Flag::Number(0.0) => Ok(false),
		Flag::Number(1.0) => Ok(true),
		Flag::Number(number) => Err(serde::de::Error::custom(format!(
			"a switch is 1 or 0, not {number}"
		))),
	}
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
	moq_core::prefixes::check(&params.relay, &params.token, &params.broadcast)?;
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
	/// Whether its rows are cues read off a text track.
	text: bool,
}

struct RenditionMeta {
	name: Option<String>,
	codecs: Option<String>,
	/// Bits a second, where the catalog states it.
	bandwidth: Option<u64>,
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
			// Where the host's timer is coarse, a frame would wait for the
			// socket reactor's next sweep; see `park_if_coarse`.
			let (sleep, coarse) = cell
				.get()
				.expect("just set")
				.enter(moq_core::wasi::park_if_coarse());
			if coarse {
				eprintln!(
					"subscribe: a 1 ms sleep lasts {:.1} ms here, so the socket reactor waits \
					 in wasi:io/poll",
					sleep.as_secs_f64() * 1000.0
				);
			}
		}
		Ok(cell.get().expect("just set").enter(work))
	})
}

/// What one reader task says about its track.
enum Wire {
	/// One frame of a group, as it arrives: a complete fmp4 fragment, a
	/// legacy sample, or for a data track the one message. A frame is
	/// whole on its own, so it is handed on without waiting for the rest
	/// of its group.
	Frame(usize, Received),
	/// The group of this sequence has been read to its end.
	End(usize, u64),
	/// One complete group, asked for outright: its sequence and its frames.
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
	/// them. A track that was lost says so instead.
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
	/// A WebVTT track: each fragment a sample of the cues shown over its
	/// span, read by the track its init segment describes and handed on as
	/// cue rows; see [`moq_core::text`].
	Text {
		track: ffrwd_bmff::track::Track,
		cues: moq_core::text::Reassembly,
	},
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
	/// Its catalog name and kind.
	name: String,
	kind: moq_core::catalog::Kind,
	/// Its catalog entry as last read, which a broadcast that comes back
	/// has to match in everything a decoder reads; see
	/// [`moq_core::catalog::changed`].
	entry: moq_core::catalog::Rendition,
	/// Set when the track was taken up again and nothing has arrived on
	/// it yet, with nothing yet showing that its publisher started again:
	/// its first group says whether it carried on.
	resumed: bool,
	/// The first frame of the last group handed on, which tells that group
	/// served again from another group of the same number.
	last_first: Option<moq_core::subscribe::FirstFrame>,
	/// After a publisher finished its tracks: the first frame of their last
	/// group. A relay still holding the finished broadcast hands a new
	/// session that group again, and it is passed over rather than shown
	/// twice; `replaying` is the group being passed over.
	replay: Option<moq_core::subscribe::FirstFrame>,
	replaying: Option<u64>,
}

impl Rendition {
	/// The ticks its packets count in.
	fn timescale(&self) -> u32 {
		match &self.body {
			Body::Media(track) => track.timescale,
			Body::Legacy(_) => 1_000_000,
			Body::Data { timescale } => *timescale,
			Body::Text { track, .. } => track.timescale,
		}
	}

	/// Whether its packets are media, a picture or sound, rather than
	/// messages or cues.
	fn media(&self) -> bool {
		!matches!(self.body, Body::Data { .. } | Body::Text { .. })
	}

	/// The track was taken up on a fresh subscription at the live edge:
	/// what its hold waits for is not coming, and a picture has to start
	/// again at a sync sample, since the frames it would have referred
	/// back to went with the groups in between.
	fn restart(&mut self) {
		self.queue.restart();
		self.begin_again();
	}

	/// What a picture and a legacy track's decode times start from again.
	fn begin_again(&mut self) {
		self.order.restart();
		if self.video {
			self.join = moq_core::group::Join::video();
		}
	}

	/// A frame of the last group of a broadcast whose tracks finished,
	/// handed over again by a relay that still holds it.
	fn passed_over(&mut self, frame: &Received) -> bool {
		if let Some(group) = self.replaying {
			if frame.group == group {
				return true;
			}
			self.replaying = None;
		}
		match self.replay.take() {
			Some(replay) if replay.again(frame) => {
				eprintln!(
					"subscribe: track '{}': the relay handed over group {} again, of the \
					 broadcast whose tracks finished; passed over",
					self.name, frame.group
				);
				self.replaying = Some(frame.group);
				true
			}
			_ => false,
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

/// Why taking the tracks up again ended without them: a refusal, which
/// is final, or a window spent with nothing to take them up on.
enum Mend {
	Refused(String),
	Spent(String),
}

impl Mend {
	fn message(self) -> String {
		match self {
			Mend::Refused(message) | Mend::Spent(message) => message,
		}
	}
}

/// How the broadcast the tracks were on went away, while the session
/// under it stayed up.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ended {
	/// Its publisher finished every track, which a run that ends does.
	Finished,
	/// It was dropped with its tracks unfinished: another publisher took
	/// the name, or the relay gave up on a publisher it no longer heard.
	Vanished,
}

/// A newly dialed session, for the reader to move onto.
struct Dialed {
	endpoint: quinn::Endpoint,
	session: moq_net::Session,
	driver: tokio::task::JoinHandle<Result<(), moq_net::Error>>,
	announced: moq_net::announce::Consumer,
}

/// The first hop of a broadcast's route: the origin of the publisher
/// that announced it, which a publisher keeps across its own sessions
/// and a new one does not share. None where the wire carries no hops.
fn publisher_of(broadcast: &moq_net::broadcast::Consumer) -> Option<u64> {
	broadcast
		.route()
		.hops
		.iter()
		.next()
		.map(|origin| origin.id())
		.filter(|id| *id != 0)
}

/// Every track being read, and the session under them.
struct Reader {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
	/// The session's protocol task, which ends when the session does.
	driver: tokio::task::JoinHandle<Result<(), moq_net::Error>>,
	/// What the session's relay announces at the broadcast's path: where a
	/// broadcast that went away shows up again. A wait for it reads it on a
	/// task of its own meanwhile; see [`Wait`].
	announced: Option<moq_net::announce::Consumer>,
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
	/// Who published the broadcast the tracks are on; see
	/// [`publisher_of`].
	publisher: Option<u64>,
	/// Every track's timestamps on the way out, which a publisher that
	/// started again shifts.
	timeline: Timeline<Packet>,
	/// Tracks whose reader the publisher finished, and the groups a hole
	/// is waiting on an answer for.
	finished: usize,
	asking: Vec<(usize, u64)>,
	/// What every track's hold is bounded by.
	hold: Hold,
	/// When the last pull returned, which is when the session last stopped
	/// turning.
	left: Option<std::time::Instant>,
	/// The tracks being taken up again, a wait that goes on across pulls.
	wait: Option<Wait>,
	/// The stills the query asked for, handed on while the publisher is
	/// away.
	holding: Option<Holding>,
}

/// A wait for the tracks to be taken up again: after a publisher that went
/// away ([`Reader::follow`]), or a session that ended
/// ([`Reader::reconnect`]). It goes on across pulls, so that a reader that
/// holds a still hands it on meanwhile. What it looks for is looked for by
/// a task of its own on the instance's runtime, which runs whenever a pull
/// waits, and each pull looks at where it has got; see [`Reader::advance`].
struct Wait {
	/// What took the tracks away.
	why: String,
	/// What a window spent with nothing found does: end the run, after a
	/// publisher that finished its tracks, or fail it.
	ends: bool,
	began: std::time::Instant,
	window: Duration,
	stage: Stage,
}

/// What a search for the broadcast announced again hands back: the
/// announcements to read on, and the broadcast, None once nothing more
/// will be announced on the session.
type Searched = (
	Option<moq_net::announce::Consumer>,
	Option<moq_net::broadcast::Consumer>,
);

/// Where a wait has got to.
enum Stage {
	/// Waiting on this session for the broadcast to be announced again; see
	/// [`announced_again`].
	Announced {
		ended: Ended,
		search: tokio::task::JoinHandle<Searched>,
	},
	/// Reading the catalog of the broadcast announced again.
	Catalog {
		ended: Ended,
		broadcast: moq_net::broadcast::Consumer,
		search: tokio::task::JoinHandle<Result<Vec<moq_core::catalog::Rendition>, String>>,
	},
	/// Opening a new session, the try `attempts` counts; see [`dial`].
	Dial {
		ended: Option<Ended>,
		attempts: u64,
		backoff: Duration,
		search: tokio::task::JoinHandle<Result<Opened, String>>,
	},
}

impl Stage {
	/// Stops what the wait was looking for.
	fn abort(&self) {
		match self {
			Stage::Announced { search, .. } => search.abort(),
			Stage::Catalog { search, .. } => search.abort(),
			Stage::Dial { search, .. } => search.abort(),
		}
	}
}

/// The stills a reader hands on while its publisher is away, when the query
/// asks for them (`still`): each picture's last sync sample again, silent
/// sound, and progress marks on the data and text ports. They go out while
/// the reader waits for a publisher it knows has gone, and once it has
/// heard no media for [`still::QUIET`], which is all a publisher lost
/// outright says until the relay times it out. They stop when the
/// publisher's media comes again, and a data or text port's marks when it
/// hands on a row of its own. When and with what timestamps is
/// [`moq_core::still`]'s; the timeline places the publisher's media after
/// the stills (see [`Timeline::still`]). Marks do not pass through the
/// timeline; a packet that lands behind one goes out at the mark's time,
/// since a port's times never go back.
struct Holding {
	stills: Stills,
	/// Each picture's last sync sample, the bytes its stills carry.
	sync: Vec<Option<Vec<u8>>>,
	/// Each sound track's silent frame, where it has one.
	silent: Vec<Option<&'static [u8]>>,
	/// When media last came off the wire.
	heard: Option<std::time::Instant>,
	/// The stills going out since the publisher's media last did.
	spell: Option<Spell>,
}

/// One stretch of stills: when the first went out, and how many of each.
struct Spell {
	began: std::time::Instant,
	pictures: u64,
	frames: u64,
}

impl Spell {
	/// One line saying the stills are over, how long they went out for and
	/// how many there were, with what ended them.
	fn say(&self, ended: &str) {
		eprintln!(
			"subscribe: {ended} after {:.1}s of stills: {} pictures, {} silent frames",
			self.began.elapsed().as_secs_f64(),
			self.pictures,
			self.frames
		);
	}
}

impl Holding {
	/// The stills of the tracks the reader pulls, in its order. Sound with no
	/// silent frame says so here, once.
	fn new(renditions: &[Rendition]) -> Self {
		let mut kinds = Vec::with_capacity(renditions.len());
		let mut silent = Vec::with_capacity(renditions.len());
		for rendition in renditions {
			let (kind, frame) = match rendition.kind {
				_ if !rendition.media() => (still::Kind::Marks, None),
				moq_core::catalog::Kind::Video { .. } => (still::Kind::Picture, None),
				moq_core::catalog::Kind::Audio { sample_rate, .. } => {
					match still::silent_aac(&rendition.entry.config) {
						Ok(frame) if sample_rate > 0 => {
							(still::Kind::Sound { rate: sample_rate }, Some(frame))
						}
						Ok(_) => {
							eprintln!(
								"subscribe: track '{}' states no sample rate, so its sound has no \
								 still",
								rendition.name
							);
							(still::Kind::Nothing, None)
						}
						Err(why) => {
							eprintln!(
								"subscribe: track '{}': {why}, so its sound has no still",
								rendition.name
							);
							(still::Kind::Nothing, None)
						}
					}
				}
				_ => (still::Kind::Nothing, None),
			};
			kinds.push((rendition.timescale(), kind));
			silent.push(frame);
		}
		Holding {
			stills: Stills::new(kinds),
			sync: (0..renditions.len()).map(|_| None).collect(),
			silent,
			heard: None,
			spell: None,
		}
	}

	/// The still of `track` at `stamp`, a picture or sound counted in the
	/// spell, and a mark an empty message.
	fn packet(&mut self, track: usize, stamp: Stamp) -> Packet {
		let kind = self.stills.kind(track);
		if let (Some(spell), false) = (self.spell.as_mut(), kind == still::Kind::Marks) {
			match kind {
				still::Kind::Picture => spell.pictures += 1,
				_ => spell.frames += 1,
			}
		}
		let data = match kind {
			still::Kind::Picture => self.sync[track].clone().unwrap_or_default(),
			still::Kind::Marks => Vec::new(),
			_ => self.silent[track].unwrap_or_default().to_vec(),
		};
		Packet {
			pts: stamp.pts,
			dts: Some(stamp.dts),
			duration: Some(stamp.duration).filter(|ticks| *ticks > 0),
			keyframe: true,
			data,
		}
	}

	/// A packet of the publisher's about to go out on `track`'s pad at
	/// `now`. The stills that fit between the track's last still and it go
	/// first, and the track's stills carry on from it from here on.
	fn carry_on(&mut self, track: usize, packet: &Packet, pad: &mut Vec<Packet>, now: Instant) {
		let stamp = stamp_of(packet);
		for still in self.stills.bridge(track, stamp.dts) {
			let filled = self.packet(track, still);
			pad.push(filled);
		}
		self.stills.handed_on(track, stamp, packet.keyframe, now);
		if packet.keyframe && self.stills.kind(track) == still::Kind::Picture {
			self.sync[track] = Some(packet.data.clone());
		}
	}
}

/// The reader the instance's runtime drives.
struct State {
	reader: Reader,
}

/// A dialed relay with the broadcast open: what both calls need
/// before they part ways.
struct Opened {
	dialed: Dialed,
	broadcast: moq_net::broadcast::Consumer,
	renditions: Vec<moq_core::catalog::Rendition>,
}

/// Connects, waits for the broadcast to be announced and reads its
/// catalog. The session driver is spawned here and runs whenever a
/// call blocks on the executor. A broadcast not announced within `wait`
/// is asked for again on a new session, up to `dials` sessions in all.
///
/// Where the host holds the run to prefixes, the session asks the relay
/// to announce only what is under them; see
/// [`moq_core::prefixes::subscriber_origin`].
async fn open_broadcast(params: &Params, wait: Duration, dials: u32) -> Result<Opened, String> {
	let path = params.broadcast.as_str();
	let prefixes = moq_core::prefixes::Prefixes::from_env();
	let mut dialed = 0;
	let (endpoint, session, driver, broadcast, announced) = loop {
		dialed += 1;
		let origin = moq_core::prefixes::subscriber_origin(
			prefixes.as_ref(),
			&params.relay,
			&params.token,
			path,
		)?;
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
			Ok(Some(broadcast)) => {
				// Watched from here on: a broadcast that goes away is
				// announced again on this session when a publisher takes
				// the name up.
				let scope = [moq_net::AsPath::as_path(&path)];
				let announced = origin
					.consume()
					.scope(&scope)
					.ok_or_else(|| format!("broadcast '{path}' is outside this session's scope"))?
					.announced();
				break (endpoint, session, driver, broadcast, announced);
			}
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
		dialed: Dialed {
			endpoint,
			session,
			driver,
			announced,
		},
		broadcast,
		renditions: moq_core::catalog::parse(&document)
			.map_err(|err| format!("broadcast '{path}': {err}"))?,
	})
}

/// The next broadcast a session's relay announces at `path` that the
/// tracks are not on already (`current`), and that is not `tried`. After a
/// publisher finished its tracks the relay goes on announcing that
/// broadcast for a while (moq-relay does until its publisher's session
/// closes and the linger after it), and a new session would be handed it
/// again, so one from the same publisher is passed over. None once nothing
/// more will be announced. The announcements are handed back, to be read
/// on.
async fn announced_again(
	mut announced: moq_net::announce::Consumer,
	path: String,
	current: moq_net::broadcast::Consumer,
	publisher: Option<u64>,
	ended: Ended,
	tried: Option<moq_net::broadcast::Consumer>,
) -> Searched {
	loop {
		let Some(update) = announced.next().await else {
			return (Some(announced), None);
		};
		let Some(broadcast) = update.broadcast else {
			continue;
		};
		let same_finished = ended == Ended::Finished
			&& publisher.is_some()
			&& publisher_of(&broadcast) == publisher;
		if update.path.as_str() == path
			&& !broadcast.is_closed()
			&& !broadcast.is_clone(&current)
			&& !tried
				.as_ref()
				.is_some_and(|tried| broadcast.is_clone(tried))
			&& !same_finished
		{
			return (Some(announced), Some(broadcast));
		}
	}
}

/// One try at a new session after `pause`: dialed, the broadcast announced
/// on it and its catalog read, each within what is left of `window` since
/// `began`, and never less than [`RECONNECT_FIRST`].
async fn dial(
	params: Params,
	pause: Duration,
	began: std::time::Instant,
	window: Duration,
) -> Result<Opened, String> {
	if !pause.is_zero() {
		tokio::time::sleep(pause).await;
	}
	let left = window.saturating_sub(began.elapsed());
	open_broadcast(&params, left.max(RECONNECT_FIRST), 1).await
}

/// The catalog of a broadcast announced again on a session already up.
async fn read_renditions(
	broadcast: &moq_net::broadcast::Consumer,
	path: &str,
	wait: Duration,
) -> Result<Vec<moq_core::catalog::Rendition>, String> {
	let document = tokio::time::timeout(wait, moq_core::subscribe::read_catalog(broadcast))
		.await
		.map_err(|_| format!("sent no catalog within {}s", wait.as_secs()))?
		.map_err(|err| err.to_string())?;
	moq_core::catalog::parse(&document).map_err(|err| format!("broadcast '{path}': {err}"))
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
			bandwidth: None,
		},
		text: false,
	})
}

/// One catalog text rendition as the track a source publishes: cue rows,
/// in the ticks its init segment counts in, read out of a `wvtt` track.
fn text_track(rendition: &moq_core::catalog::Rendition) -> Result<(SourceTrack, Body), String> {
	let track = ffrwd_bmff::track::Track::from_init(&rendition.init)
		.map_err(|err| format!("rendition '{}': init segment: {err}", rendition.name))?;
	if &track.entry.kind != b"wvtt" {
		return Err(format!(
			"rendition '{}' is packaged as '{}', and a text track reads wvtt",
			rendition.name,
			String::from_utf8_lossy(&track.entry.kind)
		));
	}
	Ok((
		SourceTrack {
			coded: CodedStream {
				codec: "json".to_string(),
				time_base: Rational {
					num: 1,
					den: track.timescale as i32,
				},
				format: CodedFormat::Data,
				extradata: Vec::new(),
				profile: None,
				level: None,
			},
			row: rendition.row,
			rendition: RenditionMeta {
				name: Some(rendition.name.clone()),
				codecs: Some(rendition.codec.clone()),
				bandwidth: None,
			},
			text: true,
		},
		Body::Text {
			track,
			cues: moq_core::text::Reassembly::default(),
		},
	))
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
	if let moq_core::catalog::Kind::Text = rendition.kind {
		return text_track(rendition);
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
				bandwidth: rendition.bitrate,
			},
			text: false,
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
				bandwidth: rendition.bitrate,
			},
			text: false,
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
			kind: rendition.kind,
			entry: rendition.clone(),
			order: moq_core::legacy::Order::default(),
			resumed: false,
			last_first: None,
			replay: None,
			replaying: None,
		});
	}
	Ok((Catalog { tracks }, readers))
}

impl Reader {
	/// One pull: the packets that came in, a list per track in catalog
	/// order, or none once every track has finished and nothing is held.
	///
	/// A pull returns as soon as it has packets to hand on. The reader
	/// tasks and the QUIC driver run only while a call is on the executor,
	/// so a pull first lets them run once, which takes in what the wire
	/// has already brought, and then waits on them only while it has
	/// nothing to hand on. The host calls a source again as soon as a call
	/// returns, so the session turns between the two.
	///
	/// Before 0.9.2 a pull went on turning the session for up to 200 more
	/// sleeps of a millisecond once it had something, while groups kept
	/// arriving. Packets then left in lumps, and on Windows, where each of
	/// those sleeps is nearer 2 ms, lumps of up to 400 ms.
	async fn pull(&mut self) -> Result<Option<Vec<PadPackets>>, String> {
		let began = std::time::Instant::now();
		let gap = self.left.map(|left| began.duration_since(left));
		let pulled = self.take_in().await;
		let now = std::time::Instant::now();
		self.left = Some(now);
		for rendition in &mut self.renditions {
			rendition.queue.note_call(gap, now.duration_since(began));
		}
		pulled
	}

	async fn take_in(&mut self) -> Result<Option<Vec<PadPackets>>, String> {
		let mut pads: Vec<Vec<Packet>> = (0..self.renditions.len()).map(|_| Vec::new()).collect();
		// What the wire brought while the host had the call.
		tokio::task::yield_now().await;
		let mut produced = false;
		loop {
			if let Some(ends) = self.wait.as_ref().map(|wait| wait.ends) {
				// The tracks are being taken up again. The wire they were on
				// is left alone meanwhile: nothing of it reaches the holds.
				match self.advance().await {
					Ok(true) => continue,
					Ok(false) => {}
					Err(Mend::Spent(why)) if ends => {
						eprintln!("subscribe: {why}; ending");
						return Ok(self.end());
					}
					Err(mend) => {
						self.end_stills();
						return Err(mend.message());
					}
				}
			} else {
				while let Ok(wire) = self.frames.try_recv() {
					self.hold(wire)?;
					// A burst can bring more than a track's hold may keep, a
					// backlog hundreds of groups deep in one go. The budget is
					// held to as it fills, not once the burst is in.
					let hold = self.hold;
					if self
						.renditions
						.iter()
						.any(|rendition| rendition.queue.full(hold))
					{
						produced |= self.release(&mut pads, false)?;
					}
				}
				// The session ended under the tracks, or one of them could not
				// be taken up again on it: open a new one before anything else.
				// A broadcast closed under a session that is still up is the
				// publisher gone, not the session: another may already be
				// announced on it.
				if self.driver.is_finished() || self.lost.is_some() {
					let alive = !self.driver.is_finished();
					let why = match self.lost.take() {
						Some(why) => why,
						None => match (&mut self.driver).await {
							Ok(Ok(())) => "the relay closed the session".to_string(),
							Ok(Err(err)) => err.to_string(),
							Err(err) => err.to_string(),
						},
					};
					// A track lost under a session that is still up may be its
					// publisher gone, which a relay drops some seconds after it
					// fails the tracks: waited for as long as either window
					// allows, whether or not the broadcast is closed yet.
					let reconnect = Duration::from_secs(self.params.reconnect_s);
					let restart = reconnect.max(Duration::from_secs(self.params.restart_s));
					let waiting = if alive && self.broadcast.is_closed() {
						self.follow(why, Ended::Vanished, restart, false)
					} else {
						let window = if alive { restart } else { reconnect };
						self.reconnect(why, window, None, false)
					};
					if let Err(mend) = waiting {
						self.end_stills();
						return Err(mend.message());
					}
					continue;
				}
				produced |= self.release(&mut pads, self.over())?;
				self.ask();
				self.report();
				if !produced && self.over() {
					// Every track has finished and no answer is outstanding:
					// whatever was held has just gone out. A publisher that
					// finished may be followed by one that takes the broadcast
					// up again, for as long as `restart_s` allows; otherwise
					// this is the end of the run.
					let window = Duration::from_secs(self.params.restart_s);
					match self.follow(
						"its tracks finished".to_string(),
						Ended::Finished,
						window,
						true,
					) {
						Ok(()) => continue,
						Err(Mend::Refused(err)) => return Err(err),
						Err(Mend::Spent(_)) => return Ok(self.end()),
					}
				}
			}
			produced |= self.hand_on_stills(&mut pads);
			if produced {
				return Ok(Some(into_pads(pads)));
			}
			let slice = self.slice();
			if self.wait.is_some() {
				tokio::time::sleep(slice).await;
				continue;
			}
			// Nothing ready: wait for the wire, but only for a tick. The
			// hold releases on a deadline as well as on an arrival, and a
			// track that has gone quiet would otherwise sleep through it.
			match tokio::time::timeout(slice, self.frames.recv()).await {
				Ok(Some(wire)) => self.hold(wire)?,
				// Nothing holds a sender but this reader, so the channel
				// closes only if it has been dropped out from under us.
				Ok(None) => return Ok(None),
				Err(_) => {}
			}
		}
	}

	/// The end of the run: every track's last row, and the stills going
	/// out end with it.
	fn end(&mut self) -> Option<Vec<PadPackets>> {
		self.end_stills();
		for rendition in &mut self.renditions {
			rendition.queue.report(true);
		}
		None
	}

	/// The stills going out, if any, end with the run.
	fn end_stills(&mut self) {
		if let Some(spell) = self
			.holding
			.as_mut()
			.and_then(|holding| holding.spell.take())
		{
			spell.say("the run ends");
		}
	}

	/// Whether stills go out now: the query asked for them, and the reader
	/// is waiting for its publisher or has heard no media for
	/// [`still::QUIET`].
	fn holds(&self, now: std::time::Instant) -> bool {
		let Some(holding) = self.holding.as_ref() else {
			return false;
		};
		self.wait.is_some()
			|| holding
				.heard
				.is_some_and(|heard| now.saturating_duration_since(heard) >= still::QUIET)
	}

	/// The stills due, onto their pads, while the reader holds them, and the
	/// marks a data or text port goes on with after them; see [`Holding`].
	/// True when any went out.
	fn hand_on_stills(&mut self, pads: &mut [Vec<Packet>]) -> bool {
		let now = Instant::now();
		let holds = self.holds(now);
		let Some(holding) = self.holding.as_mut() else {
			return false;
		};
		let due = holding.stills.due(now, holds);
		if due.is_empty() {
			return false;
		}
		let media = due
			.iter()
			.any(|(index, _)| holding.stills.kind(*index) != still::Kind::Marks);
		if media && holding.spell.is_none() {
			let why = match self.wait.as_ref() {
				Some(wait) => wait.why.clone(),
				None => {
					let quiet = holding
						.heard
						.map_or(Duration::ZERO, |heard| now.saturating_duration_since(heard));
					format!("no media for {:.1}s", quiet.as_secs_f64())
				}
			};
			eprintln!("subscribe: {why}; holding a still until the publisher is back");
			holding.spell = Some(Spell {
				began: now,
				pictures: 0,
				frames: 0,
			});
		}
		for (index, stamp) in due {
			match holding.stills.kind(index) {
				still::Kind::Marks => {}
				kind => self.timeline.still(index, stamp, kind == still::Kind::Picture),
			}
			pads[index].push(holding.packet(index, stamp));
		}
		true
	}

	/// How long a pull with nothing to hand on waits before it looks again:
	/// a tick, or less when a still or a mark falls due sooner.
	fn slice(&self) -> Duration {
		let now = Instant::now();
		match self.holding.as_ref() {
			Some(holding) => holding
				.stills
				.next(self.holds(now))
				.map_or(TICK, |next| next.saturating_duration_since(now).min(TICK)),
			_ => TICK,
		}
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
			Wire::Frame(index, frame) => {
				if self.renditions[index].passed_over(&frame) {
					return Ok(());
				}
				self.heard(index);
				self.timeline.new_frame(index);
				self.resumed_at(index, &frame)?;
				let rendition = &mut self.renditions[index];
				let sequence = frame.group;
				// Whether a decoder could start at this group, which is
				// what a live join lands on, when this frame is what says
				// so. The keyframe flag comes off the fragment the publisher
				// wrote rather than off a convention about where groups are
				// cut.
				let decides = match rendition.queue.deciding(sequence) {
					true => frame_decides(rendition, &frame),
					false => None,
				};
				rendition.queue.frame(sequence, frame, decides)
			}
			Wire::End(index, sequence) => {
				let rendition = &mut self.renditions[index];
				match rendition.replaying.take_if(|group| *group == sequence) {
					Some(_) => {}
					None => rendition.queue.end(sequence),
				}
			}
			Wire::Group(index, sequence, frames) => {
				if let Some(first) = frames.first() {
					self.heard(index);
					self.timeline.new_frame(index);
					self.resumed_at(index, first)?;
				}
				let rendition = &mut self.renditions[index];
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

	/// Media came off the wire for `track`: the publisher is there.
	fn heard(&mut self, track: usize) {
		if let (Some(holding), true) = (self.holding.as_mut(), self.renditions[track].media()) {
			holding.heard = Some(Instant::now());
		}
	}

	/// The first frame of a track taken up again, when nothing had shown
	/// yet whether its publisher carried on. One that carried on counts on
	/// from where it was; one that started again counts from nothing (see
	/// [`moq_core::subscribe::started_again`]), and its track lets out what
	/// it still holds of the old numbering and joins the new one, and the
	/// timeline is told, since the publisher's clock went back with it.
	fn resumed_at(&mut self, index: usize, frame: &Received) -> Result<(), String> {
		let rendition = &mut self.renditions[index];
		if !std::mem::take(&mut rendition.resumed) {
			return Ok(());
		}
		let cursor = rendition.queue.cursor();
		if moq_core::subscribe::started_again(cursor, rendition.last_first.as_ref(), frame) {
			self.let_out_old(index)?;
			self.timeline.renumbered();
		}
		Ok(())
	}

	/// What a track still holds of a publisher that started again, out in
	/// sequence under the shift it was made under, before the track joins
	/// the new numbering at a sync sample. A text track's cues still on
	/// screen end there, where that publisher last had them.
	fn let_out_old(&mut self, index: usize) -> Result<(), String> {
		let rendition = &mut self.renditions[index];
		let old = rendition.queue.rejoin(self.hold);
		let mut packets = Vec::new();
		for (_, frames) in old {
			demux(rendition, index, frames, &mut packets)?;
		}
		if let Body::Text { track, cues } = &mut rendition.body {
			let timescale = track.timescale;
			packets.extend(cues.finish().iter().map(|row| text_packet(row, timescale)));
		}
		rendition.begin_again();
		for packet in packets {
			let stamp = stamp_of(&packet);
			self.timeline
				.push_late(index, stamp, packet.keyframe, packet);
		}
		Ok(())
	}

	/// Opens a new session and takes every track up again at the live
	/// edge, trying for as long as `window` allows: `reconnect_s` for a
	/// session the relay dropped.
	///
	/// A relay resets sessions now and then - every one at once, the
	/// same instant for two readers on two machines - and a live
	/// broadcast outlives that. So the reader dials again, waits for the
	/// broadcast to be announced, and reads its catalog: the tracks it
	/// pulls have to be there under the same names, carrying what a
	/// decoder reads unchanged, or what comes next is not the stream this
	/// run was describing, and that stops it. Everything that fails before
	/// then is tried again, a doubling wait apart, until the window is
	/// spent. `ended` says how the broadcast had gone, when it went before
	/// the session did.
	///
	/// This starts the wait, which goes on across pulls (see [`Wait`]);
	/// `ends` says what a window spent does. It fails at once on a refusal,
	/// and with a window of nothing.
	fn reconnect(
		&mut self,
		why: String,
		window: Duration,
		ended: Option<Ended>,
		ends: bool,
	) -> Result<(), Mend> {
		if window.is_zero() {
			return Err(Mend::Spent(why));
		}
		// A refusal is final, where a failure below is tried again.
		moq_core::prefixes::check(
			&self.params.relay,
			&self.params.token,
			&self.params.broadcast,
		)
		.map_err(Mend::Refused)?;
		eprintln!("subscribe: the session ended: {why}; connecting again");
		let began = Instant::now();
		let search =
			tokio::task::spawn_local(dial(self.params.clone(), Duration::ZERO, began, window));
		self.wait = Some(Wait {
			why,
			ends,
			began,
			window,
			stage: Stage::Dial {
				ended,
				attempts: 1,
				backoff: RECONNECT_FIRST,
				search,
			},
		});
		Ok(())
	}

	/// The broadcast the tracks were on went away while the session under
	/// it stayed up. Its publisher finished it, as a program's run does
	/// when its input ends; or another publisher took the name, which
	/// moq-relay grants the moment the newcomer announces, while the old
	/// publisher's session is still timing out; or the relay gave up on a
	/// publisher it stopped hearing from, at its idle timeout and the
	/// linger after it.
	///
	/// So the reader waits on this session for the broadcast to be
	/// announced again, and takes every track up on it at the live edge,
	/// for as long as `window` allows. A publisher that finished does not
	/// come back, so whatever follows it is one that started again, and so
	/// is a broadcast announced by another publisher; see
	/// [`Reader::resume`]. A session that ends meanwhile is dialed again
	/// within what is left of the window.
	///
	/// This starts the wait, which goes on across pulls (see [`Wait`]);
	/// `ends` says what a window spent does. It fails at once on a refusal,
	/// and with a window of nothing.
	fn follow(
		&mut self,
		why: String,
		ended: Ended,
		window: Duration,
		ends: bool,
	) -> Result<(), Mend> {
		if window.is_zero() {
			return Err(Mend::Spent(why));
		}
		moq_core::prefixes::check(
			&self.params.relay,
			&self.params.token,
			&self.params.broadcast,
		)
		.map_err(Mend::Refused)?;
		let stage = self.search(ended, None);
		self.wait = Some(Wait {
			why,
			ends,
			began: Instant::now(),
			window,
			stage,
		});
		Ok(())
	}

	/// A search for the next broadcast announced at the path, other than
	/// `tried`; see [`announced_again`]. It reads the session's
	/// announcements, and hands them back when it is done.
	fn search(&mut self, ended: Ended, tried: Option<moq_net::broadcast::Consumer>) -> Stage {
		let search = match self.announced.take() {
			Some(announced) => tokio::task::spawn_local(announced_again(
				announced,
				self.params.broadcast.clone(),
				self.broadcast.clone(),
				self.publisher,
				ended,
				tried,
			)),
			// Gone with a search the session's end cut short: the session is
			// what has to be opened again.
			None => tokio::task::spawn_local(async { (None, None) }),
		};
		Stage::Announced { ended, search }
	}

	/// One look at the wait for the tracks to be taken up again: true once
	/// they are, false while it goes on, and why it ended without them.
	///
	/// A wait for the broadcast to be announced again on this session ends
	/// when the window is spent, or turns into a wait for a new session when
	/// this one ends. The catalog of a broadcast that was announced is read
	/// whatever is left of the window, as is a try at a new session; a try
	/// that fails is followed by the next a doubling wait later, while the
	/// window lasts.
	async fn advance(&mut self) -> Result<bool, Mend> {
		let Some(Wait {
			why,
			ends,
			began,
			window,
			stage,
		}) = self.wait.take()
		else {
			return Ok(true);
		};
		let left = window.saturating_sub(began.elapsed());
		let on = |stage| Wait {
			why: why.clone(),
			ends,
			began,
			window,
			stage,
		};
		match stage {
			Stage::Announced { ended, search } => {
				if self.driver.is_finished() {
					search.abort();
					self.reconnect(why, left, Some(ended), ends)?;
					return Ok(false);
				}
				if !search.is_finished() {
					if left.is_zero() {
						search.abort();
						return Err(Mend::Spent(format!(
							"{why}, and no publisher took the broadcast up again within {}s",
							window.as_secs()
						)));
					}
					self.wait = Some(on(Stage::Announced { ended, search }));
					return Ok(false);
				}
				let (announced, found) = search.await.unwrap_or((None, None));
				self.announced = announced;
				let Some(broadcast) = found else {
					self.reconnect(why, left, Some(ended), ends)?;
					return Ok(false);
				};
				let path = self.params.broadcast.clone();
				let reading = broadcast.clone();
				let search = tokio::task::spawn_local(async move {
					read_renditions(&reading, &path, left.min(OPEN_TIMEOUT)).await
				});
				self.wait = Some(on(Stage::Catalog {
					ended,
					broadcast,
					search,
				}));
				Ok(false)
			}
			Stage::Catalog {
				ended,
				broadcast,
				search,
			} => {
				if !search.is_finished() {
					self.wait = Some(on(Stage::Catalog {
						ended,
						broadcast,
						search,
					}));
					return Ok(false);
				}
				let read = search.await.unwrap_or_else(|err| Err(err.to_string()));
				let failure = match read {
					Err(err) => err,
					Ok(renditions) => {
						let resumed = self.resume(broadcast.clone(), renditions, None, Some(ended));
						match resumed.await {
							Ok(()) => return Ok(true),
							Err(Resume::Refused(err)) => return Err(Mend::Refused(err)),
							Err(Resume::Again(err)) => err,
						}
					}
				};
				eprintln!("subscribe: the broadcast came back and {failure}; waiting on");
				let stage = self.search(ended, Some(broadcast));
				self.wait = Some(on(stage));
				Ok(false)
			}
			Stage::Dial {
				ended,
				attempts,
				backoff,
				search,
			} => {
				if !search.is_finished() {
					self.wait = Some(on(Stage::Dial {
						ended,
						attempts,
						backoff,
						search,
					}));
					return Ok(false);
				}
				let opened = search.await.unwrap_or_else(|err| Err(err.to_string()));
				let failure = match opened {
					Ok(opened) => match self
						.resume(
							opened.broadcast,
							opened.renditions,
							Some(opened.dialed),
							ended,
						)
						.await
					{
						Ok(()) => {
							moq_core::order::report_reconnect(
								attempts,
								began.elapsed().as_millis() as u64,
								&why,
							);
							return Ok(true);
						}
						Err(Resume::Refused(err)) => return Err(Mend::Refused(err)),
						Err(Resume::Again(err)) => err,
					},
					Err(err) => err,
				};
				let left = window.saturating_sub(began.elapsed());
				if left.is_zero() {
					return Err(Mend::Spent(format!(
						"{why}; no new session took the tracks up within {}s ({attempts} tries, \
						 the last: {failure})",
						window.as_secs()
					)));
				}
				eprintln!("subscribe: connecting again: {failure}; trying again");
				let search = tokio::task::spawn_local(dial(
					self.params.clone(),
					backoff.min(left),
					began,
					window,
				));
				self.wait = Some(on(Stage::Dial {
					ended,
					attempts: attempts + 1,
					backoff: (backoff * 2).min(RECONNECT_MAX),
					search,
				}));
				Ok(false)
			}
		}
	}

	/// Takes every track up on a broadcast that came back: on a new
	/// session (`dialed`), or announced again on this one. The old
	/// readers answer into a channel nobody reads any more, so nothing of
	/// them reaches the holds.
	///
	/// What the reader does next depends on whether the publisher is the
	/// one it was reading. A publisher whose own session was replaced
	/// carries on: its group numbers and its clock went on meanwhile, so
	/// each hold is told its track restarted, which gives up the hole
	/// between the two at once, and its packets go on out as they come.
	/// One that started again numbers its groups from nothing and its
	/// clock with them: what each track still holds of the old numbering
	/// goes out first, each track joins the new numbering afresh, and the
	/// timeline shifts the new media to follow the old (see
	/// [`moq_core::restart`]). It is the same publisher when the
	/// broadcast's route starts at the same origin; a publisher that
	/// finished its tracks never is. Where the wire carries no route, a
	/// track's first group (see [`moq_core::subscribe::started_again`])
	/// or the first media packet behind where its track had got says it
	/// started again.
	async fn resume(
		&mut self,
		broadcast: moq_net::broadcast::Consumer,
		renditions: Vec<moq_core::catalog::Rendition>,
		dialed: Option<Dialed>,
		ended: Option<Ended>,
	) -> Result<(), Resume> {
		let publisher = publisher_of(&broadcast);
		if ended == Some(Ended::Finished) && publisher.is_some() && publisher == self.publisher {
			return Err(Resume::Again(
				"the relay still announces the broadcast whose tracks finished".to_string(),
			));
		}
		let seen = match (ended, self.publisher, publisher) {
			(Some(Ended::Finished), _, _) => Some(Seen::Finished),
			(_, Some(was), Some(now)) if was != now => Some(Seen::Replaced),
			_ => None,
		};
		let carried_on = seen.is_none() && publisher.is_some() && publisher == self.publisher;
		let mut entries = Vec::with_capacity(self.renditions.len());
		for rendition in &self.renditions {
			let Some(now) = renditions
				.iter()
				.find(|candidate| candidate.name == rendition.name)
			else {
				return Err(Resume::Refused(format!(
					"the broadcast came back without track '{}'",
					rendition.name
				)));
			};
			if let Some(what) = moq_core::catalog::changed(&rendition.entry, now) {
				return Err(Resume::Refused(format!(
					"the broadcast came back with {what} changed on track '{}': its publisher \
					 started again with other settings",
					rendition.name
				)));
			}
			entries.push(now.clone());
		}
		let (sender, frames) = mpsc::unbounded_channel::<Wire>();
		let mut opening = Vec::with_capacity(self.renditions.len());
		for rendition in &self.renditions {
			let subscribing =
				subscribe_track(&broadcast, &rendition.name, Start::Live).map_err(Resume::Again)?;
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
				broadcast.clone(),
				name,
				subscriber,
				sender.clone(),
			));
		}
		match (seen, carried_on) {
			(Some(_), _) => self.timeline.resume(seen),
			(None, false) => self.timeline.resume(None),
			(None, true) => {}
		}
		for (index, entry) in entries.into_iter().enumerate() {
			if seen.is_some() {
				self.let_out_old(index).map_err(Resume::Refused)?;
			}
			let rendition = &mut self.renditions[index];
			// The same stream in all a decoder reads. Its fragments are read
			// by the init segment that came back, which may number its track
			// or lay out its defaults otherwise.
			if entry.init != rendition.entry.init {
				if let Body::Media(_) | Body::Text { .. } = rendition.body {
					let track =
						ffrwd_bmff::track::Track::from_init(&entry.init).map_err(|err| {
							Resume::Refused(format!(
								"rendition '{}': init segment: {err}",
								rendition.name
							))
						})?;
					match &mut rendition.body {
						Body::Text { track: was, .. } => *was = track,
						body => *body = Body::Media(track),
					}
				}
			}
			rendition.entry = entry;
			rendition.queue.fetching = None;
			rendition.replay = None;
			rendition.replaying = None;
			match seen {
				Some(seen) => {
					rendition.resumed = false;
					if seen == Seen::Finished {
						rendition.replay = rendition.last_first.clone();
					}
				}
				None => {
					rendition.restart();
					rendition.resumed = !carried_on;
				}
			}
		}
		if let Some(dialed) = dialed {
			self.driver.abort();
			self.endpoint = dialed.endpoint;
			self.session = Some(dialed.session);
			self.driver = dialed.driver;
			self.announced = Some(dialed.announced);
		}
		self.broadcast = broadcast;
		self.publisher = publisher;
		self.sender = sender;
		self.frames = frames;
		self.finished = 0;
		self.asking.clear();
		Ok(())
	}

	/// Every held group whose turn has come, onto its track's pad, in
	/// sequence, by way of the timeline. `last` releases what is left
	/// whatever the sequence says: nothing more is coming. True when
	/// packets went out.
	fn release(&mut self, pads: &mut [Vec<Packet>], last: bool) -> Result<bool, String> {
		let hold = self.hold;
		let asking = &mut self.asking;
		let mut ask = Vec::new();
		let mut demuxed: Vec<Vec<Packet>> =
			(0..self.renditions.len()).map(|_| Vec::new()).collect();
		for (index, (rendition, packets)) in self
			.renditions
			.iter_mut()
			.zip(demuxed.iter_mut())
			.enumerate()
		{
			while let Some((_, frames)) = rendition.queue.take(hold, last, &mut ask) {
				demux(rendition, index, frames, packets)?;
			}
			asking.extend(ask.drain(..).map(|sequence| (index, sequence)));
			// At the end every cue still on screen has its final row.
			if let (true, Body::Text { track, cues }) = (last, &mut rendition.body) {
				let timescale = track.timescale;
				packets.extend(cues.finish().iter().map(|row| text_packet(row, timescale)));
			}
		}
		let now = std::time::Instant::now();
		for (index, packets) in demuxed.into_iter().enumerate() {
			for packet in packets {
				let stamp = stamp_of(&packet);
				self.timeline
					.push(index, stamp, packet.keyframe, packet, now);
			}
		}
		// At the end nothing more is coming, so what a resume held goes out
		// as it stands.
		let (least, most) = match last {
			true => (Duration::ZERO, Duration::ZERO),
			false => (moq_core::restart::SETTLE_LEAST, moq_core::restart::SETTLE),
		};
		self.timeline.settle(now, least, most);
		for (index, rendition) in self.renditions.iter_mut().enumerate() {
			rendition.queue.note_behind(self.timeline.behind(index));
		}
		let out = self.timeline.take_ready();
		let any = !out.is_empty();
		// The publisher's media going on after a still: the data and text
		// ports' marks carry on from the earliest of it.
		if let Some(holding) = self.holding.as_mut() {
			let first = out
				.iter()
				.filter(|(index, _, _)| {
					holding.stills.kind(*index) != still::Kind::Marks
						&& holding.stills.after_still(*index)
				})
				.min_by_key(|(index, stamp, _)| {
					ffrwd_bmff::time::rescale(
						stamp.pts.min(stamp.dts),
						u64::from(self.renditions[*index].timescale()),
						1_000_000,
					)
				});
			if let Some((index, stamp, _)) = first {
				holding.stills.media_went_on(*index, *stamp, now);
			}
		}
		// Whether the publisher's media went on here, which ends a spell of
		// stills.
		let mut went_on = false;
		for (index, stamp, mut packet) in out {
			// A cue row carries its times inside it as well, and they move
			// with the shift its pts gets. A progress mark carries nothing.
			if let Body::Text { track, .. } = &self.renditions[index].body {
				if stamp.pts != packet.pts && !packet.data.is_empty() {
					let seconds = (stamp.pts - packet.pts) as f64 / f64::from(track.timescale.max(1));
					packet.data = moq_core::text::shifted(&packet.data, seconds)?;
				}
			}
			packet.pts = stamp.pts;
			packet.dts = packet.dts.map(|_| stamp.dts);
			if let Some(holding) = self.holding.as_mut() {
				holding.carry_on(index, &packet, &mut pads[index], now);
				went_on |= self.renditions[index].media();
			}
			pads[index].push(packet);
		}
		// A restart decided here says how long the still it ends was held.
		let mut held = None;
		if let (true, Some(holding)) = (went_on, self.holding.as_mut()) {
			if let Some(spell) = holding.spell.take() {
				spell.say("the publisher's media goes on");
				held = Some((spell.began.elapsed(), spell.pictures + spell.frames));
			}
		}
		for restart in self.timeline.take_restarts() {
			moq_core::order::report_restart(&restart, held.take());
			match restart.seen {
				Seen::Overtaken => eprintln!(
					"subscribe: the publisher's media came back behind the stills, {:.1}s after its \
					 last packet; its timestamps move by {:+.3}s to follow them",
					restart.away.as_secs_f64(),
					restart.shift_us as f64 / 1e6
				),
				seen => eprintln!(
					"subscribe: the publisher started again ({}); the stream was away {:.1}s, and \
					 its timestamps move by {:+.3}s to follow on",
					seen.as_str(),
					restart.away.as_secs_f64(),
					restart.shift_us as f64 / 1e6
				),
			}
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
	frames
		.iter()
		.find_map(|frame| frame_decides(rendition, frame))
		== Some(true)
}

/// What one frame says about whether a decoder can start at its group:
/// the first frame with a sample in it decides, and one with none - a
/// fragment carrying nothing for this track, a legacy frame that is a pts
/// alone - says nothing. A frame that cannot be read says no.
fn frame_decides(rendition: &Rendition, frame: &Received) -> Option<bool> {
	match &rendition.body {
		Body::Data { .. } | Body::Text { .. } => Some(true),
		Body::Legacy(codec) => match legacy_sample(rendition, *codec, &frame.payload) {
			Ok(Some((_, _, keyframe))) => Some(!rendition.video || keyframe),
			Ok(None) => None,
			Err(_) => Some(false),
		},
		Body::Media(track) => match track.fragment_samples(&frame.payload) {
			Ok(samples) => samples
				.first()
				.map(|sample| !rendition.video || sample.keyframe),
			Err(_) => Some(false),
		},
	}
}

/// A packet's times as the timeline counts them.
fn stamp_of(packet: &Packet) -> Stamp {
	Stamp {
		pts: packet.pts,
		dts: packet.dts.unwrap_or(packet.pts),
		duration: packet.duration.unwrap_or(0),
	}
}

/// One group's frames, as the hold hands them on, into packets, and the
/// first frame of the group kept.
fn demux(
	rendition: &mut Rendition,
	index: usize,
	frames: Vec<Received>,
	packets: &mut Vec<Packet>,
) -> Result<(), String> {
	for frame in frames {
		if frame.frame_in_group == 0 {
			rendition.last_first = Some(moq_core::subscribe::FirstFrame::of(&frame));
		}
		match rendition.body {
			Body::Data { timescale } => take_message(timescale, index, frame, packets)?,
			Body::Text { .. } => take_text(rendition, index, &frame, packets)?,
			Body::Media(_) => take_fragment(rendition, index, &frame.payload, packets)?,
			Body::Legacy(codec) => take_legacy(rendition, codec, index, frame, packets)?,
		};
	}
	Ok(())
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

/// One text fragment's cue rows onto a pad: each sample's cues, followed
/// from the samples before it, as the rows [`moq_core::text::Reassembly`]
/// makes of them, each at the time it applies in the track's own ticks.
/// Each sample ends with an empty message at its end, a sample with nothing
/// shown as well, which a consumer pairing the rows by time reads as how
/// far the track has got.
fn take_text(
	rendition: &mut Rendition,
	index: usize,
	frame: &Received,
	pad: &mut Vec<Packet>,
) -> Result<bool, String> {
	let Body::Text { track, cues } = &mut rendition.body else {
		unreachable!("a text track's frames are text fragments");
	};
	let timescale = track.timescale;
	let mut any = false;
	for sample in track
		.fragment_samples(&frame.payload)
		.map_err(|err| format!("track {index}, group {}: {err}", frame.group))?
	{
		let bytes = ffrwd_bmff::fragment::sample_bytes(&frame.payload, 0, &sample)
			.map_err(|err| format!("track {index}, group {}: {err}", frame.group))?;
		let shown = ffrwd_bmff::webvtt::cues(bytes)
			.map_err(|err| format!("track {index}, group {}: {err}", frame.group))?;
		for row in cues.sample(sample.pts, sample.duration, shown) {
			pad.push(text_packet(&row, timescale));
		}
		let end = sample.pts + sample.duration.max(0);
		pad.push(Packet {
			pts: end,
			dts: Some(end),
			duration: None,
			keyframe: true,
			data: Vec::new(),
		});
		any = true;
	}
	Ok(any)
}

/// One cue row as the packet its port hands on.
fn text_packet(row: &moq_core::text::Out, timescale: u32) -> Packet {
	Packet {
		pts: row.pts,
		dts: Some(row.pts),
		duration: None,
		keyframe: true,
		data: row.json(0, timescale),
	}
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

/// The catalog's relation: a row per rendition row, with the name, the
/// codecs and the bandwidth of the media on it, and each track's index
/// into it. A row's name and bandwidth are its first media track's, which
/// is its video when it has one: the bitrate a reader picks a rung by.
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
				"bandwidth": named.and_then(|t| t.rendition.bandwidth),
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
			drop(opened.dialed.announced);
			drop(opened.dialed.session);
			opened.dialed.endpoint.wait_idle().await;
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
				CodedFormat::Data if track.text => {
					Output::rows(name).schema_json(moq_core::text::ROW_SCHEMA)
				}
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
			let timeline = Timeline::new(
				renditions
					.iter()
					.map(|rendition| (rendition.timescale(), rendition.media())),
			);
			let holding = params.still.then(|| Holding::new(&renditions));
			Ok::<_, String>((
				ports,
				Reader {
					endpoint: opened.dialed.endpoint,
					session: Some(opened.dialed.session),
					driver: opened.dialed.driver,
					announced: Some(opened.dialed.announced),
					lost: None,
					params: params.clone(),
					renditions,
					frames,
					publisher: publisher_of(&opened.broadcast),
					broadcast: opened.broadcast,
					sender,
					timeline,
					finished: 0,
					asking: Vec::new(),
					hold: Hold::new(hold_ms(&params, start), params.hold_mib, params.join_ms),
					left: None,
					wait: None,
					holding,
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
			if let Some(wait) = done.reader.wait.take() {
				wait.stage.abort();
			}
			done.reader.end_stills();
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
/// Each frame goes out the moment the stream hands it over, and the end
/// of its group once the stream says the group has been read to its end.
/// A frame is whole on its own, so nothing waits for the rest of its
/// group: before 0.9.2 a group went out whole at its end, which held a
/// GOP of pictures back, and a message waited for its group's end.
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
	// The reader holds no sender of its own once this returns, so the
	// end of the run is counted rather than read off a closed channel:
	// a fetch for a missing group answers on the same one. A track that
	// was lost has said so already.
	if read_groups(index, broadcast, name, subscriber, &sender).await {
		let _ = sender.send(Wire::Finished);
	}
}

async fn read_groups(
	index: usize,
	broadcast: moq_net::broadcast::Consumer,
	name: String,
	subscriber: moq_net::track::Subscriber,
	sender: &mpsc::UnboundedSender<Wire>,
) -> bool {
	let mut stream = moq_core::subscribe::FrameStream::new(subscriber);
	let mut attempts = 0u32;
	loop {
		// Each frame goes on as it arrives, and the end of its group after
		// it. A group the stream broke off in is left to the hold, which is
		// told the track restarted and takes the group up again if the new
		// subscription hands it over from its start.
		let mut open: Option<u64> = None;
		let failure = loop {
			match stream.step().await {
				Ok(Some(Step::Frame(frame))) => {
					// A group whose end was never read, which only a stream
					// that moved on without one can hand over.
					if let Some(sequence) = open.take_if(|sequence| *sequence != frame.group) {
						if sender.send(Wire::End(index, sequence)).is_err() {
							return false;
						}
					}
					open = Some(frame.group);
					if sender.send(Wire::Frame(index, frame)).is_err() {
						return false;
					}
				}
				Ok(Some(Step::End(ended))) => {
					if open.take_if(|sequence| *sequence == ended).is_some()
						&& sender.send(Wire::End(index, ended)).is_err()
					{
						return false;
					}
				}
				// The publisher finished the track: its last group ends and
				// this reader is done.
				Ok(None) => {
					if let Some(sequence) = open.take() {
						let _ = sender.send(Wire::End(index, sequence));
					}
					return true;
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
			return false;
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
					return false;
				}
				stream = moq_core::subscribe::FrameStream::new(subscriber);
			}
			// The subscription could not even be asked for again, which
			// is what a session the relay has dropped looks like from
			// here.
			Err(err) => {
				let _ = sender.send(Wire::Lost(index, err));
				return false;
			}
		}
	}
}

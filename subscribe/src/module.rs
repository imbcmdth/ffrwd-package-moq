wit_bindgen::generate!({
	path: "wit",
	world: "packet-source-module",
});

use std::cell::RefCell;
use std::time::Duration;

use bytes::Bytes;
use exports::ffrwd::av::packet_source::{
	Catalog, Guest, Meta, PadPackets, RenditionMeta, SourceTrack, StreamInfo,
};
use ffrwd::av::types::{CodedAudio, CodedFormat, CodedStream, CodedVideo, Packet, Rational};
use moq_core::order::{Hold, Queue};
use moq_core::subscribe::Start;
use serde::Deserialize;
use tokio::sync::mpsc;

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP, and a path the session opens under, which is where a relay's own address carries a token: https://relay.example/<JWT>"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none, and a token already in the relay URL needs none"},"hold_ms":{"type":"integer","minimum":0,"default":30000,"description":"how long a hole in a track's group sequence is held open for the group that would fill it, in milliseconds; the default is the subscription's own latency window, which is as long as the relay may still serve it"},"hold_mib":{"type":"integer","minimum":1,"default":64,"description":"how much one track holds meanwhile, in MiB; 64 covers a 30s window up to about 17 Mbit/s"},"join_ms":{"type":"integer","minimum":0,"default":2000,"description":"how long a joining reader waits for a lower group sequence before it fixes its cursor on a backlog join, in milliseconds; 0 starts at the first group that arrives, and a live join never waits at all"},"start":{"type":"string","enum":["live","backlog"],"default":"live","description":"where to join a broadcast already running: 'live' at the publisher's newest group, starting at the first group a decoder can begin at - a keyframe group for video, any group for audio - or 'backlog' at the oldest group the relay still holds, which reads the whole cache before the first packet comes out"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// How long the relay gets to announce the broadcast, and then to hand
/// over its catalog, before the call gives up.
///
/// A compile-time probe wants a quick answer: a broadcast that is not
/// there is a query to fix, not something to wait on. A run wants the
/// opposite - it may well be started beside the publisher it reads, and
/// giving up on that is worse than waiting.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);

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
	/// The hold, per track: see [`moq_core::order`]. A run whose relay
	/// keeps less history than this package asks for, or one that would
	/// rather stall for less time, says so here.
	#[serde(default = "default_hold_ms")]
	hold_ms: u64,
	#[serde(default = "default_hold_mib")]
	hold_mib: u64,
	#[serde(default = "default_join_ms")]
	join_ms: u64,
	/// Where this reader joins a broadcast already running: the live
	/// edge, which is the default, or the backlog; see
	/// [`moq_core::subscribe::Start`].
	#[serde(default = "default_start")]
	start: String,
}

fn default_hold_ms() -> u64 {
	moq_core::order::HOLD_WAIT.as_millis() as u64
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

fn parse_params(params: &str) -> Result<(Params, Start), String> {
	let params: Params = serde_json::from_str(params).map_err(|err| format!("params: {err}"))?;
	// The relay URL's path and the token argument are two spellings of
	// one field, so what the session will ask for is settled here, where
	// params are refused. `probe` reads these params at compile time, so
	// a URL and a token that disagree stop the query rather than the run.
	moq_core::relay::session_path(&params.relay, &params.token)?;
	// And so is a start nobody spells: a typo stops the query rather
	// than quietly reading the wrong end of the broadcast.
	let start = Start::parse(&params.start)?;
	Ok((params, start))
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

/// What one reader task says about its track.
enum Wire {
	/// One complete group: its sequence and its frames, each frame a
	/// complete fmp4 fragment.
	Group(usize, u64, Vec<Bytes>),
	/// The relay was asked for one group outright and would not serve
	/// it: it does not have it, so the hole waiting for it is a hole
	/// nothing will fill. Carries why.
	Gone(usize, u64, String),
	/// The track was taken up again on a fresh subscription, which
	/// starts at the live edge: whatever the hold is waiting for is not
	/// coming.
	Restarted(usize),
	/// A track's reader has finished: the publisher ended it, or the
	/// wire broke past mending. Which track it was does not matter -
	/// what the run waits for is all of them.
	Finished,
	/// The track failed, and why.
	Failed(usize, String),
}

/// One rendition being read: how to take its fragments apart, where its
/// groups have got to, and whether its packets have started.
struct Rendition {
	track: ffrwd_bmff::track::Track,
	video: bool,
	/// The NAL length prefix this track's `avcC` declares; 0 for audio,
	/// whose frames carry no framing of their own.
	length_size: usize,
	/// Where this reader may start, and what a fragment carrying
	/// nothing for the track means.
	join: moq_core::group::Join,
	/// The group sequence this rendition's fragments arrive in.
	queue: Queue,
}

/// Every track being read, and the session under them.
struct Reader {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
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

/// The executor and the reader it drives, split so a call can block on
/// the one while the work borrows the other.
struct State {
	executor: Executor,
	reader: Reader,
}

thread_local! {
	static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

/// A dialed relay with the broadcast open: what both calls need
/// before they part ways.
struct Opened {
	endpoint: quinn::Endpoint,
	session: moq_net::Session,
	broadcast: moq_net::broadcast::Consumer,
	renditions: Vec<moq_core::catalog::Rendition>,
}

/// Connects, waits for the broadcast to be announced and reads its
/// catalog. The session driver is spawned here and runs whenever a
/// call blocks on the executor.
async fn open_broadcast(params: &Params, wait: Duration) -> Result<Opened, String> {
	let origin = moq_net::Origin::random().produce();
	let connected = moq_core::relay::dial(
		&params.relay,
		&params.cert,
		&params.token,
		moq_net::Client::new().with_subscriber(origin.clone()),
	)
	.await?;
	let driver = connected.driver;
	tokio::task::spawn_local(async move {
		let _ = driver.await;
	});

	let path = params.broadcast.as_str();
	let broadcast = tokio::time::timeout(wait, origin.consume().announced_broadcast(path))
		.await
		.map_err(|_| {
			format!(
				"broadcast '{path}' was not announced within {}s",
				wait.as_secs()
			)
		})?
		.ok_or_else(|| format!("broadcast '{path}' is not on the relay"))?;

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
		endpoint: connected.endpoint,
		session: connected.session,
		broadcast,
		renditions: moq_core::catalog::parse(&document)
			.map_err(|err| format!("broadcast '{path}': {err}"))?,
	})
}

/// One catalog rendition as the track a source publishes: its coded
/// stream read off the init segment, its geometry off the catalog.
fn source_track(
	index: usize,
	rendition: &moq_core::catalog::Rendition,
) -> Result<(SourceTrack, ffrwd_bmff::track::Track), String> {
	let track = ffrwd_bmff::track::Track::from_init(&rendition.init)
		.map_err(|err| format!("rendition '{}': init segment: {err}", rendition.name))?;
	let time_base = Rational {
		num: 1,
		den: track.timescale as i32,
	};
	let config = &track.entry.config;
	let (kind, codec, format, extradata, profile, level) = match (&rendition.kind, &track.entry.kind)
	{
		(moq_core::catalog::Kind::Video { width, height }, b"avc1" | b"avc3") => {
			let extradata = ffrwd_nal::config::avcc_to_annexb_extradata(config)
				.map_err(|err| format!("rendition '{}': its avcC: {err}", rendition.name))?;
			let (profile, level) = match ffrwd_nal::sps::profile_level(config) {
				Some((profile, level)) => (Some(i32::from(profile)), Some(i32::from(level))),
				None => (None, None),
			};
			(
				"video",
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
			"audio",
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
			info: StreamInfo {
				index: index as u32,
				kind: kind.to_string(),
				codec: codec.to_string(),
				// A broadcast runs as long as its publisher does.
				duration: None,
				tags: vec![],
				time_base,
			},
			row: rendition.row,
			rendition: RenditionMeta {
				name: Some(rendition.name.clone()),
				bandwidth: None,
				codecs: Some(rendition.codec.clone()),
				language: None,
			},
		},
		track,
	))
}

/// The renditions `order` names, as catalog indices: what `open` was told
/// to pull. An index the broadcast's catalog does not carry is refused by
/// name, before anything is subscribed to.
fn chosen(
	renditions: &[moq_core::catalog::Rendition],
	tracks: &[u32],
) -> Result<Vec<usize>, String> {
	tracks
		.iter()
		.map(|index| {
			let index = *index as usize;
			if index >= renditions.len() {
				return Err(format!(
					"this broadcast's catalog names {} rendition(s), so track {index} is not one of them",
					renditions.len()
				));
			}
			Ok(index)
		})
		.collect()
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
		let (track, demux) = source_track(index, rendition)?;
		let video = matches!(rendition.kind, moq_core::catalog::Kind::Video { .. });
		let length_size = match video {
			true => ffrwd_nal::config::avcc_length_size(&demux.entry.config),
			false => 0,
		};
		tracks.push(track);
		readers.push(Rendition {
			track: demux,
			video,
			length_size,
			join: match video {
				true => moq_core::group::Join::video(),
				false => moq_core::group::Join::audio(),
			},
			queue: Queue::new(rendition.name.clone(), start),
		});
	}
	Ok((
		Catalog {
			tracks,
			// A relay broadcast ends when its publisher stops, which
			// nothing here can know before it happens.
			bounded: false,
		},
		readers,
	))
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
			Wire::Restarted(index) => self.renditions[index].queue.restarted = true,
			Wire::Finished => self.finished += 1,
			Wire::Failed(index, err) => {
				let name = &self.renditions[index].queue.name;
				return Err(format!("track '{name}': {err}"));
			}
		}
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
				for fragment in frames {
					any |= take_fragment(rendition, index, &fragment, pad)?;
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
/// either way, and the next one is read.
fn decodable(rendition: &Rendition, frames: &[Bytes]) -> bool {
	for fragment in frames {
		let Ok(samples) = rendition.track.fragment_samples(fragment) else {
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
	let samples = rendition
		.join
		.playable(&rendition.track, fragment)
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

struct Subscribe;

impl Guest for Subscribe {
	fn describe() -> Meta {
		Meta {
			name: "subscribe".to_string(),
			version: "0.5.0".to_string(),
			params_schema: PARAMS_SCHEMA.to_string(),
			// A packet source has no row channel in `ffrwd:av`, so this
			// is the shape of the rows that go to stderr instead; see
			// [`moq_core::order::ROWS_SCHEMA`].
			rows_schema: moq_core::order::ROWS_SCHEMA.to_string(),
			// No decoded payload ever crosses, so no format list fills in.
			pixel_formats: vec![],
			sample_formats: vec![],
			sample_rates: vec![],
			channel_counts: vec![],
			rows_language: vec![],
		}
	}

	fn probe(params: String) -> Result<Catalog, String> {
		let (params, start) = parse_params(&params)?;
		let executor = Executor::new()?;
		executor.enter(async {
			let opened = open_broadcast(&params, PROBE_TIMEOUT).await?;
			// The whole catalog, which is what makes the rows known at
			// compile time. It is built before the session closes, so a
			// rendition this module cannot read is named here rather
			// than at run time.
			let every = (0..opened.renditions.len()).collect::<Vec<_>>();
			let (catalog, _) = catalog_of(&opened.renditions, &every, start)?;
			drop(opened.broadcast);
			drop(opened.session);
			opened.endpoint.wait_idle().await;
			Ok(catalog)
		})
	}

	fn open(params: String, tracks: Vec<u32>) -> Result<Catalog, String> {
		let (params, start) = parse_params(&params)?;
		let executor = Executor::new()?;
		let (catalog, reader) = executor.enter(async {
			let opened = open_broadcast(&params, OPEN_TIMEOUT).await?;
			let order = chosen(&opened.renditions, &tracks)?;
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
			let (catalog, renditions) = catalog_of(&opened.renditions, &order, start)?;
			// Only the tracks this run was told to pull are subscribed
			// to, and each becomes the pad at its place in `order`.
			//
			// Every one of them is REGISTERED before any is awaited, which
			// is what keeps a ladder's rungs together: a live subscription
			// starts at whatever group was the publisher's latest when it
			// was accepted, so subscribing a track and then waiting out a
			// round trip before subscribing the next would join each rung
			// a round trip further on. moq-net registers the subscription
			// in `Consumer::subscribe` and only the wait for the track's
			// info is deferred to the await, so these go out in one flight
			// and every track joins the same edge.
			let (sender, frames) = mpsc::unbounded_channel::<Wire>();
			let mut opening = Vec::with_capacity(order.len());
			for &index in &order {
				// Subscribed once here, so a name the broadcast does not
				// carry is refused by `open` rather than mid-run.
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
			Ok::<_, String>((
				catalog,
				Reader {
					endpoint: opened.endpoint,
					session: Some(opened.session),
					renditions,
					frames,
					broadcast: opened.broadcast,
					sender,
					finished: 0,
					asking: Vec::new(),
					hold: Hold::new(params.hold_ms, params.hold_mib, params.join_ms),
				},
			))
		})?;

		STATE.with(|state| {
			*state.borrow_mut() = Some(State { executor, reader });
		});
		Ok(catalog)
	}

	fn next() -> Result<Option<Vec<PadPackets>>, String> {
		STATE.with(|state| {
			let mut holder = state.borrow_mut();
			let State { executor, reader } = holder.as_mut().ok_or("next called before open")?;
			let pulled = executor.enter(reader.pull())?;
			if pulled.is_none() {
				// Every track finished: close the session rather than
				// leave it open for a call that will not come.
				let mut done = holder.take().expect("the reader is here");
				done.executor.enter(async {
					drop(done.reader.session.take());
					done.reader.endpoint.wait_idle().await;
				});
			}
			Ok(pulled)
		})
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
			frames.push(frame.payload);
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
/// Frames are gathered per group and the group goes out whole: the
/// stream reads a group to completion before the next, so the first
/// frame of another group is what says the one before it is done.
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
		let mut open: Option<(u64, Vec<Bytes>)> = None;
		let failure = loop {
			match stream.next().await {
				Ok(Some(frame)) => match &mut open {
					Some((sequence, frames)) if *sequence == frame.group => {
						frames.push(frame.payload)
					}
					_ => {
						if let Some((sequence, frames)) = open.take() {
							if sender.send(Wire::Group(index, sequence, frames)).is_err() {
								return;
							}
						}
						open = Some((frame.group, vec![frame.payload]));
					}
				},
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
		// not stay up is what stops the run.
		attempts += 1;
		if attempts > TRACK_RETRIES {
			let _ = sender.send(Wire::Failed(index, failure));
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
			Err(err) => {
				let _ = sender.send(Wire::Failed(index, format!("track '{name}': {err}")));
				return;
			}
		}
	}
}

export!(Subscribe);


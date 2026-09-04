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
use serde::Deserialize;
use tokio::sync::mpsc;

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// How long the relay gets to announce the broadcast, and then to hand
/// over its catalog, before the call gives up.
///
/// A compile-time probe wants a quick answer: a broadcast that is not
/// there is a query to fix, not something to wait on. A run wants the
/// opposite - it may well be started beside the publisher it reads, and
/// giving up on that is worse than waiting.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a hole in a track's group sequence is held open for the
/// group that would fill it. Groups travel over parallel streams and a
/// relay serving a backlog sends them newest-first, so arrival order is
/// nothing like sequence order; a group that has not arrived by this
/// much later was lost rather than reordered, and the ones behind it go
/// out without it.
const GAP_WAIT: Duration = Duration::from_secs(3);

/// How many groups one track holds meanwhile, whatever the clock says.
const GAP_HOLD: usize = 256;

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
}

fn parse_params(params: &str) -> Result<Params, String> {
	serde_json::from_str(params).map_err(|err| format!("params: {err}"))
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
	/// The track failed, and why.
	Failed(usize, String),
}

/// One rendition being read: how to take its fragments apart, where
/// its groups have got to, and whether its packets have started.
struct Rendition {
	track: moq_core::demux::Track,
	video: bool,
	/// False until a video track's first sync sample; a subscription
	/// begins at the group in flight, and the samples before that
	/// keyframe are a group no decoder can start at.
	started: bool,
	/// Completed groups waiting for the one before them.
	pending: std::collections::BTreeMap<u64, Vec<Bytes>>,
	/// The next group in sequence; None until the first goes out.
	cursor: Option<u64>,
	/// When the hole at the head of `pending` opened; see [`GAP_WAIT`].
	gap_since: Option<std::time::Instant>,
}

/// Every track being read, and the session under them.
struct Reader {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
	renditions: Vec<Rendition>,
	frames: mpsc::UnboundedReceiver<Wire>,
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
) -> Result<(SourceTrack, moq_core::demux::Track), String> {
	let track = moq_core::demux::Track::read(&rendition.init)
		.map_err(|err| format!("rendition '{}': init segment: {err}", rendition.name))?;
	let time_base = Rational {
		num: 1,
		den: track.timescale as i32,
	};
	let (kind, codec, format, extradata, profile, level) = match (&rendition.kind, &track.media) {
		(
			moq_core::catalog::Kind::Video { width, height },
			moq_core::demux::Media::Video { avcc, .. },
		) => {
			let extradata = moq_core::avc::avcc_to_annexb_extradata(avcc)
				.map_err(|err| format!("rendition '{}': {err}", rendition.name))?;
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
				avcc.get(1).map(|byte| i32::from(*byte)),
				avcc.get(3).map(|byte| i32::from(*byte)),
			)
		}
		(
			moq_core::catalog::Kind::Audio {
				sample_rate,
				channels,
			},
			moq_core::demux::Media::Audio { asc, .. },
		) => (
			"audio",
			"aac",
			CodedFormat::Audio(CodedAudio {
				sample_rate: *sample_rate,
				channels: *channels,
				channel_layout: None,
			}),
			asc.clone(),
			None,
			None,
		),
		_ => {
			return Err(format!(
				"rendition '{}' is described as one kind and packaged as another",
				rendition.name
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
) -> Result<(Catalog, Vec<Rendition>), String> {
	let mut tracks = Vec::with_capacity(order.len());
	let mut readers = Vec::with_capacity(order.len());
	for &index in order {
		let rendition = &renditions[index];
		let (track, demux) = source_track(index, rendition)?;
		let video = matches!(rendition.kind, moq_core::catalog::Kind::Video { .. });
		tracks.push(track);
		readers.push(Rendition {
			track: demux,
			video,
			started: !video,
			pending: std::collections::BTreeMap::new(),
			cursor: None,
			gap_since: None,
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
			produced |= self.release(&mut pads, false)?;
			if produced {
				break;
			}
			// Nothing ready: wait for the wire. Every reader task is
			// gone once every track has finished, which closes this and
			// makes whatever is still held the last of it.
			match self.frames.recv().await {
				Some(wire) => self.hold(wire)?,
				None => {
					produced |= self.release(&mut pads, true)?;
					return Ok(produced.then(|| into_pads(pads)));
				}
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
			idle = if arrived { 0 } else { idle + 1 };
			if idle >= IDLE_TURNS {
				break;
			}
		}
		Ok(Some(into_pads(pads)))
	}

	/// Files one completed group under its track, or fails the pull when
	/// a track did. A group behind one already sent is dropped: its place
	/// in the sequence has passed.
	fn hold(&mut self, wire: Wire) -> Result<(), String> {
		let (index, sequence, frames) = match wire {
			Wire::Group(index, sequence, frames) => (index, sequence, frames),
			Wire::Failed(index, err) => return Err(format!("track {index}: {err}")),
		};
		let rendition = &mut self.renditions[index];
		if rendition.cursor.is_some_and(|next| sequence < next) {
			return Ok(());
		}
		rendition.pending.insert(sequence, frames);
		Ok(())
	}

	/// Every held group whose turn has come, onto its track's pad, in
	/// sequence. `last` releases what is left whatever the sequence says:
	/// nothing more is coming. True when packets went out.
	fn release(&mut self, pads: &mut [Vec<Packet>], last: bool) -> Result<bool, String> {
		let mut any = false;
		for (index, (rendition, pad)) in self.renditions.iter_mut().zip(pads.iter_mut()).enumerate()
		{
			loop {
				let Some((&oldest, _)) = rendition.pending.iter().next() else {
					rendition.gap_since = None;
					break;
				};
				let ready = match rendition.cursor {
					// The first group out is where the sequence starts.
					None => true,
					Some(next) if oldest == next => true,
					Some(_) => {
						let since = *rendition
							.gap_since
							.get_or_insert_with(std::time::Instant::now);
						last || since.elapsed() >= GAP_WAIT || rendition.pending.len() > GAP_HOLD
					}
				};
				if !ready {
					break;
				}
				let frames = rendition.pending.remove(&oldest).expect("just found");
				rendition.cursor = Some(oldest + 1);
				rendition.gap_since = None;
				for fragment in frames {
					any |= take_fragment(rendition, index, &fragment, pad)?;
				}
			}
		}
		Ok(any)
	}
}

fn into_pads(pads: Vec<Vec<Packet>>) -> Vec<PadPackets> {
	pads.into_iter()
		.map(|packets| PadPackets { packets })
		.collect()
}

/// One fmp4 fragment's samples onto a pad. True when any went out; a
/// video track's samples before its first sync sample do not, since a
/// subscription joins at the group in flight and no decoder can start
/// in the middle of one.
fn take_fragment(
	rendition: &mut Rendition,
	index: usize,
	fragment: &[u8],
	pad: &mut Vec<Packet>,
) -> Result<bool, String> {
	let samples = rendition
		.track
		.samples(fragment)
		.map_err(|err| format!("track {index}: {err}"))?;
	let mut any = false;
	for sample in samples {
		if !rendition.started {
			if !sample.keyframe {
				continue;
			}
			rendition.started = true;
		}
		let data = if rendition.video {
			moq_core::avc::avcc_to_annexb(&sample.data, rendition.track.length_size())
				.map_err(|err| format!("track {index}: {err}"))?
		} else {
			sample.data
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
			version: "0.4.0".to_string(),
			params_schema: PARAMS_SCHEMA.to_string(),
			// A source emits no rows.
			rows_schema: String::new(),
			// No decoded payload ever crosses, so no format list fills in.
			pixel_formats: vec![],
			sample_formats: vec![],
			sample_rates: vec![],
			channel_counts: vec![],
			rows_language: vec![],
		}
	}

	fn probe(params: String) -> Result<Catalog, String> {
		let params = parse_params(&params)?;
		let executor = Executor::new()?;
		executor.enter(async {
			let opened = open_broadcast(&params, PROBE_TIMEOUT).await?;
			// The whole catalog, which is what makes the rows known at
			// compile time. It is built before the session closes, so a
			// rendition this module cannot read is named here rather
			// than at run time.
			let every = (0..opened.renditions.len()).collect::<Vec<_>>();
			let (catalog, _) = catalog_of(&opened.renditions, &every)?;
			drop(opened.broadcast);
			drop(opened.session);
			opened.endpoint.wait_idle().await;
			Ok(catalog)
		})
	}

	fn open(params: String, tracks: Vec<u32>) -> Result<Catalog, String> {
		let params = parse_params(&params)?;
		let executor = Executor::new()?;
		let (catalog, reader) = executor.enter(async {
			let opened = open_broadcast(&params, OPEN_TIMEOUT).await?;
			let order = chosen(&opened.renditions, &tracks)?;
			// One line per run naming what this source pulls, so a run can
			// be read back against the catalog it narrowed.
			eprintln!(
				"subscribe: pulling {} of {} rendition(s): {}",
				order.len(),
				opened.renditions.len(),
				order
					.iter()
					.map(|&index| format!("{index}={}", opened.renditions[index].name))
					.collect::<Vec<_>>()
					.join(", ")
			);
			let (catalog, renditions) = catalog_of(&opened.renditions, &order)?;
			// Only the tracks this run was told to pull are subscribed
			// to, and each becomes the pad at its place in `order`.
			let (sender, frames) = mpsc::unbounded_channel();
			for (pad, &index) in order.iter().enumerate() {
				let rendition = &opened.renditions[index];
				// Subscribed once here, so a name the broadcast does not
				// carry is refused by `open` rather than mid-run.
				let subscriber = subscribe_track(&opened.broadcast, &rendition.name, true).await?;
				tokio::task::spawn_local(read_track(
					pad,
					opened.broadcast.clone(),
					rendition.name.clone(),
					subscriber,
					sender.clone(),
				));
			}
			// The last sender here, so the receiver closes once every
			// reader task has finished its track.
			drop(sender);
			Ok::<_, String>((
				catalog,
				Reader {
					endpoint: opened.endpoint,
					session: Some(opened.session),
					renditions,
					frames,
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

/// Subscribes to one named track of a broadcast, asking for the whole
/// backlog the publisher still holds, or - once that has been refused -
/// for the latest group and whatever follows it.
async fn subscribe_track(
	broadcast: &moq_net::broadcast::Consumer,
	name: &str,
	backlog: bool,
) -> Result<moq_net::track::Subscriber, String> {
	let subscription = if backlog {
		moq_core::subscribe::from_start()
	} else {
		moq_core::subscribe::live_edge()
	};
	broadcast
		.track(name)
		.map_err(|err| format!("track '{name}': {err}"))?
		.subscribe(subscription)
		.await
		.map_err(|err| format!("track '{name}': {err}"))
}

/// One track's groups onto the shared channel until it finishes.
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
		// The backlog is asked for once. A relay that dropped the
		// subscription rather than serve it will do so again, and the
		// latest group is what it does have.
		match subscribe_track(&broadcast, &name, false).await {
			Ok(subscriber) => stream = moq_core::subscribe::FrameStream::new(subscriber),
			Err(err) => {
				let _ = sender.send(Wire::Failed(index, err));
				return;
			}
		}
	}
}

export!(Subscribe);

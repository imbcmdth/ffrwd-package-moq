wit_bindgen::generate!({
	path: "wit",
	world: "packet-module",
});

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use exports::ffrwd::av::packet_sink::{
	Arity, CodedFormat, Guest, InputStream, Meta, PacketSinkMeta, PadPackets, Processed,
};
use serde::{Deserialize, Serialize};

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"track":{"type":"string","default":"video","description":"media track name; one stream publishes it as written, several name a rendition apiece under it"},"audio_track":{"type":"string","default":"audio","description":"the audio track's name"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// One schema covers both row shapes: a group row carries `group`, the
/// trailing summary carries `groups`, and each leaves the other's
/// fields out. `pts_start`/`pts_end` are seconds of media time.
const ROWS_SCHEMA: &str = r#"{"type":"object","properties":{"track":{"type":"string"},"group":{"type":"integer"},"packets":{"type":"integer"},"bytes":{"type":"integer"},"pts_start":{"type":"number"},"pts_end":{"type":"number"},"tracks":{"type":"integer"},"groups":{"type":"integer"},"init_bytes":{"type":"integer"}},"additionalProperties":false}"#;

/// How long the session stays open after the last fragment, for the
/// wire to drain: there is no delivered signal for a subscription.
const DRAIN: Duration = Duration::from_secs(2);

/// How long a relay gets to answer the dial before init gives up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a non-latest group is kept for a backlogged subscriber. The
/// default evicts after 5s, which is not enough for one that arrives
/// mid-broadcast and asks for the catalog.
const KEEP: Duration = Duration::from_secs(30);

/// How often the publisher re-checks for its first subscriber. Nothing is
/// published before one arrives: a subscription starts at the LATEST group,
/// so anything published earlier is simply gone.
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Params {
	relay: String,
	broadcast: String,
	#[serde(default = "default_track")]
	track: String,
	#[serde(default = "default_audio_track")]
	audio_track: String,
	#[serde(default)]
	cert: String,
}

fn default_track() -> String {
	"video".into()
}

fn default_audio_track() -> String {
	"audio".into()
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
	muxer: moq_core::mux::Muxer,
	codec: String,
	media: Media,
	time_base: (i32, i32),
}

/// One track: one encoded stream, its own fmp4 muxer, and the MoQ track
/// carrying its fragments. The init segment rides inside the catalog.
struct Rendition {
	name: String,
	codec: String,
	media: Media,
	track: Option<moq_net::track::Producer>,
	muxer: moq_core::mux::Muxer,
	time_base: (i32, i32),
	init_bytes: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
}

impl Rendition {
	fn seconds(&self, ticks: i64) -> f64 {
		ticks as f64 * self.time_base.0 as f64 / self.time_base.1 as f64
	}

	/// Publishes one fragment as one MoQ frame in a group of its own,
	/// and returns the group's row.
	fn publish_fragment(&mut self, fragment: moq_core::mux::Fragment) -> Result<String, String> {
		let timestamp_us =
			moq_core::mux::ticks_to_micros(fragment.pts_min, self.time_base.0, self.time_base.1);
		let bytes_len = fragment.bytes.len() as u64;
		let row = GroupRow {
			track: self.name.clone(),
			group: self.groups,
			packets: fragment.samples,
			bytes: bytes_len,
			pts_start: self.seconds(fragment.pts_min),
			pts_end: self.seconds(fragment.pts_max),
		};
		let track = self.track.as_mut().expect("track lives until last");
		write_group(track, timestamp_us, fragment.bytes)?;
		self.groups += 1;
		self.bytes += bytes_len;
		Ok(serde_json::to_string(&row).expect("a group row serializes"))
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
				self.muxer.decoder_config(),
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
				self.muxer.decoder_config(),
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
}

struct State {
	executor: Executor,
	session: Session,
}

thread_local! {
	static STATE: RefCell<Option<State>> = const { RefCell::new(None) };
}

fn parse_params(params: &str) -> Result<Params, String> {
	serde_json::from_str(params).map_err(|err| format!("params: {err}"))
}

/// The relay URL taken apart: host and port. The scheme is
/// decorative. An IP-literal host is dialed as written; a name goes
/// through the DNS-over-HTTPS lookup in [`crate::doh`], since the
/// runner grants UDP and outgoing HTTP but no name lookup.
fn parse_relay(url: &str) -> Result<(String, u16), String> {
	let rest = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
	let rest = rest.split(['/', '?']).next().unwrap_or(rest);
	let (host, port) = match rest.rsplit_once(':') {
		Some((host, port)) if !host.is_empty() && !port.contains(']') => {
			let port: u16 = port
				.parse()
				.map_err(|_| format!("relay '{url}': '{port}' is not a port"))?;
			(host, port)
		}
		_ => (rest, 443),
	};
	let host = host.trim_start_matches('[').trim_end_matches(']');
	if host.is_empty() {
		return Err(format!("relay '{url}' names no host"));
	}
	Ok((host.to_string(), port))
}

fn unhex(hex: &str) -> Result<Vec<u8>, String> {
	let digits: Vec<u8> = hex.trim().bytes().collect();
	if digits.len() % 2 != 0 {
		return Err("cert: odd hex length".into());
	}
	digits
		.chunks_exact(2)
		.map(|pair| {
			let hi = (pair[0] as char).to_digit(16).ok_or("cert: bad hex digit")?;
			let lo = (pair[1] as char).to_digit(16).ok_or("cert: bad hex digit")?;
			Ok((hi * 16 + lo) as u8)
		})
		.collect()
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
		write_group(track, 0, document)
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
			while !self.wanted() {
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
			while !self.rendition_wanted() {
				tokio::time::sleep(POLL).await;
			}
			self.started = true;
		}

		for (index, pad) in pads.iter().enumerate() {
			let Some(rendition) = self.renditions.get_mut(index) else {
				continue;
			};
			for packet in &pad.packets {
				let closed = rendition.muxer.push(moq_core::mux::Packet {
					pts: packet.pts,
					dts: packet.dts,
					keyframe: packet.keyframe,
					data: &packet.data,
				})?;
				rendition.packets += 1;
				if let Some(fragment) = closed {
					rows.push(rendition.publish_fragment(fragment)?);
				}
			}
			// Let the driver move the frames onto the wire now, not
			// after the next call.
			tokio::task::yield_now().await;
		}

		if last {
			// Every track's last fragment goes out BEFORE any track is
			// finished: a subscriber stops at the finish, so a track told
			// it is over while its own tail is still queued loses that
			// tail. The yield is what lets the driver move them.
			for index in 0..self.renditions.len() {
				if let Some(fragment) = self.renditions[index].muxer.finish()? {
					rows.push(self.renditions[index].publish_fragment(fragment)?);
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
				version: "0.3.0".to_string(),
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
			codecs: vec!["h264".to_string()],
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
		if streams.is_empty() {
			return Err("publish reads at least one stream".into());
		}
		// Every muxer is built before the session, so a stream this
		// module cannot package is refused before anything is dialed.
		let mut built = Vec::with_capacity(streams.len());
		for stream in &streams {
			let coded = &stream.coded;
			built.push(match coded.format {
				CodedFormat::Video(video) => {
					if coded.codec != "h264" {
						return Err(format!(
							"publish packages h264 video, and this stream is {}",
							coded.codec
						));
					}
					let muxer = moq_core::mux::Muxer::video(
						&coded.extradata,
						video.width,
						video.height,
						coded.time_base.num,
						coded.time_base.den,
					)?;
					let codec = moq_core::catalog::avc_codec(
						muxer.avcc().expect("a video muxer builds an avcC"),
					);
					Built {
						muxer,
						codec,
						media: Media::Video {
							width: video.width,
							height: video.height,
						},
						time_base: (coded.time_base.num, coded.time_base.den),
					}
				}
				CodedFormat::Audio(audio) => {
					if coded.codec != "aac" {
						return Err(format!(
							"publish packages aac audio, and this stream is {}",
							coded.codec
						));
					}
					let muxer = moq_core::mux::Muxer::audio(
						&coded.extradata,
						audio.sample_rate,
						audio.channels,
						coded.time_base.num,
						coded.time_base.den,
					)?;
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
					}
				}
			});
		}
		// The video streams name a rendition apiece by height; the audio
		// stream takes the track name it was given.
		let heights: Vec<u32> = built
			.iter()
			.filter_map(|b| match b.media {
				Media::Video { height, .. } => Some(height),
				Media::Audio { .. } => None,
			})
			.collect();
		let mut video_names = moq_core::catalog::track_names(&params.track, &heights).into_iter();
		let mut audio_names = 0u32;
		let names: Vec<String> = built
			.iter()
			.map(|b| match b.media {
				Media::Video { .. } => video_names.next().expect("one name per video stream"),
				Media::Audio { .. } => {
					audio_names += 1;
					match audio_names {
						1 => params.audio_track.clone(),
						nth => format!("{}.{}", params.audio_track, nth - 1),
					}
				}
			})
			.collect();

		let (host, port) = parse_relay(&params.relay)?;
		let literal: Option<IpAddr> = host.parse().ok();
		let addrs = match literal {
			Some(ip) => vec![ip],
			None => crate::doh::resolve(&host)
				.map_err(|err| format!("relay '{}': {err}", params.relay))?,
		};
		let cert_der = match params.cert.trim() {
			"" => None,
			hex => Some(unhex(hex)?),
		};

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
				.create_track(catalog_name, info.clone())
				.map_err(|err| format!("track '{catalog_name}': {err}"))?;
			let mut tracks = Vec::with_capacity(names.len());
			for name in &names {
				let track = broadcast
					.create_track(name.as_str(), info.clone())
					.map_err(|err| format!("track '{name}': {err}"))?;
				tracks.push(track);
			}
			// A looked-up name can carry several addresses; each gets
			// the full connect timeout before the next is tried.
			let client = moq_net::Client::new().with_publisher(origin.consume());
			let mut connected = None;
			let mut last_err = String::new();
			for addr in &addrs {
				let relay = moq_core::wasi::Relay {
					addr: SocketAddr::new(*addr, port),
					server_name: host.clone(),
					cert_der: cert_der.clone(),
				};
				match tokio::time::timeout(
					CONNECT_TIMEOUT,
					moq_core::wasi::connect(&relay, client.clone()),
				)
				.await
				{
					Ok(Ok(session)) => {
						connected = Some(session);
						break;
					}
					Ok(Err(err)) => last_err = err.to_string(),
					Err(_) => last_err = "no answer within 10s".to_string(),
				}
			}
			let connected = connected.ok_or_else(|| match literal {
				Some(_) => format!("relay '{}': {last_err}", params.relay),
				None => format!(
					"relay '{}': no address of '{host}' answered ({} tried; last: {last_err})",
					params.relay,
					addrs.len()
				),
			})?;
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
				muxer: built.muxer,
				time_base: built.time_base,
				init_bytes: 0,
				groups: 0,
				packets: 0,
				bytes: 0,
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

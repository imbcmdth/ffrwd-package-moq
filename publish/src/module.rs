wit_bindgen::generate!({
	path: "wit",
	world: "packet-module",
});

use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use bytes::Bytes;
use exports::ffrwd::av::packet_sink::{
	CodedStream, Guest, Meta, Packet, PacketSinkMeta, Processed, StreamInfo,
};
use serde::{Deserialize, Serialize};

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"track":{"type":"string","default":"video","description":"media track name; the init segment rides <track>.init"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// One schema covers both row shapes: a group row carries `group`, the
/// trailing summary carries `groups`, and each leaves the other's
/// fields out. `pts_start`/`pts_end` are seconds of media time.
const ROWS_SCHEMA: &str = r#"{"type":"object","properties":{"group":{"type":"integer"},"packets":{"type":"integer"},"bytes":{"type":"integer"},"pts_start":{"type":"number"},"pts_end":{"type":"number"},"groups":{"type":"integer"},"init_bytes":{"type":"integer"}},"additionalProperties":false}"#;

/// How long the session stays open after the last fragment, for the
/// wire to drain: there is no delivered signal for a subscription.
const DRAIN: Duration = Duration::from_secs(2);

/// How long a relay gets to answer the dial before init gives up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Params {
	relay: String,
	broadcast: String,
	#[serde(default = "default_track")]
	track: String,
	#[serde(default)]
	cert: String,
}

fn default_track() -> String {
	"video".into()
}

/// One published group's row.
#[derive(Serialize)]
struct GroupRow {
	group: u64,
	packets: u64,
	bytes: u64,
	pts_start: f64,
	pts_end: f64,
}

/// The trailing summary, once per stream.
#[derive(Serialize)]
struct SummaryRow {
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

struct Session {
	endpoint: quinn::Endpoint,
	session: Option<moq_net::Session>,
	driver: Option<tokio::task::JoinHandle<()>>,
	broadcast: Option<moq_net::broadcast::Producer>,
	init_track: Option<moq_net::track::Producer>,
	track: Option<moq_net::track::Producer>,
	muxer: moq_core::mux::Muxer,
	time_base: (i32, i32),
	params: Params,
	/// Set once a subscriber arrived and the init segment went out.
	init_published: bool,
	init_bytes: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
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
	/// Publishes one fragment as one MoQ frame in a group of its own,
	/// and returns the group's row.
	fn publish_fragment(&mut self, fragment: moq_core::mux::Fragment) -> Result<String, String> {
		let timestamp_us =
			moq_core::mux::ticks_to_micros(fragment.pts_min, self.time_base.0, self.time_base.1);
		let bytes_len = fragment.bytes.len() as u64;
		let track = self.track.as_mut().expect("track lives until last");
		let mut group = track
			.append_group()
			.map_err(|err| format!("moq group: {err}"))?;
		group
			.write_frame(
				moq_net::Timestamp::from_micros(timestamp_us)
					.map_err(|err| format!("moq timestamp: {err}"))?,
				Bytes::from(fragment.bytes),
			)
			.map_err(|err| format!("moq frame: {err}"))?;
		group.finish().map_err(|err| format!("moq group: {err}"))?;

		let row = GroupRow {
			group: self.groups,
			packets: fragment.samples,
			bytes: bytes_len,
			pts_start: self.seconds(fragment.pts_min),
			pts_end: self.seconds(fragment.pts_max),
		};
		self.groups += 1;
		self.bytes += row.bytes;
		Ok(serde_json::to_string(&row).expect("a group row serializes"))
	}

	fn seconds(&self, ticks: i64) -> f64 {
		ticks as f64 * self.time_base.0 as f64 / self.time_base.1 as f64
	}

	/// One host call's work: hold for the first subscriber, feed the
	/// muxer, publish what closed, and on the final call drain and
	/// close the session.
	async fn drive(&mut self, packets: &[Packet], last: bool) -> Result<Processed, String> {
		let mut rows = Vec::new();
		let mut trailing = Vec::new();

		if !self.init_published {
			// A moq subscription starts at the LATEST group: anything
			// published before a subscriber asks is simply gone, and a
			// from-the-start playback needs group 0. The relay
			// subscribes upstream the moment a downstream subscriber
			// wants the track, which surfaces here - so hold the first
			// publish until then.
			self.track
				.as_mut()
				.expect("track lives until last")
				.used()
				.await
				.map_err(|err| format!("waiting for a subscriber: {err}"))?;
			let init = self.muxer.init_segment();
			self.init_bytes = init.len() as u64;
			let init_track = self.init_track.as_mut().expect("init track lives until last");
			let mut group = init_track
				.append_group()
				.map_err(|err| format!("moq group: {err}"))?;
			group
				.write_frame(
					moq_net::Timestamp::from_micros(0)
						.map_err(|err| format!("moq timestamp: {err}"))?,
					Bytes::from(init),
				)
				.map_err(|err| format!("moq frame: {err}"))?;
			group.finish().map_err(|err| format!("moq group: {err}"))?;
			self.init_published = true;
		}

		for packet in packets {
			let closed = self.muxer.push(moq_core::mux::Packet {
				pts: packet.pts,
				dts: packet.dts,
				keyframe: packet.keyframe,
				data: &packet.data,
			})?;
			self.packets += 1;
			if let Some(fragment) = closed {
				rows.push(self.publish_fragment(fragment)?);
			}
			// Let the driver move the frame onto the wire now, not
			// after the next call.
			tokio::task::yield_now().await;
		}

		if last {
			if let Some(fragment) = self.muxer.finish()? {
				rows.push(self.publish_fragment(fragment)?);
			}
			if let Some(mut track) = self.track.take() {
				track.finish().map_err(|err| format!("moq track: {err}"))?;
			}
			if let Some(mut init_track) = self.init_track.take() {
				init_track
					.finish()
					.map_err(|err| format!("moq track: {err}"))?;
			}
			// The finish and the last fragments still have to cross
			// the wire; the session offers no delivered signal, so
			// hold it open briefly.
			tokio::time::sleep(DRAIN).await;
			drop(self.broadcast.take());
			drop(self.session.take());
			if let Some(driver) = self.driver.take() {
				let _ = driver.await;
			}
			self.endpoint.wait_idle().await;
			trailing.push(
				serde_json::to_string(&SummaryRow {
					groups: self.groups,
					packets: self.packets,
					bytes: self.bytes,
					init_bytes: self.init_bytes,
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
				version: "0.2.0".to_string(),
				params_schema: PARAMS_SCHEMA.to_string(),
				rows_schema: ROWS_SCHEMA.to_string(),
				// No decoded payload ever arrives, so no format list fills in.
				pixel_formats: vec![],
				sample_formats: vec![],
				sample_rates: vec![],
				channel_counts: vec![],
				rows_language: vec![],
			},
			// The fmp4 packaging is h264-shaped: avcC from SPS/PPS.
			codecs: vec!["h264".to_string()],
		}
	}

	fn init(
		coded_stream: CodedStream,
		_stream_info: StreamInfo,
		params: String,
	) -> Result<(), String> {
		let params = parse_params(&params)?;
		if coded_stream.codec != "h264" {
			return Err(format!(
				"publish packages h264, and this stream is {}",
				coded_stream.codec
			));
		}
		let muxer = moq_core::mux::Muxer::new(
			&coded_stream.extradata,
			coded_stream.width,
			coded_stream.height,
			coded_stream.time_base.num,
			coded_stream.time_base.den,
		)?;
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
		let (broadcast, init_track, track, connected) = executor.enter(async {
			let origin = moq_net::Origin::random().produce();
			let mut broadcast = origin
				.create_broadcast(
					params.broadcast.as_str(),
					moq_net::broadcast::Route::announced(),
				)
				.map_err(|err| format!("broadcast '{}': {err}", params.broadcast))?;
			let init_track_name = format!("{}.init", params.track);
			let init_track = broadcast
				.create_track(init_track_name.as_str(), None)
				.map_err(|err| format!("track '{init_track_name}': {err}"))?;
			// The default keep window evicts a non-latest group after
			// 5s; give a backlogged subscriber more rope.
			let media_info =
				moq_net::track::Info::default().with_latency_max(Duration::from_secs(30));
			let track = broadcast
				.create_track(params.track.as_str(), media_info)
				.map_err(|err| format!("track '{}': {err}", params.track))?;
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
			Ok::<_, String>((broadcast, init_track, track, connected))
		})?;

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
					init_track: Some(init_track),
					track: Some(track),
					muxer,
					time_base: (coded_stream.time_base.num, coded_stream.time_base.den),
					params,
					init_published: false,
					init_bytes: 0,
					groups: 0,
					packets: 0,
					bytes: 0,
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

	fn process(packets: Vec<Packet>, last: bool) -> Processed {
		STATE.with(|s| {
			let mut holder = s.borrow_mut();
			let state = holder.as_mut().expect("process called before init");

			// Nothing to publish and no close asked: stay off the network.
			if packets.is_empty() && !last {
				return Processed {
					rows: vec![],
					trailing: vec![],
				};
			}

			let State { executor, session } = state;
			match executor.enter(session.drive(&packets, last)) {
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

wit_bindgen::generate!({
	path: "wit",
	world: "packet-sink-module",
});

use std::cell::RefCell;
use std::time::Duration;

use bytes::Bytes;
use exports::ffrwd::av::packet_sink::{
	Arity, Guest, InputStream, Meta, PacketSinkMeta, PadPackets, Processed, Wants,
};
use ffrwd::av::types::CodedFormat;
use serde::{Deserialize, Serialize};

const PARAMS_SCHEMA: &str = r#"{"type":"object","properties":{"relay":{"type":"string","description":"relay URL, e.g. moqt://relay.example.net:4443 - the host by name or IP, and a path the session opens under, which is where a relay's own address carries a token: https://relay.example/<JWT>"},"broadcast":{"type":"string","description":"broadcast path on the relay"},"cert":{"type":"string","default":"","description":"a private relay's certificate, DER as hex; empty trusts the webpki roots"},"token":{"type":"string","default":"","description":"an auth token the relay demands, sent as the SETUP request path; empty sends none, and a token already in the relay URL needs none"},"rows":{"type":"string","enum":["summary","groups","none"],"default":"summary","description":"what the sink reports: 'summary' is one row per track every 5s and a final total, 'groups' a row per published group, 'none' the final total alone"},"hold_s":{"type":"number","minimum":0,"default":10,"description":"how long the first media waits for a first subscriber before it goes out anyway, in seconds; a subscription starts at the latest group, so holding keeps a file's opening from being lost, and 0 publishes at once, which suits a live source nobody watches yet"},"reconnect_s":{"type":"integer","minimum":0,"default":60,"description":"how long a publisher whose session the relay dropped keeps trying to open a new one and announce the broadcast again, in seconds; groups are still written meanwhile, and 0 ends the run on the first drop"},"audio_group_ms":{"type":"integer","minimum":0,"default":200,"description":"how long an audio group runs, in milliseconds; a fifth of a second is ten AAC frames at 48kHz, and shorter groups have been seen losing whole groups through a public relay under load. 0 is one frame per group, which is what upstream hang writes and is experimental here"}},"required":["relay","broadcast"],"additionalProperties":false}"#;

/// The base name a video track falls back to when its row's rendition
/// carries none, and the ladder holds only the one stream.
const DEFAULT_VIDEO_TRACK: &str = "video";

/// The base name an audio track falls back to under the same rule.
const DEFAULT_AUDIO_TRACK: &str = "audio";

/// The base name a data track falls back to, numbered past the first as
/// audio is.
const DEFAULT_DATA_TRACK: &str = "data";

/// One schema covers the three row shapes, each leaving the others'
/// fields out. A GROUP row (`rows => 'groups'`) carries `track` and
/// `group`; a TRACK row (`rows => 'summary'`, every [`SUMMARY_EVERY`])
/// carries `track` and `groups` with the totals so far and `media`,
/// the seconds of media published on it; the TRAILING row, which every
/// run ends with, carries `tracks`. `pts_start`/`pts_end` are seconds
/// of media time.
const ROWS_SCHEMA: &str = r#"{"type":"object","properties":{"track":{"type":"string"},"group":{"type":"integer"},"packets":{"type":"integer"},"bytes":{"type":"integer"},"pts_start":{"type":"number"},"pts_end":{"type":"number"},"groups":{"type":"integer"},"media":{"type":"number"},"tracks":{"type":"integer"},"init_bytes":{"type":"integer"},"appended":{"type":"integer"},"closed":{"type":"integer"},"sub_latency_ms":{"type":"integer"},"sub_priority":{"type":"integer"},"sub_ordered":{"type":"boolean"},"gap_max_ms":{"type":"integer"},"call_max_ms":{"type":"integer"},"queued":{"type":"integer"},"queue_max":{"type":"integer"},"wait_max_ms":{"type":"integer"},"unpaced":{"type":"integer"},"event":{"type":"string","enum":["reconnect"]},"attempts":{"type":"integer"},"down_ms":{"type":"integer"},"error":{"type":"string"}},"additionalProperties":false}"#;

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
/// goes out regardless, unless `hold_s` says otherwise. The hold keeps a
/// file's start from being lost to the latest-group rule, and the harness
/// readers arrive within a second - but an unwatched live publish must
/// still flow: the pipes feeding the module are bounded, and a stalled
/// stage is killed. First reader or this, whichever comes first. A live
/// source loses nothing by starting at once, and `hold_s => 0` does.
const HOLD_MAX: f64 = 10.0;

/// How often a summary row per track goes out while a run lasts.
const SUMMARY_EVERY: Duration = Duration::from_secs(5);

/// One turn of the session between two groups: enough for the driver
/// and the serving tasks to take the group just closed before the next
/// one is appended. A yield alone runs the ready tasks once, and the
/// timer turn lets a task that parked on the socket come back.
async fn drive_once() {
	tokio::task::yield_now().await;
}

/// How long a call keeps driving the session after it has written
/// something, and in what steps. The session makes progress only while
/// a host call is blocked on the executor, so a group finished at the
/// end of a call is bytes that have not left yet; these turns are what
/// puts them on the socket before the call returns. Small: a few
/// milliseconds against an AAC frame's 21ms, and nothing at all for a
/// call that wrote nothing.
const FLUSH_SLICE: Duration = Duration::from_millis(1);
const FLUSH_TURNS: usize = 3;

/// How many turns of the session a call gives an acknowledgement that
/// arrived between calls before it first looks at what is held back; see
/// [`Session::catch_up`].
const CATCH_UP_TURNS: usize = 8;

/// How long a broadcast's media may go without a packet before a call
/// stops counting on the next one to let a held group go, on a host that
/// calls only when packets arrive.
///
/// A group held back behind one the relay has not acknowledged goes out
/// from a later call. A host that also calls with no packets when nothing
/// has arrived for a while (see [`Session::turns`]) makes that call a few
/// tens of milliseconds away at most. An older host calls only as packets
/// arrive: while media flows the next call is a frame away, but once it
/// stops - a source that ended before its data, a stalled upstream - calls
/// come only as messages do, seconds apart, and a message held to the next
/// one would wait that long. So on such a host a call made with the media
/// this quiet waits for its own queue, as every call did before 0.7.2:
/// there is no media left in it to hold up. The same span as
/// [`moq_core::delivery::DELIVER_MAX`], the longest the queue waits behind
/// any one group.
const MEDIA_QUIET: Duration = moq_core::delivery::DELIVER_MAX;

/// How often the catalog goes out again, the same snapshot in a fresh
/// group, while the broadcast lives. A relay that does not retain a
/// track's last closed group has nothing to hand a late joiner, whose
/// catalog.json subscription would otherwise wait forever. It is ~1KB;
/// the cost is nothing.
const CATALOG_REFRESH: Duration = Duration::from_secs(3);

/// How long a publisher whose session ended waits between tries at a
/// new one: the first wait, and the longest the doubling reaches.
const RECONNECT_FIRST: Duration = Duration::from_millis(500);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

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
	/// What the sink reports: see [`Rows`].
	#[serde(default = "default_rows")]
	rows: String,
	/// How long a dropped session is tried again for, in seconds; see
	/// [`Session::mend`].
	#[serde(default = "default_reconnect_s")]
	reconnect_s: u64,
	/// How long the first media waits for a first subscriber, in seconds;
	/// see [`HOLD_MAX`].
	#[serde(default = "default_hold_s")]
	hold_s: f64,
}

fn default_reconnect_s() -> u64 {
	60
}

fn default_hold_s() -> f64 {
	HOLD_MAX
}

/// What a run says about itself. A group a second is nothing; ten a
/// second, which a tenth-of-a-second audio group comes to, is a
/// terminal nobody can read, so the default is a periodic total.
#[derive(Clone, Copy, PartialEq)]
enum Rows {
	/// One row per track every [`SUMMARY_EVERY`], and the trailing total.
	Summary,
	/// A row per published group, for a run piping them somewhere.
	Groups,
	/// The trailing total alone.
	None,
}

impl Rows {
	fn parse(asked: &str) -> Result<Self, String> {
		match asked {
			"summary" => Ok(Rows::Summary),
			"groups" => Ok(Rows::Groups),
			"none" => Ok(Rows::None),
			other => Err(format!(
				"rows '{other}': publish reports 'summary' (a row per track every {}s), 				 'groups' (a row per group) or 'none'",
				SUMMARY_EVERY.as_secs()
			)),
		}
	}
}

fn default_rows() -> String {
	"summary".to_string()
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

/// One track's totals so far, every [`SUMMARY_EVERY`] while a run
/// lasts. `media` is the seconds of media published on the track,
/// which is what says whether it is keeping up.
#[derive(Serialize)]
struct TrackRow {
	track: String,
	groups: u64,
	packets: u64,
	bytes: u64,
	media: f64,
	/// Groups put on the MoQ track and groups finished there: they
	/// differ only by the one still open, so anything else is a group
	/// this module failed to close. `groups` runs ahead of both by what
	/// the track is holding back; see [`moq_core::delivery`].
	appended: u64,
	closed: u64,
	/// Groups held back right now, behind one the relay has not yet
	/// acknowledged, and over the window the most held at once, the
	/// longest one was held in milliseconds, and how many went before
	/// the one ahead of them was known delivered (at the cap or the
	/// bound), each one a group a relay that keeps only a track's newest
	/// could have lost.
	queued: u64,
	queue_max: u64,
	wait_max_ms: u64,
	unpaced: u64,
	/// What the relay is asking for on this track, which is what
	/// decides what a group of ours is worth to it: its latency window
	/// in milliseconds, its priority, and whether it wants groups in
	/// sequence order. -1 where nobody is subscribed.
	sub_latency_ms: i64,
	sub_priority: i64,
	sub_ordered: bool,
	/// The longest this window went with the session UNDRIVEN, and the
	/// longest one call held it, in milliseconds. The session runs only
	/// while a host call is on the executor, so the first is what a
	/// loaded machine costs and the second is what this module costs.
	gap_max_ms: u64,
	call_max_ms: u64,
}

/// The trailing summary, once per run over every track.
#[derive(Serialize)]
struct ReconnectRow<'a> {
	event: &'static str,
	attempts: u64,
	down_ms: u64,
	error: &'a str,
}

#[derive(Serialize)]
struct SummaryRow {
	tracks: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
	init_bytes: u64,
	/// Over the whole run and every track: the most groups one track
	/// held back at once, the longest one was held, and how many went
	/// unpaced. See [`TrackRow`].
	queue_max: u64,
	wait_max_ms: u64,
	unpaced: u64,
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
	/// JSON messages, their pts counted in `timescale` units per second
	/// as the catalog says; see [`message_timescale`].
	Data { timescale: u32 },
}

/// One stream packaged before the relay is dialed, waiting for the MoQ
/// tracks it will publish on.
struct Built {
	/// None for a data stream, whose messages travel as they are.
	muxer: Option<ffrwd_bmff::mux::Muxer>,
	codec: String,
	media: Media,
	time_base: (i32, i32),
	/// The NAL length prefix this track's `avcC` declares; 0 for audio,
	/// whose frames carry no framing of their own.
	length_size: usize,
}

/// One track: one encoded stream, its own fmp4 muxer, and the MoQ track
/// carrying its fragments. The init segment rides inside the catalog. A
/// data track has no muxer: each message is one frame in a group of its
/// own, its pts ahead of its bytes as they arrived.
struct Rendition {
	name: String,
	codec: String,
	media: Media,
	track: Option<moq_net::track::Producer>,
	/// Where the track's groups are cut and how they go onto the wire:
	/// each once the relay has the one before it. See
	/// [`moq_core::delivery`].
	pacer: moq_core::delivery::Pacer,
	/// Whether a group is open, on the wire or held in the pacer.
	group_open: bool,
	/// Which fragments open a group: this package's own convention, not
	/// anything the muxer decides.
	discipline: moq_core::group::Groups,
	muxer: Option<ffrwd_bmff::mux::Muxer>,
	time_base: (i32, i32),
	length_size: usize,
	init_bytes: u64,
	groups: u64,
	packets: u64,
	bytes: u64,
	/// The last group's end in seconds of media time, for the track row.
	media_seconds: f64,
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
			Media::Audio { .. } | Media::Data { .. } => Ok(data.to_vec()),
		}
	}

	/// The fmp4 muxer a media track packages through; a data track has
	/// none, and is never handed to one.
	fn muxer(&mut self) -> &mut ffrwd_bmff::mux::Muxer {
		self.muxer.as_mut().expect("a media track has a muxer")
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
		emit: bool,
	) -> Result<Vec<String>, String> {
		self.publish_frame(fragment.pts, fragment.keyframe, fragment.bytes, emit)
	}

	/// One MoQ frame at `pts`, in the open group or one the discipline
	/// opens for it: an fmp4 fragment for media, a framed message for
	/// data, whose discipline gives every message a group and closes it
	/// at once.
	fn publish_frame(
		&mut self,
		pts: i64,
		keyframe: bool,
		bytes: Vec<u8>,
		emit: bool,
	) -> Result<Vec<String>, String> {
		let mut rows = Vec::new();
		if self.discipline.starts_a_group(keyframe, pts) {
			rows.extend(self.close_group(emit)?);
			let subscribed = self.subscribed();
			let track = self.track.as_mut().expect("track lives until last");
			self.pacer
				.open(track, subscribed, tokio::time::Instant::now())
				.map_err(|err| format!("moq group: {err}"))?;
			self.group_open = true;
			self.group_packets = 0;
			self.group_bytes = 0;
			self.group_pts_min = pts;
			self.group_pts_max = pts;
		}
		let timestamp_us = ffrwd_bmff::time::ticks_to_micros(pts, self.time_base.0, self.time_base.1);
		let bytes_len = bytes.len() as u64;
		self.pacer
			.write(
				moq_net::Timestamp::from_micros(timestamp_us)
					.map_err(|err| format!("moq timestamp: {err}"))?,
				Bytes::from(bytes),
			)
			.map_err(|err| format!("moq frame: {err}"))?;
		self.group_packets += 1;
		self.group_bytes += bytes_len;
		self.group_pts_min = self.group_pts_min.min(pts);
		self.group_pts_max = self.group_pts_max.max(pts);
		self.bytes += bytes_len;
		if self.discipline.one_fragment_each() {
			rows.extend(self.close_group(emit)?);
		}
		Ok(rows)
	}

	/// Closes the open group, if any. `emit` asks for its row; a run
	/// reporting totals counts the group and says nothing.
	fn close_group(&mut self, emit: bool) -> Result<Option<String>, String> {
		if !self.group_open {
			return Ok(None);
		}
		self.group_open = false;
		self.pacer
			.close(tokio::time::Instant::now())
			.map_err(|err| format!("moq group: {err}"))?;
		let row = emit.then(|| GroupRow {
			track: self.name.clone(),
			group: self.groups,
			packets: self.group_packets,
			bytes: self.group_bytes,
			pts_start: self.seconds(self.group_pts_min),
			pts_end: self.seconds(self.group_pts_max),
		});
		self.groups += 1;
		self.media_seconds = self.seconds(self.group_pts_max);
		Ok(row.map(|row| serde_json::to_string(&row).expect("a group row serializes")))
	}

	/// Whether anybody is subscribed to this track. A track nobody reads
	/// sends nothing, and holds nothing back.
	fn subscribed(&self) -> bool {
		self.track
			.as_ref()
			.is_some_and(|track| track.subscription().is_some())
	}

	/// Puts on the wire whatever this track's queue may let go now, and
	/// says how many groups went. Never waits.
	fn release(&mut self) -> Result<usize, String> {
		let subscribed = self.subscribed();
		let Some(track) = self.track.as_mut() else {
			return Ok(0);
		};
		self.pacer
			.release(track, subscribed, tokio::time::Instant::now())
			.map_err(|err| format!("track '{}': moq group: {err}", self.name))
	}

	/// Notes whether the session has taken the track's last group yet,
	/// so a later look knows a group that has already gone from one
	/// nobody took.
	fn watch(&mut self) {
		self.pacer.watch();
	}

	/// This track's totals so far, as a row, with what the session and
	/// the host looked like over the window `gap_max`/`call_max` cover.
	fn track_row(&self, gap_max: Duration, call_max: Duration) -> String {
		let asked = self
			.track
			.as_ref()
			.and_then(moq_net::track::Producer::subscription);
		let held = self.pacer.window();
		serde_json::to_string(&TrackRow {
			track: self.name.clone(),
			groups: self.groups,
			packets: self.packets,
			bytes: self.bytes,
			media: self.media_seconds,
			appended: self.pacer.appended(),
			closed: self.pacer.closed(),
			queued: self.pacer.queued() as u64,
			queue_max: held.queue_max as u64,
			wait_max_ms: held.wait_max.as_millis() as u64,
			unpaced: held.unpaced,
			sub_latency_ms: asked
				.as_ref()
				.map_or(-1, |sub| sub.latency_max.as_millis() as i64),
			sub_priority: asked.as_ref().map_or(-1, |sub| i64::from(sub.priority)),
			sub_ordered: asked.as_ref().is_some_and(|sub| sub.ordered),
			gap_max_ms: gap_max.as_millis() as u64,
			call_max_ms: call_max.as_millis() as u64,
		})
		.expect("a track row serializes")
	}

	/// The catalog entry naming this track: its decoder configuration
	/// and its init segment inside.
	fn catalog_entry(&mut self) -> moq_core::catalog::Track {
		if let Media::Data { timescale } = self.media {
			return moq_core::catalog::Track::data(self.name.clone(), self.codec.clone(), timescale);
		}
		let init = self.muxer().init_segment();
		self.init_bytes = init.len() as u64;
		let muxer = self.muxer.as_ref().expect("a media track has a muxer");
		match self.media {
			Media::Video { width, height } => moq_core::catalog::Track::video(
				self.name.clone(),
				self.codec.clone(),
				muxer.config(),
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
				muxer.config(),
				sample_rate,
				channels,
				init,
			),
			Media::Data { .. } => unreachable!("a data track returned above"),
		}
	}
}

/// The timescale a data track's catalog entry names: the stream's own
/// ticks per second when its time base counts whole ticks (`1/n`), and
/// microseconds otherwise, which is what the wire's timestamps count in
/// anyway.
fn message_timescale(num: i32, den: i32) -> u32 {
	match (num, u32::try_from(den)) {
		(1, Ok(den)) if den > 0 => den,
		_ => 1_000_000,
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
	/// The session's protocol task, which ends when the session does.
	driver: Option<tokio::task::JoinHandle<Result<(), moq_net::Error>>>,
	/// What the broadcast lives in. It outlives any one session: a new
	/// session publishes the same origin, so the broadcast, its tracks and
	/// their group numbers carry on across a drop.
	origin: moq_net::origin::Producer,
	/// A new session being opened, with when the old one ended and why.
	redial: Option<Redial>,
	broadcast: Option<moq_net::broadcast::Producer>,
	catalog: Option<moq_net::track::Producer>,
	renditions: Vec<Rendition>,
	params: Params,
	/// Set once a subscriber arrived and the init segments went out.
	started: bool,
	/// When the catalog last went out; see [`CATALOG_REFRESH`].
	catalog_sent: Option<std::time::Instant>,
	/// What this run reports, and when it last reported it.
	rows: Rows,
	summary_sent: Option<std::time::Instant>,
	/// The cadence of the host's calls: the session runs only inside
	/// them, so the gap between them is how long it lay still.
	left_at: Option<std::time::Instant>,
	gap_max: Duration,
	call_max: Duration,
	/// When a call last carried a media packet; see [`MEDIA_QUIET`].
	media_at: Option<std::time::Instant>,
	/// Whether the host has called with no packets at all: one that does
	/// gives the session a turn whenever nothing has arrived for a while,
	/// so a held group goes out from that turn rather than from the next
	/// packet's call, and no call has to wait for a queue.
	turns: bool,
}

/// A new session being opened in the background while groups go on
/// being written.
struct Redial {
	task: tokio::task::JoinHandle<Result<(moq_core::wasi::Connected, u64), String>>,
	since: std::time::Instant,
	why: String,
}

/// Dials until a session opens or `window` is spent, a doubling wait
/// apart. The origin is what the new session announces, so a relay that
/// took every session back is handed the same broadcast again.
async fn redial(
	params: Params,
	origin: moq_net::origin::Consumer,
	window: Duration,
	why: String,
) -> Result<(moq_core::wasi::Connected, u64), String> {
	let began = std::time::Instant::now();
	let mut wait = RECONNECT_FIRST;
	let mut attempts = 0u64;
	loop {
		attempts += 1;
		let failure = match moq_core::relay::dial(
			&params.relay,
			&params.cert,
			&params.token,
			moq_net::Client::new().with_publisher(origin.clone()),
		)
		.await
		{
			Ok(connected) => return Ok((connected, attempts)),
			Err(err) => err,
		};
		let left = window.saturating_sub(began.elapsed());
		if left.is_zero() {
			return Err(format!(
				"{why}; no new session opened within {}s ({attempts} tries, the last: {failure})",
				window.as_secs()
			));
		}
		eprintln!("publish: connecting again: {failure}; trying again");
		tokio::time::sleep(wait.min(left)).await;
		wait = (wait * 2).min(RECONNECT_MAX);
	}
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
	Rows::parse(&params.rows)?;
	if !(params.hold_s.is_finite() && params.hold_s >= 0.0) {
		return Err(format!(
			"params: hold_s is how long to wait, in seconds, and {} is no such time",
			params.hold_s
		));
	}
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

	/// Replaces a session the relay dropped, without holding up the
	/// groups: the tracks live in the origin, not the session, so they go
	/// on being written while a new session is dialed in the background,
	/// and a subscriber on the new one starts at the latest of them.
	///
	/// A row goes out when the new session is in. A window spent without
	/// one, or a window of 0, is what stops the run.
	async fn mend(&mut self, rows: &mut Vec<String>) -> Result<(), String> {
		if let Some(redial) = &self.redial {
			if !redial.task.is_finished() {
				return Ok(());
			}
			let redial = self.redial.take().expect("just looked");
			let (connected, attempts) = redial
				.task
				.await
				.map_err(|err| format!("{}; {err}", redial.why))??;
			let down = redial.since.elapsed();
			self.endpoint = connected.endpoint;
			self.session = Some(connected.session);
			self.driver = Some(tokio::task::spawn_local(connected.driver));
			// A reader on the new session reads the catalog before it names
			// a rendition, so it goes out again now rather than on the timer.
			self.publish_catalog()?;
			eprintln!(
				"publish: a new session is in after {}ms and {attempts} tries",
				down.as_millis()
			);
			rows.push(
				serde_json::to_string(&ReconnectRow {
					event: "reconnect",
					attempts,
					down_ms: down.as_millis() as u64,
					error: &redial.why,
				})
				.expect("a reconnect row serializes"),
			);
			return Ok(());
		}
		let Some(driver) = self.driver.as_mut() else {
			return Ok(());
		};
		if !driver.is_finished() {
			return Ok(());
		}
		let why = match driver.await {
			Ok(Ok(())) => "the relay closed the session".to_string(),
			Ok(Err(err)) => err.to_string(),
			Err(err) => err.to_string(),
		};
		self.driver = None;
		drop(self.session.take());
		let window = Duration::from_secs(self.params.reconnect_s);
		if window.is_zero() {
			return Err(why);
		}
		eprintln!("publish: the session ended: {why}; connecting again");
		self.redial = Some(Redial {
			task: tokio::task::spawn_local(redial(
				self.params.clone(),
				self.origin.consume(),
				window,
				why.clone(),
			)),
			since: std::time::Instant::now(),
			why,
		});
		Ok(())
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

	/// Every track's queue given a turn, and how many groups went; see
	/// [`Rendition::release`].
	fn release(&mut self) -> Result<usize, String> {
		let mut released = 0;
		for rendition in &mut self.renditions {
			released += rendition.release()?;
		}
		Ok(released)
	}

	/// Whether any track is holding a group back.
	fn queued(&self) -> bool {
		self.renditions
			.iter()
			.any(|rendition| rendition.pacer.queued() > 0)
	}

	/// A few short turns of the session, looking at every track's queue
	/// after each: what was written goes onto the socket, and a group the
	/// relay acknowledged meanwhile lets the next one go. See
	/// [`FLUSH_TURNS`].
	async fn flush(&mut self) -> Result<(), String> {
		for _ in 0..FLUSH_TURNS {
			tokio::time::sleep(FLUSH_SLICE).await;
			for rendition in &mut self.renditions {
				rendition.watch();
			}
			self.release()?;
		}
		Ok(())
	}

	/// A call's first look at its queues, given the session enough turns
	/// to have read what arrived since the last call. An acknowledgement
	/// that landed between calls is a datagram in the socket, and it
	/// reaches the serving task that lets the group go only through
	/// several tasks in turn - the socket's reactor, the endpoint, the
	/// connection, the stream - each woken by the one before. Until it
	/// does, a group delivered long ago still looks held, and the next
	/// one cut behind it would wait on it or, past the cap, go counted as
	/// unpaced. Yields and no sleeps: a few microseconds a turn, and none
	/// when no track is waiting on anything. Says how many groups went.
	async fn catch_up(&mut self) -> Result<usize, String> {
		drive_once().await;
		let mut released = 0;
		for _ in 0..CATCH_UP_TURNS {
			let waiting = self
				.renditions
				.iter()
				.any(|rendition| rendition.pacer.queued() > 0 || rendition.pacer.unsettled());
			if !waiting {
				break;
			}
			for rendition in &mut self.renditions {
				rendition.watch();
			}
			released += self.release()?;
			drive_once().await;
		}
		Ok(released + self.release()?)
	}

	/// Until no track holds a group back, each going in its turn and none
	/// later than the cap behind the one before it. Only for a call that
	/// no later call will follow soon enough to let them go.
	async fn empty_queues(&mut self) -> Result<(), String> {
		if !self.queued() {
			return Ok(());
		}
		while self.queued() {
			tokio::time::sleep(FLUSH_SLICE).await;
			for rendition in &mut self.renditions {
				rendition.watch();
			}
			self.release()?;
		}
		// The last of them went into the session, not onto the socket: a
		// call that returned now would leave it there until the next one.
		self.flush().await
	}

	/// One host call's work: hold for the first subscriber, feed each
	/// pad's muxer, publish what closed, and on the final call drain and
	/// close the session.
	///
	/// A call with no packets and no close asked is a TURN: the session
	/// runs, what the queues may let go goes, and the call returns. Before
	/// the first subscriber there is nothing to let go, and a turn does not
	/// wait for one: the first packet does.
	async fn drive(&mut self, pads: &[PadPackets], last: bool) -> Result<Processed, String> {
		let mut rows = Vec::new();
		let mut trailing = Vec::new();
		let per_group = self.rows == Rows::Groups;
		let turn = !last && pads.iter().all(|pad| pad.packets.is_empty());
		if turn && !self.started {
			return Ok(Processed { rows, trailing });
		}
		let entered = std::time::Instant::now();
		if let Some(left) = self.left_at {
			self.gap_max = self.gap_max.max(entered.duration_since(left));
		}
		if !last {
			self.mend(&mut rows).await?;
		}

		if !self.started {
			// The catalog goes out to the first reader of anything: it is
			// what a subscriber needs before it can name a rendition, so
			// holding it until a rendition is named would hold it forever.
			let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(self.params.hold_s);
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

		// Every track's groups leave one at a time: a group cut while the
		// one before it on its track is still on its way is held in that
		// track's queue, since a relay that keeps only a track's latest
		// group drops one that a newer group overtakes on its way through.
		// Nothing here waits for one. What the relay has acknowledged since
		// the last call lets the groups behind it go now, before anything
		// new is cut, and every turn of the session below looks again. See
		// [`moq_core::delivery`].
		let released = self.catch_up().await?;
		let media = pads.iter().zip(&self.renditions).any(|(pad, rendition)| {
			!pad.packets.is_empty() && !matches!(rendition.media, Media::Data { .. })
		});
		if media {
			self.media_at = Some(std::time::Instant::now());
		}

		// Messages first, each one its own group, written and put on the
		// socket before any media of the same call is touched. A message is
		// an announcement - a break named seconds ahead of its cue - and
		// what it announces is the media behind it, so it must not wait in
		// this call behind a GOP's worth of fragments.
		let mut messages = false;
		for (index, pad) in pads.iter().enumerate() {
			let Some(rendition) = self.renditions.get_mut(index) else {
				continue;
			};
			if !matches!(rendition.media, Media::Data { .. }) {
				continue;
			}
			for packet in &pad.packets {
				rendition.packets += 1;
				// The pts rides in the frame, ahead of the message: the
				// frame's own timestamp does not survive every draft of the
				// wire. See [`moq_core::message`].
				let pts_us = ffrwd_bmff::time::ticks_to_micros(
					packet.pts,
					rendition.time_base.0,
					rendition.time_base.1,
				);
				let framed = moq_core::message::encode(pts_us, &packet.data).map_err(|err| {
					format!("track '{}': the message at pts {}: {err}", rendition.name, packet.pts)
				})?;
				rows.extend(rendition.publish_frame(packet.pts, true, framed, per_group)?);
				drive_once().await;
				rendition.watch();
				messages = true;
			}
		}
		if messages {
			self.flush().await?;
		}

		for (index, pad) in pads.iter().enumerate() {
			let Some(rendition) = self.renditions.get_mut(index) else {
				continue;
			};
			if matches!(rendition.media, Media::Data { .. }) {
				continue;
			}
			for packet in &pad.packets {
				let stored = rendition.stored(packet.pts, &packet.data)?;
				// Every AAC frame can be decoded from, whatever the wire
				// said about it.
				let keyframe = packet.keyframe || matches!(rendition.media, Media::Audio { .. });
				let fragments = rendition
					.muxer()
					.push(ffrwd_bmff::mux::Packet {
						pts: packet.pts,
						dts: packet.dts,
						duration: packet.duration,
						keyframe,
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
					let before = rendition.groups;
					rows.extend(rendition.publish_fragment(fragment, per_group)?);
					// A closed group is one the session has not been
					// given a chance to take. The session runs only while
					// this call is on the executor, so a call that closes
					// several groups in a row - a batch spanning several
					// boundaries, which is what a stalled upstream hands
					// over when it catches up - would finish them all
					// before the session saw any. Drive it here, per
					// group, and look at once whether it has taken it:
					// the next group on the track waits for that one.
					if rendition.groups != before {
						drive_once().await;
						rendition.watch();
					}
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
		//
		// Not for a group held back: its predecessor's acknowledgement is
		// a round trip away, further than these turns reach, and the next
		// call looks for it first thing. Waiting here for it is the call
		// blocked on delivery again.
		//
		// A turn that let a held group go puts it on the socket the same way:
		// the next turn is further off than these few milliseconds.
		if !rows.is_empty() || (turn && released > 0) {
			self.flush().await?;
		}
		// Unless no call is coming soon to do it: on a host that calls only
		// as packets arrive, with the media quiet, the next call is the next
		// message, and what this one holds back waits here instead. See
		// [`MEDIA_QUIET`].
		if !last
			&& !self.turns
			&& self
				.media_at
				.is_none_or(|at| at.elapsed() >= MEDIA_QUIET)
		{
			self.empty_queues().await?;
		}

		// The periodic word from a run that says nothing per group: what
		// each track has published so far, and how far into the media it
		// has got.
		if self.rows == Rows::Summary
			&& self
				.summary_sent
				.is_none_or(|sent| sent.elapsed() >= SUMMARY_EVERY)
		{
			let (gap_max, call_max) = (self.gap_max, self.call_max);
			rows.extend(
				self.renditions
					.iter()
					.map(|rendition| rendition.track_row(gap_max, call_max)),
			);
			self.summary_sent = Some(std::time::Instant::now());
			// Each window's own worst, not the run's.
			self.gap_max = Duration::ZERO;
			self.call_max = Duration::ZERO;
			for rendition in &mut self.renditions {
				rendition.pacer.next_window();
			}
		}

		if last {
			// Every track's last fragment goes out BEFORE any track is
			// finished: a subscriber stops at the finish, so a track told
			// it is over while its own tail is still queued loses that
			// tail. The yield is what lets the driver move them.
			for index in 0..self.renditions.len() {
				// A data track's groups are closed as they are written, and
				// it has no muxer holding anything back.
				let flushed = match self.renditions[index].muxer.as_mut() {
					Some(muxer) => muxer.finish().map_err(|err| {
						format!("track '{}': {err}", self.renditions[index].name)
					})?,
					None => Vec::new(),
				};
				for fragment in flushed {
					rows.extend(self.renditions[index].publish_fragment(fragment, per_group)?);
				}
				if let Some(row) = self.renditions[index].close_group(per_group)? {
					rows.push(row);
				}
			}
			// And what the tracks still hold back goes in its turn. This
			// call is the last, so nothing after it would let them go.
			self.empty_queues().await?;
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
			if let Some(redial) = self.redial.take() {
				redial.task.abort();
			}
			if let Some(driver) = self.driver.take() {
				let _ = driver.await;
			}
			self.endpoint.wait_idle().await;
			let held: Vec<moq_core::delivery::Stats> =
				self.renditions.iter().map(|r| r.pacer.run()).collect();
			trailing.push(
				serde_json::to_string(&SummaryRow {
					tracks: self.renditions.len() as u64,
					groups: self.renditions.iter().map(|r| r.groups).sum(),
					packets: self.renditions.iter().map(|r| r.packets).sum(),
					bytes: self.renditions.iter().map(|r| r.bytes).sum(),
					init_bytes: self.renditions.iter().map(|r| r.init_bytes).sum(),
					queue_max: held.iter().map(|s| s.queue_max as u64).max().unwrap_or(0),
					wait_max_ms: held
						.iter()
						.map(|s| s.wait_max.as_millis() as u64)
						.max()
						.unwrap_or(0),
					unpaced: held.iter().map(|s| s.unpaced).sum(),
				})
				.expect("a summary row serializes"),
			);
		}

		let now = std::time::Instant::now();
		self.call_max = self.call_max.max(now.duration_since(entered));
		self.left_at = Some(now);
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
			// and the audio and the data streams it names beside them - or
			// none, for a query that has none.
			video: Arity::Many,
			audio: Arity::Any,
			data: Arity::Any,
			// Every packet is published, so every packet is wanted.
			wants: Wants::All,
		}
	}

	fn init(streams: Vec<InputStream>, params: String) -> Result<(), String> {
		let params = parse_params(&params)?;
		let audio_group_ms = params.audio_group_ms;
		let reporting = Rows::parse(&params.rows)?;
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
						muxer: Some(muxer),
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
						muxer: Some(muxer),
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
				CodedFormat::Data => {
					if coded.codec != "json" {
						return Err(format!(
							"publish carries json data, one JSON object a message, and this data \
							 stream is {}",
							coded.codec
						));
					}
					Built {
						muxer: None,
						codec: coded.codec.clone(),
						media: Media::Data {
							timescale: message_timescale(
								coded.time_base.num,
								coded.time_base.den,
							),
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
					Media::Data { .. } => moq_core::catalog::RowKind::Data,
				},
				name: stream.rendition.name.clone(),
			})
			.collect();
		let names = moq_core::catalog::track_names_for_rows(
			&pads,
			DEFAULT_VIDEO_TRACK,
			DEFAULT_AUDIO_TRACK,
			DEFAULT_DATA_TRACK,
		);
		// What each track is worth when the session has more to send than
		// the wire takes. A relay reads every track of a broadcast on one
		// session and asks for them alike, so this tie-break is what keeps
		// a video keyframe from sitting in front of a sound, and either of
		// them in front of a message announcing what comes next.
		//
		// A track's frame timestamps travel in its own timescale, which
		// moq-net sets at milliseconds unless told otherwise. A media
		// track's time rides inside its fragments and the wire's is only a
		// label, but a message's pts IS the frame's timestamp, so a data
		// track counts in microseconds, which is what it is written in.
		let kinds: Vec<(u8, bool, moq_net::Timescale)> = built
			.iter()
			.map(|b| match b.media {
				Media::Video { .. } => (
					moq_core::catalog::PRIORITY_VIDEO,
					false,
					moq_net::Timescale::default(),
				),
				Media::Audio { .. } => (
					moq_core::catalog::PRIORITY_AUDIO,
					true,
					moq_net::Timescale::default(),
				),
				Media::Data { .. } => (
					moq_core::catalog::PRIORITY_DATA,
					true,
					moq_net::Timescale::MICRO,
				),
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
		let (origin, broadcast, catalog, tracks, connected) = executor.enter(async {
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
			for (name, &(priority, ordered, timescale)) in names.iter().zip(&kinds) {
				// Audio also asks to be served in sequence order. A track's
				// groups are otherwise newest-first, which is right for
				// video - a late picture is worth less than the next one -
				// and wrong for sound, where a group skipped is a click and
				// the one behind it cannot stand in. moq-net carries this in
				// TRACK_INFO and a subscriber takes it as the default for
				// its own subscription, so it reaches the relay's re-serve
				// rather than only our own queue. Data asks the same: every
				// message counts, and a newer one does not stand in for it.
				let track = broadcast
					.create_track(
						name.as_str(),
						info.clone()
							.with_priority(priority)
							.with_ordered(ordered)
							.with_timescale(timescale),
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
			Ok::<_, String>((origin, broadcast, catalog, tracks, connected))
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
				pacer: moq_core::delivery::Pacer::new(),
				group_open: false,
				discipline: match built.media {
					Media::Video { .. } => moq_core::group::Groups::video(),
					Media::Data { .. } => moq_core::group::Groups::messages(),
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
				media_seconds: 0.0,
				group_packets: 0,
				group_bytes: 0,
				group_pts_min: 0,
				group_pts_max: 0,
			})
			.collect();

		let driver = executor.local.spawn_local(connected.driver);

		STATE.with(|s| {
			*s.borrow_mut() = Some(State {
				executor,
				session: Session {
					endpoint: connected.endpoint,
					session: Some(connected.session),
					driver: Some(driver),
					origin,
					redial: None,
					broadcast: Some(broadcast),
					catalog: Some(catalog),
					renditions,
					params,
					started: false,
					catalog_sent: None,
					rows: reporting,
					summary_sent: None,
					left_at: None,
					gap_max: Duration::ZERO,
					call_max: Duration::ZERO,
					media_at: None,
					turns: false,
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

			// Nothing to publish and no close asked: a turn for the session,
			// which lets go what the queues may. See [`Session::drive`].
			let State { executor, session } = state;
			if !last && pads.iter().all(|pad| pad.packets.is_empty()) {
				session.turns = true;
			}
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

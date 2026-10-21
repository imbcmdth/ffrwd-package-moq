//! The live test's THIRD-PARTY subscriber: hang's own stack against our
//! broadcast.
//!
//! Everything between the relay and the output file is code from
//! github.com/kixelated/moq, pinned in Cargo.toml: moq-native dials the
//! relay, hang's parser reads our `catalog.json`, moq-mux picks the
//! renditions it is told to, decodes our CMAF fragments with their
//! container code and re-encodes them as one fmp4 byte stream, written
//! to a file ffprobe must then accept. If our catalog or fragments bend
//! their rules anywhere, this harness is what breaks.
//!
//! Environment: `RELAY_PORT`, `RELAY_CERT_PEM` (path to the relay's
//! certificate), `BROADCAST` (path), `OUTPUT` (file path). `VIDEO` and
//! `AUDIO` each name a rendition to take; either may be absent, not
//! both. Prints `hang:` lines the test asserts. `RELAY_URL` (a full
//! relay URL, credentials and all) replaces the localhost pair:
//! moq-native dials it as written, trusting the OS roots. An `OUTPUT` of
//! `-` writes the fmp4 stream to stdout as it arrives, a chunk at a time,
//! and the `hang:` lines to stderr: a decoder reading the pipe sees each
//! fragment the moment the export hands it over.
//!
//! `FRAMES=1` takes hang's export out of the path, which hands a group
//! over once it has all of it, and is a reader of the `VIDEO` rendition's
//! frames instead: the init segment off the catalog, then each frame's
//! payload (one `moof`+`mdat` fragment) the moment moq-native has it. That
//! is what a latency measurement wants from a native reader.

use std::io::Write;

/// The `hang:` lines go where the media does not.
static TO_STDERR: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

macro_rules! say {
	($($arg:tt)*) => {
		if TO_STDERR.load(std::sync::atomic::Ordering::Relaxed) {
			eprintln!($($arg)*);
		} else {
			println!($($arg)*);
		}
	};
}

fn env(name: &str) -> anyhow::Result<String> {
	std::env::var(name).map_err(|_| anyhow::anyhow!("missing env {name}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let broadcast_path = env("BROADCAST")?;
	let output = env("OUTPUT")?;
	TO_STDERR.store(output == "-", std::sync::atomic::Ordering::Relaxed);
	let video = std::env::var("VIDEO").ok();
	let audio = std::env::var("AUDIO").ok();
	if video.is_none() && audio.is_none() {
		anyhow::bail!("name a rendition: VIDEO, AUDIO or both");
	}

	let mut config = moq_native::ClientConfig::default();
	let url = match std::env::var("RELAY_URL") {
		// A full URL: dialed as written, trusted against the OS roots.
		Ok(relay_url) => url::Url::parse(&relay_url)?,
		Err(_) => {
			let port: u16 = env("RELAY_PORT")?.parse()?;
			let cert_pem = env("RELAY_CERT_PEM")?;
			let url = url::Url::parse(&format!("moqt://127.0.0.1:{port}"))?;
			config.tls.root = vec![cert_pem.into()];
			// The certificate names localhost; the dial is by IP.
			config.tls.host_name = Some("localhost".into());
			url
		}
	};
	config.connect = Some(url.clone());
	let client = config.init()?;

	let origin = moq_net::Origin::random().produce();
	let session = client.with_subscriber(origin.clone()).connect(url).await?;
	say!("hang: connected, version {:?}", session.version());

	// Wait for the publisher's announcement before resolving: a request
	// for a path nobody serves yet is refused, not held.
	say!("hang: waiting for {broadcast_path}");
	let broadcast = origin
		.consume()
		.announced_broadcast(broadcast_path.as_str())
		.await
		.ok_or_else(|| anyhow::anyhow!("broadcast never announced"))?;
	say!("hang: {broadcast_path} announced, reading the catalog");
	if std::env::var("FRAMES").is_ok_and(|on| on == "1") {
		let name = video
			.as_deref()
			.ok_or_else(|| anyhow::anyhow!("FRAMES reads the VIDEO rendition; name one"))?;
		let mut out: Box<dyn Write> = match output.as_str() {
			"-" => Box::new(std::io::stdout().lock()),
			path => Box::new(std::fs::File::create(path)?),
		};
		let count = frames(&broadcast, name, &mut out).await?;
		say!("hang: wrote {count} frames");
		say!("hang: PASS");
		return Ok(());
	}
	let source = moq_mux::Source::new(origin.consume(), broadcast_path.as_str());

	// THEIR catalog consumer over OUR catalog track, narrowed to the
	// renditions this run wants.
	let catalog: moq_mux::catalog::Consumer =
		moq_mux::catalog::Consumer::new(&broadcast, moq_mux::catalog::CatalogFormat::Hang).await?;
	let mut selection = moq_mux::select::Broadcast::default();
	if let Some(name) = &video {
		say!("hang: selecting video {name}");
		selection = selection.video(moq_mux::select::Video::default().name(name));
	}
	if let Some(name) = &audio {
		say!("hang: selecting audio {name}");
		selection = selection.audio(moq_mux::select::Audio::default().name(name));
	}
	let selected = moq_mux::catalog::Stream::select(catalog, selection);

	// THEIR export pipeline: subscribe the selected tracks, decode the
	// CMAF, re-encode one fmp4 stream.
	let mut export = moq_mux::container::fmp4::Export::new(source, selected)
		.with_latency(std::time::Duration::from_secs(30));

	let mut file: Box<dyn Write> = match output.as_str() {
		"-" => Box::new(std::io::stdout().lock()),
		path => Box::new(std::fs::File::create(path)?),
	};
	let mut chunks = 0u64;
	let mut bytes = 0u64;
	while let Some(chunk) = export.next().await? {
		file.write_all(&chunk)?;
		file.flush()?;
		bytes += chunk.len() as u64;
		if chunks == 0 {
			say!("hang: init segment {} bytes", chunk.len());
		} else {
			say!("hang: fragment {} {} bytes", chunks - 1, chunk.len());
		}
		chunks += 1;
	}
	file.flush()?;
	drop(file);

	if chunks < 2 {
		anyhow::bail!("the export produced {chunks} chunks; media never arrived");
	}
	say!("hang: wrote {chunks} chunks, {bytes} bytes");
	say!("hang: PASS");
	Ok(())
}

/// A subscription the way this package's own reader asks for one:
/// ordered, a non-latest group waited for 30 seconds.
fn subscription() -> moq_net::track::Subscription {
	moq_net::track::Subscription::default()
		.with_ordered(true)
		.with_latency_max(std::time::Duration::from_secs(30))
}

/// The `name` video rendition's init segment, off the catalog, and then
/// its frames as they arrive, until the track ends. Says how many frames.
async fn frames(
	broadcast: &moq_net::broadcast::Consumer,
	name: &str,
	out: &mut dyn Write,
) -> anyhow::Result<u64> {
	use base64::Engine;
	let mut catalog = broadcast
		.track("catalog.json")?
		.subscribe(subscription())
		.await?;
	let mut group = catalog
		.recv_group()
		.await?
		.ok_or_else(|| anyhow::anyhow!("the catalog track ended"))?;
	let document = group
		.read_frame()
		.await?
		.ok_or_else(|| anyhow::anyhow!("the catalog group is empty"))?;
	let document: serde_json::Value = serde_json::from_slice(&document.payload)?;
	let init = document["video"]["renditions"][name]["container"]["init"]
		.as_str()
		.ok_or_else(|| anyhow::anyhow!("the catalog has no init segment for '{name}'"))?;
	out.write_all(&base64::engine::general_purpose::STANDARD.decode(init)?)?;
	out.flush()?;
	let mut track = broadcast.track(name)?.subscribe(subscription()).await?;
	let mut count = 0u64;
	while let Some(mut group) = track.recv_group().await? {
		while let Some(frame) = group.read_frame().await? {
			out.write_all(&frame.payload)?;
			out.flush()?;
			count += 1;
		}
	}
	Ok(count)
}

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
//! moq-native dials it as written, trusting the OS roots.

use std::io::Write;

fn env(name: &str) -> anyhow::Result<String> {
	std::env::var(name).map_err(|_| anyhow::anyhow!("missing env {name}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let broadcast_path = env("BROADCAST")?;
	let output = env("OUTPUT")?;
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
	println!("hang: connected, version {:?}", session.version());

	// Wait for the publisher's announcement before resolving: a request
	// for a path nobody serves yet is refused, not held.
	println!("hang: waiting for {broadcast_path}");
	let broadcast = origin
		.consume()
		.announced_broadcast(broadcast_path.as_str())
		.await
		.ok_or_else(|| anyhow::anyhow!("broadcast never announced"))?;
	println!("hang: {broadcast_path} announced, reading the catalog");
	let source = moq_mux::Source::new(origin.consume(), broadcast_path.as_str());

	// THEIR catalog consumer over OUR catalog track, narrowed to the
	// renditions this run wants.
	let catalog: moq_mux::catalog::Consumer =
		moq_mux::catalog::Consumer::new(&broadcast, moq_mux::catalog::CatalogFormat::Hang).await?;
	let mut selection = moq_mux::select::Broadcast::default();
	if let Some(name) = &video {
		println!("hang: selecting video {name}");
		selection = selection.video(moq_mux::select::Video::default().name(name));
	}
	if let Some(name) = &audio {
		println!("hang: selecting audio {name}");
		selection = selection.audio(moq_mux::select::Audio::default().name(name));
	}
	let selected = moq_mux::catalog::Stream::select(catalog, selection);

	// THEIR export pipeline: subscribe the selected tracks, decode the
	// CMAF, re-encode one fmp4 stream.
	let mut export = moq_mux::container::fmp4::Export::new(source, selected)
		.with_latency(std::time::Duration::from_secs(30));

	let mut file = std::fs::File::create(&output)?;
	let mut chunks = 0u64;
	let mut bytes = 0u64;
	while let Some(chunk) = export.next().await? {
		file.write_all(&chunk)?;
		bytes += chunk.len() as u64;
		if chunks == 0 {
			println!("hang: init segment {} bytes", chunk.len());
		} else {
			println!("hang: fragment {} {} bytes", chunks - 1, chunk.len());
		}
		chunks += 1;
	}
	file.flush()?;
	drop(file);

	if chunks < 2 {
		anyhow::bail!("the export produced {chunks} chunks; media never arrived");
	}
	println!("hang: wrote {chunks} chunks, {bytes} bytes");
	println!("hang: PASS");
	Ok(())
}

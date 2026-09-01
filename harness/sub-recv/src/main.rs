//! MoQ subscriber for the package's live test: reassembles a published
//! fmp4 broadcast into a file a decoder must then accept whole.
//!
//! Connects to the relay, waits for the broadcast to be announced,
//! reads `catalog.json` to find out what the broadcast carries, then
//! the init segment off its own track and every fragment off the media
//! track - subscribed from group 0, ordered, tolerating backlog -
//! writing the bytes in arrival order. Prints `sub:` lines the test
//! asserts, the catalog's tracks among them.
//!
//! Environment: `RELAY_PORT`, `RELAY_CERT_HEX` (the relay certificate,
//! DER as hex), `BROADCAST` (path), `OUTPUT` (file path inside a
//! preopened directory). `TRACK` and `INIT_TRACK` name the tracks to
//! read; left unset, they come from the catalog - `RENDITION` picks
//! which of its tracks by index, and defaults to the first.

#[cfg(target_os = "wasi")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_time()
		.build()?;
	// The session driver is not Send on wasm, so it runs on a LocalSet.
	let local = tokio::task::LocalSet::new();
	local.block_on(&runtime, live::run())
}

#[cfg(not(target_os = "wasi"))]
fn main() {
	eprintln!("sub-recv is the live test's wasm32-wasip2 guest; build it for that target");
	std::process::exit(2);
}

#[cfg(target_os = "wasi")]
mod live {
	use std::error::Error;
	use std::io::Write;

	pub async fn run() -> Result<(), Box<dyn Error>> {
		let port: u16 = env("RELAY_PORT")?.parse()?;
		let cert_der = unhex(&env("RELAY_CERT_HEX")?)?;
		let broadcast_path = env("BROADCAST")?;
		let output = env("OUTPUT")?;

		let relay = moq_core::wasi::Relay {
			addr: format!("127.0.0.1:{port}").parse()?,
			server_name: "localhost".into(),
			cert_der: Some(cert_der),
		};

		let origin = moq_net::Origin::random().produce();
		let connected = moq_core::wasi::connect(
			&relay,
			moq_net::Client::new().with_subscriber(origin.clone()),
		)
		.await?;
		println!("sub: connected, version {:?}", connected.session.version());
		let session = connected.session;
		let driver = tokio::task::spawn_local(connected.driver);

		println!("sub: waiting for {broadcast_path}");
		let broadcast = origin
			.consume()
			.announced_broadcast(broadcast_path.as_str())
			.await
			.ok_or("broadcast never announced")?;
		println!("sub: {broadcast_path} announced, reading {CATALOG_TRACK}");

		let catalog = read_catalog(&broadcast).await?;
		println!("sub: catalog {}", catalog.trim());
		let tracks = catalog_tracks(&catalog)?;
		println!("sub: catalog names {} track(s)", tracks.len());
		for (name, init, codec, size) in &tracks {
			println!("sub: catalog track {name} init {init} codec {codec} {size}");
		}
		let rendition: usize = std::env::var("RENDITION")
			.ok()
			.map(|written| written.parse())
			.transpose()?
			.unwrap_or(0);
		let chosen = tracks
			.get(rendition)
			.ok_or("the catalog has no track at RENDITION")?;
        let track_name = std::env::var("TRACK").unwrap_or_else(|_| chosen.0.clone());
        let init_track_name = std::env::var("INIT_TRACK").unwrap_or_else(|_| chosen.1.clone());
		println!("sub: subscribing to {track_name}");

		// Both subscriptions exist before any frame is read, so no
		// media group can slip past while the init segment is fetched.
		let init_track = broadcast.track(init_track_name.as_str())?;
		let init_subscriber = init_track.subscribe(None).await?;
		let track = broadcast.track(track_name.as_str())?;
		// A default subscription is live-edge: start at the latest
		// group and skip any older one the moment a newer exists.
		// Reassembly wants the opposite - every group from 0, in
		// order, tolerating backlog.
		let subscription = moq_net::track::Subscription::default()
			.with_ordered(true)
			.with_latency_max(std::time::Duration::from_secs(30))
			.with_group_start(0);
		let subscriber = track.subscribe(subscription).await?;

		let mut file = std::fs::File::create(&output)?;

		let mut init_stream = moq_core::subscribe::FrameStream::new(init_subscriber);
		let init = init_stream
			.next()
			.await?
			.ok_or("init track finished without an init segment")?;
		file.write_all(&init.payload)?;
		println!("sub: init segment {} bytes", init.payload.len());

		let mut stream = moq_core::subscribe::FrameStream::new(subscriber)
			.on_group(|group, count| println!("sub: group {group} complete with {count} fragments"));
		let mut fragments = 0u64;
		let mut groups_seen = 0u64;
		let mut last_group = None;
		let mut bytes = init.payload.len() as u64;
		while let Some(frame) = stream.next().await? {
			file.write_all(&frame.payload)?;
			bytes += frame.payload.len() as u64;
			if last_group != Some(frame.group) {
				last_group = Some(frame.group);
				groups_seen += 1;
			}
			println!(
				"sub: fragment {fragments} group {} bytes {} pts {:.3}s",
				frame.group,
				frame.payload.len(),
				frame.timestamp_us as f64 / 1_000_000.0,
			);
			fragments += 1;
		}
		file.flush()?;
		drop(file);

		if fragments == 0 {
			return Err("no fragments received".into());
		}
		println!("sub: reassembled {fragments} fragments in {groups_seen} groups, {bytes} bytes");

		drop(broadcast);
		drop(session);
		driver.await.ok();
		connected.endpoint.wait_idle().await;
		println!("sub: clean close");
		println!("sub: PASS");
		Ok(())
	}

	/// The track a broadcast describes itself on, moq-rs's own name for it.
	const CATALOG_TRACK: &str = "catalog.json";

	/// The catalog document, read from group 0 of its own track.
	async fn read_catalog(
		broadcast: &moq_net::broadcast::Consumer,
	) -> Result<String, Box<dyn Error>> {
		let track = broadcast.track(CATALOG_TRACK)?;
		let subscription = moq_net::track::Subscription::default()
			.with_ordered(true)
			.with_latency_max(std::time::Duration::from_secs(30))
			.with_group_start(0);
		let mut stream =
			moq_core::subscribe::FrameStream::new(track.subscribe(subscription).await?);
		let frame = stream
			.next()
			.await?
			.ok_or("catalog track finished without a document")?;
		Ok(String::from_utf8(frame.payload.to_vec())?)
	}

	/// Each catalog track as `(name, init, codec, WxH)`.
	///
	/// Read with a small scan rather than a JSON crate: the harness has no
	/// serde dependency, and the document is this package's own.
	fn catalog_tracks(
		document: &str,
	) -> Result<Vec<(String, String, String, String)>, Box<dyn Error>> {
		let mut found = Vec::new();
		for entry in document.split("{\"name\"").skip(1) {
			let name = field(entry, "")?;
			let init = field(entry, "\"init\"")?;
			let codec = field(entry, "\"codec\"")?;
			let width = number(entry, "\"width\"")?;
			let height = number(entry, "\"height\"")?;
			found.push((name, init, codec, format!("{width}x{height}")));
		}
		if found.is_empty() {
			return Err(format!("no tracks in catalog {document}").into());
		}
		Ok(found)
	}

	/// One string field's value, after `key` (empty for the entry's first).
	fn field(entry: &str, key: &str) -> Result<String, Box<dyn Error>> {
		let rest = match entry.split_once(key) {
			Some((_, rest)) => rest,
			None => return Err(format!("catalog entry has no {key}").into()),
		};
		let (_, rest) = rest
			.split_once('"')
			.ok_or_else(|| format!("catalog {key} has no value"))?;
		let (value, _) = rest
			.split_once('"')
			.ok_or_else(|| format!("catalog {key} is unterminated"))?;
		Ok(value.to_string())
	}

	/// One numeric field's value, after `key`.
	fn number(entry: &str, key: &str) -> Result<u32, Box<dyn Error>> {
		let (_, rest) = entry
			.split_once(key)
			.ok_or_else(|| format!("catalog entry has no {key}"))?;
		let digits: String = rest
			.chars()
			.skip_while(|c| !c.is_ascii_digit())
			.take_while(|c| c.is_ascii_digit())
			.collect();
		Ok(digits.parse()?)
	}

	fn env(name: &str) -> Result<String, Box<dyn Error>> {
		std::env::var(name).map_err(|_| format!("missing env {name}").into())
	}

	fn unhex(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
		let digits: Vec<u8> = hex.trim().bytes().collect();
		if digits.len() % 2 != 0 {
			return Err("odd hex length".into());
		}
		digits
			.chunks_exact(2)
			.map(|pair| {
				let hi = (pair[0] as char).to_digit(16).ok_or("bad hex digit")?;
				let lo = (pair[1] as char).to_digit(16).ok_or("bad hex digit")?;
				Ok((hi * 16 + lo) as u8)
			})
			.collect()
	}
}

//! MoQ subscriber for the package's live test: reassembles a published
//! fmp4 broadcast into a file a decoder must then accept whole.
//!
//! Connects to the relay, waits for the broadcast to be announced,
//! reads `catalog.json` - hang's shape, the init segment base64 inside
//! each rendition's `cmaf` container - then every fragment off the
//! media track, subscribed from group 0, ordered, tolerating backlog,
//! writing init then fragments in arrival order. Prints `sub:` lines
//! the test asserts, the catalog's tracks among them.
//!
//! Environment: `RELAY_PORT`, `RELAY_CERT_HEX` (the relay certificate,
//! DER as hex), `BROADCAST` (path), `OUTPUT` (file path inside a
//! preopened directory). `TRACK` names the track to read; left unset,
//! it comes from the catalog - `RENDITION` picks a track by index over
//! the video renditions then the audio ones, each set in the
//! document's own (alphabetical) order, and defaults to the first.

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
		for track in &tracks {
			println!(
				"sub: catalog track {} codec {} {} init {} bytes",
				track.name,
				track.codec,
				track.shape,
				track.init.len()
			);
		}
		let rendition: usize = std::env::var("RENDITION")
			.ok()
			.map(|written| written.parse())
			.transpose()?
			.unwrap_or(0);
		let chosen = match std::env::var("TRACK") {
			Ok(name) => tracks
				.iter()
				.find(|track| track.name == name)
				.ok_or("the catalog has no track named TRACK")?,
			Err(_) => tracks.get(rendition).ok_or("the catalog has no track at RENDITION")?,
		};
		let track_name = chosen.name.clone();
		println!("sub: subscribing to {track_name}");

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

		// The init segment came with the catalog, not off a track.
		println!("sub: init segment {} bytes", chosen.init.len());
		let init = chosen.init.clone();

		let mut stream = moq_core::subscribe::FrameStream::new(subscriber)
			.on_group(|group, count| println!("sub: group {group} complete with {count} fragments"));
		// A backlogged subscription may hand groups out of order - the
		// relay serves them over parallel streams - so the fragments are
		// collected and written in GROUP order, which is decode order.
		let mut collected: Vec<(u64, Vec<u8>)> = Vec::new();
		let mut fragments = 0u64;
		let mut bytes = init.len() as u64;
		while let Some(frame) = stream.next().await? {
			bytes += frame.payload.len() as u64;
			println!(
				"sub: fragment {fragments} group {} bytes {} pts {:.3}s",
				frame.group,
				frame.payload.len(),
				frame.timestamp_us as f64 / 1_000_000.0,
			);
			collected.push((frame.group, frame.payload.to_vec()));
			fragments += 1;
		}

		if fragments == 0 {
			return Err("no fragments received".into());
		}
		collected.sort_by_key(|(group, _)| *group);
		let groups_seen = {
			let mut distinct = collected.iter().map(|(group, _)| *group).collect::<Vec<_>>();
			distinct.dedup();
			distinct.len() as u64
		};
		let mut file = std::fs::File::create(&output)?;
		file.write_all(&init)?;
		for (_, payload) in &collected {
			file.write_all(payload)?;
		}
		file.flush()?;
		drop(file);
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

	/// One track the catalog names: its name, codec, a shape line for
	/// the transcript, and the decoded init segment.
	struct CatalogTrack {
		name: String,
		codec: String,
		shape: String,
		init: Vec<u8>,
	}

	/// The catalog's tracks: the video renditions then the audio ones,
	/// each set in the document's own (alphabetical) order.
	fn catalog_tracks(document: &str) -> Result<Vec<CatalogTrack>, Box<dyn Error>> {
		let parsed: serde_json::Value = serde_json::from_str(document)?;
		let mut found = Vec::new();
		for kind in ["video", "audio"] {
			let Some(renditions) = parsed
				.get(kind)
				.and_then(|section| section.get("renditions"))
				.and_then(|map| map.as_object())
			else {
				continue;
			};
			for (name, entry) in renditions {
				let codec = string(entry, "codec")?;
				let shape = if kind == "audio" {
					format!("{}Hz {}ch", number(entry, "sampleRate")?, number(entry, "numberOfChannels")?)
				} else {
					format!("{}x{}", number(entry, "codedWidth")?, number(entry, "codedHeight")?)
				};
				let container = entry.get("container").ok_or("catalog entry has no container")?;
				if string(container, "kind")? != "cmaf" {
					return Err(format!("track {name} is not cmaf").into());
				}
				let init = unbase64(&string(container, "init")?)?;
				found.push(CatalogTrack {
					name: name.clone(),
					codec,
					shape,
					init,
				});
			}
		}
		if found.is_empty() {
			return Err(format!("no tracks in catalog {document}").into());
		}
		Ok(found)
	}

	fn string(entry: &serde_json::Value, key: &str) -> Result<String, Box<dyn Error>> {
		entry
			.get(key)
			.and_then(|value| value.as_str())
			.map(str::to_string)
			.ok_or_else(|| format!("catalog entry has no {key}").into())
	}

	fn number(entry: &serde_json::Value, key: &str) -> Result<u64, Box<dyn Error>> {
		entry
			.get(key)
			.and_then(|value| value.as_u64())
			.ok_or_else(|| format!("catalog entry has no {key}").into())
	}

	/// Standard base64 with padding, the coding the catalog's init uses.
	fn unbase64(text: &str) -> Result<Vec<u8>, Box<dyn Error>> {
		fn value(c: u8) -> Result<u32, Box<dyn Error>> {
			match c {
				b'A'..=b'Z' => Ok(u32::from(c - b'A')),
				b'a'..=b'z' => Ok(u32::from(c - b'a') + 26),
				b'0'..=b'9' => Ok(u32::from(c - b'0') + 52),
				b'+' => Ok(62),
				b'/' => Ok(63),
				_ => Err("bad base64 digit".into()),
			}
		}
		let digits: Vec<u8> = text.trim().trim_end_matches('=').bytes().collect();
		let mut out = Vec::with_capacity(digits.len() * 3 / 4);
		for chunk in digits.chunks(4) {
			if chunk.len() == 1 {
				return Err("truncated base64".into());
			}
			let mut word = 0u32;
			for (i, c) in chunk.iter().enumerate() {
				word |= value(*c)? << (18 - 6 * i);
			}
			out.push((word >> 16) as u8);
			if chunk.len() > 2 {
				out.push((word >> 8) as u8);
			}
			if chunk.len() > 3 {
				out.push(word as u8);
			}
		}
		Ok(out)
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

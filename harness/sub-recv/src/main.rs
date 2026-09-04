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
		let cert_der = moq_core::catalog::unhex(&env("RELAY_CERT_HEX")?)?;
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
		println!(
			"sub: {broadcast_path} announced, reading {}",
			moq_core::catalog::TRACK
		);

		let catalog = moq_core::subscribe::read_catalog(&broadcast).await?;
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
		// Reassembly reads every group from 0, in order, tolerating
		// backlog - the opposite of the live edge a player takes.
		let subscriber = track.subscribe(moq_core::subscribe::from_start()).await?;

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
		// No per-frame print: a fragment is one sample now, and the
		// harness reads these pipes only at the end - hundreds of lines
		// would fill the pipe and block the guest mid-broadcast. The
		// on_group line above carries each group's frame count.
		while let Some(frame) = stream.next().await? {
			bytes += frame.payload.len() as u64;
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

	/// One track the catalog names: its name, codec, a shape line for
	/// the transcript, and the decoded init segment.
	struct CatalogTrack {
		name: String,
		codec: String,
		shape: String,
		init: Vec<u8>,
	}

	/// The catalog's tracks, in the document's own order: the video
	/// renditions then the audio ones, each alphabetical.
	fn catalog_tracks(document: &str) -> Result<Vec<CatalogTrack>, Box<dyn Error>> {
		Ok(moq_core::catalog::parse(document)?
			.into_iter()
			.map(|rendition| CatalogTrack {
				name: rendition.name,
				codec: rendition.codec,
				shape: match rendition.kind {
					moq_core::catalog::Kind::Video { width, height } => {
						format!("{width}x{height}")
					}
					moq_core::catalog::Kind::Audio {
						sample_rate,
						channels,
					} => format!("{sample_rate}Hz {channels}ch"),
				},
				init: rendition.init,
			})
			.collect())
	}

	fn env(name: &str) -> Result<String, Box<dyn Error>> {
		std::env::var(name).map_err(|_| format!("missing env {name}").into())
	}

}

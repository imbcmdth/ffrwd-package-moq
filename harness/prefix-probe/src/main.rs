//! The guest `tests/live_prefixes.py` runs under wasmtime: one session,
//! held to the prefixes in `FFRWD_MODULE_MOQ_PREFIXES` through the same
//! `moq_core::prefixes` calls the modules make.
//!
//! `MODE=publish` checks the name as `publish` does, dials, and writes a
//! group a fifth of a second for `SECONDS` on one track. `MODE=subscribe`
//! builds its session's origin as `subscribe` does, prints the prefixes
//! that session asks the relay to announce, and then every broadcast it
//! is told of for `SECONDS`.
//!
//! Environment: `MODE`, `RELAY` (the relay URL), `RELAY_CERT_HEX` (the
//! relay certificate, DER as hex), `TOKEN` (optional), `BROADCAST`,
//! `SECONDS` (default 3). A refusal prints `probe: refused: <why>` and
//! exits with an error, as a module's refused params fail its call.

#[cfg(target_os = "wasi")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_time()
		.build()?;
	let local = tokio::task::LocalSet::new();
	local.block_on(&runtime, live::run())
}

#[cfg(not(target_os = "wasi"))]
fn main() {
	eprintln!("prefix-probe is the prefix test's wasm32-wasip2 guest; build it for that target");
	std::process::exit(2);
}

#[cfg(target_os = "wasi")]
mod live {
	use std::error::Error;
	use std::time::Duration;

	struct Setup {
		relay: String,
		cert: String,
		token: String,
		broadcast: String,
		seconds: Duration,
	}

	pub async fn run() -> Result<(), Box<dyn Error>> {
		let setup = Setup {
			relay: env("RELAY")?,
			cert: env("RELAY_CERT_HEX")?,
			token: std::env::var("TOKEN").unwrap_or_default(),
			broadcast: env("BROADCAST")?,
			seconds: Duration::from_secs_f64(
				std::env::var("SECONDS")
					.ok()
					.map(|seconds| seconds.parse())
					.transpose()?
					.unwrap_or(3.0),
			),
		};
		match env("MODE")?.as_str() {
			"publish" => publish(&setup).await,
			"subscribe" => subscribe(&setup).await,
			other => Err(format!("MODE '{other}' is neither publish nor subscribe").into()),
		}
	}

	fn refused(why: String) -> ! {
		println!("probe: refused: {why}");
		std::process::exit(1);
	}

	async fn publish(setup: &Setup) -> Result<(), Box<dyn Error>> {
		if let Err(why) = moq_core::prefixes::check(&setup.relay, &setup.token, &setup.broadcast) {
			refused(why);
		}
		let origin = moq_net::Origin::random().produce();
		let mut broadcast = origin.create_broadcast(
			setup.broadcast.as_str(),
			moq_net::broadcast::Route::announced(),
		)?;
		let mut track = broadcast.create_track("data", moq_net::track::Info::default())?;
		let connected = moq_core::relay::dial(
			&setup.relay,
			&setup.cert,
			&setup.token,
			moq_net::Client::new().with_publisher(origin.consume()),
		)
		.await?;
		let driver = tokio::task::spawn_local(connected.driver);
		println!("probe: publishing '{}'", setup.broadcast);
		let began = std::time::Instant::now();
		let mut groups = 0u64;
		while began.elapsed() < setup.seconds {
			let mut group = track.append_group()?;
			group.write_frame(
				moq_net::Timestamp::from_micros(began.elapsed().as_micros() as u64)?,
				moq_net::bytes::Bytes::from_static(b"{}"),
			)?;
			group.finish()?;
			groups += 1;
			tokio::time::sleep(Duration::from_millis(200)).await;
		}
		track.finish()?;
		drop(broadcast);
		drop(connected.session);
		driver.abort();
		connected.endpoint.wait_idle().await;
		println!("probe: published {groups} groups");
		Ok(())
	}

	async fn subscribe(setup: &Setup) -> Result<(), Box<dyn Error>> {
		let prefixes = moq_core::prefixes::Prefixes::from_env();
		let origin = match moq_core::prefixes::subscriber_origin(
			prefixes.as_ref(),
			&setup.relay,
			&setup.token,
			&setup.broadcast,
		) {
			Ok(origin) => origin,
			Err(why) => refused(why),
		};
		for prefix in origin.allowed() {
			println!("probe: asks for '{prefix}'");
		}
		let mut announced = origin.consume().announced();
		let connected = moq_core::relay::dial(
			&setup.relay,
			&setup.cert,
			&setup.token,
			moq_net::Client::new().with_subscriber(origin.clone()),
		)
		.await?;
		let driver = tokio::task::spawn_local(connected.driver);
		let deadline = tokio::time::Instant::now() + setup.seconds;
		while let Ok(Some(event)) = tokio::time::timeout_at(deadline, announced.next()).await {
			if event.broadcast.is_some() {
				println!("probe: announced '{}'", event.path);
			}
		}
		if driver.is_finished() {
			return Err("the session ended before the window did".into());
		}
		drop(connected.session);
		driver.abort();
		connected.endpoint.wait_idle().await;
		println!("probe: done");
		Ok(())
	}

	fn env(name: &str) -> Result<String, Box<dyn Error>> {
		std::env::var(name).map_err(|_| format!("missing env {name}").into())
	}
}

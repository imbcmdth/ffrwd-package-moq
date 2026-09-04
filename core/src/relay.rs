//! Dialing a relay: the URL, the trust, the token, and the addresses a
//! host name resolves to.
//!
//! What a publisher and a subscriber share before either says anything
//! about media. [`parse_url`] takes the relay URL apart; [`dial`] does
//! the rest - an IP-literal host as written, a name through the
//! DNS-over-HTTPS lookup in [`crate::doh`] since the runner links none,
//! each address in turn with its own timeout, the relay's own
//! certificate as the trust root where one is handed over, and the
//! token as the raw-QUIC session's request path.

/// The relay URL taken apart: host and port. The scheme is decorative -
/// a raw-QUIC dial carries no request URI - and a missing port is 443.
pub fn parse_url(url: &str) -> Result<(String, u16), String> {
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

/// How long a relay gets to answer one address before the next is tried.
#[cfg(target_os = "wasi")]
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Dials `url` for `client`: resolves the host, trusts `cert` (DER as
/// hex; empty trusts the webpki roots), and sends `token` as the
/// session's request path (empty sends none).
#[cfg(target_os = "wasi")]
pub async fn dial(
	url: &str,
	cert: &str,
	token: &str,
	mut client: moq_net::Client,
) -> Result<crate::wasi::Connected, String> {
	use std::net::{IpAddr, SocketAddr};

	let (host, port) = parse_url(url)?;
	let literal: Option<IpAddr> = host.parse().ok();
	let addrs = match literal {
		Some(ip) => vec![ip],
		None => crate::doh::resolve(&host).map_err(|err| format!("relay '{url}': {err}"))?,
	};
	let cert_der = match cert.trim() {
		"" => None,
		hex => Some(crate::catalog::unhex(hex).map_err(|err| format!("cert: {err}"))?),
	};
	// A raw-QUIC dial carries no request URI; the token a public relay
	// wants is the SETUP path itself.
	if !token.is_empty() {
		client = client.with_path(format!("/{token}"));
	}

	let mut last_err = String::new();
	for addr in &addrs {
		let relay = crate::wasi::Relay {
			addr: SocketAddr::new(*addr, port),
			server_name: host.clone(),
			cert_der: cert_der.clone(),
		};
		match tokio::time::timeout(
			CONNECT_TIMEOUT,
			crate::wasi::connect(&relay, client.clone()),
		)
		.await
		{
			Ok(Ok(session)) => return Ok(session),
			Ok(Err(err)) => last_err = err.to_string(),
			Err(_) => last_err = "no answer within 10s".to_string(),
		}
	}
	Err(match literal {
		Some(_) => format!("relay '{url}': {last_err}"),
		None => format!(
			"relay '{url}': no address of '{host}' answered ({} tried; last: {last_err})",
			addrs.len()
		),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_url_names_its_host_and_port() {
		assert_eq!(
			parse_url("moqt://relay.example.net:4443").expect("parses"),
			("relay.example.net".to_string(), 4443)
		);
		assert_eq!(
			parse_url("https://relay.example.net/live/demo").expect("parses"),
			("relay.example.net".to_string(), 443)
		);
		assert_eq!(
			parse_url("127.0.0.1:4443").expect("parses"),
			("127.0.0.1".to_string(), 4443)
		);
	}

	#[test]
	fn a_bracketed_v6_literal_keeps_its_colons() {
		assert_eq!(
			parse_url("moqt://[2606:4700::1111]:4443").expect("parses"),
			("2606:4700::1111".to_string(), 4443)
		);
		assert_eq!(
			parse_url("moqt://[2606:4700::1111]").expect("parses"),
			("2606:4700::1111".to_string(), 443)
		);
	}

	#[test]
	fn a_url_naming_no_host_or_no_port_is_refused() {
		assert!(parse_url("moqt://").is_err());
		let err = parse_url("moqt://relay.example.net:https").expect_err("a refusal");
		assert!(err.contains("is not a port"), "{err}");
	}
}

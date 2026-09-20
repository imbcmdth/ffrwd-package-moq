//! Dialing a relay: the URL, the trust, the token, and the addresses a
//! host name resolves to.
//!
//! What a publisher and a subscriber share before either says anything
//! about media. [`parse_url`] takes the relay URL apart; [`session_path`]
//! settles what the session asks the relay for, since a relay hands out
//! its address with the token already in it as often as a caller passes
//! one separately; [`dial`] does the rest - an IP-literal host as
//! written, a name through the DNS-over-HTTPS lookup in [`crate::doh`]
//! since the runner links none, each address in turn with its own
//! timeout, the relay's own certificate as the trust root where one is
//! handed over, and the path as the raw-QUIC session's request path.
//!
//! A token is a credential, so nothing here prints one whole: a message
//! names a URL by its origin alone and a token by its ends
//! ([`shorten`]).

/// The query parameter a relay reads a credential from. moq-relay takes
/// the JWT from `?jwt=` and ignores every other parameter, so that is
/// the one spelling this module carries through.
const JWT: &str = "jwt";

/// A relay URL taken apart: where to dial, and what the session asks
/// for. A missing port is 443.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Address {
	/// Everything before the path, as written: the scheme, if any, and
	/// the authority. This is the whole of the URL a message prints.
	pub origin: String,
	/// The host, an IPv6 literal unbracketed.
	pub host: String,
	pub port: u16,
	/// The path without its leading slash; empty for none and for a bare
	/// `/`. A relay reads it as the root the session works under, and it
	/// is where a token rides: `https://relay.example/<JWT>`.
	pub path: String,
	/// The query without its `?`; empty for none.
	pub query: String,
	/// The fragment without its `#`; empty for none.
	pub fragment: String,
}

/// Splits at the first `mark`, which is dropped; everything before and
/// nothing after where there is none.
fn split_at(text: &str, mark: char) -> (&str, &str) {
	match text.split_once(mark) {
		Some((before, after)) => (before, after),
		None => (text, ""),
	}
}

/// The URL as a message prints it: the origin, and a mark for each part
/// that is there. The path is where a token rides, so neither it nor the
/// query is ever printed.
fn shown_url(origin: &str, path: &str, query: &str, fragment: &str) -> String {
	let mut shown = origin.to_string();
	for (mark, part) in [('/', path), ('?', query), ('#', fragment)] {
		if !part.is_empty() {
			shown.push(mark);
			shown.push_str("...");
		}
	}
	shown
}

/// A token as a message may print it: the first four characters and the
/// last four of a long one, and the length alone of a short one, whose
/// ends would give it away. A message ends up in a log, and a token is a
/// credential.
pub fn shorten(token: &str) -> String {
	let chars: Vec<char> = token.chars().collect();
	if chars.len() <= 12 {
		return format!("{} characters", chars.len());
	}
	let head: String = chars[..4].iter().collect();
	let tail: String = chars[chars.len() - 4..].iter().collect();
	format!("{head}...{tail}")
}

/// The relay URL taken apart. The scheme decides nothing - a raw-QUIC
/// dial carries no request URI - but everything past the authority does:
/// see [`Address::session_path`].
pub fn parse_url(url: &str) -> Result<Address, String> {
	let (scheme, rest) = match url.split_once("://") {
		Some((scheme, rest)) => (format!("{scheme}://"), rest),
		None => (String::new(), url),
	};
	let (rest, fragment) = split_at(rest, '#');
	let (rest, query) = split_at(rest, '?');
	let (authority, path) = split_at(rest, '/');
	let origin = format!("{scheme}{authority}");
	let shown = shown_url(&origin, path, query, fragment);
	let (host, port) = match authority.rsplit_once(':') {
		Some((host, port)) if !host.is_empty() && !port.contains(']') => {
			let port: u16 = port
				.parse()
				.map_err(|_| format!("relay '{shown}': '{port}' is not a port"))?;
			(host, port)
		}
		_ => (authority, 443),
	};
	let host = host.trim_start_matches('[').trim_end_matches(']');
	if host.is_empty() {
		return Err(format!("relay '{shown}' names no host"));
	}
	Ok(Address {
		origin,
		host: host.to_string(),
		port,
		path: path.to_string(),
		query: query.to_string(),
		fragment: fragment.to_string(),
	})
}

impl Address {
	/// This URL as a message prints it: the origin, and a mark for every
	/// part that is there but not shown.
	pub fn shown(&self) -> String {
		shown_url(&self.origin, &self.path, &self.query, &self.fragment)
	}

	/// The credential this URL carries, empty where it carries none: the
	/// `?jwt=` a relay reads one from, else the path, which is what a
	/// relay handing out `https://relay.example/<JWT>` means by it.
	fn url_token(&self) -> &str {
		for pair in self.query.split('&') {
			let (key, value) = split_at(pair, '=');
			if key == JWT && !value.is_empty() {
				return value;
			}
		}
		&self.path
	}

	/// What the session asks the relay for: the request path a raw-QUIC
	/// SETUP carries, empty where it asks for nothing.
	///
	/// The URL's path and the `token` argument are two spellings of the
	/// same field, so either alone is used as it stands and a token in
	/// the URL opens exactly the session the argument would. Both
	/// naming the same token is fine; both naming different ones is a
	/// mistake with no safe reading, and is refused. Nothing else in the
	/// URL is dropped on the way: the query rides along, since that is
	/// how a `?jwt=` credential reaches a relay.
	pub fn session_path(&self, token: &str) -> Result<String, String> {
		let shown = self.shown();
		if !self.fragment.is_empty() {
			return Err(format!(
				"relay '{shown}': a '#' fragment is never sent to a relay, and this will not \
				 quietly drop one; pass what it carries as the URL's path or as the token"
			));
		}
		for pair in self.query.split('&') {
			let (key, _) = split_at(pair, '=');
			if key.eq_ignore_ascii_case("token") {
				return Err(format!(
					"relay '{shown}': a relay reads a credential from '?jwt=', not from \
					 '?{key}=', so this one would go unread; spell it 'jwt', put it in the \
					 URL's path, or pass it as the token argument"
				));
			}
		}
		let carried = self.url_token();
		if !token.is_empty() && !carried.is_empty() && carried != token {
			return Err(format!(
				"relay '{shown}': the URL carries one token ({}) and the token argument \
				 names another ({}); publish and subscribe send one token, so give the one \
				 the relay issued in one place - and where the URL's path is the broadcast \
				 root rather than the token, spell the token '?jwt=' instead",
				shorten(carried),
				shorten(token),
			));
		}
		// A raw-QUIC dial carries no request URI, so the request path is
		// the whole of what the session asks for: the root it works
		// under, the query beside it, and a public relay's token as
		// either.
		if self.path.is_empty() && self.query.is_empty() {
			return Ok(match token.is_empty() {
				true => String::new(),
				false => format!("/{token}"),
			});
		}
		let mut path = format!("/{}", self.path);
		if !self.query.is_empty() {
			path.push('?');
			path.push_str(&self.query);
		}
		Ok(path)
	}
}

/// What the session opened for `url` and `token` asks the relay for, as
/// [`Address::session_path`] settles it. The modules call this where
/// they read their params, so a URL and a token that disagree are
/// refused before anything is dialed.
pub fn session_path(url: &str, token: &str) -> Result<String, String> {
	parse_url(url)?.session_path(token)
}

/// How long a relay gets to answer one address before the next is tried.
#[cfg(target_os = "wasi")]
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Dials `url` for `client`: resolves the host, trusts `cert` (DER as
/// hex; empty trusts the webpki roots), and asks for the request path
/// the URL and `token` settle between them (empty asks for none).
#[cfg(target_os = "wasi")]
pub async fn dial(
	url: &str,
	cert: &str,
	token: &str,
	mut client: moq_net::Client,
) -> Result<crate::wasi::Connected, String> {
	use std::net::{IpAddr, SocketAddr};

	let address = parse_url(url)?;
	let shown = address.shown();
	let path = address.session_path(token)?;
	let host = address.host;
	let literal: Option<IpAddr> = host.parse().ok();
	let addrs = match literal {
		Some(ip) => vec![ip],
		None => crate::doh::resolve(&host).map_err(|err| format!("relay '{shown}': {err}"))?,
	};
	let cert_der = match cert.trim() {
		"" => None,
		hex => Some(crate::catalog::unhex(hex).map_err(|err| format!("cert: {err}"))?),
	};
	// The token a public relay wants is the SETUP path itself, which is
	// also where a relay's own address carries it.
	if !path.is_empty() {
		client = client.with_path(path);
	}

	let mut last_err = String::new();
	for addr in &addrs {
		let relay = crate::wasi::Relay {
			addr: SocketAddr::new(*addr, address.port),
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
		Some(_) => format!("relay '{shown}': {last_err}"),
		None => format!(
			"relay '{shown}': no address of '{host}' answered ({} tried; last: {last_err})",
			addrs.len()
		),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A JWT is the shape a public relay hands out, and long enough that
	/// the shortened form is a middle cut rather than a length.
	const JWT_A: &str = "eyJhbGciOiJIUzI1NiJ9.eyJyb290IjoibGl2ZSJ9.qSsp0wLyaMJ_Nv0";
	const JWT_B: &str = "eyJhbGciOiJIUzI1NiJ9.eyJyb290IjoibmV3cyJ9.f4XQ2bhcXZs_M1c";

	fn address(url: &str) -> Address {
		parse_url(url).expect("parses")
	}

	fn path_of(url: &str, token: &str) -> String {
		session_path(url, token).expect("a session path")
	}

	#[test]
	fn a_url_names_its_host_and_port() {
		let parsed = address("moqt://relay.example.net:4443");
		assert_eq!((parsed.host.as_str(), parsed.port), ("relay.example.net", 4443));
		let parsed = address("https://relay.example.net/live/demo");
		assert_eq!((parsed.host.as_str(), parsed.port), ("relay.example.net", 443));
		let parsed = address("127.0.0.1:4443");
		assert_eq!((parsed.host.as_str(), parsed.port), ("127.0.0.1", 4443));
	}

	#[test]
	fn a_bracketed_v6_literal_keeps_its_colons() {
		let parsed = address("moqt://[2606:4700::1111]:4443");
		assert_eq!((parsed.host.as_str(), parsed.port), ("2606:4700::1111", 4443));
		let parsed = address("moqt://[2606:4700::1111]");
		assert_eq!((parsed.host.as_str(), parsed.port), ("2606:4700::1111", 443));
		let parsed = address("moqt://[2606:4700::1111]:4443/live/demo");
		assert_eq!(
			(parsed.host.as_str(), parsed.port, parsed.path.as_str()),
			("2606:4700::1111", 4443, "live/demo")
		);
	}

	#[test]
	fn a_url_naming_no_host_or_no_port_is_refused() {
		assert!(parse_url("moqt://").is_err());
		let err = parse_url("moqt://relay.example.net:https").expect_err("a refusal");
		assert!(err.contains("is not a port"), "{err}");
	}

	#[test]
	fn a_path_is_the_token() {
		assert_eq!(path_of(&format!("https://relay.example/{JWT_A}"), ""), format!("/{JWT_A}"));
		// And a path of several segments is a root, carried as written.
		assert_eq!(path_of("moqt://relay.example:4443/live/demo", ""), "/live/demo");
	}

	#[test]
	fn an_argument_is_the_token() {
		assert_eq!(path_of("moqt://relay.example:4443", JWT_A), format!("/{JWT_A}"));
		// And a URL that names no token at all asks for nothing.
		assert_eq!(path_of("moqt://relay.example:4443", ""), "");
	}

	#[test]
	fn a_bare_slash_is_no_token() {
		assert_eq!(path_of("https://relay.example/", ""), "");
		assert_eq!(path_of("https://relay.example/", JWT_A), format!("/{JWT_A}"));
	}

	#[test]
	fn the_two_spellings_of_one_token_agree() {
		assert_eq!(
			path_of(&format!("https://relay.example/{JWT_A}"), JWT_A),
			format!("/{JWT_A}")
		);
	}

	#[test]
	fn two_different_tokens_are_refused_without_printing_either() {
		let err = session_path(&format!("https://relay.example/{JWT_A}"), JWT_B)
			.expect_err("a refusal");
		assert!(err.contains("the token argument names another"), "{err}");
		assert!(!err.contains(JWT_A), "{err}");
		assert!(!err.contains(JWT_B), "{err}");
		// The ends, and nothing between them.
		assert!(err.contains("eyJh..._Nv0") && err.contains("eyJh..._M1c"), "{err}");
	}

	#[test]
	fn a_jwt_query_is_carried_to_the_relay() {
		assert_eq!(
			path_of(&format!("https://relay.example/live?jwt={JWT_A}"), ""),
			format!("/live?jwt={JWT_A}")
		);
		// The same token in both places is one token.
		assert_eq!(
			path_of(&format!("https://relay.example/live?jwt={JWT_A}"), JWT_A),
			format!("/live?jwt={JWT_A}")
		);
		// A different one in each is the same conflict a path makes.
		let err = session_path(&format!("https://relay.example/live?jwt={JWT_A}"), JWT_B)
			.expect_err("a refusal");
		assert!(err.contains("the token argument names another"), "{err}");
		assert!(!err.contains(JWT_A) && !err.contains(JWT_B), "{err}");
	}

	#[test]
	fn a_query_that_is_not_a_jwt_is_carried_or_refused() {
		// A relay reads no credential from '?token=', so carrying it
		// would publish unauthenticated with a token in hand.
		let err = session_path(&format!("https://relay.example/live?token={JWT_A}"), "")
			.expect_err("a refusal");
		assert!(err.contains("'?jwt='"), "{err}");
		assert!(!err.contains(JWT_A), "{err}");
		// Anything else the relay is welcome to ignore rides along
		// rather than being dropped behind the caller's back.
		assert_eq!(path_of("https://relay.example/live?tier=gold", ""), "/live?tier=gold");
	}

	#[test]
	fn a_fragment_is_refused() {
		let err = session_path(&format!("https://relay.example/live#{JWT_A}"), "")
			.expect_err("a refusal");
		assert!(err.contains("fragment"), "{err}");
		assert!(!err.contains(JWT_A), "{err}");
	}

	#[test]
	fn a_message_names_the_url_without_its_token() {
		let shown = address(&format!("https://relay.example/{JWT_A}?jwt={JWT_A}#x")).shown();
		assert_eq!(shown, "https://relay.example/...?...#...");
		let err = parse_url(&format!("https://relay.example:https/{JWT_A}")).expect_err("a refusal");
		assert!(!err.contains(JWT_A), "{err}");
	}

	#[test]
	fn a_shortened_token_shows_its_ends_at_most() {
		assert_eq!(shorten("eyJhbGciOiJIUzI1NiJ9"), "eyJh...NiJ9");
		assert_eq!(shorten("short"), "5 characters");
		assert_eq!(shorten(""), "0 characters");
	}
}

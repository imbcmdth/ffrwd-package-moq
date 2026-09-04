//! Relay hostnames resolved over DNS-over-HTTPS (RFC 8484): the
//! runner grants UDP and outgoing HTTP, but no name lookup. The
//! resolvers are pinned here by IP, so reaching them needs no lookup
//! either, and nothing from the query's parameters picks them.

use std::net::IpAddr;

use crate::dns;
use wasi::http::outgoing_handler;
use wasi::http::types::{Fields, Method, OutgoingBody, OutgoingRequest, RequestOptions, Scheme};

/// The pinned resolvers, tried in order. IP literals only.
const RESOLVERS: [Resolver; 2] = [
	Resolver {
		authority: "1.1.1.1",
		url: "https://1.1.1.1/dns-query",
	},
	Resolver {
		authority: "8.8.8.8",
		url: "https://8.8.8.8/dns-query",
	},
];

struct Resolver {
	authority: &'static str,
	url: &'static str,
}

/// Per-request limit on connect, first byte and byte spacing.
const HTTP_TIMEOUT_NS: u64 = 5_000_000_000;

/// The largest DoH answer read before giving up.
const RESPONSE_LIMIT: usize = 64 * 1024;

/// Resolves `host` to its A and AAAA addresses, v4 first.
///
/// The second resolver is tried only when the first could not answer;
/// a well-formed empty answer is final - the name resolves to nothing,
/// and asking elsewhere would not change that.
pub fn resolve(host: &str) -> Result<Vec<IpAddr>, String> {
	let queries = [
		dns::encode_query(host, dns::TYPE_A)?,
		dns::encode_query(host, dns::TYPE_AAAA)?,
	];
	let mut unanswered = String::new();
	for resolver in &RESOLVERS {
		match ask(resolver, &queries) {
			Ok(addrs) if addrs.is_empty() => {
				return Err(format!("'{host}' resolved to nothing"));
			}
			Ok(addrs) => return Ok(addrs),
			Err(err) => unanswered = err,
		}
	}
	Err(format!(
		"no DNS resolver answered ({}, {}; last: {unanswered})",
		RESOLVERS[0].url, RESOLVERS[1].url
	))
}

/// One resolver's combined A + AAAA answer. Errs when the resolver
/// answered neither query, or answered only queries that came up
/// empty while the other failed - either way the next resolver still
/// has something to add.
fn ask(resolver: &Resolver, queries: &[Vec<u8>; 2]) -> Result<Vec<IpAddr>, String> {
	let mut addrs = Vec::new();
	let mut failure = None;
	for query in queries {
		match exchange(resolver, query) {
			Ok(mut some) => addrs.append(&mut some),
			Err(err) => failure = Some(err),
		}
	}
	match failure {
		Some(err) if addrs.is_empty() => Err(err),
		_ => Ok(addrs),
	}
}

/// One RFC 8484 GET: the query rides base64url in `?dns=`.
fn exchange(resolver: &Resolver, query: &[u8]) -> Result<Vec<IpAddr>, String> {
	let path = format!("/dns-query?dns={}", dns::base64url(query));
	let body = fetch(resolver.authority, &path).map_err(|err| format!("{}: {err}", resolver.url))?;
	dns::parse_response(&body).map_err(|err| format!("{}: {err}", resolver.url))
}

/// GETs `https://<authority><path_with_query>` over wasi:http and
/// returns the response body.
fn fetch(authority: &str, path_with_query: &str) -> Result<Vec<u8>, String> {
	let headers = Fields::from_list(&[("accept".to_string(), b"application/dns-message".to_vec())])
		.map_err(|err| format!("headers: {err}"))?;
	let request = OutgoingRequest::new(headers);
	request.set_method(&Method::Get).map_err(|()| "GET refused")?;
	request
		.set_scheme(Some(&Scheme::Https))
		.map_err(|()| "https refused")?;
	request
		.set_authority(Some(authority))
		.map_err(|()| "authority refused")?;
	request
		.set_path_with_query(Some(path_with_query))
		.map_err(|()| "path refused")?;
	// A GET still owns a body resource; finish it empty so the
	// request is complete.
	let empty = request.body().map_err(|()| "request body taken twice")?;
	OutgoingBody::finish(empty, None).map_err(|err| format!("request body: {err}"))?;

	let options = RequestOptions::new();
	let _ = options.set_connect_timeout(Some(HTTP_TIMEOUT_NS));
	let _ = options.set_first_byte_timeout(Some(HTTP_TIMEOUT_NS));
	let _ = options.set_between_bytes_timeout(Some(HTTP_TIMEOUT_NS));

	let future = outgoing_handler::handle(request, Some(options))
		.map_err(|err| format!("request: {err}"))?;
	let response = loop {
		match future.get() {
			Some(ready) => {
				break ready
					.map_err(|()| "response taken twice".to_string())?
					.map_err(|err| format!("request: {err}"))?;
			}
			None => future.subscribe().block(),
		}
	};

	let status = response.status();
	if status != 200 {
		return Err(format!("http {status}"));
	}
	let body = response.consume().map_err(|()| "response body taken twice")?;
	let stream = body.stream().map_err(|()| "response body unreadable")?;
	let mut bytes = Vec::new();
	loop {
		match stream.blocking_read(8 * 1024) {
			Ok(chunk) => {
				bytes.extend_from_slice(&chunk);
				if bytes.len() > RESPONSE_LIMIT {
					return Err("response too large".into());
				}
			}
			Err(wasi::io::streams::StreamError::Closed) => break,
			Err(wasi::io::streams::StreamError::LastOperationFailed(err)) => {
				return Err(format!("read: {}", err.to_debug_string()));
			}
		}
	}
	Ok(bytes)
}

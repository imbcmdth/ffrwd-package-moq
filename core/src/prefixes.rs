//! The broadcast names a run may use, as the host running it gives them.
//!
//! A host running other people's queries on relays that check nothing
//! gives each run the prefixes its names have to fall under, in the
//! environment variable [`VARIABLE`]: broadcast path prefixes separated
//! by commas. Unset, a run may use any name. Set, every publish and every
//! subscribe has to fall under one of them, and set to nothing, none may.
//! A query cannot set it, since the host builds the module's environment.
//!
//! The name checked is the one the relay sees. A relay roots a session at
//! the path the session asks for ([`crate::relay::Address::session_path`]):
//! the relay URL's path, or the token where the URL has none. So
//! `moqt://relay.example/jobs/b` with broadcast `out` is `jobs/b/out` on
//! the relay, and a check of `out` alone would let it through.
//!
//! A prefix matches whole segments: `jobs/a` and `jobs/a/` both cover
//! `jobs/a` and `jobs/a/out`, and neither covers `jobs/ab`. While prefixes
//! are set, a name with an empty, `.` or `..` segment is refused, since a
//! relay may read `jobs/a//out` as `jobs/a/out` or `jobs/a/../b` as `jobs/b`.

/// Where the host puts the prefixes.
pub const VARIABLE: &str = "FFRWD_MODULE_MOQ_PREFIXES";

/// The longest segment of a session's root a message prints whole. The
/// root can be a credential, and a longer segment is printed by its ends,
/// the way [`crate::relay::shorten`] prints a token.
const SHOWN_WHOLE: usize = 12;

/// The prefixes a run may publish and subscribe under, each without the
/// slashes around it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prefixes {
	list: Vec<String>,
}

impl Prefixes {
	/// The prefixes the host set for this run, or `None` where it set
	/// none. A value that is not UTF-8 lists nothing, so it refuses
	/// everything.
	pub fn from_env() -> Option<Self> {
		let value = std::env::var_os(VARIABLE)?;
		Some(Self::parse(value.to_str().unwrap_or("")))
	}

	/// The prefixes in `value`: split at commas, each trimmed of spaces and
	/// of the slashes around it, and an entry left empty skipped.
	pub fn parse(value: &str) -> Self {
		let list = value
			.split(',')
			.map(|entry| entry.trim().trim_matches('/'))
			.filter(|entry| !entry.is_empty())
			.map(str::to_string)
			.collect();
		Prefixes { list }
	}

	/// Whether a session opened for `relay` and `token` may publish or
	/// subscribe to `broadcast`. A refusal names the path the relay would
	/// see and the prefixes allowed, and says the host sets them.
	pub fn check(&self, relay: &str, token: &str, broadcast: &str) -> Result<(), String> {
		let root = session_root(relay, token)?;
		let path = join(&root, broadcast);
		let segments: Vec<&str> = path.split('/').collect();
		let refused = |why: &str| {
			Err(format!(
				"the relay would see broadcast '{broadcast}' as '{}', which {why}; the host \
				 running this query sets the prefixes in {VARIABLE}",
				join(&shown_root(&root, token), broadcast),
			))
		};
		if segments
			.iter()
			.any(|segment| matches!(*segment, "" | "." | ".."))
		{
			return refused(&format!(
				"has an empty, '.' or '..' segment, and those are refused while this run is held \
				 to prefixes ({})",
				self.listed()
			));
		}
		if !self.list.iter().any(|prefix| covers(prefix, &segments)) {
			return refused(&format!(
				"is under none of the prefixes this run may use ({})",
				self.listed()
			));
		}
		Ok(())
	}

	/// What a subscriber session opened for `relay` and `token` asks the
	/// relay to announce: each allowed prefix the session's root reaches,
	/// relative to that root, which is where a relay reads an announce
	/// interest from. `[""]`, the whole root, where a prefix covers the
	/// root itself; empty where no prefix reaches it.
	pub fn announce_scope(&self, relay: &str, token: &str) -> Result<Vec<String>, String> {
		let root = session_root(relay, token)?;
		let root: Vec<&str> = match root.is_empty() {
			true => Vec::new(),
			false => root.split('/').collect(),
		};
		let mut scope = Vec::new();
		for prefix in &self.list {
			let prefix: Vec<&str> = prefix.split('/').collect();
			if root.starts_with(&prefix) {
				return Ok(vec![String::new()]);
			}
			if prefix.starts_with(&root) {
				scope.push(prefix[root.len()..].join("/"));
			}
		}
		Ok(scope)
	}

	/// The prefixes as a message lists them.
	fn listed(&self) -> String {
		match self.list.is_empty() {
			true => "it was given none".to_string(),
			false => self
				.list
				.iter()
				.map(|prefix| format!("'{prefix}'"))
				.collect::<Vec<_>>()
				.join(", "),
		}
	}
}

/// [`Prefixes::check`] against the prefixes the host set, which passes
/// everything where it set none.
pub fn check(relay: &str, token: &str, broadcast: &str) -> Result<(), String> {
	match Prefixes::from_env() {
		Some(prefixes) => prefixes.check(relay, token, broadcast),
		None => Ok(()),
	}
}

/// The origin a subscriber session for `relay` and `token` announces
/// into, for a reader of `broadcast`.
///
/// Without prefixes it is the whole of the session's root, which is what
/// a session asks the relay to announce: every broadcast under it. With
/// them it is scoped to [`Prefixes::announce_scope`], so the session asks
/// only for those, and a name outside them is refused first.
pub fn subscriber_origin(
	prefixes: Option<&Prefixes>,
	relay: &str,
	token: &str,
	broadcast: &str,
) -> Result<moq_net::origin::Producer, String> {
	let origin = moq_net::Origin::random().produce();
	let Some(prefixes) = prefixes else {
		return Ok(origin);
	};
	prefixes.check(relay, token, broadcast)?;
	let scope = prefixes.announce_scope(relay, token)?;
	let paths: Vec<moq_net::Path> = scope.iter().map(|path| moq_net::Path::new(path)).collect();
	origin.scope(&paths).ok_or_else(|| {
		format!(
			"none of the prefixes this run may use ({}) is under the root this session opens at; \
			 the host running this query sets them in {VARIABLE}",
			prefixes.listed()
		)
	})
}

/// The path the relay sees for `broadcast` on a session opened for
/// `relay` and `token`: the session's root, then the broadcast.
pub fn relay_path(relay: &str, token: &str, broadcast: &str) -> Result<String, String> {
	Ok(join(&session_root(relay, token)?, broadcast))
}

/// The root a session opened for `relay` and `token` works under on the
/// relay: the path its SETUP asks for, without the query a relay splits
/// off and without the leading slash.
fn session_root(relay: &str, token: &str) -> Result<String, String> {
	let asked = crate::relay::session_path(relay, token)?;
	let path = asked
		.split_once('?')
		.map_or(asked.as_str(), |(path, _)| path);
	Ok(path.strip_prefix('/').unwrap_or(path).to_string())
}

fn join(root: &str, broadcast: &str) -> String {
	match root.is_empty() {
		true => broadcast.to_string(),
		false => format!("{root}/{broadcast}"),
	}
}

/// Whether `prefix` covers a path of `segments`, segment for segment.
fn covers(prefix: &str, segments: &[&str]) -> bool {
	let prefix: Vec<&str> = prefix.split('/').collect();
	segments.starts_with(&prefix)
}

/// The session's root as a message may print it. A root that is the token
/// is printed the way a token always is; any other is a URL's path, which
/// can carry a token too, so a long segment of it is printed by its ends.
fn shown_root(root: &str, token: &str) -> String {
	if root.is_empty() {
		return String::new();
	}
	if !token.is_empty() && root == token {
		return format!("<token {}>", crate::relay::shorten(token));
	}
	root.split('/')
		.map(|segment| match segment.chars().count() > SHOWN_WHOLE {
			true => crate::relay::shorten(segment),
			false => segment.to_string(),
		})
		.collect::<Vec<_>>()
		.join("/")
}

#[cfg(test)]
mod tests {
	use super::*;

	const RELAY: &str = "moqt://127.0.0.1:4443";
	const JWT: &str = "eyJhbGciOiJIUzI1NiJ9.eyJyb290IjoibGl2ZSJ9.qSsp0wLyaMJ_Nv0";

	fn allowed(prefixes: &str, relay: &str, token: &str, broadcast: &str) -> bool {
		Prefixes::parse(prefixes)
			.check(relay, token, broadcast)
			.is_ok()
	}

	#[test]
	fn a_value_is_split_trimmed_and_emptied_of_blanks() {
		assert_eq!(
			Prefixes::parse(" jobs/a , ,jobs/b/,/jobs/c,, / ").list,
			vec!["jobs/a", "jobs/b", "jobs/c"]
		);
		assert!(Prefixes::parse("").list.is_empty());
		assert!(Prefixes::parse(" , ,").list.is_empty());
	}

	#[test]
	fn the_path_is_the_url_path_then_the_token_then_the_broadcast() {
		assert_eq!(relay_path(RELAY, "", "jobs/a/out").unwrap(), "jobs/a/out");
		assert_eq!(
			relay_path(&format!("{RELAY}/jobs/a"), "", "out").unwrap(),
			"jobs/a/out"
		);
		// A URL with no path roots the session at the token.
		assert_eq!(relay_path(RELAY, "jobs", "a/out").unwrap(), "jobs/a/out");
		assert_eq!(relay_path(RELAY, JWT, "out").unwrap(), format!("{JWT}/out"));
		// The token spelled in the URL's path is the same root.
		assert_eq!(
			relay_path(&format!("{RELAY}/{JWT}"), JWT, "out").unwrap(),
			format!("{JWT}/out")
		);
		// A '?jwt=' rides the query, which a relay splits off the root.
		assert_eq!(
			relay_path(&format!("{RELAY}/jobs/a?jwt={JWT}"), JWT, "out").unwrap(),
			"jobs/a/out"
		);
		assert_eq!(
			relay_path(&format!("{RELAY}/?jwt={JWT}"), "", "out").unwrap(),
			"out"
		);
		// A bare slash asks for no root.
		assert_eq!(
			relay_path(&format!("{RELAY}/"), "", "jobs/a/out").unwrap(),
			"jobs/a/out"
		);
	}

	#[test]
	fn a_prefix_matches_whole_segments() {
		for prefixes in ["jobs/abc", "jobs/abc/"] {
			assert!(allowed(prefixes, RELAY, "", "jobs/abc"));
			assert!(allowed(prefixes, RELAY, "", "jobs/abc/out"));
			assert!(allowed(prefixes, RELAY, "", "jobs/abc/out/1080p"));
			assert!(!allowed(prefixes, RELAY, "", "jobs/abcd"));
			assert!(!allowed(prefixes, RELAY, "", "jobs/abcd/out"));
			assert!(!allowed(prefixes, RELAY, "", "jobs"));
			assert!(!allowed(prefixes, RELAY, "", "other/jobs/abc"));
		}
		// Any one of several will do.
		assert!(allowed("jobs/a,jobs/b", RELAY, "", "jobs/b/out"));
		assert!(!allowed("jobs/a,jobs/b", RELAY, "", "jobs/c/out"));
	}

	#[test]
	fn the_url_path_and_the_token_count() {
		// The broadcast alone is under the prefix, the path the relay sees
		// is not.
		assert!(!allowed("jobs/a", &format!("{RELAY}/jobs/b"), "", "out"));
		assert!(!allowed(
			"jobs/a",
			&format!("{RELAY}/jobs/b"),
			"",
			"jobs/a/out"
		));
		assert!(!allowed("jobs/a", RELAY, "jobs/b", "out"));
		assert!(!allowed("jobs/a", RELAY, JWT, "jobs/a/out"));
		// And they count toward a name that is allowed.
		assert!(allowed("jobs/a", &format!("{RELAY}/jobs/a"), "", "out"));
		assert!(allowed("jobs/a", &format!("{RELAY}/jobs"), "", "a/out"));
		assert!(allowed("jobs/a", RELAY, "jobs", "a/out"));
		assert!(allowed(
			"jobs/a",
			&format!("{RELAY}/jobs/a?jwt={JWT}"),
			JWT,
			"out"
		));
	}

	#[test]
	fn set_but_empty_refuses_everything() {
		for value in ["", " ", ",", "/"] {
			let err = Prefixes::parse(value)
				.check(RELAY, "", "jobs/a/out")
				.expect_err("a refusal");
			assert!(err.contains("it was given none"), "{err}");
		}
	}

	#[test]
	fn empty_dot_and_dot_dot_segments_are_refused() {
		for broadcast in [
			"jobs/a//out",
			"jobs/a/./out",
			"jobs/a/../b/out",
			"jobs/a/",
			"/jobs/a/out",
			"jobs/a/..",
			"",
		] {
			let err = Prefixes::parse("jobs/a, ")
				.check(RELAY, "", broadcast)
				.expect_err("a refusal");
			assert!(
				err.contains("an empty, '.' or '..' segment"),
				"{broadcast}: {err}"
			);
		}
		// In the URL's path too, and in the token.
		assert!(!allowed("jobs/a", &format!("{RELAY}/jobs/a/"), "", "out"));
		assert!(!allowed("jobs", &format!("{RELAY}/jobs/../b"), "", "out"));
		assert!(!allowed("jobs", RELAY, "jobs/.", "out"));
	}

	#[test]
	fn a_url_and_a_token_that_disagree_are_refused_as_before() {
		let err = Prefixes::parse("jobs")
			.check(&format!("{RELAY}/jobs/a"), "jobs/b", "out")
			.expect_err("a refusal");
		assert!(err.contains("the token argument names another"), "{err}");
	}

	#[test]
	fn a_refusal_names_the_path_and_the_prefixes_and_the_host() {
		let err = Prefixes::parse("jobs/a, jobs/b")
			.check(&format!("{RELAY}/jobs/c"), "", "out")
			.expect_err("a refusal");
		assert!(err.contains("'jobs/c/out'"), "{err}");
		assert!(err.contains("'jobs/a', 'jobs/b'"), "{err}");
		assert!(err.contains(VARIABLE) && err.contains("host"), "{err}");
	}

	#[test]
	fn a_refusal_prints_no_token_whole() {
		let err = Prefixes::parse("jobs/a")
			.check(RELAY, JWT, "out")
			.expect_err("a refusal");
		assert!(!err.contains(JWT), "{err}");
		assert!(err.contains("'<token eyJh..._Nv0>/out'"), "{err}");
		let err = Prefixes::parse("jobs/a")
			.check(RELAY, "secret", "out")
			.expect_err("a refusal");
		assert!(!err.contains("secret"), "{err}");
		// A long segment of a URL's path is printed by its ends, a short
		// one whole.
		let err = Prefixes::parse("jobs/a")
			.check(&format!("{RELAY}/live/{JWT}"), "", "out")
			.expect_err("a refusal");
		assert!(!err.contains(JWT), "{err}");
		assert!(err.contains("'live/eyJh..._Nv0/out'"), "{err}");
	}

	#[test]
	fn the_announce_scope_is_relative_to_the_session_root() {
		let scope = |prefixes: &str, relay: &str, token: &str| {
			Prefixes::parse(prefixes)
				.announce_scope(relay, token)
				.unwrap()
		};
		// No root: the prefixes as they are.
		assert_eq!(scope("jobs/a, jobs/b", RELAY, ""), vec!["jobs/a", "jobs/b"]);
		// A root above a prefix: the rest of the prefix under it, and
		// nothing for a prefix it does not reach.
		assert_eq!(
			scope("jobs/a, live/x", &format!("{RELAY}/jobs"), ""),
			vec!["a"]
		);
		assert_eq!(scope("jobs/a/b", RELAY, "jobs"), vec!["a/b"]);
		// A root at or below a prefix: all of the root.
		assert_eq!(scope("jobs/a", &format!("{RELAY}/jobs/a"), ""), vec![""]);
		assert_eq!(
			scope("jobs, live", &format!("{RELAY}/jobs/a/b"), ""),
			vec![""]
		);
		// A root no prefix reaches.
		assert!(scope("jobs/a", &format!("{RELAY}/live"), "").is_empty());
		assert!(scope("jobs/a", &format!("{RELAY}/jobs/ab"), "").is_empty());
	}

	fn interest(origin: &moq_net::origin::Producer) -> Vec<String> {
		origin.allowed().map(|path| path.to_string()).collect()
	}

	#[test]
	fn a_subscriber_asks_for_its_whole_root_without_prefixes() {
		let origin = subscriber_origin(None, &format!("{RELAY}/jobs/b"), "", "x").unwrap();
		assert_eq!(interest(&origin), vec![""]);
	}

	#[test]
	fn a_subscriber_asks_for_its_prefixes_alone() {
		let prefixes = Prefixes::parse("jobs/a, jobs/b, live");
		let origin = subscriber_origin(Some(&prefixes), RELAY, "", "jobs/a/out").unwrap();
		assert_eq!(interest(&origin), vec!["live", "jobs/a", "jobs/b"]);
		let origin =
			subscriber_origin(Some(&prefixes), &format!("{RELAY}/jobs"), "", "a/out").unwrap();
		assert_eq!(interest(&origin), vec!["a", "b"]);
		let origin =
			subscriber_origin(Some(&prefixes), &format!("{RELAY}/jobs/a"), "", "out").unwrap();
		assert_eq!(interest(&origin), vec![""]);
		// The broadcast it reads is under what it asks for, as the session
		// root sees it.
		let consumer = origin.consume();
		assert!(consumer
			.allowed()
			.any(|path| moq_net::Path::new("out").has_prefix(path)));
	}

	#[test]
	fn a_subscriber_outside_its_prefixes_is_refused_before_it_dials() {
		let prefixes = Prefixes::parse("jobs/a");
		assert!(subscriber_origin(Some(&prefixes), RELAY, "", "jobs/b/out").is_err());
		assert!(subscriber_origin(Some(&prefixes), &format!("{RELAY}/jobs/b"), "", "out").is_err());
		assert!(subscriber_origin(Some(&Prefixes::parse("")), RELAY, "", "out").is_err());
	}
}

//! Writes a fresh self-signed localhost certificate into the directory
//! named by the first argument - `cert.pem` and `key.pem` for the
//! relay - and prints the certificate DER as hex, which is what the
//! guests trust explicitly. Any further arguments are additional
//! subject names, for a relay reached by hostname.

use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
	let mut args = std::env::args().skip(1);
	let dir = args.next().map(PathBuf::from).ok_or("usage: certgen <dir> [name ...]")?;
	std::fs::create_dir_all(&dir)?;
	// The guests dial the relay by IP, so the certificate carries the
	// loopback address as well as the name.
	let mut names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
	names.extend(args);
	let cert = rcgen::generate_simple_self_signed(names)?;
	std::fs::write(dir.join("cert.pem"), cert.serialize_pem()?)?;
	std::fs::write(dir.join("key.pem"), cert.serialize_private_key_pem())?;
	let der = cert.serialize_der()?;
	let hex: String = der.iter().map(|b| format!("{b:02x}")).collect();
	println!("{hex}");
	Ok(())
}

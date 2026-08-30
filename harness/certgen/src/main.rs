//! Writes a fresh self-signed localhost certificate into the directory
//! named by the first argument - `cert.pem` and `key.pem` for the
//! relay - and prints the certificate DER as hex, which is what the
//! guests trust explicitly.

use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
	let dir = std::env::args()
		.nth(1)
		.map(PathBuf::from)
		.ok_or("usage: certgen <dir>")?;
	std::fs::create_dir_all(&dir)?;
	// The guests dial the relay by IP, so the certificate carries the
	// loopback address as well as the name.
	let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
	std::fs::write(dir.join("cert.pem"), cert.serialize_pem()?)?;
	std::fs::write(dir.join("key.pem"), cert.serialize_private_key_pem())?;
	let der = cert.serialize_der()?;
	let hex: String = der.iter().map(|b| format!("{b:02x}")).collect();
	println!("{hex}");
	Ok(())
}

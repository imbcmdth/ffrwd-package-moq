// Puts the `ffrwd:av` wit where `wit_bindgen::generate!` reads it, from
// whichever of the two sources is available: FFRWD_WIT_DIR when the
// environment names one, else the `ffrwd/wasm` package installed here.
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

const WIT_DIR_ENV: &str = "FFRWD_WIT_DIR";
const WIT_PACKAGE: &str = "ffrwd/wasm";
const WIT_FILE: &str = "av.wit";

fn main() {
    println!("cargo::rerun-if-env-changed={WIT_DIR_ENV}");
    let source = match env::var_os(WIT_DIR_ENV) {
        Some(named) => PathBuf::from(named),
        None => installed_wit_dir(),
    }
    .join(WIT_FILE);
    println!("cargo::rerun-if-changed={}", source.display());

    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let wit = manifest.join("wit");
    fs::create_dir_all(&wit).expect("create wit/");
    fs::copy(&source, wit.join(WIT_FILE))
        .unwrap_or_else(|err| panic!("copy {}: {err}", source.display()));
}

/// The `wit` directory of the installed `ffrwd/wasm` package, asked of ffrwd.
fn installed_wit_dir() -> PathBuf {
    let asked = Command::new("ffrwd")
        .args(["path", WIT_PACKAGE])
        .output()
        .unwrap_or_else(|err| {
            panic!("`ffrwd path {WIT_PACKAGE}` could not be run ({err}); set {WIT_DIR_ENV} instead")
        });
    if !asked.status.success() {
        panic!(
            "`ffrwd path {WIT_PACKAGE}` failed: {}",
            String::from_utf8_lossy(&asked.stderr).trim()
        );
    }
    let printed = String::from_utf8(asked.stdout).expect("a path, in utf-8");
    PathBuf::from(printed.trim()).join("wit")
}

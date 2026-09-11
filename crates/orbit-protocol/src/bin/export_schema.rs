//! Writes the protocol JSON Schema document to the path given as the first argument, or stdout.

use std::io::Write;

fn main() {
    let doc = orbit_protocol::export_document();
    let text = serde_json::to_string_pretty(&doc).expect("serializable") + "\n";
    match std::env::args().nth(1) {
        Some(path) => {
            if let Some(parent) = std::path::Path::new(&path).parent() {
                std::fs::create_dir_all(parent).expect("create output dir");
            }
            std::fs::write(&path, text).expect("write schema");
            eprintln!("wrote {path}");
        }
        None => {
            std::io::stdout().write_all(text.as_bytes()).expect("stdout");
        }
    }
}

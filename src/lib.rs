//! meshpool: pool the compute of untrusted devices. See docs/architecture.md.

pub mod agent;
pub mod check;
pub mod client;
pub mod coordinator;
pub mod proto;
pub mod sandbox;
pub mod store;

/// Read a task module: `.wat` text is compiled to the binary format devices accept.
pub fn load_module(path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let data = std::fs::read(path)?;
    Ok(if path.extension().is_some_and(|e| e == "wat") {
        wat::parse_bytes(&data)?.into_owned()
    } else {
        data
    })
}

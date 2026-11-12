//! Generates the Agent Substrate control API client from
//! proto/substrate/api/ateapi/v1/ateapi.proto.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Laid out as in Substrate's repository, whose imports are relative to
    // its root.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto/substrate");
    let proto = root.join("api/ateapi/v1/ateapi.proto");
    println!("cargo:rerun-if-changed={}", proto.display());

    let fds = protox::compile([&proto], [&root])?;
    // Servers get default Unimplemented methods so tests can fake the few
    // RPCs they need.
    tonic_prost_build::configure()
        .build_server(true)
        .generate_default_stubs(true)
        .compile_fds(fds)?;
    Ok(())
}

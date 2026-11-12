//! Generates the AX v1alpha1 types and gRPC service from AX's protos, copied
//! unchanged to proto/pkg/apis/v1alpha1: ax.proto holds the service and
//! imports the per-resource files (common, task, workspace, gateway and
//! model.proto).
//!
//! gateway.proto imports Agent Substrate's `api/ateapi/v1/ateapi.proto` for its
//! egress rules. AX links Substrate's Go module to .tmp/buf/substrate for
//! that; here the file is copied to proto/substrate, the second import root.
//! Only its messages are generated, into `rax_api::substrate::ateapi::v1`: the
//! Control service client lives in rax-substrate.
//!
//! protox compiles the proto in pure Rust, so no protoc is needed. prost and
//! tonic generate the messages and service; pbjson generates serde impls that
//! follow the protobuf JSON mapping (lowerCamelCase names, json_name, omitted
//! defaults), the same wire format protojson gives the Go implementation.

use std::{env, path::PathBuf};

const SUBSTRATE_PACKAGE: &str = ".substrate.ateapi.v1";
const SUBSTRATE_RUST: &str = "crate::substrate::ateapi::v1";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let substrate = root.join("substrate");
    // AX's imports are relative to its repository root, as here.
    let dir = root.join("pkg/apis/v1alpha1");
    let proto = dir.join("ax.proto");
    println!("cargo:rerun-if-changed={}", dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        substrate.join("api/ateapi/v1/ateapi.proto").display()
    );

    let fds = protox::compile([&proto], [&root, &substrate])?;
    let out = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out.join("ax_descriptor.bin");
    std::fs::write(&descriptor_path, prost::Message::encode_to_vec(&fds))?;

    // Substrate's messages, without its service: prost alone generates no
    // services, and AX's own package is generated below.
    // It goes to its own directory: tonic writes a services-only file for the
    // package below.
    let substrate_out = out.join("substrate");
    std::fs::create_dir_all(&substrate_out)?;
    let mut config = prost_build::Config::new();
    config
        .out_dir(&substrate_out)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .extern_path(".ax.v1alpha1", "crate::v1alpha1");
    config.compile_fds(fds.clone())?;

    // AX's messages and service.
    let mut config = prost_build::Config::new();
    config
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .extern_path(SUBSTRATE_PACKAGE, SUBSTRATE_RUST);
    tonic_prost_build::configure().compile_fds_with_config(fds, config)?;

    let descriptors = std::fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors)?
        .extern_path(SUBSTRATE_PACKAGE, SUBSTRATE_RUST)
        .build(&[".ax.v1alpha1"])?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors)?
        .out_dir(&substrate_out)
        .build(&[SUBSTRATE_PACKAGE])?;
    Ok(())
}

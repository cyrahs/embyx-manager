//! Generates the CloudDrive gRPC client from `proto/clouddrive.fds.bin`.
//!
//! The descriptor set is extracted from the baseline's `clouddrive_pb2.py` by
//! `tools/codegen/clouddrive_descriptor.py` (CI checks it is current). Only the 14 RPCs
//! embyx-manager calls are kept in the service, so the generated client cannot reach the
//! rest of CloudDrive's API (198 methods) by accident. No `protoc` is involved.

use std::path::PathBuf;

use prost::Message as _;

/// The RPCs `clients/clouddrive/client.py` calls, in the order it defines them.
const USED_RPCS: [&str; 14] = [
    "GetSystemInfo",
    "GetSubFiles",
    "CreateFolder",
    "RenameFile",
    "MoveFile",
    "CopyFile",
    "GetCopyTasks",
    "GetUploadFileList",
    "RestartCopyTask",
    "CancelCopyTask",
    "DeleteFiles",
    "AddOfflineFiles",
    "ListOfflineFilesByPath",
    "RemoveOfflineFiles",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/clouddrive.fds.bin");
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_FEATURE_CLOUDDRIVE").is_none() {
        return Ok(());
    }
    let bytes = std::fs::read("proto/clouddrive.fds.bin")?;
    let mut set = prost_types::FileDescriptorSet::decode(bytes.as_slice())?;
    let mut kept = Vec::new();
    for file in &mut set.file {
        for service in &mut file.service {
            service
                .method
                .retain(|method| method.name.as_deref().is_some_and(|name| USED_RPCS.contains(&name)));
            kept.extend(service.method.iter().filter_map(|method| method.name.clone()));
        }
    }
    let mut missing: Vec<&str> = USED_RPCS
        .iter()
        .copied()
        .filter(|name| !kept.iter().any(|kept| kept == name))
        .collect();
    missing.sort_unstable();
    if !missing.is_empty() || kept.len() != USED_RPCS.len() {
        return Err(
            format!("descriptor does not have exactly the used RPCs: missing {missing:?}, kept {kept:?}").into(),
        );
    }
    let out = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out)
        .compile_fds(set)?;
    Ok(())
}

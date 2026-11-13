//! External services of embyx-manager and the one place their HTTP clients are built.
//!
//! In WP0: [`net`] (the httpx-shaped client, retry ladders, the subprocess runner and, behind
//! `test-support`, the fake web; copied from fav) and [`clouddrive::proto`], the gRPC code
//! generated from the baseline's descriptor (feature `clouddrive`, on by default). WP2 adds
//! JavBus, sukebei, feeds, AVBase, jinjier and Emby; WP3 the CloudDrive wrapper and the
//! Kubernetes Jobs client.

#[cfg(feature = "clouddrive")]
pub mod clouddrive;
pub mod net;

#[cfg(test)]
mod probes;

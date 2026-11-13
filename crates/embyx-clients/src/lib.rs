//! External services of embyx-manager and the one place their HTTP clients are built.
//!
//! In WP0: [`net`] (the httpx-shaped client, retry ladders, the subprocess runner and, behind
//! `test-support`, the fake web; copied from fav) and [`clouddrive::proto`], the gRPC code
//! generated from the baseline's descriptor (feature `clouddrive`, on by default). WP2 adds
//! JavBus, sukebei, feeds, AVBase, jinjier and Emby; WP3 the CloudDrive client and wrapper
//! ([`clouddrive::aio`]) and the Kubernetes Jobs client ([`kube`]), and behind `test-support` the
//! scenario fakes ([`clouddrive::fake`], [`fakehttp`], [`scenario`]).

#[cfg(feature = "clouddrive")]
pub mod clouddrive;
#[cfg(any(test, feature = "test-support"))]
pub mod fakehttp;
pub mod kube;
pub mod net;
#[cfg(all(feature = "clouddrive", any(test, feature = "test-support")))]
pub mod scenario;

#[cfg(test)]
mod probes;

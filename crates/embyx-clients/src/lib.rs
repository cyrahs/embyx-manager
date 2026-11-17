//! External services of embyx-manager and the one place their HTTP clients are built.
//!
//! In WP0: [`net`] (the httpx-shaped client, retry ladders, the subprocess runner and, behind
//! `test-support`, the fake web; copied from fav) and [`clouddrive::proto`], the gRPC code
//! generated from the baseline's descriptor (feature `clouddrive`, on by default). WP2 added
//! the site clients ([`javbus`], [`sukebei`], [`rss_magnet`], [`feeds`], [`avbase`],
//! [`jinjier`], [`emby`]) and the Python-library layers they stand on ([`http`] for httpx,
//! [`pyquery`] for pyquery/lxml, [`etree`] for defusedxml, [`size`] for humanfriendly,
//! [`tenacity`], [`pyvalue`] for walking JSON as Python does). WP3 added the CloudDrive client
//! and wrapper ([`clouddrive::aio`]) and the Kubernetes Jobs client ([`kube`]), and behind
//! `test-support` the scenario fakes ([`clouddrive::fake`], [`fakehttp`], [`scenario`]).

pub mod avbase;
#[cfg(feature = "clouddrive")]
pub mod clouddrive;
pub mod emby;
pub mod etree;
#[cfg(any(test, feature = "test-support"))]
pub mod fakehttp;
pub mod feeds;
pub mod http;
pub mod javbus;
pub mod jinjier;
pub mod kube;
pub mod net;
pub mod pyquery;
pub mod pyvalue;
pub mod rss_magnet;
#[cfg(all(feature = "clouddrive", any(test, feature = "test-support")))]
pub mod scenario;
pub mod size;
pub mod sukebei;
pub mod tenacity;

#[cfg(test)]
mod probes;

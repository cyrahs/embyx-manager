//! Multi-part merges.
//!
//! Python origin: `src/embyx_manager/merge/` without `home_worker.py` (which is
//! `embyx-transcode-worker`) and `kube.py` (which is in `embyx-clients`). WP13 ported
//! `detect.py` ([`detect`]), `tasks.py` ([`tasks`]), `worker.py` ([`worker`], the
//! `merge-worker` body) and the path helpers of `transcode.py` the worker reads
//! ([`transcode`]); WP14 adds the service, auto-merge, transcode bookkeeping and the merge API.

#[cfg(feature = "db")]
pub mod auto;
#[cfg(feature = "db")]
pub mod catalog;
pub mod detect;
#[cfg(feature = "test-support")]
pub mod fakes;
#[cfg(feature = "db")]
pub mod ports;
mod py;
#[cfg(feature = "db")]
pub mod service;
#[cfg(feature = "db")]
pub mod tasks;
pub mod transcode;
pub mod worker;

pub use py::PyException;

/// Python `float(text)` for a command-line value (`argparse`'s `type=float`): `None` where
/// Python raises `ValueError`.
pub fn parse_float(text: &str) -> Option<f64> {
    py::float_from_str(text)
}

//! The home transcode worker: `embyx-manager transcode-worker`.
//!
//! Python origin: `src/embyx_manager/merge/home_worker.py`. It runs by hand in a terminal
//! on the PC with the NVIDIA card (Windows or Linux) and builds there without database or
//! gRPC dependencies. It asks the cluster, through the WireGuard tunnel, for a title
//! waiting to be re-encoded, then for up to `jobs` parts at once:
//!
//! * ffmpeg reads the part over HTTP ranges from the cluster, decodes on the GPU and
//!   encodes the video only with NVENC AV1, cutting it into chunks of about a minute that
//!   start on a keyframe;
//! * each finished chunk is checked (frames, SHA-256) and uploaded, one at a time and no
//!   faster than `upload_mbps`; when `max_queue_gb` of chunks wait, the encoders are
//!   suspended until the queue drains;
//! * a part whose chunks are all received is reported finished.
//!
//! Ctrl+C once lets every encoder finish its current chunk, uploads what is done and
//! exits; a second Ctrl+C exits at once. Either way the next start carries on from the
//! last chunk the cluster has. Pausing on the merge page does the same as the first Ctrl+C
//! but waits for a resume instead of exiting.
//!
//! The protocol with the cluster, the cache layout and the configuration are cross-version
//! contract 5 (`docs/rewrite/PLAN.md`): the Python worker works against the Rust listener
//! and this worker against the Python one.

pub mod chunks;
pub mod config;
pub mod console;
pub mod ffmpeg;
pub mod process;
pub mod py;
pub mod run;
pub mod server;
pub mod task;

#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod probes;
#[cfg(test)]
mod testing;

pub use config::{Args, WorkerConfig};
pub use run::main;

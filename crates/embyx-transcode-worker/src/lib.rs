//! The home transcode worker.
//!
//! Python origin: `src/embyx_manager/merge/home_worker.py`. It runs by hand on a Windows PC
//! and builds on Windows and Linux without database or gRPC dependencies. Empty in WP0 apart
//! from the API-coverage probe for process control (`probes`, tests only), which CI also
//! compiles on Windows; WP15 ports the worker and its `process` module, the one place where
//! `unsafe` may be allowed.

#[cfg(test)]
mod probes;

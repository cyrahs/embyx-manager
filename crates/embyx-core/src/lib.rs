//! Foundation shared by every other embyx-manager crate.
//!
//! In WP0 it holds the Python-compatible helpers copied from fav (docs/CONVENTIONS.md, "Code
//! copied from fav"): JSON read and written like Python ([`pyjson`]), Python's string
//! predicates ([`pytext`]), `urllib.parse` ([`pyurl`]), `pathlib.PurePosixPath` ([`pypath`])
//! and Python `re` patterns ([`pyre`]). WP1 adds the env settings, config models, errors,
//! the AVID parser, magnets and media helpers; WP3 `posixpath` ([`posixpath`]).
//!
//! Behind the `test-support` feature: [`golden`] reads the Python-generated goldens and
//! [`scenario`] (Unix only) is the Rust side of the effect transcripts (docs/rewrite/SCENARIOS.md).

pub mod avid;
pub mod config;
pub mod cron;
pub mod errors;
#[cfg(any(test, feature = "test-support"))]
pub mod golden;
pub mod magnet;
pub mod media;
pub mod posixpath;
pub mod pyjson;
pub mod pypath;
pub mod pyre;
pub mod pytext;
pub mod pyurl;
// The transcripts record Unix file metadata (links, symlinks); the scenarios run on Linux only.
#[cfg(all(unix, any(test, feature = "test-support")))]
pub mod scenario;
pub mod settings;

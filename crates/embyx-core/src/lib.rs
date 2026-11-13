//! Foundation shared by every other embyx-manager crate.
//!
//! In WP0 it holds the Python-compatible helpers copied from fav (docs/CONVENTIONS.md, "Code
//! copied from fav"): JSON read and written like Python ([`pyjson`]), Python's string
//! predicates ([`pytext`]), `urllib.parse` ([`pyurl`]), `pathlib.PurePosixPath` ([`pypath`])
//! and Python `re` patterns ([`pyre`]). WP1 adds the env settings, config models, errors,
//! the AVID parser, magnets and media helpers.
//!
//! Behind the `test-support` feature: [`golden`] reads the Python-generated goldens and
//! [`scenario`] is the Rust side of the effect transcripts (docs/rewrite/SCENARIOS.md).

#[cfg(any(test, feature = "test-support"))]
pub mod golden;
pub mod pyjson;
pub mod pypath;
pub mod pyre;
pub mod pytext;
pub mod pyurl;
#[cfg(any(test, feature = "test-support"))]
pub mod scenario;

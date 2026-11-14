//! HTTP API of embyx-manager: the shell every feature's routes sit in, and `serve`.
//!
//! Python origin: `api.py`, `config/api.py`, the shell of `bootstrap.py` and `_serve` in
//! `__main__.py` (WP4); `monitor/api.py`, `monitor/playlists_api.py`, `fill_actor/api.py`,
//! `merge/api.py` and `merge/transcode_api.py` bring their routers in later packages.
//!
//! The dispatcher is Starlette's ([`routing`]), not axum's router: routing on the decoded
//! path, the SPA mount catching every unmatched path, 405/redirect answers only without a
//! front end. axum (hyper) is the HTTP server underneath ([`serve`]).

pub mod app;
pub mod auth;
pub mod body;
pub mod bootstrap;
pub mod config_api;
pub mod fill_actor_api;
pub mod http;
pub mod logging;
pub mod merge_api;
pub mod mimetypes;
pub mod monitor_api;
pub mod openapi;
pub mod params;
pub mod playlists_api;
pub mod routing;
pub mod serve;
pub mod static_files;
#[cfg(test)]
mod testing;
pub mod transcode_api;

//! HTTP API of embyx-manager.
//!
//! Python origin: `api.py`, `config/api.py`, `monitor/api.py`, `monitor/playlists_api.py`,
//! `fill_actor/api.py`, `merge/api.py`, `merge/transcode_api.py` and the shell of
//! `bootstrap.py`. Empty in WP0; WP4 brings the axum shell (two listeners, bearer auth,
//! health, the 64 KiB limit, SPA hosting), later packages add their routers.

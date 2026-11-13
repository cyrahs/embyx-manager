//! Multi-part merges.
//!
//! Python origin: `src/embyx_manager/merge/` without `home_worker.py` (which is
//! `embyx-transcode-worker`) and `kube.py` (which is in `embyx-clients`). Empty in WP0;
//! WP13 ports `detect.py`, `tasks.py` and `worker.py` (the `merge-worker` body), WP14 the
//! service, auto-merge, transcode bookkeeping and the merge API.

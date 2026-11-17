//! What the fill-actor service needs from the rest of the app.
//!
//! Python origin: `src/embyx_manager/fill_actor/ports.py`: the actor catalog, the
//! acquisition gateway and the brand resolver, as protocols the service is built with.
//! [`crate::adapters`] has the implementations the app wires in.

use chrono::NaiveDate;
use indexmap::IndexMap;

use crate::cloud_moves::BoxFuture;
use crate::models::str_enum;

/// `PageProgressCallback`: `await progress_callback(completed, total, current)`.
pub type PageProgressCallback = embyx_clients::javbus::PageProgress;

/// `CatalogListing`: what the catalogs know about one actor: who they are and what they are
/// credited in.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogListing {
    /// The catalog's primary name for the actor, when it knows them.
    pub actor_name: Option<String>,
    /// The AVBase talent id, when AVBase knows them.
    pub talent_id: Option<i128>,
    pub aliases: Vec<String>,
    /// Every credited video id, as the sources spell them.
    pub video_ids: Vec<String>,
    /// Release dates by video id, for the ones a source dated (a `dict`: insertion order).
    pub release_dates: IndexMap<String, NaiveDate>,
    /// How many ids each source contributed, for the plan's record (`-1`: the source failed).
    pub source_counts: IndexMap<String, i64>,
}

/// Why a catalog listing failed: the exception's class and `str(exc)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogError {
    pub class: String,
    pub message: String,
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CatalogError {}

/// `ActorCatalog`: lists the videos an actor is credited in.
pub trait ActorCatalog: Send + Sync {
    fn list_videos<'a>(
        &'a self,
        actor_ref: &'a str,
        progress_callback: Option<&'a PageProgressCallback>,
    ) -> BoxFuture<'a, Result<CatalogListing, CatalogError>>;
}

str_enum! {
    /// `AcquisitionOutcome`: how far a missing video got into the download tracking system.
    ///
    /// `Queued` and `AlreadyTracked` mirror the monitor intake's outcomes, so the adapter
    /// maps by value; the two `*NotConfigured` values are the adapter's own, naming the
    /// configuration gap that kept it from reaching the intake at all. The rest are legacy
    /// values from when scans resolved magnets and submitted inline, kept for plans persisted
    /// back then.
    AcquisitionOutcome {
        Queued = "queued",
        Submitted = "submitted",
        AlreadyTracked = "already_tracked",
        NoMagnet = "no_magnet",
        SubmitFailed = "submit_failed",
        CloudNotConfigured = "cloud_not_configured",
        TaskDirNotConfigured = "task_dir_not_configured",
    }
}

/// Why handing a video to the ledger failed: the exception's class and `str(exc)`.
pub type AcquisitionError = embyx_monitor::intake::IntakeError;

/// `AcquisitionGateway`: hands a missing video to the acquisition ledger; the tracker owns
/// it afterwards.
pub trait AcquisitionGateway: Send + Sync {
    fn queue_missing<'a>(
        &'a self,
        video_id: &'a str,
        release_date: Option<NaiveDate>,
    ) -> BoxFuture<'a, Result<AcquisitionOutcome, AcquisitionError>>;
}

/// `BrandResolver`: the library brand directory of a video identifier.
pub trait BrandResolver: Send + Sync {
    fn resolve_brand(&self, video_id: &str) -> Option<String>;
}

//! The errors the fill-actor API maps to stable responses.
//!
//! Python origin: `src/embyx_manager/fill_actor/errors.py`: a `FillActorError` subclass per
//! failure, each with the `code` the API puts in its error envelope. The codes are part of
//! the HTTP surface (cross-version contract 8; `goldens/fill_actor/records.json` pins them).
//! `embyx_api::fill_actor_api::service_error_status` maps them to status codes (WP12).

use std::fmt;

/// One `FillActorError` subclass, with the text it was raised with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillActorError {
    pub kind: FillActorErrorKind,
    /// `str(exc)`: the message the code raised it with (often empty).
    pub message: String,
}

impl FillActorError {
    pub fn new(kind: FillActorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// The class's `code`.
    pub fn code(&self) -> &'static str {
        self.kind.code()
    }

    /// The Python class name.
    pub fn class_name(&self) -> &'static str {
        self.kind.class_name()
    }
}

impl fmt::Display for FillActorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FillActorError {}

macro_rules! kinds {
    ($($(#[$meta:meta])* $variant:ident = ($class:literal, $code:literal)),+ $(,)?) => {
        /// The subclasses of `FillActorError`, the base class included.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum FillActorErrorKind {
            $($(#[$meta])* $variant),+
        }

        impl FillActorErrorKind {
            /// Every class, in `errors.py`'s order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => $code),+
                }
            }

            pub fn class_name(self) -> &'static str {
                match self {
                    $(Self::$variant => $class),+
                }
            }
        }
    };
}

kinds! {
    /// The base class: errors that API adapters can map to stable responses.
    Base = ("FillActorError", "fill_actor_error"),
    InvalidActorId = ("InvalidActorIdError", "invalid_actor_id"),
    TooManyActors = ("TooManyActorsError", "too_many_actors"),
    TooManyVideos = ("TooManyVideosError", "too_many_videos"),
    JobQueueFull = ("JobQueueFullError", "job_queue_full"),
    ApplyRequestConflict = ("ApplyRequestConflictError", "apply_request_conflict"),
    ApplyJobNotCancellable = ("ApplyJobNotCancellableError", "apply_job_not_cancellable"),
    UnknownApplyJob = ("UnknownApplyJobError", "unknown_apply_job"),
    MoveDisabled = ("MoveDisabledError", "move_disabled"),
    /// Library roots are needed but the feature has none configured.
    NotConfigured = ("NotConfiguredError", "fill_actor_not_configured"),
    LegacyPlan = ("LegacyPlanError", "legacy_plan_requires_rescan"),
    UnknownPlan = ("UnknownPlanError", "unknown_plan"),
    ExpiredPlan = ("ExpiredPlanError", "expired_plan"),
    RevisionMismatch = ("RevisionMismatchError", "revision_mismatch"),
    UnknownCandidate = ("UnknownCandidateError", "unknown_candidate"),
}

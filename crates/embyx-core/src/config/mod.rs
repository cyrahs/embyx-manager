//! The configuration sections kept in `app_config`: models, validation and the stored form.
//!
//! Python origin: `src/embyx_manager/config/models.py` and the pure half of `config/store.py`
//! (`masked_values`, `secret_flags`). The store itself lives in `embyx-db`.

pub mod dump;
pub mod lax;
pub mod models;
pub mod normalize;

pub use lax::{ErrorDetail, Mode, ValidationError};
pub use models::{
    ArchiveConfig, AvidRulesConfig, CloudDriveConfig, EmbyConfig, FillActorConfig, MappingConfig, MergeConfig,
    PlaylistsConfig, RssCategory, RssConfig, Section, masked_values, secret_flags,
};

use crate::pyjson::Value;

/// One stored section of any kind, for the store's snapshot.
#[derive(Debug, Clone, PartialEq)]
pub enum AnySection {
    CloudDrive(CloudDriveConfig),
    Emby(EmbyConfig),
    Avid(AvidRulesConfig),
    Rss(RssConfig),
    Archive(ArchiveConfig),
    Mapping(MappingConfig),
    Playlists(PlaylistsConfig),
    FillActor(FillActorConfig),
    Merge(MergeConfig),
}

/// A section type registered in `SECTION_MODELS`, convertible to and from [`AnySection`].
pub trait Registered: Section {
    fn wrap(self) -> AnySection;
    fn unwrap(section: &AnySection) -> Option<&Self>;
}

macro_rules! registry {
    ($($variant:ident => $model:ty),* $(,)?) => {
        /// `SECTION_MODELS.keys()`, in registry order.
        pub const SECTION_NAMES: &[&str] = &[$(<$model as Section>::NAME),*];

        $(
            impl Registered for $model {
                fn wrap(self) -> AnySection {
                    AnySection::$variant(self)
                }

                fn unwrap(section: &AnySection) -> Option<&Self> {
                    match section {
                        AnySection::$variant(value) => Some(value),
                        #[allow(unreachable_patterns)]
                        _ => None,
                    }
                }
            }
        )*

        impl AnySection {
            /// `SECTION_MODELS[name]()`: the defaults, or `None` for an unknown section.
            pub fn default_for(name: &str) -> Option<Self> {
                $(if name == <$model as Section>::NAME {
                    return Some(Self::$variant(<$model>::default()));
                })*
                None
            }

            /// `SECTION_MODELS[name].model_validate(value)`.
            pub fn validate(name: &str, value: &Value, mode: Mode) -> Option<Result<Self, ValidationError>> {
                $(if name == <$model as Section>::NAME {
                    return Some(<$model as Section>::validate(value, mode).map(Self::$variant));
                })*
                None
            }

            /// `SECTION_MODELS[name].model_validate_json(text)`.
            pub fn validate_json(name: &str, text: &str) -> Option<Result<Self, ValidationError>> {
                $(if name == <$model as Section>::NAME {
                    return Some(<$model as Section>::validate_json(text).map(Self::$variant));
                })*
                None
            }

            /// The section's registry name.
            pub fn name(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => <$model as Section>::NAME),*
                }
            }

            /// The model's `SECRET_FIELDS`, sorted.
            pub fn secret_fields(&self) -> &'static [&'static str] {
                match self {
                    $(Self::$variant(_) => <$model as Section>::SECRET_FIELDS),*
                }
            }

            /// `model_dump()`.
            pub fn dump(&self) -> Value {
                match self {
                    $(Self::$variant(value) => value.dump()),*
                }
            }

            /// `model_dump_json()`.
            pub fn dump_json(&self) -> String {
                match self {
                    $(Self::$variant(value) => value.dump_json()),*
                }
            }

            /// `masked_values(section)`.
            pub fn masked_values(&self) -> Value {
                match self {
                    $(Self::$variant(value) => masked_values(value)),*
                }
            }

            /// `secret_flags(section)`.
            pub fn secret_flags(&self) -> Vec<(&'static str, bool)> {
                match self {
                    $(Self::$variant(value) => secret_flags(value)),*
                }
            }
        }
    };
}

impl AnySection {
    /// The section's `configured` / `cloud_configured` properties, for those that have them.
    pub fn properties(&self) -> Vec<(&'static str, bool)> {
        match self {
            Self::CloudDrive(value) => vec![("configured", value.configured())],
            Self::Emby(value) => vec![("configured", value.configured())],
            Self::Archive(value) => vec![("configured", value.configured())],
            Self::Mapping(value) => vec![("configured", value.configured())],
            Self::FillActor(value) => vec![
                ("configured", value.configured()),
                ("cloud_configured", value.cloud_configured()),
            ],
            Self::Merge(value) => vec![("configured", value.configured())],
            Self::Avid(_) | Self::Rss(_) | Self::Playlists(_) => Vec::new(),
        }
    }
}

registry! {
    CloudDrive => CloudDriveConfig,
    Emby => EmbyConfig,
    Avid => AvidRulesConfig,
    Rss => RssConfig,
    Archive => ArchiveConfig,
    Mapping => MappingConfig,
    Playlists => PlaylistsConfig,
    FillActor => FillActorConfig,
    Merge => MergeConfig,
}

#[cfg(test)]
mod tests;

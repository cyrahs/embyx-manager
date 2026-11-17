//! The CloudDrive calls the tracker, the replacement (换版) and the manual source make.
//!
//! Python passes `AsyncCloudDrive` itself and the tests pass duck-typed fakes; here the calls
//! are two traits so a test can do the same. [`AsyncCloudDrive`] implements both. Every call
//! runs through the wrapper's own task (cross-version contract 6): a mutation that was sent
//! completes even when the caller's future is dropped, and a caller that cancels a pass and
//! then wants to act on its outcome awaits [`TrackerCloud::settle`] on a
//! [`AsyncCloudDrive::scoped`] handle first.

use embyx_clients::clouddrive::{AsyncCloudDrive, CloudDriveError, CloudFile, OfflineTask};

use crate::intake::SourceError;
use crate::support::BoxFuture;

/// `list_directory(api_dir)`: a fresh listing (`force_refresh=True`, Python's default).
pub trait CloudListing: Send + Sync {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>>;
}

/// What the tracker asks CloudDrive besides listings.
pub trait TrackerCloud: CloudListing {
    /// `list_offline_files(path)`: every offline task queued under one directory.
    fn list_offline_files<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>>;
    /// `remove_offline_files(info_hashes, path, delete_files=)`. The answer is not read.
    fn remove_offline_files<'a>(
        &'a self,
        info_hashes: &'a [String],
        path: &'a str,
        delete_files: bool,
    ) -> BoxFuture<'a, Result<(), SourceError>>;
    /// `delete_files(api_paths)`: into the cloud's recycle bin. The answer is not read.
    fn delete_files<'a>(&'a self, api_paths: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>>;
    /// Wait for every call this handle started, also those whose caller was dropped (what a
    /// cancelled Python caller waits for before it re-raises). Immediate for a handle that is
    /// not scoped and for fakes.
    fn settle(&self) -> BoxFuture<'_, ()>;
}

impl CloudListing for AsyncCloudDrive {
    fn list_directory<'a>(&'a self, api_dir: &'a str) -> BoxFuture<'a, Result<Vec<CloudFile>, SourceError>> {
        Box::pin(async move { Ok(AsyncCloudDrive::list_directory(self, api_dir, true).await?) })
    }
}

impl TrackerCloud for AsyncCloudDrive {
    fn list_offline_files<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Vec<OfflineTask>, SourceError>> {
        Box::pin(async move { Ok(AsyncCloudDrive::list_offline_files(self, path).await?) })
    }

    fn remove_offline_files<'a>(
        &'a self,
        info_hashes: &'a [String],
        path: &'a str,
        delete_files: bool,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(async move {
            AsyncCloudDrive::remove_offline_files(self, info_hashes, path, delete_files).await?;
            Ok(())
        })
    }

    fn delete_files<'a>(&'a self, api_paths: &'a [String]) -> BoxFuture<'a, Result<(), SourceError>> {
        Box::pin(async move {
            AsyncCloudDrive::delete_files(self, api_paths).await?;
            Ok(())
        })
    }

    fn settle(&self) -> BoxFuture<'_, ()> {
        Box::pin(AsyncCloudDrive::settle(self))
    }
}

/// `isinstance(exc, FileNotFoundError)`.
pub fn is_file_not_found(error: &SourceError) -> bool {
    match error {
        SourceError::CloudDrive(error) => error.is_file_not_found(),
        SourceError::Raised(raised) => raised.class == "FileNotFoundError",
        SourceError::Sukebei(_) | SourceError::JavBus(_) => false,
    }
}

/// `isinstance(exc, ValueError)`: the wrapper's path validation and a closed channel.
pub fn is_value_error(error: &SourceError) -> bool {
    match error {
        SourceError::CloudDrive(error) => {
            matches!(error, CloudDriveError::InvalidPath(_) | CloudDriveError::Closed(_))
        }
        SourceError::Raised(raised) => {
            matches!(
                raised.class.as_str(),
                "ValueError" | "UnicodeError" | "UnicodeEncodeError" | "UnicodeDecodeError" | "JSONDecodeError"
            )
        }
        SourceError::Sukebei(_) | SourceError::JavBus(_) => false,
    }
}

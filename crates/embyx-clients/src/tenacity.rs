//! The `tenacity` retry the JavBus and sukebei clients wrap their requests in.
//!
//! Python origin: `@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1,
//! min=1, max=10), retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
//! reraise=True)` on `clients/javbus.py` and `clients/sukebei.py`. An attempt that fails with a
//! retryable error waits `min(max(2 ** (attempt - 1), 1), 10)` seconds (1 s, then 2 s) and runs
//! again; after the third the last error is returned as it is. The wait goes through the
//! injected [`Sleeper`] so tests record it.

use std::future::Future;

use crate::net::retry::Sleeper;

/// Attempts before the last error is returned (`stop_after_attempt(3)`).
pub const ATTEMPTS: u32 = 3;

/// `wait_exponential(multiplier=1, min=1, max=10)` after the `attempt`-th failure (1-based).
pub fn wait_seconds(attempt: u32) -> f64 {
    let exponential = 2_f64.powi(i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX));
    exponential.clamp(1.0, 10.0)
}

/// Run `operation` up to [`ATTEMPTS`] times while it fails with an error `retryable` accepts.
pub async fn retry<T, E, F, Fut>(sleeper: &Sleeper, retryable: impl Fn(&E) -> bool, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 1;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < ATTEMPTS && retryable(&error) => {
                sleeper(wait_seconds(attempt)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::retry::RecordingSleeper;

    #[tokio::test]
    async fn three_attempts_with_one_and_two_seconds_between() {
        let recorder = RecordingSleeper::new();
        let mut calls = 0;
        let result: Result<(), &str> = retry(
            &recorder.sleeper(),
            |_| true,
            || {
                calls += 1;
                async { Err("down") }
            },
        )
        .await;
        assert_eq!(result, Err("down"));
        assert_eq!(calls, 3);
        assert_eq!(recorder.delays(), vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn an_error_that_is_not_retryable_ends_at_once() {
        let recorder = RecordingSleeper::new();
        let mut calls = 0;
        let result: Result<(), &str> = retry(
            &recorder.sleeper(),
            |_| false,
            || {
                calls += 1;
                async { Err("parse") }
            },
        )
        .await;
        assert_eq!(result, Err("parse"));
        assert_eq!(calls, 1);
        assert!(recorder.delays().is_empty());
    }

    #[test]
    fn the_wait_is_capped_at_ten_seconds() {
        assert_eq!(
            (1..=6).map(wait_seconds).collect::<Vec<_>>(),
            [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]
        );
    }
}

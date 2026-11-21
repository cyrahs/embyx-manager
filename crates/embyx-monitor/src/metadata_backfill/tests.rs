//! Metadata backfill: which movies a pass refreshes, in what order and how often.

use std::sync::{Arc, Mutex};

use chrono::{TimeZone as _, Utc};
use embyx_clients::emby::{EmbyError, EmbyMovie};
use embyx_clients::net::retry::RecordingSleeper;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::support::fixed_clock;

#[derive(Default)]
struct FakeServer {
    movies: Vec<EmbyMovie>,
    refreshed: Mutex<Vec<String>>,
    /// Item ids whose refresh fails, and with what.
    failing: Vec<(String, EmbyError)>,
    list_error: Option<EmbyError>,
    /// Fired after this many refreshes.
    stop_after: Option<(usize, CancellationToken)>,
}

impl BackfillServer for FakeServer {
    fn movies(&self) -> BoxFuture<'_, Result<Vec<EmbyMovie>, EmbyError>> {
        Box::pin(async move {
            match &self.list_error {
                Some(error) => Err(error.clone()),
                None => Ok(self.movies.clone()),
            }
        })
    }

    fn refresh_metadata<'a>(&'a self, item_id: &'a str) -> BoxFuture<'a, Result<(), EmbyError>> {
        Box::pin(async move {
            let mut refreshed = self.refreshed.lock().expect("lock");
            refreshed.push(item_id.to_owned());
            if let Some((count, stop)) = &self.stop_after
                && refreshed.len() >= *count
            {
                stop.cancel();
            }
            match self.failing.iter().find(|(id, _)| id == item_id) {
                Some((_, error)) => Err(error.clone()),
                None => Ok(()),
            }
        })
    }
}

fn movie(id: &str, name: &str, created: &str, providers: &[&str]) -> EmbyMovie {
    EmbyMovie {
        item_id: id.to_owned(),
        name: name.to_owned(),
        provider_ids: providers.iter().map(|name| (*name).to_owned()).collect(),
        date_created: created.to_owned(),
    }
}

fn library() -> Vec<EmbyMovie> {
    vec![
        movie("1", "SNOS-475 title", "2026-11-19T21:02:59.0000000Z", &["MetaTube"]),
        movie("2", "JQJ-011", "2026-11-19T21:02:59.0000000Z", &[]),
        movie("3", "DOCP-078 title", "2025-04-19T03:28:22.0000000Z", &["metatube"]),
        movie("4", "ABS-086", "2026-10-21T20:00:23.0000000Z", &["Tmdb"]),
        // Created ten minutes before the pass: Emby's own refresh may still be running.
        movie("5", "NEW-001", "2026-11-20T11:50:00.0000000Z", &[]),
        movie("6", "FC2-1202738", "2025-04-19T03:28:22.0000000Z", &[]),
        // No date: refreshed, after the dated ones.
        movie("7", "NODATE-001", "", &[]),
    ]
}

fn backfill(settings: BackfillSettings) -> (MetadataBackfill, RecordingSleeper) {
    let sleeper = RecordingSleeper::new();
    let now = Utc.with_ymd_and_hms(2026, 11, 20, 12, 0, 0).single().expect("time");
    (
        MetadataBackfill::new(settings, fixed_clock(now), sleeper.sleeper()),
        sleeper,
    )
}

fn refreshed(server: &FakeServer) -> Vec<String> {
    server.refreshed.lock().expect("lock").clone()
}

#[tokio::test]
async fn a_pass_refreshes_movies_without_metatube_newest_first_and_paced() {
    let server = FakeServer {
        movies: library(),
        ..FakeServer::default()
    };
    let (backfill, sleeper) = backfill(BackfillSettings::default());
    let report = backfill.pass(&server, &CancellationToken::new()).await.expect("pass");
    assert_eq!(refreshed(&server), ["2", "4", "6", "7"]);
    assert_eq!(
        report,
        PassReport {
            movies: 7,
            missing: 5,
            too_new: 1,
            given_up: 0,
            refreshed: vec![
                "JQJ-011".to_owned(),
                "ABS-086".to_owned(),
                "FC2-1202738".to_owned(),
                "NODATE-001".to_owned()
            ],
            failed: 0,
            stopped: false,
        }
    );
    // One pause between two refreshes, none before the first.
    assert_eq!(sleeper.delays(), [15.0, 15.0, 15.0]);
    assert_eq!(
        report.summary(),
        "metadata backfill: 7 movies, 5 without MetaTube metadata (1 too new, 0 given up); \
         queued 4 refreshes, 0 failed"
    );
}

#[tokio::test]
async fn a_movie_is_given_up_after_max_attempts() {
    let server = FakeServer {
        movies: library(),
        ..FakeServer::default()
    };
    let (backfill, _) = backfill(BackfillSettings {
        max_attempts: 2,
        ..BackfillSettings::default()
    });
    let stop = CancellationToken::new();
    backfill.pass(&server, &stop).await.expect("first");
    backfill.pass(&server, &stop).await.expect("second");
    let third = backfill.pass(&server, &stop).await.expect("third");
    assert_eq!(refreshed(&server).len(), 8);
    assert!(third.refreshed.is_empty());
    assert_eq!(third.given_up, 4);
    assert_eq!(third.too_new, 1);
}

#[tokio::test]
async fn a_pass_takes_at_most_per_pass_and_the_rest_come_next_time() {
    let server = FakeServer {
        movies: library(),
        ..FakeServer::default()
    };
    let (backfill, _) = backfill(BackfillSettings {
        per_pass: 2,
        ..BackfillSettings::default()
    });
    let stop = CancellationToken::new();
    backfill.pass(&server, &stop).await.expect("first");
    assert_eq!(refreshed(&server), ["2", "4"]);
    // The ones refreshed once still come first (newest), so a second pass repeats them
    // until they reach max_attempts; per_pass bounds what one pass asks of Emby.
    backfill.pass(&server, &stop).await.expect("second");
    assert_eq!(refreshed(&server), ["2", "4", "2", "4"]);
}

#[tokio::test]
async fn a_failed_refresh_is_counted_and_the_pass_goes_on() {
    let server = FakeServer {
        movies: library(),
        failing: vec![(
            "4".to_owned(),
            EmbyError::Server("POST /Items/4/Refresh: HTTP 500".to_owned()),
        )],
        ..FakeServer::default()
    };
    let (backfill, _) = backfill(BackfillSettings::default());
    let report = backfill.pass(&server, &CancellationToken::new()).await.expect("pass");
    assert_eq!(refreshed(&server), ["2", "4", "6", "7"]);
    assert_eq!(report.failed, 1);
    assert_eq!(report.refreshed.len(), 3);
}

#[tokio::test]
async fn a_rejected_key_or_an_unreadable_list_ends_the_pass() {
    let rejected = EmbyError::Auth("POST /Items/2/Refresh: the API key was rejected".to_owned());
    let server = FakeServer {
        movies: library(),
        failing: vec![("2".to_owned(), rejected.clone())],
        ..FakeServer::default()
    };
    let (backfill, _) = backfill(BackfillSettings::default());
    let error = backfill.pass(&server, &CancellationToken::new()).await.unwrap_err();
    assert_eq!(error, rejected);
    assert_eq!(refreshed(&server), ["2"]);

    let unreadable = EmbyError::Server("GET /Items: HTTP 503".to_owned());
    let server = FakeServer {
        list_error: Some(unreadable.clone()),
        ..FakeServer::default()
    };
    let error = backfill.pass(&server, &CancellationToken::new()).await.unwrap_err();
    assert_eq!(error, unreadable);
}

#[tokio::test]
async fn stop_ends_a_pass_between_two_refreshes() {
    let stop = CancellationToken::new();
    let server = FakeServer {
        movies: library(),
        stop_after: Some((2, stop.clone())),
        ..FakeServer::default()
    };
    let (backfill, _) = backfill(BackfillSettings::default());
    let report = backfill.pass(&server, &stop).await.expect("pass");
    assert_eq!(refreshed(&server), ["2", "4"]);
    assert!(report.stopped);
    assert!(report.summary().ends_with("; stopped early"));
}

#[tokio::test]
async fn the_loop_waits_runs_skips_without_emby_and_ends_on_stop() {
    let stop = CancellationToken::new();
    let server = Arc::new(FakeServer {
        movies: library(),
        ..FakeServer::default()
    });
    let calls = Arc::new(Mutex::new(0_usize));
    let server_for: ServerFor = {
        let (server, calls, stop) = (Arc::clone(&server), Arc::clone(&calls), stop.clone());
        Arc::new(move || {
            let mut calls = calls.lock().expect("lock");
            *calls += 1;
            match *calls {
                // The first pass: Emby not configured yet.
                1 => None,
                2 => Some(Arc::clone(&server) as Arc<dyn BackfillServer>),
                _ => {
                    stop.cancel();
                    None
                }
            }
        })
    };
    let (backfill, sleeper) = backfill(BackfillSettings {
        per_pass: 1,
        ..BackfillSettings::default()
    });
    Arc::new(backfill).run(server_for, stop.clone()).await;
    assert_eq!(*calls.lock().expect("lock"), 3);
    assert_eq!(refreshed(&server), ["2"]);
    assert_eq!(sleeper.delays(), [600.0, 86_400.0, 86_400.0]);
}

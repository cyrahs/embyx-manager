//! API-coverage probes for the monitor crate (docs/rewrite/API-COVERAGE.md, "croner" and
//! "notify").
//!
//! `croner` is run against what cronsim 2.7 does in the baseline (`goldens/cron/`): which
//! expressions the settings model accepts, and the chain of next fires
//! `monitor/scheduler.py::_next_cron_fire` computes in several local zones across DST
//! changes. Every difference is listed here by name, so the probe fails when croner changes
//! and the list is what WP9 must cover itself. `notify` is checked for the events the
//! `.strm` watcher (`monitor/strm_watch.py`) needs. No application code is ported here.

use std::collections::BTreeSet;
use std::str::FromStr as _;

use chrono::{DateTime, SubsecRound as _, TimeDelta, Utc};
use chrono_tz::Tz;
use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};
use embyx_core::golden;
use embyx_core::pyjson::Value;

fn parser() -> CronParser {
    // cronsim: five fields, or six with seconds first; day-of-month and day-of-week OR-ed.
    CronParser::builder()
        .seconds(Seconds::Optional)
        .year(Year::Disallowed)
        .dom_and_dow(false)
        .build()
}

fn parse(expr: &str) -> Option<Cron> {
    parser().parse(expr).ok()
}

/// What `_next_cron_fire` does: the next occurrence strictly after `now` in local time, in
/// UTC without sub-second digits. `now.astimezone()` gives a datetime with the *fixed*
/// offset the zone has at `now`, so cronsim never sees a DST change inside one call: a fire
/// across the change keeps the old offset, and the next call (after that fire) uses the
/// new one. The port must evaluate in that fixed offset, not in the zone.
fn next_fire(cron: &Cron, zone: Tz, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let local = now.with_timezone(&zone).fixed_offset();
    cron.find_next_occurrence(&local, false)
        .ok()
        .map(|fire| fire.with_timezone(&Utc).trunc_subsecs(0))
}

fn chain(expr: &str, zone: Tz, start: DateTime<Utc>, length: usize) -> Vec<Option<String>> {
    let Some(cron) = parse(expr) else {
        return vec![None];
    };
    let mut fires = Vec::new();
    let mut now = start;
    for _ in 0..length {
        let fire = next_fire(&cron, zone, now);
        fires.push(fire.map(|fire| fire.to_rfc3339()));
        match fire {
            Some(fire) => now = fire + TimeDelta::seconds(1),
            None => break,
        }
    }
    fires
}

/// Expressions cronsim and croner disagree on accepting (cronsim's verdict second): croner
/// takes the `W` and `?` forms and the `@` nicknames that cronsim 2.7 refuses, and a date
/// that never exists (30 February), which cronsim refuses as "Bad day-of-month". The port
/// refuses these before croner sees them, with cronsim's message (WP1 validation, WP9).
const VALIDATION_DIFFERENCES: &[(&str, bool)] = &[
    ("0 0 15W * *", false),
    ("0 4 * * ?", false),
    ("0 4 30 2 *", false),
    ("0 4 ? * 1", false),
    ("@daily", false),
    ("@hourly", false),
];

#[test]
fn croner_accepts_what_cronsim_accepts() {
    let document = golden::load("cron/validation.json");
    let mut differences = BTreeSet::new();
    for case in golden::cases(&document) {
        let normalized = golden::string(golden::get(case, "normalized"));
        let cronsim_accepts = matches!(golden::get(case, "error"), Value::Null);
        let croner_accepts = parse(&normalized).is_some();
        if cronsim_accepts != croner_accepts {
            differences.insert((golden::string(golden::get(case, "expr")), cronsim_accepts));
        }
    }
    let expected: BTreeSet<(String, bool)> = VALIDATION_DIFFERENCES
        .iter()
        .map(|(expr, accepts)| ((*expr).to_owned(), *accepts))
        .collect();
    assert_eq!(
        differences, expected,
        "(expression, cronsim accepts) where croner disagrees"
    );
}

/// Expressions whose next-fire chain differs in at least one zone and start: exactly the
/// ones cronsim refuses and croner accepts (above; for 30 February croner finds no fire,
/// as `_next_cron_fire` returns None). Every expression both accept fires at the same
/// instants in all zones and across the DST changes, once evaluated in the fixed offset.
const NEXT_FIRE_DIFFERENCES: &[&str] = &["0 0 15W * *", "0 4 * * ?", "0 4 ? * 1", "@daily", "@hourly"];

#[test]
fn croner_fires_when_cronsim_fires() {
    let document = golden::load("cron/next_fires.json");
    let mut differences = BTreeSet::new();
    let mut details = Vec::new();
    let mut compared = 0;
    for case in golden::cases(&document) {
        let expr = golden::string(golden::get(case, "expr"));
        let zone = Tz::from_str(&golden::string(golden::get(case, "zone"))).expect("zone");
        let start: DateTime<Utc> = DateTime::parse_from_rfc3339(&golden::string(golden::get(case, "start")))
            .expect("start")
            .to_utc();
        let want: Vec<Option<String>> = golden::list(golden::get(case, "fires"))
            .iter()
            .map(|fire| {
                fire.as_str()
                    .map(|text| DateTime::parse_from_rfc3339(text).expect("fire").to_utc().to_rfc3339())
            })
            .collect();
        let have = chain(&expr, zone, start, want.len().max(1));
        let have = if want == [None] {
            have.into_iter().take(1).collect()
        } else {
            have
        };
        compared += 1;
        if have != want {
            if !NEXT_FIRE_DIFFERENCES.contains(&expr.as_str()) {
                details.push(format!(
                    "{zone} {expr:?} from {start}:\n  cronsim {want:?}\n  croner  {have:?}"
                ));
            }
            differences.insert(expr);
        }
    }
    assert_eq!(compared, 1148);
    let expected: BTreeSet<String> = NEXT_FIRE_DIFFERENCES.iter().map(|expr| (*expr).to_owned()).collect();
    assert_eq!(
        differences,
        expected,
        "expressions whose fires differ from cronsim's\n{}",
        details.join("\n")
    );
}

mod watcher {
    //! The `.strm` watcher (`monitor/scheduler.py::_StrmEventHandler`, watchdog 6.0 on
    //! inotify) forwards created, modified, deleted and moved `.strm` files, recursively.
    //! watchdog synthesizes events for the files inside a directory that is moved into or
    //! within the tree (`generate_sub_created_events`, `generate_sub_moved_events`); `notify`
    //! does not, so the port has to walk such a directory itself. What `notify` reports, on
    //! Linux.

    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::time::Duration;

    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    use notify::{Event, EventKind, RecursiveMode, Watcher as _};

    fn collect(receiver: &mpsc::Receiver<notify::Result<Event>>) -> Vec<(EventKind, Vec<PathBuf>)> {
        let mut events = Vec::new();
        while let Ok(event) = receiver.recv_timeout(Duration::from_millis(300)) {
            let event = event.expect("watch event");
            events.push((event.kind, event.paths));
        }
        events
    }

    fn has(events: &[(EventKind, Vec<PathBuf>)], kind: EventKind, paths: &[&Path]) -> bool {
        events.iter().any(|(have, have_paths)| {
            *have == kind && have_paths.iter().map(PathBuf::as_path).eq(paths.iter().copied())
        })
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn notify_reports_what_the_strm_watcher_needs() {
        let root = tempfile::tempdir().expect("tempdir");
        let base = root.path().canonicalize().expect("canonical");
        let outside = tempfile::tempdir().expect("outside");
        let (sender, receiver) = mpsc::channel();
        let mut watcher = notify::recommended_watcher(sender).expect("watcher");
        watcher.watch(&base, RecursiveMode::Recursive).expect("watch");

        // A file written in place: a create, then writes and a close.
        let created = base.join("ABC-123.strm");
        std::fs::write(&created, "http://x/1").expect("write");
        let events = collect(&receiver);
        assert!(
            has(&events, EventKind::Create(CreateKind::File), &[&created]),
            "{events:?}"
        );

        // A rename inside the tree names both paths in one event.
        let renamed = base.join("ABC-124.strm");
        std::fs::rename(&created, &renamed).expect("rename");
        let events = collect(&receiver);
        assert!(
            has(
                &events,
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                &[&created, &renamed]
            ),
            "{events:?}"
        );

        // A file moved in from outside the watched tree arrives as a rename "to" only.
        let staged = outside.path().join("ABC-125.strm");
        std::fs::write(&staged, "http://x/2").expect("stage");
        let moved_in = base.join("ABC-125.strm");
        std::fs::rename(&staged, &moved_in).expect("move in");
        let events = collect(&receiver);
        assert!(
            has(
                &events,
                EventKind::Modify(ModifyKind::Name(RenameMode::To)),
                &[&moved_in]
            ),
            "{events:?}"
        );

        // A directory created then filled: the new directory is watched too (recursive mode
        // adds it), so the file inside is reported.
        let folder = base.join("ABC-126");
        std::fs::create_dir(&folder).expect("mkdir");
        std::thread::sleep(Duration::from_millis(100));
        let inner = folder.join("ABC-126.strm");
        std::fs::write(&inner, "http://x/3").expect("write inner");
        let events = collect(&receiver);
        assert!(
            has(&events, EventKind::Create(CreateKind::Folder), &[&folder]),
            "{events:?}"
        );
        assert!(
            has(&events, EventKind::Create(CreateKind::File), &[&inner]),
            "{events:?}"
        );

        // A directory renamed in from outside names only the directory, not the files in it:
        // the watcher must scan a directory that arrives.
        let staged_dir = outside.path().join("ABC-127");
        std::fs::create_dir(&staged_dir).expect("stage dir");
        std::fs::write(staged_dir.join("ABC-127.strm"), "http://x/4").expect("stage file");
        let arrived = base.join("ABC-127");
        std::fs::rename(&staged_dir, &arrived).expect("move dir in");
        let events = collect(&receiver);
        assert!(
            has(
                &events,
                EventKind::Modify(ModifyKind::Name(RenameMode::To)),
                &[&arrived]
            ),
            "{events:?}"
        );
        let inner_arrived = arrived.join("ABC-127.strm");
        assert!(
            !events.iter().any(|(_, paths)| paths.contains(&inner_arrived)),
            "{events:?}"
        );

        // A directory renamed inside the tree: one event naming both directories, nothing
        // for the files in it (watchdog adds a moved event per file).
        let renamed_dir = base.join("ABC-128");
        std::fs::rename(&arrived, &renamed_dir).expect("rename dir");
        let events = collect(&receiver);
        assert!(
            has(
                &events,
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                &[&arrived, &renamed_dir]
            ),
            "{events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|(_, paths)| paths.iter().any(|path| path.ends_with("ABC-127.strm"))),
            "{events:?}"
        );

        // A removal.
        std::fs::remove_file(&renamed).expect("remove");
        let events = collect(&receiver);
        assert!(
            has(&events, EventKind::Remove(RemoveKind::File), &[&renamed]),
            "{events:?}"
        );
    }
}

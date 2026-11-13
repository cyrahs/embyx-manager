//! Ported from `tests/test_monitor_reports.py`; the bounded tail and error list are pinned by
//! `goldens/monitor/run_context.json`.

use embyx_core::golden;

use super::*;

fn make_ctx() -> RunContext {
    RunContext::new("test-reports")
}

/// `OSError('disk detached')`.
fn boom() -> Raised {
    Raised::new("OSError", "disk detached")
}

// py: tests/test_monitor_reports.py::test_exception_records_the_cause_in_the_tail
#[test]
fn exception_records_the_cause_in_the_tail() {
    // The dashboard only ever sees the recorded tail, so the line must carry the exception
    // itself, not just the caller's message.
    let ctx = make_ctx();
    ctx.exception(&format!("{} run failed", "mapping"), Some(&boom()));

    assert_eq!(ctx.log_tail(), ["ERROR: mapping run failed: OSError('disk detached')"]);
    assert_eq!(ctx.errors(), ["ERROR: mapping run failed: OSError('disk detached')"]);
}

// py: tests/test_monitor_reports.py::test_exception_outside_handler_records_plain_message
#[test]
fn exception_outside_handler_records_plain_message() {
    let ctx = make_ctx();
    ctx.exception("mapping run failed", None);

    assert_eq!(ctx.log_tail(), ["ERROR: mapping run failed"]);
}

/// The golden's call sequences, replayed: each case lists `[method, message]` calls (with
/// `exception` carrying `[class, message]` of the exception or null) and Python's tail,
/// errors and counters after them.
#[test]
fn run_context_sequences_match_python() {
    let document = golden::load("monitor/run_context.json");
    for case in golden::cases(&document) {
        let ctx = make_ctx();
        for call in case.get("calls").and_then(Value::as_list).expect("calls") {
            let call = call.as_list().expect("a call");
            let method = call[0].as_str().expect("method");
            match method {
                "info" => ctx.info(call[1].as_str().expect("message")),
                "warning" => ctx.warning(call[1].as_str().expect("message")),
                "error" => ctx.error(call[1].as_str().expect("message")),
                "exception" => {
                    let raised = call[2].as_list().map(|raised| {
                        Raised::new(raised[0].as_str().expect("class"), raised[1].as_str().expect("message"))
                    });
                    ctx.exception(
                        call[1].as_str().expect("message"),
                        raised.as_ref().map(|raised| raised as &dyn PyException),
                    );
                }
                "add" => ctx.add(call[1].as_str().expect("counter"), int(&call[2])),
                "set" => ctx.set(call[1].as_str().expect("counter"), int(&call[2])),
                other => panic!("unknown call {other}"),
            }
        }
        let name = case.get("name").and_then(Value::as_str).expect("name");
        assert_eq!(ctx.log_tail(), strings(case.get("log_tail")), "{name}: log tail");
        assert_eq!(ctx.errors(), strings(case.get("errors")), "{name}: errors");
        let dumped = embyx_core::pyjson::dumps(
            &ctx.stats().to_value(),
            &embyx_core::pyjson::DumpOptions::python_default(),
        )
        .expect("dump");
        assert_eq!(
            Some(dumped.as_str()),
            case.get("stats_json").and_then(Value::as_str),
            "{name}: stats"
        );
    }
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int(number) => i64::try_from(*number).expect("small"),
        other => panic!("not an int: {other:?}"),
    }
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_list)
        .expect("a list")
        .iter()
        .map(|item| item.as_str().expect("a string").to_owned())
        .collect()
}

#[test]
fn stop_requests_cancel_the_run() {
    let ctx = make_ctx();
    assert!(!ctx.stop_requested());
    assert_eq!(ctx.check_cancelled(), Ok(()));
    ctx.request_stop();
    assert!(ctx.stop_requested());
    assert_eq!(ctx.check_cancelled(), Err(RunCancelledError));
    assert_eq!(RunCancelledError.py_repr(), "RunCancelledError()");
}

#[test]
fn enums_round_trip_their_values() {
    for name in PipelineName::ALL {
        assert_eq!(PipelineName::parse(name.as_str()), Some(*name));
    }
    assert_eq!(
        PipelineName::ALL.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["rss", "archive", "mapping", "playlists"]
    );
    assert_eq!(
        RunTrigger::ALL.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["scheduled", "manual", "watchdog", "startup"]
    );
    assert_eq!(
        RunState::ALL.iter().map(|name| name.as_str()).collect::<Vec<_>>(),
        ["running", "completed", "failed", "cancelled"]
    );
    assert_eq!(RunState::parse("RUNNING"), None);
}

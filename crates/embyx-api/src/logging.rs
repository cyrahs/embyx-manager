//! The server's log lines, as uvicorn's default logging configuration writes them.
//!
//! Python origin: `uvicorn.config.LOGGING_CONFIG` (uvicorn 0.52.1) with the root logger
//! left alone. `uvicorn.error` (the server's own lines) goes to stderr and `uvicorn.access`
//! (one line per request) to stdout, both as `LEVEL:` padded to nine characters, a space and
//! the message. Every other logger has no handler and the root's level, WARNING: its INFO
//! and DEBUG lines are dropped, and the rest reach Python's last-resort handler, which
//! writes the bare message to stderr. Fields an event carries besides its message follow it
//! as `key=value` (D52).

use std::fmt::Write as _;
use std::io::Write as _;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// The server's own logger (`uvicorn.error`).
pub const SERVER_TARGET: &str = "uvicorn.error";
/// The access log (`uvicorn.access`).
pub const ACCESS_TARGET: &str = "uvicorn.access";

/// `levelname` as Python's `logging` names it.
fn level_name(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARNING",
        Level::INFO => "INFO",
        Level::DEBUG | Level::TRACE => "DEBUG",
    }
}

/// uvicorn's `%(levelprefix)s`: `INFO:` and padding to nine characters.
pub fn level_prefix(level: Level) -> String {
    format!("{:<9}", format!("{}:", level_name(level)))
}

/// Collects an event's message and its other fields.
#[derive(Default)]
struct Message {
    text: String,
    fields: String,
}

impl Visit for Message {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.text.push_str(value);
        } else {
            let _ = write!(self.fields, " {}={value}", field.name());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.text, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

/// Where a line goes, and how it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Stdout,
    Stderr,
}

/// The line an event becomes, if any.
pub fn render(target: &str, level: Level, message: &str) -> Option<(Destination, String)> {
    if target == ACCESS_TARGET {
        // `uvicorn.access` logs at INFO and above.
        return (level <= Level::INFO).then(|| (Destination::Stdout, format!("{} {message}", level_prefix(level))));
    }
    if target == SERVER_TARGET || target == "uvicorn" {
        return (level <= Level::INFO).then(|| (Destination::Stderr, format!("{} {message}", level_prefix(level))));
    }
    // The application's loggers: WARNING and above, the bare message.
    (level <= Level::WARN).then(|| (Destination::Stderr, message.to_owned()))
}

/// The layer that writes the lines.
#[derive(Debug, Default)]
pub struct UvicornFormat;

impl<S: Subscriber> Layer<S> for UvicornFormat {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        let mut message = Message::default();
        event.record(&mut message);
        let text = format!("{}{}", message.text, message.fields);
        let Some((destination, line)) = render(metadata.target(), *metadata.level(), &text) else {
            return;
        };
        // A closed stream drops the line, as Python's handler would report and go on.
        let _ = match destination {
            Destination::Stdout => writeln!(std::io::stdout().lock(), "{line}"),
            Destination::Stderr => writeln!(std::io::stderr().lock(), "{line}"),
        };
    }
}

/// Install the format for the process.
pub fn init() {
    let _ = tracing_subscriber::registry().with(UvicornFormat).try_init();
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    use super::{Destination, level_prefix, render};

    #[test]
    fn lines_look_like_uvicorn_s() {
        assert_eq!(level_prefix(Level::INFO), "INFO:    ");
        assert_eq!(level_prefix(Level::WARN), "WARNING: ");
        assert_eq!(level_prefix(Level::ERROR), "ERROR:   ");
        assert_eq!(
            render("uvicorn.error", Level::INFO, "Application startup complete."),
            Some((
                Destination::Stderr,
                "INFO:     Application startup complete.".to_owned()
            ))
        );
        assert_eq!(
            render("uvicorn.access", Level::INFO, "127.0.0.1:5000 - \"GET / HTTP/1.1\" 200"),
            Some((
                Destination::Stdout,
                "INFO:     127.0.0.1:5000 - \"GET / HTTP/1.1\" 200".to_owned()
            ))
        );
        assert_eq!(render("uvicorn.error", Level::DEBUG, "x"), None);
        // The application's own loggers: WARNING and above, the bare message.
        assert_eq!(render("embyx_manager.config.api", Level::INFO, "rejected"), None);
        assert_eq!(
            render("embyx_db::config_store", Level::ERROR, "stored config is invalid"),
            Some((Destination::Stderr, "stored config is invalid".to_owned()))
        );
        assert_eq!(
            render("embyx-manager.sukebei", Level::WARN, "Skipping"),
            Some((Destination::Stderr, "Skipping".to_owned()))
        );
    }
}

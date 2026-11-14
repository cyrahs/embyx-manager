//! `embyx-manager`: one binary for the server, the merge Job's worker and the home transcode
//! worker.
//!
//! Python origin: `src/embyx_manager/__main__.py`. The three commands and their flags are
//! declared here as Python declares them; their bodies arrive with their work packages
//! (`docs/rewrite/PLAN.md`): `serve` in WP4 (`embyx_api::serve::run`, Unix only, D58),
//! `merge-worker` in WP13 (`embyx_merge::worker::run`), `transcode-worker` in WP15
//! (`embyx_transcode_worker::main`). `import-config` is not ported (PLAN.md, decision 1).
//!
//! The Windows build carries `merge-worker` and `transcode-worker` only: the server's crates
//! are Unix-only dependencies, and `embyx-merge` comes without its `db` feature (D144), so
//! `embyx-manager.exe` needs no database, gRPC or TLS stack beyond the worker's HTTP client.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "embyx-manager")]
struct Cli {
    /// What to run; `serve` when left out.
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// run the web application (default)
    Serve,
    /// merge one title's parts into a single file (run by the merge tab's Kubernetes Job)
    MergeWorker(MergeWorkerArgs),
    /// re-encode merges too big for 115 on this machine's NVIDIA GPU (the home transcode worker)
    TranscodeWorker(TranscodeWorkerArgs),
}

/// `merge-worker`'s arguments (`__main__.py`, `worker_parser`). As with argparse, a unique
/// prefix of a long option is taken (`--out`), a repeated option keeps its last value, and a
/// negative number is a value (`--reserve-gib -1`).
#[derive(Debug, Args)]
#[command(infer_long_args = true, args_override_self = true)]
struct MergeWorkerArgs {
    /// the merged file to write
    #[arg(long)]
    output: String,
    /// the JSON status file the app polls
    #[arg(long)]
    status: String,
    /// space to leave free on the volume
    #[arg(long = "reserve-gib", default_value_t = 20.0, value_parser = parse_float, allow_negative_numbers = true)]
    reserve_gib: f64,
    /// probe the parts and copy their audio here for the home transcode worker
    #[arg(long = "home-prepare")]
    home_prepare: Option<String>,
    /// merge the home transcode worker's chunks and the audio kept here
    #[arg(long = "home-assemble")]
    home_assemble: Option<String>,
    /// the parts, in order
    parts: Vec<String>,
}

/// `transcode-worker`'s arguments (`merge/home_worker.py`, `add_arguments`), with argparse's
/// habits as `merge-worker` has them (D145): a unique prefix of a long option, the last of a
/// repeated option, a negative number as a value. `Debug` is written by hand so the bearer
/// token never reaches a log.
#[derive(Args)]
#[command(infer_long_args = true, args_override_self = true)]
struct TranscodeWorkerArgs {
    /// TOML settings file
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, help = "the transcode endpoint, e.g. http://10.117.200.1:8080")]
    server: Option<String>,
    /// bearer token, when the cluster asks for one
    #[arg(long)]
    token: Option<String>,
    /// where chunks wait for their upload
    #[arg(long = "cache-dir")]
    cache_dir: Option<PathBuf>,
    /// upload speed cap in Mbit/s (default 30)
    #[arg(long = "upload-mbps", value_parser = parse_float, allow_negative_numbers = true)]
    upload_mbps: Option<f64>,
    /// parts encoded at once (default 3)
    #[arg(long, value_parser = parse_int, allow_negative_numbers = true)]
    jobs: Option<i128>,
    /// chunks kept waiting before encoding pauses
    #[arg(long = "max-queue-gb", value_parser = parse_float, allow_negative_numbers = true)]
    max_queue_gb: Option<f64>,
    /// ffmpeg executable
    #[arg(long)]
    ffmpeg: Option<String>,
    /// ffprobe executable
    #[arg(long)]
    ffprobe: Option<String>,
}

impl std::fmt::Debug for TranscodeWorkerArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TranscodeWorkerArgs")
            .field("config", &self.config)
            .field("server", &self.server)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("cache_dir", &self.cache_dir)
            .field("upload_mbps", &self.upload_mbps)
            .field("jobs", &self.jobs)
            .field("max_queue_gb", &self.max_queue_gb)
            .field("ffmpeg", &self.ffmpeg)
            .field("ffprobe", &self.ffprobe)
            .finish()
    }
}

/// `argparse`'s `type=float`: Python's `float()` of the text.
fn parse_float(text: &str) -> Result<f64, String> {
    embyx_merge::parse_float(text).ok_or_else(|| format!("invalid float value: {text:?}"))
}

/// `argparse`'s `type=int`: Python's `int()` of the text.
fn parse_int(text: &str) -> Result<i128, String> {
    embyx_core::pytext::py_int(text).map_err(|_| format!("invalid int value: {text:?}"))
}

impl TranscodeWorkerArgs {
    fn into_worker_args(self) -> embyx_transcode_worker::Args {
        embyx_transcode_worker::Args {
            config: self.config,
            server: self.server,
            token: self.token,
            cache_dir: self.cache_dir,
            upload_mbps: self.upload_mbps,
            jobs: self.jobs,
            max_queue_gb: self.max_queue_gb,
            ffmpeg: self.ffmpeg,
            ffprobe: self.ffprobe,
        }
    }
}

/// `transcode-worker`: `raise SystemExit(home_worker.main(args))`.
fn transcode_worker(args: TranscodeWorkerArgs) -> ExitCode {
    let code = embyx_transcode_worker::main(&args.into_worker_args());
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

/// The command a parsed command line runs, and the work package that ported it.
#[cfg(test)]
fn planned(command: Option<&Command>) -> (&'static str, &'static str) {
    match command {
        None | Some(Command::Serve) => ("serve", "WP4"),
        Some(Command::MergeWorker(_)) => ("merge-worker", "WP13"),
        Some(Command::TranscodeWorker(_)) => ("transcode-worker", "WP15"),
    }
}

/// `merge-worker`: `worker.run(...)` in `__main__.py`.
fn merge_worker(args: MergeWorkerArgs) -> ExitCode {
    use embyx_merge::worker::{self, Environment, StatusFile, Worker};
    let command = worker::Command {
        parts: args.parts,
        output: args.output,
        status: args.status,
        reserve_gib: args.reserve_gib,
        home_prepare: args.home_prepare,
        home_assemble: args.home_assemble,
    };
    let mut status = StatusFile::new(worker::py_path(&command.status));
    let code = worker::run(
        &Worker::default(),
        &command,
        &mut status,
        &Environment::from_process(),
        &mut std::io::stderr(),
    );
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

/// `serve`: `_serve()` in `__main__.py`.
#[cfg(unix)]
fn serve() -> ExitCode {
    ExitCode::from(embyx_api::serve::run())
}

/// The server is not built for Windows, where only the home transcode worker runs (D58).
#[cfg(not(unix))]
fn serve() -> ExitCode {
    #[allow(
        clippy::print_stderr,
        reason = "the only output of a command this build does not carry"
    )]
    {
        eprintln!("embyx-manager serve: the server runs on Linux only");
    }
    ExitCode::FAILURE
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        None | Some(Command::Serve) => serve(),
        Some(Command::MergeWorker(args)) => merge_worker(args),
        Some(Command::TranscodeWorker(args)) => transcode_worker(args),
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory as _, Parser as _};

    use super::{Cli, Command, planned};

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    /// argparse's habits, which the Python app never relies on but an operator might.
    #[test]
    fn merge_worker_options_parse_as_argparse_parses_them() {
        let parse = |args: &[&str]| {
            let cli = Cli::try_parse_from(args).expect("parses");
            let Some(Command::MergeWorker(args)) = cli.command else {
                panic!("merge-worker");
            };
            args
        };
        // A unique prefix, a repeated option (the last wins), a negative number.
        let args = parse(&[
            "embyx-manager",
            "merge-worker",
            "--out",
            "x.mp4",
            "--output",
            "A.mp4",
            "--stat",
            "s.json",
            "--reserve-gib",
            "-1.5",
            "--",
            "a-1.mp4",
            "a-2.mp4",
        ]);
        assert_eq!(args.output, "A.mp4");
        assert_eq!(args.status, "s.json");
        assert!((args.reserve_gib + 1.5).abs() < f64::EPSILON);
        assert_eq!(args.parts, ["a-1.mp4", "a-2.mp4"]);
        // An ambiguous prefix is refused, as argparse refuses it.
        assert!(
            Cli::try_parse_from([
                "embyx-manager",
                "merge-worker",
                "--output",
                "A.mp4",
                "--status",
                "s.json",
                "--home",
                "x",
            ])
            .is_err()
        );
    }

    #[test]
    fn serve_is_the_default_and_every_command_names_its_work_package() {
        let parse = |args: &[&str]| Cli::try_parse_from(args).expect("parses");
        assert_eq!(planned(parse(&["embyx-manager"]).command.as_ref()), ("serve", "WP4"));
        assert_eq!(
            planned(parse(&["embyx-manager", "serve"]).command.as_ref()),
            ("serve", "WP4")
        );
        let merge = parse(&[
            "embyx-manager",
            "merge-worker",
            "--output",
            "/upload/merge/A.mp4",
            "--status",
            "/upload/merge/.merge/1.json",
            "/mnt/cd2/a-1.mp4",
            "/mnt/cd2/a-2.mp4",
        ]);
        let Some(Command::MergeWorker(args)) = &merge.command else {
            panic!("merge-worker parses into its own arguments");
        };
        assert_eq!(args.parts, ["/mnt/cd2/a-1.mp4", "/mnt/cd2/a-2.mp4"]);
        assert!((args.reserve_gib - 20.0).abs() < f64::EPSILON);
        assert_eq!(planned(merge.command.as_ref()), ("merge-worker", "WP13"));
        let worker = parse(&[
            "embyx-manager",
            "transcode-worker",
            "--server",
            "http://10.117.200.1:8080",
            "--jobs",
            "2",
        ]);
        assert_eq!(planned(worker.command.as_ref()), ("transcode-worker", "WP15"));
    }

    #[test]
    fn the_worker_token_never_shows_in_debug() {
        let cli =
            Cli::try_parse_from(["embyx-manager", "transcode-worker", "--token", "s3cret-token"]).expect("parses");
        let shown = format!("{:?}", cli.command);
        assert!(!shown.contains("s3cret-token"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    /// `home_worker_settings.json`'s `argv` cases: argparse's reading of the worker's options.
    #[test]
    fn transcode_worker_options_parse_as_argparse_parses_them() {
        use embyx_core::golden;
        use embyx_core::pyjson::{Dict, Value};

        let document = golden::load("merge/home_worker_settings.json");
        let mut checked = 0;
        for case in golden::cases(&document) {
            if golden::string(golden::get(case, "kind")) != "argv" {
                continue;
            }
            let argv: Vec<String> = golden::list(golden::get(case, "argv"))
                .iter()
                .map(golden::string)
                .collect();
            let parsed = Cli::try_parse_from(
                ["embyx-manager", "transcode-worker"]
                    .into_iter()
                    .map(str::to_owned)
                    .chain(argv.iter().cloned()),
            );
            if case.get("refused").is_some() {
                assert!(parsed.is_err(), "{argv:?} is refused");
                checked += 1;
                continue;
            }
            let Some(Command::TranscodeWorker(options)) = parsed.expect("parses").command else {
                panic!("transcode-worker");
            };
            let text = |value: Option<String>| value.map_or(Value::Null, Value::Str);
            let path = |value: Option<std::path::PathBuf>| {
                value.map_or(Value::Null, |path| Value::Str(path.to_string_lossy().into_owned()))
            };
            let mut got = Dict::new();
            got.insert("config", path(options.config));
            got.insert("server", text(options.server));
            got.insert("token", text(options.token));
            got.insert("cache_dir", path(options.cache_dir));
            got.insert("upload_mbps", options.upload_mbps.map_or(Value::Null, Value::Float));
            got.insert("jobs", options.jobs.map_or(Value::Null, Value::Int));
            got.insert("max_queue_gb", options.max_queue_gb.map_or(Value::Null, Value::Float));
            got.insert("ffmpeg", text(options.ffmpeg));
            got.insert("ffprobe", text(options.ffprobe));
            assert_eq!(&Value::Dict(got), golden::get(case, "value"), "{argv:?}");
            checked += 1;
        }
        assert_eq!(checked, 12);
    }

    #[test]
    fn merge_worker_requires_its_output_and_status() {
        assert!(Cli::try_parse_from(["embyx-manager", "merge-worker", "a.mp4"]).is_err());
    }
}

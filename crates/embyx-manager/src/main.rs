//! `embyx-manager`: one binary for the server, the merge Job's worker and the home transcode
//! worker.
//!
//! Python origin: `src/embyx_manager/__main__.py`. The three commands and their flags are
//! declared here as Python declares them; their bodies arrive with their work packages
//! (`docs/rewrite/PLAN.md`): `serve` in WP4, `merge-worker` in WP13 (done:
//! `embyx_merge::worker::run`), `transcode-worker` in WP15. Until then `serve` and
//! `transcode-worker` stop with "not implemented yet", the one placeholder WP0 is allowed.
//! `import-config` is not ported (PLAN.md, decision 1).

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

/// `transcode-worker`'s arguments (`merge/home_worker.py`, `add_arguments`). `Debug` is
/// written by hand so the bearer token never reaches a log.
#[derive(Args)]
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
    #[arg(long = "upload-mbps")]
    upload_mbps: Option<f64>,
    /// parts encoded at once (default 3)
    #[arg(long)]
    jobs: Option<i64>,
    /// chunks kept waiting before encoding pauses
    #[arg(long = "max-queue-gb")]
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

/// The command a parsed command line runs, and the work package that implements it.
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Some(Command::MergeWorker(args)) = cli.command {
        return merge_worker(args);
    }
    let (name, package) = planned(cli.command.as_ref());
    // The binary has no logging yet (WP4 adds it); this one line is its whole output.
    #[allow(
        clippy::print_stderr,
        reason = "the placeholder's only output, until WP4 and WP15 replace it"
    )]
    {
        eprintln!("embyx-manager {name}: not implemented yet ({package}, docs/rewrite/PLAN.md)");
    }
    ExitCode::FAILURE
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

    #[test]
    fn merge_worker_requires_its_output_and_status() {
        assert!(Cli::try_parse_from(["embyx-manager", "merge-worker", "a.mp4"]).is_err());
    }
}

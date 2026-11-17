//! `home_worker_settings.json`: the default paths and what `load_config` makes of a TOML file
//! and the command line (the configuration of contract 5). The `argv` cases are the binary's
//! option parsing and are checked in `embyx-manager`.

use std::fs;
use std::path::{Path, PathBuf};

use embyx_core::golden;
use embyx_core::pyjson::Value;

use super::{cases, expected, object, raised, value};
use crate::config::{self, Args, ConfigError, Environment, WorkerConfig};

const HOST: &str = "<host>";

fn rooted(text: &str, root: &Path) -> String {
    super::slashes(&text.replace(&*root.to_string_lossy(), "<root>"))
}

/// The options the golden's command lines use, as argparse fills them.
fn args(argv: &[Value], folder: &Path) -> Args {
    let mut parsed = Args::default();
    let mut items = argv.iter().map(golden::string);
    while let Some(option) = items.next() {
        let text = items.next().expect("an option's value");
        match option.as_str() {
            "--config" => parsed.config = Some(folder.join(text)),
            "--server" => parsed.server = Some(text),
            "--token" => parsed.token = Some(text),
            "--cache-dir" => parsed.cache_dir = Some(PathBuf::from(text)),
            "--upload-mbps" => parsed.upload_mbps = Some(text.parse().expect("float")),
            "--jobs" => parsed.jobs = Some(text.parse().expect("int")),
            "--max-queue-gb" => parsed.max_queue_gb = Some(text.parse().expect("float")),
            "--ffmpeg" => parsed.ffmpeg = Some(text),
            "--ffprobe" => parsed.ffprobe = Some(text),
            other => panic!("unknown option {other}"),
        }
    }
    parsed
}

fn view(config: &WorkerConfig, root: &Path) -> Value {
    object([
        ("server", Value::Str(config.server.clone())),
        ("token", Value::Str(config.token.clone())),
        (
            "cache_dir",
            Value::Str(rooted(&config.cache_dir.to_string_lossy(), root)),
        ),
        ("upload_mbps", Value::Float(config.upload_mbps)),
        ("jobs", Value::Int(config.jobs)),
        ("max_queue_gb", Value::Float(config.max_queue_gb)),
        ("ffmpeg", Value::Str(config.ffmpeg.clone())),
        ("ffprobe", Value::Str(config.ffprobe.clone())),
        ("name", Value::Str(config.name.clone())),
        ("encoder", Value::Dict(config.encoder.clone())),
    ])
}

#[test]
fn settings_match_python() {
    let root = tempfile::tempdir().expect("temporary directory");
    let root = root.path();
    let home = root.join("home");
    let (mut defaults, mut loads) = (0, 0);
    for case in cases("settings") {
        match golden::string(golden::get(&case, "kind")).as_str() {
            "defaults" => {
                let given = golden::get(&case, "environment");
                let pick = |key: &str| {
                    given.get(key).map(|value| {
                        golden::string(value)
                            .replace("<appdata>", &root.join("appdata").to_string_lossy())
                            .replace("<localappdata>", &root.join("local").to_string_lossy())
                    })
                };
                let environment = Environment {
                    appdata: pick("APPDATA"),
                    local_appdata: pick("LOCALAPPDATA"),
                    home: home.clone(),
                    hostname: HOST.to_owned(),
                };
                let config_path = config::default_config_path(&environment);
                let cache_dir = config::default_cache_dir(&environment);
                assert_eq!(
                    Value::Str(rooted(&config_path.to_string_lossy(), root)),
                    *golden::get(&case, "config_path"),
                    "case {case:?}"
                );
                assert_eq!(
                    Value::Str(rooted(&cache_dir.to_string_lossy(), root)),
                    *golden::get(&case, "cache_dir"),
                    "case {case:?}"
                );
                defaults += 1;
            }
            "load" => {
                let folder = root.join(format!("config{loads}"));
                let appdata = folder.join("appdata");
                if let Some(text) = golden::get(&case, "toml").as_str() {
                    let path = appdata.join("embyx-transcode").join("config.toml");
                    fs::create_dir_all(path.parent().expect("parent")).expect("folders");
                    fs::write(&path, text).expect("config");
                } else {
                    fs::create_dir_all(&folder).expect("folder");
                }
                let environment = Environment {
                    appdata: Some(appdata.to_string_lossy().into_owned()),
                    local_appdata: Some(folder.join("local").to_string_lossy().into_owned()),
                    home: home.clone(),
                    hostname: HOST.to_owned(),
                };
                let args = args(golden::list(golden::get(&case, "argv")), &folder);
                let got = match config::load_config_with(&args, &environment) {
                    Ok(config) => value(view(&config, root)),
                    Err(ConfigError::Exit(message)) => object([("exit", Value::Str(rooted(&message, root)))]),
                    Err(ConfigError::Raised(error)) => raised(super::py_error(&error)),
                };
                assert_eq!(got, expected(&case), "case {case:?}");
                loads += 1;
            }
            "argv" => {}
            other => panic!("unknown kind {other}"),
        }
    }
    assert_eq!((defaults, loads), (3, 18));
}

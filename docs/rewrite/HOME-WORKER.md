# The home transcode worker, Rust build

The Rust build's home worker is the same program as the Python one (`docs/home-transcode.md`):
the same command, the same `config.toml`, the same flags, the same protocol with the cluster. What
changes is how it gets onto the Windows machine: one `embyx-manager.exe`, no Python, no uv.

This page is for the day the Rust build is deployed. Until then the worker stays the Python one,
installed with `uv tool install` as `docs/home-transcode.md` says.

## Where the exe comes from

A GitHub Release of this repository, created only at deployment (the release workflow is in
`docs/rewrite/DEPLOY.md`; it is not added before). Each release carries `embyx-manager.exe` (Windows
x86-64) and `embyx-manager.exe.sha256`. Until then `rust.yml`'s Windows job keeps the exe of every
CI run as a workflow artifact (`embyx-manager-windows`, 14 days), which is for a look, not for use.

## Install

1. The tools, as before, minus uv:

   ```powershell
   winget install Gyan.FFmpeg      # the full build has av1_nvenc
   winget install WireGuard.WireGuard
   ```

2. Download `embyx-manager.exe` and its `.sha256` from the release into a folder of your choice,
   for example `%LOCALAPPDATA%\Programs\embyx-manager`, and check it:

   ```powershell
   $dir = "$env:LOCALAPPDATA\Programs\embyx-manager"
   (Get-FileHash "$dir\embyx-manager.exe" -Algorithm SHA256).Hash.ToLower()
   Get-Content "$dir\embyx-manager.exe.sha256"     # the same 64 characters
   ```

   Add that folder to your user PATH (Settings, System, About, Advanced system settings,
   Environment Variables) or run the exe by its full path.

3. If the Python worker is installed, remove it so that `embyx-manager` on PATH is the exe:

   ```powershell
   uv tool uninstall embyx-manager
   ```

4. The WireGuard tunnel and `%APPDATA%\embyx-transcode\config.toml` stay as they are: the Rust
   worker reads the same file with the same keys (`server`, `token`, `upload_mbps`, `jobs`,
   `max_queue_gb`, `cache_dir`, `ffmpeg`, `ffprobe`, `[encoder]`) and the same defaults.

5. Run it as before, in a terminal you leave open:

   ```powershell
   embyx-manager transcode-worker
   ```

   Ctrl+C once finishes the chunk under way, uploads what is queued and exits; twice exits at
   once. The command-line flags are the same (`--server`, `--upload-mbps`, `--jobs`,
   `--max-queue-gb`, `--cache-dir`, `--ffmpeg`, `--ffprobe`, `--config`).

To update, download the new release's exe over the old one while the worker is not running.

## ffmpeg and ffprobe must be the real executables

The worker pauses, resumes and stops ffmpeg itself (when the upload queue is full, on Ctrl+C, when
the cluster cancels a task). It can only do that to the process it started. Some package managers
put a launcher in front of ffmpeg instead of the program:

- Chocolatey: `C:\ProgramData\chocolatey\bin\ffmpeg.exe` is a shim that starts the real ffmpeg as
  its child.
- Scoop: `%USERPROFILE%\scoop\shims\ffmpeg.exe` is the same kind of shim.

With a shim, a stop or a pause reaches only the shim: the real ffmpeg keeps encoding and holding
the output, and the worker's stop only lands when the part is finished. This is true of the Python
worker as well; neither build can see through a shim.

winget's `Gyan.FFmpeg` is fine: the `ffmpeg` and `ffprobe` it puts on PATH are links to the real
executables. To check what the worker will start:

```powershell
(Get-Command ffmpeg).Source
(Get-Command ffprobe).Source
```

If either path is under `chocolatey\bin` or `scoop\shims`, point the worker at the real files in
`config.toml` (or with `--ffmpeg` / `--ffprobe`):

```toml
# Chocolatey
ffmpeg = 'C:\ProgramData\chocolatey\lib\ffmpeg\tools\ffmpeg\bin\ffmpeg.exe'
ffprobe = 'C:\ProgramData\chocolatey\lib\ffmpeg\tools\ffmpeg\bin\ffprobe.exe'
# Scoop
# ffmpeg = 'C:\Users\<you>\scoop\apps\ffmpeg\current\bin\ffmpeg.exe'
# ffprobe = 'C:\Users\<you>\scoop\apps\ffmpeg\current\bin\ffprobe.exe'
```

(Single quotes: TOML takes the backslashes literally.)

## A choice for Max: a Job Object around ffmpeg

The worker could put every ffmpeg it starts into a Windows Job Object. Ending the job ends the
shim and the real ffmpeg together, so a shim would no longer matter for a stop; with the job's
kill-on-close flag the encoders would also end when the worker process is killed from outside
(Task Manager), where today, as with Python, a child on Windows outlives its parent. Pausing would
still need the real process (`NtSuspendProcess` on the shim does not pause its child), so the
advice above stays either way.

It is not built: the worker matches the Python one here, and with winget's ffmpeg nothing needs it.
If you want it, it is a small change to the worker's process module (`crates/embyx-transcode-worker`,
D165) with a test that runs a shim.

## Switching from the Python worker

The cluster keeps every chunk it has accepted, and a worker that starts resumes after the last one
the cluster holds, whichever build sent it (the protocol test runs every pairing: Rust worker with
Python listener, Python worker with Rust listener, and Rust with Rust). The local cache's sidecars
have the Python worker's format (D172), but resuming a part from a cache the other build wrote was
not tested; at worst the chunks the cluster lacks are encoded again. Stop the Python worker with one
Ctrl+C, let it finish uploading, then start the exe.

# Home GPU transcodes

A merged title must stay under 115's 115 GB upload limit. When the parts add up to more,
the merge has to re-encode the video. The merge Job can do that on the cluster's CPU
with SVT-AV1, but a 4K VR title takes days there. The home transcode worker runs the
encode on an NVIDIA GPU at home instead (NVENC AV1), and the cluster does the rest.

## How a task moves

1. Auto-merge or **合并** queues a title. With **超过 115GB 的交给家里的 GPU 转码** on in
   Settings → 合并, a title whose parts exceed the limit stays `queued` until the worker
   claims it. While the worker is offline, the title does not hold up the lossless
   merges behind it. Once the worker is online, auto-merge stops queuing new titles,
   so the title goes next after the merge under way.
2. When the worker asks for work and nothing else is merging, the task moves to
   `transcoding`. An *audio Job* (`embyx-audio-<id>-…`) probes the parts and copies each
   part's audio into `/upload/merge/.transcode/<id>/audio/`, at the same time as the GPU works.
3. The worker reads each part over HTTP Range requests and encodes video only. ffmpeg
   writes chunks of about a minute that start on keyframes. Each chunk is uploaded with
   its start, duration, frame count and SHA-256. The cluster keeps a chunk only if it
   continues the part without a gap.
4. When every part is finished and the audio is copied, the merge Job runs with
   `--home-assemble`. It concatenates the chunks and the audio with `-c copy`, using each
   part's original duration so the audio stays in sync. After that the task uploads,
   verifies and archives like any other merge.

If the worker goes offline, the task waits for it, marked "等家里的 GPU worker 上线", and
lossless merges go on meanwhile. The merge page shows the worker (online or not, fps per
part, upload rate, queue) and has three controls for a home task:

- **暂停** / **继续** stop and resume the encoder. Uploads already queued still go out.
- **改用集群 CPU** drops the chunks and sends the title to the merge Job's SVT-AV1 path.
- **取消** removes the task and its chunks.

A failed audio Job or assembly can be retried. The chunks the cluster already holds are kept.

## Cluster side

| Variable | Purpose |
| --- | --- |
| `EMBYX_MANAGER_TRANSCODE_PORT` | Opens a second listener that serves only `/api/transcode/*` (claim, heartbeat, part reads, chunk uploads). Unset: the worker has nothing to talk to. |
| `EMBYX_MANAGER_TRANSCODE_TOKEN` | Optional bearer token the worker must send. |

The second listener is separate from the main app so that the worker's tunnel reaches
nothing else. Its uploads stream straight to disk; the main app's 64 KB body limit does not
apply. In nas-gitops, a WireGuard gateway pod on nas-us-2 forwards the tunnel to this port.

## Windows worker

The worker is one file, `embyx-manager.exe` (Windows x86-64), from this repository's
[Releases](https://github.com/cyrahs/embyx-manager/releases). It needs no Python, no uv and no
Visual C++ redistributable. You need an NVIDIA GPU with AV1 NVENC (RTX 40 series or later) and
a recent driver.

1. Install the tools:

   ```powershell
   winget install Gyan.FFmpeg      # the full build has av1_nvenc
   winget install WireGuard.WireGuard
   ```

2. Download `embyx-manager.exe` and `embyx-manager.exe.sha256` from the latest release into a
   folder of your choice, for example `%LOCALAPPDATA%\Programs\embyx-manager`, and check it:

   ```powershell
   $dir = "$env:LOCALAPPDATA\Programs\embyx-manager"
   (Get-FileHash "$dir\embyx-manager.exe" -Algorithm SHA256).Hash.ToLower()
   Get-Content "$dir\embyx-manager.exe.sha256"     # the same 64 characters
   ```

   Add that folder to your user PATH (Settings, System, About, Advanced system settings,
   Environment Variables) or run the exe by its full path.

3. Import the WireGuard tunnel you were given and activate it.
4. Write `%APPDATA%\embyx-transcode\config.toml`:

   ```toml
   server = "http://10.117.200.1:8080"
   # token = "…"            # only if the cluster sets EMBYX_MANAGER_TRANSCODE_TOKEN
   upload_mbps = 30         # stay under the line's upstream, leave room for other use
   jobs = 3                 # parts encoded at once
   max_queue_gb = 8         # chunks waiting for upload before encoding pauses
   # cache_dir = "D:\\embyx-transcode"   # default %LOCALAPPDATA%\embyx-transcode
   # ffmpeg = 'C:\…\ffmpeg.exe'           # default: ffmpeg on PATH (see below)
   # ffprobe = 'C:\…\ffprobe.exe'

   # [encoder]              # overrides what the cluster sends, e.g.
   # tune = "hq"
   ```

5. Run it in a terminal and leave the window open:

   ```powershell
   embyx-manager transcode-worker
   ```

The status line shows each part's progress and fps, the upload rate and the queue.
**Ctrl+C** once finishes the chunk under way, uploads what is queued, and exits.
**Ctrl+C** twice exits at once. The next start picks up after the last chunk the cluster
holds, or after the last chunk still in the local cache.

Every setting can also be given on the command line (`--server`, `--token`, `--upload-mbps`,
`--jobs`, `--max-queue-gb`, `--cache-dir`, `--ffmpeg`, `--ffprobe`, `--config`).

To update, stop the worker (one Ctrl+C, let it finish uploading) and replace the exe with the
new release's.

### ffmpeg and ffprobe must be the real executables

The worker pauses, resumes and stops ffmpeg itself (when the upload queue is full, on Ctrl+C,
when the cluster cancels a task). It can only do that to the process it started. Some package
managers put a launcher in front of ffmpeg instead of the program:

- Chocolatey: `C:\ProgramData\chocolatey\bin\ffmpeg.exe` is a shim that starts the real ffmpeg
  as its child.
- Scoop: `%USERPROFILE%\scoop\shims\ffmpeg.exe` is the same kind of shim.

With a shim, a stop or a pause reaches only the shim: the real ffmpeg keeps encoding and holding
the output, and the stop only lands when the part is finished. winget's `Gyan.FFmpeg` is fine:
the `ffmpeg` and `ffprobe` it puts on PATH are links to the real executables. To check what the
worker will start:

```powershell
(Get-Command ffmpeg).Source
(Get-Command ffprobe).Source
```

If either path is under `chocolatey\bin` or `scoop\shims`, find the real files and point the
worker at them with `ffmpeg` / `ffprobe` in `config.toml` (or `--ffmpeg` / `--ffprobe`):

```powershell
# Chocolatey (the folder name depends on the package version)
(Get-ChildItem C:\ProgramData\chocolatey\lib\ffmpeg -Recurse -Filter ffmpeg.exe | Select-Object -First 1).FullName
# Scoop
(Get-ChildItem $env:USERPROFILE\scoop\apps\ffmpeg\current -Recurse -Filter ffmpeg.exe | Select-Object -First 1).FullName
```

Use single quotes for those paths in TOML, which then takes the backslashes literally.

### The Python worker

Until November 2026 the worker was a Python program installed with uv. It speaks the same
protocol and reads the same `config.toml`, so a PC that still runs it keeps working against the
cluster. It now installs only from the tag of the last Python build:

```powershell
uv tool install --force git+https://github.com/cyrahs/embyx-manager@python-final
```

(The old command without `@python-final` no longer works: `main` has no Python package.)

To switch to the exe: stop the Python worker with one Ctrl+C and let it finish uploading, remove
it with `uv tool uninstall embyx-manager` so that `embyx-manager` on PATH is the exe, then start
the exe as above. The WireGuard tunnel and `config.toml` stay as they are. The cluster keeps every
chunk it has accepted, and a worker resumes after the last one it holds, whichever build sent it.

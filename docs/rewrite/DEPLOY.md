# Deploying the Rust build

What it takes to run the Rust build in the cluster and on the home worker, and how to go back. Nothing
here has been done: no image of the Rust build has been pushed, no release made, no workflow below
added, and nas-gitops is unchanged. Each step is Max's to start.

## What stays the same

- **The database.** The schema stays at version 19 and the Rust build adds no migration, so either
  build runs on the same database, and Python can be put back at any time without a restore. The
  resume matrix of the scenario transcripts (`docs/rewrite/SCENARIOS.md`) continues rows and files
  one build wrote with the other, both ways; the plan's cross-version contracts list what that holds.
- **The configuration.** The same nine `app_config` sections with the same keys, defaults and
  validation (the config goldens); no new key.
- **The environment.** The Rust build reads exactly the variables the Python one reads
  (`EMBYX_MANAGER_*`, `POD_NAME`, `POD_NAMESPACE`, `KUBERNETES_SERVICE_*`), so
  `secrets/embyx-manager.sops.env` and the deployment's `env` stay as they are.
- **The image's contract.** `Dockerfile.rust` keeps what the manifests rely on: `embyx-manager` on
  `PATH` (the merge Job's command is `embyx-manager merge-worker`), entry point `embyx-manager` with
  command `serve`, port 8000 and the transcode port from `EMBYX_MANAGER_TRANSCODE_PORT`, ffmpeg and
  ffprobe from Debian 13 (7.1.5 with SVT-AV1 2.3.0, as in the Python image), CA certificates and time
  zone data. It writes only to `HOME`/`TMPDIR` (`/tmp`, an `emptyDir`) and the mounted volumes, so
  `readOnlyRootFilesystem` and `runAsUser: 0` with `DAC_OVERRIDE` work unchanged. The front end sits
  at `/opt/embyx-manager/static`, next to the executable (D59).
- **The merge Job template** (`merge-job.json`): unchanged. The app starts each Job with the image
  digest its own pod runs, so after the switch new merges run the Rust worker; a Job the Python app
  started finishes on the Python image, and either build reads the other's status files.
- **The home worker's protocol.** Every pairing of worker and listener works (contract 5), so the
  cluster and the home worker can switch on different days, in either order.

## The image workflow for `rust` (not added)

`rust.yml` builds `Dockerfile.rust` on every pull request and push but pushes nothing. Publishing the
Rust image needs a workflow of its own, for example `.github/workflows/rust-image.yml`. It runs only
after `rust.yml` passed on a push to `rust`, and never writes the `latest` tag, which Flux follows and
which `ci.yml` keeps building from `main` (Python) until the plan's later steps:

```yaml
name: Rust image

on:
  workflow_run:
    workflows: [Rust rewrite]
    types: [completed]
    branches: [rust]

permissions:
  contents: read
  packages: write

jobs:
  image:
    if: github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.event == 'push'
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v5
      with:
        ref: ${{ github.event.workflow_run.head_sha }}
    - uses: docker/setup-buildx-action@v3
    - uses: docker/login-action@v3
      with:
        registry: ghcr.io
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}
    - uses: docker/build-push-action@v6
      with:
        context: .
        file: Dockerfile.rust
        push: true
        tags: |
          ghcr.io/cyrahs/embyx-manager:rust
          ghcr.io/cyrahs/embyx-manager:rust-${{ github.event.workflow_run.head_sha }}
        cache-from: type=gha,scope=rust-image
        cache-to: type=gha,scope=rust-image,mode=max
```

Adding it means widening rule 4 of `tools/check_tree.sh`, which today allows only the baseline's
workflows and a read-only `rust.yml`.

## The release workflow for the home worker (not added)

The Windows exe is published as a GitHub Release, created only at deployment, for example by
`.github/workflows/worker-release.yml` on a tag `worker-v<version>`:

```yaml
name: Home worker release

on:
  push:
    tags: [ 'worker-v*' ]

permissions:
  contents: write

jobs:
  release:
    runs-on: windows-latest
    env:
      # The MSVC runtime linked in, so the exe needs no Visual C++ redistributable.
      RUSTFLAGS: -C target-feature=+crt-static
    steps:
    - uses: actions/checkout@v5
    - run: rustup toolchain install
    - run: cargo build -p embyx-manager --release --locked
    - name: Checksum
      shell: pwsh
      run: |
        $hash = (Get-FileHash target/release/embyx-manager.exe -Algorithm SHA256).Hash.ToLower()
        "$hash  embyx-manager.exe" | Out-File -Encoding ascii target/release/embyx-manager.exe.sha256
    - name: Release
      shell: bash
      env:
        GH_TOKEN: ${{ github.token }}
      run: >
        gh release create "$GITHUB_REF_NAME" --title "Home worker $GITHUB_REF_NAME" --notes "docs/rewrite/HOME-WORKER.md"
        target/release/embyx-manager.exe target/release/embyx-manager.exe.sha256
```

The CI exe (`rust.yml`, workflow artifact) links the MSVC runtime dynamically; the `crt-static` build
above was not tried in CI. `docs/rewrite/HOME-WORKER.md` is what Max follows on the Windows machine.

## Switching the cluster

A way to do it with what is there today; Max decides when and whether.

1. Prefer a quiet moment: no merge Job running and no home transcode in progress. Both survive a
   switch by design (rows, Job names and status files are shared: cross-version contract 4), but a switch with a
   Job running was not tried on the cluster.
2. Take a dump of the embyx database (the schema does not change, so this is for a mistake in the
   rows, not for a rollback of the schema).
3. Point Flux at the Rust image: in nas-gitops `infra/image-automation/embyx-manager-ghcr.yaml`, set
   the ImagePolicy's `filterTags.pattern` from `'^latest$'` to `'^rust$'`. Flux then writes the `rust`
   digest into `apps/emby/embyx-manager/kustomization.yaml`; the deployment (`Recreate`) restarts.
   Nothing else in the manifests changes.
4. Check: `/api/health`, every page of the dashboard, the run history, one manual mapping and archive
   run, the merge tab's task list, and the pod's memory for a few days (below).
5. Going back: set the pattern to `'^latest$'` again (Python's image is still built from `main`).
   Rows the Rust build wrote are the Python build's shapes.

The home worker switches on its own schedule (`HOME-WORKER.md`), after a release exists.

## Memory and the pod's resources

The deployment asks for 256Mi and limits at 768Mi (nas-gitops `apps/emby/embyx-manager/deployment.yaml`).
The WP16 benchmark (`tools/bench/memory.py`, `docs/rewrite/REPORT.md`) ran both builds as real server
processes with every service wired, over fakes, through idle, a request mix, a mapping run, two
archive runs, a fill-actor job and a 1 GiB chunk upload:

| Build | RSS idle | RSS at the end | highest RSS seen |
| --- | --- | --- | --- |
| Python (uvicorn, 3.13) | 105.8 MiB | 119.1 MiB | 123.1 MiB |
| Rust, system allocator | 25.5 MiB | 36.5 MiB | 43.0 MiB |

The Rust build uses the system allocator: jemalloc ended at 47.2 MiB and mimalloc at 93.1 MiB on the
same run, so neither is linked. Both figures are process memory; the pod's working set also counts
page cache from the archive and mapping file work, which the benchmark does not model.

A choice for Max: keep 256Mi / 768Mi (room to spare), or lower the request (for example to 128Mi)
once the pod's `container_memory_working_set_bytes` has been watched for a week after the switch.
The merge Job's resources (1Gi / 3Gi) are ffmpeg's and do not change with the build.

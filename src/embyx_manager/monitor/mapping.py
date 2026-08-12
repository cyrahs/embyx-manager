"""STRM mapping pipeline: mirror flat xx/yy/zz.strm into xx/yy/zz/zz.strm.

Remote storage keeps a flat structure to reduce CloudDrive access; the local
mirror adds one directory per title so Emby classifies entries correctly.
Ported from embyx-monitor's mapping.py with injected configuration and
structured run reporting.
"""

import filecmp
import shutil
from pathlib import Path

from embyx_manager.config.models import MappingConfig
from embyx_manager.core.avid import AvidParser
from embyx_manager.monitor.reports import RunContext


class MappingPipeline:
    def __init__(self, *, config: MappingConfig, avid_parser: AvidParser) -> None:
        self._config = config
        self._avid_parser = avid_parser
        self.src_dir = Path(config.src_dir)
        self.dst_dir = Path(config.dst_dir)

    # -- full sync --------------------------------------------------------

    def run_full(self, ctx: RunContext) -> None:
        """Full rebuild: update everything, delete strays, remove empty dirs."""
        if not self.src_dir.is_dir():
            msg = f'{self.src_dir} is not a directory'
            raise ValueError(msg)
        if not self.dst_dir.exists():
            self.dst_dir.mkdir(parents=True)
        elif not self.dst_dir.is_dir():
            msg = f'{self.dst_dir} is not a directory'
            raise ValueError(msg)

        ctx.info('starting mapping from %s to %s', self.src_dir, self.dst_dir)
        self._update_all(ctx)
        self._delete_strays(ctx)
        self._delete_empty_dirs(ctx)

    # -- incremental ------------------------------------------------------

    def run_incremental(
        self,
        ctx: RunContext,
        *,
        changed: set[Path],
        deleted: set[Path],
    ) -> tuple[set[Path], set[Path]]:
        """Apply one debounced batch of watchdog events; returns paths that failed."""
        failed_changed: set[Path] = set()
        failed_deleted: set[Path] = set()
        for path in sorted(deleted):
            ctx.check_cancelled()
            try:
                self.delete_one(path, ctx)
            except Exception:  # noqa: BLE001
                ctx.exception('Incremental delete failed for %s', path)
                failed_deleted.add(path)
        for path in sorted(changed):
            ctx.check_cancelled()
            try:
                self.update_one(path, ctx)
            except Exception:  # noqa: BLE001
                ctx.exception('Incremental update failed for %s', path)
                failed_changed.add(path)
        return failed_changed, failed_deleted

    # -- single-path operations -------------------------------------------

    def map_strm_path(self, src: Path) -> Path | None:
        if src.suffix.lower() != '.strm':
            return None
        rel_path = _relative_to(src, self.src_dir)
        if rel_path is None:
            return None
        avid = self._avid_parser.get_avid(src.name)
        if not avid:
            return None
        return self.dst_dir / rel_path.parent / avid / src.name

    def update_one(self, src: Path, ctx: RunContext) -> None:
        dst = self.map_strm_path(src)
        if not dst:
            if src.suffix.lower() == '.strm':
                ctx.warning('failed to get avid for %s, skipping', src)
            return
        if not src.exists():
            ctx.warning('source file missing, skipping %s', src)
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists() and not dst.is_file():
            msg = f'{dst} exists and is not a file'
            raise FileExistsError(msg)
        if dst.exists() and src.stat().st_mtime <= dst.stat().st_mtime and filecmp.cmp(src, dst, shallow=False):
            ctx.add('files_skipped')
            return
        shutil.copy2(src, dst)
        ctx.add('files_updated')
        ctx.info('updated %s -> %s', _display(src, self.src_dir), _display(dst, self.dst_dir))

    def delete_one(self, src: Path, ctx: RunContext) -> None:
        dst = self.map_strm_path(src)
        if not dst or not dst.exists():
            return
        dst.unlink()
        ctx.add('files_deleted')
        ctx.info('deleted %s', _display(dst, self.dst_dir))
        self._delete_empty_dirs_for_path(dst.parent, ctx)

    # -- internals ----------------------------------------------------------

    def _update_all(self, ctx: RunContext) -> None:
        for src in self.src_dir.glob('**/*.strm'):
            ctx.check_cancelled()
            self.update_one(src, ctx)

    def _delete_strays(self, ctx: RunContext) -> None:
        """Delete xx/yy/zz/zz.strm in dst whose xx/yy/zz.strm no longer exists."""
        for dst in self.dst_dir.glob('**/*.strm'):
            ctx.check_cancelled()
            src_rel_dir = dst.relative_to(self.dst_dir).parent.parent
            src = self.src_dir / src_rel_dir / dst.name
            if not src.exists():
                dst.unlink()
                ctx.add('files_deleted')
                ctx.info('deleted %s', dst.relative_to(self.dst_dir))

    def _delete_empty_dirs(self, ctx: RunContext) -> None:
        # Intentional cleanup: directories without descendant .strm files are removed
        # as derived mapping output, even if non-.strm leftovers are still present.
        empty_dirs = [p for p in self.dst_dir.glob('**/*') if p.is_dir() and not any(p.glob('**/*.strm'))]
        if not empty_dirs:
            return
        empty_dirs.sort(reverse=True)
        for empty_dir in empty_dirs:
            ctx.check_cancelled()
            if not empty_dir.exists():
                continue
            shutil.rmtree(empty_dir)
            ctx.add('dirs_deleted')
            ctx.info('deleted empty directory: %s', empty_dir.relative_to(self.dst_dir))

    def _delete_empty_dirs_for_path(self, path: Path, ctx: RunContext) -> None:
        if not self.dst_dir.exists():
            return
        current = path
        while current != self.dst_dir and self.dst_dir in current.parents:
            try:
                if any(current.iterdir()):
                    break
            except FileNotFoundError:
                current = current.parent
                continue
            current.rmdir()
            ctx.add('dirs_deleted')
            ctx.info('deleted empty directory: %s', current.relative_to(self.dst_dir))
            current = current.parent


def _relative_to(src: Path, src_dir: Path) -> Path | None:
    try:
        return src.relative_to(src_dir)
    except ValueError:
        try:
            return src.absolute().relative_to(src_dir.absolute())
        except ValueError:
            return None


def _display(path: Path, base: Path) -> Path:
    try:
        return path.relative_to(base)
    except ValueError:
        return path

"""Archiving one downloaded folder into the library.

A folder holds one video, or the parts of one video, plus whatever the source
bundled with it. Archiving reads the AVID, renames the video after it, moves it
into the library under its brand, and deletes the folder: only the video is
wanted, and leaving ad reels and artwork behind would waste cloud storage.

The entry point is :meth:`ArchivePipeline.archive_folder`, which is told where
to put the result and optionally which AVID to expect. The tracker calls it for
a specific finished download; the reconcile scan calls it for whatever it finds
under a configured route. Both get the same decisions, and both get a structured
outcome rather than a log line: a folder that cannot be identified is reported
as needing attention instead of being guessed at or deleted.

Filesystem work is synchronous by nature; the scheduler runs it in a worker
thread and cancellation is checked between entries. Every operation crosses a
shared mount that can refuse any single call, so failures are contained per
folder: one unreadable file is reported and the rest of the run still gets its
turn.
"""

import re
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import HIGH_RESOLUTION_TAGS, AvidParser, get_brand, variant_tags
from embyx_manager.core.media import is_video
from embyx_manager.monitor.reports import RunCancelledError, RunContext

MIN_MULTI_PART_VIDEOS = 2
COPY_SUFFIX_RE = re.compile(r'\s*\(\d+\)$')


class Outcome(StrEnum):
    ARCHIVED = 'archived'
    #: No video worth keeping: ads, samples, or an empty shell.
    JUNK = 'junk'
    #: Videos are present but their identity is unclear; nothing was touched.
    NEEDS_ATTENTION = 'needs_attention'
    FAILED = 'failed'


@dataclass(frozen=True)
class ArchiveResult:
    outcome: Outcome
    avid: str = ''
    #: Library-relative paths of everything moved in, for the ledger to record.
    archived_paths: tuple[str, ...] = ()
    reason: str = ''


@dataclass
class _VideoSet:
    """The videos in one folder that are worth archiving, and their AVID."""

    avid: str
    videos: list[Path] = field(default_factory=list)


def multi_part_video_check(videos: list[Path]) -> bool:
    """Whether these files are parts of one video rather than separate videos."""
    if len(videos) < MIN_MULTI_PART_VIDEOS:
        return False
    # check videos only have different digits
    non_digit_parts = {re.sub(r'\d+', '', video.name) for video in videos}
    if len(non_digit_parts) == 1:
        return True
    # check videos like xxx-A.mp4 xxx-B.mp4
    non_index_parts = {re.sub(r'-[A-Z]', '', video.name) for video in videos}
    return len(non_index_parts) == 1


def is_4k_video(video: Path) -> bool:
    """Whether the file name marks this as the higher-resolution cut."""
    return bool(variant_tags(video.name) & HIGH_RESOLUTION_TAGS)


def normalize_copy_suffix(stem: str) -> str:
    return COPY_SUFFIX_RE.sub('', stem)


@contextmanager
def _isolate(ctx: RunContext, message: str, *args: object) -> Iterator[None]:
    """Report a failure and carry on, so one bad entry cannot end the run.

    Cancellation is deliberately let through: it is the operator asking to
    stop, not a failure to contain.
    """
    try:
        yield
    except RunCancelledError:
        raise
    except Exception:  # noqa: BLE001
        ctx.exception(message, *args)
        ctx.add('items_failed')


class ArchivePipeline:
    def __init__(self, *, config: ArchiveConfig, avid_parser: AvidParser) -> None:
        self._config = config
        self._avid = avid_parser
        self.src_dir = Path(config.src_dir)
        self.dst_dir = Path(config.dst_dir)

    # -- the full scan -------------------------------------------------------

    def run(self, ctx: RunContext) -> None:
        """Archive every folder sitting under the configured routes.

        Priority routes go first so that a copy arriving there can claim an AVID
        already filed under a normal route.
        """
        routes = [(src, dst, True) for src, dst in self._config.priority_mapping.items()]
        routes += [(src, dst, False) for src, dst in self._config.mapping.items()]
        for src, dst, priority in routes:
            ctx.check_cancelled()
            root = self.src_dir / src
            if not root.is_dir():
                # A route whose source is absent has nothing to do; it is not a failure.
                ctx.info('skipping %s, its source directory does not exist', _display(root, self.src_dir))
                continue
            ctx.info('processing %s -> %s%s', root, self.dst_dir / dst, ' (priority)' if priority else '')
            self.scan_route(root, dst, ctx, priority=priority)

    def scan_route(self, root: Path, dst_subdir: str, ctx: RunContext, *, priority: bool = False) -> None:
        entries = sorted(root.iterdir())
        for folder in entries:
            ctx.check_cancelled()
            if folder.is_dir():
                self.archive_folder(folder, dst_subdir, ctx, priority=priority)
        loose = [entry for entry in entries if is_video(entry)]
        if loose:
            self._archive_loose_videos(root, loose, dst_subdir, ctx, priority=priority)

    def _archive_loose_videos(
        self,
        root: Path,
        videos: list[Path],
        dst_subdir: str,
        ctx: RunContext,
        *,
        priority: bool,
    ) -> None:
        """File videos sitting directly in a route, with no folder of their own.

        Manual drops and leftovers from the pipeline that used to stage videos
        here. They are grouped by AVID so a multi-part set still travels together,
        and unlike a folder there is nothing to delete afterwards.
        """
        by_avid: dict[str, list[Path]] = {}
        for video in videos:
            avid = self._avid.get_avid(video.name)
            if not avid:
                ctx.warning('failed to read an avid for the loose file %s, skipping', video.name)
                continue
            by_avid.setdefault(avid, []).append(video)
        for avid, group in by_avid.items():
            ctx.check_cancelled()
            with _isolate(ctx, 'failed to archive the loose file %s', _display(root / avid, self.src_dir)):
                archived = self._move_into_library(
                    _VideoSet(avid=avid, videos=group), dst_subdir, ctx, priority=priority
                )
                ctx.add('videos_archived', len(archived))

    # -- the one way in ------------------------------------------------------

    def archive_folder(
        self,
        folder: Path,
        dst_subdir: str,
        ctx: RunContext,
        *,
        priority: bool = False,
        expected_avid: str = '',
    ) -> ArchiveResult:
        """Move one folder's video into the library and delete the folder.

        ``dst_subdir`` names the library subdirectory this folder's route feeds;
        brand routing may still send an individual brand elsewhere.
        ``expected_avid`` is the AVID the caller already knows this download to
        be: anything else found is reported rather than filed under a guess.
        """
        try:
            selection = self._select_videos(folder, ctx, expected_avid=expected_avid)
        except RunCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            ctx.exception('failed to inspect %s', _display(folder, self.src_dir))
            ctx.add('items_failed')
            return ArchiveResult(Outcome.FAILED, reason=str(exc))
        if isinstance(selection, ArchiveResult):
            return selection

        try:
            archived = self._move_into_library(selection, dst_subdir, ctx, priority=priority)
        except RunCancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            ctx.exception('failed to archive %s', _display(folder, self.src_dir))
            ctx.add('items_failed')
            return ArchiveResult(Outcome.FAILED, avid=selection.avid, reason=str(exc))
        if not archived:
            return ArchiveResult(
                Outcome.NEEDS_ATTENTION,
                avid=selection.avid,
                reason='destination already holds this avid',
            )

        self._discard(folder, ctx)
        ctx.add('videos_archived', len(archived))
        return ArchiveResult(Outcome.ARCHIVED, avid=selection.avid, archived_paths=archived)

    # -- selection -----------------------------------------------------------

    def _select_videos(  # noqa: PLR0911 - each early return names a distinct verdict
        self,
        folder: Path,
        ctx: RunContext,
        *,
        expected_avid: str,
    ) -> _VideoSet | ArchiveResult:
        """Decide which files in a folder are the video, or why we cannot tell."""
        if not folder.is_dir():
            return ArchiveResult(Outcome.FAILED, reason=f'{folder} is not a directory')
        min_size_bytes = self._config.min_size_mb * 1024 * 1024
        # Recursive: sources nest the video inside a subfolder often enough, and
        # the folder is deleted afterwards, so missing one would delete it.
        videos = [
            candidate
            for candidate in sorted(folder.rglob('*'))
            if is_video(candidate) and candidate.stat().st_size > min_size_bytes
        ]
        if not videos:
            ctx.info('%s holds no video above %dMB', folder.name, self._config.min_size_mb)
            return ArchiveResult(Outcome.JUNK, reason='no video above the size threshold')

        videos = self._drop_duplicate_copies(folder, videos, ctx)
        avids = {self._avid.get_avid(video.name) for video in videos}
        if len(avids) > 1:
            reason = f'multiple avids in one folder: {", ".join(sorted(avids))}'
            ctx.warning('%s in %s, skipping', reason, folder.name)
            return ArchiveResult(Outcome.NEEDS_ATTENTION, reason=reason)
        avid = next(iter(avids))
        if not avid:
            # The file names gave nothing; the folder name is the last clue.
            avid = self._avid.get_avid(folder.name)
        if not avid:
            ctx.warning('failed to read an avid for %s, skipping', folder.name)
            return ArchiveResult(Outcome.NEEDS_ATTENTION, reason='no avid could be read')
        if expected_avid and avid != expected_avid:
            reason = f'expected {expected_avid} but found {avid}'
            ctx.warning('%s in %s, skipping', reason, folder.name)
            return ArchiveResult(Outcome.NEEDS_ATTENTION, avid=avid, reason=reason)

        if len(videos) > 1 and not multi_part_video_check(videos):
            kept = self._pick_high_resolution(folder, videos, avid, ctx)
            if kept is None:
                return ArchiveResult(
                    Outcome.NEEDS_ATTENTION,
                    avid=avid,
                    reason='several unrelated videos in one folder',
                )
            videos = [kept]
        return _VideoSet(avid=avid, videos=videos)

    def _pick_high_resolution(self, folder: Path, videos: list[Path], avid: str, ctx: RunContext) -> Path | None:
        """Choose the 4K cut when a folder ships several resolutions of one video."""
        high_resolution = [video for video in videos if is_4k_video(video)]
        if len(high_resolution) != 1:
            ctx.warning('several videos in %s that are not one multi-part set, skipping', folder.name)
            return None
        kept = high_resolution[0]
        if self._avid.get_avid(kept.name) != avid:
            ctx.warning('the high-resolution cut in %s has a different avid, skipping', folder.name)
            return None
        dropped = ', '.join(video.name for video in videos if video != kept)
        ctx.info('keeping the high-resolution %s in %s and dropping %s', kept.name, folder.name, dropped)
        return kept

    def _drop_duplicate_copies(self, folder: Path, videos: list[Path], ctx: RunContext) -> list[Path]:
        """Delete "name (1).mp4" copies that duplicate a same-sized original."""
        base_by_key: dict[tuple[str, str, int], Path] = {}
        copies_by_key: dict[tuple[str, str, int], list[Path]] = {}
        for video in videos:
            key = (normalize_copy_suffix(video.stem), video.suffix.lower(), video.stat().st_size)
            if video.stem == key[0]:
                base_by_key.setdefault(key, video)
            else:
                copies_by_key.setdefault(key, []).append(video)
        dropped: set[Path] = set()
        for key, copies in copies_by_key.items():
            base = base_by_key.get(key)
            if base is None:
                continue
            ctx.info(
                'duplicate videos in %s, keeping %s and dropping %s',
                folder.name,
                base.name,
                ', '.join(copy.name for copy in copies),
            )
            for copy in copies:
                dropped.add(copy)
                try:
                    copy.unlink()
                    ctx.add('duplicate_copies_deleted')
                except OSError:
                    ctx.exception('failed to remove the duplicate %s in %s', copy.name, folder.name)
        return [video for video in videos if video not in dropped]

    # -- moving --------------------------------------------------------------

    def _move_into_library(
        self,
        selection: _VideoSet,
        dst_subdir: str,
        ctx: RunContext,
        *,
        priority: bool,
    ) -> tuple[str, ...]:
        """Rename each part after the AVID and move it in; () when it is already there."""
        route_dst = self.dst_dir / dst_subdir
        brand_dir = self.find_dst_dir(selection.avid, route_dst, ctx)
        if brand_dir is None:
            return ()
        brand = get_brand(selection.avid)
        # Brands pinned by brand_mapping land in the same directory whatever the
        # route, so there is nothing to move between and nothing to guard.
        if brand and not self._brand_routed(brand):
            if priority:
                self._promote_from_normal(selection.avid, brand, brand_dir, ctx)
            elif self._held_by_priority(selection.avid, brand, brand_dir, ctx):
                return ()

        multi_part = len(selection.videos) > 1
        targets: list[tuple[Path, Path]] = []
        for index, video in enumerate(sorted(selection.videos, key=lambda path: path.name)):
            suffix = f'-cd{index + 1}{video.suffix}' if multi_part else video.suffix
            target = brand_dir / f'{selection.avid}{suffix}'
            if target.exists():
                ctx.warning('%s exists, skipping', _display(target, self.dst_dir))
                return ()
            targets.append((video, target))

        brand_dir.mkdir(parents=True, exist_ok=True)
        archived: list[str] = []
        for video, target in targets:
            ctx.info('moving %s to %s', video.name, _display(target, self.dst_dir))
            video.rename(target)
            archived.append(str(_display(target, self.dst_dir)))
        return tuple(archived)

    def _discard(self, folder: Path, ctx: RunContext) -> None:
        """Delete the folder once its video is in the library.

        Only the video is wanted: subtitles, artwork and the ad reels these
        downloads come padded with would otherwise pile up in cloud storage.
        """
        try:
            shutil.rmtree(folder)
            ctx.add('folders_discarded')
        except OSError:
            ctx.exception('archived %s but failed to remove it', folder.name)

    # -- destinations ---------------------------------------------------------

    def find_dst_dir(self, avid: str, dst_dir: Path, ctx: RunContext) -> Path | None:
        brand = get_brand(avid)
        if not brand:
            ctx.warning('failed to get brand for %s, skipping find_dst', avid)
            return None
        for brand_dst, brand_avids in self._config.brand_mapping.items():
            if brand in brand_avids:
                return self.dst_dir / brand_dst / brand
        return dst_dir / brand

    def _brand_routed(self, brand: str) -> bool:
        return any(brand in brand_avids for brand_avids in self._config.brand_mapping.values())

    @staticmethod
    def _matching_archived(avid: str, brand_dir: Path) -> list[Path]:
        """Archived videos belonging to avid, i.e. AVID.ext or AVID-cdN.ext."""
        if not brand_dir.is_dir():
            return []
        # The '-' keeps ABC-12 from claiming ABC-123.mp4 while still matching -cdN parts.
        return sorted(
            f for f in brand_dir.iterdir() if is_video(f) and (f.stem == avid or f.stem.startswith(avid + '-'))
        )

    def _promote_from_normal(self, avid: str, brand: str, target_dir: Path, ctx: RunContext) -> None:
        """Move avid out of every normal route's destination into the priority one."""
        for normal_dst in dict.fromkeys(self._config.mapping.values()):
            source_dir = self.dst_dir / normal_dst / brand
            if source_dir == target_dir:
                continue
            for archived in self._matching_archived(avid, source_dir):
                target = target_dir / archived.name
                if target.exists():
                    ctx.warning(
                        'cannot promote %s, %s exists',
                        _display(archived, self.dst_dir),
                        _display(target, self.dst_dir),
                    )
                    continue
                target_dir.mkdir(parents=True, exist_ok=True)
                ctx.info('promoting %s to %s', _display(archived, self.dst_dir), _display(target, self.dst_dir))
                # The emptied brand directory is left behind; the pipeline never
                # removes brand directories and an empty one routes nothing.
                archived.rename(target)
                ctx.add('duplicates_promoted')

    def _held_by_priority(self, avid: str, brand: str, own_dir: Path, ctx: RunContext) -> bool:
        for priority_dst in dict.fromkeys(self._config.priority_mapping.values()):
            holder_dir = self.dst_dir / priority_dst / brand
            if holder_dir == own_dir:
                continue
            if self._matching_archived(avid, holder_dir):
                ctx.warning('%s already archived in priority %s, skipping', avid, _display(holder_dir, self.dst_dir))
                ctx.add('skipped_priority')
                return True
        return False


def _display(path: Path, base: Path) -> Path:
    try:
        return path.relative_to(base)
    except ValueError:
        return path

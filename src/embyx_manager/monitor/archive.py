"""Archiving one downloaded folder into the library.

A folder holds one video — possibly in parts, possibly in several cuts of
which only the best is wanted — plus whatever the source bundled with it.
Archiving reads the AVID, renames the video after it, moves it into the
library under its brand, and deletes the folder: only the video is wanted, and
leaving ad reels and artwork behind would waste cloud storage.

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
from pathlib import Path, PurePosixPath

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import HIGH_RESOLUTION_TAGS, AvidParser, get_brand, strip_variant_tags, variant_tags
from embyx_manager.core.media import is_video
from embyx_manager.monitor.reports import RunCancelledError, RunContext

MIN_MULTI_PART_VIDEOS = 2
COPY_SUFFIX_RE = re.compile(r'\s*\(\d+\)$')
#: Separator debris left behind where a quality tag was stripped out of a name.
_SEPARATOR_RUN_RE = re.compile(r'[-_. ]{2,}')
#: A part number at the end of a name: ``xxx-2``, ``xxx_02``. Two digits at
#: most, so a trailing ID number ("wsp-162") can never read as a part index.
_TRAILING_INDEX_RE = re.compile(r'^(?P<base>.+?)[-_ ](?P<index>\d{1,2})$')
#: A part letter at the end of a name: ``sdmu-371a``, with or without separator.
_TRAILING_LETTER_RE = re.compile(r'^(?P<base>.+)(?P<letter>[a-z])$')
#: hjd2048 releases bundle a phone-sized rip of the main video, always named
#: ``{avid}-5``; it is an inferior cut of the whole video, never a part of it.
_PHONE_RIP_RE = re.compile(r'^(?P<base>.+)[-_]5$')
#: A phone rip is a fraction of the main file; anything above half its size is
#: not confidently a rip and is left for an operator.
PHONE_RIP_MAX_SHARE = 0.5
#: DMM content IDs sometimes carry one zero to fill their numeric part to five
#: digits. Keep that spelling in the parser, but let a tracked download reconcile
#: it with the four-digit AVID the RSS item supplied.
_SINGLE_ZERO_PADDED_AVID_RE = re.compile(r'^(?P<brand>[A-Z]{2,10})-0(?P<number>\d{4})$')


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
    """The videos in one folder that are worth archiving, in cd order."""

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


def _variant_base(name: str) -> str:
    """A file's stem with quality tags stripped, for comparing sibling files.

    Two names with the same base are cuts of the same thing; a base that only
    differs by a trailing index or letter is a part of it.
    """
    base = _SEPARATOR_RUN_RE.sub('-', strip_variant_tags(name))
    return base.strip('-_. ').casefold()


def _order_as_parts(videos: list[Path]) -> list[Path] | None:
    """The videos ordered as cd1..cdN, or None when they do not read as one set.

    Three naming shapes count as a set, all judged on the tag-stripped names:
    every file carries an index and they run 1..N; an unindexed file leads
    files indexed 2..N (``wsp-162`` + ``wsp-162-2``); every file carries a
    letter and they run from A (``sdmu-371a`` + ``sdmu-371b``).
    """
    bases = {video: _variant_base(video.name) for video in videos}
    return _indexed_run(videos, bases) or _unindexed_first(videos, bases) or _letter_run(videos, bases)


def _indexed_run(videos: list[Path], bases: dict[Path, str]) -> list[Path] | None:
    parsed: dict[Path, tuple[str, int]] = {}
    for video in videos:
        match = _TRAILING_INDEX_RE.match(bases[video])
        if match is None:
            return None
        parsed[video] = (match['base'], int(match['index']))
    if len({base for base, _ in parsed.values()}) != 1:
        return None
    ordered = sorted(videos, key=lambda video: parsed[video][1])
    if [parsed[video][1] for video in ordered] != list(range(1, len(videos) + 1)):
        return None
    return ordered


def _unindexed_first(videos: list[Path], bases: dict[Path, str]) -> list[Path] | None:
    for lead in videos:
        rest = [video for video in videos if video is not lead]
        sibling_re = re.compile(re.escape(bases[lead]) + r'[-_ ](\d{1,2})$')
        indices = {}
        for video in rest:
            match = sibling_re.match(bases[video])
            if match is None:
                break
            indices[video] = int(match.group(1))
        else:
            ordered = sorted(rest, key=lambda video: indices[video])
            if [indices[video] for video in ordered] == list(range(2, len(videos) + 1)):
                return [lead, *ordered]
    return None


def _letter_run(videos: list[Path], bases: dict[Path, str]) -> list[Path] | None:
    parsed: dict[Path, tuple[str, str]] = {}
    for video in videos:
        match = _TRAILING_LETTER_RE.match(bases[video])
        if match is None:
            return None
        parsed[video] = (match['base'], match['letter'])
    if len({base for base, _ in parsed.values()}) != 1:
        return None
    ordered = sorted(videos, key=lambda video: parsed[video][1])
    letters = [parsed[video][1] for video in ordered]
    if letters != [chr(ord('a') + offset) for offset in range(len(videos))]:
        return None
    return ordered


def normalize_copy_suffix(stem: str) -> str:
    return COPY_SUFFIX_RE.sub('', stem)


def _avids_are_padding_equivalent(first: str, second: str) -> bool:
    """Whether two regular AVIDs differ only by one five-digit padding zero."""
    if first == second:
        return True
    for padded, unpadded in ((first, second), (second, first)):
        match = _SINGLE_ZERO_PADDED_AVID_RE.fullmatch(padded)
        if match is not None and unpadded == f'{match["brand"]}-{match["number"]}':
            return True
    return False


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
        self._route_roots = frozenset(
            self.src_dir / source for table in (config.priority_mapping, config.mapping) for source in table
        )

    def avid_of(self, name: str) -> str:
        """The AVID a file or folder name reads as; '' when none can be read."""
        return self._avid.get_avid(name)

    def covers_other_route(self, folder: Path, *, route_root: Path) -> bool:
        """Whether a folder in one route is, or contains, another route's source.

        Routes nest when a category downloads into a subdirectory of the shared
        inbox. The outer route must leave that subdirectory alone: archiving it
        as if it were a download would file one video out of everything inside
        and then delete the whole directory.
        """
        return any(other != route_root and (other == folder or folder in other.parents) for other in self._route_roots)

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
            if not folder.is_dir():
                continue
            if self.covers_other_route(folder, route_root=root):
                ctx.info('skipping %s, it is another route of its own', _display(folder, self.src_dir))
                continue
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

    def _select_videos(
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
        identified = self._identify_avid(folder, videos, ctx, expected_avid=expected_avid)
        if isinstance(identified, ArchiveResult):
            return identified
        avid = identified

        if len(videos) > 1:
            arranged = self._arrange_videos(folder, videos, avid, ctx)
            if arranged is None:
                reason = 'several unrelated videos in one folder'
                ctx.warning('%s in %s, skipping', reason, folder.name)
                return ArchiveResult(Outcome.NEEDS_ATTENTION, avid=avid, reason=reason)
            videos = arranged
        return _VideoSet(avid=avid, videos=videos)

    def _identify_avid(
        self,
        folder: Path,
        videos: list[Path],
        ctx: RunContext,
        *,
        expected_avid: str,
    ) -> str | ArchiveResult:
        """Read the videos' AVID and reconcile it with a trusted expected value."""
        avids = {self._avid.get_avid(video.name) for video in videos}
        if len(avids) > 1:
            if expected_avid and all(_avids_are_padding_equivalent(expected_avid, avid) for avid in avids):
                found = ', '.join(sorted(avids))
                ctx.info(
                    '%s in %s are padding-equivalent to expected %s; using expected avid',
                    found,
                    folder.name,
                    expected_avid,
                )
                avid = expected_avid
            else:
                reason = f'multiple avids in one folder: {", ".join(sorted(avids))}'
                ctx.warning('%s in %s, skipping', reason, folder.name)
                return ArchiveResult(Outcome.NEEDS_ATTENTION, reason=reason)
        else:
            avid = next(iter(avids))
        if not avid:
            # The file names gave nothing; the folder name is the last clue.
            avid = self._avid.get_avid(folder.name)
        if not avid:
            ctx.warning('failed to read an avid for %s, skipping', folder.name)
            return ArchiveResult(Outcome.NEEDS_ATTENTION, reason='no avid could be read')
        if expected_avid and avid != expected_avid:
            if _avids_are_padding_equivalent(expected_avid, avid):
                ctx.info(
                    '%s in %s is padding-equivalent to expected %s; using expected avid',
                    avid,
                    folder.name,
                    expected_avid,
                )
                avid = expected_avid
            else:
                reason = f'expected {expected_avid} but found {avid}'
                ctx.warning('%s in %s, skipping', reason, folder.name)
                return ArchiveResult(Outcome.NEEDS_ATTENTION, avid=avid, reason=reason)
        return avid

    def _arrange_videos(self, folder: Path, videos: list[Path], avid: str, ctx: RunContext) -> list[Path] | None:
        """Explain a folder holding several videos of one AVID, or give up.

        In order: the files are one multi-part set; the same cut ships in
        several resolutions and the best of each survives; what survives is a
        multi-part set after all; the extras are phone rips of the main file.
        Returns the videos to archive in cd order, or None for an operator.
        """
        if multi_part_video_check(videos):
            return sorted(videos, key=lambda video: video.name)
        collapsed = self._collapse_resolution_variants(folder, videos, ctx)
        if collapsed is None:
            return None
        if len(collapsed) == 1:
            return collapsed
        return _order_as_parts(collapsed) or self._drop_phone_rips(folder, collapsed, avid, ctx)

    def _collapse_resolution_variants(self, folder: Path, videos: list[Path], ctx: RunContext) -> list[Path] | None:
        """Keep one file per cut when the same cut ships in several resolutions.

        Files whose tag-stripped names are equal are the same cut; the one
        marked high-resolution wins. A group without exactly one such marker
        cannot be decided, and None sends the folder to an operator.
        """
        groups: dict[str, list[Path]] = {}
        for video in videos:
            groups.setdefault(_variant_base(video.name), []).append(video)
        kept: list[Path] = []
        for group in groups.values():
            if len(group) == 1:
                kept.append(group[0])
                continue
            high_resolution = [video for video in group if is_4k_video(video)]
            if len(high_resolution) != 1:
                return None
            winner = high_resolution[0]
            dropped = ', '.join(video.name for video in group if video is not winner)
            ctx.info('keeping the high-resolution %s in %s and dropping %s', winner.name, folder.name, dropped)
            kept.append(winner)
        return sorted(kept, key=lambda video: video.name)

    def _drop_phone_rips(self, folder: Path, videos: list[Path], avid: str, ctx: RunContext) -> list[Path] | None:
        """Keep only the main video when every extra is a ``-5`` phone rip.

        A companion qualifies only when its tag-stripped name is the AVID plus
        ``-5`` and it is at most half the main file's size; anything else means
        the folder is not understood and nothing is touched.
        """
        dominant = max(videos, key=lambda video: video.stat().st_size)
        limit = dominant.stat().st_size * PHONE_RIP_MAX_SHARE
        rips = [video for video in videos if video is not dominant]
        for rip in rips:
            match = _PHONE_RIP_RE.match(_variant_base(rip.name))
            if (
                match is None
                or not _avids_are_padding_equivalent(self.avid_of(match['base']), avid)
                or rip.stat().st_size > limit
            ):
                return None
        dropped = ', '.join(rip.name for rip in rips)
        ctx.info('keeping %s in %s and dropping the phone rip %s', dominant.name, folder.name, dropped)
        return [dominant]

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
        # The selection's order is the cd order; an unnumbered first part would
        # sort after its "-2" sibling by name.
        for index, video in enumerate(selection.videos):
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

    def route_for_task_dir(self, task_dir_path: str) -> tuple[str, bool] | None:
        """The (dst_subdir, priority) of the route rooted at this offline directory.

        Offline directories are configured as CloudDrive API paths while routes
        are rooted on the local mount of the same tree, so the two are matched
        by path suffix: the mount exposes the cloud tree unchanged, which makes
        the API path a suffix of exactly the route root it feeds. None when no
        route root matches, e.g. before the operator has added the route.
        """
        wanted = PurePosixPath(task_dir_path).parts
        if wanted and wanted[0] == '/':
            wanted = wanted[1:]
        if not wanted:
            return None
        for table, priority in ((self._config.priority_mapping, True), (self._config.mapping, False)):
            for source, dst in table.items():
                parts = (self.src_dir / source).parts
                if len(parts) >= len(wanted) and parts[len(parts) - len(wanted) :] == wanted:
                    return dst, priority
        return None

    def library_holdings(self, avid: str, ctx: RunContext, *, task_dir_path: str | None = None) -> tuple[str, ...]:
        """Library paths already holding this AVID, () when the library has none.

        The library keeps one copy of an AVID across every route destination,
        so a caller about to queue a download asks here first. When the caller's
        directory feeds a priority route, copies sitting under normal
        destinations are moved into it — the promotion archiving itself would
        perform — so the returned paths are where the library keeps this AVID
        from now on.
        """
        brand = get_brand(avid)
        if not brand:
            return ()
        route = self.route_for_task_dir(task_dir_path) if task_dir_path is not None else None
        if route is not None and route[1] and not self._brand_routed(brand):
            target_dir = self.find_dst_dir(avid, self.dst_dir / route[0], ctx)
            if target_dir is not None:
                self._promote_from_normal(avid, brand, target_dir, ctx)
        found: list[str] = []
        for dst in dict.fromkeys((*self._config.priority_mapping.values(), *self._config.mapping.values())):
            brand_dir = self.find_dst_dir(avid, self.dst_dir / dst, ctx)
            if brand_dir is None:
                continue
            for archived in self._matching_archived(avid, brand_dir):
                path = str(_display(archived, self.dst_dir))
                if path not in found:
                    found.append(path)
        return tuple(found)

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

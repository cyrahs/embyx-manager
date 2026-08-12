"""Archive pipeline: normalize intake naming and move videos into the library.

Ported from embyx-monitor's archive.py: for each configured mapping the run
does clear_dirname -> flatten -> rename -> archive. Filesystem work is
synchronous by nature; the scheduler executes ``run`` in a worker thread and
cancellation is checked between directory entries.
"""

import re
import shutil
import time
from pathlib import Path

from embyx_manager.config.models import ArchiveConfig
from embyx_manager.core.avid import AvidParser, get_brand
from embyx_manager.core.media import has_video_suffix, is_video
from embyx_manager.monitor.reports import RunContext

MAX_RENAME_ATTEMPTS = 5
COPY_SUFFIX_RE = re.compile(r'\s*\(\d+\)$')
POST_MUTATION_SLEEP_SECONDS = 5


def remove_00(avid: str) -> str:
    match = re.match(r'[A-Z0-9]+-00\d{3,4}', avid)
    if match:
        return re.sub('00', '', avid, count=1)
    return avid


def multi_part_video_check(videos: list[Path]) -> bool:
    if len(videos) == 1:
        msg = 'only one video file'
        raise ValueError(msg)
    # check videos only have different digits
    non_digit_parts = {re.sub(r'\d+', '', video.name) for video in videos}
    if len(non_digit_parts) == 1:
        return True
    # check videos like xxx-A.mp4 xxx-B.mp4
    non_index_parts = {re.sub(r'-[A-Z]', '', video.name) for video in videos}
    return len(non_index_parts) == 1


def is_4k_video(video: Path) -> bool:
    stem = video.stem.lower()
    if not stem.endswith('4k'):
        return False
    if stem == '4k':
        return False
    return not stem[-3].isalnum()


def normalize_copy_suffix(stem: str) -> str:
    return COPY_SUFFIX_RE.sub('', stem)


class ArchivePipeline:
    def __init__(self, *, config: ArchiveConfig, avid_parser: AvidParser) -> None:
        self._config = config
        self._avid = avid_parser
        self.src_dir = Path(config.src_dir)
        self.dst_dir = Path(config.dst_dir)

    def run(self, ctx: RunContext) -> None:
        for src, dst in self._config.mapping.items():
            ctx.check_cancelled()
            src_path = self.src_dir / src
            dst_path = self.dst_dir / dst
            ctx.info('processing %s -> %s', src_path, dst_path)
            self.clear_dirname(src_path, ctx)
            self.flatten(src_path, dst_path, ctx)
            self.rename(src_path, ctx)
            self.archive(src_path, dst_path, ctx)

    # -- stages -------------------------------------------------------------

    def clear_dirname(self, root: Path, ctx: RunContext) -> None:
        """Rewrite folder names ending in a video suffix to avoid suffix confusion."""
        for folder in root.iterdir():
            ctx.check_cancelled()
            if not folder.is_dir():
                continue
            if has_video_suffix(folder):
                new_name = folder.stem + folder.suffix.replace('.', '-')
                ctx.info('clearing dirname for %s -> %s', folder.name, new_name)
                new_path = root / new_name
                counter = 1
                while new_path.exists():
                    new_name_with_counter = new_name + f'-{counter}'
                    new_path = root / new_name_with_counter
                    counter += 1
                    if counter > MAX_RENAME_ATTEMPTS:
                        break
                if new_path.exists():
                    ctx.warning('failed to clear dirname for %s, skipping', folder.name)
                    continue
                folder.rename(new_path)
                ctx.add('dirnames_cleared')

    def flatten(self, root: Path, dst_dir: Path, ctx: RunContext) -> None:  # noqa: C901, PLR0912, PLR0915
        """Move qualifying videos from intake subfolders into the intake root."""
        flattened_any = False
        if not root.is_dir():
            msg = f'{root} is not a directory'
            raise ValueError(msg)
        exist_avids = {self._avid.get_avid(f.name) for f in root.iterdir() if is_video(f)}
        min_size_bytes = self._config.min_size_mb * 1024 * 1024
        for folder in root.iterdir():
            ctx.check_cancelled()
            if not folder.is_dir():
                continue
            videos = [f for f in folder.iterdir() if is_video(f) and f.stat().st_size > min_size_bytes]
            if len(videos) == 0:
                folder_avid = self._avid.get_avid(folder.name)
                ctx.info('%s has no video files larger than %dMB', folder.name, self._config.min_size_mb)
                if not folder_avid:
                    continue
                folder_avid = remove_00(folder_avid)
                folder_avid_dst_dir = self.find_dst_dir(folder_avid, dst_dir, ctx)
                if folder_avid_dst_dir is None or not folder_avid_dst_dir.exists():
                    continue
                for f in folder_avid_dst_dir.iterdir():
                    if not is_video(f) or not f.name.startswith(folder_avid):
                        continue
                    # Intentional cleanup: folders without a qualifying video are
                    # treated as disposable leftovers, usually ads or junk files.
                    ctx.warning('%s has no video and %s exists (%s), deleting', folder.name, folder_avid, f)
                    shutil.rmtree(folder)
                    ctx.add('junk_folders_deleted')
                    break
                continue
            videos = self._drop_duplicate_copies(folder, videos, ctx)
            avids = [self._avid.get_avid(t.name) for t in videos]
            if len(set(avids)) != 1:
                ctx.warning(
                    'multiple avid result: %s found in %s in %s, skipping',
                    ', '.join(avids),
                    folder.name,
                    ', '.join([t.name for t in videos]),
                )
                continue
            avid = avids[0]
            if len(videos) > 1 and not multi_part_video_check(videos):
                four_k = [v for v in videos if is_4k_video(v)]
                if len(four_k) == 1:
                    kept = four_k[0]
                    kept_avid = self._avid.get_avid(kept.name)
                    if kept_avid != avid:
                        ctx.warning('4k video avid mismatch in %s, skipping', folder.name)
                        continue
                    dropped = [v.name for v in videos if v != kept]
                    ctx.info(
                        '4k variant found in %s, keeping %s and dropping %s',
                        folder.name,
                        kept.name,
                        ', '.join(dropped),
                    )
                    videos = [kept]
                else:
                    ctx.warning('multiple videos found, but seems not multi-part video, skipping %s', folder.name)
                    continue
            if not avid:
                ctx.warning('failed to get avid for %s, skipping folder', ', '.join([t.name for t in videos]))
                continue
            avid = remove_00(avid)
            if avid in exist_avids:
                ctx.warning('%s exists in %s, skipping', avid, root)
                continue
            ctx.info('flattening %s', folder.name)
            exist_avids.add(avid)
            for v in videos:
                dst = root / v.name
                if dst.exists():
                    msg = f'{dst} exists'
                    raise FileExistsError(msg)
                v.rename(dst)
            shutil.rmtree(folder)
            ctx.add('folders_flattened')
            flattened_any = True
        if flattened_any:
            self._settle(ctx, 'flattening')

    def rename(self, root: Path, ctx: RunContext) -> None:
        """Rename videos to AVID.ext (or AVID-cdN.ext for multi-part sets)."""
        renamed_any = False
        if not root.is_dir():
            msg = f'{root} is not a directory'
            raise ValueError(msg)
        avids: dict[str, set[Path]] = {}
        for video in root.iterdir():
            if not is_video(video):
                continue
            avid = remove_00(self._avid.get_avid(video.name))
            avids.setdefault(avid, set()).add(video)
        for avid, videos_set in avids.items():
            ctx.check_cancelled()
            videos = sorted(videos_set, key=lambda x: x.name)
            planned_renames = self._build_rename_plan(root, avid, videos, ctx)
            _check_rename_targets(planned_renames)
            for video, target, new_name in planned_renames:
                ctx.info('%s -> %s', video.relative_to(root), new_name)
                video.rename(target)
                ctx.add('videos_renamed')
                renamed_any = True
        if renamed_any:
            self._settle(ctx, 'renaming')

    def archive(self, src_dir: Path, dst_dir: Path, ctx: RunContext) -> None:
        """Move renamed videos into their per-brand destination directories."""
        if not src_dir.is_dir():
            msg = f'{src_dir} is not a directory'
            raise ValueError(msg)
        if not dst_dir.is_dir():
            msg = f'{dst_dir} is not a directory'
            raise ValueError(msg)
        for video in src_dir.iterdir():
            ctx.check_cancelled()
            if not (dst := self.find_video_dst(video, dst_dir, ctx)):
                continue
            if not dst.parent.exists():
                dst.parent.mkdir(parents=True)
            if dst.exists():
                ctx.warning('%s exists, skipping', _display(dst, self.dst_dir))
                continue
            ctx.info('moving %s to %s', video.relative_to(src_dir), _display(dst, self.dst_dir))
            video.rename(dst)
            ctx.add('videos_archived')

    # -- helpers --------------------------------------------------------------

    def find_dst_dir(self, avid: str, dst_dir: Path, ctx: RunContext) -> Path | None:
        brand = get_brand(avid)
        if not brand:
            ctx.warning('failed to get brand for %s, skipping find_dst', avid)
            return None
        for brand_dst, brand_avids in self._config.brand_mapping.items():
            if brand in brand_avids:
                return self.dst_dir / brand_dst / brand
        return dst_dir / brand

    def find_video_dst(self, video: Path, dst_dir: Path, ctx: RunContext) -> Path | None:
        if not is_video(video):
            return None
        if not (avid := self._avid.get_avid(video.name)):
            ctx.warning('failed to get avid for %s, skipping find_video_dst', _display(video, self.src_dir))
            return None
        video_dst_dir = self.find_dst_dir(avid, dst_dir, ctx)
        if video_dst_dir is None:
            return None
        return video_dst_dir / video.name

    def _drop_duplicate_copies(self, folder: Path, videos: list[Path], ctx: RunContext) -> list[Path]:
        base_by_key: dict[tuple[str, str, int], Path] = {}
        copies_by_key: dict[tuple[str, str, int], list[Path]] = {}
        for video in videos:
            size = video.stat().st_size
            normalized_stem = normalize_copy_suffix(video.stem)
            key = (normalized_stem, video.suffix.lower(), size)
            if video.stem == normalized_stem:
                base_by_key.setdefault(key, video)
            else:
                copies_by_key.setdefault(key, []).append(video)
        dropped: list[Path] = []
        for key, copies in copies_by_key.items():
            base = base_by_key.get(key)
            if base is None:
                continue
            dropped.extend(copies)
            ctx.info(
                'duplicate videos found in %s, keeping %s and dropping %s',
                folder.name,
                base.name,
                ', '.join(copy.name for copy in copies),
            )
            for copy in copies:
                try:
                    copy.unlink()
                    ctx.add('duplicate_copies_deleted')
                except OSError:
                    ctx.exception('failed to remove duplicate video %s in %s', copy.name, folder.name)
        if not dropped:
            return videos
        dropped_set = set(dropped)
        return [video for video in videos if video not in dropped_set]

    def _build_rename_plan(
        self,
        root: Path,
        avid: str,
        videos: list[Path],
        ctx: RunContext,
    ) -> list[tuple[Path, Path, str]]:
        planned_renames: list[tuple[Path, Path, str]] = []
        for i, video in enumerate(videos):
            suffix = f'-cd{i + 1}{video.suffix}' if len(videos) > 1 else video.suffix
            new_name = f'{avid}{suffix}'
            if video.name == new_name:
                ctx.info('no change for %s, skipping', video.relative_to(root))
                continue
            planned_renames.append((video, root / new_name, new_name))
        return planned_renames

    def _settle(self, ctx: RunContext, stage: str) -> None:
        """Give the CloudDrive mount a moment to observe the mutations."""
        if ctx.stop_requested:
            return
        ctx.info('Sleeping %d seconds after %s', POST_MUTATION_SLEEP_SECONDS, stage)
        time.sleep(POST_MUTATION_SLEEP_SECONDS)


def _check_rename_targets(planned_renames: list[tuple[Path, Path, str]]) -> None:
    planned_targets: set[Path] = set()
    for _, target, _ in planned_renames:
        if target in planned_targets:
            msg = f'{target} planned more than once'
            raise FileExistsError(msg)
        if target.exists():
            msg = f'{target} exists'
            raise FileExistsError(msg)
        planned_targets.add(target)


def _display(path: Path, base: Path) -> Path:
    try:
        return path.relative_to(base)
    except ValueError:
        return path

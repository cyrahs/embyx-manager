from pathlib import Path

VIDEO_SUFFIXES = frozenset({'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.flv', '.m4v', '.ts', '.rmvb'})


def has_video_suffix(path: Path) -> bool:
    return path.suffix.lower() in VIDEO_SUFFIXES


def is_video(path: Path) -> bool:
    if not path.is_file():
        return False
    return has_video_suffix(path)

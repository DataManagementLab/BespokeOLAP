import hashlib
import json
import logging
import os
import pickle
from pathlib import Path
from typing import Any, TypeVar

logger = logging.getLogger(__name__)


def ask_yes_no(prompt: str, default: bool | None = None) -> bool:
    """
    Ask a yes/no question.

    - default=True  -> Enter means "yes"
    - default=False -> Enter means "no"
    - default=None  -> Enter not allowed, must type y/n
    """
    if default is True:
        suffix = " [Y/n] "
    elif default is False:
        suffix = " [y/N] "
    else:
        suffix = " [y/n] "

    while True:
        reply = input(prompt + suffix).strip().lower()

        if not reply:
            if default is not None:
                return default
            continue

        if reply in ("y", "yes"):
            return True
        if reply in ("n", "no"):
            return False


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


class _PathEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


def stable_json(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, cls=_PathEncoder
    )


def atomic_write(path: Path, data: bytes, mode: int = 0o777) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)  # atomic
    try:
        os.chmod(path, mode)
    except Exception:
        pass  # best effort, ignore failures


def create_parent_and_set_permissions(path: Path, mode: int = 0o777) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(path.parent, mode)
    except Exception:
        pass  # best effort, ignore failures


T = TypeVar("T")


def load_pickle(path: Path, expected: type[T]) -> T | None:
    """
    Load a pickled object from `path` and verify its type.

    Returns the object if it matches `expected`. On deserialization failure
    or type mismatch, the file is renamed with a `.corrupt` suffix and
    None is returned.
    """
    try:
        obj = pickle.loads(path.read_bytes())
        if isinstance(obj, expected):
            return obj
    except Exception as e:
        logger.exception(f"Failed to read from {path}: {e}")

    # quarantine corrupted / unexpected cache entry
    try:
        os.replace(path, path.with_suffix(path.suffix + ".corrupt"))
    except Exception:
        pass

    return None


def dump_pickle(path: Path, obj: T) -> None:
    """
    Dumps an object to a pickle file at the given path.

    Args:
        path (Path): The file path where the object will be saved.
        obj (T): The object to pickle, can be any type.
    """
    try:
        data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        atomic_write(path, data)
    except Exception as e:
        logger.exception(f"Failed to write to {path}: {e}")
        raise e

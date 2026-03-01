import json
import logging
import subprocess
from pathlib import Path

from llm_cache import utils

logger = logging.getLogger(__name__)


def calculate_loc(
    cloc_cache_dir: Path | None, current_hash: str, working_dir: Path
) -> int:
    if cloc_cache_dir is not None:
        # check if cloc is in cache
        payload = {
            "snapshot_hash": current_hash,
        }
        hash = utils.sha256(utils.stable_json(payload))
        cache_path = _cache_path_for(cloc_cache_dir, hash)

        if cache_path.exists():
            output = utils.load_pickle(cache_path, expected=int)
            assert output is not None, "Cache file exists but failed to load"
            return output
    else:
        cache_path = None

    # run cloc with json output
    cmd = "cloc . --json"

    # execute the command with subprocess and capture the output
    result = subprocess.run(
        cmd, shell=True, cwd=working_dir, capture_output=True, text=True
    )

    # check for error
    if result.returncode != 0:
        logger.error(f"Error running cloc: {result.stderr}")
        return 0

    count_stats = result.stdout.strip()
    if not count_stats:
        return 0

    loc = 0
    for file_type, stats in json.loads(result.stdout).items():
        # skip general cloc files
        if file_type in ("SUM", "header", "SUM!"):
            continue

        # skip text / json files
        if file_type in ("Text", "JSON", "Markdown"):
            continue

        # accumulate lines of code for each file type
        loc += stats.get("code", 0)

    if cache_path is not None:
        # write out to cache
        utils.dump_pickle(cache_path, loc)

    return loc


def _cache_path_for(cloc_cache_dir: Path, hash: str) -> Path:
    assert cloc_cache_dir is not None, "cloc_cache_dir must be set to use cache"
    return cloc_cache_dir / f"{hash}.pkl"

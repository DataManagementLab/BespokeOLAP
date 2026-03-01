import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from llm_cache import utils
from llm_cache.git_snapshotter import GitSnapshotter
from misc.fasttest.compiler import Compiler

logger = logging.getLogger(__name__)


class CachedCompiler(Compiler):
    def __init__(
        self,
        args: Dict,
        git_snapshotter: Optional[GitSnapshotter] = None,
        compile_cache_dir: Optional[Path] = None,
    ):
        super().__init__(**args)
        self.args = args
        self.git_snapshotter = git_snapshotter
        self.cache_dir = compile_cache_dir

        # create cache dir if needed
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            # make 777
            try:
                self.cache_dir.chmod(0o777)
            except PermissionError:
                pass

    def build(self) -> Optional[str]:
        # forward to cache function. This is only to override the build function of the parent class, which is called by FasttestProc. The actual caching logic is implemented in build_cached, which is called by this function.
        cached_result, used_cache, compile_key_hash = self.build_cached()
        return cached_result

    def build_cached(
        self,
        skip_cache: bool = False,
        current_git_snapshot: Optional[str] = None,
        only_from_cache: bool = False,
    ) -> Tuple[str | None, bool, str]:
        """
        Build with caching support. Returns if the result was from cache.
        This is going beyond the original def build() by returning a tuple
        of (output, from_cache).
        """

        is_cached, cached_result, cache_path, compile_key_hash = (
            self._check_answer_from_cache(current_git_snapshot)
        )
        if is_cached and not skip_cache:
            return cached_result, True, compile_key_hash

        if only_from_cache:
            raise Exception(
                f"Result not found in cache for key {compile_key_hash} and only_from_cache is set. Cache path: {cache_path}"
            )

        # call normal build
        output = super().build()

        # store output in cache
        if cache_path is not None:
            utils.dump_pickle(
                cache_path,
                CompileCacheType(outputs=output),
            )
            logger.debug(f"Saved compile result to cache: {cache_path}")

        return output, False, compile_key_hash

    def _check_answer_from_cache(
        self, current_git_snapshot: Optional[str] = None
    ) -> Tuple[bool, Optional[str], Optional[Path], str]:
        if self.git_snapshotter is None and current_git_snapshot is None:
            logger.warning(
                "Can't determine current code version (GitSnapshotter is None); "
                "skipping compile cache lookup."
            )
            return False, None, None, ""

        # fetch git hash
        if current_git_snapshot is not None:
            assert self.git_snapshotter is None, (
                "Cannot provide current_git_snapshot if git_snapshotter is set"
            )
            git_hash = current_git_snapshot
        else:
            assert self.git_snapshotter is not None, (
                "git_snapshotter must be set to fetch git hash"
            )
            git_hash = self.git_snapshotter.current_hash

        if self.cache_dir is None:
            logger.info(
                "Cache directory not configured; skipping compile cache lookup."
            )
            return False, None, None, ""

        hash_payload = dict(self.args)
        hash_payload.pop("working_dir", None)
        hash_payload.update(
            {
                "snapshotter_hash": git_hash,
                "cxx_flags": self.extra_cxxflags,
            }
        )
        compile_key_hash = utils.sha256(utils.stable_json(hash_payload))
        cache_path = _cache_path_for_hash(self.cache_dir, compile_key_hash)

        if not cache_path.exists():
            logger.info(f"No matching compile cache found at {cache_path=}")
            return False, None, cache_path, compile_key_hash

        cached: Optional[CompileCacheType] = utils.load_pickle(
            cache_path, CompileCacheType
        )
        assert cached is not None
        logger.debug(f"Loaded compile result from cache: {cache_path}")
        return True, cached.outputs, cache_path, compile_key_hash


def _cache_path_for_hash(cache_dir: Path, hash: str) -> Path:
    return cache_dir / f"{hash}.pkl"


class CompileCacheType:
    def __init__(self, outputs: Optional[str]):  # , parent_hash: Optional[str] = None):
        self.outputs = outputs
        # self.parent_hash = parent_hash

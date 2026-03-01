import functools
import logging
from pathlib import Path
from typing import Optional

from llm_cache import utils

logger = logging.getLogger(__name__)

CEB_DIR = Path("/mnt/labstore/bespoke_olap/datasets/ceb/imdb")


def get_query_gen(benchmark: str):
    # prepare query gen
    if benchmark == "tpch":
        from dataset.gen_tpch.gen_tpch_query import gen_query

        gen_query_fn = gen_query
    elif benchmark == "ceb":
        from dataset.gen_ceb.gen_ceb_query import gen_query_single_only

        gen_query_fn = functools.partial(gen_query_single_only, ceb_dir=CEB_DIR)
    else:
        raise ValueError(f"Unknown benchmark: {benchmark}")

    return gen_query_fn


def get_placeholders_fn(benchmark: str, cache_dir: Optional[Path] = None):
    # prepare query gen
    gen_fn = None
    if benchmark == "tpch":
        from dataset.gen_tpch.gen_tpch_query import gen_query

        def gen_placeholder_tpch(**kwargs):
            # we only need the placeholders dict
            return gen_query(**kwargs)[2]

        gen_fn = gen_placeholder_tpch

    elif benchmark == "ceb":
        from dataset.gen_ceb.gen_ceb_query import gen_query_single_only

        # load placeholders from disk

        def gen_placeholder_ceb(**kwargs):
            # check cache first
            hash_payload = {
                "benchmark": "ceb",
                "query_name": kwargs["query_name"],
            }

            hash = utils.sha256(utils.stable_json(hash_payload))

            if cache_dir is None:
                cache_path = None
            else:
                # create cache dir if needed
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache_path = _cache_path_for_hash(cache_dir, hash)

            # check compile cache - replay compile result from cache if available
            if cache_path is not None and cache_path.exists():
                cached: Optional[PlaceholdersCacheType] = utils.load_pickle(
                    cache_path, PlaceholdersCacheType
                )
                assert cached is not None
                logger.debug(f"Loaded placeholders from cache: {cache_path}")

                return cached.placeholders

            # we only need the placeholders dict
            placeholders = gen_query_single_only(**kwargs, ceb_dir=CEB_DIR)[2]

            # store output in cache
            if cache_path is not None:
                utils.dump_pickle(
                    cache_path,
                    PlaceholdersCacheType(placeholders=placeholders),
                )

            return placeholders

        gen_fn = gen_placeholder_ceb

    else:
        raise ValueError(f"Unknown benchmark: {benchmark}")

    return gen_fn


def _cache_path_for_hash(cache_dir: Path, hash: str) -> Path:
    return cache_dir / f"{hash}.pkl"


class PlaceholdersCacheType:
    def __init__(self, placeholders: dict):
        self.placeholders = placeholders

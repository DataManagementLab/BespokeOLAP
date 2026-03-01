import logging
import pickle
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

from llm_cache import utils
from tools.validate_tool.duckdb_connection_manager import DuckDBConnectionManager
from utils.sql_utils import extract_order_by_columns

logger = logging.getLogger(__name__)


@dataclass
class QueryInstantiation:
    """Represents a single query instantiation with its metadata."""

    query_id: str
    sql: str
    placeholders: Dict[str, str]
    order_by_info: List[Tuple[str, str]]
    duckdb_result: pd.DataFrame
    duckdb_exec_time_ms: float
    duckdb_plan: Dict


class QueryCache:
    """
    Pre-generates query instantiations and caches DuckDB results.
    """

    def __init__(
        self,
        gen_query_fn: Callable,
        query_ids: List[str],
        sf_list: List[float],
        num_instantiations_per_query: int,
        duckdb_managers: Optional[Dict[float, DuckDBConnectionManager]],
        cache_dir: Path,
    ):
        """
        Initialize the query cache.

        Parameters
        ----------
        query_ids : List[str]
            List of query IDs to pre-generate
        sf_list : List[int]
            List of scale factors to pre-generate for
        num_instantiations_per_query : int
            Number of instantiations to generate per query
        duckdb_managers : Dict[int, DuckDBConnectionManager]
            DuckDB connection managers keyed by scale factor
        cache_dir : Path
            Directory to store cache files (default: "cache")
        """
        self.gen_query_fn = gen_query_fn
        self.query_ids = query_ids
        self.sf_list = sf_list
        self.num_instantiations_per_query = num_instantiations_per_query
        self.duckdb_managers = duckdb_managers
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Cache structure: {scale_factor: {query_id: [QueryInstantiation, ...]}}
        self.cache: Dict[float, Dict[str, List[QueryInstantiation]]] = {}

        # Pre-generate all query instantiations
        self._pregenerate_queries()

    def _pregenerate_queries(self):
        """Pre-generate all query instantiations and cache DuckDB results."""
        logger.info("Pre-generating query instantiations and executing with DuckDB...")

        rnd = random.Random(42)

        for sf in self.sf_list:
            self.cache[sf] = {}

            if self.duckdb_managers is not None:
                if sf not in self.duckdb_managers:
                    logger.warning(f"No DuckDB manager found for SF{sf}, skipping")
                    continue

                duckdb_con = self.duckdb_managers[sf]
            else:
                duckdb_con = None

            # keep for first two sf positions the same number of instantiations, but for larger sfs only generate 1 instantiation to save time and disk space
            max_pos = 2 if len(self.sf_list) > 2 else len(self.sf_list) - 1

            if (
                sf >= sorted(self.sf_list)[max_pos]
            ):  # Only generate 1 instantiation for the largest SF
                num_instantiations = 1
            else:
                num_instantiations = self.num_instantiations_per_query

            for query_id in tqdm(
                self.query_ids,
                desc=f"Gen and exec {num_instantiations} queries for SF{sf}",
            ):
                query_id_str = str(query_id)

                # Try to load from cache first
                cached_instantiations = self._load_from_disk(
                    sf, query_id_str, num_instantiations
                )
                if cached_instantiations is not None:
                    self.cache[sf][query_id_str] = cached_instantiations
                    continue

                # only cached versions work without duckdb managers
                assert duckdb_con is not None, "DuckDB managers must be provided"

                self.cache[sf][query_id_str] = []

                instantiations_generated = 0
                seen_sqls = set()

                # Generate unique query instantiations
                max_attempts = num_instantiations * 10
                attempts = 0

                while (
                    instantiations_generated < num_instantiations
                    and attempts < max_attempts
                ):
                    attempts += 1

                    # Generate a random instantiation
                    template, sql, placeholders = self.gen_query_fn(
                        query_name=f"Q{query_id_str}", rnd=rnd
                    )

                    # Skip duplicates
                    if sql in seen_sqls:
                        continue

                    seen_sqls.add(sql)

                    # Extract order by information
                    order_by_info = extract_order_by_columns(sql)

                    # Execute with DuckDB and cache result
                    try:
                        duckdb_time, duckdb_df, duckdb_plan = duckdb_con.duckdb_sql(sql)

                        # Create instantiation object
                        instantiation = QueryInstantiation(
                            query_id=query_id_str,
                            sql=sql,
                            placeholders=placeholders,
                            order_by_info=order_by_info,
                            duckdb_result=duckdb_df.copy(),
                            duckdb_exec_time_ms=duckdb_time,
                            duckdb_plan=duckdb_plan,
                        )

                        self.cache[sf][query_id_str].append(instantiation)
                        instantiations_generated += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to execute Q{query_id_str} with DuckDB: {e}\n{sql}"
                        )
                        continue

                # Save to disk after generating all instantiations for this query
                if self.cache[sf][query_id_str]:
                    self._save_to_disk(sf, query_id_str, num_instantiations)

        logger.info("Query pre-generation complete")

    def _get_cache_filepath(
        self, sf: float, query_id_str: str, num_instantiations: int
    ) -> Path:
        """Get the cache file path for a specific query configuration."""
        hash_payload = {
            "sf": sf,
            "query_id": query_id_str,
            "num_instantiations": num_instantiations,
        }
        hash = utils.sha256(utils.stable_json(hash_payload))
        filename = f"{hash}.pkl"
        return self.cache_dir / filename

    def _save_to_disk(self, sf: float, query_id_str: str, num_instantiations: int):
        """Save query instantiations to disk."""
        filepath = self._get_cache_filepath(sf, query_id_str, num_instantiations)
        try:
            instantiations = self.cache[sf][query_id_str]
            with open(filepath, "wb") as f:
                pickle.dump(instantiations, f)
            # logger.debug(f"Saved {len(instantiations)} instantiations to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save cache to {filepath}: {e}")

    def _load_from_disk(
        self, sf: float, query_id_str: str, num_instantiations: int
    ) -> List[QueryInstantiation] | None:
        """Load query instantiations from disk if available."""
        filepath = self._get_cache_filepath(sf, query_id_str, num_instantiations)
        if not filepath.exists():
            return None

        try:
            with open(filepath, "rb") as f:
                instantiations = pickle.load(f)
            return instantiations
        except Exception as e:
            logger.error(f"Failed to load cache from {filepath}: {e}")
            return None

    def get_instantiations(
        self,
        scale_factor: float,
        query_id: str | List[str] | None = None,
        num_samples: int | None = None,
    ) -> List[QueryInstantiation]:
        """
        Get query instantiations from cache.

        Parameters
        ----------
        scale_factor : float
            Scale factor to retrieve instantiations for
        query_id : str | List[str] | None
            Query ID(s) to retrieve. If None, retrieve all queries.
        num_samples : int | None
            Number of samples to retrieve per query. If None, retrieve all.

        Returns
        -------
        List[QueryInstantiation]
            List of query instantiations
        """
        if scale_factor not in self.cache:
            logger.error(f"Scale factor {scale_factor} not found in cache")
            return []

        # Determine which query IDs to retrieve
        if isinstance(query_id, list):
            query_ids_to_get = [str(qid) for qid in query_id]
        elif query_id is not None:
            query_ids_to_get = [str(query_id)]
        else:
            query_ids_to_get = list(self.cache[scale_factor].keys())

        # Collect instantiations
        instantiations = []
        rnd = random.Random(42)  # Use consistent seed for sampling

        for qid in query_ids_to_get:
            if qid not in self.cache[scale_factor]:
                logger.warning(f"Query {qid} not found in cache for SF{scale_factor}")
                continue

            available = self.cache[scale_factor][qid]

            if num_samples is None or num_samples >= len(available):
                # Return all available instantiations
                instantiations.extend(available)
            else:
                # Sample without replacement
                sampled = rnd.sample(available, num_samples)
                instantiations.extend(sampled)

        return instantiations

    def get_cache_stats(self) -> Dict:
        """Get statistics about the cache."""
        stats = {}
        for sf in self.cache:
            stats[sf] = {}
            for qid in self.cache[sf]:
                stats[sf][qid] = len(self.cache[sf][qid])
        return stats

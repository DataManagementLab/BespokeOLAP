import logging
from pathlib import Path

from tools.validate_tool.duckdb_connection_manager import DuckDBConnectionManager

logger = logging.getLogger(__name__)


class DuckDBRunner:
    name = "DuckDB"

    def __init__(
        self, parquet_path: Path, benchmark: str, pin_worker: bool = True
    ) -> None:
        self._parquet_path = parquet_path
        self._benchmark = benchmark
        self._pin_worker = pin_worker

    def run_scale_factor(
        self,
        scale_factor: float,
        query_ids_needed: set[str],
        query_list: list[str],
        sql_list: list[str],
        args_list: list[str],
        snapshot: str,
    ) -> list[float | None]:
        if not query_ids_needed:
            return [None] * len(query_list)

        logger.info("Running DuckDB timings...")
        duckdb_con = DuckDBConnectionManager(
            pre_load_duckdb_tables=True,
            parquet_path=self._parquet_path.as_posix(),
            sf=scale_factor,
            pin_worker=self._pin_worker,
            benchmark=self._benchmark,
        )

        results: list[float | None] = []
        for query_id, sql in zip(query_list, sql_list):
            if query_id in query_ids_needed:
                time_ms, _, _ = duckdb_con.duckdb_sql(sql)
                results.append(time_ms)
            else:
                results.append(None)
        return results

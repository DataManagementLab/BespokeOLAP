import json
import os
import tempfile
from typing import Dict, Optional, Tuple

import duckdb
import pandas as pd
from tqdm import tqdm

from dataset.dataset_tables_dict import get_tables_for_benchmark


class DuckDBConnectionManager:
    def __init__(
        self,
        pre_load_duckdb_tables: bool,
        parquet_path: str,
        benchmark: str,
        sf: float = 1,
        pin_worker: bool = True,
        pin_core: Optional[int] = 3,
    ):
        self.con = None
        self.pre_load_duckdb_tables = pre_load_duckdb_tables
        self.parquet_path = parquet_path
        self.sf = sf
        self.pin_worker = pin_worker
        self.pin_core = pin_core
        self.benchmark = benchmark

        if self.pin_worker:
            assert self.pin_core is not None

        if pre_load_duckdb_tables:
            self.con = self.con_duckdb(parquet_path, benchmark=benchmark, sf=sf)

    def duckdb_sql(self, sql: str) -> Tuple[float, pd.DataFrame, Dict]:
        if not self.pre_load_duckdb_tables or self.con is None:
            self.con = self.con_duckdb(
                self.parquet_path, benchmark=self.benchmark, sf=self.sf
            )
        pid = 0  # 0 = current process
        orig_affinity = {}
        if self.pin_worker:
            orig_affinity = os.sched_getaffinity(pid)
            assert self.pin_core is not None
            os.sched_setaffinity(pid, {self.pin_core})  # pin to core 3

        # execute sql and get execution time and result dataframe
        with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
            profile_output_path = tmpfile.name

            # Enable profiling and request JSON output
            self.con.execute("PRAGMA enable_profiling = 'json'")
            self.con.execute(f"PRAGMA profiling_output ='{profile_output_path}'")

            # Run query
            result_df = self.con.execute(sql).fetchdf()

            # Read and parse the profiling output
            with open(profile_output_path, "r") as f:
                profile_data = json.load(f)

            exec_time_ms = profile_data["latency"] * 1000.0  # convert to ms

        if self.pin_worker:
            os.sched_setaffinity(pid, orig_affinity)

        return exec_time_ms, result_df, profile_data

    def con_duckdb(
        self, parquet_path: str, benchmark: str, sf: float = 1
    ) -> duckdb.DuckDBPyConnection:
        # pre-load duckdb tables to warm up cache
        self.con = duckdb.connect(database=":memory:")
        for table in tqdm(
            get_tables_for_benchmark(benchmark),
            desc=f"Loading DuckDB tables for SF{sf}",
        ):
            self.con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{parquet_path}/sf{sf}/{table}.parquet')"
            )

        # disable parallelism in duckdb for more consistent benchmarking
        self.con.execute("PRAGMA threads=1;")

        return self.con

    def clear_mem_footprint(self) -> None:
        if self.con is not None:
            self.con.close()
            self.con = None

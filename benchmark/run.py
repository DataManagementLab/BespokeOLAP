import logging
import os
import random
import re
import socket
from pathlib import Path
from typing import Dict, List

from benchmark.systems import (
    SYSTEM_REGISTRY,
    BespokeRunner,
    DuckDBRunner,
    SystemRunner,
)
from benchmark.writer import BenchmarkWriter
from dataset.dataset_tables_dict import get_dataset_name
from dataset.query_gen_factory import get_query_gen
from llm_cache.git_snapshotter import GitSnapshotter
from llm_cache.logger import setup_logging
from tools.fasttest.run import RunTool
from tools.validate_tool.query_validator_class import format_args_string

logger = logging.getLogger(__name__)

def _filter_query_ids(all_ids: List[str], query_ids: str | None) -> List[str]:
    if not query_ids:
        return all_ids
    if query_ids.strip().lower() == "all":
        return all_ids

    def _normalize_query_id(raw: str) -> str:
        qid = raw.strip().lower()
        if qid.startswith("q"):
            qid = qid[1:]
        m = re.fullmatch(r"0*(\d+)([a-z]?)", qid)
        if m:
            num, suffix = m.groups()
            return f"{int(num)}{suffix}"
        return qid

    requested: list[str] = []
    for part in query_ids.split(","):
        part = part.strip()
        if not part:
            continue
        requested.append(_normalize_query_id(part))
    if not requested:
        return all_ids
    requested_set = set(requested)
    filtered = [qid for qid in all_ids if _normalize_query_id(qid) in requested_set]
    if not filtered:
        available = ", ".join(all_ids[:30])
        raise ValueError(
            f"No matching query IDs found for: {query_ids}. "
            f"Available in snapshot: {available}"
        )
    return filtered


def _parse_scale_factors(raw: str) -> List[float]:
    parts = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue

        if "." in part:
            parts.append(float(part))
        else:
            parts.append(int(part))
    if not parts:
        raise ValueError("scale_factors list is empty.")
    return parts


def _parse_systems(raw: str) -> list[str]:
    """Parse comma-separated system names, lower-cased."""
    return [s.strip().lower() for s in raw.split(",") if s.strip()]


def _build_runners(
    system_names: list[str],
    db_engine: RunTool | None,
    snapshotter: GitSnapshotter | None,
    parquet_path: Path,
    benchmark: str,
    scale_factors: list,
) -> list[SystemRunner]:
    runners: list[SystemRunner] = []
    for name in system_names:
        if name == "bespoke":
            assert db_engine is not None, "db_engine required for BespokeRunner"
            assert snapshotter is not None, "snapshotter required for BespokeRunner"
            runners.append(
                BespokeRunner(
                    db_engine=db_engine,
                    snapshotter=snapshotter,
                )
            )
        elif name == "duckdb":
            runners.append(DuckDBRunner(parquet_path=parquet_path, benchmark=benchmark))
        else:
            cls = SYSTEM_REGISTRY.get(name)
            if cls is None:
                raise ValueError(
                    f"Unknown system '{name}'. "
                    f"Available: {', '.join(SYSTEM_REGISTRY.keys())}"
                )
            runners.append(cls())
    return runners


def run_benchmark(args) -> None:
    old_umask = os.umask(0)
    try:
        try:
            from agents.tracing import set_tracing_disabled

            set_tracing_disabled(True)
        except ImportError:
            pass

        setup_logging(logging.DEBUG, logfile=None)
        system_names = _parse_systems(getattr(args, "systems", "bespoke,duckdb"))
        if not system_names:
            raise ValueError("--systems must specify at least one system.")
        use_snapshots = "bespoke" in system_names

        snapshots: list[str] = []
        if getattr(args, "snapshots", None):
            snapshots = [s.strip() for s in args.snapshots.split(",") if s.strip()]
        if use_snapshots and not snapshots:
            raise ValueError("Provide --snapshots when benchmarking the bespoke system.")

        out_path = Path("./output")
        snapshotter: GitSnapshotter | None = None
        if use_snapshots:
            snapshotter = GitSnapshotter(
                cache_repo=None
                if args.disable_repo_sync
                else "git://c01/bespoke_cache.git",
                working_dir=out_path,
                extra_gitignore=[],
            )
            snapshotter.fetch_snapshots()

        csv_writer = None
        host = socket.gethostname()
        if args.csv:
            output_path = Path(args.csv)
            logger.info(f"Appending benchmark CSV to {output_path}")
            csv_writer = BenchmarkWriter(output_path)
            csv_writer.write_header_if_needed(
                [
                    "query_id",
                    "scale_factor",
                    "benchmark",
                    "system",
                    "time_ms",
                    "hostname",
                    "snapshot",
                ]
            )

        # parse scale factors
        scale_factors = _parse_scale_factors(args.scale_factors)
        logger.info(f"Scale factors: {', '.join(map(str, scale_factors))}")
        parquet_path = (
            Path(args.artifacts_dir) / f"{get_dataset_name(args.benchmark)}_parquet"
        )
        logger.info(f"Parquet path: {parquet_path.as_posix()}")

        query_ids = get_all_query_ids(args.benchmark)

        # prepare query generator
        gen_query_fn = get_query_gen(args.benchmark)

        # Only set up Bespoke infrastructure when it's actually requested.
        db_engine: RunTool | None = None
        if use_snapshots:
            assert snapshotter is not None
            db_engine = RunTool(
                cwd=out_path,
                query_validator=None,
                dataset_name=get_dataset_name(args.benchmark),
                base_parquet_dir=args.base_parquet_dir
                + f"/{get_dataset_name(args.benchmark)}_parquet/",
                git_snapshotter=snapshotter,
            )

        query_ids = _filter_query_ids(query_ids, args.query_ids)
        run_snapshots = snapshots if use_snapshots else [""]

        runners = _build_runners(
            system_names=system_names,
            db_engine=db_engine,
            snapshotter=snapshotter,
            parquet_path=parquet_path,
            benchmark=args.benchmark,
            scale_factors=scale_factors,
        )
        logger.info(f"Benchmarking systems: {', '.join(r.name for r in runners)}")

        logger.info(f"Benchmarking queries: {','.join(map(str, query_ids))}")

        for snapshot in run_snapshots:
            if use_snapshots:
                bespoke_runner = next(
                    (r for r in runners if isinstance(r, BespokeRunner)), None
                )
                assert bespoke_runner is not None, (
                    "Bespoke runner missing despite use_snapshots=True"
                )
                bespoke_runner.restore_snapshot(snapshot)

            for scale_factor in scale_factors:
                logger.info(f"Scale factor: {scale_factor}")
                query_ids_needed = set(query_ids)

                sql_list: list[str] = []
                placeholder_list: list[dict] = []
                query_list: list[str] = []

                for repeat_idx in range(args.repeat):
                    rnd = random.Random(42 + repeat_idx)
                    for query_id in query_ids:
                        template, query, placeholders = gen_query_fn(
                            query_name=f"Q{query_id}", rnd=rnd
                        )
                        query_list.append(str(query_id))
                        placeholder_list.append(placeholders)
                        sql_list.append(query)

                args_list = format_args_string(query_list, placeholder_list)

                # Run each system and collect timings.
                timings_by_runner: Dict[str, list[float | None]] = {}
                for runner in runners:
                    timings_by_runner[runner.name] = runner.run_scale_factor(
                        scale_factor=scale_factor,
                        query_ids_needed=query_ids_needed,
                        query_list=query_list,
                        sql_list=sql_list,
                        args_list=args_list,
                        snapshot=snapshot,
                    )

                for idx, query_id in enumerate(query_list):
                    if csv_writer is not None:
                        rows_to_write = []
                        for runner in runners:
                            times = timings_by_runner[runner.name]
                            t = times[idx] if times else None
                            if t is not None:
                                rows_to_write.append(
                                    [
                                        query_id,
                                        scale_factor,
                                        args.benchmark,
                                        runner.name,
                                        t,
                                        host,
                                        snapshot,
                                    ]
                                )
                        if rows_to_write:
                            csv_writer.write_rows(rows_to_write)

        if csv_writer is not None:
            csv_writer.close()
    finally:
        os.umask(old_umask)


def get_all_query_ids(benchmark: str) -> List[str]:
    if benchmark == "tpch":
        query_ids = [str(i) for i in range(1, 23)]
    elif benchmark == "ceb":
        query_ids = [
            "1a",
            "2a",
            "2b",
            "2c",
            "3a",
            "3b",
            "4a",
            "5a",
            "6a",
            "7a",
            "8a",
            "9a",
            "9b",
            "10a",
            "11a",
            "11b",
        ]
    else:
        raise ValueError(f"Unknown benchmark: {benchmark}")

    return query_ids

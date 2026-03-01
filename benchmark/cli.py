import argparse
import sys

from utils.cli_config import add_common_args


def build_run_parser(*, add_help: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=add_help)
    parser.add_argument(
        "--snapshots",
        type=str,
        default=None,
        help="Comma-separated list of snapshot commit hashes to iterate (for bespoke).",
    )
    parser.add_argument(
        "--scale_factors",
        type=str,
        default="1",
        help="Comma-separated scale factors to benchmark.",
    )
    parser.add_argument(
        "--query_ids",
        type=str,
        default=None,
        help="Comma-separated list of query IDs to benchmark.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="How many times to run the query list for timings.",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="bench.csv",
        help="Write benchmark results to this CSV file.",
    )
    parser.add_argument(
        "--benchmark",
        type=str,
        default="tpch",
        help="Benchmark to run (e.g., tpch, ceb).",
    )
    parser.add_argument(
        "--systems",
        type=str,
        default="bespoke,duckdb",
        help="Comma-separated systems to benchmark (e.g. bespoke,duckdb).",
    )
    add_common_args(
        parser,
        include_notify=True,
        include_disable_repo_sync=True,
        include_artifacts_dir=True,
        include_base_parquet_dir=True,
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    run_args = build_run_parser().parse_args(args)
    from benchmark.run import run_benchmark

    run_benchmark(run_args)

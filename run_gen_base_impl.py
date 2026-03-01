import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from conversations.conversation import (
    COMPACTION_MARKER,
    VALIDATE_OFF,
    VALIDATE_ON,
    VALIDATE_OUTPUT_STDOUT_OFF,
)
from main import run_conv_wrapper
from tools.validate_tool.sf_list_gen import gen_sf
from utils.cli_config import add_common_args, build_run_config
from utils.gen_common import parse_query_ids
from utils.get_sample_q_args import get_sample_query_args
from utils.wandb_api_helper import wandb_retrieve_metrics_for_run

### RUN CMD
# python gen_base_code_fasttest.py --conv basef1-22v1 --with_storage_plan --benchmark tpch --notify --replay_cache --auto_u --auto_finish


def main(args):
    # extract parameters
    with_storage_plan = args.with_storage_plan
    short_name = args.conv
    benchmark = args.benchmark

    # extract queries from short name
    prefix = "basef"
    assert short_name.startswith(prefix)

    assert "wstorage" not in short_name, (
        f"Use --with_storage_plan flag instead of encoding it in the conversation name {short_name}. This is automatically added to the versioning string"
    )

    if "v" in short_name:
        query_ids = parse_query_ids(short_name, prefix, benchmark=benchmark)
        assert query_ids is not None, (
            f"Could not parse query ids from short name {short_name}"
        )

    if with_storage_plan:
        short_name += "_wstorage"

    # assemble default sf values for the selected benchmark
    verify_sf_list, max_scale_factor = gen_sf(benchmark)

    if with_storage_plan:
        if benchmark == "tpch":
            from_storage_plan_snapshot = (
                "nhpul25g"  # previous storage plan run for tpch (wandb-id)
            )
        elif benchmark == "ceb":
            from_storage_plan_snapshot = (
                "x527bk9j"  # previous storage plan run for ceb (wandb-id)
            )
        else:
            raise ValueError(f"Unknown benchmark {benchmark}")
    else:
        from_storage_plan_snapshot = None

    if from_storage_plan_snapshot is not None:
        storage_plan_snapshot = from_storage_plan_snapshot
        statistics, _ = wandb_retrieve_metrics_for_run(
            benchmark,
            storage_plan_snapshot,
        )

        storage_plan_snapshot = statistics["last_commit_hash"]  # type: ignore
    else:
        storage_plan_snapshot = None

    config = build_run_config(
        benchmark=benchmark,
        conv_name=short_name,
        conv_mode="scripted",
        query_list=",".join(map(str, query_ids)),
        notify=args.notify,
        disable_repo_sync=args.disable_repo_sync,
        max_scale_factor=max_scale_factor,
        replay_cache=args.replay_cache,
        storage_plan_snapshot=storage_plan_snapshot,
        keep_csv=True,  # keep .csv files around instead of git-ignoring them (maybe to backtrack correctness issues)
        disable_tracing=args.disable_tracing,
        disable_wandb=args.disable_wandb,  # TODO fix this, by overwriting sys.args
        auto_u=args.auto_u,
        auto_finish=args.auto_finish,
        is_bespoke_storage=with_storage_plan,
        replay=args.replay,
    )

    # get sample query args for later use in the conversation (e.g. for better prompt formatting)
    sample_query_args_dict: Dict[str, str] = get_sample_query_args(config)

    # create conversation
    create_conversation(
        short_name,
        query_ids,
        verify_sf_list=verify_sf_list,
        max_scale_factor=max_scale_factor,
        artifacts_dir=Path(config.artifacts_dir),
        conversation_dir=Path(config.artifacts_dir) / "conversations",
        benchmark=benchmark,
        read_storage_plan=from_storage_plan_snapshot is not None,
        sample_query_args_dict=sample_query_args_dict,
    )

    # run conversation
    run_conv_wrapper(config)


def create_conversation(
    short_name,
    query_ids,
    verify_sf_list: List[float],
    max_scale_factor: int,
    artifacts_dir: Path,
    benchmark: str,
    conversation_dir: Path,
    read_storage_plan: bool = False,
    sample_query_args_dict: Optional[Dict[str, str]] = None,
):
    prompt_list = []

    # assemble sf verify string
    if len(verify_sf_list) == 1:
        sf_verify_str = str(verify_sf_list[0])
    elif len(verify_sf_list) == 2:
        sf_verify_str = f"{verify_sf_list[0]} and {verify_sf_list[1]}"
    else:
        sf_verify_str = (
            ", ".join(map(str, verify_sf_list[:-1])) + f", and {verify_sf_list[-1]}"
        )

    if benchmark == "tpch":
        example_query = "Q42"
        example_query_params = "42"
    elif benchmark == "ceb":
        example_query = "Q42a"
        example_query_params = "42a"
    else:
        raise ValueError(f"Unknown benchmark {benchmark}")

    # planner
    parquet_path = artifacts_dir / f"{benchmark}_parquet" / "sf<SCALE_FACTOR>"
    queries_path = "queries.txt"

    builder_path = "`builder_impl.hpp`/`builder_impl.cpp`"
    query_impl_path = "`query_impl.cpp`"

    if read_storage_plan:
        storage_plan = "storage_plan.txt"
        storage_hint = f"The storage plan is described in the file `{storage_plan}`. It describes how to store the parquet data in-memory for optimal query execution. Use this storage plan to implement the in-memory data structure accordingly. "
    else:
        storage_hint = "The minimum should be a struct-of-arrays."
    args_path = "args_parser.hpp"

    # turn validate off at the beginning - we want to execute without checking output correctness first
    prompt_list.append(VALIDATE_OFF)
    prompt_list.append(
        f"""You are an expert database engineer and skilled programmer.
Write a specialized high-performance database engine in C++ that is optimized to only execute a predefined set of queries.
The database engine should run the SQL queries described in `{queries_path}` ({len(query_ids)} {"query" if len(query_ids) == 1 else "queries"}). 
Datatypes and operators can be hard-coded into the program to avoid interpretation overhead.

First convert the tables from ArrowTable into a custom data-structure (build) in file {builder_path}.
Store it as a Database struct object (predefined in {builder_path}). 
Use an efficient in-memory representation of the data that allows fast execution of the queries. {storage_hint}

Then execute the queries on this data structure (execution). The execution logic interface is predefined in {query_impl_path}.
The query interface on an QueryRequest list where each request is one query to be executed.
Each query is specified as: `<QUERY_NR> <QUERY_PARAMETERS>` (e.g. for query {example_query} with parameters "EUROPE" and "1995-01-01": \"{example_query_params} EUROPE 1995-01-01\").
To parse the QUERY_NR and PARAMETERS, use the header-only C++ parser defined in `{args_path}`.
The database engine should execute all queries specified in the arg list sequentially.

Write the output of each query into a separate csv file.
Name it `result<RUN_NR>.csv` where `<RUN_NR>` is the position of the query in the arg list (starting from 1).
CSV arguments: (delimiter=',', escapechar='\\', quotechar='"', header=True).

You can get the table schemas like this: `parquet-dump-schema {parquet_path.as_posix()}/lineitem.parquet`.

Create a TODO plan with steps to implement such an database engine. Include conceptual comments of the data structure/... as well. Do not start the implementation yet.
Write the steps and the query into a file where they can be later marked as done.
Do not execute the steps yet."""
    )

    # implementation
    prompt_list.append(
        f"finish all todos. Focus on the build logic to convert ArrowTable into an efficient in-memory data structure ({builder_path}). For now use stubs for the query execution logic in {query_impl_path}."
    )

    prompt_list.append(
        f"Execute and check termination without error. First call the compile tool, then check the run tool (scalefactors {sf_verify_str} and also {max_scale_factor}). If there are errors, fix the implementation accordingly."
    )

    # add time measurement
    prompt_list.append(
        'add time measurement for execution. Exclude the csv output writing from the timing. Print/Output: once after each execution: "<RUN_NR> | Execution ms: YYY".'
    )
    if benchmark == "ceb" and read_storage_plan:
        # run compaction - otherwise context blows up. Model starts to make strange mistakes.
        prompt_list.append(COMPACTION_MARKER)

    if benchmark == "ceb":
        multi_query = (
            False  # ceb is complex: multi query execution is too complex for now
        )
    else:
        multi_query = False
        # multi_query = not read_storage_plan  # multiple queries at once only if we do not have a storage plan yet. Storage plan makes things more compliated -> hence separete iterations for each query (no batching).

    stride_len = 3 if multi_query else 1

    # from now on run with validation on
    prompt_list.append(VALIDATE_ON)

    # do not print stdout in case of success. We only want stdout in case of errors to not blow up the context with too much logs.
    prompt_list.append(VALIDATE_OUTPUT_STDOUT_OFF)

    # implement at each time 3 queries - and test their correctness
    for i in range(0, len(query_ids), stride_len):
        if multi_query:
            # multi-query
            # implement queries
            if i == 0:
                prefix = "Lets start implementing the query execution logic. Implement all queries in the next steps step by step. Start with"
            else:
                prefix = "Next, continue implementing the query execution logic for"

            # pre-assemble query string for better prompt formatting
            query_nr_list = query_ids[i : min(i + stride_len, len(query_ids))]
            query_str_sgl_plural = "query" if len(query_nr_list) == 1 else "queries"

            prompt_list.append(
                f" {prefix} {query_str_sgl_plural} {','.join(map(str, query_nr_list))}. Create a separate file for the implementation of each query. Do not print file contents after you are done."
            )

            # check correctness
            prompt_list.append(
                f"Execute and check correctness of {query_str_sgl_plural} {', '.join(map(str, query_nr_list))} by using the run tool. Run with scale_factor {sf_verify_str}. Call the run tool once for these queries together. If there are errors, fix the implementation accordingly."
            )
        else:
            # single query
            # implement queries
            if i == 0:
                prefix = "Lets start implementing the query execution logic. Implement all queries in the next steps step by step. Start with"
            else:
                prefix = "Next, continue implementing the query execution logic for"

            if (
                sample_query_args_dict is not None
                and query_ids[i] in sample_query_args_dict
            ):
                sample_args_str = f" Example instantiation of the query placeholders are:\n{sample_query_args_dict[query_ids[i]]}\nNULL values might appear in IN-Lists and are represented with the string '<<NULL>>'."
            else:
                sample_args_str = ""

            prompt_list.append(
                f"{prefix} query {query_ids[i]}. Create a separate file for the implementation of this query. Do not print file contents after you are done.{sample_args_str}"
            )

            # check correctness
            prompt_list.append(
                f'Execute and check correctness by using the run tool. Run with query_id "{query_ids[i]}" and scale_factor {sf_verify_str}. If there are errors, fix the implementation accordingly.'
            )

        prompt_list.append(COMPACTION_MARKER)

    # check correctness
    prompt_list.append(
        f"Check correctness of the output of all queries by using the run tool. Run with scale_factor {sf_verify_str}. Call the run tool once for all queries together. If there are errors, fix the implementation accordingly."
    )

    # benchmark
    prompt_list.append(
        f"Call the run tool with scale_factor {max_scale_factor}. Benchmark the execution time of all queries. Fix any error if occurs."
    )

    # optimize build
    prompt_list.append(
        f"Optimize the build implementation. You should reduce build time to below 10 seconds for scale factor {max_scale_factor}. Use multithreading, and make build as fast as duckdb. Run the implementation with scale_factor {sf_verify_str} to check for correctness and measure speedup build time with scale_factor {max_scale_factor}."
    )

    target_path = conversation_dir / f"{benchmark}_{short_name}.json"

    if os.path.exists(target_path):
        raise ValueError(f"Conversation file {target_path} already exists.")

    with open(target_path, "w") as f:
        json.dump(prompt_list, f, indent=2)


def build_parser(*, add_help: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=add_help)
    parser.add_argument(
        "--conv",
        type=str,
        required=True,
        help="Short name for the conversation",
    )
    parser.add_argument(
        "--with_storage_plan",
        action="store_true",
        default=False,
        help="Whether to read the storage plan from a previous run",
    )

    add_common_args(
        parser,
        include_notify=True,
        include_disable_repo_sync=True,
        include_replay_cache=True,
        include_benchmark=True,
        include_disable_wandb=True,
        include_disable_tracing=True,
        include_auto_u=True,
        include_auto_finish=True,
        include_replay=True,
        include_only_from_llm_cache=True,
    )
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)

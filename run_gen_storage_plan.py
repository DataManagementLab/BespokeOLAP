import argparse
import json
import os
import sys
from pathlib import Path

from main import run_conv_wrapper

# add parent to path
sys.path.append(os.path.join(os.getcwd(), ".."))
from dataset.dataset_tables_dict import get_benchmark_schema
from utils.cli_config import add_common_args, build_run_config
from utils.gen_common import parse_query_ids


def main(args):

    # ===== CONFIGURATION =====
    short_name = args.conv
    benchmark = args.benchmark

    # extract queries from short name
    prefix = "storageplan"
    assert short_name.startswith(prefix)  # b-ase
    if "v" in short_name:
        query_ids = parse_query_ids(short_name, prefix, benchmark=benchmark)
        assert query_ids is not None, f"Failed to parse query ids from {short_name}"

    max_scale_factor = 20
    # =========================

    config = build_run_config(
        benchmark=benchmark,
        conv_name=short_name,
        query_list=",".join(map(str, query_ids)),
        notify=args.notify,
        conv_mode="scripted",
        disable_repo_sync=args.disable_repo_sync,
        max_scale_factor=max_scale_factor,
        replay_cache=args.replay_cache,
        auto_u=args.auto_u,
        auto_finish=args.auto_finish,
    )

    # create conversation
    create_conversation(
        benchmark,
        short_name,
        schema=get_benchmark_schema(benchmark),
        conversation_dir=Path(config.artifacts_dir) / "conversations",
    )

    # run conversation
    run_conv_wrapper(config)


def create_conversation(
    benchmark,
    short_name,
    schema: str,
    conversation_dir: Path,
):
    prompt_list = []

    # parquet engine
    queries_path = "queries.txt"
    prompt_list.append(
        f"""Your task is to analyze the workload and produce a creative in-memory storage-layout summary for the tables accessed by the query. You have the flexibility to return detailed, free-form text that explores not only conventional storage-layout recommendations but also unconventional, novel, and even 'crazy' storage designs. 
You are encouraged to include additional ideas, new partitioning strategies, speculative encoding techniques, or experimental ways of grouping and organizing columns or data. 
For each accessed table, feel free to be inventive and elaborate on possibilities such as hybrid layouts, speculative SoA/AoS (Array of Structures/Structure of Arrays) approaches, novel column encodings, or adaptive partitioning.
Use this as an opportunity to push beyond current norms and propose storage techniques that might be futuristic or outlandish. 
Output the storage layout for each table. Output only the final storage layout.

Important:
- store all the data, and store them in a way that it could be flattened back to the original data
- do not store data redundantly, but you can use compression or encoding, meta data, or special datastructures
- optimized for in-memory (single-node) analytical query processing
    
The queries are listed in the file: {queries_path}.
The schema is:
{schema}

Based on the given queries and schema, provide a detailed and creative storage layout summary for the tables accessed by the query. Feel free to explore unconventional and novel storage designs, including speculative encoding techniques or experimental ways of organizing data. Write it to the file: `storage_plan.txt`."""
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

    add_common_args(
        parser,
        include_notify=True,
        include_disable_repo_sync=True,
        include_replay_cache=True,
        include_benchmark=True,
        include_auto_u=True,
        include_auto_finish=True,
    )
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)

import argparse

from main import run_conv_wrapper
from tools.validate_tool.sf_list_gen import gen_sf
from utils.cli_config import add_common_args, build_run_config
from utils.gen_common import parse_query_ids
from utils.wandb_api_helper import wandb_retrieve_metrics_for_run

### RUN CMD
# python run_optim_loop.py --conv brunoptim1-22v1 --bespoke_storage --benchmark tpch --notify --replay_cache --auto_u --auto_finish


def main(args):
    # extract parameters
    bespoke_storage = args.bespoke_storage
    short_name = args.conv
    benchmark = args.benchmark

    # extract queries from short name
    prefix = "runoptim"
    assert short_name.startswith(prefix)

    assert "wstorage" not in short_name, (
        f"Use --bespoke_storage flag instead of encoding it in the conversation name {short_name}. This is automatically added to the versioning string"
    )

    if "v" in short_name:
        query_ids = parse_query_ids(short_name, prefix, benchmark=benchmark)
        assert query_ids is not None, (
            f"Could not parse query ids from short name {short_name}"
        )

    if bespoke_storage:
        short_name += "_wstorage"

    # assemble default sf values for the selected benchmark
    verify_sf_list, max_scale_factor = gen_sf(benchmark)

    if benchmark == "tpch":
        if bespoke_storage:
            wandb_id = "a2tlnfrk"
        else:
            wandb_id = "ijvzlkif"
    elif benchmark == "ceb":
        if bespoke_storage:
            wandb_id = "blqeh6i0"
        else:
            wandb_id = "fx7rshq2"
    else:
        raise ValueError(f"Unknown benchmark {benchmark}")

    # lookup git snapshot from wandb
    statistics, _ = wandb_retrieve_metrics_for_run(
        benchmark, wandb_id, fetch_latest_runtimes=False
    )
    commit_hash = statistics["last_commit_hash"]

    config = build_run_config(
        benchmark=benchmark,
        conv_name=short_name,
        conv_mode="optimization",  # delegate the optimization loop logic to the conversation instead of hardcoding it in the main function
        query_list=",".join(map(str, query_ids)),
        notify=args.notify,
        disable_repo_sync=args.disable_repo_sync,
        max_scale_factor=max_scale_factor,
        replay_cache=args.replay_cache,
        start_snapshot=commit_hash,
        storage_plan_snapshot=None,
        keep_csv=True,  # keep .csv files around instead of git-ignoring them (maybe to backtrack correctness issues)
        disable_tracing=args.disable_tracing,
        disable_wandb=args.disable_wandb,
        auto_u=args.auto_u,
        auto_finish=args.auto_finish,
        is_bespoke_storage=bespoke_storage,
        run_tool_offer_trace_option=True,  # for optimization conversations, we want to offer the option to run with tracing compile flag enabled to collect more fine-grained performance data for the optimized plans
        only_from_llm_cache=args.only_from_llm_cache,
        only_from_cache=args.only_from_cache,
    )

    # run conversation
    run_conv_wrapper(config)


def build_parser(*, add_help: bool = True) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=add_help)
    parser.add_argument(
        "--conv",
        type=str,
        required=True,
        help="Short name for the conversation",
    )
    parser.add_argument(
        "--bespoke_storage",
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
        include_only_from_llm_cache=True,
        include_only_from_cache=True,
    )
    return parser


if __name__ == "__main__":
    args = build_parser().parse_args()
    main(args)

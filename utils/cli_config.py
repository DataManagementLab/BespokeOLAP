from __future__ import annotations

import argparse

DEFAULT_MODEL = "gpt-5.2-codex"
DEFAULT_ARTIFACTS_DIR = "/mnt/labstore/bespoke_olap/"
DEFAULT_PARQUET_DIR = "/mnt/labstore/bespoke_olap/"


def build_run_config(
    *,
    benchmark: str,
    conv_name: str,
    query_list: str,
    notify: bool,
    conv_mode: str,  # scripted, optimization, ...
    start_snapshot: str | None = None,
    storage_plan_snapshot: str | None = None,
    max_scale_factor: int | None = None,
    continue_run: bool = False,
    replay: bool = False,
    disable_tracing: bool = False,
    disable_wandb: bool = False,
    model: str = DEFAULT_MODEL,
    base_parquet_dir: str = DEFAULT_PARQUET_DIR,
    artifacts_dir: str = DEFAULT_ARTIFACTS_DIR,
    no_preload: bool = False,
    disable_repo_sync: bool = False,
    replay_cache: bool = False,
    keep_csv: bool = False,
    disable_valtool: bool = False,
    disable_artifacts_context: bool = False,
    auto_u: bool = False,  # automatically use all prompts - skip user confirmation prompt
    auto_finish: bool = False,  # automatically finish if no more prompt is found in conversation / i.e. Str-D in last iteration
    is_bespoke_storage: bool = False,  # for wandb: mark that this run is using bespoke storage plan
    run_tool_offer_trace_option: bool = False,  # whether to offer the option to enable tracing in the conversation (for collecting execution traces for training data generation)
    only_from_llm_cache: bool = False,
    only_from_cache: bool = False,  # whether to only answer from cache and not call the LLM / run tool. Will raise an error if a cache miss occurs.
) -> argparse.Namespace:
    assert not conv_name.startswith(f"{benchmark}_"), (
        f"conv_name '{conv_name}' should not be prefixed with benchmark name '{benchmark}_'. We will add it automatically."
    )
    prefixed_conv_name = f"{benchmark}_{conv_name}"

    return argparse.Namespace(
        benchmark=benchmark,
        conv_name=prefixed_conv_name,
        query_list=query_list,
        continue_run=continue_run,
        replay=replay,
        disable_tracing=disable_tracing,
        disable_wandb=disable_wandb,
        model=model,
        artifacts_dir=artifacts_dir,
        no_preload=no_preload,
        notify=notify,
        start_snapshot=start_snapshot,
        storage_plan_snapshot=storage_plan_snapshot,
        max_scale_factor=max_scale_factor,
        disable_repo_sync=disable_repo_sync,
        replay_cache=replay_cache,
        keep_csv=keep_csv,
        disable_valtool=disable_valtool,
        disable_artifacts_context=disable_artifacts_context,
        auto_u=auto_u,
        auto_finish=auto_finish,
        is_bespoke_storage=is_bespoke_storage,
        conv_mode=conv_mode,
        run_tool_offer_trace_option=run_tool_offer_trace_option,
        only_from_llm_cache=only_from_llm_cache,
        only_from_cache=only_from_cache,
        base_parquet_dir=base_parquet_dir,
    )


def add_common_args(
    parser: argparse.ArgumentParser,
    *,
    include_model: bool = False,
    include_benchmark: bool = False,
    include_replay: bool = False,
    include_disable_tracing: bool = False,
    include_disable_wandb: bool = False,
    include_conv_name: bool = False,
    include_query_list: bool = False,
    include_continue_run: bool = False,
    include_artifacts_dir: bool = False,
    include_no_preload: bool = False,
    include_notify: bool = False,
    include_start_snapshot: bool = False,
    include_storage_plan_snapshot: bool = False,
    start_snapshot_required: bool = False,
    include_disable_repo_sync: bool = False,
    include_replay_cache: bool = False,
    include_auto_u: bool = False,
    include_auto_finish: bool = False,
    include_keep_csv: bool = False,
    include_disable_valtool: bool = False,
    include_disable_artifacts_context: bool = False,
    include_conv_mode: bool = False,
    include_run_tool_offer_trace_option: bool = False,
    include_is_bespoke_storage: bool = False,
    include_only_from_llm_cache: bool = False,
    include_base_parquet_dir: bool = False,
    include_only_from_cache: bool = False,
) -> None:
    if include_model:
        parser.add_argument(
            "--model",
            default=DEFAULT_MODEL,
            help="Model ID to use for the agent.",
        )

    if include_benchmark:
        parser.add_argument(
            "--benchmark",
            default="tpch",  # options: tpch, ceb
            help="Benchmark to use for the agent.",
        )
    if include_replay:
        parser.add_argument(
            "--replay",
            action="store_true",
            default=False,
            help="Replay previous conversation if set.",
        )
    if include_disable_tracing:
        parser.add_argument(
            "--disable_tracing",
            action="store_true",
            default=False,
            help="Disable tracing if set.",
        )
    if include_disable_wandb:
        parser.add_argument(
            "--disable_wandb",
            action="store_true",
            default=False,
            help="Disable wandb if set.",
        )
    if include_conv_name:
        parser.add_argument(
            "--conv_name",
            help="Name of conversation.",
            required=True,
        )
    if include_query_list:
        parser.add_argument(
            "--query_list",
            help="Comma-separated list of queries.",
            required=True,
        )
    if include_continue_run:
        parser.add_argument(
            "--continue_run",
            action="store_true",
            default=False,
            help="Continue with the current snapshot in the working-dir. Does not start empty.",
        )
    if include_artifacts_dir:
        parser.add_argument(
            "--artifacts_dir",
            type=str,
            default=DEFAULT_ARTIFACTS_DIR,
            help="Directory to store artifacts like logs.",
        )
    if include_no_preload:
        parser.add_argument(
            "--no_preload",
            action="store_true",
            default=False,
            help="Skip validate tool preloading",
        )
    if include_notify:
        parser.add_argument(
            "--notify",
            action="store_true",
            default=False,
            help="Notify when conversation requires action",
        )
    if include_start_snapshot:
        parser.add_argument(
            "--start_snapshot",
            type=str,
            default=None,
            required=start_snapshot_required,
            help="Path to snapshot to start from (if not continuing current snapshot).",
        )
    if include_base_parquet_dir:
        parser.add_argument(
            "--base_parquet_dir",
            type=str,
            default=DEFAULT_PARQUET_DIR,
            help="Base parquet directory.",
        )
    if include_storage_plan_snapshot:
        parser.add_argument(
            "--storage_plan_snapshot",
            type=str,
            default=None,
            help="Path to snapshot to load storage plan from (incompatible with --continue_run).",
        )
    if include_disable_repo_sync:
        parser.add_argument(
            "--disable_repo_sync",
            action="store_true",
            default=False,
            help="Disable syncing snapshots with the cache repo.",
        )
    if include_replay_cache:
        parser.add_argument(
            "--replay_cache",
            action="store_true",
            default=False,
            help="Auto press 'u' until first non-cached LLM call",
        )
    if include_auto_u:
        parser.add_argument(
            "--auto_u",
            action="store_true",
            default=False,
            help="Auto press 'u' for all prompts (skip user interaction, and auto-approve all prompts). This is dangerous and might lead to large bills / unwanted changes / ... Huge caution advised.",
        )

    if include_auto_finish:
        parser.add_argument(
            "--auto_finish",
            action="store_true",
            default=False,
            help="Automatically finish if no more prompt is found in conversation / i.e. Str-D in last iteration",
        )

    if include_keep_csv:
        parser.add_argument(
            "--keep_csv",
            action="store_true",
            default=False,
            help="Keep csv if set.",
        )

    if include_disable_valtool:
        parser.add_argument(
            "--disable_valtool",
            action="store_true",
            default=False,
            help="Disable validate tool if set",
        )
    if include_disable_artifacts_context:
        parser.add_argument(
            "--disable_artifacts_context",
            action="store_true",
            default=False,
            help="Do not include workspace artifacts in cache hashing.",
        )

    if include_conv_mode:
        parser.add_argument(
            "--conv_mode",
            type=str,
            default="scripted",  # options: scripted, optimization, ...
            help="Conversation mode to use for the agent. E.g. 'scripted', 'optimization', ...",
        )

    if include_run_tool_offer_trace_option:
        parser.add_argument(
            "--run_tool_offer_trace_option",
            action="store_true",
            default=False,
            help="Whether to include trace options in the run tool (and consequently offer the option to enable tracing in the conversation). This is needed for collecting execution traces for training data generation.",
        )
    if include_is_bespoke_storage:
        parser.add_argument(
            "--is_bespoke_storage",
            action="store_true",
            default=False,
            help="For wandb logging: mark that this run is using bespoke storage plan",
        )

    if include_only_from_llm_cache:
        parser.add_argument(
            "--only_from_llm_cache",
            action="store_true",
            default=False,
            help="Only answer from LLM cache and do not call the LLM. Will raise an error if a cache miss occurs.",
        )
    if include_only_from_cache:
        parser.add_argument(
            "--only_from_cache",
            action="store_true",
            default=False,
            help="Only answer from cache (including both LLM cache and run tool cache) and do not call the LLM or run tool. Will raise an error if a cache miss occurs.",
        )

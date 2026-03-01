import argparse
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from agents import (
    Agent,
    ApplyPatchTool,
    ModelSettings,
    Runner,
    ShellTool,
    trace,
)
from agents.extensions.memory import AdvancedSQLiteSession
from agents.tracing import set_tracing_disabled
from dotenv import load_dotenv

import wandb
from conversations.conversation import (
    COMPACTION_MARKER,
    VALIDATE_OFF,
    VALIDATE_ON,
    VALIDATE_OUTPUT_STDOUT_OFF,
    VALIDATE_OUTPUT_STDOUT_ON,
)
from conversations.optimization_conversation import OptimizationConversation
from conversations.scripted_conversation import ScriptedConversation
from dataset.dataset_tables_dict import get_dataset_name
from dataset.query_gen_factory import get_placeholders_fn, get_query_gen
from llm_cache import (
    CachedLitellmModel,
    CachedOpenAIResponsesModel,
    GitSnapshotter,
    setup_logging,
)
from llm_cache.cached_compaction_session import CachedOpenAIResponsesCompactionSession
from llm_cache.utils import ask_yes_no
from tools.fasttest import copy_template_to, make_compile_tool, make_run_tool
from tools.litellm_apply_patch import make_litellm_apply_patch_tool
from tools.litellm_shell import make_litellm_shell_tool
from tools.shell_executor import ShellExecutor
from tools.validate_tool.query_validator_class import QueryValidator
from tools.validate_tool.sf_list_gen import gen_sf
from tools.workspace_editor import WorkspaceEditor
from utils.cli_config import add_common_args
from utils.general_utils import write_query_and_args_file
from utils.model_setup import setup_model_config
from utils.pkgconfig import check_pkg
from utils.snapshot_utils import load_storage_plan_from_snapshot
from utils.token_usage import get_tokens_context_and_dollar_info
from utils.truncate_model_log import truncate_model_final_output
from utils.wandb_stats_logging import WandbRunHook
from utils.weave_cache import configure_weave_cache_dirs

logger = logging.getLogger(__name__)


async def main(args: argparse.Namespace) -> None:
    workspace_path = Path("./output")
    workspace_path.mkdir(exist_ok=True)

    cache_path = Path(args.artifacts_dir) / "cache"
    cache_repo = None if args.disable_repo_sync else "git://c01/bespoke_cache.git"

    conversations_dir = Path(args.artifacts_dir) / "conversations"

    extra_gitignore = ["*.o", "*.d", "/db", "/build/", "*.log", "*.tmp"]
    if not args.keep_csv:
        # in optimize mode, ignore all .csv files (they are generated during validation).
        # in gen_code mode, we want to keep them around in case of major issues with ensuring correctness while generating the base implementation.
        extra_gitignore.append("*.csv")

    # add versioning for the table dataset (dataset got regenerated, scale-up/down code was changed, query args input syntax was changed, etc. - in these cases we want to make sure that old cache entries are not used for the new dataset version)
    dataset_version = None
    if args.benchmark == "ceb":
        dataset_version = "3"

    snapshotter = GitSnapshotter(
        cache_repo=cache_repo,
        working_dir=workspace_path,
        extra_gitignore=extra_gitignore,
    )
    assert not snapshotter.is_dirty(), (
        f'Please remove all uncommitted changes in "{workspace_path}". We expect a clean working directory to ensure reproducibility.'
    )

    ##############################
    # Prepare workspace / snapshot
    ##############################

    # prepare query gen
    gen_query_fn = get_query_gen(args.benchmark)
    gen_placeholders_fn = get_placeholders_fn(
        args.benchmark, cache_path / "placeholders_cache"
    )

    query_list = [q.strip() for q in args.query_list.split(",")]

    if args.storage_plan_snapshot is not None:
        # load storage plan snapshot and read storage plan form it.
        # afterwards a clean or other snapshot will be loaded
        storage_plan = load_storage_plan_from_snapshot(
            args, snapshotter, workspace_path
        )

        assert args.start_snapshot is None, (
            "loading a storage plan snapshot, but also providing a start snapshot is not supported. Are you really sure? Usually the storage plan will be kept in the snapshots as soons as coding starts, and you don't have to pass them again."
        )
    else:
        storage_plan = None

    artifacts_in_context = ""
    disable_artifacts_context = getattr(args, "disable_artifacts_context", False)
    # setup snapshot / workspace according to mode
    if args.start_snapshot is None:
        # gen -code mode
        if not args.continue_run:
            # create an empty snapshot
            snapshotter.create_empty_snapshot(args.conv_name)

            # uses fasttest
            # add template files to workspace
            # assemble string containing content of copied files - for versioning / snapshotting
            template_artifacts = copy_template_to(workspace_path, args.benchmark)
            if not disable_artifacts_context:
                artifacts_in_context += template_artifacts

            # when snapshow is created, we assume these files are part of the snapshot and already present
            logger.info(
                f"Generating query and args files for queries: {args.benchmark}/{query_list}"
            )
            query_artifacts = write_query_and_args_file(
                benchmark_name=args.benchmark,
                gen_placeholders_fn=gen_placeholders_fn,
                query_list=query_list,
                out_dir=workspace_path.as_posix(),
                use_fasttest_format=True,  # old validate tool uses old format (fasttest format introduced with hotpatching / compile_tool, run_tool)
                storage_plan=storage_plan,
            )
            if not disable_artifacts_context:
                artifacts_in_context += query_artifacts
        else:
            pass
    else:
        assert not args.continue_run

        # check that snapshot exists
        assert snapshotter.has_snapshot(args.start_snapshot), (
            f"Snapshot {args.start_snapshot} not found in repo."
        )

        # load from provided snapshot
        logger.info(f"Restoring snapshot {args.start_snapshot}")
        snapshotter.restore(args.start_snapshot)

        # delete all .csv files from prior runs
        csv_files = list(workspace_path.rglob("result*.csv"))
        logger.info(f"Deleting existing result-csv files ({len(csv_files)} files).")
        for csv_file in csv_files:
            csv_file.unlink()

    ###############
    # Misc setup
    ###############

    parquet_path = args.artifacts_dir + f"/{get_dataset_name(args.benchmark)}_parquet/"

    max_scale_factor = (
        args.max_scale_factor if hasattr(args, "max_scale_factor") else 20
    )

    assert max_scale_factor is not None, "max_scale_factor must be set and not None."

    # Create hooks instance for tracking metrics
    wandb_metrics_hook: WandbRunHook | None = None
    if not args.disable_wandb:
        wandb_metrics_hook = WandbRunHook(
            model=args.model,
            git_snapshotter=snapshotter,
            cloc_cache_dir=cache_path / "cloc_cache",
        )

    # assemble default sf values for the selected benchmark
    verify_sf_list, max_scale_factor = gen_sf(args.benchmark)

    compile_cache_dir = cache_path / "compile"
    query_validator: QueryValidator | None = None
    if not args.disable_valtool:
        query_validator = QueryValidator(
            benchmark=args.benchmark,
            gen_query_fn=gen_query_fn,
            sf_list=verify_sf_list + [max_scale_factor],
            parquet_path=parquet_path,
            wandb_pin_worker=True,
            all_query_ids=query_list,
            num_random_query_instantiations=10,
            query_cache_dir=cache_path / "query_cache",
            validate_cache_dir=cache_path / "validate_tool",
            workspace_path=workspace_path,
            git_snapshotter=snapshotter,
        )

    ###############
    # Prepare Tools
    ###############
    editor = WorkspaceEditor(workspace_path, wandb_metrics_hook=wandb_metrics_hook)
    shell = ShellExecutor(
        workspace_path,
        snapshotter=snapshotter,
        cache_dir=cache_path / "shell",
        wandb_metrics_hook=wandb_metrics_hook,
    )

    run_tool_wrapper, run_tool = make_run_tool(
        cwd=workspace_path,
        query_validator=query_validator,
        wandb_metrics_hook=wandb_metrics_hook,
        compile_cache_dir=compile_cache_dir,
        git_snapshotter=snapshotter,
        dataset_name=get_dataset_name(args.benchmark),
        base_parquet_dir=args.base_parquet_dir,
        run_tool_offer_trace_option=args.run_tool_offer_trace_option,
        only_from_cache=args.only_from_cache,
    )

    tools = [
        ApplyPatchTool(editor=editor),
        ShellTool(executor=shell),
        make_compile_tool(
            cwd=workspace_path,
            compile_cache_dir=compile_cache_dir,
            git_snapshotter=snapshotter,
            wandb_metrics_hook=wandb_metrics_hook,
        ),
        run_tool_wrapper,
    ]

    #########################
    # Prepare Model and Agent
    #########################

    # setup cached compaction session
    use_litellm, model_name, api_key, client = setup_model_config(args.model)
    underlying_session = AdvancedSQLiteSession(
        session_id=args.conv_name, create_tables=True
    )

    def log_should_trigger_compaction(context: dict[str, Any]) -> bool:
        """Default decision: compact when >= 10 candidate items exist."""
        # logger.info(
        #     f"Ctx len candidate items: {len(context['compaction_candidate_items'])}",
        # )
        return False

    # assemble session
    session = CachedOpenAIResponsesCompactionSession(
        session_id=args.conv_name,
        client=client,
        underlying_session=underlying_session,
        should_trigger_compaction=log_should_trigger_compaction,
        cache_dir=cache_path / "compaction",
        model="gpt-5.2",
        wandb_metrics_hook=wandb_metrics_hook,
    )

    # prepare dict to be included in hash
    config_kwargs: Dict[str, Any] = {"max_snapshot_csv_size_mb": 5.0}
    if args.start_snapshot is not None:
        # include start snapshot in hash - makes cache specific to this code base
        config_kwargs["start_snapshot"] = args.start_snapshot

    if dataset_version is not None:
        config_kwargs["dataset_version"] = dataset_version

    if use_litellm:
        model = CachedLitellmModel(
            model=model_name,
            api_key=api_key,
            llm_cache_dir=cache_path / "llm_cache",
            snapshotter=snapshotter,
            stop_on_cache_miss=args.replay,
            query_gen_list=query_list,
            artifacts_in_context=artifacts_in_context,
            config_kwargs=config_kwargs,
        )
        tools = [
            tool for tool in tools if not isinstance(tool, (ApplyPatchTool, ShellTool))
        ]
        tools.insert(
            0,
            make_litellm_apply_patch_tool(
                root=workspace_path,
                wandb_metrics_hook=wandb_metrics_hook,
            ),
        )
        tools.insert(
            0,
            make_litellm_shell_tool(
                cwd=workspace_path,
                cache_dir=cache_path / "shell",
                git_snapshotter=snapshotter,
                wandb_metrics_hook=wandb_metrics_hook,
            ),
        )
    else:
        model = CachedOpenAIResponsesModel(
            model=model_name,
            openai_client=client,
            llm_cache_dir=cache_path / "llm_cache",
            snapshotter=snapshotter,
            stop_on_cache_miss=args.replay
            or args.only_from_llm_cache
            or args.only_from_cache,
            query_gen_list=query_list,  # add to hash to make sure cache is specific to these queries
            artifacts_in_context=artifacts_in_context,  # add to hash to make sure cache is specific to these queries - these files might be different even for same query ids - prevent that snapshotter is overwritting never versions of them.
            config_kwargs=config_kwargs,  # will be included in hash
        )

    instructions = [
        f"You can edit files inside {workspace_path} using the apply_patch tool. ",  # follows openai cookbook: https://github.com/openai/openai-agents-python/blob/main/examples/tools/apply_patch.py
        "When modifying an existing file, include the file contents between ",
        "<BEGIN_FILES> and <END_FILES> in your prompt. ",
        "You can run shell commands using the shell tool. Do not emit argv form. ",
        "You can compile the code using the compile tool. ",
        "You can run a list of queries using the run tool. The run tool automatically compiles the code. You can specify the queries to run and the scale factors to use. If no queries are specified, all queries will be run.",
    ]
    if use_litellm:
        instructions = [
            f"You can edit files inside {workspace_path} using the apply_patch tool. ",
            "When modifying an existing file, include the file contents between ",
            "<BEGIN_FILES> and <END_FILES> in your prompt. ",
            "You can run shell commands using the shell tool. Do not emit argv form. ",
            "You can compile the code using the compile tool. ",
            "You can run a list of queries using the run tool. The run tool automatically compiles the code. You can specify the queries to run and the scale factors to use. If no queries are specified, all queries will be run.",
        ]

    model_settings = ModelSettings(tool_choice="auto")
    if use_litellm:
        model_settings = ModelSettings(tool_choice="auto", include_usage=True)
    default_agent_name = "Bespoke Assistant"
    agent = Agent(
        name=default_agent_name,
        model=model,
        instructions="".join(instructions),
        tools=tools,
        model_settings=model_settings,
    )

    logger.info(f"Workspace root: {workspace_path}")
    logger.info(f"Using model: {model}")

    async def handle_prompt(
        text: str,
        short_desc: Optional[str],
        idx: int,
        max_turns: Optional[int] = None,
    ) -> Optional[str]:

        # set default max_turns value
        if max_turns is None:
            max_turns = 75

        # check for compaction marker in the prompt string - in this case run compaction and return
        if text == COMPACTION_MARKER:
            logger.info(f"Triggering compaction at prompt index {idx}")
            await session.run_compaction({"force": True, "compaction_mode": "input"})
            return None
        # check for markers to enable / disable validation
        if text == VALIDATE_ON:
            run_tool.parse_out_and_validate_output = True
            logger.info(f"Enabled output parsing and validation at prompt index {idx}")
            return None
        if text == VALIDATE_OFF:
            run_tool.parse_out_and_validate_output = False
            logger.info(f"Disabled output parsing and validation at prompt index {idx}")
            return None
        if text == VALIDATE_OUTPUT_STDOUT_ON:
            assert query_validator is not None
            query_validator.output_stdout_stderr = True
            logger.info(
                f"Enabled output stdout in validation results at prompt index {idx}"
            )
            return None
        if text == VALIDATE_OUTPUT_STDOUT_OFF:
            assert query_validator is not None
            query_validator.output_stdout_stderr = False
            logger.info(
                f"Disabled output stdout in validation results at prompt index {idx}"
            )
            return None

        logger.info("=" * 80)
        logger.info(text)
        logger.info("=" * 80)

        # Update prompt index in hooks
        if wandb_metrics_hook is not None:
            wandb_metrics_hook.prompt_idx = idx
            wandb_metrics_hook.current_prompt = text
            wandb_metrics_hook.current_prompt_descriptor = short_desc

        # Rename the agent for each stage based on the short description - this makes it easier to analyze the tracing logs and see which stage is producing which output, without having to rely on the prompt content which might be very long. The name will be reset to default_agent_name if short_desc is None, which is the case for normal prompts that are not associated with a specific stage.
        # We rewrite it to hack a different header for each stage into the tracing log.
        # THIS IS RISKY: if openai somehow refers to agent.name this is a problem, since it will be not an identifier anymore.
        if short_desc is None:
            agent.name = default_agent_name
        else:
            agent.name = f"{default_agent_name} ({short_desc})"

        # Run with hooks for automatic metric tracking
        result = await Runner.run(
            agent,
            input=text,
            session=session,
            max_turns=max_turns,
            hooks=wandb_metrics_hook,
        )

        # # store run usage
        # session.underlying_session.store_run_usage(result)

        # Log cost summary
        get_tokens_context_and_dollar_info(
            result.context_wrapper.usage, str(model), last_entry_only=False, log=True
        )

        # log final output (truncated)
        logger.info(truncate_model_final_output(result.final_output))

        return result.final_output

    # manually traced conversation - otherwise will produce multiple separate traces (for each Runner.run() invocation)
    with trace(
        f"Bespoke-Agent {args.conv_name} Conversation",
        metadata={  # log some metadata about this run
            "query": args.conv_name,
            "model": args.model,
            "tools": str([type(t).__name__ for t in tools]),
        },
    ):
        conv_args = dict(
            conversation_json_path=conversations_dir / f"{args.conv_name}.json",
            callback=handle_prompt,
            auto_finish=args.auto_finish,
            replay_cache=args.replay_cache,
            auto_u=args.auto_u,
            replay=args.replay,
            notify=args.notify,
            model=model,
        )
        if args.conv_mode == "scripted":
            # all prompts are pre-defined and are listed in the json file (hence "scripted") - user can still give input.
            conv = ScriptedConversation(**conv_args)
        elif args.conv_mode == "optimization":
            # optimization loop is self-steered. It will vary based on model output, measured speedups, ...
            assert query_validator is not None, (
                "query_validator must be provided for optimization conversation"
            )
            conv = OptimizationConversation(
                query_ids=query_list,
                bespoke_storage=args.is_bespoke_storage,
                run_tool=run_tool,
                verify_sf_list=verify_sf_list,
                benchmark_sf=max_scale_factor,
                query_validator=query_validator,
                git_snapshotter=snapshotter,
                revert_on_regression=True,
                session=underlying_session,
                wandb_run_hook=wandb_metrics_hook,
                **conv_args,
            )
        else:
            raise ValueError(f"Unknown conversation mode: {args.conv_mode}")

        await conv.run()

    logger.debug(f"Model cache total saved: ${model.total_saved:0.6f}")

    if not args.disable_wandb:
        assert wandb_metrics_hook is not None
        # Log final summary to wandb
        wandb.log(
            {
                "final/total_cost_usd": wandb_metrics_hook.total_stats["cost_usd"],
                "final/total_turns": wandb_metrics_hook.last_turn,
                "final/total_tokens": wandb_metrics_hook.total_stats["output_tokens"]
                + wandb_metrics_hook.total_stats["input_tokens"]
                + wandb_metrics_hook.total_stats["reasoning_tokens"],
                "final/num_prompts": wandb_metrics_hook.prompt_idx + 1,
            }
        )


def run_conv_wrapper(args: argparse.Namespace) -> None:
    if args.continue_run:
        ask_yes_no(
            "Are you really sure you want to continue the current snapshot? Does not start from fresh and continues from current state of output folder. This is DANGEROUS as it might include unwanted files already present in the output folder!"
        )

    log_path = Path(args.artifacts_dir) / "logs"
    log_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    setup_logging(logging.DEBUG, log_path / f"{timestamp}_{args.conv_name}.log")

    load_dotenv()
    if args.disable_tracing:
        set_tracing_disabled(True)
    elif not args.disable_wandb:
        # add weave (wandb) tracing in addition to openai tracing
        configure_weave_cache_dirs()
        import weave

        entity = os.getenv("WANDB_ENTITY", "learneddb")
        project = os.getenv("WANDB_PROJECT", "bespoke-olap-agents")

        weave.init(
            f"{entity}/{project}",
            # weave_log_level="info",
            settings={"log_level": "INFO", "print_call_link": False},
        )

        # log statistics to wandb
        tags = [args.benchmark]
        if args.is_bespoke_storage:
            tags.append("bespoke-storage")

        wandb_run = wandb.init(
            config=vars(args),
            entity=entity,
            project=project,
            name=f"{args.conv_name}",
            tags=tags,
            # dir=f"/tmp/{os.environ['USER']}/wandb",
        )

    asyncio.run(main(args))


if __name__ == "__main__":
    if not check_pkg("arrow", "parquet"):
        raise Exception("arrow and parquet are not available. See README.")

    # example call:
    # python main.py manual --conv_name test43 --query_list 1

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    manual = subparsers.add_parser(
        "manual",
        help="Run a conversation using explicit mode/query args.",
    )
    add_common_args(
        manual,
        include_model=True,
        include_replay=True,
        include_disable_tracing=True,
        include_disable_wandb=True,
        include_conv_name=True,
        include_query_list=True,
        include_continue_run=True,
        include_artifacts_dir=True,
        include_no_preload=True,
        include_notify=True,
        include_start_snapshot=True,
        include_disable_repo_sync=True,
        include_replay_cache=True,
        include_auto_u=True,
        include_keep_csv=True,
        include_disable_valtool=True,
        include_disable_artifacts_context=True,
        include_benchmark=True,
        include_auto_finish=True,
        include_storage_plan_snapshot=True,
        include_conv_mode=True,
        include_is_bespoke_storage=True,
        include_run_tool_offer_trace_option=True,
    )
    args = parser.parse_args()
    args.write_query_and_args_files = True

    if args.command == "manual":
        run_conv_wrapper(args)
    else:
        raise Exception(f"Unknown {args.command}")

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agents.run_context import RunContextWrapper
from agents.tool import FunctionTool
from pydantic import BaseModel, Field

from llm_cache.git_snapshotter import GitSnapshotter
from misc.fasttest.compiler_cached import CachedCompiler
from misc.fasttest.fasttest_proc import FasttestProc
from tools.validate_tool.query_validator_class import (
    QueryValidator,
    approx_timeout_for_validation,
)
from tools.validate_tool.run_and_check_queries import assemble_error
from utils.wandb_stats_logging import WandbRunHook

from .pool import FastTestPool
from .utils import make_compiler

logger = logging.getLogger(__name__)


@dataclass
class RunWorkerResult:
    msg: str
    metrics: Optional[Dict] = None
    resp: Optional[str] = None
    out: Optional[str] = None
    err: Optional[str] = None


class RunTool:
    """Runs the database and executes a query by id"""

    parse_out_and_validate_output: bool = True

    def __init__(
        self,
        cwd: Path,
        dataset_name: str,
        base_parquet_dir: str,  # must contain per scale-factors subdirs: e.g. base_parquet_dir/sf1/, base_parquet_dir/sf10/..., each containing the corresponding parquet files for the scale factor
        query_validator: Optional[QueryValidator] = None,
        wandb_metrics_hook: Optional[WandbRunHook] = None,
        compile_cache_dir: Optional[Path] = None,
        git_snapshotter: Optional[GitSnapshotter] = None,
        parse_out_and_validate_output: bool = True,
        api_path: Optional[Path] = None,
        only_from_cache: bool = False,
    ):
        self.cwd = cwd
        self.dataset_name = dataset_name
        self.base_parquet_dir = base_parquet_dir
        self.compiler: CachedCompiler = make_compiler(
            cwd,
            compile_cache_dir=compile_cache_dir,
            git_snapshotter=git_snapshotter,
            api_path=api_path,
        )
        self.query_validator: Optional[QueryValidator] = query_validator
        self.wandb_metrics_hook = wandb_metrics_hook
        self.parse_out_and_validate_output = parse_out_and_validate_output
        self.only_from_cache = only_from_cache

    def run(
        self,
        scale_factor: float,
        optimize: bool,
        query_id: Optional[List[str]] = None,
        trace_mode: bool = False,  # set trace flag
        external_call: bool = False,  # only for logging purposes
    ) -> Tuple[str, Optional[Dict]]:
        try:
            run_result = self.run_worker(
                scale_factor=scale_factor,
                optimize=optimize,
                query_id=query_id,
                trace_mode=trace_mode,
                external_call=external_call,
            )
        except FileNotFoundError:
            # run with force compile to make sure ./db file exists (and not skipped because of caching)
            run_result = self.run_worker(
                scale_factor=scale_factor,
                optimize=optimize,
                query_id=query_id,
                trace_mode=trace_mode,
                force_compile=True,
                external_call=external_call,
            )

        return run_result.msg, run_result.metrics

    def run_worker(
        self,
        scale_factor: float,
        optimize: bool,
        query_id: Optional[List[str]] = None,
        trace_mode: bool = False,  # set trace flag
        force_compile: bool = False,
        external_call: bool = False,
        stdin_args_data: Optional[List[str]] = None,
        current_git_snapshot: Optional[
            str
        ] = None,  # for external instrumentation: e.g. from benchmarking script (will not use git snapshotter)
    ) -> RunWorkerResult:
        if scale_factor >= 1:
            # it has to be an int
            assert int(scale_factor) == scale_factor, (
                "Scale factor has to be integer >= 1"
            )
            scale_factor = int(scale_factor)

        # check that scalefactor is prepared  /availabe in validator
        if (
            self.query_validator is not None
            and scale_factor not in self.query_validator.sf_list
            and stdin_args_data
            is None  # if manual stdin args are provided, we skip the check and just execute (e.g. for testing purposes
        ):
            metrics = assemble_error(
                scale_factor=scale_factor,
                query_ids_executed=query_id if query_id is not None else [],
            )
            metrics["type"] = "validate"
            metrics["validation/fasttest_optimize"] = optimize
            metrics["validation/trace_mode"] = trace_mode
            metrics["validation/compile_error"] = True
            metrics["validation/external_call"] = external_call
            if self.wandb_metrics_hook is not None:
                self.wandb_metrics_hook.log_metrics_callback(
                    metrics, log_and_increment=True
                )
            return RunWorkerResult(
                msg=f"Scale factor {scale_factor} not available in query validator (not prepared). Available scale factors: {self.query_validator.sf_list}",
                metrics=metrics,
            )

        if stdin_args_data is not None:
            logger.warning(
                "Launching with manual stdin args data. Query-Validator will not be invoked!"
            )

        cxx_flags = []
        if optimize:
            cxx_flags.extend(["-O3", "-flto"])
        if trace_mode:
            cxx_flags.append("-DTRACE")
        self.compiler.set_extra_cxxflags(cxx_flags)

        err, compile_used_cache, compile_key_hash = self.compiler.build_cached(
            skip_cache=force_compile,
            current_git_snapshot=current_git_snapshot,
            only_from_cache=self.only_from_cache,
        )
        if err is not None:
            if self.wandb_metrics_hook is not None:
                # assemble validation error
                metrics = assemble_error(
                    scale_factor=scale_factor,
                    query_ids_executed=query_id if query_id is not None else [],
                )
                metrics["type"] = "validate"
                metrics["validation/fasttest_optimize"] = optimize
                metrics["validation/trace_mode"] = trace_mode
                metrics["validation/compile_error"] = True
                metrics["validation/external_call"] = external_call
                self.wandb_metrics_hook.log_metrics_callback(
                    metrics, log_and_increment=True
                )
            return RunWorkerResult(msg=err, err=err)

        parquet_dir = f"{self.base_parquet_dir}/sf{scale_factor}/"
        cmd = f"./db {parquet_dir}"
        runner = FastTestPool.get(
            cmd,
            lambda: FasttestProc(
                cmd,
                echo_output=True,
                cwd=self.cwd,
            ),
        )
        logger.info(
            f"Run with: {query_id=} {scale_factor=} {self.dataset_name=} {trace_mode=} {optimize=} {self.base_parquet_dir=}"
        )

        # callback executing the query
        def exec_callback(args_list: List[str], timeout_s: int) -> Tuple[str, str, str]:
            # send queries to runner
            for arg in args_list:
                # logger.info(f"{arg=}")
                runner.send(arg)  # send to runner

            # signal end of input
            # --> stops only parsers, do not close stdin
            runner.send("")

            resp, out, err = runner.run(timeout=timeout_s)
            logger.info(f"resp={resp.rstrip()}")
            return resp, out, err

        # validate output correctness
        # in case query-validator is not provided or manual-stdin args are provided, just execute without validation
        if self.query_validator and stdin_args_data is None:
            msg, success, metrics, exec_used_cache = (
                self.query_validator.exec_and_validate(
                    exec_callback_fn=exec_callback,
                    scale_factor=scale_factor,
                    query_id=query_id,
                    other_config={"optimize": optimize},
                    skip_validate=not self.parse_out_and_validate_output,
                    compile_key_hash=compile_key_hash,
                    trace_mode=trace_mode,
                    only_from_cache=self.only_from_cache,
                )
            )

            # this assertion does unfortunately not work: it is valid that args for validate change, but compile is the same. E.g. different scale factors.
            # assert compile_used_cache == exec_used_cache, (
            #     "Inconsistent cache usage between compile and execute. This should always be chained! If this happens, potentially a change in the wrapper code/... happened. Please delete both cache entries (compile & exec), check your changes and re-run."
            # )
            if exec_used_cache:
                assert compile_used_cache, (
                    "Inconsistent cache usage between compile and execute: if exec was cached then compile also needs to be cached. This should always be chained! If this happens, potentially a change in the wrapper code/... happened. Please delete the corresponding cache entry (validate cache), check your changes and re-run."
                )
            resp = None
            out = None
            err = None
        else:
            logger.warning(
                "No query validator provided, just executing the query without validation!"
            )

            if stdin_args_data is None:
                stdin_args_data = [f"{query_id} x=12 v=32"]

            timeout = approx_timeout_for_validation(
                scale_factor=scale_factor,
                num_queries=len(stdin_args_data),
                repetitions=1,
                num_random_query_instantiations=1,
            )

            resp, out, err = exec_callback(stdin_args_data, timeout_s=timeout)
            msg = f"stdout: {out.rstrip()}\nstderr: {err.rstrip()}\n{resp}"
            metrics = None

        if self.wandb_metrics_hook is not None:
            assert isinstance(metrics, Dict)
            metrics["type"] = "validate"
            metrics["validation/fasttest_optimize"] = optimize
            metrics["validation/trace_mode"] = trace_mode
            metrics["validation/external_call"] = external_call
            self.wandb_metrics_hook.log_metrics_callback(
                metrics, log_and_increment=True
            )

        return RunWorkerResult(msg=msg, metrics=metrics, resp=resp, out=out, err=err)

    def __call__(
        self,
        scale_factor: float,
        optimize: bool,
        query_id: Optional[List[str]] = None,
        trace_mode: bool = False,  # sets trace flag for the run
    ) -> str:
        return self.run(
            scale_factor=scale_factor,
            optimize=optimize,
            query_id=query_id,
            trace_mode=trace_mode,
        )[0]


class RunArgs(BaseModel):
    scale_factor: int = Field(..., ge=1, description="Scale factor (>= 1)")
    optimize: bool = Field(..., description="Enable compiler optimization")
    query_id: List[str] | None = Field(
        None,
        description="List of Query-IDs to execute. None means all queries.",
    )


class IMDBRunArgs(BaseModel):
    scale_factor: float = Field(..., gt=0, description="Scale factor (> 0)")
    optimize: bool = Field(..., description="Enable compiler optimization")
    query_id: List[str] | None = Field(
        None,
        description="List of Query-IDs to execute. None means all queries.",
    )


trace_flag_description = "Whether to set TRACE flag for the run (setting cxx flag -DTRACE, e.g. enables collecting execution statistics for code optimization if implemented in the codebase)"


class RunArgsTrace(RunArgs):
    trace_mode: bool = Field(
        False,
        description=trace_flag_description,
    )


class IMDBRunArgsTrace(IMDBRunArgs):
    trace_mode: bool = Field(
        False,
        description=trace_flag_description,
    )


def make_run_tool(
    cwd: Path,
    dataset_name: str,
    base_parquet_dir: str,  # must contain per scale-factors subdirs: e.g. base_parquet_dir/sf1/, base_parquet_dir/sf10/..., each containing the corresponding parquet files for the scale factor
    query_validator: Optional[QueryValidator] = None,
    wandb_metrics_hook: Optional[WandbRunHook] = None,
    compile_cache_dir: Optional[Path] = None,
    git_snapshotter: Any = None,
    run_tool_offer_trace_option: bool = False,
    only_from_cache: bool = False,
) -> Tuple[FunctionTool, RunTool]:

    parquet_dir = f"{base_parquet_dir}/{dataset_name}_parquet/"

    impl = RunTool(
        cwd,
        query_validator=query_validator,
        wandb_metrics_hook=wandb_metrics_hook,
        compile_cache_dir=compile_cache_dir,
        git_snapshotter=git_snapshotter,
        dataset_name=dataset_name,
        base_parquet_dir=parquet_dir,
        only_from_cache=only_from_cache,
    )

    def get_args_model():
        if dataset_name == "imdb":
            return IMDBRunArgsTrace if run_tool_offer_trace_option else IMDBRunArgs
        else:
            return RunArgsTrace if run_tool_offer_trace_option else RunArgs

    args_model = get_args_model()

    async def on_invoke(ctx: RunContextWrapper[Any], args_json: str) -> str:
        args = args_model.model_validate_json(args_json)

        if run_tool_offer_trace_option:
            return impl(
                scale_factor=args.scale_factor,
                optimize=args.optimize,
                query_id=args.query_id,
                trace_mode=args.trace_mode,  # type: ignore
            )
        else:
            return impl(
                scale_factor=args.scale_factor,
                optimize=args.optimize,
                query_id=args.query_id,
            )

    return FunctionTool(
        name="run",
        description="Runs the database and executes a query by query-id",
        params_json_schema=args_model.model_json_schema(),
        on_invoke_tool=on_invoke,
    ), impl

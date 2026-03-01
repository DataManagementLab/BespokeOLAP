import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from agents.extensions.memory import AdvancedSQLiteSession

from conversations.conversation import (
    COMPACTION_MARKER,
    VALIDATE_ON,
    VALIDATE_OUTPUT_STDOUT_ON,
    AbstractConversation,
)
from conversations.prompts_gen import (
    load_expert_knowledge,
    optim_prompt_add_timings,
    optim_prompt_add_timings_per_query,
    optim_prompt_constraints,
    optim_prompt_pinning,
    optim_prompt_pretext,
    optim_prompt_pretext_optim,
    optim_prompt_w_trace,
    optim_prompt_with_expert_knowledge,
    optim_prompt_with_human_reference,
    optim_prompt_with_sample_plan,
)
from llm_cache.git_snapshotter import GitSnapshotter
from tools.fasttest.run import RunTool
from tools.validate_tool.query_validator_class import QueryValidator
from utils.wandb_stats_logging import WandbRunHook

logger = logging.getLogger(__name__)


@dataclass
class StageConfig:
    name: str
    # Called with the current impl runtime (seconds) just before the stage runs.
    # Returns the full prompt string for this stage.
    get_prompt: Callable[[float], str]
    get_descriptor: Callable[[], Optional[str]] = lambda: None
    max_turns: Optional[int] = None


@dataclass
class StageResult:
    name: str
    rt_before_s: float
    rt_after_s: float
    speedup_vs_duckdb: float

    @property
    def improved(self) -> bool:
        return self.rt_after_s < self.rt_before_s

    @property
    def improvement_factor(self) -> float:
        if self.rt_after_s > 0:
            return self.rt_before_s / self.rt_after_s
        return float("inf")


class OptimizationConversation(AbstractConversation):
    def __init__(
        self,
        query_ids: List[str],
        run_tool: RunTool,
        verify_sf_list: List[float],
        benchmark_sf: float,
        query_validator: QueryValidator,
        git_snapshotter: GitSnapshotter,
        session: AdvancedSQLiteSession,
        wandb_run_hook: Optional[WandbRunHook],
        bespoke_storage: bool = True,
        revert_on_regression: bool = True,  # in case of a regression, should we automatically revert to the last snapshot before continuing with the next stage? or should we keep the regression and continue anyway? This parameter does not need to be added to cache-hashes - the current snapshot id is already part of the state-hashes.
        **kwargs,
    ):
        super().__init__(
            allowed_choices=("u",),
            **kwargs,
        )

        self.query_ids = query_ids
        self.bespoke_storage = bespoke_storage
        self.run_tool = run_tool
        self.verify_sf_list = verify_sf_list
        self.benchmark_sf = benchmark_sf
        self.git_snapshotter = git_snapshotter
        self.revert_on_regression = revert_on_regression
        self.session = session
        self.wandb_run_hook = wandb_run_hook

        assert not self.replay, (
            "Replay mode is not supported for OptimizationConversation. Use replay_cache if you want to replay from cache without user interaction."
        )

        # retrieve sample plans / instantions for the queries
        self.sample_plan_dict = dict()
        for query_id in query_ids:
            instantiations, _ = query_validator._get_instantiations(
                scale_factor=benchmark_sf, query_id=[query_id]
            )

            self.sample_plan_dict[query_id] = instantiations[0].duckdb_plan

        self.query_rt_log: Dict[str, float] = dict()

    def _build_stages(
        self,
        query_id: str,
        mandatory_constraints: str,
    ) -> List[StageConfig]:
        """Return the ordered list of optimization stages for a single query."""
        sf = self.benchmark_sf
        sample_plan = self.sample_plan_dict[query_id]

        # load expert knowledge once - shared across all query optimization stages
        expert_knowledge = load_expert_knowledge()

        return [
            StageConfig(
                name="sample_plan",
                get_descriptor=lambda: f"Optim w. Sample Plan ({query_id})",
                # Stage 1: use the DuckDB sample plan for cardinality / optimizer hints.
                # The current runtime is not yet known, so `_rt` is ignored.
                get_prompt=lambda _rt: optim_prompt_with_sample_plan(
                    query_id=query_id,
                    constraints_str=mandatory_constraints,
                    duckdb_plan=sample_plan,
                    sf=sf,
                ),
            ),
            StageConfig(
                name="trace",
                get_descriptor=lambda: f"Optim w. Trace Stats ({query_id})",
                # Stage 2: use tracing statistics; target 10x improvement.
                get_prompt=lambda rt: optim_prompt_w_trace(
                    query_id=query_id,
                    constraints_str=mandatory_constraints,
                    target_rt_ms=rt / 10,
                    current_rt_ms=rt,
                    sf=sf,
                    factor=10,
                    storage_is_bespoke=self.bespoke_storage,
                ),
                max_turns=125,
            ),
            StageConfig(
                name="expert_knowledge",
                get_descriptor=lambda: f"Optim w. Expert Knowledge ({query_id})",
                # Stage 3: apply domain-expert best practices; target 2x improvement.
                get_prompt=lambda rt: optim_prompt_with_expert_knowledge(
                    query_id=query_id,
                    constraints_str=mandatory_constraints,
                    expert_knowledge=expert_knowledge,
                    current_rt_ms=rt,
                    target_rt_ms=rt / 2,
                    sf=sf,
                    storage_is_bespoke=self.bespoke_storage,
                ),
                max_turns=150,
            ),
            StageConfig(
                name="human_reference",
                get_descriptor=lambda: f"Optim w. Human Reference ({query_id})",
                # Stage 4: final polish in the style of Thomas Neumann / Matthias Jasny.
                get_prompt=lambda rt: optim_prompt_with_human_reference(
                    query_id=query_id,
                    constraints_str=mandatory_constraints,
                    target_rt_ms=rt / 2,
                    current_rt_ms=rt,
                    sf=sf,
                    storage_is_bespoke=self.bespoke_storage,
                ),
                max_turns=125,
            ),
        ]

    async def _run_stage(
        self,
        query_id: str,
        stage: StageConfig,
        pretext_optim: str,
        rt_before_s: float,
    ) -> StageResult:
        """Execute one optimization stage and return its measured outcome."""

        # extract current git snapshot
        current_snapshot = self.git_snapshotter.current_hash
        assert current_snapshot is not None, "Current git snapshot is None."

        # run the LLM optimization loop
        await self._exec(
            pretext_optim
            + "\n"
            + stage.get_prompt(rt_before_s * 1000),  # pass runtime in ms to the prompt
            stage.get_descriptor(),
            max_turns=stage.max_turns,
        )

        try:
            # measure performance after LLM interaction for this stage
            msg, metrics = self.run_tool.run(
                scale_factor=self.benchmark_sf,
                optimize=True,
                query_id=[query_id],
                trace_mode=False,
                external_call=True,
            )

            # assert that implementation is correct
            assert metrics is not None, (
                f"Metrics is None after running stage '{stage.name}' for query {query_id}. Message: {msg}"
            )
            assert metrics["validation/correct"], (
                f"Implementation is not correct after stage '{stage.name}' for query {query_id}. Metrics: {metrics}. {msg}"
            )

            assert metrics is not None
            rt_after_s, _, speedup = extract_speedup_of_last_snapshot(
                metrics, query_id, self.benchmark_sf
            )
            self.query_rt_log[query_id] = rt_after_s
            e = None
        except Exception as e:
            # hit a timeout
            rt_after_s = float("inf")
            speedup = 0.0
            logger.error(
                f"Error while measuring performance after stage '{stage.name}' for query {query_id}: {e}."
            )

        result = StageResult(
            name=stage.name,
            rt_before_s=rt_before_s,
            rt_after_s=rt_after_s,
            speedup_vs_duckdb=speedup,
        )
        if result.improved:
            logger.info(
                f"Query {query_id} | Stage '{stage.name}': "
                f"{rt_before_s:.3f}s -> {rt_after_s:.3f}s "
                f"(improved x{result.improvement_factor:.2f}), "
                f"speedup vs DuckDB: {speedup:.2f}x"
            )
        else:
            logger.info(
                f"Query {query_id} | Stage '{stage.name}': "
                f"{rt_before_s:.3f}s -> {rt_after_s:.3f}s "
                f"(no improvement), "
                f"speedup vs DuckDB: {speedup:.2f}x"
            )

        if result.improved:
            # if we improved, keep the change and continue with the next stage
            pass
        else:
            if self.revert_on_regression:
                # roll back to the state before this stage (discard changes from this stage)
                logger.warning(
                    f"Reverting changes from stage '{stage.name}' for query {query_id} due to no improvement (revert to: {current_snapshot}). Turn: {self.wandb_run_hook.last_turn if self.wandb_run_hook else 'N/A'}"
                )
                self.git_snapshotter.restore(current_snapshot)

                # measure and log performance after the rollback
                out_str, metrics = self.run_tool.run(
                    scale_factor=self.benchmark_sf,
                    optimize=True,
                    query_id=[query_id],
                    trace_mode=False,
                    external_call=True,
                )
                assert metrics is not None, (
                    f"Metrics is None after reverting stage '{stage.name}' for query {query_id}."
                )

                if not metrics["validation/correct"]:
                    logger.warning(
                        f"Reverted stage '{stage.name}' for query {query_id} but the reverted version is not correct ('{out_str}'). This should not happen!"
                    )
                    await self._exec(
                        f"I rolled back your changes since the output was not correct. But after rollback, the results are still wrong ('{out_str}'). Please re-evaluate your implementation of query {query_id} (and also with all queries query_id=None) and make sure that it is correct for all scale-factors!",  # pass runtime in ms to the prompt
                        stage.get_descriptor(),
                        max_turns=stage.max_turns,
                    )
                    # measure and log performance after the rollback
                    _, metrics = self.run_tool.run(
                        scale_factor=self.benchmark_sf,
                        optimize=True,
                        query_id=[query_id],
                        trace_mode=False,
                        external_call=True,
                    )
                    assert metrics is not None, (
                        f"Metrics is None after reverting stage '{stage.name}' for query {query_id}."
                    )

                rt_after_s, _, speedup = extract_speedup_of_last_snapshot(
                    metrics, query_id, self.benchmark_sf
                )
                self.query_rt_log[query_id] = rt_after_s
            else:
                logger.warning(
                    f"Keeping changes from stage '{stage.name}' for query {query_id} despite no improvement."
                )

        return result

    async def run(self) -> Optional[List[str]]:
        # reset used prompts to empty and start from the beginning of the conversation
        self.used = []

        queries_path = "queries.txt"

        # general system prompt
        pretext = optim_prompt_pretext(
            queries_path=queries_path, num_queries=len(self.query_ids)
        )

        # describe the optimization problem
        pretext_optim = optim_prompt_pretext_optim(
            bespoke_storage=self.bespoke_storage,
        )

        # what the agent is allowed to change in the codebase to optimize performance
        mandatory_constraints = optim_prompt_constraints(
            allow_storage_changes=self.bespoke_storage
        )

        # task model with pinning (and explain how to do pinning - refer to helper library from us)
        pinning_prompt = optim_prompt_pinning(core_id=3)

        # ensure initial implementation is correct
        assert await self._check_correctness(self.query_ids, trace_mode=False), (
            "Initial implementation does not produce correct results according to the validation tool. Please fix the implementation until it is correct before starting with optimization."
        )

        # perform pinning
        await self._exec(pretext + "\n" + pinning_prompt, "Pinning")

        # turn on validation and output stdout
        await self._exec(VALIDATE_ON, None)
        await self._exec(VALIDATE_OUTPUT_STDOUT_ON, None)

        # run validation - get initial runtimes
        initial_metrics = dict()
        for sf in self.verify_sf_list + [self.benchmark_sf]:
            msg, metrics = self.run_tool.run(
                scale_factor=sf,
                optimize=True,
                query_id=self.query_ids,
                external_call=True,
            )
            initial_metrics[sf] = metrics

        # add timings collection and statistics gathering to the codebase
        add_timings_prompt = optim_prompt_add_timings()

        # add timing instrumentation in 3-query batches
        for i in range(0, len(self.query_ids), 3):
            qids = self.query_ids[i : min(i + 3, len(self.query_ids))]
            qids_str = ", ".join(qids)
            prompt = optim_prompt_add_timings_per_query(
                qids_str=qids_str,
                refer_to_prev_queries=i > 0,
                scale_factor=self.benchmark_sf,  # use benchmark sf - avoid showing too many repetitions (benchmark sf is executed only once).
            )

            if i == 0:
                full_prompt = add_timings_prompt + "\n" + prompt
            else:
                full_prompt = prompt

            await self._exec(full_prompt, f"Add Timings for Queries {qids_str}")

            # check correctness and feedback to the agent before moving on to the next batch of queries
            await self.check_and_feedback_correctness(qids)

            # do compaction to keep costs low / avoid context full with super many queries
            await self._exec(COMPACTION_MARKER, "compaction")

        # delete result.csv files
        delete_result_csv_files(self.run_tool.cwd)

        # write statistics output to a file for easier access in the future (instead of parsing from stdout every time)
        await self._exec(
            "Instead of writing tracing output to stdout, write it to a file `tracing_output.log`.",
            "Trace->File",
        )

        # optimization loop: work through each stage across all queries
        # (outer loop = stages, inner loop = queries)
        per_query_stages: Dict[
            str, List[StageConfig]
        ] = {}  # query_id -> ordered list of StageConfigs
        per_query_branch: Dict[str, str] = {}

        # num turns in conversation
        last_turn_nr = (await self.session.get_conversation_turns())[-1]["turn"]

        logger.debug(
            f"Collect statistics of initial implementation for all queries ({len(self.query_ids)})."
        )
        for query_id in self.query_ids:
            per_query_stages[query_id] = self._build_stages(
                query_id, mandatory_constraints
            )

            # Switch back to main branch
            await self.session.switch_to_branch("main")

            # create conversation branches for each query
            try:
                per_query_branch[query_id] = await self.session.create_branch_from_turn(
                    last_turn_nr, branch_name=f"query_{query_id}_{last_turn_nr}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to create conversation branch for query {query_id} from turn {last_turn_nr}: {e}"
                )
                logger.error(await self.session.get_conversation_turns())
                raise e

        logger.debug(f"Branches created for all queries: {per_query_branch}")

        num_stages = len(per_query_stages[self.query_ids[0]])

        # clear query rt log
        self.query_rt_log: Dict[str, float] = dict()

        for stage_id in range(num_stages):
            for query_id in self.query_ids:
                # switch to the conversation branch for this query
                await self.session.switch_to_branch(per_query_branch[query_id])

                stage = per_query_stages[query_id][stage_id]

                # collect initial runtime for this query before starting with the first stage of optimization - we will use this as the baseline for measuring improvements in the first stage, and also for calculating speedups vs DuckDB.
                _, metrics = self.run_tool.run(
                    scale_factor=self.benchmark_sf,
                    optimize=True,
                    query_id=[query_id],
                    trace_mode=False,
                    external_call=True,
                )
                _, _ = self.run_tool.run(
                    scale_factor=self.benchmark_sf,
                    optimize=True,
                    query_id=[query_id],
                    trace_mode=True,  # make sure to collect fresh tracing stats for the first stage as well - avoid using stale stats from prior runs that might have been executed in a different order or with different code changes.
                    external_call=True,
                )
                assert metrics is not None

                try:
                    impl_rt_s, _, _ = extract_speedup_of_last_snapshot(
                        metrics,
                        query_id,
                        self.benchmark_sf,
                    )
                    self.query_rt_log[query_id] = impl_rt_s
                except AssertionError as e:
                    logger.warning(
                        f"Failed to extract speedup for query {query_id}: {e}"
                    )
                    # lookup runtime from a past run
                    impl_rt_s = self.query_rt_log[query_id]

                # run the stage - includes automatic reverts if regressions are detected and the revert_on_regression flag is set to True
                await self._run_stage(
                    query_id=query_id,
                    stage=stage,
                    pretext_optim=pretext_optim,
                    rt_before_s=impl_rt_s,
                )

                # delete result.csv files
                delete_result_csv_files(self.run_tool.cwd)

                await self._exec(COMPACTION_MARKER, "compaction")

            # perform full benchmarking across all queries at the end of the stage
            stage_end_msg, stage_end_metrics = self.run_tool.run(
                scale_factor=self.benchmark_sf,
                optimize=True,
                query_id=None,
                trace_mode=False,
                external_call=True,
            )

            # assert stage_end_metrics is not None
            # if not stage_end_metrics["validation/correct"]:
            #     logger.warning(
            #         f"Validation check reported results are incorrect at the end of stage '{stage.name}' after optimizing all queries. This should not happen! Message: {stage_end_msg}"
            #     )
            # #TODO add prompt for this edge case

            #     # perform full benchmarking across all queries at the end of the stage
            #     stage_end_msg, stage_end_metrics = self.run_tool.run(
            #         scale_factor=self.benchmark_sf,
            #         optimize=True,
            #         query_id=None,
            #         trace_mode=False,
            #         external_call=True,
            #     )

        logger.info(f"Final validation metrics after optimization: {stage_end_msg}")

        # TODO: switch back to main conversation branch - or maybe we want to keep the branches for future reference?

        # signal this is the end of the conversation - save the used prompts
        used = await self.ask_to_finish_and_save()

        return used

    async def _exec(
        self,
        prompt: str,
        prompt_descriptor: Optional[str],
        max_turns: Optional[int] = None,
    ) -> str | None:
        # execute the prompt and get the outcome
        user_choice, executed_prompt, last_outcome = await self.process_prompt(
            prompt, prompt_descriptor, max_turns
        )

        if user_choice in ["u", "r"]:
            # consider as executed
            # assert last_outcome is not None, (
            #     "Expected an outcome after executing the prompt."
            # )
            return last_outcome
        else:
            raise Exception(
                f"Unexpected user choice: {user_choice}. Expected 'u' or 'r'."
            )

    async def check_and_feedback_correctness(self, qids: List[str]):
        for tracing_mode in [False, True]:
            attempts = 0
            while True:
                # check correctness
                if not await self._check_correctness(qids, trace_mode=tracing_mode):
                    # incorrect
                    await self._exec(
                        f"Validation check reported results are incorrect (with trace_mode={tracing_mode}, qids={qids}). Please fix the instrumentation to ensure correctness while still collecting the necessary timing information.",
                        f"Fix Correctness (tracing_mode={tracing_mode}, qids={qids})",
                    )
                else:
                    break

                attempts += 1
                if attempts >= 3:
                    raise Exception(
                        f"Validation check still fails after {attempts} attempts to fix it for trace_mode={tracing_mode}, qids={qids}. Please investigate the issue."
                    )

    async def _check_correctness(self, qids: List[str], trace_mode: bool) -> bool:
        _, metrics = self.run_tool.run(
            scale_factor=self.benchmark_sf,
            optimize=True,
            query_id=qids,
            trace_mode=trace_mode,
            external_call=True,
        )
        if metrics is None or not metrics["validation/correct"]:
            logger.error(
                f"Validation check reported results are incorrect (with trace_mode={trace_mode}, qids={qids})."
            )
            return False
        return True


def extract_speedup_of_last_snapshot(
    statistics: Dict, query: str, current_reference_scalefactor: float
):

    assert "validation/scale_factor" in statistics, (
        f"Expected 'validation/scale_factor' in statistics: {statistics.keys()}"
    )
    scale_factor = statistics["validation/scale_factor"]
    assert scale_factor is not None, f"Scale factor in statistics is None: {statistics}"

    # sometimes old runs have slightly different scale factors - adjust runtimes accordingly
    scale_factor_multiplicant = (
        current_reference_scalefactor / scale_factor
    )  # translate runtimes to this scalefactor

    # extract row from statistics
    # prepent with zeros until three chars long
    query_3chars = query.zfill(3)

    impl_key = f"validation/query_{query_3chars}/impl_runtime_ms"
    duckdb_key = f"validation/query_{query_3chars}/duckdb_runtime_ms"

    assert impl_key in statistics, f"Key {impl_key} not found in {statistics.keys()}"
    assert duckdb_key in statistics, (
        f"Key {duckdb_key} not found in {statistics.keys()}"
    )

    impl_runtime_ms = float(statistics[impl_key])
    duckdb_runtime_ms = float(statistics[duckdb_key])

    last_impl_rt = (
        impl_runtime_ms / 1000 / scale_factor_multiplicant
    )  # translate runtime to seconds and adjust for scale factor if needed
    duckdb_rt = (
        duckdb_runtime_ms / 1000 / scale_factor_multiplicant
    )  # translate runtime to seconds and adjust for scale factor if needed

    # calculate speedup
    speedup = duckdb_rt / last_impl_rt if last_impl_rt > 0 else float("inf")

    return last_impl_rt, duckdb_rt, speedup


def delete_result_csv_files(workspace_path: Path):
    # delete all .csv files from prior runs
    csv_files = list(workspace_path.rglob("result*.csv"))
    logger.info(f"Deleting existing result-csv files ({len(csv_files)} files).")
    for csv_file in csv_files:
        csv_file.unlink()

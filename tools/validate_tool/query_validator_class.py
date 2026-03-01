import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from agents import custom_span

from llm_cache import utils
from tools.validate_tool.duckdb_connection_manager import DuckDBConnectionManager
from tools.validate_tool.query_cache import QueryCache, QueryInstantiation
from tools.validate_tool.run_and_check_queries import (
    assemble_error,
    assemble_exec,
    check_output_correctness,
)
from tools.validate_tool.validate_cache_type import (
    ExtendedValidateCacheType,
)

logger = logging.getLogger(__name__)

PIN_CORE = 3


class QueryValidator:
    ############
    # WARNING: add all call args to cache hash to ensure correct cache hits. If you change the call args, old cache entries will not be hit anymore and validation results will not be replayed from cache anymore until new cache entries for the new call args are generated.
    ############

    def __init__(
        self,
        benchmark: str,
        gen_query_fn: Callable,
        sf_list: List[float],
        parquet_path: str,
        wandb_pin_worker: bool,
        all_query_ids: List[str],
        num_random_query_instantiations: int,
        query_cache_dir: Path,
        validate_cache_dir: Path,
        workspace_path: Path,
        git_snapshotter: Optional[Any] = None,
        output_stdout_stderr: bool = False,  # whether to include stdout and stderr in the validation result message also in case of correct validation (not only in case of errors)
    ):
        self.benchmark = benchmark
        self.sf_list = sf_list
        self.all_query_ids = all_query_ids
        self.workspace_path = workspace_path
        self.query_cache_dir = query_cache_dir
        self.validate_cache_dir = validate_cache_dir
        self.num_random_query_instantiations = num_random_query_instantiations
        self.git_snapshotter = git_snapshotter
        self.wandb_pin_worker = wandb_pin_worker
        self.output_stdout_stderr = output_stdout_stderr

        # Create DuckDB connection managers for each scale factor
        self.duckdb_con: Dict[float, DuckDBConnectionManager] = dict()

        for sf in sf_list:
            self.duckdb_con[sf] = DuckDBConnectionManager(
                benchmark=benchmark,
                pre_load_duckdb_tables=True,
                parquet_path=parquet_path,
                sf=sf,
                pin_worker=wandb_pin_worker,
                pin_core=PIN_CORE,
            )

        # Pre-generate all query instantiations and execute them with DuckDB
        # Results are cached in the QueryCache for efficient validation
        logger.info("Initializing query cache with pre-generated instantiations...")
        self.query_cache = QueryCache(
            gen_query_fn=gen_query_fn,
            query_ids=self.all_query_ids,
            sf_list=sf_list,
            num_instantiations_per_query=self.num_random_query_instantiations,
            duckdb_managers=self.duckdb_con,
            cache_dir=self.query_cache_dir,
        )

        self.validate_cache_dir.mkdir(parents=True, exist_ok=True)

    def exec_and_validate(
        self,
        exec_callback_fn: Callable,
        scale_factor: float,
        query_id: Optional[List[str]],
        compile_key_hash: str,
        trace_mode: bool,
        other_config: Dict[str, Any] = {},
        skip_validate: bool = False,
        only_from_cache: bool = False,
    ) -> Tuple[str, bool, Dict[str, Any], bool]:
        with custom_span(
            f"exec_and_validate ({query_id if query_id is not None else 'all queries'}, sf={scale_factor}, trace_mode={trace_mode}, {'no-validate' if skip_validate else ''})",
        ):
            # in trace mode do not execute multiple times
            if scale_factor == self.sf_list[-1] and not trace_mode:
                # average runtime for largest scale factor can be long, so use more repetitions to increase confidence in validation result
                repetitions = 3
            else:
                repetitions = 1

            # approximate a timeout
            timeout = approx_timeout_for_validation(
                scale_factor=scale_factor,
                num_queries=len(query_id)
                if query_id is not None
                else len(self.all_query_ids),
                repetitions=repetitions,
                num_random_query_instantiations=self.num_random_query_instantiations,
            )
            logger.debug(f"Run with timeout: {timeout} seconds")

            result, cache_path = self._check_answer_from_cache(
                query_id=query_id,
                scale_factor=scale_factor,
                skip_validate=skip_validate,
                other_config=other_config,
                stop_on_first_error=True,
                timeout=timeout,
                compile_key_hash=compile_key_hash,
                repetitions=repetitions,
            )

            replayed_from_cache = False
            if result is not None:
                msg, success, metrics = result
                replayed_from_cache = True

                # fallback for old cache state where query_ids_executed was not stored - extract from query cache
                if "validation/query_ids_executed" not in metrics:
                    exec_list = list(
                        query_id if query_id is not None else self.all_query_ids
                    )
                    metrics["validation/query_ids_executed"] = exec_list

                    logger.debug(f"Log: {exec_list} / query id: {query_id}")

            else:
                if only_from_cache:
                    raise Exception(
                        f"Validation result not found in cache for key {compile_key_hash} and only_from_cache is set. Cache path: {cache_path}"
                    )

                # check query-IDs are existing
                all_found = True
                for q_id in query_id or []:
                    if q_id not in self.all_query_ids:
                        all_found = False
                        msg = f"Error: query_id {q_id} not recognized. Known query IDs: {self.all_query_ids}"
                        logger.error(msg)

                        success = False
                        metrics = assemble_error(
                            scale_factor=scale_factor,
                            query_ids_executed=[],
                            exception=True,
                        )
                        break
                if all_found:
                    # get query instantiations and convert to arg list
                    args_list, instantiations, num_queries = (
                        self._get_instantiations_and_convert_to_arg_list(
                            scale_factor=scale_factor,
                            query_id=query_id,
                            repetitions=repetitions,
                        )
                    )

                    # execute queries via callback
                    _resp, out, err = exec_callback_fn(args_list, timeout_s=timeout)

                    if not skip_validate:
                        # validate output
                        msg, success, metrics = self._validate_query(
                            instantiations=instantiations,
                            scale_factor=scale_factor,
                            resp=_resp,
                            stdout=out,
                            stderr=err,
                            cmd=None,
                            stop_on_first_error=True,
                        )
                    else:
                        logger.warning(
                            f"Skipping correctness validation as requested ({query_id=}, {scale_factor=})"
                        )
                        msg = f"stdout: {out.rstrip()}\nstderr: {err.rstrip()}\n{_resp}"
                        success = True
                        metrics = assemble_exec(
                            scale_factor=scale_factor, num_queries_executed=num_queries
                        )
                else:
                    instantiations = []

                metrics["validation/repetitions"] = repetitions
                metrics["validation/instantiations"] = len(instantiations) / repetitions

                # write to cache
                if cache_path is not None:
                    utils.dump_pickle(
                        cache_path,
                        ExtendedValidateCacheType(
                            outputs=msg, success=success, metrics=metrics
                        ),
                    )
                    logger.debug(f"Saved validation result to cache: {cache_path}")

        logger.info(
            f"Validate Tool Result: {'correct' if success else 'incorrect'} (Query ID: {query_id}, Scale Factor: {scale_factor}, Replayed from cache: {replayed_from_cache})"
        )

        # truncate msg if too long for logging
        if len(msg) > 5000:
            shortened_msg = msg[:5000] + "...(truncated)"
        else:
            shortened_msg = msg

        with custom_span(
            f"exec_and_validate [result] ({'correct' if success else 'incorrect'}, {'replayed from cache' if replayed_from_cache else ''})",
            {
                "result": shortened_msg,
                "sql": query_id,
                "git snapshot": self.git_snapshotter.current_hash
                if self.git_snapshotter is not None
                else None,
            },
        ):
            return msg, success, metrics, replayed_from_cache

    def _check_answer_from_cache(
        self,
        query_id: Optional[List[str]],
        scale_factor: float,
        skip_validate: bool,
        other_config: Dict[str, Any],
        stop_on_first_error: bool,
        timeout: int,
        compile_key_hash: str,
        repetitions: int,
    ) -> Tuple[Any, Optional[Path]]:
        if self.git_snapshotter is not None:
            hash_payload = {
                "snapshotter_hash": self.git_snapshotter.current_hash,
                "query_id": query_id,
                "scale_factor": scale_factor,
                "skip_validate": skip_validate,
                "stop_on_first_error": stop_on_first_error,
                "wandb_pin_worker": self.wandb_pin_worker,
                "wandb_pin_core": PIN_CORE,
                "num_random_query_instantiations": self.num_random_query_instantiations,
                "output_stdout": self.output_stdout_stderr,
                "timeout": timeout,
                "compile_key_hash": compile_key_hash,
                "repetitions": repetitions,
                **other_config,
            }

            hash = utils.sha256(utils.stable_json(hash_payload))

            if self.validate_cache_dir is None:
                cache_path = None
            else:
                cache_path = _cache_path_for_hash(self.validate_cache_dir, hash)

            # check validation-tool cache - replay validation result from val-tool cache if available
            if cache_path is not None and cache_path.exists():
                cached: Optional[ExtendedValidateCacheType] = utils.load_pickle(
                    cache_path, ExtendedValidateCacheType
                )
                assert cached is not None
                logger.debug(f"Loaded validation result from cache: {cache_path}")

                return (cached.outputs, cached.success, cached.metrics), cache_path
            else:
                # logger.info(f"No matching validation-tool cache found at {cache_path=}")
                pass

        else:
            logger.warning(
                "I don't know the current code version because GitSnapshotter is None. Hence I can't search for matching validation-tool cache."
            )
            cache_path = None

        return None, cache_path

    def _get_queries_executed_from_queryid_arg(
        self, query_id: Optional[List[str]]
    ) -> List[str]:
        if isinstance(query_id, list):
            filtered_queries = query_id
        elif query_id is None:
            filtered_queries = self.all_query_ids
        else:
            raise ValueError(
                f"Unexpected query_id type: {type(query_id)}. Expected list or None."
            )

        assert len(filtered_queries) > 0
        return filtered_queries

    def _get_instantiations(
        self,
        scale_factor: float,
        query_id: Optional[List[str]],
    ) -> Tuple[List[QueryInstantiation], int]:
        # determine which queries to execute
        executed_queries = self._get_queries_executed_from_queryid_arg(query_id)

        assert scale_factor in self.sf_list, (
            f"Scale factor {scale_factor} not in configured list."
        )

        # Sample query instantiations from cache
        instantions = self.query_cache.get_instantiations(
            scale_factor=scale_factor,
            query_id=executed_queries,
        )

        assert len(instantions) > 0
        return instantions, len(executed_queries)

    def _get_instantiations_and_convert_to_arg_list(
        self,
        scale_factor: float,
        query_id: Optional[List[str]],
        repetitions: int,
    ) -> Tuple[List[str], List[QueryInstantiation], int]:

        instantiations, num_queries = self._get_instantiations(
            scale_factor=scale_factor,
            query_id=query_id,
        )

        # Prepare arguments for implementation
        args_list = format_args_string(
            query_list=[inst.query_id for inst in instantiations],
            placeholder_list=[inst.placeholders for inst in instantiations],
        )

        # add repetitions to args list if repetitions > 1
        repeated_args_list = []
        repeated_instantiations = []
        for arg, inst in zip(args_list, instantiations):
            repeated_args_list.extend([arg] * repetitions)
            repeated_instantiations.extend([inst] * repetitions)

        return repeated_args_list, repeated_instantiations, num_queries

    def _validate_query(
        self,
        instantiations: List[QueryInstantiation],
        resp: str,
        stdout: str,
        stderr: str,
        scale_factor: float,
        cmd: Optional[str],
        stop_on_first_error: bool = True,
    ) -> Tuple[str, bool, Dict[str, Any]]:
        # parse output
        parsed_output = _parse_output(stdout, stderr, resp)

        query_ids_executed = sorted(
            list(set([inst.query_id for inst in instantiations]))
        )

        if isinstance(parsed_output, str):
            # Error in parse output: forward error message
            return (
                parsed_output,
                False,
                assemble_error(
                    scale_factor, query_ids_executed=query_ids_executed, exception=True
                ),
            )

        # successful parse
        ingest_time_ms, measurements = parsed_output

        from_cmd_str = "" if cmd is None else f" from command: {cmd}"

        if len(measurements) == 0:
            return (
                f"Error: unexpected output format{from_cmd_str}\nOutput:\n{stdout}. Expected stdout containing only: Ingest ms: <num> | Execution ms: <num>\n EOF",
                False,
                assemble_error(
                    scale_factor, query_ids_executed=query_ids_executed, exception=True
                ),
            )

        if len(measurements) != len(instantiations):
            return (
                f"Error: unexpected number of measurements{from_cmd_str}. Parsed {len(measurements)} timing lines but passed {len(instantiations)} query instantiations.",
                False,
                assemble_error(
                    scale_factor, query_ids_executed=query_ids_executed, exception=True
                ),
            )

        # validate with duckdb
        return check_output_correctness(
            scale_factor=scale_factor,
            impl_ingest_time_ms=ingest_time_ms,
            instantiations=instantiations,
            measurements=measurements,
            out_path=self.workspace_path,
            cmd=cmd,
            stop_on_first_error=stop_on_first_error,
            all_query_ids=self.all_query_ids,
            stdout=stdout if self.output_stdout_stderr else None,
            stderr=stderr if self.output_stdout_stderr else None,
        )


def _parse_output(
    stdout: str,
    stderr: str,
    resp: str,
    expect_ingest_time: bool = False,
) -> Tuple[Optional[float], List[Tuple[str, str]]] | str:
    # ./db BRAZIL FRANCE
    # Ingest ms: 5140 | Execution ms: 218
    lines = stdout.strip().split("\n")

    # search for ingest line matching "Ingest ms: <num>"
    ingest_lines = [line for line in lines if line.startswith("Ingest ms:")]
    if expect_ingest_time:
        if len(ingest_lines) == 0:
            return (
                "Error: no ingest line found in program stdout. "
                "Expected line like: 'Ingest ms: <num>'.\n"
                + f"STDERR:\n{stderr}\nSTDOUT:\n{stdout}\nResp:\n{resp}"
            )

    # search for line matching "<run> | Execution ms: <num>"
    timing_lines = [
        line for line in lines if line.count("|") == 1 and "Execution ms:" in line
    ]

    if len(timing_lines) == 0:
        return (
            "Error: no timing lines found in program stdout. "
            "Expected lines like: '<run> | Execution ms: <num>'.\n"
            + f"STDERR:\n{stderr}\nSTDOUT:\n{stdout}\nResp:\n{resp}"
        )
    if expect_ingest_time:
        if len(ingest_lines) > 1:
            return (
                "Error: multiple ingest lines found in program stdout. "
                "Expected only one line like: 'Ingest ms: <num>'.\n"
                + f"STDERR:\n{stderr}\nSTDOUT:\n{stdout}\nResp:\n{resp}"
            )

    if len(ingest_lines) == 1:
        # parse ingest time
        ingest_time_ms_str = ingest_lines[0].strip()
        ingest_time_ms_str = (
            ingest_time_ms_str[len("Ingest ms:") :].strip().strip(":").strip()
        )
        ingest_time_ms = float(ingest_time_ms_str)
    else:
        ingest_time_ms = None

    measurements = []
    for timing_line in timing_lines:
        query_name, exec_time = timing_line.split("|")

        query_name = query_name.strip()
        exec_time = exec_time.strip()

        assert exec_time.startswith("Execution ms:"), (
            f"Unexpected exec time format: \"{exec_time}\" Expected to start with 'Execution ms:'"
        )
        exec_time = exec_time[len("Execution ms:") :].strip().strip(":").strip()

        if not query_name.isdigit():
            return (
                "Error: timing line run number is not an integer.\n"
                + f"Bad line: {timing_line}\nSTDERR:\n{stderr}\nSTDOUT:\n{stdout}\nResp:\n{resp}"
            )

        measurements.append((query_name, exec_time))

    return ingest_time_ms, measurements


def _cache_path_for_hash(validate_cache_dir: Path, hash: str) -> Path:
    return validate_cache_dir / f"{hash}.pkl"


# separate args by , and add double quotes around each arg (except for IN lists which start with '(')
def format_args_string(
    query_list: List[str], placeholder_list: List[Dict[str, Any]]
) -> List[str]:
    args_list = []
    for qid_str, placeholders in zip(query_list, placeholder_list):
        # Don't add double quotes to IN lists (they start with '(')
        tmp_vals = []
        for v in placeholders.values():
            if isinstance(v, str) and v.startswith("("):
                # IN list - don't add quotes
                tmp_vals.append(v)
            else:
                # Regular value - add quotes
                tmp_vals.append(f'"{v}"')
        tmp_vals_str = " ".join(tmp_vals)
        args_list.append(f"{qid_str} {tmp_vals_str}")
    return args_list


def approx_timeout_for_validation(
    scale_factor: float,
    num_queries: int,
    repetitions: int,
    num_random_query_instantiations: int,
) -> int:
    # approximate a timeout for validation based on scale factor and number of queries
    timeout = (
        scale_factor * num_queries * 2 * num_random_query_instantiations * repetitions
    )  # 2 seconds per query with sf=1 as a rough estimate, can be adjusted as needed
    timeout = max(timeout, 120)  # at least 1 minute total timeout
    timeout = min(
        timeout, 1200
    )  # at most 20 minutes total timeout - for sf100 or similar this might take long

    # round up to minutes
    timeout = ((timeout + 59) // 60) * 60

    return int(timeout)

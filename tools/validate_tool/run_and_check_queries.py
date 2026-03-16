import logging
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Optional, Tuple

import pandas as pd

import wandb
from tools.validate_tool.query_cache import QueryInstantiation
from utils.wandb_plots_gen import create_wandb_speedup_plot

logger = logging.getLogger(__name__)


def assemble_error(
    scale_factor: float,
    query_ids_executed: List[str],
    exception: bool = True,
    query_id: Optional[str] = None,
) -> Dict:
    # assemble default failed metrics
    return {
        "validation/scale_factor": scale_factor,
        "validation/correct": False,
        "validation/error": exception,
        "validation/query_ids_executed": query_ids_executed,
        "validation/num_queries": len(query_ids_executed),
        "validation/num_successful_queries": 0,
        "validation/failed_query_id": query_id,
    }


def assemble_exec(scale_factor: float, num_queries_executed: int) -> Dict:
    # assemble default successful metrics without correctness info
    return {
        "validation/scale_factor": scale_factor,
        "validation/num_queries": num_queries_executed,
    }


def check_output_correctness(
    scale_factor: float,
    impl_ingest_time_ms: Optional[float],
    instantiations: List[QueryInstantiation],
    measurements: List[Tuple[str, str]],
    out_path: Path,
    cmd: Optional[str],
    stop_on_first_error: bool,
    all_query_ids: List[str],
    stdout: Optional[str],
    stderr: Optional[str],
):
    logger.info(f"Comparing results with DuckDB for SF{scale_factor}...")

    # retrieve query ids executed from instantiations
    query_ids_executed_set = set()
    for inst in instantiations:
        query_ids_executed_set.add(inst.query_id)
    query_ids_executed: List[str] = sorted(list(query_ids_executed_set))

    # collect the runtimes for each query
    duckdb_rt_lists: DefaultDict[str, List] = defaultdict(list)
    impl_rt_lists: DefaultDict[str, List] = defaultdict(list)

    log_collector = ""

    # compare with duckdb output
    if impl_ingest_time_ms is not None:
        logger.info(f"Impl. Ingest time: {impl_ingest_time_ms}ms")
    for i, (inst, rt) in enumerate(zip(instantiations, measurements)):
        run_nr, impl_exec_time = rt
        assert int(run_nr) == i + 1, (
            f"Unexpected run nr {run_nr} in line {i}, expected {i + 1}"
        )

        # Get cached DuckDB result
        duckdb_df = inst.duckdb_result
        duckdb_time = inst.duckdb_exec_time_ms

        duckdb_rt_lists[inst.query_id].append(duckdb_time)

        # write df to csv and read back in - ensure consistent formatting
        duckdb_df.to_csv(out_path / "duckdb_result.csv", index=False)
        duckdb_df = pd.read_csv(out_path / "duckdb_result.csv")

        # remove duckdb_result.csv
        (out_path / "duckdb_result.csv").unlink()

        # log times
        impl_exec_time = float(impl_exec_time.strip())
        faster = impl_exec_time < duckdb_time

        logger.info(
            f"Q{inst.query_id} (SF={scale_factor}): {impl_exec_time}ms (Bespoke), {duckdb_time:.2f}ms (DuckDB) {'-- faster' if faster else ''}"
        )
        impl_rt_lists[inst.query_id].append(impl_exec_time)

        # check that result was produced
        filename = f"result{i + 1}.csv"
        res_path = out_path / filename
        if not res_path.exists():
            return (
                f"Error: {res_path} not found after executing command: {cmd}",
                False,
                assemble_error(
                    scale_factor=scale_factor,
                    query_ids_executed=query_ids_executed,
                    exception=True,
                    query_id=inst.query_id,
                ),
            )

        # load result from csv into dataframe
        try:
            impl_df = pd.read_csv(
                res_path,
                header=0,
                delimiter=",",
                escapechar="\\",
                quotechar='"',
                doublequote=True,
            )
        except Exception as e:
            return (
                f"Error: failed to read result CSV {filename} into DataFrame: {e}",
                False,
                assemble_error(
                    scale_factor=scale_factor,
                    query_ids_executed=query_ids_executed,
                    exception=True,
                    query_id=inst.query_id,
                ),
            )

        # check equality of columns
        if set(duckdb_df.columns) != set(impl_df.columns):
            output = (
                f"Result columns do not match for query {inst.query_id} "
                f"with placeholders: {inst.placeholders}\n(SQL: {inst.sql})\n"
                f"DuckDB columns: {duckdb_df.columns.tolist()}\n"
                f"Implementation columns: {impl_df.columns.tolist()}\n"
            )
            if stop_on_first_error:
                return (
                    output,
                    False,
                    assemble_error(
                        scale_factor=scale_factor,
                        query_ids_executed=query_ids_executed,
                        exception=False,
                        query_id=inst.query_id,
                    ),
                )
            else:
                logger.error(output)
                log_collector += output + "\n"

        # check row-content using set semantic (i.e., ignore ordering)
        duckdb_df_sorted = duckdb_df.sort_values(
            by=list(duckdb_df.columns)
        ).reset_index(drop=True)
        impl_df_sorted = impl_df.sort_values(by=list(impl_df.columns)).reset_index(
            drop=True
        )

        try:
            pd.testing.assert_frame_equal(
                duckdb_df_sorted,
                impl_df_sorted,
                check_dtype=False,
                check_column_type=False,
                check_index_type=False,
                # give 1% tolerance for float columns
                atol=1e-2,
                rtol=1e-2,
            )
        except AssertionError as e:
            output = (
                f"Results do not match for query {inst.query_id} "
                f"with placeholders: {inst.placeholders}\n(SQL: {inst.sql}). "
                "Checking with set-semantic and rows are not equal.\n"
                f"DuckDB result:\n{duckdb_df_sorted}\n"
                f"Implementation result:\n{impl_df_sorted}\n"
                f"Assert Dataframe Equal Error:\n{e}\n"
            )
            if stop_on_first_error:
                return (
                    output,
                    False,
                    assemble_error(
                        scale_factor=scale_factor,
                        query_ids_executed=query_ids_executed,
                        exception=False,
                        query_id=inst.query_id,
                    ),
                )
            else:
                logger.error(output)
                log_collector += output + "\n"

        # Check that ordering constraints are obeyed
        if inst.order_by_info is not None and len(inst.order_by_info) > 0:
            sort_cols = [col for col, _ in inst.order_by_info]

            # perform col rewrites
            rewritten = []
            for c in sort_cols:
                if c.lower() == "count(*)":
                    rewritten.append("count_star()")
                else:
                    rewritten.append(c)
            sort_cols = rewritten

            # ensure all sort cols are present
            for c in sort_cols:
                assert c in duckdb_df.columns, (
                    f"ORDER BY column {c} not in DuckDB result {duckdb_df.columns.tolist()}\n{inst.sql}\n{inst.placeholders}"
                )

            try:
                pd.testing.assert_frame_equal(
                    duckdb_df[sort_cols],
                    impl_df[sort_cols],
                    check_dtype=False,
                    check_column_type=False,
                    check_index_type=False,
                    atol=1e-2,
                    rtol=1e-2,
                )
            except AssertionError as e:
                output = (
                    f"Ordering constraints violated for query {inst.query_id} "
                    f"with placeholders: {inst.placeholders}\n(SQL: {inst.sql})\n"
                    f"Expected ORDER BY: {inst.order_by_info}\n"
                    f"DuckDB result:\n{duckdb_df[sort_cols]}\n"
                    f"Implementation result:\n{impl_df[sort_cols]}\n"
                    f"Assert Dataframe Equal Error:\n{e}\n"
                )
                if stop_on_first_error:
                    return (
                        output,
                        False,
                        assemble_error(
                            scale_factor=scale_factor,
                            query_ids_executed=query_ids_executed,
                            exception=False,
                            query_id=inst.query_id,
                        ),
                    )
                else:
                    logger.error(output)
                    log_collector += output + "\n"

    if log_collector != "":
        return (
            log_collector,
            False,
            assemble_error(
                scale_factor=scale_factor,
                query_ids_executed=query_ids_executed,
                exception=False,
            ),
        )

    # Compute aggregate statistics
    avg_duckdb_rts = dict()
    avg_impl_rts = dict()

    assert len(query_ids_executed) > 0, "No queries to validate"
    assert len(impl_rt_lists) > 0, "No runtimes recorded"

    for query in query_ids_executed:
        q = str(query)
        if q not in duckdb_rt_lists or q not in impl_rt_lists:
            return (
                f"Error: missing runtime measurements for query {q}. "
                f"DuckDB keys: {sorted(duckdb_rt_lists.keys())}, impl keys: {sorted(impl_rt_lists.keys())}",
                False,
                assemble_error(
                    scale_factor=scale_factor,
                    query_ids_executed=query_ids_executed,
                    exception=False,
                    query_id=q,
                ),
            )

        if len(duckdb_rt_lists[q]) == 0 or len(impl_rt_lists[q]) == 0:
            return (
                f"Error: no runtime measurements recorded for query {q}. "
                f"DuckDB runtimes: {duckdb_rt_lists[q]}, impl runtimes: {impl_rt_lists[q]}",
                False,
                assemble_error(
                    scale_factor=scale_factor,
                    query_ids_executed=query_ids_executed,
                    exception=False,
                    query_id=q,
                ),
            )

        avg_duckdb_rt = sum(duckdb_rt_lists[q]) / len(duckdb_rt_lists[q])
        avg_impl_rt = sum(impl_rt_lists[q]) / len(impl_rt_lists[q])
        avg_duckdb_rts[q] = avg_duckdb_rt
        avg_impl_rts[q] = avg_impl_rt

    # compute total runtimes, total speedup, average speedup
    total_duckdb_rt = sum(avg_duckdb_rts.values())
    total_impl_rt = sum(avg_impl_rts.values())
    total_speedup = (
        total_duckdb_rt / total_impl_rt if total_impl_rt > 0 else float("inf")
    )
    average_speedup = sum(
        (
            avg_duckdb_rts[q] / avg_impl_rts[q]
            for q in avg_duckdb_rts
            if avg_impl_rts[q] > 0
        )
    ) / len(avg_duckdb_rts)

    logger.info(
        f"Aggregated Runtimes: {total_impl_rt:.2f}ms (Bespoke) vs {total_duckdb_rt:.2f}ms (DuckDB)"
    )
    logger.info(f"Avg. Speedup: {average_speedup:.2f}x")
    logger.info(f"Total Speedup: {total_speedup:.2f}x")

    # Report metrics to wandb if callback is set

    metrics = {
        "validation/scale_factor": scale_factor,
        "validation/correct": True,
        "validation/error": False,
        "validation/total_duckdb_runtime_ms": total_duckdb_rt,
        "validation/total_impl_runtime_ms": total_impl_rt,
        "validation/total_speedup": total_speedup,
        "validation/avg_speedup": average_speedup,
        "validation/num_queries": len(query_ids_executed),
        "validation/num_successful_queries": len(query_ids_executed),
        "validation/query_ids_executed": query_ids_executed,
    }

    if impl_ingest_time_ms is not None:
        metrics["validation/ingest_time_ms"] = impl_ingest_time_ms

    if len(query_ids_executed) == len(all_query_ids):
        metrics["validation/all_queries"] = True
        metrics[f"validation/sf{scale_factor}_all_queries_avg_speedup"] = (
            average_speedup
        )
        metrics[f"validation/sf{scale_factor}_all_queries_total_speedup"] = (
            total_speedup
        )

        # prepare to log full speeedups table
        measurements_df = pd.DataFrame(
            {
                "query_id": query_ids_executed,
                "duckdb_runtime_ms": [
                    avg_duckdb_rts[str(q)] for q in query_ids_executed
                ],
                "impl_runtime_ms": [avg_impl_rts[str(q)] for q in query_ids_executed],
                "speedup": [
                    avg_duckdb_rts[str(q)] / avg_impl_rts[str(q)]
                    if avg_impl_rts[str(q)] > 0
                    else float("inf")
                    for q in query_ids_executed
                ],
            }
        )
        t = wandb.Table(dataframe=measurements_df)
        metrics[f"validation/sf{scale_factor}_all_queries_data"] = t
        metrics[f"validation/sf{scale_factor}_all_queries_plot"] = (
            create_wandb_speedup_plot(t, scale_factor)
        )

    # Add per-query metrics
    for q in query_ids_executed:
        # make q 3-digit string with leading zeros
        q_str = str(q)
        q_3d_str = q_str.zfill(3)

        metrics[f"validation/query_{q_3d_str}/duckdb_runtime_ms"] = avg_duckdb_rts[
            q_str
        ]
        metrics[f"validation/query_{q_3d_str}/impl_runtime_ms"] = avg_impl_rts[q_str]
        metrics[f"validation/query_{q_3d_str}/speedup"] = (
            avg_duckdb_rts[q_str] / avg_impl_rts[q_str]
            if avg_impl_rts[q_str] > 0
            else float("inf")
        )

    # Parse ingest time if available
    if impl_ingest_time_ms is not None:
        metrics["validation/ingest_time_ms"] = impl_ingest_time_ms

    result_message = ""
    if stdout is not None or stderr is not None:
        result_message += f"STDOUT:\n{stdout}\n"
        result_message += f"STDERR:\n{stderr}\n"
        result_message += "BENCHMARK AND VALIDATION RESULTS:\n"

    result_message += (
        "All results match!\n"
        + f"DuckDB runtimes (ms): {', '.join([f'Query {q}: {r:.2f}' for q, r in avg_duckdb_rts.items()])} - sum: {sum(avg_duckdb_rts.values()):.2f}\n"
        + f"Your Implementation runtimes (ms): {', '.join([f'Query {q}: {r:.2f}' for q, r in avg_impl_rts.items()])} - sum: {sum(avg_impl_rts.values()):.2f}\n"
    )

    if impl_ingest_time_ms is not None:
        result_message += f"Ingest time (ms): {impl_ingest_time_ms}\n"

    return result_message, True, metrics

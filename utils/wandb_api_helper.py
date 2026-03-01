import logging
from typing import Any, Dict, Optional, Tuple

import wandb

logger = logging.getLogger(__name__)


def wandb_retrieve_metrics_for_run(
    benchmark: str,
    run_id: str,
    entity: str = "learneddb",
    project: str = "bespoke-olap-agents",
    output_hist: bool = False,
    fetch_latest_runtimes: bool = False,
) -> Tuple[Dict, Optional[Any]]:
    api = wandb.Api()
    run = api.run(f"{entity}/{project}/{run_id}")

    run_name = run.name
    assert benchmark in run_name, f"Expected benchmark name in run name, got {run_name}"

    summary = run.summary

    # extract last commit hash
    last_commit_hash = summary.get("current_hash", "N/A")

    # extract scale factor from history
    scale_factors_used = (
        run.history(keys=["validation/scale_factor"]).dropna().reset_index(drop=True)
    )

    if fetch_latest_runtimes:
        max_scale_factor = int(scale_factors_used["validation/scale_factor"].max())
        logger.info(f"Fetching latest runtimes for scale factor {max_scale_factor}...")
        # get runtimes table
        table_art_list = []
        for artifact in run.logged_artifacts():
            name = artifact.name

            if f"sf{max_scale_factor}_all_queries_data" not in name:
                continue
            table_art_list.append(artifact)

        assert len(table_art_list) > 0, (
            f"No speedup measurements found for scale factor {max_scale_factor} in run {run_id} / {run_name}"
        )

        # get most recent version
        table_art_list.sort(key=lambda x: x.created_at, reverse=True)
        latest_art = table_art_list[0]

        table = latest_art.get(
            "validation/sf" + str(max_scale_factor) + "_all_queries_data"
        )

        runtimes_df = table.get_dataframe()
    else:
        runtimes_df = None
        max_scale_factor = None

    out = {
        "last_commit_hash": last_commit_hash,
        "scalefactor": max_scale_factor,
        "query_runtimes": runtimes_df,
    }

    if output_hist:
        return out, run.history(samples=10000)

    return out, None


# if __name__ == "__main__":
#     run_id = "qhljecm3"
#     print(wandb_retrieve_metrics_for_run(run_id))

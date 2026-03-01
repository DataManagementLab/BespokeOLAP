from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from llm_cache import utils


def get_wandb_stats(
    run_id: str,
    entity: str = "jwehrstein",
    project: str = "BespokeOLAP",
    samples: int = 10000,
    skip_cache: bool = False,
    wandb_run_cache_path: Optional[Path] = None,
):
    """
    Fetch W&B run data with error handling.

    Note: wandb is imported on first use to avoid compatibility issues.

    Args:
        run_id: W&B run ID
        entity: W&B entity/workspace name
        project: W&B project name
        samples: Number of history samples to retrieve

    Returns:
        Tuple of (summary_dict, history_dataframe)
    """

    hash_payload = {"entity": entity, "project": project, "run_id": run_id}
    hash = utils.sha256(utils.stable_json(hash_payload))
    if wandb_run_cache_path is None:
        cache_path_summary = None
        cache_path_history = None
        cache_path_config = None
    else:
        # create cache dir if needed
        wandb_run_cache_path.mkdir(parents=True, exist_ok=True)
        cache_path_summary, cache_path_history, cache_path_config = (
            _cache_path_for_hash(wandb_run_cache_path, hash)
        )

    # check compile cache - replay compile result from cache if available
    if (
        not skip_cache
        and cache_path_summary is not None
        and cache_path_summary.exists()
    ):
        assert cache_path_history is not None
        assert cache_path_config is not None
        summary = utils.load_pickle(cache_path_summary, expected=dict)
        history = utils.load_pickle(cache_path_history, expected=pd.DataFrame)
        config = utils.load_pickle(cache_path_config, expected=dict)

        print(f"Loaded wandb data from cache: {cache_path_summary}")

        return summary, history, config

    try:
        import wandb

        api = wandb.Api()
        run = api.run(f"{entity}/{project}/{run_id}")

        print(f"✓ Run loaded: {run.name}")
        print(f"  State: {run.state}")
        print(f"  Created: {run.created_at}")

        summary = dict(run.summary)
        history = run.history(samples=samples)
        config = run.config

        print(f"✓ Data fetched: {len(history)} turns, {len(history.columns)} columns")

        # store output in cache
        if cache_path_summary is not None:
            for key in ["_wandb"]:
                summary.pop(key, None)  # remove non-serializable entry

            for key, value in summary.items():
                # check if artifact reference
                if hasattr(value, "path"):
                    # overwrite with path str
                    value = str(value.path)
                    summary[key] = value

            utils.dump_pickle(cache_path_summary, summary)
            print(f"✓ W&B data cached to: {cache_path_summary}")

            # use pandas to_pickle for history
            history.to_pickle(cache_path_history)

            assert cache_path_config is not None
            utils.dump_pickle(cache_path_config, dict(config))

        return summary, history, config

    except Exception as e:
        print(f"✗ Error loading W&B data: {e}")
        # return summary, history
        raise e


def _cache_path_for_hash(cache_dir: Path, hash: str) -> Tuple[Path, Path, Path]:
    return (
        cache_dir / f"{hash}.pkl",
        cache_dir / f"{hash}_hist.pkl",
        cache_dir / f"{hash}_config.pkl",
    )


def combine_histories(hists: List) -> pd.DataFrame:
    # conitue counting steps across runs
    combined_parts = []
    step_offset = 0

    for hist in hists:
        part = hist.copy()

        # ensure columns are identical: _step, turn
        assert part["turn"].equals(part["_step"]), (
            "Expected 'turn' and '_turn' columns to be identical"
        )

        # add offset to cols
        part["_step"] = part["_step"] + step_offset
        part["turn"] = part["_step"]
        combined_parts.append(part)

        step_offset += hist["turn"].max()

    combined = pd.concat(combined_parts, ignore_index=True)

    print(
        f"Combined history has {len(combined)} rows ({[len(part) for part in combined_parts]})"
    )

    return combined

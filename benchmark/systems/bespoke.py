import logging

from llm_cache.git_snapshotter import GitSnapshotter
from tools.fasttest.run import RunTool
from tools.validate_tool.query_validator_class import _parse_output

logger = logging.getLogger(__name__)


class BespokeRunner:
    name = "Bespoke"

    def __init__(
        self,
        db_engine: RunTool,
        snapshotter: GitSnapshotter,
    ) -> None:
        self._db_engine = db_engine
        self._snapshotter = snapshotter
        self._active_snapshot: str | None = None

    def restore_snapshot(self, snapshot: str) -> None:
        if self._active_snapshot == snapshot:
            return
        if not self._snapshotter.has_snapshot(snapshot):
            raise ValueError(f"Snapshot {snapshot} not found in repo.")
        # Avoid stale untracked files from previous snapshots (e.g. queries.txt).
        self._snapshotter.clear_untracked(include_ignored=True)
        logger.info("Restoring snapshot %s", snapshot)
        self._snapshotter.restore(snapshot)
        self._active_snapshot = snapshot

    def run_scale_factor(
        self,
        scale_factor: float,
        query_ids_needed: set[str],
        query_list: list[str],
        sql_list: list[str],
        args_list: list[str],
        snapshot: str,
    ) -> list[float | None]:
        if not query_ids_needed:
            return [None] * len(query_list)

        unique_query_ids = list(dict.fromkeys(query_list))

        logger.info("Running ./db for benchmark...")
        result = self._db_engine.run_worker(
            scale_factor=scale_factor,
            optimize=True,
            query_id=unique_query_ids,
            stdin_args_data=args_list,
        )

        assert result.resp is not None, (
            f"Expected response from ./db execution, got None. {result}"
        )
        assert result.out is not None, (
            f"Expected stdout from ./db execution, got None. {result}"
        )
        assert result.err is not None, (
            f"Expected stderr from ./db execution, got None. {result}"
        )

        parsed = _parse_output(result.out, result.err, result.resp)
        if isinstance(parsed, str):
            raise Exception(
                f"Error parsing ./db output: {parsed}"
                f"\nSTDOUT:\n{result.out}\nSTDERR:\n{result.err}"
            )

        _, measurements = parsed
        exec_times = [float(rt) for _, rt in measurements]

        return [
            exec_times[idx] if query_list[idx] in query_ids_needed else None
            for idx in range(len(query_list))
        ]

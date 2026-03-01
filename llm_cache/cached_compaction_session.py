import logging
from pathlib import Path
from typing import Any, Optional

from agents import TResponseInputItem, custom_span
from agents.memory.openai_responses_compaction_session import (
    OpenAIResponsesCompactionSession,
    select_compaction_candidate_items,
)
from agents.memory.session import OpenAIResponsesCompactionArgs

from llm_cache import utils
from utils.wandb_stats_logging import WandbRunHook

logger = logging.getLogger(__name__)


class CompactCacheType:
    def __init__(self, response_id: str, output_items: list[TResponseInputItem]):
        self.response_id = response_id
        self.output_items = output_items


class CachedOpenAIResponsesCompactionSession(OpenAIResponsesCompactionSession):
    def __init__(
        self, cache_dir: Path, wandb_metrics_hook: Optional[WandbRunHook], **kwargs
    ):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.wandb_metrics_hook = wandb_metrics_hook
        super().__init__(**kwargs)

    async def run_compaction(
        self, args: OpenAIResponsesCompactionArgs | None = None
    ) -> None:
        """Run compaction using responses.compact API."""
        if args and args.get("response_id"):
            self._response_id = args["response_id"]  # type: ignore
        requested_mode = args.get("compaction_mode") if args else None
        if args and "store" in args:
            store = args["store"]
            if store is False and self._response_id:
                self._last_unstored_response_id = self._response_id
            elif store is True and self._response_id == self._last_unstored_response_id:
                self._last_unstored_response_id = None
        else:
            store = None
        resolved_mode = self._resolve_compaction_mode_for_response(
            response_id=self._response_id,
            store=store,
            requested_mode=requested_mode,
        )

        if resolved_mode == "previous_response_id" and not self._response_id:
            raise ValueError(
                "OpenAIResponsesCompactionSession.run_compaction requires a response_id "
                "when using previous_response_id compaction."
            )

        (
            compaction_candidate_items,
            session_items,
        ) = await self._ensure_compaction_candidates()

        force = args.get("force", False) if args else False
        should_compact = force or self.should_trigger_compaction(
            {
                "response_id": self._response_id,
                "compaction_mode": resolved_mode,
                "compaction_candidate_items": compaction_candidate_items,
                "session_items": session_items,
            }
        )

        if not should_compact:
            # logger.debug(
            #     f"skip: decision hook declined compaction for {self._response_id}"
            # )
            return
        with custom_span(f'Compaction ("{self.model}")', {}) as span:
            self._deferred_response_id = None
            logger.debug(
                f"compact: start for {self._response_id} using {self.model} (mode={resolved_mode})"
            )
            compact_kwargs: dict[str, Any] = {"model": self.model}
            if resolved_mode == "previous_response_id":
                compact_kwargs["previous_response_id"] = self._response_id
            else:
                compact_kwargs["input"] = session_items

            output_items = None  # type: ignore

            # try to get output_items from cache
            path = self._get_cache_path()
            if path.exists():
                logger.debug(
                    f"Retrieving compaction from cache for response_id: {self._response_id} (model: {self.model})"
                )
                cached = utils.load_pickle(path, CompactCacheType)
                if cached is not None:
                    output_items = cached.output_items
                    assert cached.response_id == self._response_id

            # fallback if not successfully loaded from cache
            if output_items is None:
                compacted = await self.client.responses.compact(**compact_kwargs)

                logger.debug(
                    f"Running compaction. Model: {self.model} for response_id: {self._response_id}"
                )

                output_items: list[TResponseInputItem] = []
                if compacted.output:
                    for item in compacted.output:
                        if isinstance(item, dict):
                            output_items.append(item)
                        else:
                            # Suppress Pydantic literal warnings: responses.compact can return
                            # user-style input_text content inside ResponseOutputMessage.
                            output_items.append(
                                item.model_dump(exclude_unset=True, warnings=False)  # type: ignore
                            )

                # write to cache
                utils.dump_pickle(
                    path, CompactCacheType(self._response_id, output_items)
                )

            # clear the session
            await self.underlying_session.clear_session()

            logger.debug(f"compaction: {len(output_items)=}")

            # store the compacted items and add to session
            if output_items:
                await self.underlying_session.add_items(output_items)
                # await self.underlying_session.store_run_usage(compacted)
            else:
                raise Exception(
                    f"Compaction returned no output items for response_id {self._response_id} - cannot proceed with empty session"
                )

            self._compaction_candidate_items = select_compaction_candidate_items(
                output_items
            )
            self._session_items = output_items

            logger.debug(
                f"compact: done for {self._response_id} "
                f"(mode={resolved_mode}, output={len(output_items)}, "
                f"candidates={len(self._compaction_candidate_items)})"
            )

            if self.wandb_metrics_hook is not None:
                # log compaction stats
                self.wandb_metrics_hook.log_metrics_callback(
                    {
                        "type": "compaction",
                        "compaction/output_items": len(output_items),
                        "compaction/candidate_items": len(
                            self._compaction_candidate_items
                        ),
                    },
                    log_and_increment=True,
                )

            span.set_error(
                {
                    "message": "test",
                    "data": {
                        "output": len(output_items),
                        "candidates": len(self._compaction_candidate_items),
                    },
                }
            )

    def _get_cache_path(self) -> Path:
        payload = {
            "response_id": self._response_id,
            "model": str(self.model),
        }
        hash = utils.sha256(utils.stable_json(payload))
        return self._cache_path_for(hash)

    def _cache_path_for(self, hash: str) -> Path:
        return self.cache_dir / f"{hash}.pkl"

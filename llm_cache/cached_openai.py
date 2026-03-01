import logging
from dataclasses import is_dataclass
from pathlib import Path
from typing import Any, Dict, Literal, overload

from agents import ApplyPatchTool, ShellTool
from agents.agent_output import AgentOutputSchemaBase
from agents.handoffs import Handoff
from agents.model_settings import ModelSettings
from agents.models.openai_responses import OpenAIResponsesModel
from agents.tool import Tool
from openai import BaseModel
from openai.types.responses import Response

from utils.token_usage import get_tokens_context_and_dollar_info
from utils.truncate_csv import truncate_csvs_recursively

from . import utils
from .git_snapshotter import GitSnapshotter

logger = logging.getLogger(__name__)


class CacheType:
    def __init__(self, response: Response, parent_hash: str | None = None):
        self.response = response
        self.parent_hash = parent_hash


first_invocation = True


class CachedOpenAIResponsesModel(OpenAIResponsesModel):
    def __init__(
        self,
        *args,
        llm_cache_dir: Path,
        snapshotter: GitSnapshotter | None = None,
        stop_on_cache_miss: bool = False,
        query_gen_list: list[str] | None = None,
        artifacts_in_context: str | None = None,
        config_kwargs: Dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.cache_dir = llm_cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.snapshotter = snapshotter
        self.stop_on_cache_miss = stop_on_cache_miss
        self.query_gen_list = query_gen_list
        self.artifacts_in_context = artifacts_in_context
        self.total_saved = 0.0
        self.llm_was_cached = False
        self.config_kwargs = config_kwargs

    def _hash_payload(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any | None,
        stream: bool,
    ) -> str:
        if handoffs:
            raise RuntimeError("Handoffs are not supported with caching.")

        # serialize config args dict () - use key sorting to ensure stable order
        config_kwargs_serialized = ",".join(
            f"{k}={v}" for k, v in sorted(self.config_kwargs.items())
        )

        tools_serialized = []
        try:
            for t in tools:
                if isinstance(t, ApplyPatchTool) or isinstance(t, ShellTool):
                    data = t.name
                elif isinstance(t, BaseModel):
                    # response pydanctic model
                    data = utils.stable_json(t.to_dict())
                elif is_dataclass(t):
                    # dataclass object
                    data = t.__dict__.copy()
                    data.pop("on_invoke_tool", None)
                    data = utils.stable_json(data)
                else:
                    raise Exception(f"Cannot hash tool of type {type(t)}")

                # check that no memory addresses are present in the serialized data
                assert "0x" not in data, (
                    f"Cannot hash tool with non-deterministic data. Discovered likely a function or object reference in the tool definition: {data}"
                )

                tools_serialized.append(data)
        except Exception as e:
            logger.debug(f"Error serializing tools for hashing: {e}\n{str(t)}")
            raise Exception(f"Error serializing tools for hashing: {e}")

        global first_invocation
        if first_invocation:
            logger.debug(f"Tools encoded for hashing: {tools_serialized}")
            first_invocation = False

        payload = {
            "model": str(self.model),
            "system_instructions": system_instructions,
            "input": input,
            "model_settings": model_settings.to_json_dict(),
            "tools": tools_serialized,
            "output_schema": (
                output_schema.json_schema() if output_schema is not None else None
            ),
            # "handoffs": [h.model_dump() if hasattr(h, "model_dump") else repr(h) for h in handoffs],
            "conversation_id": conversation_id,
            "previous_response_id": previous_response_id,
            "prompt": prompt,
            "stream": stream,
            "query_gen_list": self.query_gen_list,
            "artifacts_in_context": self.artifacts_in_context,
            "config_kwargs": config_kwargs_serialized,
        }
        # logger.debug("Cache hash payload: %s", utils.stable_json(payload))
        return utils.sha256(utils.stable_json(payload))

    def _cache_path_for(self, hash: str) -> Path:
        return self.cache_dir / f"{hash}.pkl"

    def __str__(self):
        return str(self.model)

    @overload
    async def _fetch_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        previous_response_id: str | None,
        conversation_id: str | None,
        stream: Literal[False],
        prompt: Any | None = None,
    ) -> Response: ...

    @overload
    async def _fetch_response(
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        previous_response_id: str | None,
        conversation_id: str | None,
        stream: Literal[True],
        prompt: Any | None = None,
    ) -> Any: ...

    async def _fetch_response(  # type: ignore[override]
        self,
        system_instructions: str | None,
        input: Any,
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        previous_response_id: str | None,
        conversation_id: str | None,
        stream: bool,
        prompt: Any | None = None,
    ):
        assert not stream, "stream not supported"

        req_hash = self._hash_payload(
            system_instructions,
            input,
            model_settings,
            tools,
            output_schema,
            handoffs,
            previous_response_id,
            conversation_id,
            prompt,
            stream,
        )

        path = self._cache_path_for(req_hash)

        if path.exists():
            cached = utils.load_pickle(path, CacheType)
            if cached is not None:
                # logger.info(f'Found in cache: {path}')
                resp = cached.response
                assert resp.usage is not None
                cost = get_tokens_context_and_dollar_info(
                    resp.usage, self.model, last_entry_only=True, log=False
                )["cost"]
                logger.debug(f"Saved: ${cost:0.6f}")
                self.total_saved += cost

                assert resp.usage is not None

                # snapshotter restore state
                assert self.snapshotter is not None
                if cached.parent_hash:
                    exists = self.snapshotter.has_snapshot(cached.parent_hash)
                    if not exists:  # fetch and check again
                        self.snapshotter.fetch_snapshots()
                    exists = self.snapshotter.has_snapshot(cached.parent_hash)
                    if not exists:
                        raise Exception(
                            f"Directory does not contain snapshot {cached.parent_hash}, but cache references it."
                        )
                    # Disable this path because commands with runtimes won't produce exact reproducible snapshots
                    # valid = self.snapshotter.matches_snapshot(cached.parent_hash)
                    # if not valid:
                    #    print(path)
                    #    rollback = utils.ask_yes_no(
                    #        f"Rollback to previous snapshot {cached.parent_hash}?",
                    #        default=True,
                    #    )
                    #    if not rollback:
                    #        raise Exception(
                    #            f"Directory does not match parent_hash {cached.parent_hash}"
                    #        )

                    self.snapshotter.clear_untracked(include_ignored=True)
                    self.snapshotter.reset_changes()
                    self.snapshotter.restore(cached.parent_hash)
                    # For backwards compatibility
                    # self.snapshotter.merge_commit_into_trunk(cached.parent_hash)
                else:
                    if self.snapshotter.is_dirty():
                        raise Exception("No parent hash and directory is dirty")

                self.llm_was_cached = True
                return resp

        if self.stop_on_cache_miss:
            raise Exception("Stop on cache miss. Did not found in cache: " + str(path))

        resp = await super()._fetch_response(
            system_instructions=system_instructions,
            input=input,
            model_settings=model_settings,
            tools=tools,
            output_schema=output_schema,
            handoffs=handoffs,
            previous_response_id=previous_response_id,
            conversation_id=conversation_id,
            stream=stream,
            prompt=prompt,
        )
        assert resp.usage is not None
        cost = get_tokens_context_and_dollar_info(
            resp.usage, self.model, last_entry_only=True, log=False
        )["cost"]

        logger.debug(f"Cost: ${cost:0.6f}")

        assert self.snapshotter is not None

        # truncate_csvs_recursively
        if self.config_kwargs.get("max_snapshot_csv_size_mb") is not None:
            truncate_csvs_recursively(
                self.snapshotter.working_dir,
                max_size_mb=self.config_kwargs["max_snapshot_csv_size_mb"],
            )

        # Take snapshot of the created/edited/... files
        _, commit = self.snapshotter.snapshot(req_hash)

        utils.dump_pickle(path, CacheType(resp, parent_hash=commit))
        # logger.debug(f'Wrote to cache: {path}')

        self.snapshotter.push_snapshots()

        self.llm_was_cached = False
        return resp

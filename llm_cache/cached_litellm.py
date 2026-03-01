import logging
from dataclasses import is_dataclass
from pathlib import Path
from typing import Any, Dict

from agents import ApplyPatchTool, ShellTool
from agents.agent_output import AgentOutputSchemaBase
from agents.extensions.models.litellm_model import LitellmModel
from agents.handoffs import Handoff
from agents.model_settings import ModelSettings
from agents.tool import Tool
from openai import BaseModel

from agents.usage import RequestUsage, Usage
from utils.token_usage import get_tokens_context_and_dollar_info
from utils.truncate_csv import truncate_csvs_recursively

from . import utils
from .git_snapshotter import GitSnapshotter

logger = logging.getLogger(__name__)


class CacheType:
    def __init__(self, response, parent_hash: str | None = None):
        self.response = response
        self.parent_hash = parent_hash


class CachedLitellmModel(LitellmModel):
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
    ) -> str:
        if handoffs:
            raise RuntimeError("Handoffs are not supported with caching.")

        config_kwargs_serialized = ",".join(
            f"{k}={v}" for k, v in sorted(self.config_kwargs.items())
        )

        tools_serialized = []
        try:
            for t in tools:
                if isinstance(t, ApplyPatchTool) or isinstance(t, ShellTool):
                    data = t.name
                elif isinstance(t, BaseModel):
                    data = utils.stable_json(t.to_dict())
                elif is_dataclass(t):
                    data = t.__dict__.copy()
                    data.pop("on_invoke_tool", None)
                    data = utils.stable_json(data)
                else:
                    raise Exception(f"Cannot hash tool of type {type(t)}")

                assert "0x" not in data, (
                    "Cannot hash tool with non-deterministic data. "
                    f"Discovered likely a function or object reference in the tool definition: {data}"
                )

                tools_serialized.append(data)
        except Exception as e:
            logger.debug(f"Error serializing tools for hashing: {e}\n{str(t)}")
            raise Exception(f"Error serializing tools for hashing: {e}")

        payload = {
            "model": str(self.model),
            "system_instructions": system_instructions,
            "input": input,
            "model_settings": model_settings.to_json_dict(),
            "tools": tools_serialized,
            "output_schema": (
                output_schema.json_schema() if output_schema is not None else None
            ),
            "conversation_id": conversation_id,
            "previous_response_id": previous_response_id,
            "prompt": prompt,
            "query_gen_list": self.query_gen_list,
            "artifacts_in_context": self.artifacts_in_context,
            "config_kwargs": config_kwargs_serialized,
        }
        return utils.sha256(utils.stable_json(payload))

    def _cache_path_for(self, hash: str) -> Path:
        return self.cache_dir / f"{hash}.pkl"

    def __str__(self) -> str:
        return str(self.model)

    @staticmethod
    def _ensure_usage_entries(usage: Usage) -> None:
        if usage.request_usage_entries:
            return
        if usage.total_tokens <= 0:
            return
        request = RequestUsage(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
            input_tokens_details=usage.input_tokens_details,
            output_tokens_details=usage.output_tokens_details,
        )
        usage.request_usage_entries.append(request)

    async def get_response(self, *args, **kwargs):
        system_instructions = kwargs.get("system_instructions")
        input = kwargs.get("input")
        model_settings = kwargs.get("model_settings")
        tools = kwargs.get("tools") or []
        output_schema = kwargs.get("output_schema")
        handoffs = kwargs.get("handoffs") or []
        previous_response_id = kwargs.get("previous_response_id")
        conversation_id = kwargs.get("conversation_id")
        prompt = kwargs.get("prompt")

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
        )

        path = self._cache_path_for(req_hash)

        if path.exists():
            cached = utils.load_pickle(path, CacheType)
            if cached is not None:
                resp = cached.response
                self._ensure_usage_entries(resp.usage)
                cost = get_tokens_context_and_dollar_info(
                    resp.usage, self.model, last_entry_only=True, log=False
                )["cost"]
                logger.debug(f"Saved: ${cost:0.6f}")
                self.total_saved += cost

                assert self.snapshotter is not None
                if cached.parent_hash:
                    exists = self.snapshotter.has_snapshot(cached.parent_hash)
                    if not exists:
                        self.snapshotter.fetch_snapshots()
                    exists = self.snapshotter.has_snapshot(cached.parent_hash)
                    if not exists:
                        raise Exception(
                            f"Directory does not contain snapshot {cached.parent_hash}, but cache references it."
                        )

                    self.snapshotter.clear_untracked(include_ignored=True)
                    self.snapshotter.reset_changes()
                    self.snapshotter.restore(cached.parent_hash)
                else:
                    if self.snapshotter.is_dirty():
                        raise Exception("No parent hash and directory is dirty")

                self.llm_was_cached = True
                return resp

        if self.stop_on_cache_miss:
            raise Exception("Stop on cache miss. Did not found in cache: " + str(path))

        resp = await super().get_response(*args, **kwargs)
        self._ensure_usage_entries(resp.usage)
        cost = get_tokens_context_and_dollar_info(
            resp.usage, self.model, last_entry_only=True, log=False
        )["cost"]

        logger.debug(f"Cost: ${cost:0.6f}")

        assert self.snapshotter is not None

        if self.config_kwargs.get("max_snapshot_csv_size_mb") is not None:
            truncate_csvs_recursively(
                self.snapshotter.working_dir,
                max_size_mb=self.config_kwargs["max_snapshot_csv_size_mb"],
            )

        _, commit = self.snapshotter.snapshot(req_hash)

        utils.dump_pickle(path, CacheType(resp, parent_hash=commit))

        self.snapshotter.push_snapshots()

        self.llm_was_cached = False
        return resp

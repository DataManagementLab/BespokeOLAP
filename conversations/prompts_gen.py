from pathlib import Path
from string import Template

from utils.general_utils import get_affinity_prompt

_PROMPTS_DIR = Path(__file__).parent / "prompts"
_EXPERT_KNOWLEDGE_PATH = _PROMPTS_DIR / "expert_knowledge.txt"


def _load_txt(path: Path) -> str:
    with open(path, "r") as f:
        return f.read()


def optim_prompt_pretext(queries_path: str, num_queries: int) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_pretext_general.txt")
    template = Template(template_str)
    query_str = "query" if num_queries == 1 else "queries"
    return template.substitute(
        queries_path=queries_path, num_queries=num_queries, query_str=query_str
    )


def optim_prompt_pretext_optim(bespoke_storage: bool) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_pretext_optim.txt")
    template = Template(template_str)
    storage_layout = "storage layout, " if bespoke_storage else ""
    return template.substitute(
        storage_layout=storage_layout,
    )


def optim_prompt_constraints(allow_storage_changes: bool = True) -> str:
    txt = _load_txt(_PROMPTS_DIR / "optim_constraints.txt")
    if not allow_storage_changes:
        txt = (
            txt
            + "\n- You are NOT allowed to change the storage layout. Leave it as Struct-of-Arrays. Do not change the ordering of columns."
        )
    return txt


def optim_prompt_pinning(core_id: int) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_pinning.txt")
    template = Template(template_str)
    affinity_prompt = get_affinity_prompt(include_numa=False)
    return template.substitute(
        query_impl_path="query_impl.cpp",
        affinity_prompt=affinity_prompt,
        core_id=core_id,
    )


def optim_prompt_add_timings() -> str:
    return _load_txt(_PROMPTS_DIR / "optim_add_timings_collect_stats.txt")


def optim_prompt_add_timings_per_query(
    qids_str: str, refer_to_prev_queries: bool, scale_factor: float
) -> str:
    template_str = _load_txt(
        _PROMPTS_DIR / "optim_add_timings_collect_stats_per_query.txt"
    )
    template = Template(template_str)
    return template.substitute(
        qids_str=qids_str,
        refer_to_prev=" Align instrumentation with previous queries."
        if refer_to_prev_queries
        else "",
        sf=scale_factor,
    )


def optim_prompt_w_trace(
    query_id: str,
    constraints_str: str,
    current_rt_ms: float,
    target_rt_ms: float,
    sf: float,
    factor: float,
    storage_is_bespoke: bool,
) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_w_trace.txt")
    template = Template(template_str)
    return template.substitute(
        query_id=query_id,
        constraints=constraints_str,
        target_rt=f"{int(target_rt_ms)}ms",
        current_rt=f"{int(current_rt_ms)}ms",
        sf=sf,
        factor=factor,
        bespoke_storage_related=" e.g. changes to the storage layout and especially ordering of columns"
        if storage_is_bespoke
        else "",
    )


def optim_prompt_with_sample_plan(
    query_id: str, constraints_str: str, duckdb_plan: str, sf: float
) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_with_sample_plan.txt")
    template = Template(template_str)
    return template.substitute(
        query_id=query_id,
        constraints=constraints_str,
        duckdb_plan=duckdb_plan,
        sf=sf,
    )


def optim_prompt_with_human_reference(
    query_id: str,
    constraints_str: str,
    current_rt_ms: float,
    target_rt_ms: float,
    sf: float,
    storage_is_bespoke: bool,
) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_w_human_reference.txt")
    template = Template(template_str)
    return template.substitute(
        query_id=query_id,
        constraints=constraints_str,
        target_rt=f"{int(target_rt_ms)}ms",
        current_rt=f"{int(current_rt_ms)}ms",
        sf=sf,
        bespoke_storage_related=" e.g. changes to the storage layout and especially ordering of columns"
        if storage_is_bespoke
        else "",
    )


def load_expert_knowledge() -> str:
    return _load_txt(_EXPERT_KNOWLEDGE_PATH)


def optim_prompt_with_expert_knowledge(
    query_id: str,
    constraints_str: str,
    expert_knowledge: str,
    current_rt_ms: float,
    target_rt_ms: float,
    sf: float,
    storage_is_bespoke: bool,
) -> str:
    template_str = _load_txt(_PROMPTS_DIR / "optim_w_expert_knowledge.txt")
    template = Template(template_str)
    return template.substitute(
        query_id=query_id,
        constraints=constraints_str,
        expert_knowledge=expert_knowledge,
        target_rt=f"{int(target_rt_ms)}ms",
        current_rt=f"{int(current_rt_ms)}ms",
        sf=sf,
        bespoke_storage_related=" e.g. changes to the storage layout and especially ordering of columns"
        if storage_is_bespoke
        else "",
    )

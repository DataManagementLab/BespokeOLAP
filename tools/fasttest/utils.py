import logging
import os
import re
from pathlib import Path
from typing import List, Optional

from dataset.dataset_tables_dict import get_tables_for_benchmark
from llm_cache.git_snapshotter import GitSnapshotter
from misc.fasttest.compiler_cached import CachedCompiler

logger = logging.getLogger(__name__)


def _gen_table_defs(tables: List[str]) -> str:
    indent = " " * 4
    return "\n".join(f"{indent}ArrowTable {name};" for name in tables)


def _gen_table_reads(tables: List[str]) -> str:
    indent = " " * 4
    return "\n".join(
        f'{indent}tables->{name} = ReadParquetTable(path + "{name}.parquet");'
        for name in tables
    )


def copy_template_to(destination_dir: Path, benchmark: str):
    assert destination_dir.exists()

    project_dir = Path(__file__).parents[2]
    src_dir = project_dir / "misc" / "fasttest"

    files = [
        "loader_impl.hpp",
        "loader_impl.cpp",
        "builder_impl.hpp",
        "builder_impl.cpp",
        "query_impl.hpp",
        "query_impl.cpp",
    ]

    tables = get_tables_for_benchmark(benchmark)

    content = ""
    for filename in files:
        source = src_dir / filename

        if not source.is_file():
            raise FileNotFoundError(f"Source file not found: {source}")

        file_content = source.read_text()

        if filename == "loader_impl.hpp":
            file_content = replace_cpp_marked_block(
                file_content, "table-defs", _gen_table_defs(tables)
            )
        elif filename == "loader_impl.cpp":
            file_content = replace_cpp_marked_block(
                file_content, "table-reads", _gen_table_reads(tables)
            )

        # assemble string containing content of copied files - for versioning / snapshotting
        content += f"// ---- {filename} ----\n"
        content += file_content + "\n\n"

        dest = destination_dir / filename
        logger.info(f"Writing {filename} to {dest}")
        dest.write_text(file_content)

    return content


def replace_cpp_marked_block(text, marker_name, replacement):
    name = re.escape(marker_name)

    pattern = re.compile(
        rf"""(?ms)
        ^[ \t]*//[ \t]*start:[ \t]*{name}[ \t]*\r?\n?
        .*?
        ^[ \t]*//[ \t]*end:[ \t]*{name}[ \t]*(?:\r?\n|$)
        """,
        re.VERBOSE,
    )

    if replacement and not replacement.endswith(("\n", "\r\n")):
        replacement += "\n"

    result, n = pattern.subn(replacement, text, count=1)

    if n != 1:
        raise ValueError(f"expected exactly one replacement, got {n}")

    return result


def relpath(target: Path, base: Path) -> Path:
    return Path(os.path.relpath(target, base))


def make_compiler(
    cwd: Path,
    compile_cache_dir: Optional[Path] = None,
    git_snapshotter: Optional[GitSnapshotter] = None,
    api_path: Optional[Path] = None,
) -> CachedCompiler:
    if api_path is None:
        api_path = relpath(
            cwd.resolve().parent / "misc" / "fasttest",
            cwd.resolve(),
        )

    args = dict(
        working_dir=cwd,
        libs={
            "loader": [
                api_path / "loader_api.cpp",
                "loader_impl.cpp",
                api_path / "loader_utils.cpp",
            ],  # for now do not share the loader_impl
            "builder": [api_path / "builder_api.cpp", "builder_impl.cpp"],
            "query": [api_path / "query_api.cpp", "query_impl.cpp"],
        },
        main_src=api_path / "db.cpp",
        include_dirs=[api_path],
        app_extra_srcs=[api_path / "utils/build_id.cpp"],
        build_dir="build",
        link_libs=[],
        pkgconfig_libs=["arrow", "parquet"],
    )
    return CachedCompiler(
        args=args,
        compile_cache_dir=compile_cache_dir,
        git_snapshotter=git_snapshotter,
    )

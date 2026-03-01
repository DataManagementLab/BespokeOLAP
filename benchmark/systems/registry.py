from benchmark.systems.bespoke import BespokeRunner
from benchmark.systems.duckdb import DuckDBRunner

# Lower-case key -> runner class.
SYSTEM_REGISTRY: dict[str, type] = {
    "bespoke": BespokeRunner,
    "duckdb": DuckDBRunner,
}

"""Pluggable benchmark system runners.

To add a new system:
1. Implement a class with ``name: str`` and ``run_scale_factor(...)`` matching
   the ``SystemRunner`` protocol.
2. Register it in ``SYSTEM_REGISTRY`` with a lower-case key.
"""

from benchmark.systems.base import SystemRunner
from benchmark.systems.bespoke import BespokeRunner
from benchmark.systems.duckdb import DuckDBRunner
from benchmark.systems.registry import SYSTEM_REGISTRY

__all__ = [
    "SystemRunner",
    "BespokeRunner",
    "DuckDBRunner",
    "SYSTEM_REGISTRY",
]

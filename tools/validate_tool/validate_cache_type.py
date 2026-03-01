from typing import Dict


class ExtendedValidateCacheType:
    """Extension of ValidateCacheType to include metrics for Wandb logging. This is for backwards compatibility with existing cache entries (make sure they can still be loaded by pickle into ValidateCacheType)."""

    def __init__(
        self,
        outputs: str,
        success: bool,
        metrics: Dict,
    ):
        self.outputs = outputs
        self.metrics = metrics
        self.success = success

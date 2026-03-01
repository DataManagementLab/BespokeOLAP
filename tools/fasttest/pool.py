from misc.fasttest.fasttest_proc import FasttestProc
from typing import Callable


class _FasttestHolder:
    def __init__(self) -> None:
        self._runners: dict[str, FasttestProc] = {}

    def get(self, key: str, factory: Callable[[], FasttestProc]) -> FasttestProc:
        runner = self._runners.get(key)
        if runner is None:
            runner = factory()
            self._runners[key] = runner
        return runner

    def terminate(self, key: str) -> bool:
        runner = self._runners.pop(key, None)
        if runner is None:
            return False
        runner.terminate()
        return True

    def terminate_all(self) -> None:
        for key in list(self._runners.keys()):
            self.terminate(key)


FastTestPool = _FasttestHolder()

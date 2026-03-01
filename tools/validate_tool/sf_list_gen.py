from typing import List


def gen_sf(benchmark: str):
    if benchmark == "tpch":
        verify_sf_list: List[float] = [1, 2]
        max_scale_factor = 20
    elif benchmark == "ceb":
        verify_sf_list: List[float] = [
            0.25,
            0.5,
        ]  # just two different scales to make sure that the code works well with different data.
        max_scale_factor = 2
    else:
        raise ValueError(f"Unknown benchmark {benchmark}")

    return verify_sf_list, max_scale_factor

import simulation_analyzer
import numpy as np
from filter_functions import filter_by_parameters, EQUALS, ISNULL
from utils import collect_simulations, get_simulation_identifier

TRACE_CACHE_SIZES = {
    "wiki2019.tr": [
        68719476736,
        137438953472,
        274877906944,
        549755813888,
        1099511627776
    ]
}


def run():
    results_dir = "./results_0227"
    cache_types = ["AdaptSize", "LHD", "LRU", "GDSF"]
    trace = "wiki2019.tr"
    df = collect_simulations(results_dir)
    for cache_type in cache_types:
        print(cache_type)
        filtered_df = filter_by_parameters(df, [
            EQUALS("cache_type", cache_type),
            EQUALS("filter_type", "Bloom"),
            EQUALS("max_n_element", "80000000")
        ])

        print(simulation_analyzer.to_bmr_list(filtered_df, "cache_size"))
        # print(simulation_analyzer.to_omr_list(filtered_df, "cache_size"))
        # print(simulation_analyzer.cache_sizes_omr(TRACE_CACHE_SIZES[trace], filtered_df))


if __name__ == "__main__":
    run()

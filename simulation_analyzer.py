import pprint

from utils import get_simulation_identifier, np


def _get_bmr(simulation):
    return simulation["no_warmup_byte_miss_ratio"]


def _get_bhr(simulation):
    return 1 - _get_bmr(simulation)


def _get_omr(simulation):
    object_miss_count = sum(simulation["segment_object_miss"])
    total_object_count = int(simulation["n_req"])
    return object_miss_count / total_object_count


def _get_ohr(simulation):
    return 1 - _get_omr(simulation)


## Transformation functions
"""

"""


def bhr_cache_types(simulations):
    bhr, cache_types = [], []
    for simulation in simulations:
        bhr.append(_get_bhr(simulation))
        cache_types.append(simulation["cache_type"])
    return bhr, cache_types


def bmr_cache_types(simulations):
    bmr, cache_types = [], []
    for simulation in simulations:
        bmr.append(_get_bmr(simulation))
        cache_types.append(simulation["cache_type"])
    return bmr, cache_types


def ohr_cache_types(simulations):
    ohr, cache_types = [], []
    for simulation in simulations:
        ohr.append(_get_ohr(simulation))
        cache_types.append(simulation["cache_type"])
    return ohr, cache_types


def omr_cache_types(simulations):
    omr, cache_types = [], []
    for simulation in simulations:
        omr.append(_get_omr(simulation))
        cache_types.append(simulation["cache_type"])
    return omr, cache_types


"""
All you need to figure out is to get the 
"""


def assert_same_parameters_except(df, parameter):
    identifiers = {get_simulation_identifier(simulation, {parameter}) for _, simulation in df.iterrows()}
    if len(identifiers) != 1:
        assert len(identifiers) == 1, identifiers


def to_bmr_list(df, x_axis_parameter_type):
    assert_same_parameters_except(df, x_axis_parameter_type)
    bmr_list = []
    for i, simulation in df.iterrows():
        bmr_list.append((getattr(simulation, x_axis_parameter_type), _get_bmr(simulation)))

    bmr_list.sort(key=lambda item: item[0])
    return [val for val in bmr_list]


def to_omr_list(df, x_axis_parameter_type):
    assert_same_parameters_except(df, x_axis_parameter_type)
    omr_list = []
    for i, simulation in df.iterrows():
        omr_list.append((getattr(simulation, x_axis_parameter_type), _get_omr(simulation)))
    omr_list.sort(key=lambda item: item[0])
    return [val for val in omr_list]

def to_segment_object_miss_list(df, ):
    """

    :param df:
    :return:
    [
        [(obj_count at segment 1, bmr), (obj_count at segment 2, bmr)],
        [(obj_count at segment 1, bmr), (obj_count at segment 2, bmr)],
    ]
    """
    pass


# def cache_sizes_bmr(cache_sizes, df):
#     """
#     Assumes df is composed of simulations unique for cache_type and cache_size
#     Exception if there exists a simulation with duplicate simulation for cache_type and cache_size.
#     Exception if there exists a simulation for a cache_type without cache_size.
#     Exception if there exists df from two different traces.
#     :param cache_sizes:
#     :param df: list of dict of simulation result unique for each size and cache type.
#     :return: dict of key = cache type, value = List[bmr] where the list of bmr is
#     of equal size.
#     """
#     # {<cache_type> : [bmr at cache_sizes[0], bmr at cache_sizes[1], ..., bmr at cache_sizes[n]], ...}
#     dic = {simulation[1]["cache_type"]: [0 for _ in range(len(cache_sizes))] for simulation in df.iterrows()}
#     cache_size_to_index = {cache_size: i for i, cache_size in enumerate(cache_sizes)}
#     # group df by cache_sizes
#     for i, simulation in df.iterrows():
#         index = cache_size_to_index[simulation["cache_size"]]
#         dic[simulation["cache_type"]][index] = _get_bmr(simulation)
#     return dic
#
#
#
# def cache_sizes_omr(cache_sizes, df):
#     """
#     :param cache_sizes:
#     :param df:
#     :return:
#     """
#     # {<cache_type> : [bmr at cache_sizes[0], bmr at cache_sizes[1], ..., bmr at cache_sizes[n]], ...}
#     dic = {simulation[1]["cache_type"]: [0 for _ in range(len(cache_sizes))] for simulation in df.iterrows()}
#     cache_size_to_index = {cache_size: i for i, cache_size in enumerate(cache_sizes)}
#     # group df by cache_sizes
#     for i, simulation in df.iterrows():
#         index = cache_size_to_index[simulation["cache_size"]]
#         dic[simulation["cache_type"]][index] = _get_omr(simulation)
#     return dic


if __name__ == "__main__":
    simulation_results = []  # list of dictionaries with simulation data
    # Generalized evaluation
    pass

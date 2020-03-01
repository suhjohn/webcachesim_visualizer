import json
import os
import glob

import numpy as np
import pandas as pd

SIMULATION_DATA_KEYS = {'task_id', 'no_warmup_byte_miss_ratio',
                        'segment_byte_miss', 'segment_byte_req',
                        'segment_object_miss',
                        'segment_object_req', 'segment_rss',
                        'real_time_segment_byte_miss',
                        'real_time_segment_byte_req',
                        'real_time_segment_object_miss',
                        'real_time_segment_object_req',
                        'real_time_segment_rss',
                        'simulation_time',
                        'simulation_timestamp'}


def _to_label(x: dict):
    """
    what to put on legend
    """
    return ' '.join([
        f'{k}: {v}' for k, v in x.items() if k not in {
            'cache_size',
            'trace_file',
            'simulation_time',
            'n_warmup',
            'n_early_stop',
            'uni_size',
            'byte_miss_ratio',
            'object_miss_ratio',
            'segment_byte_miss_ratio',
            'segment_object_miss_ratio',
            ''
            'segment_byte_miss',
            'miss_decouple',
            'cache_size_decouple',
        }
    ])


def collect_simulations(results_dir):
    filenames = []
    for file in glob.glob(f"{results_dir}/*"):
        filenames.append(file)

    simulation_results = []
    for filename in filenames:
        with open(filename) as f:
            simulation_result = json.load(f)
            for k in ['cache_size', 'uni_size', 'n_warmup', 'byte_miss_ratio', 'object_miss_ratio']:
                if k in simulation_result:
                    try:
                        simulation_result[k] = float(simulation_result[k])
                    except Exception as e:
                        simulation_result[k] = np.nan
            # simulation_result['label'] = get_simulation_identifier(simulation_result)
            # clean
            for k, v in simulation_result.items():
                if v is np.nan or v is None:
                    simulation_result[k] = '0'
            simulation_results.append(simulation_result)
    return pd.DataFrame(simulation_results)


def get_simulation_identifier(simulation_result, excludes=None):
    if excludes is None:
        excludes = set()

    simulation_result_keys = sorted(set(simulation_result.keys()) - SIMULATION_DATA_KEYS)
    identifier = []
    for key in simulation_result_keys:
        if key in excludes:
            continue
        identifier.append(key)
        value = simulation_result[key]
        if type(value) == list:
            value = tuple(value)
        elif value is np.nan:
            value = '0'
        identifier.append(value)
    return tuple(identifier)

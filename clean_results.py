import glob
import json
import utils
import os


def get_identifier(filename):
    with open(filename) as f:
        simulation_result = json.load(f)
        # identifier = utils.get_simulation_identifier(simulation_result)

    return simulation_result["task_id"]


def get_duplicates(filenames):
    unique_identifier_set = set()
    duplicate_dict = dict()
    identifier_to_original_filename = dict()
    for filename in filenames:
        identifier = get_identifier(filename)
        if identifier in unique_identifier_set:
            original_filename = identifier_to_original_filename[identifier]
            duplicate_dict[original_filename].append(filename)
            continue
        unique_identifier_set.add(identifier)
        identifier_to_original_filename[identifier] = filename
        duplicate_dict[filename] = list()

    duplicate_dict = {k: v for k, v in duplicate_dict.items() if v}
    return duplicate_dict


def remove_duplicates(duplicate_dict):
    for filename, duplicate_list in duplicate_dict.items():
        for duplicate in duplicate_list:
            os.remove(duplicate)


def add_name_trace_size(filename):
    trace_file, cache_type, cache_size, _ = filename.split("__")
    trace_file = trace_file.split('/')[-1]
    with open(filename) as f:
        simulation_result = json.load(f)
        simulation_result["trace_file"] = trace_file
        simulation_result["cache_type"] = cache_type
        simulation_result["cache_size"] = cache_size

    os.remove(filename)
    with open(filename, 'w') as f:
        json.dump(simulation_result, f)


def remove_if_no_realtime(filename):
    should_remove = False
    with open(filename) as f:
        simulation_result = json.load(f)
        if 'real_time_segment_byte_req' not in simulation_result.keys():
            should_remove = True

    if should_remove:
        os.remove(filename)

def remove_if_no_task_id(filename):
    should_remove = False
    with open(filename) as f:
        simulation_result = json.load(f)
        if not simulation_result.get("task_id"):
            should_remove = True

    if should_remove:
        os.remove(filename)
    return should_remove

def remove_files_if_no_task_id():
    filenames = []
    for file in glob.glob("./results/*"):
        filenames.append(file)
    print(f"before removal count: {len(filenames)}")
    removed_count = 0
    for filename in filenames:
        if remove_if_no_task_id(filename):
            removed_count += 1
    print(f"after removal count: {len(filenames) - removed_count}")

def remove_files_if_wrong_task_id():
    filenames = []
    for file in glob.glob("./results/*"):
        filenames.append(file)
    print(f"before removal count: {len(filenames)}")
    removed_count = 0
    for filename in filenames:
        with open(filename) as f:
            simulation_result = json.load(f)
            if len(simulation_result["task_id"]) < 32:
                os.remove(filename)
    print(f"after removal count: {len(filenames) - removed_count}")


def remove_files_if_filename_with_underscore():
    filenames = []
    for file in glob.glob("./results_0227/*"):
        filenames.append(file)
    print(f"before removal count: {len(filenames)}")
    removed_count = 0
    for filename in filenames:
        if len(filename) > len('./results_0227/a69f8045af8cd73c4f521a45ce9c323e'):
            os.remove(filename)
            removed_count += 1
    print(f"after removal count: {len(filenames) - removed_count}")

if __name__ == "__main__":
    filenames = []
    for file in glob.glob("./results_0227/*"):
        filenames.append(file)
    remove_files_if_filename_with_underscore()
    # for filename in filenames:
    #     add_name_trace_size(filename)
    # duplicate_dict = get_duplicates(filenames)
    # print(duplicate_dict)
    # remove_duplicates(duplicate_dict)

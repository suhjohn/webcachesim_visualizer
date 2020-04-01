import datetime
import json
import math
import numpy
import logging
from collections import defaultdict
from multiprocessing import Process, Queue

import sys

CONVERSION_MULTIPLE = {
    "seconds": 1,
    "milliseconds": 1000,
    "microseconds": 1000000,
    # Add here
}


class StringCacheTraceIterator:
    def __init__(self, file_path, ts_format="seconds"):
        assert ts_format in CONVERSION_MULTIPLE, "ts_format not supported."
        self.file_path = file_path
        self.ts_format = ts_format

    def __iter__(self):
        """

        :return: generator((int, int, int))
        """
        file = open(self.file_path, 'r')
        next_line = file.readline()
        while next_line:
            trace = None
            try:
                trace = self.parse_line(next_line)
            except:
                next_line = file.readline()
                trace = self.parse_line(next_line)
            # trace: (timestamp, key, size)
            yield trace
            next_line = file.readline()
        file.close()

    def head(self, n=1):
        lines = []
        file = open(self.file_path, 'r')
        for i in range(n):
            next_line = file.readline()
            lines.append(self.parse_line(next_line))
        file.close()
        return lines

    def parse_line(self, tr_data_line):
        split_line = tr_data_line.split(" ")
        if len(split_line) < 3:
            print(tr_data_line)
            raise ValueError
        try:
            timestamp = int(split_line[0])
        except:
            timestamp = split_line[0]
        try:
            size = int(split_line[2])
        except:
            raise
        try:
            key = int(split_line[1])
        except:
            key = split_line[1]
        return timestamp, key, size


def init_empty_array(size):
    arr = [0 for _ in range(size)]
    return arr


class SizeBins:
    def __init__(self, max_pow):
        self.max_pow = max_pow
        self.max_bin_size = max_pow + 1
        self.bins = init_empty_array(1 + 1 + max_pow)

    def incr_count(self, size):
        bucket_index = min(size.bit_length(), self.max_bin_size)
        self.bins[bucket_index] += 1

    def get_bins(self):
        return list(self.bins)

    def reset_bins(self):
        self.bins = init_empty_array(1 + self.max_bin_size)


class AgeBins:
    def __init__(self, max_pow):
        self.bins = init_empty_array(1 + 1 + max_pow)
        self.last_accessed = defaultdict(int)  # key: last access logical time
        self.max_bin_size = max_pow + 1

    def incr_count(self, key, curr_index):
        if self.last_accessed[key]:
            age_index = min((curr_index - self.last_accessed[key]).bit_length(), self.max_bin_size)
            self.bins[age_index] += 1
        self.last_accessed[key] = curr_index

    def get_bins(self):
        return list(self.bins)

    def reset_bins(self):
        self.bins = init_empty_array(1 + self.max_bin_size)


class FreqBins:
    def __init__(self, max_pow):
        self.freq_counter = defaultdict(int)
        self.bins = init_empty_array(1 + 1 + max_pow)
        self.max_bin_size = max_pow + 1

    def incr_count(self, key):
        self.freq_counter[key] += 1
        freq_index = min(self.freq_counter[key].bit_length(), self.max_bin_size)
        self.bins[freq_index] += 1

    def get_bins(self):
        return list(self.bins)

    def reset_bins(self):
        self.bins = init_empty_array(1 + self.max_bin_size)


class UniqueFrequencyBins:
    def __init__(self, max_pow):
        self.freq_counter = defaultdict(int)
        self.bins = init_empty_array(1 + 1 + max_pow)
        self.max_bin_size = max_pow + 1

    def incr_count(self, key):
        self.freq_counter[key] += 1

    def get_bins(self):
        for key in self.freq_counter.keys():
            freq_index = min(self.freq_counter[key].bit_length(), self.max_bin_size)
            self.bins[freq_index] += 1
        return list(self.bins)


class TraceStatistics:
    MAX_FREQ_BUCKET_COUNT = 32

    def __init__(self, trace_iterator, max_pow=32,
                 real_time_window=600, logical_window=100000):
        self.trace_index = 0
        self.trace_iterator = trace_iterator
        self.mean = -1
        self.max_pow = max_pow
        self.logical_window = logical_window
        self.real_time_window = real_time_window * CONVERSION_MULTIPLE[self.trace_iterator.ts_format]

        self.total_obj_size = None
        self.total_obj_count = None
        self.non_one_hit_wonder_count = None
        self.unique_obj_count = None
        self.unique_obj_size = None
        self.frequency_bins = None
        self.size_bins = None
        self.age_bins = None
        self.logical_window_frequency_bins = None
        self.logical_window_size_bins = None
        self.logical_window_age_bins = None
        self.real_time_window_frequency_bins = None
        self.real_time_window_size_bins = None
        self.real_time_window_age_bins = None
        res_dict = self._gather_statistics()
        for key, value in res_dict.items():
            setattr(self, key, value)

    def _gather_statistics(self):
        logging.info("start gathering statistics")
        fns = [
            self._count_general_statistics,
            self._count_frequency,
            self._count_size,
            self._count_age,
            self._count_logical_window_frequency,
            self._count_logical_window_size,
            self._count_logical_window_age,
            self._count_real_time_window_frequency,
            self._count_real_time_window_size,
            self._count_real_time_window_age,
        ]
        q = Queue()
        processes = [Process(target=fn, args=(q,)) for fn in fns]
        for process in processes:
            process.start()
        res_dict = {}
        for i in range(len(processes)):
            res_dict.update(q.get())
        for process in processes:
            process.join()

        # post-condition validation
        assert "total_obj_size" in res_dict
        assert "total_obj_count" in res_dict
        assert "non_one_hit_wonder_count" in res_dict
        assert "unique_obj_count" in res_dict
        assert "unique_obj_size" in res_dict
        assert "frequency_bins" in res_dict
        assert "size_bins" in res_dict
        assert "age_bins" in res_dict
        assert "logical_window_frequency_bins" in res_dict
        assert "logical_window_size_bins" in res_dict
        assert "logical_window_age_bins" in res_dict
        assert "real_time_window_frequency_bins" in res_dict
        assert "real_time_window_size_bins" in res_dict
        assert "real_time_window_age_bins" in res_dict
        return res_dict

    def _count_general_statistics(self, q):
        unique_obj = {}
        trace_count = 0
        total_size = 0
        obj_count = defaultdict(int)
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            unique_obj[key] = size
            obj_count[key] += 1
            total_size += size
            trace_count += 1

        non_one_hit_wonder_count = 0
        for count in obj_count.values():
            non_one_hit_wonder_count += int(count > 1)
        logging.info("count_general_statistics complete")
        q.put({
            "total_obj_size": total_size,
            "total_obj_count": trace_count,
            "non_one_hit_wonder_count": non_one_hit_wonder_count,
            "unique_obj_count": len(unique_obj),
            "unique_obj_size": sum(unique_obj.values())
        })

    def _count_frequency(self, q):
        freq_bins = UniqueFrequencyBins(self.MAX_FREQ_BUCKET_COUNT)
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            freq_bins.incr_count(key)
        logging.info("count_frequency complete")
        q.put({
            "frequency_bins": freq_bins.get_bins()
        })

    def _count_logical_window_frequency(self, q):
        freq_bins = FreqBins(self.MAX_FREQ_BUCKET_COUNT)
        logical_window_frequency_bins = []  # list[list[int]]
        curr_index = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            freq_bins.incr_count(key)
            if curr_index % self.logical_window == 0 and curr_index != 0:
                logical_window_frequency_bins.append(freq_bins.get_bins())
                freq_bins.reset_bins()
            curr_index += 1
        logical_window_frequency_bins.append(freq_bins.get_bins())

        logging.info("count_logical_window_frequency complete")
        q.put({
            "logical_window_frequency_bins": logical_window_frequency_bins,
        })

    def _count_real_time_window_frequency(self, q):
        freq_bins = FreqBins(self.MAX_FREQ_BUCKET_COUNT)
        window_start_ts, _, _ = self.trace_iterator.head()[0]
        real_time_window_freq_bins = []  # list[list[int]]
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            freq_bins.incr_count(key)
            if timestamp - window_start_ts > self.real_time_window:
                real_time_window_freq_bins.append(freq_bins.get_bins())
                freq_bins.reset_bins()
                window_start_ts = timestamp

        real_time_window_freq_bins.append(freq_bins.get_bins())

        logging.info("count_real_time_window_frequency complete")
        q.put({
            "real_time_window_frequency_bins": real_time_window_freq_bins,
        })

    def _count_size(self, q):
        size_bins = SizeBins(self.max_pow)
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            size_bins.incr_count(size)

        logging.info("count_size complete")
        q.put({
            "size_bins": size_bins.get_bins(),
        })

    def _count_logical_window_size(self, q):
        size_bins = SizeBins(self.max_pow)
        logical_window_size_bins = []  # list[list[int]]
        curr_index = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            size_bins.incr_count(size)
            if curr_index % self.logical_window == 0 and curr_index != 0:
                logical_window_size_bins.append(size_bins.get_bins())
                size_bins.reset_bins()
            curr_index += 1
        logical_window_size_bins.append(size_bins.get_bins())

        logging.info("count_logical_window_size complete")
        q.put({
            "logical_window_size_bins": logical_window_size_bins,
        })

    def _count_real_time_window_size(self, q):
        window_start_ts, _, _ = self.trace_iterator.head()[0]
        size_bins = SizeBins(self.max_pow)
        real_time_window_size_bins = []  # list[list[int]]
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            size_bins.incr_count(size)
            if timestamp - window_start_ts > self.real_time_window:
                real_time_window_size_bins.append(size_bins.get_bins())
                size_bins.reset_bins()
                window_start_ts = timestamp
        real_time_window_size_bins.append(size_bins.get_bins())

        logging.info("count_real_time_window_size complete")
        q.put({
            "real_time_window_size_bins": real_time_window_size_bins,
        })

    def _count_age(self, q):
        age_bins = AgeBins(self.max_pow)
        curr_index = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            age_bins.incr_count(key, curr_index)
            curr_index += 1

        logging.info("count_age complete")
        q.put({
            "age_bins": age_bins.get_bins(),
        })

    def _count_logical_window_age(self, q):
        age_bins = AgeBins(self.max_pow)
        logical_window_age_bins = []  # list[list[int]]
        curr_index = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            age_bins.incr_count(key, curr_index)
            if curr_index % self.logical_window == 0 and curr_index != 0:
                logical_window_age_bins.append(age_bins.get_bins())
                age_bins.reset_bins()
            curr_index += 1
        logical_window_age_bins.append(age_bins.get_bins())

        logging.info("count_logical_window_age complete")
        q.put({
            "logical_window_age_bins": logical_window_age_bins,
        })

    def _count_real_time_window_age(self, q):
        age_bins = AgeBins(self.max_pow)
        window_start_ts, _, _ = self.trace_iterator.head()[0]
        real_time_window_age_bins = []  # list[list[int]]
        curr_index = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            age_bins.incr_count(key, curr_index)
            if timestamp - window_start_ts > self.real_time_window:
                real_time_window_age_bins.append(age_bins.get_bins())
                age_bins.reset_bins()
                window_start_ts = timestamp
            curr_index += 1
        real_time_window_age_bins.append(age_bins.get_bins())

        logging.info("count_real_time_window_age complete")
        q.put({
            "real_time_window_age_bins": real_time_window_age_bins,
        })

    def as_dict(self):
        return {
            "total_obj_size": self.total_obj_size,
            "total_obj_count": self.total_obj_count,
            "non_one_hit_wonder_count": self.non_one_hit_wonder_count,
            "unique_obj_count": self.unique_obj_count,
            "unique_obj_size": self.unique_obj_size,
            "frequency_bins": self.frequency_bins,
            "size_bins": self.size_bins,
            "age_bins": self.age_bins,
            "logical_window_frequency_bins": self.logical_window_frequency_bins,
            "logical_window_size_bins": self.logical_window_size_bins,
            "logical_window_age_bins": self.logical_window_age_bins,
            "real_time_window_frequency_bins": self.real_time_window_frequency_bins,
            "real_time_window_size_bins": self.real_time_window_size_bins,
            "real_time_window_age_bins": self.real_time_window_age_bins,
            # Metadata
            "logical_window": self.logical_window,
            "real_time_window": self.real_time_window,
            "real_time_ts_format": self.trace_iterator.ts_format,
        }


if __name__ == "__main__":
    logging.basicConfig(filename='trace_analyzer.log', level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    start = datetime.datetime.now()
    trace_filepath = sys.argv[1]
    real_time_window = int(sys.argv[2])
    ts_format = sys.argv[3]
    trace_iterator = StringCacheTraceIterator(trace_filepath, ts_format)
    statistics = TraceStatistics(trace_iterator, real_time_window=real_time_window)
    print(statistics.as_dict())
    with open(trace_filepath + ".stat", "w") as f:
        json.dump(statistics.as_dict(), f)
    print(datetime.datetime.now() - start)

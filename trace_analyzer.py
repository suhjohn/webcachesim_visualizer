import datetime
import json
import math
import numpy
from collections import defaultdict
from multiprocessing import Process, Queue

import sys

"""
p = Process(target=f, args=('bob',))
p.start()
p.join()

"""

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
    arr = numpy.empty(size, dtype=numpy.int)
    for i in range(len(arr)):
        arr[i] = 0
    return arr


class TraceStatistics:
    MAX_FREQ_COUNT = 10000

    def __init__(self, trace_iterator, max_pow=32, real_time_window=600, logical_window=100000):
        """
        0    1    2    3     ... max_pow - 1                             max_pow
             2^1  2^2  2^3  2^4   ... 2^(max_pow-1)~2^(max_pow)   2^(max_pow)+
             12   24   48   816   ...
        :param filename:
        :param max_pow: represents the size of bins
        """
        self.trace_index = 0
        self.trace_iterator = trace_iterator
        self.mean = -1
        self.size_bins = init_empty_array(1 + 1 + max_pow)
        self.max_bin_size = max_pow + 1
        # 1 index, up to 10 million age, 10 million+
        self.age_bins = init_empty_array(1 + 1 + max_pow)
        self.frequency_bins = init_empty_array(1 + 1 + self.MAX_FREQ_COUNT)
        self.last_accessed = defaultdict(int)  # key: last access logical time
        self.freq_counter = defaultdict(int)

        self.logical_window = logical_window
        self.logical_window_mean_size = []
        self.real_time_window = real_time_window * CONVERSION_MULTIPLE[self.trace_iterator.ts_format]
        self.real_time_window_mean_size = []

        self._start_ts = datetime.datetime.now()
        # run statistics
        # self._collect_statistics()
        self.unique_obj_count = 0
        self.unique_obj_size = 0
        self.total_obj_size = 0
        q = Queue()
        p = Process(target=self._collect_frequency, args=(q,))
        p2 = Process(target=self._collect_size, args=(q,))
        p3 = Process(target=self._collect_age, args=(q,))
        p4 = Process(target=self._collect_real_time_window_statistics, args=(q,))
        p5 = Process(target=self._collect_logical_window_statistics, args=(q,))
        p6 = Process(target=self._collect_unique_obj_statistics, args=(q,))
        processes = [p, p2, p3, p4, p5, p6]

        for _p in processes:
            _p.start()
        res_dict = {}
        for i in range(len(processes)):
            res_dict.update(q.get())
        for _p in processes:
            _p.join()

        for key, value in res_dict.items():
            setattr(self, key, value)

    def _print_status(self):
        # i = self.trace_index % self.logical_window
        # if not i and self.trace_index > 0:
        #     print(f"current_count:{self.trace_index} time_taken:{datetime.datetime.now() - self._start_ts}")
        #     self._start_ts = datetime.datetime.now()
        self.trace_index += 1

    def _collect_unique_obj_statistics(self, q):
        unique_obj = {}
        total_size = 0
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            unique_obj[key] = size
            total_size += size
            self._print_status()

        q.put({
            "total_obj_size": total_size,
            "unique_obj_count": len(unique_obj),
            "unique_obj_size": sum(unique_obj.values())
        })

    def _collect_real_time_window_statistics(self, q):
        real_time_start_ts, _, _ = self.trace_iterator.head()[0]
        real_time_window_size_arr = []
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            if timestamp - real_time_start_ts > self.real_time_window:
                self.real_time_window_mean_size.append(numpy.average(real_time_window_size_arr))
                real_time_start_ts = timestamp
                real_time_window_size_arr = []
            real_time_window_size_arr.append(size)
            self._print_status()

        self.real_time_window_mean_size.append(numpy.average(real_time_window_size_arr))
        q.put({
            "real_time_window_mean_size": self.real_time_window_mean_size,
        })

    def _collect_logical_window_statistics(self, q):
        real_time_start_ts, _, _ = self.trace_iterator.head()[0]
        logical_window_size_arr = init_empty_array(self.logical_window)
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            i = self.trace_index % self.logical_window
            if not i and self.trace_index > 0:
                self.logical_window_mean_size.append(numpy.average(logical_window_size_arr))
                logical_window_size_arr = init_empty_array(self.logical_window)
            logical_window_size_arr[i] = size
            self._print_status()

        logical_window_size_arr = numpy.trim_zeros(logical_window_size_arr)
        self.logical_window_mean_size.append(numpy.average(logical_window_size_arr))
        q.put({
            "logical_window_mean_size": self.logical_window_mean_size,
        })

    def _collect_frequency(self, q):
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            self.freq_counter[key] += 1
            self._print_status()

        for _, counter in self.freq_counter.items():
            freq_index = min(counter, self.MAX_FREQ_COUNT + 1)
            self.frequency_bins[freq_index] += 1
        q.put({
            "frequency_bins": self.frequency_bins
        })

    def _collect_size(self, q):
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            self._put_to_size_bin(size)
            self._print_status()

        q.put({
            "size_bins": list(self.size_bins),
        })

    def _collect_age(self, q):
        for trace in self.trace_iterator:
            timestamp, key, size = trace
            self._put_to_age_bin(key)
            self._print_status()

        q.put({
            "trace_index": self.trace_index,
            "age_bins": list(self.age_bins),
        })

    # def _collect_statistics(self):
    #     real_time_start_ts, _, _ = self.trace_iterator.head()[0]
    #     start_ts = datetime.datetime.now()
    #
    #     logical_window_size_arr = init_empty_array(self.logical_window)
    #     real_time_window_size_arr = []
    #     for trace in self.trace_iterator:
    #         timestamp, key, size = trace
    #         self._put_to_size_bin(size)
    #         self._put_to_age_bin(key)
    #         self.freq_counter[key] += 1
    #
    #         i = self.trace_index % self.logical_window
    #         if not i and self.trace_index > 0:
    #             self.logical_window_mean_size.append(numpy.average(logical_window_size_arr))
    #             # print(f"current_count:{self.trace_index} time_taken:{datetime.datetime.now() - start_ts}")
    #             start_ts = datetime.datetime.now()
    #             logical_window_size_arr = init_empty_array(self.logical_window)
    #
    #         if timestamp - real_time_start_ts > self.real_time_window:
    #             self.real_time_window_mean_size.append(numpy.average(real_time_window_size_arr))
    #             real_time_start_ts = timestamp
    #             real_time_window_size_arr = []
    #
    #         logical_window_size_arr[i] = size
    #         real_time_window_size_arr.append(size)
    #         self.trace_index += 1
    #
    #     # clean up calculation of last window
    #     logical_window_size_arr = numpy.trim_zeros(logical_window_size_arr)
    #     self.logical_window_mean_size.append(numpy.average(logical_window_size_arr))
    #     self.real_time_window_mean_size.append(numpy.average(real_time_window_size_arr))
    #     for _, counter in self.freq_counter.items():
    #         freq_index = min(counter, self.MAX_FREQ_COUNT + 1)
    #         self.frequency_bins[freq_index] += 1

    def as_dict(self):
        return {
            "total_count": self.trace_index,
            "total_size": self.total_obj_size,
            "size_bins": [int(val) for val in self.size_bins],
            "age_bins": [int(val) for val in self.age_bins],
            "frequency_bins": [int(val) for val in self.frequency_bins],
            "logical_window": self.logical_window,
            "logical_window_mean_size": list(self.logical_window_mean_size),
            "real_time_window": self.real_time_window,
            "real_time_window_mean_size": list(self.real_time_window_mean_size),
            "real_time_ts_format": self.trace_iterator.ts_format,
            "unique_obj_size": self.unique_obj_size,
            "unique_obj_count": self.unique_obj_count
        }

    def _put_to_size_bin(self, size):
        bucket_index = min(size.bit_length(), self.max_bin_size)
        self.size_bins[bucket_index] += 1

    def _put_to_age_bin(self, key):
        if not self.last_accessed[key]:
            self.last_accessed[key] = self.trace_index
            return
        age_index = min((self.trace_index - self.last_accessed[key]).bit_length(), self.max_bin_size)
        self.age_bins[age_index] += 1


B = 1
KB = B * 1024
MB = KB * 1024
GB = MB * 1024

if __name__ == "__main__":
    start = datetime.datetime.now()
    trace_filepath = sys.argv[1]
    ts_format = "microseconds"
    trace_iterator = StringCacheTraceIterator(trace_filepath, ts_format)
    statistics = TraceStatistics(trace_iterator)
    print(statistics.as_dict())
    with open(trace_filepath + ".stat", "w") as f:
        json.dump(statistics.as_dict(), f)
    print(datetime.datetime.now() - start)

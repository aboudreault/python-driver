# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
from base import benchmark, BenchmarkThread
import json

log = logging.getLogger(__name__)

def my_row_parser1(request_id):
    pass

def my_row_parser2(request_id):
    # a totally simple non-logic code for simulation purpose
    ids = []
    c = 0
    ids.append(request_id)  # log the request id received
    c += request_id
    json_dict = {'id': request_id, 'c1': 4, 'c2': 5}
    return json.dumps(json_dict)


def my_row_parser3(request_id):
    time.sleep(0.000001)


class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        start = time.time()
        for i in range(self.num_queries):
            key = "{}-{}".format(self.thread_num, i)
            future = self.run_query(key)
            #future.add_callback(my_row_parser3)
            futures.append(future)

        stop = time.time()
        print "All requests sent in ", int(stop-start), " seconds"

        for future in futures:
            future.result()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)

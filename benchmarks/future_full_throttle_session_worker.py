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

import time
import logging

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)

def my_row_parser3(request_id):
    time.sleep(0.000001)

def my_row_parser32(request_id):
    time.sleep(0.000001)

def my_row_parser33(request_id):
    time.sleep(0.000001)

def my_row_parser4(request_id):
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)

def my_row_parser42(request_id):
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)

def my_row_parser43(request_id):
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)
    time.sleep(0.0000001)

class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        session_worker = self.cluster.create_session_worker()
        session_worker.start()

        session_worker.execute_async('USE testkeyspace')
        for i in range(self.num_queries):
            key = "{}-{}".format(self.thread_num, i)
            futures.append(session_worker.execute_async(self.query.format(key=key)))
            futures[-1].add_callback(my_row_parser4)
            #futures[-1].add_callback(my_row_parser42)
            #futures[-1].add_callback(my_row_parser43)
            #if futures[-1].id > 1250:
            #    print 'wtf ', futures[-1].id

        c = 0
        for future in futures:
            c+=1
            future.result()
            #if c == 1250:
            #    print self.thread_num, ' ', c

        session_worker.stop()
        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)

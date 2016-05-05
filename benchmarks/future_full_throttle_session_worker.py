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


class Runner(BenchmarkThread):

    def run(self):
        futures = []

        self.start_profile()

        session_worker = self.cluster.create_session_worker()
        session_worker.start()

        session_worker.execute_async('USE testkeyspace')
        for i in range(self.num_queries):
            key = "{}-{}".format(self.thread_num, i)
            session_worker.execute_async(self.query.format(key=key))

        session_worker.stop()
        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)

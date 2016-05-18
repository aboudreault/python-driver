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
from multiprocessing import Process, Queue as MQueue
from six.moves import queue as Queue

from cassandra import (ConsistencyLevel, AuthenticationFailed,
                       OperationTimedOut, UnsupportedOperation,
                       SchemaTargetType, DriverException)
from cassandra.connection import (ConnectionException, ConnectionShutdown,
                                  ConnectionHeartbeat, ProtocolVersionUnsupported)
from cassandra.pool import (Host, _ReconnectionHandler, _HostReconnectionHandler,
                            HostConnectionPool, HostConnection,
                            NoConnectionsAvailable)
from cassandra.policies import (TokenAwarePolicy, DCAwareRoundRobinPolicy, SimpleConvictionPolicy,
                                ExponentialReconnectionPolicy, HostDistance,
                                RetryPolicy, IdentityTranslator)

log = logging.getLogger(__name__)


class RequestExecutor(object):

    workers = None
    request_queue = None
    response_queue = None
    num_worker = 3

    def __init__(self, session, future_class):  # import hack
        self.workers = []
        self.request_queue = MQueue()
        self.response_queue = MQueue()
        for i in range(self.num_worker):
            w = RequestIOWorker(session, self.request_queue, self.response_queue, future_class)
            w.start()
            self.workers.append(w)

    def shutdown(self):
        for worker in self.workers:
            worker.shutdown()

        for worker in self.workers:
            worker.join()

    def send_request(self, query):
        self.request_queue.put_nowait(query)


class RequestIOWorker(Process):

    hosts = None
    future_class = None
    futures = None

    _pools = None
    _protocol_version = None
    _load_balander = None
    _request_queue = None
    _stop = False

    def __init__(self, session, request_queue, response_queue, future_class):
        super(RequestIOWorker, self).__init__()

        self.session = session
        self.future_class = future_class
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._pools = {}
        self.futures = []
        self._protocol_version = session._protocol_version
        self._load_balancer = session._load_balancer
        self.hosts = session.hosts
        self.connection_class = session.cluster.connection_class

    def run(self):

        self.connection_class.initialize_reactor()

        # create connection pools in parallel
        futures = []
        for host in self.hosts:
            future = self.add_or_renew_pool(host, is_host_addition=False)
            if future is not None:
                futures.append(future)

        for future in futures:
            future.result()

        #time.sleep(30)

        while True:
            try:
                request = self._request_queue.get()
                if request == 'STOP':
                    self._stop = True
                else:
                    future = self.future_class(self, request.message, request.query, request.timeout,
                                            metrics=request.metrics, prepared_statement=request.prepared_statement)
                    future.send_request()
                    future.add_callback(self.handle_results, request.id)
                    future.add_errback(self.handle_results, request.id)
                    self.futures.append(future)
            except Queue.Empty:
                time.sleep(0.1)  # use less cpu

            if self._stop:
                #self.wait_futures()
                break

    def handle_results(self, rows, id):
        self._response_queue.put_nowait(id)

    def wait_futures(self):
        while True:
            try:
                future = self.futures.pop()
                future.result()
            except IndexError:
                break

    def shutdown(self):
        self._request_queue.put('STOP')

    def add_or_renew_pool(self, host, is_host_addition):
        distance = self._load_balancer.distance(host)
        if distance == HostDistance.IGNORED:
            return None

        def run_add_or_renew_pool():
            try:
                if self._protocol_version >= 3:
                    new_pool = HostConnection(host, distance, self.session)
                else:
                    new_pool = HostConnectionPool(host, distance, self.session)
            except AuthenticationFailed as auth_exc:
                conn_exc = ConnectionException(str(auth_exc), host=host)
                #self.cluster.signal_connection_failure(host, conn_exc, is_host_addition)
                return False
            except Exception as conn_exc:
                log.warning("Failed to create connection pool for new host %s:",
                            host, exc_info=conn_exc)
                # the host itself will still be marked down, so we need to pass
                # a special flag to make sure the reconnector is created
                #self.cluster.signal_connection_failure(
                #    host, conn_exc, is_host_addition, expect_host_to_be_down=True)
                return False

            previous = self._pools.get(host)
            self._pools[host] = new_pool
            log.debug("Added pool for host %s to session", host)
            if previous:
                previous.shutdown()

            return True

        return self.session.cluster.executor.submit(run_add_or_renew_pool)

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
import pickle
import zmq
import copy

import logging
from collections import deque

from multiprocessing import Process, Event
from threading import Thread, Lock

from zmq.eventloop import ioloop, zmqstream

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
    num_worker = 6
    proxy_class = None

    request_lock = Lock()
    request_id = 0
    requests = {}

    deque = None
    deque_lock = Lock()

    response_thread = None

    def __init__(self, session, proxy_class, future_class):  # import hack
        self.workers = []
        self.proxy_class = proxy_class
        self.deque = deque()
        self.zmq_context = zmq.Context()

        self._thread = Thread(target=self._run_request_loop, name="request_event_loop")
        self._thread.daemon = True
        self._thread.start()

        self._thread = Thread(target=self._run_response_loop, name="response_event_loop")
        self._thread.daemon = True
        self._thread.start()

        for i in range(self.num_worker):
             w = RequestIOWorker(session, future_class)
             w.start()
             self.workers.append(w)

    def shutdown(self):
        for worker in self.workers:
            with self.deque_lock:
                self.deque.append('STOP')

        for worker in self.workers:
            worker.join()

        # should stop threads properly...

    def send_request(self, query):
        with self.request_lock:
            request_id = self.request_id
            self.request_id += 1  # test purpose..

        fp = self.proxy_class(request_id, query)

        with self.deque_lock:
            self.deque.append(copy.copy(fp))

        fp.init_event()
        self.requests[request_id] = fp
        return fp

    def _run_request_loop(self):
        print 'Starting request loop'

        request_socket = self.zmq_context.socket(zmq.PUSH)
        request_socket.bind("tcp://127.0.0.1:5557")

        while True:
            try:
                with self.deque_lock:
                    next_request = self.deque.popleft()
            except IndexError:
                time.sleep(0.1)
                continue

            try:
                request_socket.send_pyobj(obj=next_request, flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                with self.deque_lock:
                    self.deque.appendleft(next_request)

    def _run_response_loop(self):
        print 'Starting response loop'

        def on_recv(msg):
            id = pickle.loads(msg[0])
            if id in self.requests:  # bah...
                f = self.requests[id]
                del self.requests[id]
                f.set_event()

        response_socket = self.zmq_context.socket(zmq.PULL)
        response_socket.bind("tcp://127.0.0.1:5558")
        response_stream = zmqstream.ZMQStream(response_socket)
        response_stream.on_recv(on_recv)

        ioloop.IOLoop.instance().start()


class RequestIOWorker(Process):

    hosts = None
    future_class = None
    futures = None

    _pools = None
    _protocol_version = None
    _load_balander = None
    _stop = False
    zmq_context = None
    zmq_request_socket = None
    zmq_response_socket = None
    c = 0

    def __init__(self, session, future_class):
        super(RequestIOWorker, self).__init__()

        self.session = session
        self.future_class = future_class
        self._pools = {}
        self.futures = []
        self._protocol_version = session._protocol_version
        self._load_balancer = session._load_balancer
        self.hosts = session.hosts
        self.connection_class = session.cluster.connection_class

    def run(self):

        self.connection_class.initialize_reactor()

        self.zmq_context = zmq.Context()
        self.zmq_request_socket = self.zmq_context.socket(zmq.PULL)
        self.zmq_request_socket.connect("tcp://127.0.0.1:5557")

        self.zmq_response_socket = self.zmq_context.socket(zmq.PUSH)
        self.zmq_response_socket.connect("tcp://127.0.0.1:5558")

        # create connection pools in parallel
        futures = []
        for host in self.hosts:
            future = self.add_or_renew_pool(host, is_host_addition=False)
            if future is not None:
                futures.append(future)

        for future in futures:
            future.result()

        f = True
        while True:
            request = self.zmq_request_socket.recv_pyobj()
            if f:
                self.start = time.time()
                f = False
            if request == 'STOP':
                self._stop = True
            else:
                future = self.session._create_response_future(request.query, None, False, None, self.session.default_timeout, pools=self._pools)
                future.send_request()
                future.add_callback(self.handle_results, request.id)
                future.add_errback(self.handle_results, request.id)
                #self.futures.append(future)

            if self._stop:
                self.zmq_request_socket.close()
                self.zmq_response_socket.close()
                break

    def handle_results(self, rows, id):
        #we should handle response properly
        self.zmq_response_socket.send_pyobj(obj=id)

    def wait_futures(self):
        while True:
            try:
                future = self.futures.pop()
                future.result()
            except IndexError:
                break

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

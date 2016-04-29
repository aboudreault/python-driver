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
from threading import Lock, RLock
from collections import defaultdict
import six

import weakref
from weakref import WeakValueDictionary
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra.policies import HostDistance
from cassandra import (ConsistencyLevel, AuthenticationFailed,
                       OperationTimedOut, UnsupportedOperation,
                       SchemaTargetType)
from cassandra.protocol import (QueryMessage, ResultMessage,
                                ErrorMessage, ReadTimeoutErrorMessage,
                                WriteTimeoutErrorMessage,
                                UnavailableErrorMessage,
                                OverloadedErrorMessage,
                                PrepareMessage, ExecuteMessage,
                                PreparedQueryNotFound,
                                IsBootstrappingErrorMessage,
                                BatchMessage, RESULT_KIND_PREPARED,
                                RESULT_KIND_SET_KEYSPACE, RESULT_KIND_ROWS,
                                RESULT_KIND_SCHEMA_CHANGE, MIN_SUPPORTED_VERSION,
                                ProtocolHandler)

log = logging.getLogger(__name__)

DEFAULT_MIN_REQUESTS = 5
DEFAULT_MAX_REQUESTS = 100

DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST = 2
DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST = 8

DEFAULT_MIN_CONNECTIONS_PER_REMOTE_HOST = 1
DEFAULT_MAX_CONNECTIONS_PER_REMOTE_HOST = 2


_NOT_SET = object()

cimport ccluster

cdef class CCluster2(object):
    cdef ccluster.CassCluster* _cluster
    cdef ccluster.CassSession* _session

    def __cinit__(self):
        self._cluster = cass_cluster_new()
        cass_cluster_set_contact_points(self._cluster, "127.0.0.1")
        cass_cluster_set_queue_size_io(self._cluster, 1000000)
        self._session = cass_session_new()

        cdef ccluster.CassFuture* future = cass_session_connect_keyspace(self._session, self._cluster, "testkeyspace")

        cass_future_wait(future)
        rc = cass_future_error_code(future);
        if rc != CASS_OK:
            print('error')
        else:
            print('all good bro, connected to cluster')

    cpdef execute(self, const char* query):
        cdef ccluster.CassStatement* statement
        with nogil:
            statement = cass_statement_new(query, 0)
            future = cass_session_execute(self._session, statement)
            cass_future_wait(future)

            rc = cass_future_error_code(future)
        if rc != CASS_OK:
            print('error: {0}'.format(rc))


cdef void on_result(CassFuture* future, void* data) with gil:
    cdef const CassResult* result
    cdef CassIterator* iterator
    cdef const CassRow* row
    cdef CassError rc
    cdef const CassValue *value1, *value2, *value3
    cdef const char* str1, *str2, *str3
    cdef size_t str_size1, str_size2, str_size3
    results = ([u'thekey', u'col0', u'col1'],)  # test purpose, I know what I'm parsing

    with nogil:
        result = cass_future_get_result(future)
        rc = cass_future_error_code(future)
        if rc != CASS_OK:
            with gil:
                print cass_error_desc(rc)
                results = None
        else:
            # TESTING PURPOSE, row parsing...
            iterator = cass_iterator_from_result(result)
            while cass_iterator_next(iterator):
                row = cass_iterator_get_row(iterator)
                value1 = cass_row_get_column(row, 0)
                value2 = cass_row_get_column(row, 1)
                value3 = cass_row_get_column(row, 2)
                cass_value_get_string(value1, &str1, &str_size1)
                cass_value_get_string(value2, &str2, &str_size2)
                cass_value_get_string(value3, &str3, &str_size3)
                with gil:
                    results += ([(<bytes>str1[:str_size1], <bytes>str2[:str_size2], <bytes>str3[:str_size3],)],)
                #    print results
                #string_value = cass_value_get_string(value2, &string_value)
                #string_value = cass_value_get_string(value3, &string_value)

    if results and len(results) < 2:
        results = None

    response = ResultMessage(2 if results else 1,  results)
    pyfuture = (<object>data)
    pyfuture._set_result(response)
    pyfuture._event.set()


cdef class ClusterImpl(object):
    cdef ccluster.CassCluster* _cluster
    cdef ccluster.CassSession* _session

    def __cinit__(self):
        self._cluster = cass_cluster_new()

    def __init__(self):
        # test purpose, handle the session here.
        cass_cluster_set_contact_points(self._cluster, "127.0.0.1")
        cass_cluster_set_num_threads_io(self._cluster, 1)
        #cass_cluster_set_max_requests_per_flush(self._cluster, 10000)
        #cass_cluster_set_pending_requests_low_water_mark(self._cluster, 5000)
        #cass_cluster_set_pending_requests_high_water_mark(self._cluster, 10000)
        #cass_cluster_set_write_bytes_high_water_mark(self._cluster, 1000000)

        self._session = cass_session_new()

        cdef ccluster.CassFuture* future = cass_session_connect_keyspace(self._session, self._cluster, "testkeyspace")
        cass_future_wait(future)
        rc = cass_future_error_code(future);
        if rc != CASS_OK:
            print('error')
        else:
            print('all good bro, connected to cluster')

    cpdef execute_message2(self, future):
        #cdef ccluster.ResponseFuture* response_future = cass_response_future_new(self._cluster)
        cdef const char* query = future.message.query
        with nogil:
            statement = cass_statement_new(query, 0)
            #cass_statement_set_serial_consistency(statement, ...)
            future_ = cass_session_execute(self._session, statement)
            cass_future_wait(future_)
        response = ResultMessage(1,  None)
        future._set_result(response)
        future._event.set()

    cpdef execute_message(self, future):
        #cdef ccluster.ResponseFuture* response_future = cass_response_future_new(self._cluster)
        cdef const char* query = future.message.query
        with nogil:
            statement = cass_statement_new(query, 0)
            #cass_statement_set_serial_consistency(statement, ...)
            future_ = cass_session_execute(self._session, statement)
        cass_future_set_callback(future_, on_result, <void*>future)
            #cass_future_wait(future_)

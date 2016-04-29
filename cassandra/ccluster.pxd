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

cdef extern from "cassandra.h":
    ctypedef enum CassError:
        CASS_OK = 0
        #MORE_CODES...
    ctypedef enum cass_bool_t:
        cass_false = 0
        cass_true = 1

    ctypedef struct CassCluster
    ctypedef struct CassSession
    ctypedef struct CassFuture
    ctypedef struct CassResult
    ctypedef struct ResponseFuture
    ctypedef struct CassStatement
    ctypedef struct CassResult
    ctypedef struct CassIterator
    ctypedef struct CassRow
    ctypedef struct CassValue
    ctypedef struct CassString
    ctypedef void (*CassFutureCallback)(CassFuture* future, void* data)

    CassCluster* cass_cluster_new()
    CassSession* cass_session_new()
    CassError cass_cluster_set_contact_points(CassCluster* cluster, const char* contact_points)
    CassFuture* cass_session_connect_keyspace(CassSession* session, const CassCluster* cluster, const char* keyspace)
    void cass_future_wait(CassFuture* future) nogil
    CassError cass_future_error_code(CassFuture* future) nogil

    CassFuture* cass_session_execute(CassSession* session, const CassStatement* statement) nogil
    CassStatement* cass_statement_new(const char* query, size_t parameter_count) nogil

    ResponseFuture*  cass_response_future_new(CassCluster* cluster)

    CassError cass_cluster_set_num_threads_io(CassCluster* cluster, unsigned num_threads) nogil
    CassError cass_cluster_set_max_requests_per_flush(CassCluster* cluster, unsigned num_requests)
    CassError cass_cluster_set_write_bytes_high_water_mark(CassCluster* cluster, unsigned num_bytes)
    CassError cass_cluster_set_write_bytes_low_water_mark(CassCluster* cluster, unsigned num_bytes)
    CassError cass_cluster_set_pending_requests_high_water_mark(CassCluster* cluster, unsigned num_requests)
    CassError cass_cluster_set_pending_requests_low_water_mark(CassCluster* cluster, unsigned num_requests)


    CassError cass_future_set_callback(CassFuture* future, CassFutureCallback callback, void* data) nogil
    CassResult* cass_future_get_result(CassFuture* future) nogil
    CassIterator* cass_iterator_from_result(const CassResult* result) nogil
    const CassRow* cass_iterator_get_row(const CassIterator* iterator) nogil
    cass_bool_t cass_iterator_next(CassIterator* iterator) nogil
    size_t cass_result_row_count(const CassResult* result) nogil
    const char* cass_error_desc(CassError error) nogil
    const CassValue* cass_row_get_column(const CassRow* row, size_t index) nogil
    CassError cass_value_get_string(const CassValue* value, const char** output, size_t* output_size) nogil
    CassError cass_cluster_set_queue_size_io(CassCluster* cluster, unsigned queue_size)

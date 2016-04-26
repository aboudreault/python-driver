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

    ctypedef struct CassCluster
    ctypedef struct CassSession
    ctypedef struct CassFuture
    ctypedef struct ResponseFuture
    ctypedef struct CassStatement

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
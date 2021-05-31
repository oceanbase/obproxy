/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifdef STAT_EVENT_ADD_DEF
// NETWORK
STAT_EVENT_ADD_DEF(RPC_PACKET_IN, "rpc packet in", ObStatClassIds::NETWORK, "rpc packet in", 10000, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_IN_BYTES, "rpc packet in bytes", ObStatClassIds::NETWORK, "rpc packet in bytes", 10001, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT, "rpc packet out", ObStatClassIds::NETWORK, "rpc packet out", 10002, true, true)
STAT_EVENT_ADD_DEF(RPC_PACKET_OUT_BYTES, "rpc packet out bytes", ObStatClassIds::NETWORK, "rpc packet out bytes", 10003, true, true)
STAT_EVENT_ADD_DEF(RPC_DELIVER_FAIL, "rpc deliver fail", ObStatClassIds::NETWORK, "rpc deliver fail", 10004, true, true)
STAT_EVENT_ADD_DEF(RPC_NET_DELAY, "rpc net delay", ObStatClassIds::NETWORK, "rpc net delay", 10005, true, true)
STAT_EVENT_ADD_DEF(RPC_NET_FRAME_DELAY, "rpc net frame delay", ObStatClassIds::NETWORK, "rpc net frame delay", 10006, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN, "mysql packet in", ObStatClassIds::NETWORK, "mysql packet in", 10007, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_IN_BYTES, "mysql packet in bytes", ObStatClassIds::NETWORK, "mysql packet in bytes", 10008, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT, "mysql packet out", ObStatClassIds::NETWORK, "mysql packet out", 10009, true, true)
STAT_EVENT_ADD_DEF(MYSQL_PACKET_OUT_BYTES, "mysql packet out bytes", ObStatClassIds::NETWORK, "mysql packet out bytes", 10010, true, true)
STAT_EVENT_ADD_DEF(MYSQL_DELIVER_FAIL, "mysql deliver fail", ObStatClassIds::NETWORK, "mysql deliver fail", 10011, true, true)

// QUEUE
// STAT_EVENT_ADD_DEF(REQUEST_QUEUED_COUNT, "REQUEST_QUEUED_COUNT", QUEUE, "REQUEST_QUEUED_COUNT")
STAT_EVENT_ADD_DEF(REQUEST_ENQUEUE_COUNT, "request enqueue count", ObStatClassIds::QUEUE, "request enqueue count", 20000, true, true)
STAT_EVENT_ADD_DEF(REQUEST_DEQUEUE_COUNT, "request dequeue count", ObStatClassIds::QUEUE, "request dequeue count", 20001, true, true)
STAT_EVENT_ADD_DEF(REQUEST_QUEUE_TIME, "request queue time", ObStatClassIds::QUEUE, "request queue time", 20002, true, true)

// TRANS
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_TIME, "trans commit log sync time", ObStatClassIds::TRANS, "trans commit log sync time", 30000, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SYNC_COUNT, "trans commit log sync count", ObStatClassIds::TRANS, "trans commit log sync count", 30001, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_LOG_SUBMIT_COUNT, "trans commit log submit count", ObStatClassIds::TRANS, "trans commit log submit count", 30002, true, true)
STAT_EVENT_ADD_DEF(TRANS_SYSTEM_TRANS_COUNT, "trans system trans count", ObStatClassIds::TRANS, "trans system trans count", 30003, true, true)
STAT_EVENT_ADD_DEF(TRANS_USER_TRANS_COUNT, "trans user trans count", ObStatClassIds::TRANS, "trans user trans count", 30004, true, true)
STAT_EVENT_ADD_DEF(TRANS_START_COUNT, "trans start count", ObStatClassIds::TRANS, "trans start count", 30005, true, true)
STAT_EVENT_ADD_DEF(TRANS_TOTAL_USED_TIME, "trans total used time", ObStatClassIds::TRANS, "trans total used time", 30006, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_COUNT, "trans commit count", ObStatClassIds::TRANS, "trans commit count", 30007, true, true)
STAT_EVENT_ADD_DEF(TRANS_COMMIT_TIME, "trans commit time", ObStatClassIds::TRANS, "trans commit time", 30008, true, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_COUNT, "trans rollback count", ObStatClassIds::TRANS, "trans rollback count", 30009, true, true)
STAT_EVENT_ADD_DEF(TRANS_ROLLBACK_TIME, "trans rollback time", ObStatClassIds::TRANS, "trans rollback time", 30010, true, true)
STAT_EVENT_ADD_DEF(TRANS_TIMEOUT_COUNT, "trans timeout count", ObStatClassIds::TRANS, "trans timeout count", 30011, true, true)
STAT_EVENT_ADD_DEF(TRANS_SINGLE_PARTITION_COUNT, "trans single partition count", ObStatClassIds::TRANS, "trans single partition count", 30012, true, true)
STAT_EVENT_ADD_DEF(TRANS_MULTI_PARTITION_COUNT, "trans multi partition count", ObStatClassIds::TRANS, "trans multi partition count", 30013, true, true)
//STAT_EVENT_ADD_DEF(TRANS_DISTRIBUTED_STMT_COUNT, "trans distributed stmt count", TRANS, "trans distributed stmt count", 30014)
//STAT_EVENT_ADD_DEF(TRANS_LOCAL_STMT_COUNT, "trans local stmt count", TRANS, "trans local stmt count", 30015)
//STAT_EVENT_ADD_DEF(TRANS_REMOTE_STMT_COUNT, "trans remote stmt count", TRANS, "trans remote stmt count", 30016)
STAT_EVENT_ADD_DEF(TRANS_ZERO_PARTITION_COUNT, "trans zero partition count", ObStatClassIds::TRANS, "trans zero partition count", 30017, true, true)

// SQL
//STAT_EVENT_ADD_DEF(PLAN_CACHE_HIT, "PLAN_CACHE_HIT", SQL, "PLAN_CACHE_HIT")
//STAT_EVENT_ADD_DEF(PLAN_CACHE_MISS, "PLAN_CACHE_MISS", SQL, "PLAN_CACHE_MISS")
STAT_EVENT_ADD_DEF(SQL_SELECT_COUNT, "sql select count", ObStatClassIds::SQL, "sql select count", 40000, true, true)
STAT_EVENT_ADD_DEF(SQL_SELECT_TIME, "sql select time", ObStatClassIds::SQL, "sql select time", 40001, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_COUNT, "sql insert count", ObStatClassIds::SQL, "sql insert count", 40002, true, true)
STAT_EVENT_ADD_DEF(SQL_INSERT_TIME, "sql insert time", ObStatClassIds::SQL, "sql insert time", 40003, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_COUNT, "sql replace count", ObStatClassIds::SQL, "sql replace count", 40004, true, true)
STAT_EVENT_ADD_DEF(SQL_REPLACE_TIME, "sql replace time", ObStatClassIds::SQL, "sql replace time", 40005, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_COUNT, "sql update count", ObStatClassIds::SQL, "sql update count", 40006, true, true)
STAT_EVENT_ADD_DEF(SQL_UPDATE_TIME, "sql update time", ObStatClassIds::SQL, "sql update time", 40007, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_COUNT, "sql delete count", ObStatClassIds::SQL, "sql delete count", 40008, true, true)
STAT_EVENT_ADD_DEF(SQL_DELETE_TIME, "sql delete time", ObStatClassIds::SQL, "sql delete time", 40009, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_COUNT, "sql other count", ObStatClassIds::SQL, "sql other count", 40018, true, true)
STAT_EVENT_ADD_DEF(SQL_OTHER_TIME, "sql other time", ObStatClassIds::SQL, "sql other time", 40019, true, true)

STAT_EVENT_ADD_DEF(SQL_LOCAL_COUNT, "sql local count", ObStatClassIds::SQL, "sql local count", 40010, true, true)
//STAT_EVENT_ADD_DEF(SQL_LOCAL_TIME, "SQL_LOCAL_TIME", SQL, "SQL_LOCAL_TIME")
STAT_EVENT_ADD_DEF(SQL_REMOTE_COUNT, "sql remote count", ObStatClassIds::SQL, "sql remote count", 40011, true, true)
//STAT_EVENT_ADD_DEF(SQL_REMOTE_TIME, "SQL_REMOTE_TIME", SQL, "SQL_REMOTE_TIME")
STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_COUNT, "sql distributed count", ObStatClassIds::SQL, "sql distributed count", 40012, true, true)
//STAT_EVENT_ADD_DEF(SQL_DISTRIBUTED_TIME, "SQL_DISTRIBUTED_TIME", SQL, "SQL_DISTRIBUTED_TIME")
STAT_EVENT_ADD_DEF(ACTIVE_SESSIONS, "active sessions", ObStatClassIds::SQL, "active sessions", 40013, true, true)
STAT_EVENT_ADD_DEF(SQL_SINGLE_QUERY_COUNT, "single query count", ObStatClassIds::SQL, "single query count", 40014, true, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_QUERY_COUNT, "multiple query count", ObStatClassIds::SQL, "multiple query count", 40015, true, true)
STAT_EVENT_ADD_DEF(SQL_MULTI_ONE_QUERY_COUNT, "multiple query with one stmt count", ObStatClassIds::SQL, "multiple query with one stmt count", 40016, true, true)

STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_COUNT, "sql inner select count", ObStatClassIds::SQL, "sql inner select count", 40100, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_SELECT_TIME, "sql inner select time", ObStatClassIds::SQL, "sql inner select time", 40101, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_COUNT, "sql inner insert count", ObStatClassIds::SQL, "sql inner insert count", 40102, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_INSERT_TIME, "sql inner insert time", ObStatClassIds::SQL, "sql inner insert time", 40103, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_COUNT, "sql inner replace count", ObStatClassIds::SQL, "sql inner replace count", 40104, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_REPLACE_TIME, "sql inner replace time", ObStatClassIds::SQL, "sql inner replace time", 40105, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_COUNT, "sql inner update count", ObStatClassIds::SQL, "sql inner update count", 40106, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_UPDATE_TIME, "sql inner update time", ObStatClassIds::SQL, "sql inner update time", 40107, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_COUNT, "sql inner delete count", ObStatClassIds::SQL, "sql inner delete count", 40108, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_DELETE_TIME, "sql inner delete time", ObStatClassIds::SQL, "sql inner delete time", 40109, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_COUNT, "sql inner other count", ObStatClassIds::SQL, "sql inner other count", 40110, true, true)
STAT_EVENT_ADD_DEF(SQL_INNER_OTHER_TIME, "sql inner other time", ObStatClassIds::SQL, "sql inner other time", 40111, true, true)

// CACHE
STAT_EVENT_ADD_DEF(ROW_CACHE_HIT, "row cache hit", ObStatClassIds::CACHE, "row cache hit", 50000, true, true)
STAT_EVENT_ADD_DEF(ROW_CACHE_MISS, "row cache miss", ObStatClassIds::CACHE, "row cache miss", 50001, true, true)
STAT_EVENT_ADD_DEF(BLOCK_INDEX_CACHE_HIT, "block index cache hit", ObStatClassIds::CACHE, "block index cache hit", 50002, true, true)
STAT_EVENT_ADD_DEF(BLOCK_INDEX_CACHE_MISS, "block index cache miss", ObStatClassIds::CACHE, "block index cache miss", 50003, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_HIT, "bloom filter cache hit", ObStatClassIds::CACHE, "bloom filter cache hit", 50004, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_CACHE_MISS, "bloom filter cache miss", ObStatClassIds::CACHE, "bloom filter cache miss", 50005, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_FILTS, "bloom filter filts", ObStatClassIds::CACHE, "bloom filter filts", 50006, true, true)
STAT_EVENT_ADD_DEF(BLOOM_FILTER_PASSES, "bloom filter passes", ObStatClassIds::CACHE, "bloom filter passes", 50007, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_HIT, "block cache hit", ObStatClassIds::CACHE, "block cache hit", 50008, true, true)
STAT_EVENT_ADD_DEF(BLOCK_CACHE_MISS, "block cache miss", ObStatClassIds::CACHE, "block cache miss", 50009, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_HIT, "location cache hit", ObStatClassIds::CACHE, "location cache hit", 50010, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_MISS, "location cache miss", ObStatClassIds::CACHE, "location cache miss", 50011, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_WAIT, "location cache wait", ObStatClassIds::CACHE, "location cache wait", 50012, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_HIT, "location cache get hit from proxy virtual table", ObStatClassIds::CACHE, "location cache get hit from proxy virtual table", 50013, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_PROXY_MISS, "location cache get miss from proxy virtual table", ObStatClassIds::CACHE, "location cache get miss from proxy virtual table", 50014, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION, "location cache nonblock renew", ObStatClassIds::CACHE, "location cache nonblock renew", 50015, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_CLEAR_LOCATION_IGNORED, "location cache nonblock renew ignored", ObStatClassIds::CACHE, "location cache nonblock renew ignored", 50016, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_HIT, "location nonblock get hit", ObStatClassIds::CACHE, "location nonblock get hit", 50017, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_NONBLOCK_MISS, "location nonblock get miss", ObStatClassIds::CACHE, "location nonblock get miss", 50018, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_LEADER, "location check leader count", ObStatClassIds::CACHE, "location check leader count", 50019, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK_MEMBER, "location check member count", ObStatClassIds::CACHE, "location check member count", 50020, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RPC_CHECK, "location check rpc succ count", ObStatClassIds::CACHE, "location check rpc succ count", 50021, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW, "location cache renew", ObStatClassIds::CACHE, "location cache renew", 50022, true, true)
STAT_EVENT_ADD_DEF(LOCATION_CACHE_RENEW_IGNORED, "location cache renew ignored", ObStatClassIds::CACHE, "location cache renew ignored", 50023, true, true)

// STORAGE
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_READS, "MEMSTORE_LOGICAL_READS", STORAGE, "MEMSTORE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(MEMSTORE_LOGICAL_BYTES, "MEMSTORE_LOGICAL_BYTES", STORAGE, "MEMSTORE_LOGICAL_BYTES")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_READS, "SSTABLE_LOGICAL_READS", STORAGE, "SSTABLE_LOGICAL_READS")
//STAT_EVENT_ADD_DEF(SSTABLE_LOGICAL_BYTES, "SSTABLE_LOGICAL_BYTES", STORAGE, "SSTABLE_LOGICAL_BYTES")
STAT_EVENT_ADD_DEF(IO_READ_COUNT, "io read count", ObStatClassIds::STORAGE, "io read count", 60000, true, true)
STAT_EVENT_ADD_DEF(IO_READ_DELAY, "io read delay", ObStatClassIds::STORAGE, "io read delay", 60001, true, true)
STAT_EVENT_ADD_DEF(IO_READ_BYTES, "io read bytes", ObStatClassIds::STORAGE, "io read bytes", 60002, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_COUNT, "io write count", ObStatClassIds::STORAGE, "io write count", 60003, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_DELAY, "io write delay", ObStatClassIds::STORAGE, "io write delay", 60004, true, true)
STAT_EVENT_ADD_DEF(IO_WRITE_BYTES, "io write bytes", ObStatClassIds::STORAGE, "io write bytes", 60005, true, true)
//STAT_EVENT_ADD_DEF(CLOG_SIZE, "CLOG_SIZE", STORAGE, "CLOG_SIZE")
//STAT_EVENT_ADD_DEF(CLOG_ID, "CLOG_ID", STORAGE, "CLOG_ID")
//STAT_EVENT_ADD_DEF(CLOG_REPLAY_COUNT, "CLOG_REPLAY_COUNT", STORAGE, "CLOG_REPLAY_COUNT")
//STAT_EVENT_ADD_DEF(CLOG_REPLAY_DELAY, "CLOG_REPLAY_DELAY", STORAGE, "CLOG_REPLAY_DELAY")
//STAT_EVENT_ADD_DEF(CLOG_SLOW_REPLAY_COUNT, "CLOG_SLOW_REPLAY_COUNT", STORAGE, "CLOG_SLOW_REPLAY_COUNT")
//STAT_EVENT_ADD_DEF(CLOG_SLOW_REPLAY_DELAY, "CLOG_SLOW_REPLAY_DELAY", STORAGE, "CLOG_SLOW_REPLAY_DELAY")
//STAT_EVENT_ADD_DEF(CLOG_SYNC_COUNT, "CLOG_SYNC_COUNT", STORAGE, "CLOG_SYNC_COUNT")
//STAT_EVENT_ADD_DEF(CLOG_SLAVE_RECV_DELAY, "CLOG_SLAVE_RECV_DELAY", STORAGE, "CLOG_SLAVE_RECV_DELAY")
//STAT_EVENT_ADD_DEF(CLOG_SLAVE_QUEUE_DELAY, "CLOG_SLAVE_QUEUE_DELAY", STORAGE, "CLOG_SLAVE_QUEUE_DELAY")
//STAT_EVENT_ADD_DEF(CLOG_SLAVE_FLUSH_DELAY, "CLOG_SLAVE_FLUSH_DELAY", STORAGE, "CLOG_SLAVE_FLUSH_DELAY")
//STAT_EVENT_ADD_DEF(MEMSTORE_SIZE, "MEMSTORE_SIZE", STORAGE, "MEMSTORE_SIZE")
//STAT_EVENT_ADD_DEF(PARTITIONS_COUNT, "PARTITIONS_COUNT", STORAGE, "PARTITIONS_COUNT")
//STAT_EVENT_ADD_DEF(MACRO_BLOCK_COUNT, "MACRO_BLOCK_COUNT", STORAGE, "MACRO_BLOCK_COUNT")
//STAT_EVENT_ADD_DEF(MICRO_BLOCK_COUNT, "MICRO_BLOCK_COUNT", STORAGE, "MICRO_BLOCK_COUNT")
//STAT_EVENT_ADD_DEF(MERGE_START_TIMESTAMP, "MERGE_START_TIMESTAMP", STORAGE, "MERGE_START_TIMESTAMP")
//STAT_EVENT_ADD_DEF(LAST_MERGE_TIME, "LAST_MERGE_TIME", STORAGE, "LAST_MERGE_TIME")
//STAT_EVENT_ADD_DEF(MERGING_VERSION, "MERGING_VERSION", STORAGE, "MERGING_VERSION")
//STAT_EVENT_ADD_DEF(MERGED_PARTITIONS, "MERGED_PARTITIONS", STORAGE, "MERGED_PARTITIONS")
//STAT_EVENT_ADD_DEF(UNMERGED_PARTITIONS, "UNMERGED_PARTITIONS", STORAGE, "UNMERGED_PARTITIONS")
//STAT_EVENT_ADD_DEF(MERGED_VERSION, "MERGED_VERSION", STORAGE, "MERGED_VERSION")
//STAT_EVENT_ADD_DEF(FROZEN_VERSION, "FROZEN_VERSION", STORAGE, "FROZEN_VERSION")
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_COUNT, "memstore scan count", ObStatClassIds::STORAGE, "memstore scan count", 60006, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_SUCC_COUNT, "memstore scan succ count", ObStatClassIds::STORAGE, "memstore scan succ count", 60007, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_FAIL_COUNT, "memstore scan fail count", ObStatClassIds::STORAGE, "memstore scan fail count", 60008, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_COUNT, "memstore get count", ObStatClassIds::STORAGE, "memstore get count", 60009, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_SUCC_COUNT, "memstore get succ count", ObStatClassIds::STORAGE, "memstore get succ count", 60010, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_FAIL_COUNT, "memstore get fail count", ObStatClassIds::STORAGE, "memstore get fail count", 60011, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_COUNT, "memstore apply count", ObStatClassIds::STORAGE, "memstore apply count", 60012, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_SUCC_COUNT, "memstore apply succ count", ObStatClassIds::STORAGE, "memstore apply succ count", 60013, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_FAIL_COUNT, "memstore apply fail count", ObStatClassIds::STORAGE, "memstore apply fail count", 60014, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COUNT, "memstore row count", ObStatClassIds::STORAGE, "memstore row count", 60015, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_GET_TIME, "memstore get time", ObStatClassIds::STORAGE, "memstore get time", 60016, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_SCAN_TIME, "memstore scan time", ObStatClassIds::STORAGE, "memstore scan time", 60017, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_APPLY_TIME, "memstore apply time", ObStatClassIds::STORAGE, "memstore apply time", 60018, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_SUCC_COUNT, "memstore read lock succ count", ObStatClassIds::STORAGE, "memstore read lock succ count", 60019, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_READ_LOCK_FAIL_COUNT, "memstore read lock fail count", ObStatClassIds::STORAGE, "memstore read lock fail count", 60020, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_SUCC_COUNT, "memstore write lock succ count", ObStatClassIds::STORAGE, "memstore write lock succ count", 60021, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WRITE_LOCK_FAIL_COUNT, "memstore write lock fail count", ObStatClassIds::STORAGE, "memstore write lock fail count", 60022, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_WRITE_LOCK_TIME, "memstore wait write lock time", ObStatClassIds::STORAGE, "memstore wait write lock time", 60023, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_WAIT_READ_LOCK_TIME, "memstore wait read lock time", ObStatClassIds::STORAGE, "memstore wait read lock time", 60024, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_COUNT, "io read micro index count", ObStatClassIds::STORAGE, "io read micro index count", 60025, true, true)
STAT_EVENT_ADD_DEF(IO_READ_MICRO_INDEX_BYTES, "io read micro index bytes", ObStatClassIds::STORAGE, "io read micro index bytes", 60026, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_COUNT, "io prefetch micro block count", ObStatClassIds::STORAGE, "io prefetch micro block count", 60027, true, true)
STAT_EVENT_ADD_DEF(IO_READ_PREFETCH_MICRO_BYTES, "io prefetch micro block bytes", ObStatClassIds::STORAGE, "io prefetch micro block bytes", 60028, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_COUNT, "io read uncompress micro block count", ObStatClassIds::STORAGE, "io read uncompress micro block count", 60029, true, true)
STAT_EVENT_ADD_DEF(IO_READ_UNCOMP_MICRO_BYTES, "io read uncompress micro block bytes", ObStatClassIds::STORAGE, "io read uncompress micro block bytes", 60030, true, true)
STAT_EVENT_ADD_DEF(STORAGE_READ_ROW_COUNT, "storage read row count", ObStatClassIds::STORAGE, "storage read row count", 60031, true, true)
STAT_EVENT_ADD_DEF(STORAGE_DELETE_ROW_COUNT, "storage delete row count", ObStatClassIds::STORAGE, "storage delete row count", 60032, true, true)
STAT_EVENT_ADD_DEF(STORAGE_INSERT_ROW_COUNT, "storage insert row count", ObStatClassIds::STORAGE, "storage insert row count", 60033, true, true)
STAT_EVENT_ADD_DEF(STORAGE_UPDATE_ROW_COUNT, "storage update row count", ObStatClassIds::STORAGE, "storage update row count", 60034, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_MUTATOR_REPLAY_TIME, "memstore mutator replay time", ObStatClassIds::STORAGE, "memstore mutator replay time", 60035, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_MUTATOR_REPLAY_COUNT, "memstore mutator replay count", ObStatClassIds::STORAGE, "memstore mutator replay count", 60036, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_PURGE_COUNT, "memstore row purge count", ObStatClassIds::STORAGE, "memstore row purge count", 60037, true, true)
STAT_EVENT_ADD_DEF(MEMSTORE_ROW_COMPACTION_COUNT, "memstore row compaction count", ObStatClassIds::STORAGE, "memstore row compaction count", 60038, true, true)

// DEBUG
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_COUNT, "refresh schema count", ObStatClassIds::DEBUG, "refresh schema count", 70000, true, true)
STAT_EVENT_ADD_DEF(REFRESH_SCHEMA_TIME, "refresh schema time", ObStatClassIds::DEBUG, "refresh schema time", 70001, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_COUNT, "inner sql connection execute count", ObStatClassIds::DEBUG, "inner sql connection execute count", 70002, true, true)
STAT_EVENT_ADD_DEF(INNER_SQL_CONNECTION_EXECUTE_TIME, "inner sql connection execute time", ObStatClassIds::DEBUG, "inner sql connection execute time", 70003, true, true)
STAT_EVENT_ADD_DEF(PARTITION_TABLE_OPERATOR_GET_COUNT, "partition table operator get count", ObStatClassIds::DEBUG, "partition table operator get count", 70004, true, true)
STAT_EVENT_ADD_DEF(PARTITION_TABLE_OPERATOR_GET_TIME, "partition table operator get time", ObStatClassIds::DEBUG, "partition table operator get time", 70005, true, true)

//CLOG
STAT_EVENT_ADD_DEF(CLOG_SW_SUBMITTED_LOG_COUNT, "submitted to sliding window log count", ObStatClassIds::CLOG, "submitted to sliding window log count", 80001, true, true)
STAT_EVENT_ADD_DEF(CLOG_SW_SUBMITTED_LOG_SIZE, "submitted to sliding window log size", ObStatClassIds::CLOG, "submitted to sliding window log size", 80002, true, true)
STAT_EVENT_ADD_DEF(CLOG_INDEX_LOG_FLUSHED_LOG_COUNT, "index log flushed count", ObStatClassIds::CLOG, "index log flushed count", 80003, true, true)
STAT_EVENT_ADD_DEF(CLOG_INDEX_LOG_FLUSHED_LOG_SIZE, "index log flushed clog size", ObStatClassIds::CLOG, "index log flushed clog size", 80004, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSHED_COUNT, "clog flushed count", ObStatClassIds::CLOG, "clog flushed count", 80005, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSHED_SIZE, "clog flushed size", ObStatClassIds::CLOG, "clog flushed size", 80006, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_SIZE, "clog read size", ObStatClassIds::CLOG, "clog read size", 80007, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_COUNT, "clog read count", ObStatClassIds::CLOG, "clog read count", 80008, true, true)
STAT_EVENT_ADD_DEF(CLOG_DISK_READ_SIZE, "clog disk read size", ObStatClassIds::CLOG, "clog disk read size", 80009, true, true)
STAT_EVENT_ADD_DEF(CLOG_DISK_READ_COUNT, "clog disk read count", ObStatClassIds::CLOG, "clog disk read count", 80010, true, true)
STAT_EVENT_ADD_DEF(CLOG_DISK_READ_TIME, "clog disk read time", ObStatClassIds::CLOG, "clog disk read time", 80011, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_SIZE, "clog fetch log size", ObStatClassIds::CLOG, "clog fetch log size", 80012, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_COUNT, "clog fetch log count", ObStatClassIds::CLOG, "clog fetch log count", 80013, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_BY_LOCATION_SIZE, "clog fetch log by localtion size", ObStatClassIds::CLOG, "clog fetch log by localtion size", 80014, true, true)
STAT_EVENT_ADD_DEF(CLOG_FETCH_LOG_BY_LOCATION_COUNT, "clog fetch log by location count", ObStatClassIds::CLOG, "clog fetch log by localtion size", 80015, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_SUCC_SIZE, "clog read request succ size", ObStatClassIds::CLOG, "clog read request succ size", 80016, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_SUCC_COUNT, "clog read request succ count", ObStatClassIds::CLOG, "clog read request succ count", 80017, true, true)
STAT_EVENT_ADD_DEF(CLOG_READ_REQUEST_FAIL_COUNT, "clog read request fail count", ObStatClassIds::CLOG, "clog read request fail count", 80018, true, true)
STAT_EVENT_ADD_DEF(CLOG_LEADER_CONFIRM_TIME, "clog leader confirm time", ObStatClassIds::CLOG, "clog leader confirm time", 80019, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSH_TASK_GENERATE_COUNT, "clog flush task generate count", ObStatClassIds::CLOG, "clog flush task generate count", 80020, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSH_TASK_RELEASE_COUNT, "clog flush task release count", ObStatClassIds::CLOG, "clog flush task release count", 80021, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_DELAY_TIME, "clog rpc delay time", ObStatClassIds::CLOG, "clog rpc delay time", 80022, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_COUNT, "clog rpc count", ObStatClassIds::CLOG, "clog rpc count", 80023, true, true)
STAT_EVENT_ADD_DEF(CLOG_NON_KV_CACHE_HIT_COUNT, "clog non kv cache hit count", ObStatClassIds::CLOG, "clog non kv cache hit count", 80024, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_REQUEST_HANDLE_TIME, "clog rpc request handle time", ObStatClassIds::CLOG, "clog rpc request handle time", 80025, true, true)
STAT_EVENT_ADD_DEF(CLOG_RPC_REQUEST_COUNT, "clog rpc request count", ObStatClassIds::CLOG, "clog rpc request count", 80026, true, true)
STAT_EVENT_ADD_DEF(CLOG_CACHE_HIT_COUNT, "clog cache hit count", ObStatClassIds::CLOG, "clog cache hit count", 80027, true, true)
STAT_EVENT_ADD_DEF(CLOG_STATE_LOOP_COUNT, "clog state loop count", ObStatClassIds::CLOG, "clog state loop count", 80028, true, true)
STAT_EVENT_ADD_DEF(CLOG_STATE_LOOP_TIME, "clog state loop time", ObStatClassIds::CLOG, "clog state loop time", 80029, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_LOOP_COUNT, "clog replay loop count", ObStatClassIds::CLOG, "clog state loop count", 80030, true, true)
STAT_EVENT_ADD_DEF(CLOG_REPLAY_LOOP_TIME, "clog replay loop time", ObStatClassIds::CLOG, "clog state loop time", 80031, true, true)
STAT_EVENT_ADD_DEF(CLOG_TO_LEADER_ACTIVE_COUNT, "clog to leader active count", ObStatClassIds::CLOG, "clog to leader active count", 80032, true, true)
STAT_EVENT_ADD_DEF(CLOG_TO_LEADER_ACTIVE_TIME, "clog to leader active time", ObStatClassIds::CLOG, "clog to leader active time", 80033, true, true)
STAT_EVENT_ADD_DEF(ON_SUCCESS_COUNT, "on_success cb count", ObStatClassIds::CLOG, "on_success cb count", 80034, true, true)
STAT_EVENT_ADD_DEF(ON_SUCCESS_TIME, "on_success cb time", ObStatClassIds::CLOG, "on_success cb time", 80035, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_REVOKE_COUNT, "on_leader_revoke count", ObStatClassIds::CLOG, "on_leader_revoke count", 80036, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_REVOKE_TIME, "on_leader_revoke time", ObStatClassIds::CLOG, "on_leader_revoke time", 80037, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_TAKEOVER_COUNT, "on_leader_takeover count", ObStatClassIds::CLOG, "on_leader_takeover count", 80038, true, true)
STAT_EVENT_ADD_DEF(ON_LEADER_TAKEOVER_TIME, "on_leader_takeover time", ObStatClassIds::CLOG, "on_leader_takeover time", 80039, true, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_COUNT, "clog write count", ObStatClassIds::CLOG, "clog write count", 80040, true, true)
STAT_EVENT_ADD_DEF(CLOG_WRITE_TIME, "clog write time", ObStatClassIds::CLOG, "clog write time", 80041, true, true)
STAT_EVENT_ADD_DEF(ILOG_WRITE_COUNT, "ilog write count", ObStatClassIds::CLOG, "ilog write count", 80042, true, true)
STAT_EVENT_ADD_DEF(ILOG_WRITE_TIME, "ilog write time", ObStatClassIds::CLOG, "ilog write time", 80043, true, true)
STAT_EVENT_ADD_DEF(CLOG_FLUSHED_TIME, "clog flushed time", ObStatClassIds::CLOG, "clog flushed time", 80044, true, true)
STAT_EVENT_ADD_DEF(CLOG_TASK_CB_COUNT, "clog task cb count", ObStatClassIds::CLOG, "clog task cb count", 80045, true, true)
STAT_EVENT_ADD_DEF(CLOG_CB_QUEUE_TIME, "clog cb queue time", ObStatClassIds::CLOG, "clog cb queue time", 80046, true, true)
STAT_EVENT_ADD_DEF(CLOG_ACK_COUNT, "clog ack count", ObStatClassIds::CLOG, "clog ack count", 80049, true, true)
STAT_EVENT_ADD_DEF(CLOG_ACK_TIME, "clog ack time", ObStatClassIds::CLOG, "clog ack time", 80050, true, true)
STAT_EVENT_ADD_DEF(CLOG_FIRST_ACK_COUNT, "clog first ack count", ObStatClassIds::CLOG, "clog first ack count", 80051, true, true)
STAT_EVENT_ADD_DEF(CLOG_FIRST_ACK_TIME, "clog first ack time", ObStatClassIds::CLOG, "clog first ack time", 80052, true, true)
STAT_EVENT_ADD_DEF(CLOG_LEADER_CONFIRM_COUNT, "clog leader confirm count", ObStatClassIds::CLOG, "clog leader confirm count", 80053, true, true)
// ELECTION
STAT_EVENT_ADD_DEF(ELECTION_CHANGE_LEAER_COUNT, "election change leader count", ObStatClassIds::ELECT, "election change leader count", 90001, false, true)
STAT_EVENT_ADD_DEF(ELECTION_LEADER_REVOKE_COUNT, "election leader revoke count", ObStatClassIds::ELECT, "election leader revoke count", 90002, false, true)

STAT_EVENT_ADD_DEF(STAT_EVENT_ADD_END, "event add end", ObStatClassIds::DEBUG, "event add end", 1, false, false)
#endif

#ifdef STAT_EVENT_SET_DEF
// NETWORK

// QUEUE

// TRANS

// SQL

// CACHE
STAT_EVENT_SET_DEF(LOCATION_CACHE_SIZE, "location cache size", ObStatClassIds::CACHE, "location cache size", 120000, false, true)
STAT_EVENT_SET_DEF(CLOG_CACHE_SIZE, "clog cache size", ObStatClassIds::CACHE, "clog cache size", 120001, false, true)
STAT_EVENT_SET_DEF(INDEX_CLOG_CACHE_SIZE, "index clog cache size", ObStatClassIds::CACHE, "index clog cache size", 120002, false, true)
STAT_EVENT_SET_DEF(USER_TABLE_COL_STAT_CACHE_SIZE, "user table col stat cache size", ObStatClassIds::CACHE, "user table col stat cache size", 120003, false, true)
STAT_EVENT_SET_DEF(INDEX_CACHE_SIZE, "index cache size", ObStatClassIds::CACHE, "index cache size", 120004, false, true)
STAT_EVENT_SET_DEF(SYS_BLOCK_CACHE_SIZE, "sys block cache size", ObStatClassIds::CACHE, "sys block cache size", 120005, false, true)
STAT_EVENT_SET_DEF(USER_BLOCK_CACHE_SIZE, "user block cache size", ObStatClassIds::CACHE, "user block cache size", 120006, false, true)
STAT_EVENT_SET_DEF(SYS_ROW_CACHE_SIZE, "sys row cache size", ObStatClassIds::CACHE, "sys row cache size", 120007, false, true)
STAT_EVENT_SET_DEF(USER_ROW_CACHE_SIZE, "user row cache size", ObStatClassIds::CACHE, "user row cache size", 120008, false, true)
STAT_EVENT_SET_DEF(BLOOM_FILTER_CACHE_SIZE, "bloom filter cache size", ObStatClassIds::CACHE, "bloom filter cache size", 120009, false, true)

// STORAGE
STAT_EVENT_SET_DEF(ACTIVE_MEMSTORE_USED, "active memstore used", ObStatClassIds::STORAGE, "active memstore used", 130000, false, true)
STAT_EVENT_SET_DEF(TOTAL_MEMSTORE_USED, "total memstore used", ObStatClassIds::STORAGE, "total memstore used", 130001, false, true)
STAT_EVENT_SET_DEF(MAJOR_FREEZE_TRIGGER, "major freeze trigger", ObStatClassIds::STORAGE, "major freeze trigger", 130002, false, true)
STAT_EVENT_SET_DEF(MEMSTORE_LIMIT, "memstore limit", ObStatClassIds::STORAGE, "memstore limit", 130004, false, true)

// RESOURCE
//STAT_EVENT_SET_DEF(SRV_DISK_SIZE, "SRV_DISK_SIZE", RESOURCE, "SRV_DISK_SIZE")
//STAT_EVENT_SET_DEF(DISK_USAGE, "DISK_USAGE", RESOURCE, "DISK_USAGE")
//STAT_EVENT_SET_DEF(SRV_MEMORY_SIZE, "SRV_MEMORY_SIZE", RESOURCE, "SRV_MEMORY_SIZE")
STAT_EVENT_SET_DEF(MIN_MEMORY_SIZE, "min memory size", ObStatClassIds::RESOURCE, "min memory size", 140001, false, true)
STAT_EVENT_SET_DEF(MAX_MEMORY_SIZE, "max memory size", ObStatClassIds::RESOURCE, "max memory size", 140002, false, true)
STAT_EVENT_SET_DEF(MEMORY_USAGE, "memory usage", ObStatClassIds::RESOURCE, "memory usage", 140003, false, true)
//STAT_EVENT_SET_DEF(SRV_CPUS, "SRV_CPUS", RESOURCE, "SRV_CPUS")
STAT_EVENT_SET_DEF(MIN_CPUS, "min cpus", ObStatClassIds::RESOURCE, "min cpus", 140004, false, true)
STAT_EVENT_SET_DEF(MAX_CPUS, "max cpus", ObStatClassIds::RESOURCE, "max cpus", 140005, false, true)
STAT_EVENT_SET_DEF(CPU_USAGE, "cpu usage", ObStatClassIds::RESOURCE, "cpu usage", 140006, false, true)
STAT_EVENT_SET_DEF(DISK_USAGE, "disk usage", ObStatClassIds::RESOURCE, "disk usage", 140007, false, true)

//CLOG
STAT_EVENT_SET_DEF(CLOG_DISK_FREE_SIZE, "clog disk free size", ObStatClassIds::CLOG, "clog disk free size", 150001, false, true)
STAT_EVENT_SET_DEF(CLOG_DISK_FREE_RATIO, "clog disk free ratio", ObStatClassIds::CLOG, "clog disk free ratio", 150002, false, true)
STAT_EVENT_SET_DEF(CLOG_LAST_CHECK_LOG_FILE_COLLECT_TIME, "clog last check log file collect time", ObStatClassIds::CLOG, "clog last check log file collect time", 150003, false, true)

// DEBUG
STAT_EVENT_SET_DEF(OBLOGGER_WRITE_SIZE, "ObLogger application log bytes", ObStatClassIds::DEBUG, "ObLogger application log bytes", 160001, false, true)
STAT_EVENT_SET_DEF(ELECTION_WRITE_SIZE, "Election application log bytes", ObStatClassIds::DEBUG, "Election application log bytes", 160002, false, true)
STAT_EVENT_SET_DEF(STAT_EVENT_SET_END, "event set end", ObStatClassIds::DEBUG, "event set end", 200000, false, false)
#endif

#ifndef OB_STAT_EVENT_DEFINE_H_
#define OB_STAT_EVENT_DEFINE_H_

#include "lib/time/ob_time_utility.h"
#include "lib/statistic_event/ob_stat_class.h"


namespace oceanbase
{
namespace common
{
static const int64_t MAX_STAT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_STAT_EVENT_PARAM_LENGTH = 64;

struct ObStatEventIds
{
  enum ObStatEventIdEnum
  {
#define STAT_EVENT_ADD_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) def,
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
  };
};

struct ObStatEventAddStat
{
  //stat no
  int64_t stat_no_;
  //value for a stat
  int64_t stat_value_;
  ObStatEventAddStat();
  int add(const ObStatEventAddStat &other);
  int add(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const { return true; }
};

inline int64_t ObStatEventAddStat::get_stat_value()
{
  return stat_value_;
}

struct ObStatEventSetStat
{
  //stat no
  int64_t stat_no_;
  //value for a stat
  int64_t stat_value_;
  int64_t set_time_;
  ObStatEventSetStat();
  int add(const ObStatEventSetStat &other);
  void set(int64_t value);
  int64_t get_stat_value();
  void reset();
  inline bool is_valid() const { return stat_value_ > 0; }
};

inline int64_t ObStatEventSetStat::get_stat_value()
{
  return stat_value_;
}

inline void ObStatEventSetStat::set(int64_t value)
{
  stat_value_ = value;
  set_time_ = ObTimeUtility::current_time();
}

struct ObStatEvent
{
  char name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_class_;
  char display_name_[MAX_STAT_EVENT_NAME_LENGTH];
  int64_t stat_id_;
  bool summary_in_session_;
  bool can_visible_;
};


static const ObStatEvent OB_STAT_EVENTS[] = {
#define STAT_EVENT_ADD_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) \
  {name, stat_class, display_name, stat_id, summary_in_session, can_visible},
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_ADD_DEF
#define STAT_EVENT_SET_DEF(def, name, stat_class, display_name, stat_id, summary_in_session, can_visible) \
  {name, stat_class, display_name, stat_id, summary_in_session, can_visible},
#include "lib/statistic_event/ob_stat_event.h"
#undef STAT_EVENT_SET_DEF
};

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */

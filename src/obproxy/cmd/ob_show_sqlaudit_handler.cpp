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

#include "cmd/ob_show_sqlaudit_handler.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/time/ob_hrtime.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObSqlauditProcessor g_sqlaudit_processor;

// malloc
const static uint64_t MMAP_BLOCK_ALIGN = 1ULL << 21;

static inline bool is_aligned(const uint64_t x, const uint64_t align)
{
  return 0 == (x & (align - 1));
}

static inline uint64_t up2align(const uint64_t x, const uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

static inline void *mmap_aligned(const uint64_t size, const uint64_t align)
{
  void *ret = NULL;
  if (MAP_FAILED == (ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
    ret = NULL;
  } else if (is_aligned(reinterpret_cast<uint64_t>(ret), align)) {
  } else {
    munmap(ret, size);
    if (MAP_FAILED == (ret = mmap(NULL, size + align, PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      ret = NULL;
    } else {
      uint64_t aligned_addr = up2align(reinterpret_cast<uint64_t>(ret), align);
      // Compute the header/trailer size to avoid using ret after munmap.
      uint64_t header_size = aligned_addr - reinterpret_cast<uint64_t>(ret);
      uint64_t trailer_size = reinterpret_cast<uint64_t>(ret) + align - aligned_addr;

      munmap(ret, header_size);
      munmap(reinterpret_cast<void *>(aligned_addr + size), trailer_size);
      ret = reinterpret_cast<void *>(aligned_addr);
    }
  }
  return ret;
}

static inline void *direct_malloc(const int64_t size)
{
  return mmap_aligned(size, MMAP_BLOCK_ALIGN);
}

static inline void direct_free(void *p, const int64_t size)
{
  if (OB_LIKELY(NULL != p)) {
    munmap(p, size);
  }
}

// ObSqlauditRecordQueue::Iterator
ObSqlauditRecordQueue::Iterator::Iterator(ObSqlauditRecordQueue &sql_audit_record_queue,
                                          const int64_t audit_id_offset,
                                          const int64_t audit_id_limit)
    : sql_audit_record_queue_(sql_audit_record_queue),
      is_overlap_(false),
      sm_id_(LONG_MIN),
      min_audit_id_(audit_id_offset),
      max_audit_id_(audit_id_offset + audit_id_limit)
{
  reserved_len_ = static_cast<int64_t>(
      RETAIN_RATIO * static_cast<double>(sql_audit_record_queue.current_records_list_len_));
  int64_t cur_max_audit_id = sql_audit_record_queue.audit_id_;
  int64_t cur_min_audit_id = std::max((cur_max_audit_id + reserved_len_
      - sql_audit_record_queue.current_records_list_len_), 0L);

  if (audit_id_offset >= 0) {
    min_audit_id_ = std::max(min_audit_id_, cur_min_audit_id);
    max_audit_id_ = std::min(max_audit_id_, cur_max_audit_id);
  } else {
    min_audit_id_ = std::max((cur_max_audit_id - audit_id_limit), cur_min_audit_id);
    max_audit_id_ = cur_max_audit_id;
  }

  cur_index_ = min_audit_id_ % sql_audit_record_queue.current_records_list_len_;
  ndone_ = 0;
  INFO_ICMD("show sqlaudit limit m,n",
            K(audit_id_offset), K(audit_id_limit),
            K(cur_min_audit_id), K(cur_max_audit_id),
            K(min_audit_id_), K(max_audit_id_),
            K(cur_index_), K(ndone_),
            K(sql_audit_record_queue.current_records_list_len_));
}

ObSqlauditRecordQueue::Iterator::Iterator(ObSqlauditRecordQueue &sql_audit_record_queue,
                                        const int64_t sm_id)
    : sql_audit_record_queue_(sql_audit_record_queue),
      is_overlap_(false),
      sm_id_(sm_id),
      min_audit_id_(-1),
      max_audit_id_(-1)
{

  reserved_len_ = static_cast<int64_t>(
      RETAIN_RATIO * static_cast<double>(sql_audit_record_queue.current_records_list_len_));
  max_audit_id_ = sql_audit_record_queue.audit_id_;
  min_audit_id_ = std::max((max_audit_id_ + reserved_len_
      - sql_audit_record_queue.current_records_list_len_), 0L);

  cur_index_ = min_audit_id_ % sql_audit_record_queue.current_records_list_len_;
  ndone_ = 0;
  INFO_ICMD("show sqlaudit n", K_(min_audit_id), K_(max_audit_id), K(cur_index_), K(sm_id));
}

ObSqlauditRecord *ObSqlauditRecordQueue::Iterator::next()
{
  ObSqlauditRecord *record = NULL;
  int64_t audit_id_ = -1;
  int64_t ntodo = max_audit_id_ - min_audit_id_;
  if (sql_audit_record_queue_.is_inited_ && ntodo > 0) {
    int64_t critical_index = 0;
    int64_t critical_audit_id = 0;
    if (LONG_MIN == sm_id_) {
      if (ndone_ >= ntodo) {
        // do nothing
      } else {
        audit_id_ = sql_audit_record_queue_.sqlaudit_records_[cur_index_].audit_id_;
        critical_index = std::max((audit_id_ - (reserved_len_ / 2)), 0L)
                         % sql_audit_record_queue_.current_records_list_len_;
        critical_audit_id = sql_audit_record_queue_.sqlaudit_records_[critical_index].audit_id_;

        if (critical_audit_id <= max_audit_id_) {
          record = &(sql_audit_record_queue_.sqlaudit_records_[cur_index_]);
          ++ndone_;
        } else {
          is_overlap_ = true;
        }
        ++cur_index_;
        cur_index_ = cur_index_ % sql_audit_record_queue_.current_records_list_len_;
      }
    } else {
      if (OB_UNLIKELY(sm_id_ < 0)) {
        // do nothing
      } else {
        bool find = false;
        while (!find && (ndone_ < ntodo + 1)) {
          audit_id_ = sql_audit_record_queue_.sqlaudit_records_[cur_index_].audit_id_;
          critical_index = std::max((audit_id_ - (reserved_len_ / 2)), 0L)
                           % sql_audit_record_queue_.current_records_list_len_;
          critical_audit_id = sql_audit_record_queue_.sqlaudit_records_[critical_index].audit_id_;
          if (critical_audit_id <= max_audit_id_) {
            if (sql_audit_record_queue_.sqlaudit_records_[cur_index_].sm_id_ == sm_id_) {
              record = &(sql_audit_record_queue_.sqlaudit_records_[cur_index_]);
              find = true;
            }
          } else {
            is_overlap_ = true;
            find = true;
          }
          ++ndone_;
          ++cur_index_;
          cur_index_ = cur_index_ % sql_audit_record_queue_.current_records_list_len_;
        }
      }
    }
  }
  return record;
}

// ObSqlauditRecordQueue
ObSqlauditRecordQueue::ObSqlauditRecordQueue()
    : is_inited_(false),
      audit_id_(0),
      sqlaudit_records_(NULL),
      current_memory_size_(0),
      current_records_list_len_(0)
{
}

int ObSqlauditRecordQueue::init(const int64_t available_memory_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    DEBUG_ICMD("init twice", K(available_memory_size), K(ret));
  } else if (OB_UNLIKELY(available_memory_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument", K(available_memory_size), K(ret));
  } else if ((current_memory_size_ = ob_roundup(available_memory_size, MMAP_BLOCK_ALIGN)) <=  0) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("invalid current_memory_size", K(current_memory_size_), K(available_memory_size), K(MMAP_BLOCK_ALIGN));
  } else if (OB_ISNULL(sqlaudit_records_ = reinterpret_cast<ObSqlauditRecord *>(direct_malloc(current_memory_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_ICMD("fail to direct_malloc", K(current_memory_size_), K(ret));
  } else {
    current_records_list_len_ = current_memory_size_ / sizeof(ObSqlauditRecord);
    audit_id_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObSqlauditRecordQueue::destroy()
{
  if (is_inited_) {
    if (OB_LIKELY(NULL != sqlaudit_records_)) {
      direct_free(sqlaudit_records_, current_memory_size_);
      sqlaudit_records_ = NULL;
    }

    audit_id_ = 0;
    current_memory_size_ = 0;
    current_records_list_len_ = 0;
    is_inited_ = false;
  }
}

void ObSqlauditRecordQueue::enqueue(const int64_t sm_id, const int64_t gmt_create,
    const ObCmdTimeStat cmd_time_stat, const ObIpEndpoint &ip, const ObString &sql,
    const obmysql::ObMySQLCmd sql_cmd)
{
  if (is_inited_) {
    int64_t audit_id = ATOMIC_FAA((int64_t *)&audit_id_, 1);
    int64_t index = audit_id % current_records_list_len_;
    sqlaudit_records_[index].audit_id_ = audit_id;
    sqlaudit_records_[index].sm_id_ = sm_id;
    sqlaudit_records_[index].gmt_create_ = gmt_create;
    sqlaudit_records_[index].cmd_time_stat_ = cmd_time_stat;
    ip.to_string(sqlaudit_records_[index].ip_, ObSqlauditRecord::IP_LENGTH);
    sql.to_string(sqlaudit_records_[index].sql_, ObSqlauditRecord::SQL_LENGTH);

    ObString cmd = ObString::make_string(get_mysql_cmd_str(sql_cmd));
    cmd.to_string(sqlaudit_records_[index].sql_cmd_, ObSqlauditRecord::SQL_CMD_LENGTH);
  }
}

// ObSqlauditProcessor
ObSqlauditRecordQueue *ObSqlauditProcessor::acquire()
{
  ObSqlauditRecordQueue *queue = NULL;
  if (NULL != sqlaudit_record_queue_) {
    obsys::CRLockGuard lock(queue_lock_);
    if (AVAILABLE == status_) {
      queue = sqlaudit_record_queue_;
      queue->refcount_inc();
    }
  }
  return queue;
}

int ObSqlauditProcessor::release(ObSqlauditRecordQueue *queue)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(queue)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("queue is null", K(queue), K(ret));
  } else {
    (void)(queue->refcount_dec());
  }
  return ret;
}

int ObSqlauditProcessor::init_sqlaudit_record_queue(int64_t sqlaudit_mem_limited)
{
  int ret = OB_SUCCESS;
  ObSqlauditRecordQueue *queue = NULL;
  if (sqlaudit_mem_limited > 0) {
    if (OB_ISNULL(queue = new (std::nothrow) ObSqlauditRecordQueue())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      WARN_ICMD("fail to new ObSqlauditRecordQueue", K(ret));
    } else if (OB_FAIL(queue->init(sqlaudit_mem_limited))) {
      delete queue;
      WARN_ICMD("fail to init ObSqlauditRecordQueue", K(ret));
    } else {
      obsys::CWLockGuard lock(queue_lock_);
      sqlaudit_record_queue_ = queue;
      status_ = AVAILABLE;
    }
  }
  return ret;
}

ObSqlauditProcessorStatus ObSqlauditProcessor::get_status() const
{
  obsys::CRLockGuard lock(queue_lock_);
  return status_;
}

bool ObSqlauditProcessor::set_status(ObSqlauditProcessorStatus status)
{
  bool ret = false;
  obsys::CWLockGuard lock(queue_lock_, false);
  if (lock.acquired()) {
    status_ = status;
    if (STOPPING == status) {
      if (OB_UNLIKELY(NULL != sqlaudit_record_queue_last_)) {
        ERROR_ICMD("sqlaudit_record_queue_last_ is not NULL, maybe memory leak", KP(sqlaudit_record_queue_last_));
      }
      sqlaudit_record_queue_last_ = sqlaudit_record_queue_;
      sqlaudit_record_queue_last_memory_size_ = sqlaudit_record_queue_->get_current_memory_size();
      sqlaudit_record_queue_ = NULL;
    }
    ret = true;
  }
  return ret;
}

void ObSqlauditProcessor::destroy_queue()
{
  ObSqlauditRecordQueue *queue = NULL;
  if (NULL != sqlaudit_record_queue_last_) {
    obsys::CWLockGuard lock(queue_lock_, false);
    if (lock.acquired() && (0 == sqlaudit_record_queue_last_->refcount()) && STOPPING == status_) {
      queue = sqlaudit_record_queue_last_;
      sqlaudit_record_queue_last_ = NULL;
      sqlaudit_record_queue_last_memory_size_ = 0;
      status_ = UNAVAILABLE;
    }
  }
  if (NULL != queue) {
    delete queue;
    queue = NULL;
  }
}

// ObShowSqlauditHandler
enum
{
  OB_SSA_AUDIT_ID = 0,
  OB_SSA_SM_ID,
  OB_SSA_GMT_CREATE,
  OB_SSA_SVR_IP_PORT,
  OB_SSA_SQL_CONTENT,
  OB_SSA_SQL_CMD_CONTENT,

  OB_SSA_CLIENT_TRANSACTION_IDLE_TIME,
  OB_SSA_CLIENT_REQUEST_READ_TIME,
  OB_SSA_CLIENT_REQUEST_ANALYZE_TIME,
  OB_SSA_CLUSTER_RESOURCE_CREATE_TIME,
  OB_SSA_PL_LOOKUP_TIME,
  OB_SSA_PL_PROCESS_TIME,
  OB_SSA_CONGESTION_CONTROL_TIME,
  OB_SSA_CONGESTION_PROCESS_TIME,
  OB_SSA_DO_OBSERVER_OPEN_TIME,
  OB_SSA_SERVER_CONNECT_TIME,
  OB_SSA_SERVER_SYNC_SESSION_VARIABLE_TIME,
  OB_SSA_SERVER_SEND_SAVED_LOGIN_TIME,
  OB_SSA_SERVER_SEND_USE_DATABASE_TIME,
  OB_SSA_SERVER_SEND_SESSION_VARIABLE_TIME,
  OB_SSA_SERVER_SEND_ALL_SESSION_VARIABLE_TIME,
  OB_SSA_SERVER_SEND_LAST_INSERT_ID_TIME,
  OB_SSA_SERVER_SEND_START_TRANS_TIME,
  OB_SSA_BUILD_SERVER_REQUEST_TIME,
  OB_SSA_PLUGIN_COMPRESS_REQUEST_TIME,
  OB_SSA_PREPARE_SEND_REQUEST_TO_SERVER_TIME,
  OB_SSA_SERVER_REQUEST_WRITE_TIME,
  OB_SSA_SERVER_PROCESS_REQUEST_TIME,
  OB_SSA_SERVER_RESPONSE_READ_TIME,
  OB_SSA_PLUGIN_DECOMPRESS_RESPONSE_TIME,
  OB_SSA_SERVER_RESPONSE_ANALYZE_TIME,
  OB_SSA_OK_PACKET_TRIM_TIME,
  OB_SSA_CLIENT_RESPONSE_WRITE_TIME,
  OB_SSA_REQUEST_TOTAL_TIME,

  OB_SSA_MAX_COLUMN_ID
};

const ObProxyColumnSchema SSA_COLUMN_ARRAY[OB_SSA_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_SSA_AUDIT_ID,        "audit_id",    obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SM_ID,           "sm_id",       obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SSA_GMT_CREATE,      "gmt_create",  obmysql::OB_MYSQL_TYPE_TIMESTAMP),
    ObProxyColumnSchema::make_schema(OB_SSA_SVR_IP_PORT,     "svr_ip",      obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SSA_SQL_CONTENT,     "sql",         obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SSA_SQL_CMD_CONTENT, "sql_cmd",     obmysql::OB_MYSQL_TYPE_VARCHAR),

    ObProxyColumnSchema::make_schema(OB_SSA_CLIENT_TRANSACTION_IDLE_TIME,        "client_transaction_idle_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CLIENT_REQUEST_READ_TIME,            "client_request_read_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CLIENT_REQUEST_ANALYZE_TIME,         "client_request_analyze_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CLUSTER_RESOURCE_CREATE_TIME,        "cluster_resource_create_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_PL_LOOKUP_TIME,                      "pl_lookup_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_PL_PROCESS_TIME,                     "pl_process_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CONGESTION_CONTROL_TIME,             "congestion_control_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CONGESTION_PROCESS_TIME,             "congestion_process_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_DO_OBSERVER_OPEN_TIME,               "do_observer_open_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_CONNECT_TIME,                 "server_connect_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SYNC_SESSION_VARIABLE_TIME,   "server_sync_session_variable_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_SAVED_LOGIN_TIME,        "server_send_saved_login_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_USE_DATABASE_TIME,       "server_send_use_database_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_SESSION_VARIABLE_TIME,   "server_send_session_variable_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_ALL_SESSION_VARIABLE_TIME, "server_send_all_session_variable_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_LAST_INSERT_ID_TIME,     "server_send_last_insert_id_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_SEND_START_TRANS_TIME,        "server_send_start_trans_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_BUILD_SERVER_REQUEST_TIME,           "build_server_request_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_PLUGIN_COMPRESS_REQUEST_TIME,        "plugin_compress_request_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_PREPARE_SEND_REQUEST_TO_SERVER_TIME, "prepare_send_request_to_server_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_REQUEST_WRITE_TIME,           "server_request_write_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_PROCESS_REQUEST_TIME,         "server_process_request_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_RESPONSE_READ_TIME,           "server_response_read_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_PLUGIN_DECOMPRESS_RESPONSE_TIME,     "plugin_decompress_response_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_SERVER_RESPONSE_ANALYZE_TIME,        "server_response_analyze_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_OK_PACKET_TRIM_TIME,                 "ok_packet_trim_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_CLIENT_RESPONSE_WRITE_TIME,          "client_response_write_ns", obmysql::OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SSA_REQUEST_TOTAL_TIME,                  "request_total_ns", obmysql::OB_MYSQL_TYPE_LONG)
};

ObShowSqlauditHandler::ObShowSqlauditHandler(ObContinuation *cont, ObMIOBuffer *buf,
                                             const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info),
      audit_id_offset_(info.get_limit_offset()), audit_id_limit_(info.get_limit_rows()),
      sm_id_(info.get_sm_id()), sub_cmd_type_(info.get_sub_cmd_type())
{
}

int ObShowSqlauditHandler::handle_show_sqlaudit(int event, ObEvent *e)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObSqlauditRecordQueue *record_queue = NULL;

  if (OB_ISNULL(record_queue = get_global_sqlaudit_processor().acquire())) {
    ret = OB_NOT_INIT;
  } else {
    ObSqlauditRecord *record = NULL;
    ObSqlauditRecordQueue::Iterator *it = NULL;
    bool sqlaudit_argument_valid = true;

    if (OB_UNLIKELY(!is_argument_valid(event, e))) {
      ret = OB_INVALID_ARGUMENT;
      WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K_(is_inited), K(ret));
    } else if (OB_FAIL(dump_header())) {
      WARN_ICMD("fail to dump_threads_header", K(ret));
    } else {
      switch (sub_cmd_type_) {
        case OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID:
          if (OB_UNLIKELY(audit_id_limit_ < 0)) {
            sqlaudit_argument_valid = false;
          } else if (OB_ISNULL(it = new (std::nothrow) ObSqlauditRecordQueue::Iterator(
              *record_queue, audit_id_offset_, audit_id_limit_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            WARN_ICMD("fail to new ObSqlauditRecordQueue:Iterator", K(ret));
          }
          break;

        case OBPROXY_T_SUB_SQLAUDIT_SM_ID:
          if (OB_UNLIKELY(sm_id_ < 0)) {
            sqlaudit_argument_valid = false;
          } else if (OB_ISNULL(it = new (std::nothrow) ObSqlauditRecordQueue::Iterator(*record_queue, sm_id_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            WARN_ICMD("fail to new ObSqlauditRecordQueue:Iterator", K(ret));
          }
          break;

        default:
          ret = OB_INVALID_ARGUMENT;
          WARN_ICMD("invalid sub_stat_type", K(sub_cmd_type_), K(ret));
          break;
      }

      if (sqlaudit_argument_valid && OB_SUCC(ret)) {
        while ((NULL != (record = it->next())) && OB_SUCC(ret)) {
          if (match_like(record->sql_, like_name_) && OB_FAIL(dump_sqlaudit_record(*record))) {
            WARN_ICMD("fail to dump_sqlaudit_record");
          }
        }
      }
    }

    (void) get_global_sqlaudit_processor().release(record_queue);

    if (OB_SUCC(ret)) {
      if (!sqlaudit_argument_valid) {
        if (OB_FAIL(encode_err_packet(OB_ERR_OPERATOR_UNKNOWN))) {
          WARN_ICMD("fail to encode err packet", K(ret));
        }
      } else if (it->is_overlap()) {
        if (OB_FAIL(encode_err_packet(OB_BUF_NOT_ENOUGH))) {
          WARN_ICMD("fail to encode err packet", K(ret));
        }
      } else {
        if (OB_FAIL(encode_eof_packet())) {
          WARN_ICMD("fail to encode eof packet", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      DEBUG_ICMD("succ to dump sqlaudit");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }

    if (NULL != it) {
      delete it;
      it = NULL;
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowSqlauditHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(SSA_COLUMN_ARRAY, OB_SSA_MAX_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowSqlauditHandler::dump_sqlaudit_record(ObSqlauditRecord &record)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_SSA_MAX_COLUMN_ID];

  cells[OB_SSA_AUDIT_ID].set_int(record.audit_id_);
  cells[OB_SSA_SM_ID].set_int(record.sm_id_);
  cells[OB_SSA_GMT_CREATE].set_timestamp(hrtime_to_usec(HRTIME_HOURS(8) + record.gmt_create_));
  cells[OB_SSA_SVR_IP_PORT].set_varchar(record.ip_);
  cells[OB_SSA_SQL_CONTENT].set_varchar(record.sql_);
  cells[OB_SSA_SQL_CMD_CONTENT].set_varchar(record.sql_cmd_);

  cells[OB_SSA_PL_PROCESS_TIME].set_int(record.cmd_time_stat_.pl_process_time_);
  cells[OB_SSA_CONGESTION_PROCESS_TIME].set_int(record.cmd_time_stat_.congestion_process_time_);
  cells[OB_SSA_DO_OBSERVER_OPEN_TIME].set_int(record.cmd_time_stat_.do_observer_open_time_);
  cells[OB_SSA_SERVER_CONNECT_TIME].set_int(record.cmd_time_stat_.server_connect_time_);
  cells[OB_SSA_SERVER_SEND_SAVED_LOGIN_TIME].set_int(record.cmd_time_stat_.server_send_saved_login_time_);
  cells[OB_SSA_SERVER_SEND_USE_DATABASE_TIME].set_int(record.cmd_time_stat_.server_send_use_database_time_);
  cells[OB_SSA_SERVER_SEND_SESSION_VARIABLE_TIME].set_int(record.cmd_time_stat_.server_send_session_variable_time_);
  cells[OB_SSA_SERVER_SEND_ALL_SESSION_VARIABLE_TIME].set_int(record.cmd_time_stat_.server_send_all_session_variable_time_);
  cells[OB_SSA_SERVER_SEND_LAST_INSERT_ID_TIME].set_int(record.cmd_time_stat_.server_send_last_insert_id_time_);
  cells[OB_SSA_SERVER_SEND_START_TRANS_TIME].set_int(record.cmd_time_stat_.server_send_start_trans_time_);
  cells[OB_SSA_BUILD_SERVER_REQUEST_TIME].set_int(record.cmd_time_stat_.build_server_request_time_);
  cells[OB_SSA_PLUGIN_COMPRESS_REQUEST_TIME].set_int(record.cmd_time_stat_.plugin_compress_request_time_);
  cells[OB_SSA_PLUGIN_DECOMPRESS_RESPONSE_TIME].set_int(record.cmd_time_stat_.plugin_decompress_response_time_);

  cells[OB_SSA_CLIENT_TRANSACTION_IDLE_TIME].set_int(record.cmd_time_stat_.client_transaction_idle_time_);
  cells[OB_SSA_CLIENT_REQUEST_READ_TIME].set_int(record.cmd_time_stat_.client_request_read_time_);
  cells[OB_SSA_SERVER_REQUEST_WRITE_TIME].set_int(record.cmd_time_stat_.server_request_write_time_);
  cells[OB_SSA_CLIENT_REQUEST_ANALYZE_TIME].set_int(record.cmd_time_stat_.client_request_analyze_time_);
  cells[OB_SSA_CLUSTER_RESOURCE_CREATE_TIME].set_int(record.cmd_time_stat_.cluster_resource_create_time_);
  cells[OB_SSA_PL_LOOKUP_TIME].set_int(record.cmd_time_stat_.pl_lookup_time_);
  cells[OB_SSA_CONGESTION_CONTROL_TIME].set_int(record.cmd_time_stat_.congestion_control_time_);
  cells[OB_SSA_SERVER_SYNC_SESSION_VARIABLE_TIME].set_int(record.cmd_time_stat_.server_sync_session_variable_time_);
  cells[OB_SSA_PREPARE_SEND_REQUEST_TO_SERVER_TIME].set_int(record.cmd_time_stat_.prepare_send_request_to_server_time_);
  cells[OB_SSA_SERVER_PROCESS_REQUEST_TIME].set_int(record.cmd_time_stat_.server_process_request_time_);
  cells[OB_SSA_SERVER_RESPONSE_READ_TIME].set_int(record.cmd_time_stat_.server_response_read_time_);
  cells[OB_SSA_CLIENT_RESPONSE_WRITE_TIME].set_int(record.cmd_time_stat_.client_response_write_time_);
  cells[OB_SSA_SERVER_RESPONSE_ANALYZE_TIME].set_int(record.cmd_time_stat_.server_response_analyze_time_);
  cells[OB_SSA_OK_PACKET_TRIM_TIME].set_int(record.cmd_time_stat_.ok_packet_trim_time_);
  cells[OB_SSA_REQUEST_TOTAL_TIME].set_int(record.cmd_time_stat_.request_total_time_);

  row.cells_ = cells;
  row.count_ = OB_SSA_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_ICMD("fail to encode row packet", K(row), K(ret));
  } else {
    DEBUG_ICMD("succ to encode row packet", K(row), K(record.audit_id_));
  }
  return ret;
}

static int show_sqlaudit_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                  ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowSqlauditHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new (std::nothrow) ObShowSqlauditHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_ICMD("fail to new ObShowSqlauditHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowSqlauditHandler");
  } else {
    SET_CONTINUATION_HANDLER(handler, &ObShowSqlauditHandler::handle_show_sqlaudit);
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      WARN_ICMD("fail to schedule ObShowSqlauditHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowSqlauditHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_sqlaudit_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_SQLAUDIT,
                                                               &show_sqlaudit_callback))) {
    WARN_ICMD("fail to register CMD_TYPE_SQLAUDIT", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

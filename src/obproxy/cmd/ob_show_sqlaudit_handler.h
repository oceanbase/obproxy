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

#ifndef OBPROXY_SHOW_SQLAUDIT_HANDLER_H
#define OBPROXY_SHOW_SQLAUDIT_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_sm_time_stat.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObSqlauditRecord
{
  static const int32_t RECORD_SIZE = 512;
  static const int32_t MAX_CHAR_LENGTH = RECORD_SIZE
                                       - sizeof(ObCmdTimeStat)
                                       - (3 * sizeof(int64_t));
  static const int32_t IP_LENGTH = 24;
  static const int32_t SQL_CMD_LENGTH = 32;

  static const int32_t SQL_LENGTH = MAX_CHAR_LENGTH - IP_LENGTH - SQL_CMD_LENGTH;

  int64_t audit_id_;
  int64_t sm_id_;
  ObHRTime gmt_create_;
  ObCmdTimeStat cmd_time_stat_;
  char ip_[IP_LENGTH];
  char sql_[SQL_LENGTH];
  char sql_cmd_[SQL_CMD_LENGTH];
};

class ObSqlauditRecordQueue : public common::ObRefCountObj
{
public:
  class Iterator
  {
  public:
    Iterator(ObSqlauditRecordQueue &sql_audit_record_queue,
             const int64_t audit_id_offset,
             const int64_t audit_id_limit);
    Iterator(ObSqlauditRecordQueue &sql_audit_record_queue,
             const int64_t sm_id);
    ~Iterator() { }

    ObSqlauditRecord *next();
    bool is_overlap() const { return is_overlap_; }

  private:
    static const double RETAIN_RATIO = 0.1;

    ObSqlauditRecordQueue &sql_audit_record_queue_;
    bool is_overlap_;

    const int64_t sm_id_;

    int64_t min_audit_id_;
    int64_t max_audit_id_;

    int64_t reserved_len_;
    int64_t cur_index_;
    int64_t ndone_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Iterator);
  };

public:
  ObSqlauditRecordQueue();
  virtual ~ObSqlauditRecordQueue() { destroy(); }

  bool is_init() const { return is_inited_; }
  int64_t get_current_memory_size() const { return current_memory_size_; }

  int init(const int64_t available_memory_size);
  void enqueue(const int64_t sm_id, const int64_t gmt_create,
               const ObCmdTimeStat cmd_time_stat, const net::ObIpEndpoint &ip,
               const ObString &sql, const obmysql::ObMySQLCmd sql_cmd);

private:
  void destroy();

private:
  bool is_inited_;
  int64_t audit_id_;
  ObSqlauditRecord *sqlaudit_records_;

  int64_t current_memory_size_;
  int64_t current_records_list_len_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlauditRecordQueue);
};

enum ObSqlauditProcessorStatus
{
  UNAVAILABLE = 0,
  AVAILABLE,
  INITIALIZING,
  STOPPING
};

class ObSqlauditProcessor
{
public:
  ObSqlauditProcessor()
    : status_(UNAVAILABLE),
      sqlaudit_record_queue_(NULL),
      sqlaudit_record_queue_last_(NULL),
      sqlaudit_record_queue_last_memory_size_(0)
  { }
  ~ObSqlauditProcessor() { }

  ObSqlauditRecordQueue *acquire();
  int release(ObSqlauditRecordQueue *queue);
  ObSqlauditRecordQueue *get_record_queue() { return sqlaudit_record_queue_; }
  int64_t get_last_record_queue_memory_size() const { return sqlaudit_record_queue_last_memory_size_; }

  ObSqlauditProcessorStatus get_status() const;
  bool set_status(ObSqlauditProcessorStatus status);

  int init_sqlaudit_record_queue(int64_t sqlaudit_mem_limited);

  void destroy_queue();

private:
  mutable obsys::CRWLock queue_lock_;
  ObSqlauditProcessorStatus status_;
  ObSqlauditRecordQueue *sqlaudit_record_queue_;

  ObSqlauditRecordQueue *sqlaudit_record_queue_last_;
  int64_t sqlaudit_record_queue_last_memory_size_;

  DISALLOW_COPY_AND_ASSIGN(ObSqlauditProcessor);
};

class ObShowSqlauditHandler : public ObInternalCmdHandler
{
public:
  ObShowSqlauditHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                        const ObInternalCmdInfo &info);
  virtual ~ObShowSqlauditHandler() { }

  int handle_show_sqlaudit(int event, event::ObEvent *e);

private:
  int dump_header();
  int dump_sqlaudit_record(ObSqlauditRecord &record);

private:
  const int64_t audit_id_offset_;
  const int64_t audit_id_limit_;
  const int64_t sm_id_;
  const ObProxyBasicStmtSubType sub_cmd_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObShowSqlauditHandler);
};

extern ObSqlauditProcessor g_sqlaudit_processor;
inline ObSqlauditProcessor &get_global_sqlaudit_processor()
{
  return g_sqlaudit_processor;
}

int show_sqlaudit_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHOW_SQLAUDIT_HANDLER_H

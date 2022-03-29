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

#ifndef OBPROXY_SHARD_DDL_CONT_H
#define OBPROXY_SHARD_DDL_CONT_H

#include "lib/json/ob_json.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define LIMIT_RULE_MAP_BUCKET 8

typedef int64_t (* write_func)(void *ptr, int64_t size, int64_t nmemb, void *stream);

enum ObShardDDLOperation {
  SHARD_DDL_OPERATION_CREATE_TABLE,
  SHARD_DDL_OPERATION_CREATE_INDEX,
  SHARD_DDL_OPERATION_ALTER,
  SHARD_DDL_OPERATION_DROP,
  SHARD_DDL_OPERATION_RENAME,
  SHARD_DDL_OPERATION_MAX
};

class ObShardDDLStatus
{
public:
  ObShardDDLStatus(common::ObArenaAllocator &allocator) : allocator_(&allocator),
    ddl_task_id_list_(ObModIds::OB_PROXY_SHARDING_DDL, OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  virtual ~ObShardDDLStatus() {}

  int parse_id_list_from_json(const json::Value *json);
  int parse_data_from_json(const json::Value *json, const bool is_first);
  int parse_from_json(ObString &buf, const bool is_first);
  int to_json(ObSqlString &buf);

  int set_ddl_status(const ObString &ddl_status) {
    if (!ddl_status_.empty()) {
      allocator_->free(ddl_status_.ptr());
    }
    return ob_write_string(*allocator_, ddl_status, ddl_status_);
  }
  int set_error_code(const ObString &error_code) {
    if (!error_code_.empty()) {
      allocator_->free(error_code_.ptr());
    }
    return ob_write_string(*allocator_, error_code, error_code_);
  }
  int set_error_message(const ObString &error_message) {
    if (!error_message_.empty()) {
      allocator_->free(error_message_.ptr());
    }
    return ob_write_string(*allocator_, error_message, error_message_);
  }
  const ObString &get_error_code() { return error_code_; }
  const ObString &get_error_message() { return error_message_; }

  bool is_success() { return 0 == error_code_.case_compare(SHARD_DDL_SUCCESS); }
  bool is_running() { return 0 == error_code_.case_compare(SHARD_DDL_RUNNING); }

public:
  static constexpr char const *SHARD_DDL_SUCCESS = "0000";
  static constexpr char const *SHARD_DDL_RUNNING = "0004";
  static constexpr char const *SHARD_DDL_JOB_ERROR = "8204";
  static constexpr char const *SHARD_DDL_JOB_ERROR_MSG = "Execute DDL job error";
  static constexpr char const *SHARD_DDL_DATA = "data";
  static constexpr char const *SHARD_DDL_STATUS = "ddlStatus";
  static constexpr char const *SHARD_DDL_ERROR_CODE = "errorCode";
  static constexpr char const *SHARD_DDL_ERROR_MSG = "errorMessage";
  static constexpr char const *SHARD_DDL_TASK_ID_LIST = "ddlTaskIdList";

private:
  common::ObArenaAllocator *allocator_;
  common::ObSEArray<int64_t, 1> ddl_task_id_list_;
  ObString ddl_status_;
  ObString error_code_;
  ObString error_message_;
};

#define TABLE_OPTIONS_MAP_BUCKET 1

class ObShardDDLTableSourceDefinition
{
public:
  ObShardDDLTableSourceDefinition() {}
  virtual ~ObShardDDLTableSourceDefinition() {}

  int init(const ObString &schema, const ObString &sql, const ObShardDDLOperation operation);
  int set_table_options(const ObString &key, const ObString value);

  int to_json(ObSqlString &buf);

private:
  const char* get_operation_name(ObShardDDLOperation operation);

private:
  static constexpr char const *SHARD_DDL_SCHEMA = "schema";
  static constexpr char const *SHARD_DDL_SQL = "sql";
  static constexpr char const *SHARD_DDL_OPERATION = "operation";
  static constexpr char const *SHARD_DDL_TABLE_OPTIONS = "tableOptions";

private:
  ObString schema_;
  ObString sql_;
  ObShardDDLOperation operation_;
  hash::ObHashMap<ObString, ObString> table_options_;
};

class ObShardDDLCont : public obutils::ObAsyncCommonTask
{
public:
  ObShardDDLCont(event::ObContinuation *cb_cont, event::ObEThread *cb_thread);
  virtual ~ObShardDDLCont() {}

  virtual void destroy();
  virtual int init_task();
  virtual void *get_callback_data() { return static_cast<void *>(&ddl_status_); }

  int init(const ObString &instance_id, const ObString &schema,
           const ObString &sql, const ObProxyBasicStmtType stmt_type,
           const ObProxyBasicStmtSubType sub_stmt_type);

private:
  int covert_stmt_type_to_operation(const ObProxyBasicStmtType stmt_type,
                                    const ObProxyBasicStmtSubType sub_stmt_type,
                                    ObShardDDLOperation &operation);
  int get_ddl_url(const char *url_template, char *&buffer);
  int post_ddl_request(const char *url, void *postrequest, void *response, write_func write_func_callback /*NULL*/);
  static int64_t write_data(void *ptr, int64_t size, int64_t nmemb, void *stream);

private:
  static const int64_t OBPROXY_MAX_JSON_INFO_SIZE = 64 * 1024; // 64K
  static const int64_t RETRY_INTERVAL_MS = 1000;
  static const int64_t CURL_CONNECTION_TIMEOUT = 10;
  static const int64_t CURL_TRANSFER_TIMEOUT = 5;
  static constexpr char const *JSON_HTTP_HEADER = "Content-Type:application/json;charset=UTF-8";
  static constexpr char const *ASYNC_DDL_URL = "%.*s/privateapi/v1/%s/zdalproxy/ddl/ddlAsyncByODP";
  static constexpr char const *CHECK_DDL_URL = "%.*s/privateapi/v1/%s/zdalproxy/ddl/checkDDLTaskStatus";

private:
  bool is_first_;
  common::ObArenaAllocator allocator_;
  ObString instance_id_;
  ObString schema_;
  ObString sql_;
  ObShardDDLOperation operation_;
  ObString async_ddl_url_;
  ObString check_ddl_url_;
  ObString response_string_;
  char *response_buf_;
  ObShardDDLStatus ddl_status_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_SHARD_DDL_CONT_H

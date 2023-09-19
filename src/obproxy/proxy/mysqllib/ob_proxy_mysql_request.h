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

#ifndef OBPROXY_MYSQL_REQUEST_H
#define OBPROXY_MYSQL_REQUEST_H

#include "common/ob_partition_key.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_mod_define.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "obproxy/cmd/ob_internal_cmd_processor.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "packet/ob_mysql_packet_util.h"

namespace oceanbase
{
namespace obproxy
{
class ObProxySessionPrivInfo;
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{

struct ObProxyKillQueryInfo
{
  ObProxyKillQueryInfo() { reset(); }
  ~ObProxyKillQueryInfo() { }

  void reset();
  bool is_lookup_succ() const { return common::OB_ENTRY_EXIST == errcode_; }
  bool is_need_lookup() const { return common::OB_MAX_ERROR_CODE == errcode_; }
  int do_privilege_check(const ObProxySessionPrivInfo &session_priv);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool is_kill_query_;
  int64_t cs_id_;//kill query cs_id
  uint32_t real_conn_id_;//cs_id_ maybe proxy conn id when in client service mode,
                         //we need store real conn_id and rewrite req pkt before sent to observer
  int errcode_;
  char *priv_name_;
  net::ObIpEndpoint server_addr_;
};

class ObProxyMysqlRequest
{
public:
  static const int64_t PARSE_EXTRA_CHAR_NUM = 2;
  ObProxyMysqlRequest();
  ~ObProxyMysqlRequest() { reset(); }
  void reuse(bool is_reset_origin_db_table = true); // do not free req_buf
  void reset(bool is_reset_origin_db_table = true); // reuse and free req_buf

  common::ObString get_sql();
  common::ObString get_sql_id();
  char *get_sql_id_buf() { return sql_id_buf_; }
  int64_t get_sql_id_buf_len() const { return common::OB_MAX_SQL_ID_LENGTH + 1; }
  common::ObString get_parse_sql() { return get_parse_sql(get_sql()); }
  common::ObString get_expr_sql() { return get_expr_sql(get_sql(), result_.get_parsed_length()); }
  common::ObString get_print_sql(const int64_t sql_len = PRINT_SQL_LEN) {  return get_print_sql(get_sql(), sql_len); }
  static common::ObString get_expr_sql(const common::ObString &req_sql, const int64_t parsed_length);
  static common::ObString get_parse_sql(const common::ObString &req_sql);
  static common::ObString get_print_sql(const common::ObString &req_sql, const int64_t sql_len = PRINT_SQL_LEN);
  common::ObString get_req_pkt();
  obutils::ObSqlParseResult &get_parse_result();
  const obutils::ObSqlParseResult &get_parse_result() const;
  obutils::ObSqlParseResult *get_ps_parse_result() { return ps_result_; }
  void set_ps_parse_result(obutils::ObSqlParseResult *ps_result) { ps_result_ = ps_result; }
  void set_text_ps_parse_result(obutils::ObSqlParseResult *text_ps_result)
  {
    if (NULL != text_ps_result) {
      result_.set_text_ps_info(*text_ps_result);
    }
  }
  bool is_real_dml_sql() const;
  bool is_internal_cmd() const { return is_internal_cmd_; }
  bool is_kill_query() const { return is_kill_query_; }
  bool is_large_request() const { return is_large_request_; }
  bool enable_analyze_internal_cmd() const { return enable_analyze_internal_cmd_; }
  bool is_mysql_req_in_ob20_payload() const { return is_mysql_req_in_ob20_payload_; }
  
  bool is_sharding_user() const { return USER_TYPE_SHARDING == user_identity_; }
  bool is_proxysys_user() const { return USER_TYPE_PROXYSYS == user_identity_; }
  bool is_inspector_user() const { return USER_TYPE_INSPECTOR == user_identity_; }
  bool is_rootsys_user() const { return USER_TYPE_ROOTSYS == user_identity_; }
  bool is_proxysys_tenant() const { return (is_proxysys_user() || is_inspector_user()); }

  void set_internal_cmd(const bool flag) { is_internal_cmd_ = flag; }
  void set_is_kill_query(const bool flag) { is_kill_query_ = flag; }
  void set_large_request(const bool flag) { is_large_request_ = flag; }
  void set_enable_analyze_internal_cmd(const bool internal) { enable_analyze_internal_cmd_ = internal; }
  void set_mysql_req_in_ob20_payload(const bool flag) { is_mysql_req_in_ob20_payload_ = flag; }

  void set_user_identity(const ObProxyLoginUserType type) { user_identity_ = type; }
  inline ObProxyLoginUserType get_user_identity() const { return user_identity_; }

  int64_t get_packet_len() { return meta_.pkt_len_; }

  ObMysqlPacketMeta &get_packet_meta() { return meta_; }
  const ObMysqlPacketMeta &get_packet_meta() const { return meta_; }
  void set_packet_meta(const ObMysqlPacketMeta &meta) { meta_ = meta; }

  // add received request
  // @buf_len the max requset buf length we will alloc for this request
  int add_request(event::ObIOBufferReader *buf_reader, const int64_t buf_len);
  int fill_query_info(const int64_t cs_id);

  int free_request_buf();
  int alloc_request_buf(int64_t buf_len);
  int free_prepare_execute_request_buf();
  int alloc_prepare_execute_request_buf(const int64_t buf_len);
  // may lead memory leak
  inline void borrow_req_buf(char *&req_buf, int64_t &req_buf_len_);
  common::ObIAllocator &get_param_allocator() { return allocator_; }

  ObInternalCmdInfo *cmd_info_;
  ObProxyKillQueryInfo *query_info_;

  TO_STRING_KV(K_(meta), K_(req_buf_len), K_(req_pkt_len), K_(is_internal_cmd), K_(is_kill_query),
               K_(is_large_request), K_(enable_analyze_internal_cmd), K_(is_mysql_req_in_ob20_payload));
private:
  ObMysqlPacketMeta meta_;   // request packet meta
  char *req_buf_;            // request buf
  int64_t req_buf_len_;      // request buf len
  int64_t req_pkt_len_;      // request pkt len, req_pkt_len_ maybe <= meta_.pkt_len_,
                             // because we maybe just save part of the large request.
  char *req_buf_for_prepare_execute_;            // request buf
  int64_t req_buf_for_prepare_execute_len_;      // request buf len

  // sql parse result, include all of information after sql parsing
  obutils::ObSqlParseResult result_;
  obutils::ObSqlParseResult *ps_result_; // point to ps_parse_result in ps cache
  ObProxyLoginUserType user_identity_;
  bool is_internal_cmd_;//indicate whether it is internal request
  bool is_kill_query_;
  bool is_large_request_;
  bool enable_analyze_internal_cmd_;//indicate whether need analyze internal cmd
  bool is_mysql_req_in_ob20_payload_; // whether the mysql req is in ob20 protocol req payload

  common::ObArenaAllocator allocator_;
  char sql_id_buf_[common::OB_MAX_SQL_ID_LENGTH + 1];
};

bool ObProxyMysqlRequest::is_real_dml_sql() const
{
  bool bret = false;
  switch (result_.get_stmt_type()) {
    case OBPROXY_T_SELECT: {
      //select without table name is not real dml
      if (!result_.get_table_name().empty()) {
        bret = true;
      }
      break;
    }
    case OBPROXY_T_UPDATE:
    case OBPROXY_T_DELETE:
    case OBPROXY_T_INSERT:
    case OBPROXY_T_MERGE:
    case OBPROXY_T_REPLACE: {
      bret = true;
      break;
    }
    default:
      break;
  }
  return bret;
}

inline void ObProxyMysqlRequest::reset(bool is_reset_origin_db_table /* true */)
{
  reuse(is_reset_origin_db_table);
  allocator_.reset();
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(free_request_buf())) {
    PROXY_LOG(ERROR, "free request buf error", K(ret));
  }

  if (OB_FAIL(free_prepare_execute_request_buf())) {
    PROXY_LOG(ERROR, "free prepare execute request buf error", K(ret));
  }
}


inline obutils::ObSqlParseResult &ObProxyMysqlRequest::get_parse_result()
{
  obutils::ObSqlParseResult *result = &result_;
  if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == meta_.cmd_ || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == meta_.cmd_)
      && NULL != ps_result_) {
    result = ps_result_;
  }
  return *result;
}

const obutils::ObSqlParseResult &ObProxyMysqlRequest::get_parse_result() const
{
  const obutils::ObSqlParseResult *result = &result_;
  if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == meta_.cmd_ || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == meta_.cmd_)
      && NULL != ps_result_) {
    result = ps_result_;
  }
  return *result;
}

inline int ObProxyMysqlRequest::alloc_request_buf(int64_t buf_len)
{
  int ret = common::OB_SUCCESS;
  // free buf if has alloc
  if (OB_UNLIKELY(NULL != req_buf_)) {
    if (OB_FAIL(free_request_buf())) {
      PROXY_LOG(ERROR, "free request buf error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(buf_len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(ERROR, "fail to alloc mem", K(buf_len), K(ret));
    } else {
      req_buf_ = buf;
      req_buf_len_ = buf_len;
    }
  }
  return ret;
}

inline int ObProxyMysqlRequest::free_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != req_buf_) {
    if (req_buf_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(ERROR, "req_buf_len_ must > 0", K_(req_buf_len), K_(req_buf), K(ret));
    } else {
      op_fixed_mem_free(req_buf_, req_buf_len_);
      req_buf_ = NULL;
      req_buf_len_ = 0;
    }
  }
  return ret;
}

inline int ObProxyMysqlRequest::alloc_prepare_execute_request_buf(const int64_t buf_len)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(buf_len < 0)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(ERROR, "buf_len must > 0", K(buf_len), K(ret));
  }

  // free buf if has alloc
  if (OB_SUCC(ret) && OB_UNLIKELY(NULL != req_buf_for_prepare_execute_)) {
    if (OB_FAIL(free_prepare_execute_request_buf())) {
      PROXY_LOG(ERROR, "free prepare execute request buf error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(buf_len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(ERROR, "fail to alloc mem", K(buf_len), K(ret));
    } else {
      req_buf_for_prepare_execute_ = buf;
      req_buf_for_prepare_execute_len_ = buf_len;
    }
  }
  return ret;
}

inline void ObProxyMysqlRequest::borrow_req_buf(char *&req_buf, int64_t &req_buf_len) {
  req_buf = req_buf_;
  req_buf_len = req_buf_len_;
  req_buf_ = NULL;
  req_buf_len_ = 0;
}

inline int ObProxyMysqlRequest::free_prepare_execute_request_buf()
{
  int ret = common::OB_SUCCESS;
  if (NULL != req_buf_for_prepare_execute_) {
    if (req_buf_for_prepare_execute_len_ <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(ERROR, "req_buf_len_ must > 0", K_(req_buf_for_prepare_execute_len), K_(req_buf_for_prepare_execute), K(ret));
    } else {
      op_fixed_mem_free(req_buf_for_prepare_execute_, req_buf_for_prepare_execute_len_);
      req_buf_for_prepare_execute_ = NULL;
      req_buf_for_prepare_execute_len_ = 0;
    }
  }
  return ret;
}

inline common::ObString ObProxyMysqlRequest::get_sql()
{
  const char *sql = NULL;
  int64_t sql_len = 0;
  if (OB_LIKELY(NULL != req_buf_ && req_pkt_len_ > MYSQL_NET_META_LENGTH)) {
    if (OB_LIKELY(obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE != meta_.cmd_)) {
      sql = req_buf_ + MYSQL_NET_META_LENGTH; // skip pkt meta(5 bytes)
      sql_len = req_pkt_len_ - MYSQL_NET_META_LENGTH;
    } else {
      int ret = OB_SUCCESS;
      uint64_t query_len = 0;
      const char *pos = req_buf_ + MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH; // skip 9 bytes
      int64_t buf_len = req_pkt_len_ - MYSQL_NET_META_LENGTH - MYSQL_PS_EXECUTE_HEADER_LENGTH;
      if (OB_FAIL(ObMysqlPacketUtil::get_length(pos, buf_len, query_len))) {
        PROXY_LOG(ERROR, "failed to get length", K(ret));
      } else if (query_len > 0) {
        int64_t copy_len = std::min(static_cast<int64_t>(query_len), req_buf_len_ - PARSE_EXTRA_CHAR_NUM);
        if (OB_ISNULL(req_buf_for_prepare_execute_) || OB_UNLIKELY(req_buf_for_prepare_execute_len_ != req_buf_len_)) {
          if (OB_FAIL(alloc_prepare_execute_request_buf(req_buf_len_))) {
            PROXY_LOG(ERROR, "fail to alloc buf", K_(req_buf_len), K(ret));
          } else {
            PROXY_LOG(DEBUG, "alloc request buf ", K_(req_buf_len));
          }
        }

        if (OB_SUCC(ret)) {
          MEMCPY(req_buf_for_prepare_execute_, pos, copy_len);
          req_buf_for_prepare_execute_[copy_len + 1] = 0;
          req_buf_for_prepare_execute_[copy_len] = 0;

          sql = req_buf_for_prepare_execute_;
          sql_len = copy_len;
        }
      }
    }
  }
  common::ObString sql_str(sql_len, sql);
  return sql_str;
}

inline common::ObString ObProxyMysqlRequest::get_sql_id()
{
  common::ObString sql_id(static_cast<int64_t>(strlen(sql_id_buf_)), sql_id_buf_);
  return sql_id;
}

common::ObString ObProxyMysqlRequest::get_parse_sql(const common::ObString &req_sql)
{
  return (req_sql.empty()
      ? req_sql
      : common::ObString(req_sql.length() + PARSE_EXTRA_CHAR_NUM, req_sql.ptr()));
}

common::ObString ObProxyMysqlRequest::get_expr_sql(
    const common::ObString &req_sql, const int64_t parsed_length)
{
  const char *expr_sql_str = NULL;
  int64_t expr_sql_len = 0;
  if (!req_sql.empty() && req_sql.length() >= parsed_length) {
    expr_sql_str = req_sql.ptr() + parsed_length;
    expr_sql_len = req_sql.length() - parsed_length + PARSE_EXTRA_CHAR_NUM;
  }
  common::ObString expr_sql(expr_sql_len, expr_sql_str);
  return expr_sql;
}

common::ObString ObProxyMysqlRequest::get_print_sql(const common::ObString &req_sql, const int64_t sql_len)
{
  return (req_sql.empty()
      ? req_sql
      : common::ObString(std::min(req_sql.length(), static_cast<int32_t>(sql_len)), req_sql.ptr()));
}

inline common::ObString ObProxyMysqlRequest::get_req_pkt()
{
  common::ObString req_pkt(req_pkt_len_, req_buf_);
  return req_pkt;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_MYSQL_REQUEST_H */

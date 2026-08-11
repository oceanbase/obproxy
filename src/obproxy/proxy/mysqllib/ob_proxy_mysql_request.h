/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
class ObCdcDumpPacket;
struct ObProxyKillQueryInfo
{
  ObProxyKillQueryInfo() { reset(); }
  ~ObProxyKillQueryInfo() { }

  void reset();
  bool is_lookup_succ() const { return common::OB_ENTRY_EXIST == errcode_; }
  bool is_need_lookup() const { return common::OB_MAX_ERROR_CODE == errcode_; }
  bool need_change_connector() const { return common::OB_INVALID_ID != group_id_;}
  int do_privilege_check(const ObProxySessionPrivInfo &session_priv);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool is_kill_query_;
  int64_t cs_id_;//kill query cs_id
  int64_t group_id_; // only use for sharding user
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
  inline void reset_parse_result() { result_.reset(); }
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
  bool is_cdc_coordinator_sql() const { return result_.is_cdc_coordinator_related(); }
  bool is_cdc_msgserver_sql() const { return obmysql::OB_MYSQL_COM_CDC_DUMP == meta_.cmd_; }
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
  bool is_for_update_sql();
  static bool is_for_update_sql(common::ObString src_sql);

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
  void borrow_req_buf(char *&req_buf, int64_t &req_buf_len_);
  common::ObIAllocator &get_param_allocator() { return allocator_; }

  void set_enable_server_kill_connection(const bool enable_server_kill_connection) { enable_server_kill_connection_ = enable_server_kill_connection; }
  bool is_enable_server_kill_connection() const { return enable_server_kill_connection_; }
  common::ObString get_expr_parse_second_sql() { return expr_parse_second_sql_; }
  void set_expr_parse_second_sql(common::ObString &sql) { expr_parse_second_sql_ = sql; }
  // multi-stmt 首条为 BEGIN/START TRANSACTION 时，路由解析使用第二条 sql
  bool is_multi_stmt_with_start_trans() const { return !expr_parse_second_sql_.empty(); }
  int preprocess_multi_sql(ObIArray<common::ObString> &sql_array);
  void set_cdc_dump_pkt(ObCdcDumpPacket* cdc_dump_pkt) { cdc_dump_pkt_ = cdc_dump_pkt; }
  const ObCdcDumpPacket* get_cdc_dump_pkt() const { return cdc_dump_pkt_; }

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
  bool enable_server_kill_connection_; // whether server handle OBPROXY_T_SUB_KILL_CONNECTION

  struct {
    bool valid_ : 1;
    bool value_: 1;
  } is_for_update_sql_;

  common::ObArenaAllocator allocator_;
  char sql_id_buf_[common::OB_MAX_SQL_ID_LENGTH + 1];
  common::ObString expr_parse_second_sql_;  // 对multi-stmt: 使用第二条sql路由，需要存储second sql，给expr_parse使用
  ObCdcDumpPacket* cdc_dump_pkt_;
};

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

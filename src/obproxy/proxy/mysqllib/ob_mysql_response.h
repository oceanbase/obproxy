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

#ifndef OBPROXY_MYSQL_RESPONSE_H
#define OBPROXY_MYSQL_RESPONSE_H
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_object.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_mysql_resp_analyzer.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "lib/utility/ob_2_0_full_link_trace_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum ObOKPacketActionType {
  OK_PACKET_ACTION_SEND = 0, // send this packet directly (in multi stmt or mysql mode)
  OK_PACKET_ACTION_REWRITE,  // rewrite this packet, last valid ok packet
  OK_PACKET_ACTION_CONSUME,  // consume this packet, last extra ok packet
};

// ok, error, eof packet's content,
// valid only when response is received completed.
struct ObRespAnalyzeResult
{
  ObRespAnalyzeResult() { reset(); }
  ~ObRespAnalyzeResult() { destroy(); }
  
  void reset();
  void destroy() { reset(); }

  bool has_more_compress_packet() const { return has_more_compress_packet_; }
  bool is_decompressed() const { return is_decompressed_; }
  bool is_trans_completed() const { return is_trans_completed_; }
  bool is_resp_completed() const { return is_resp_completed_; }
  bool is_partition_hit() const { return is_partition_hit_; }
  bool is_last_insert_id_changed() const { return is_last_insert_id_changed_; }
  bool is_server_db_reset() const { return is_server_db_reset_; }
  bool has_new_sys_var() const { return has_new_sys_var_; }
  bool has_proxy_idc_name_user_var() const { return has_proxy_idc_name_user_var_; }
  bool is_last_ok_handled() const { return is_last_ok_handled_; }

  bool is_error_resp() const { return ERROR_PACKET_ENDING_TYPE == ending_type_; }
  bool is_ok_resp() const { return OK_PACKET_ENDING_TYPE == ending_type_; }
  bool is_eof_resp() const { return EOF_PACKET_ENDING_TYPE == ending_type_; }
  bool is_handshake_pkt() const { return HANDSHAKE_PACKET_ENDING_TYPE == ending_type_; }
  obmysql::OMPKError &get_error_pkt() { return error_pkt_; }
  const obmysql::OMPKError &get_error_pkt() const { return error_pkt_; }
  uint16_t get_error_code() const { return error_pkt_.get_err_code(); }
  common::ObString get_error_message() const { return error_pkt_.get_message(); }
  int64_t get_reserved_len() const { return reserved_len_; }
  int64_t get_reserved_len_for_ob20_ok() const { return reserved_len_for_ob20_ok_; }
  int64_t get_last_ok_pkt_len() const { return last_ok_pkt_len_; }
  int64_t get_rewritten_last_ok_pkt_len() const { return rewritten_last_ok_pkt_len_; }
  uint32_t get_connection_id() const { return connection_id_; }
  common::ObString get_scramble_string() const { return common::ObString::make_string(scramble_buf_); }
  ObOKPacketActionType get_ok_packet_action_type() const { return ok_packet_action_type_; }
  bool is_resultset_resp() const { return is_resultset_resp_; }
  bool is_server_can_use_compress() const { return (1 == server_capabilities_lower_.capability_flag_.OB_SERVER_CAN_USE_COMPRESS); }
  void set_server_trace_id(const common::ObString &trace_id);
  bool support_ssl() const { return 1 == server_capabilities_lower_.capability_flag_.OB_SERVER_SSL; }

  bool is_not_supported_error() const
  {
    return (is_error_resp() && ER_NOT_SUPPORTED_YET == error_pkt_.get_err_code());
  }
  bool is_bad_db_error() const
  {
    return (is_error_resp() && ER_BAD_DB_ERROR == error_pkt_.get_err_code());
  }
  bool is_unknown_tenant_error() const
  {
    return (is_error_resp() && -common::OB_TENANT_NOT_EXIST == error_pkt_.get_err_code());
  }
  bool is_tenant_not_in_server_error() const
  {
    return (is_error_resp() && -common::OB_TENANT_NOT_IN_SERVER == error_pkt_.get_err_code());
  }
  bool is_cluster_not_match_error() const
  {
    return (is_error_resp() && -common::OB_CLUSTER_NO_MATCH == error_pkt_.get_err_code());
  }
  bool is_server_init_error() const
  {
    return (is_error_resp() && -common::OB_SERVER_IS_INIT == error_pkt_.get_err_code());
  }
  bool is_server_stopping_error() const
  {
    return (is_error_resp() && -common::OB_SERVER_IS_STOPPING == error_pkt_.get_err_code());
  }
  bool is_session_entry_exist() const
  {
    return (is_error_resp() && -common::OB_SESSION_ENTRY_EXIST == error_pkt_.get_err_code());
  }
  bool is_net_packet_too_large_error() const
  {
    return (is_error_resp() && ER_NET_PACKET_TOO_LARGE == error_pkt_.get_err_code());
  }
  bool is_connect_error() const
  {
    return (is_error_resp() && -common::OB_CONNECT_ERROR == error_pkt_.get_err_code());
  }
  bool is_readonly_error() const
  {
    return (is_error_resp() && -common::OB_ERR_READ_ONLY == error_pkt_.get_err_code());
  }
  bool is_reroute_error() const
  {
    return (is_error_resp() && -common::OB_ERR_REROUTE == error_pkt_.get_err_code());
  }
  bool is_ora_fatal_error() const
  {
    return (is_error_resp() && -common::OB_ORA_FATAL_ERROR == error_pkt_.get_err_code());
  }
  bool is_standby_weak_readonly_error() const
  {
    return (is_error_resp() && -common::OB_STANDBY_WEAK_READ_ONLY == error_pkt_.get_err_code());
  }

  inline uint32_t get_server_capability() const
  {
    return ((server_capabilities_upper_.capability_ << 16) | server_capabilities_lower_.capability_);
  }

  Ob20ExtraInfo &get_extra_info() { return extra_info_; }
  const Ob20ExtraInfo &get_extra_info() const { return extra_info_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  
  /* define elements */
  bool has_more_compress_packet_;
  bool is_decompressed_;
  bool is_trans_completed_;
  bool is_resp_completed_;
  // indicate this mysql response ending pkt type(ok, error, eof, string eof,etc.)
  ObMysqlRespEndingType ending_type_;
  // indicate whether request data hit the target observer,
  // if not, maybe need update location cache.
  bool is_partition_hit_;
  // indicate whether sql request make last_insert_id changed,
  // used to record last_insert_id server session.
  bool is_last_insert_id_changed_;
  // indicate whether observer session level database was set to empty.
  // when it was set to emtpy, all server session in conneciton pool shuold disconnect.
  bool is_server_db_reset_;
  // if observer add new sys var, this value will be set
  bool has_new_sys_var_;
  //extract from handshake pkt
  // if observer add new sys var, this value will be set
  bool has_proxy_idc_name_user_var_;
  uint32_t connection_id_;
  obmysql::OMPKHandshake::ServerCapabilitiesLower server_capabilities_lower_;
  obmysql::OMPKHandshake::ServerCapabilitiesUpper server_capabilities_upper_;
  bool is_resultset_resp_;
  char scramble_buf_[obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE + 1];
  ObOKPacketActionType ok_packet_action_type_;
  int64_t reserved_len_;
  int64_t reserved_len_for_ob20_ok_;
  int64_t last_ok_pkt_len_;
  int64_t rewritten_last_ok_pkt_len_;
  bool is_last_ok_handled_;
  // only one of structs below is valid, according to ending_type_
  obmysql::OMPKError error_pkt_;
  obutils::ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> error_pkt_buf_; // only store error pkt
  common::ObString server_trace_id_;
  char server_trace_id_buf_[common::OB_MAX_TRACE_ID_LENGTH];

  Ob20ExtraInfo extra_info_;
  common::FLTObjManage flt_;

  DISALLOW_COPY_AND_ASSIGN(ObRespAnalyzeResult);
};

class ObMysqlResp
{
public:
  ObMysqlResp() { }
  ~ObMysqlResp() { destroy(); }
  void reset() { analyze_result_.reset(); }
  void destroy() { analyze_result_.destroy(); }
  
  ObRespAnalyzeResult &get_analyze_result() { return analyze_result_; }
  const ObRespAnalyzeResult &get_analyze_result() const { return analyze_result_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  ObRespAnalyzeResult analyze_result_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlResp);
};

inline void ObRespAnalyzeResult::reset()
{
  has_more_compress_packet_ = false;
  is_decompressed_ = false;
  is_trans_completed_ = false;
  is_resp_completed_ = false;
  ending_type_ = MAX_PACKET_ENDING_TYPE;
  is_partition_hit_ = true;
  is_last_insert_id_changed_ = false;
  is_server_db_reset_ = false;
  has_new_sys_var_ = false;
  has_proxy_idc_name_user_var_ = false;
  connection_id_ = 0;
  scramble_buf_[0] = '\0';
  ok_packet_action_type_ = OK_PACKET_ACTION_SEND;
  reserved_len_ = 0;
  reserved_len_for_ob20_ok_ = 0;
  last_ok_pkt_len_ = 0;
  rewritten_last_ok_pkt_len_ = 0;
  is_last_ok_handled_ = false;
  error_pkt_buf_.reset();
  server_capabilities_lower_.capability_ = 0;
  server_capabilities_upper_.capability_ = 0;
  is_resultset_resp_ = false;
  server_trace_id_.reset();
  extra_info_.reset();
  flt_.reset();
}

inline void ObRespAnalyzeResult::set_server_trace_id(const common::ObString &trace_id)
{
  if (trace_id.empty()) {
    server_trace_id_.reset();
  } else {
    common::ObString::obstr_size_t copy_len = std::min(trace_id.length(),
         common::ObString::obstr_size_t(common::OB_MAX_TRACE_ID_LENGTH));
    if (copy_len >= 0) { // just for defense
      MEMCPY(server_trace_id_buf_, trace_id.ptr(), copy_len);
      server_trace_id_.assign(server_trace_id_buf_, copy_len);
    }
  }
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_RESP_ANALYZE_RESULT_H

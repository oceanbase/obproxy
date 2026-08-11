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

#ifndef OBPROXY_MYSQL_REQUEST_ANALYZER_H
#define OBPROXY_MYSQL_REQUEST_ANALYZER_H
#include "ob_mysql_common_define.h"
#include "ob_proxy_mysql_request.h"
#include "ob_proxy_auth_parser.h"
#include "ob_proxy_parser_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "ob_proxy_session_info.h"
#include "obproxy/proxy/route/obproxy_part_info.h"
#include "obproxy/proxy/mysql/ob_prepare_statement_struct.h"


namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObCachedVariables;
struct SqlFieldResult;
}
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{
class ObProtocolDiagnosis;
typedef common::ObString ObRequestBuffer;

enum ObRequestPhase
{
  REQ_PHASE_HANDSHAKE = 0,
  REQ_PHASE_LOGIN_AUTH_SWITCH_RESP,         // 由 client 发送 handshake response 请求触发的 auth switch
  REQ_PHASE_LOGIN_AUTH_MORE_DATA_RESP,      // 由 client 发送 auth more data response（caching_sha2_password full auth）
  REQ_PHASE_CHANGE_USER_AUTH_SWITCH_RESP,   // 由 client 发送 com_change_user 请求触发的 auth switch
  REQ_PHASE_CHANGE_USER_AUTH_SWITCH_AUTH_MORE_DATA, // 场景: change user -> auth switch -> auth more data
  REQ_PHASE_CHANGE_USER_AUTH_MORE_DATA,             // 场景: change user -> auth more data（无 auth switch）
  REQ_PHASE_RESET_SESSION_AUTH_SWITCH_RESP, // 由 SERVER_SEND_RESET_SESSION_AS_FIRST_LOGIN (com_change_user) 触发的 auth switch
  REQ_PHASE_RESET_SESSION_AUTH_MORE_DATA_RESP, // 由 reset session 流程触发的 auth more data response
  REQ_PHASE_FILE_CONTENT,                   // 由 load data local infile 触发的文件内容传输命令
  REQ_PHASE_COMMAND,
  REQ_PHASE_COMMAND_SEND_LONG_DATA,         // 标记 client 正在发送 com_send_long_data 请求, 该请求无 Response
};
inline common::ObString get_request_phase_string(const ObRequestPhase phase)
{
  const char *str = "";
  switch (phase) {
    case REQ_PHASE_HANDSHAKE:
      str = "REQ_PHASE_HANDSHAKE";
      break;
    case REQ_PHASE_LOGIN_AUTH_SWITCH_RESP:
      str = "REQ_PHASE_LOGIN_AUTH_SWITCH_RESP";
      break;
    case REQ_PHASE_LOGIN_AUTH_MORE_DATA_RESP:
      str = "REQ_PHASE_LOGIN_AUTH_MORE_DATA_RESP";
      break;
    case REQ_PHASE_CHANGE_USER_AUTH_SWITCH_RESP:
      str = "REQ_PHASE_CHANGE_USER_AUTH_SWITCH_RESP";
      break;
    case REQ_PHASE_CHANGE_USER_AUTH_SWITCH_AUTH_MORE_DATA:
      str = "REQ_PHASE_CHANGE_USER_AUTH_SWITCH_AUTH_MORE_DATA";
      break;
    case REQ_PHASE_CHANGE_USER_AUTH_MORE_DATA:
      str = "REQ_PHASE_CHANGE_USER_AUTH_MORE_DATA";
      break;
    case REQ_PHASE_RESET_SESSION_AUTH_SWITCH_RESP:
      str = "REQ_PHASE_RESET_SESSION_AUTH_SWITCH_RESP";
      break;
    case REQ_PHASE_RESET_SESSION_AUTH_MORE_DATA_RESP:
      str = "REQ_PHASE_RESET_SESSION_AUTH_MORE_DATA_RESP";
      break;
    case REQ_PHASE_FILE_CONTENT:
      str = "REQ_PHASE_FILE_CONTENT";
      break;
    case REQ_PHASE_COMMAND:
      str = "REQ_PHASE_COMMAND";
      break;
    case REQ_PHASE_COMMAND_SEND_LONG_DATA:
      str = "REQ_PHASE_COMMAND_SEND_LONG_DATA";
      break;
    default:
      str = "UNKNOWN";
  }
  return common::ObString::make_string(str);
}

struct ObRequestAnalyzeCtx
{
  ObRequestAnalyzeCtx() { reset(); }
  ~ObRequestAnalyzeCtx() { }
  void reset() { memset(this, 0, sizeof(ObRequestAnalyzeCtx)); }

  inline const bool is_handshake_req_phase() const { return request_phase_ == REQ_PHASE_HANDSHAKE; }
  inline const bool is_auth_switch_resp_phase() const { return request_phase_ == REQ_PHASE_CHANGE_USER_AUTH_SWITCH_RESP
                                                               || request_phase_ == REQ_PHASE_RESET_SESSION_AUTH_SWITCH_RESP
                                                               || request_phase_ == REQ_PHASE_LOGIN_AUTH_SWITCH_RESP; }
  inline const bool is_auth_more_data_resp_phase() const { return request_phase_ == REQ_PHASE_CHANGE_USER_AUTH_SWITCH_AUTH_MORE_DATA
                                                                  || request_phase_ == REQ_PHASE_CHANGE_USER_AUTH_MORE_DATA
                                                                  || request_phase_ == REQ_PHASE_RESET_SESSION_AUTH_MORE_DATA_RESP
                                                                  || request_phase_ == REQ_PHASE_LOGIN_AUTH_MORE_DATA_RESP; }
  inline const bool is_file_content_req_phase() const {  return request_phase_ == REQ_PHASE_FILE_CONTENT; }
  static int init_auth_request_analyze_ctx(ObRequestAnalyzeCtx &ctx,
                                           event::ObIOBufferReader *buffer_reader,
                                           const common::ObString &vip_tenant_name,
                                           const common::ObString &vip_cluster_name);
  ObRequestPhase request_phase_;
  bool drop_origin_db_table_name_;
  bool is_sharding_mode_;
  common::ObCollationType connection_collation_;
  ObProxyParseMode parse_mode_;
  event::ObIOBufferReader *reader_;
  obutils::ObCachedVariables *cached_variables_;

  common::ObString vip_tenant_name_;
  common::ObString vip_cluster_name_;

  int64_t large_request_threshold_len_;
  int64_t request_buffer_length_;
  bool using_ldg_;
  bool using_service_name_;
};

class ObMysqlRequestAnalyzer
{
public:
  ObMysqlRequestAnalyzer() : 
      total_packet_length_(0),
      payload_len_(-1),
      packet_seq_(0),
      cmd_(0),
      nbytes_analyze_(0),
      is_last_request_packet_(false),
      request_count_(0),
      header_content_offset_(0) { MEMSET(header_length_buffer_, 0, MYSQL_NET_META_LENGTH); }
  ObMysqlRequestAnalyzer(const ObMysqlRequestAnalyzer& analyzer);
  ObMysqlRequestAnalyzer &operator=(const ObMysqlRequestAnalyzer &analyzer);
  int is_request_finished(event::ObIOBufferReader &reader, bool &is_finish, obmysql::ObMySQLCmd cmd,
                          int64_t request_len, int64_t &analyze_len,
                          ObProtocolDiagnosis *protocol_diagnosis);
  uint8_t get_packet_seq() const { return packet_seq_; }
  void reset();
  void reuse();
  static void analyze_request(const ObRequestAnalyzeCtx &ctx,
                              ObMysqlAuthRequest &auth_request,
                              ObProxyMysqlRequest &client_request,
                              obmysql::ObMySQLCmd &sql_cmd,
                              ObMysqlAnalyzeStatus &status,
                              const bool is_oracle_mode = false,
                              const bool is_client_support_ob20_protocol = false);
  static void extract_fileds(const ObExprParseResult& result, obutils::SqlFieldResult &sql_result);
  static int parse_sql_fileds(ObProxyMysqlRequest &client_request,
                              common::ObCollationType connection_collation);
  static int init_cmd_info(ObProxyMysqlRequest &client_request);

  static int analyze_execute_header(const int64_t param_num,
                                    const char *&bitmap,
                                    int8_t &new_param_bound_flag,
                                    const char *&buf, int64_t &data_len);

  static int parse_param_type(const int64_t param_num,
                              common::ObIArray<obmysql::EMySQLFieldType> &param_types,
                              const char *&buf, int64_t &data_len);

  static int parse_param_type(const int64_t param_num,
                              common::ObIArray<obmysql::EMySQLFieldType> &param_types,
                              common::ObIArray<obmysql::TypeInfo> &type_infos,
                              const char *&buf, int64_t &data_len);

  static int parse_param_type_from_reader(int64_t& param_offset,
                                          const int64_t param_num,
                                          common::ObIArray<obmysql::EMySQLFieldType> &param_types,
                                          event::ObIOBufferReader* reader,
                                          int64_t& analyzed_len,
                                          bool& is_finished);
  static int do_analyze_execute_param(const char *buf,
                                      int64_t data_len,
                                      const int64_t param_num,
                                      common::ObIArray<obmysql::EMySQLFieldType> *param_types,
                                      ObProxyMysqlRequest &client_request,
                                      const int64_t target_index,
                                      ObObj &target_obj);
  static int analyze_execute_param(const int64_t param_num,
                                   common::ObIArray<obmysql::EMySQLFieldType> &param_types,
                                   ObProxyMysqlRequest &client_request,
                                   const int64_t target_index,
                                   common::ObObj &target_obj);
  static int analyze_send_long_data_param(ObProxyMysqlRequest &client_request,
                                          const int64_t execute_param_index,
                                          ObProxyPartInfo *part_info,
                                          ObPsIdEntry *ps_id_entry,
                                          ObObj &target_obj);
  static int analyze_prepare_execute_param(ObProxyMysqlRequest &client_request,
                                           const int64_t target_index,
                                           ObObj &target_obj);

  static int parse_param_value(common::ObIAllocator &allocator,
                               const char *&data, int64_t &buf_len, const uint8_t type,
                               const ObCharsetType charset, ObObj &param);

  static int analyze_sql_id(const ObString &sql, ObProxyMysqlRequest &client_request, common::ObString &sql_id);

private:
  int get_payload_length(const char *buffer);
  int check_is_last_request_packet(obmysql::ObMySQLCmd cmd);
  int is_request_finished(const ObRequestBuffer &buff, bool &is_finish,
                          obmysql::ObMySQLCmd cmd,
                          int64_t &analyze_len,
                          ObProtocolDiagnosis *protocol_diagnosis = NULL);

  // handle auth reqeust packet
  static int handle_auth_request(event::ObIOBufferReader &reader, ObMysqlAnalyzeResult &result);
  static int handle_no_cmd_request(const ObRequestAnalyzeCtx &ctx, ObMysqlAnalyzeResult &result);

  // dispatch mysql pkt according to cmd type, and then parse each other
  static int do_analyze_request(const ObRequestAnalyzeCtx &ctx,
                                const obmysql::ObMySQLCmd sql_cmd,
                                ObMysqlAuthRequest &auth_request,
                                ObProxyMysqlRequest &client_request,
                                const bool is_oracle_mode = false);
  static int handle_internal_cmd(ObProxyMysqlRequest &client_request);
  static void extract_fileds(const ObExprParseResult& result, ObProxyMysqlRequest &client_request);

  static void mysql_hex_dump(const void *data, const int64_t size);

  
  static int parse_mysql_timestamp_value(const obmysql::EMySQLFieldType field_type,
                                         const char *&data, int64_t &buf_len, ObObj &param);
  static int parse_mysql_time_value(const char *&data, int64_t &buf_len, ObObj &param);

  static int decode_type_info(const char*& buf, int64_t &buf_len, obmysql::TypeInfo &type_info);

  static int decode_type_info_from_reader(event::ObIOBufferReader* reader,
                                          int64_t &decoded_offset,
                                          obmysql::TypeInfo &type_info);

  static int get_uint1_from_reader(event::ObIOBufferReader* reader,
                                   int64_t &decoded_offset,
                                   uint8_t &v);
  static int get_uint2_from_reader(event::ObIOBufferReader* reader,
                                   int64_t &decoded_offset,
                                   uint16_t &v);
  static int get_uint3_from_reader(event::ObIOBufferReader* reader,
                                   int64_t &decoded_offset,
                                   uint32_t &v);
  static int get_uint8_from_reader(event::ObIOBufferReader* reader,
                                   int64_t &decoded_offset,
                                   uint64_t &v);
  static int get_int1_from_reader(event::ObIOBufferReader* reader,
                                  int64_t &decoded_offset,
                                  int8_t &v);

  static int get_length_from_reader(event::ObIOBufferReader* reader,
                                    int64_t &decoded_offset,
                                    uint64_t &length);

private:
  int64_t total_packet_length_;          // total request packet length
  int64_t payload_len_;                 // current ananlyzing packet's payload len
  uint8_t packet_seq_;                  // current analyzing packet's seq
  uint8_t cmd_;                         // current analyzing packet's cmd
  int64_t nbytes_analyze_;         // total bytes already analyze
  bool is_last_request_packet_;    // whether is last mysql packet of the request
  int64_t request_count_;
  char header_length_buffer_[MYSQL_NET_META_LENGTH];
  int64_t header_content_offset_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_REQUEST_ANALYZER_H

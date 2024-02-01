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
class SqlFieldResult;
}
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{
class ObProtocolDiagnosis;
typedef common::ObString ObRequestBuffer;

struct ObRequestAnalyzeCtx
{
  ObRequestAnalyzeCtx() { reset(); }
  ~ObRequestAnalyzeCtx() { }
  void reset() { memset(this, 0, sizeof(ObRequestAnalyzeCtx)); }

  static int init_auth_request_analyze_ctx(ObRequestAnalyzeCtx &ctx,
                                           event::ObIOBufferReader *buffer_reader,
                                           const common::ObString &vip_tenant_name,
                                           const common::ObString &vip_cluster_name);

  bool is_auth_;
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
      is_last_request_packet_(true),
      request_count_(0),
      header_content_offset_(0) { MEMSET(header_length_buffer_, 0, MYSQL_NET_META_LENGTH); }
  ObMysqlRequestAnalyzer(const ObMysqlRequestAnalyzer& analyzer);
  ObMysqlRequestAnalyzer &operator=(const ObMysqlRequestAnalyzer &analyzer);
  int is_request_finished(event::ObIOBufferReader &reader, bool &is_finish,
                          obmysql::ObMySQLCmd cmd, int64_t analyze_len,
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
                          ObProtocolDiagnosis *protocol_diagnosis = NULL);

  // handle auth reqeust packet
  static int handle_auth_request(event::ObIOBufferReader &reader, ObMysqlAnalyzeResult &result);

  // dispatch mysql pkt according to cmd type, and then parse each other
  static int do_analyze_request(const ObRequestAnalyzeCtx &ctx,
                                const obmysql::ObMySQLCmd sql_cmd,
                                ObMysqlAuthRequest &auth_request,
                                ObProxyMysqlRequest &client_request,
                                const bool is_oracle_mode = false);
  static int handle_internal_cmd(ObProxyMysqlRequest &client_request);
  static void extract_fileds(const ObExprParseResult& result, ObProxyMysqlRequest &client_request);

  static int rewrite_part_key_comment(event::ObIOBufferReader *reader,
                                      ObProxyMysqlRequest &client_request);

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
  bool is_last_request_packet_;    // whether is mysql last package
  int64_t request_count_;
  char header_length_buffer_[MYSQL_NET_META_LENGTH];
  int64_t header_content_offset_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_REQUEST_ANALYZER_H

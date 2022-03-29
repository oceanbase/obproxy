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

#ifndef OBPROXY_AUTH_PARSER_H
#define OBPROXY_AUTH_PARSER_H

#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_mysql_resp_analyzer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
// handshake response request analyzed result
struct ObHSRResult
{
  static const int64_t OB_MAX_CONN_ID_BUF_LEN = 10; // string length of max uint32_t(2**32 - 1)
  static const int64_t OB_MAX_VERSION_BUF_LEN = 20; // string length of max int64_t(2**63 - 1)
  static const int64_t OB_MAX_PROXY_CONN_ID_BUF_LEN = 20; // string length of max uint64_t(2**64 - 1)

  ObHSRResult() : cluster_id_(OB_DEFAULT_CLUSTER_ID), name_len_(0), name_buf_(NULL) { reset(); }
  ~ObHSRResult() { destroy(); }
  void reset();
  void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int do_parse_auth_result(const char ut_separator,
                           const char tc_separator,
                           const char cluster_id_separator,
                           const common::ObString &user,
                           const common::ObString &tenant,
                           const common::ObString &cluster,
                           const common::ObString &cluster_id_str);

  // name of the SQL account which client wants to log in
  // this string should be interpreted using the character set
  // indicated by character set field.
  // mysql client: mysql -u UserName@TenantName#ClusterName:ClusterId -p xxxxx
  // full_name_         is UserName@TenantName#ClusterName:ClusterId
  // user_tenant_name_  is UserName@TenantName
  // cluster_name_      is ClusterName
  // tenant_name_       is TenantName
  // user_name_         is UserName
  // cluster_id_        is ClusterId
  // is_clustername_from_default_ whether login packet has clustername
  // has_tenant_username_ whether login packet has tenant
  // has_cluster_username_ whether login packet has clustername
  common::ObString full_name_;
  common::ObString user_tenant_name_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  bool is_clustername_from_default_;
  int64_t cluster_id_;
  bool has_tenant_username_;
  bool has_cluster_username_;

  obmysql::OMPKHandshakeResponse response_;

  char full_name_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];
private:
  int64_t name_len_;
  char *name_buf_;
  DISALLOW_COPY_AND_ASSIGN(ObHSRResult);
};

class ObMysqlAuthRequest
{
public:
  static const int64_t OB_AUTH_REQEUST_BUF_LEN = 128;

  ObMysqlAuthRequest() { reset(); }
  ~ObMysqlAuthRequest() { reset(); }
  void reset();
  void destroy() { reset(); }

  ObHSRResult &get_hsr_result() { return result_; }

  // add_len == 0 means to add all data in reader
  int add_auth_request(event::ObIOBufferReader *reader, const int64_t add_len);
  common::ObString &get_auth_request() { return auth_; }

  int64_t get_packet_len() { return meta_.pkt_len_; }
  void set_packet_meta(const ObMysqlPacketMeta meta) { meta_ = meta; }
  ObMysqlPacketMeta &get_packet_meta() { return meta_; }

private:
  ObMysqlPacketMeta meta_;     // mysql request meta info
  common::ObString auth_;      // auth request
  ObHSRResult result_;         // handshake response info

  // store the whole auth reqeust
  obutils::ObVariableLenBuffer<OB_AUTH_REQEUST_BUF_LEN> auth_buffer_;
};

class ObProxyAuthParser
{
public:
  ObProxyAuthParser() { }
  ~ObProxyAuthParser() { }

  static int parse_auth(ObMysqlAuthRequest &request,
                        const common::ObString &defualt_tenant_name,
                        const common::ObString &defualt_cluster_name);

  // standard full username: "UserName@TenantName#ClusterName:ClusterId"
  // nonstandard full username: "ClusterName.TenantName.UserName:ClusterId"
  static int parse_full_user_name(const common::ObString &full_name,
                                  const common::ObString &default_tenant_name,
                                  const common::ObString &default_cluster_name,
                                  ObHSRResult  &hsr);

  static int covert_hex_to_string(const common::ObString &hex, char *str, const int64_t str_len);
  static int covert_string_to_hex(const common::ObString &string, char *hex, const int64_t hex_len);

  static const int64_t MAX_UNFORMAL_FORMAT_SEPARATOR_COUNT = 64;
  static char unformal_format_separator[MAX_UNFORMAL_FORMAT_SEPARATOR_COUNT + 1]; // terminated by '\0'

public:
  static const char FORMAL_USER_TENANT_SEPARATOR = '@';
  static const char FORMAL_TENANT_CLUSTER_SEPARATOR = '#';
  static const char CLUSTER_ID_SEPARATOR = ':';

  // parse handshake response request
  static int parse_handshake_response(ObMysqlAuthRequest &request,
                                      const common::ObString &default_tenant_name,
                                      const common::ObString &defualt_cluster_name);

  static void analyze_user_name_attr(const common::ObString &full_name,
                                     bool &is_standard_username,
                                     char &separator);

  static int do_parse_full_user_name(const common::ObString &full_name,
                                     const char separator,
                                     const common::ObString &default_tenant_name,
                                     const common::ObString &default_cluster_name,
                                     ObHSRResult &hsr);

  static bool is_nonstandard_username(const common::ObString &full_name, const char separator);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_AUTH_PARSER_H

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

#define USING_LOG_PREFIX PROXY
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

void ObHSRResult::reset()
{
  user_name_.reset();
  tenant_name_.reset();
  full_name_.reset();
  user_tenant_name_.reset();
  cluster_name_.reset();
  cluster_id_ = OB_DEFAULT_CLUSTER_ID;
  is_clustername_from_default_ = false;
  has_tenant_username_ = false;
  has_cluster_username_ = false;
  response_.reset();
  full_name_buf_[0] = '\0';
  if (NULL != name_buf_) {
    name_buf_[0] = '\0';
  }
}

void ObHSRResult::destroy()
{
  if (NULL != name_buf_ && name_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_len_);
  }
  name_buf_ = NULL;
  name_len_ = 0;
  reset();
}

int64_t ObHSRResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name),
       K_(tenant_name),
       K_(user_name),
       K_(cluster_id),
       K_(user_tenant_name),
       K_(full_name),
       K_(response),
       K_(is_clustername_from_default),
       K_(has_tenant_username),
       K_(has_cluster_username));
  J_OBJ_END();
  return pos;
}

int ObHSRResult::do_parse_auth_result(const char ut_separator,
                                      const char tc_separator,
                                      const char cluster_id_separator,
                                      const ObString &user,
                                      const ObString &tenant,
                                      const ObString &cluster,
                                      const ObString &cluster_id_str)
{
  int ret = OB_SUCCESS;
  char *buf_start = NULL;
  int64_t len = user.length() + tenant.length() + cluster.length() + 2; // separators('@','#')
  if (!cluster_id_str.empty()) {
    len = len + cluster_id_str.length() + 1; // separator ':'
  }

  if (OB_UNLIKELY(user.empty()) || OB_UNLIKELY(tenant.empty()) || OB_UNLIKELY(cluster.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid full user name", K(user), K(tenant), K(cluster), K(ret));
  } else {
    if (len <= OB_PROXY_FULL_USER_NAME_MAX_LEN) {
      buf_start = full_name_buf_;
    } else {
      if (NULL != name_buf_ && name_len_ < len) {
        op_fixed_mem_free(name_buf_, name_len_);
        name_buf_ = NULL;
        name_len_ = 0;
      }

      if (NULL == name_buf_) {
        if (OB_ISNULL(name_buf_ = static_cast<char *>(op_fixed_mem_alloc(len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to alloc memory for login name", K(ret));
        } else {
          name_buf_[0] = '\0';
          name_len_ = len;
        }
      }
      if (OB_SUCC(ret)) {
        buf_start = name_buf_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    MEMCPY(buf_start, user.ptr(), user.length());
    user_name_.assign_ptr(buf_start, user.length());
    pos += user.length();
    buf_start[pos++] = ut_separator;
    MEMCPY(buf_start + pos, tenant.ptr(), tenant.length());
    tenant_name_.assign_ptr(buf_start + pos, tenant.length());
    pos += tenant.length();
    user_tenant_name_.assign_ptr(buf_start, static_cast<int32_t>(pos));
    response_.set_username(user_tenant_name_);
    buf_start[pos++] = tc_separator;
    MEMCPY(buf_start + pos, cluster.ptr(), cluster.length());
    cluster_name_.assign_ptr(buf_start + pos, cluster.length());
    pos += cluster.length();
    if (!cluster_id_str.empty()) {
      if (OB_FAIL(get_int_value(cluster_id_str, cluster_id_))) {
        LOG_WDIAG("fail to get int value for cluster id", K(cluster_id_str), K(ret));
      } else {
        buf_start[pos++] = cluster_id_separator;
        MEMCPY(buf_start + pos, cluster_id_str.ptr(), cluster_id_str.length());
        pos += cluster_id_str.length();
      }
    }
    full_name_.assign_ptr(buf_start, static_cast<int32_t>(pos));
  }
  return ret;
}

void ObMysqlAuthRequest::reset()
{
  auth_.reset();
  result_.reset();
  meta_.reset();
  auth_buffer_.reset();
}

int ObMysqlAuthRequest::add_auth_request(event::ObIOBufferReader *reader, const int64_t add_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader) || OB_UNLIKELY(add_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid buffer reader", K(reader), K(ret));
  } else {
    int64_t avail_bytes = reader->read_avail();
    int64_t len = 0;
    if (0 == add_len) {
      len = avail_bytes;
    } else {
      len = std::min(avail_bytes, add_len);
    }

    if (OB_UNLIKELY(len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid len", K(len), K(ret));
    } else {
      if (auth_buffer_.is_inited()) {
        auth_buffer_.reset();
      }
      if (OB_FAIL(auth_buffer_.init(len))) {
        LOG_WDIAG("fail to init auth buffer", K(len), K(ret));
      } else {
        char *buf = const_cast<char *>(auth_buffer_.ptr());
        char *written_pos = reader->copy(buf, len, 0);
        if (OB_UNLIKELY(written_pos != buf + len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("write pos not expected", K(written_pos), K(buf), K(len), K(ret));
        } else {
          auth_.assign_ptr(buf, static_cast<int32_t>(len));
        }
      }
    }
  }
  return ret;
}

char ObProxyAuthParser::unformal_format_separator[MAX_UNFORMAL_FORMAT_SEPARATOR_COUNT + 1] = {'\0'};

int ObProxyAuthParser::parse_auth(ObMysqlAuthRequest &request,
                                  const ObString &default_tenant_name,
                                  const ObString &default_cluster_name)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = request.get_packet_meta().cmd_;
  switch (cmd) {
    case OB_MYSQL_COM_LOGIN: {
      if (OB_FAIL(parse_handshake_response(request, default_tenant_name, default_cluster_name))) {
        LOG_WDIAG("fail to parse handshake response", "cmd",
                 ObProxyParserUtils::get_sql_cmd_name(cmd), K(default_tenant_name),
                 K(default_cluster_name), K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("proxy auth parser can not parse this cmd", "cmd",
               ObProxyParserUtils::get_sql_cmd_name(cmd), K(ret));
      break;
    }
  }

  return ret;
}

void ObProxyAuthParser::analyze_user_name_attr(const ObString &full_name,
                                               bool &is_standard_username, char &separator)
{
  if (NULL != full_name.find(FORMAL_USER_TENANT_SEPARATOR)
      || NULL != full_name.find(FORMAL_TENANT_CLUSTER_SEPARATOR)) {
    is_standard_username = true;
  } else {
    bool found_separator = false;
    int64_t len = STRLEN(unformal_format_separator);
    for (int64_t i = 0; !found_separator && i < len; ++i) {
      if (is_nonstandard_username(full_name, unformal_format_separator[i])) {
        found_separator = true;
        separator = unformal_format_separator[i];
      }
    }

    if (found_separator) {
      is_standard_username = false;
    } else {
      is_standard_username = true;
    }
  }
}

bool ObProxyAuthParser::is_nonstandard_username(const ObString &full_name, const char separator)
{
  bool bret = false;
  const char *fisrt_at_pos = full_name.find(separator);
  const char *second_at_pos = NULL;
  if (NULL != fisrt_at_pos) {
    int64_t tmp_len = full_name.length() - (fisrt_at_pos - full_name.ptr()) - 1;
    ObString tmp_name(tmp_len, fisrt_at_pos + 1);
    second_at_pos = tmp_name.find(separator);
    if (NULL != second_at_pos) {
      bret = true;
    }
  }
  return bret;
}

int ObProxyAuthParser::handle_full_user_name(ObString &full_user_name, const char separator,
                                             ObString &user, ObString &tenant,
                                             ObString &cluster, ObString &cluster_id_str)
{
  int ret = OB_SUCCESS;

  const char *tenant_pos = NULL;
  const char *user_cluster_pos = NULL;
  const char *cluster_id_pos = NULL;

  ObString name_id_str;

  if ('\0' == separator) {
    //standard full username: user@tenant#cluster:cluster_id
    tenant_pos = full_user_name.find(FORMAL_USER_TENANT_SEPARATOR);
    user_cluster_pos = full_user_name.find(FORMAL_TENANT_CLUSTER_SEPARATOR);
    if (NULL != tenant_pos && NULL != user_cluster_pos) {
      user = full_user_name.split_on(tenant_pos);
      tenant = full_user_name.split_on(user_cluster_pos);
      cluster = full_user_name;
    } else if (NULL != tenant_pos) {
      user = full_user_name.split_on(tenant_pos);
      tenant = full_user_name;
    } else if (NULL != user_cluster_pos) {
      user = full_user_name.split_on(user_cluster_pos);
      cluster = full_user_name;
    } else {
      user = full_user_name;
    }
    if (!cluster.empty()) {
      if (NULL != (cluster_id_pos = cluster.find(CLUSTER_ID_SEPARATOR))) {
        name_id_str = cluster;
        cluster = name_id_str.split_on(cluster_id_pos);
        cluster_id_str = name_id_str;
      }
    }
  } else {
    //unstandard full user name:ClusterSeparatorTenantSeparatorUserSeparatorClusterID
    tenant_pos = full_user_name.find(separator);
    cluster = full_user_name.split_on(tenant_pos);
    user_cluster_pos = full_user_name.find(separator);
    tenant = full_user_name.split_on(separator);
    user = full_user_name;
    if (NULL != (cluster_id_pos = user.find(separator))
        || NULL != (cluster_id_pos = user.find(CLUSTER_ID_SEPARATOR))) {
      name_id_str = user;
      user = name_id_str.split_on(cluster_id_pos);
      cluster_id_str = name_id_str;
    }
  }
  return ret;
}

int ObProxyAuthParser::parse_full_user_name(const ObString &full_name,
                                            const ObString &default_tenant_name,
                                            const ObString &default_cluster_name,
                                            ObHSRResult &hsr)
{
  int ret = OB_SUCCESS;
  bool is_standard_username = false;
  char separator = '\0';

  analyze_user_name_attr(full_name, is_standard_username, separator);

  if (OB_FAIL(do_parse_full_user_name(full_name, separator, default_tenant_name,
                                      default_cluster_name, hsr))) {
      LOG_WDIAG("fail to parse standard full username", K(full_name), K(ret));
  }
  return ret;
}

int ObProxyAuthParser::do_parse_full_user_name(const ObString &full_name,
                                               const char separator,
                                               const ObString &default_tenant_name,
                                               const ObString &default_cluster_name,
                                               ObHSRResult &hsr)
{
  int ret = OB_SUCCESS;

  ObString full_user_name = full_name;
  ObString user;
  ObString tenant;
  ObString cluster;
  ObString cluster_id_str;
  char tenant_str[OB_MAX_TENANT_NAME_LENGTH];
  char cluster_str[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];

  if (OB_FAIL(handle_full_user_name(full_user_name, separator, user, tenant,
                                    cluster, cluster_id_str))) {
    LOG_WDIAG("fail to handle full user name", K(full_user_name), K(separator), K(ret));
  } else {
    if (tenant.empty() && cluster.empty()) {
      // if proxy start with specified tenant and cluster, just use them
      ObProxyConfig &proxy_config = get_global_proxy_config();
      obsys::CRLockGuard guard(proxy_config.rwlock_);
      int64_t proxy_tenant_len = strlen(proxy_config.proxy_tenant_name.str());
      int64_t proxy_cluster_len = strlen(proxy_config.rootservice_cluster_name.str());
      if (proxy_tenant_len > 0 && proxy_cluster_len > 0) {
        if (OB_UNLIKELY(proxy_tenant_len > OB_MAX_TENANT_NAME_LENGTH)
            || OB_UNLIKELY(proxy_cluster_len > OB_PROXY_MAX_CLUSTER_NAME_LENGTH)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WDIAG("proxy_tenant or proxy_cluster is too long", K(proxy_tenant_len), K(proxy_cluster_len), K(ret));
        } else {
          memcpy(tenant_str, proxy_config.proxy_tenant_name.str(), proxy_tenant_len);
          memcpy(cluster_str, proxy_config.rootservice_cluster_name.str(), proxy_cluster_len);
          tenant.assign_ptr(tenant_str, static_cast<int32_t>(proxy_tenant_len));
          cluster.assign_ptr(cluster_str, static_cast<int32_t>(proxy_cluster_len));
        }
      }
    } else {
      if (!tenant.empty()) {
        hsr.has_tenant_username_ = true;
      }
      if (!cluster.empty()) {
        hsr.has_cluster_username_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant.empty()) {
      tenant = default_tenant_name;
    }
    if (cluster.empty()) {
      hsr.is_clustername_from_default_ = true;
      cluster = default_cluster_name;
    }

    if (OB_FAIL(hsr.do_parse_auth_result(FORMAL_USER_TENANT_SEPARATOR,
                                         FORMAL_TENANT_CLUSTER_SEPARATOR,
                                         CLUSTER_ID_SEPARATOR,
                                         user, tenant, cluster, cluster_id_str))) {
      LOG_WDIAG("fail to do parse auth result", K(hsr), K(ret));
    }

  }

  return ret;
}

int ObProxyAuthParser::parse_handshake_response(ObMysqlAuthRequest &request,
                                                const ObString &default_tenant_name,
                                                const ObString &default_cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_MYSQL_COM_LOGIN != request.get_packet_meta().cmd_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handshake packet is not OB_MYSQL_COM_LOGIN", "cmd",
             request.get_packet_meta().cmd_, K(ret));
  } else {
    ObString &auth = request.get_auth_request();
    ObHSRResult &result = request.get_hsr_result();

    if (OB_UNLIKELY(auth.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handshake response is empty", K(auth), K(ret));
    } else {
      const char *start = auth.ptr() + MYSQL_NET_HEADER_LENGTH;
      uint32_t len = static_cast<uint32_t>(auth.length() - MYSQL_NET_HEADER_LENGTH);
      OMPKHandshakeResponse &hsr = result.response_;

      hsr.set_content(start, len);
      if (OB_FAIL(hsr.decode())) {
        LOG_WDIAG("fail to decode hand shake response packet", K(ret));
      } else if (hsr.get_username().empty() && hsr.is_ssl_request()) {
        // maybe is SSL Request, usename is allowed empty
      } else if (hsr.get_username().empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("user name can not be empty in handshake response", K(ret));
      } else if (OB_FAIL(parse_full_user_name(hsr.get_username(), default_tenant_name,
                                              default_cluster_name, result))) {
        LOG_WDIAG("fail to parse full username", "fullname", hsr.get_username(), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to parse handshake response", K(result), K(ret));
    } else {
      LOG_WDIAG("fail to parse handshake response", K(ret));
    }
  }
  return ret;
}

int ObProxyAuthParser::covert_hex_to_string(const ObString &hex, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(hex.empty()) || OB_UNLIKELY(hex.length() % 2 != 0)
      || OB_ISNULL(str) || OB_UNLIKELY(str_len <= 0)
      || (str_len <= hex.length() / 2)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t half_len = hex.length() / 2;
    char tmp[3];
    for (int64_t i = 0; OB_SUCC(ret) && i < half_len; ++i) {
      tmp[0] = hex[2*i];
      tmp[1] = hex[2*i + 1];
      tmp[2] = '\0';
      if (OB_UNLIKELY(-1 == sscanf(tmp, "%x", (unsigned int*)(&str[i])))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to call sscanf", KERRMSGS, K(ret));
      }
    }
    str[half_len] = '\0';
  }
  return ret;
}

int ObProxyAuthParser::covert_string_to_hex(const ObString &string, char *hex_str, const int64_t hex_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(string.empty())
      || OB_ISNULL(hex_str) || OB_UNLIKELY(hex_len <= (string.length() * 2))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < string.length(); ++i) {
      if (OB_UNLIKELY(-1 == sprintf(hex_str, "%.2x", (uint8_t)string[i]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to call sprintf", KERRMSGS, K(ret));
      } else {
        hex_str += 2;
      }
    }
    *hex_str = '\0';
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

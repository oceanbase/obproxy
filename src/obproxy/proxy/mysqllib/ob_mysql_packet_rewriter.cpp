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
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "lib/encrypt/ob_encrypted_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int64_t ObHandshakeResponseParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_saved_login), K_(cluster_name), K_(proxy_scramble),
       K_(conn_id_buf), K_(proxy_conn_id_buf), K_(global_vars_version_buf),
       K_(cap_buf), K_(proxy_version_buf), K_(client_ip_buf));
  J_OBJ_END();
  return pos;
}

int ObHandshakeResponseParam::write_conn_id_buf(const uint32_t conn_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(conn_id_buf_, OB_MAX_UINT32_BUF_LEN,
                              pos, "%u", conn_id))) {
    LOG_WARN("fail to append connection id", K(pos), K(conn_id_buf_),
             K(conn_id), K(ret));
  }
  return ret;
}

int ObHandshakeResponseParam::write_proxy_version_buf()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(proxy_version_buf_, OB_MAX_VERSION_BUF_LEN, pos, "%s", PACKAGE_VERSION))) {
    LOG_WARN("fail to append proxy version", K(pos), K(proxy_version_buf_),
             K(PACKAGE_VERSION), K(ret));
  }
  return ret;
}

int ObHandshakeResponseParam::write_proxy_conn_id_buf(const uint64_t proxy_conn_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(proxy_conn_id_buf_, OB_MAX_UINT64_BUF_LEN, pos, "%lu", proxy_conn_id))) {
    LOG_WARN("fail to append proxy connection id", K(pos), K(proxy_conn_id_buf_),
             K(proxy_conn_id), K(ret));
  }
  return ret;
}

int ObHandshakeResponseParam::write_cluster_id_buf(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(cluster_id_buf_, OB_MAX_UINT64_BUF_LEN, pos, "%ld", cluster_id))) {
    LOG_WARN("fail to append cluster id", K(pos), K(cluster_id_buf_),
             K(cluster_id), K(ret));
  } else {
    cluster_id_ = cluster_id;
  }
  return ret;
}

int ObHandshakeResponseParam::write_global_vars_version_buf(const int64_t version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(global_vars_version_buf_, OB_MAX_UINT64_BUF_LEN, pos, "%ld", version))) {
    LOG_WARN("fail to append vars version failed", K(pos),
             K(global_vars_version_buf_), K(version), K(ret));
  }
  return ret;
}

int ObHandshakeResponseParam::write_capability_buf(const uint64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(cap_buf_, OB_MAX_UINT64_BUF_LEN, pos, "%lu", cap))) {
    LOG_WARN("fail to append capability flag", K(pos), K_(cap_buf),
             K(cap), K(ret));
  }
  return ret;
}

int ObHandshakeResponseParam::write_proxy_scramble(const common::ObString &proxy_scramble,
    const common::ObString &server_scramble)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(proxy_scramble.length() != server_scramble.length())
      || OB_UNLIKELY(server_scramble.length() != sizeof(proxy_scramble_buf_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scramble length", K(proxy_scramble), K(server_scramble), K(ret));
  } else if (OB_FAIL(ObEncryptedHelper::my_xor(reinterpret_cast<const unsigned char *>(proxy_scramble.ptr()),
      reinterpret_cast<const unsigned char *>(server_scramble.ptr()),
      static_cast<const unsigned int>(server_scramble.length()),
      reinterpret_cast<unsigned char *>(proxy_scramble_buf_)))) {
    LOG_WARN("fail to encrypt proxy_scramble", K(proxy_scramble), K(server_scramble), K(ret));
  } else {
    proxy_scramble_.assign_ptr(proxy_scramble_buf_, sizeof(proxy_scramble_buf_));
  }
  return ret;
}

int ObHandshakeResponseParam::write_client_addr_buf(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    //do not write
  } else if (OB_UNLIKELY(!addr.ip_to_string(client_ip_buf_, OB_MAX_IP_BUF_LEN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to ip_to_string", K(addr), K(ret));
  }
  return ret;
}

int ObMysqlPacketRewriter::rewrite_ok_packet(const OMPKOK &src_ok,
                                             const ObMySQLCapabilityFlags &des_cap,
                                             OMPKOK &des_ok,
                                             const bool need_save_sys_var)
{
  int ret = OB_SUCCESS;
  des_ok.set_affected_rows(src_ok.get_affected_rows());
  des_ok.set_last_insert_id(src_ok.get_last_insert_id());
  des_ok.set_server_status(src_ok.get_server_status());
  des_ok.set_warnings(src_ok.get_warnings());
  des_ok.set_seq(src_ok.get_seq());
  des_ok.set_message(src_ok.get_message());
  ObMySQLCapabilityFlags tmp_flags = des_cap;
//  tmp_flags.cap_flags_.OB_CLIENT_SESSION_TRACK = 0;
//  tmp_flags.cap_flags_.OB_CLIENT_DEPRECATE_EOF = 0;

  if (need_save_sys_var) {
    des_ok.set_state_changed(true);
//    tmp_flags.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
    const common::ObIArray<ObStringKV> &system_vars = src_ok.get_system_vars();
    for (int64_t i = 0; i < system_vars.count(); ++i) {
      if (ObSessionFieldMgr::is_nls_date_timestamp_format_variable(system_vars.at(i).key_)) {
        des_ok.add_system_var(system_vars.at(i));
      }
    }
  }
  des_ok.set_capability(tmp_flags);
  return ret;
};


int ObMysqlPacketRewriter::add_connect_attr(const char *key, const char *value,
                                            OMPKHandshakeResponse &tg_hsr)
{
  ObStringKV str_kv;
  str_kv.key_.assign_ptr(key, static_cast<int32_t>(STRLEN(key)));
  str_kv.value_.assign_ptr(value, static_cast<int32_t>(STRLEN(value)));
  return tg_hsr.add_connect_attr(str_kv);
}

int ObMysqlPacketRewriter::add_connect_attr(const char *key, const common::ObString &value,
                                            OMPKHandshakeResponse &tg_hsr)
{
  ObStringKV str_kv;
  str_kv.key_.assign_ptr(key, static_cast<int32_t>(STRLEN(key)));
  str_kv.value_.assign_ptr(value.ptr(), value.length());
  return tg_hsr.add_connect_attr(str_kv);
}

int ObMysqlPacketRewriter::rewrite_handshake_response_packet(
    ObMysqlAuthRequest &orig_auth_req,
    ObHandshakeResponseParam &param,
    OMPKHandshakeResponse &tg_hsr)
{
  int ret = OB_SUCCESS;
  // 0. deep copy response
  tg_hsr = orig_auth_req.get_hsr_result().response_;

  // 1. assign seq num
  tg_hsr.set_seq(static_cast<int8_t>(orig_auth_req.get_packet_meta().pkt_seq_));

  // 2. add CLIENT_CONNECT_ATTRS and CLIENT_SESSION_TRACK flags
  ObMySQLCapabilityFlags cap_flag = tg_hsr.get_capability_flags();
  cap_flag.cap_flags_.OB_CLIENT_CONNECT_ATTRS = 1;
  cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
  cap_flag.cap_flags_.OB_CLIENT_SSL = param.use_ssl_;
  tg_hsr.set_capability_flags(cap_flag);

  // 3. if is_saved_login, clear database(-D xxx)
  if (param.is_saved_login_) {
    tg_hsr.set_database(ObString::make_empty_string());
  }

  // find client_ip
  ObStringKV string_kv;
  for (int64_t i = 0; OB_SUCC(ret) && i <  tg_hsr.get_connect_attrs().count(); ++i) {
    string_kv = tg_hsr.get_connect_attrs().at(i);
    if (0 == string_kv.key_.case_compare(OB_MYSQL_CLIENT_IP)) {
      if (!string_kv.value_.empty()) {
        snprintf(param.client_ip_buf_, ObHandshakeResponseParam::OB_MAX_IP_BUF_LEN, "%.*s", string_kv.value_.length(), string_kv.value_.ptr());
      }
      break;
    }
  }

  // reset before add
  tg_hsr.reset_connect_attr();
  // 4. add obproxy specified connect attrs:
  // a. proxy_mode
  // b. connection id
  // c. global_vars_version
  // d. cluster_name
  // e. cluster_id
  // f. capability_flag
  if (OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_MODE, OB_MYSQL_CLIENT_OBPROXY_MODE, tg_hsr))) {
    LOG_WARN("fail to add proxy mode", K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_CONNECTION_ID, param.conn_id_buf_, tg_hsr))) {
    LOG_WARN("fail to add connection id", K(param.conn_id_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_PROXY_CONNECTION_ID, param.proxy_conn_id_buf_, tg_hsr))) {
    LOG_WARN("fail to add proxy_sessid", K(param.proxy_conn_id_buf_), K(ret));
  } else if (param.is_cluster_name_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_CLUSTER_NAME, param.cluster_name_, tg_hsr))) {
    LOG_WARN("fail to add cluster name", K(param.cluster_name_), K(ret));
  } else if (param.is_cluster_id_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_CLUSTER_ID, param.cluster_id_buf_, tg_hsr))) {
    LOG_WARN("fail to add cluster id", K(param.cluster_id_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_GLOBAL_VARS_VERSION, param.global_vars_version_buf_, tg_hsr))) {
    LOG_WARN("fail to add global vars version", K(param.global_vars_version_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_CAPABILITY_FLAG, param.cap_buf_, tg_hsr))) {
    LOG_WARN("fail to add capability flag", K(param.cap_buf_), K(ret));
  } else if (param.is_proxy_scramble_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_SCRAMBLE, param.proxy_scramble_, tg_hsr))) {
    LOG_WARN("fail to add global vars version", K(param.proxy_scramble_), K(ret));
  } else if (param.is_client_ip_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_IP, param.client_ip_buf_, tg_hsr))) {
    LOG_WARN("fail to add client ip", K(param.client_ip_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_PROXY_VERSION, param.proxy_version_buf_, tg_hsr))) {
    LOG_WARN("fail to add proxy version", K(param.proxy_version_buf_), K(ret));
  } else {
    //do nothing
  }


  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

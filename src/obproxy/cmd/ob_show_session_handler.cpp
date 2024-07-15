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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_show_session_handler.h"
#include "obutils/ob_read_stale_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//SessionListColumnID
enum
{
  OB_SLC_ID = 0,
  OB_SLC_TENANT,
  OB_SLC_USER,
  OB_SLC_HOST,
  OB_SLC_DB,
  OB_SLC_TRANS_COUNT,
  OB_SLC_SS_COUNT,
  OB_SLC_STATE,
  OB_SLC_TID,
  OB_SLC_PID,
  OB_SLC_MAX_SLIST_COLUMN_ID,
};

//SessionInternalListColumnID
enum
{
  OB_SILC_PROXY_SESSID = 0,
  OB_SILC_ID,
  OB_SILC_CLUSTER,
  OB_SILC_TENANT,
  OB_SILC_USER,
  OB_SILC_HOST,
  OB_SILC_DB,
  OB_SILC_TRANS_COUNT,
  OB_SILC_SS_COUNT,
  OB_SILC_STATE,
  OB_SILC_TID,
  OB_SILC_PID,
  OB_SILC_USING_SSL,
  OB_SILC_MAX_SLIST_COLUMN_ID,
};

//SessionAttributeColumnID
enum
{
  OB_SAC_NAME = 0,
  OB_SAC_VALUE,
  OB_SAC_INFO,
  OB_SAC_MAX_ATTRIBUTE_COLUMN_ID,
};

//SessionVariablesColumnId
enum
{
  OB_SVC_NAME = 0,
  OB_SVC_VALUE,
  OB_SVC_INFO,
  OB_SVC_TYPE,
  OB_SVC_FLAG,
  OB_SVC_MAX_VARIABLES_COLUMN_ID,
};

//SessionStatColumnId
enum
{
  OB_SSC_NAME = 0,
  OB_SSC_VALUE,
  OB_SSC_MAX_STAT_COLUMN_ID,
};

const ObProxyColumnSchema LIST_COLUMN_ARRAY[OB_SLC_MAX_SLIST_COLUMN_ID]           = {
    ObProxyColumnSchema::make_schema(OB_SLC_ID,           "Id",                 OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SLC_TENANT,       "Tenant",             OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_USER,         "User",               OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_HOST,         "Host",               OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_DB,           "db",                 OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_TRANS_COUNT,  "trans_count",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SLC_SS_COUNT,     "svr_session_count",  OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SLC_STATE,        "state",              OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_TID,          "tid",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SLC_PID,          "pid",                OB_MYSQL_TYPE_LONG),
};

// SessionWeakReadStaleColumnId
enum
{
  OB_SWRS_ADDR = 0,
  OB_SWRS_TABLE_ID,
  OB_SWRS_PARTITION_ID,
  OB_SWRS_FEEDBACK_TIME,
  OB_SWRS_MAX_STALE_COLUMN_ID,
};

const ObProxyColumnSchema INTERNAL_LIST_COLUMN_ARRAY[OB_SILC_MAX_SLIST_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_SILC_PROXY_SESSID,  "proxy_sessid",       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SILC_ID,            "Id",                 OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SILC_CLUSTER,       "Cluster",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_TENANT,        "Tenant",             OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_USER,          "User",               OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_HOST,          "Host",               OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_DB,            "db",                 OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_TRANS_COUNT,   "trans_count",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SILC_SS_COUNT,      "svr_session_count",  OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SILC_STATE,         "state",              OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SILC_TID,           "tid",                OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_SILC_PID,           "pid",                OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_SILC_USING_SSL,     "using_ssl",          OB_MYSQL_TYPE_LONG),
};

const ObProxyColumnSchema ATTRIBUTE_COLUMN_ARRAY[OB_SLC_MAX_SLIST_COLUMN_ID]      = {
  ObProxyColumnSchema::make_schema(OB_SAC_NAME,   "attribute_name", OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SAC_VALUE,  "value",          OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SAC_INFO,   "info",           OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema VARIABLES_COLUMN_ARRAY[OB_SVC_MAX_VARIABLES_COLUMN_ID]  = {
  ObProxyColumnSchema::make_schema(OB_SVC_NAME,   "variable_name",      OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SVC_VALUE,  "value",              OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SVC_INFO,   "info",               OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SVC_TYPE,   "modified_type",      OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SVC_FLAG,   "sys_variable_flag",  OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema STAT_COLUMN_ARRAY[OB_SSC_MAX_STAT_COLUMN_ID]            = {
  ObProxyColumnSchema::make_schema(OB_SSC_NAME,   "stat_name",  OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SSC_VALUE,  "value",      OB_MYSQL_TYPE_LONGLONG),
};

const ObProxyColumnSchema READ_STALE_ARRAY[OB_SWRS_MAX_STALE_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_SWRS_ADDR,           "server_addr",    OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SWRS_TABLE_ID,       "table_id",       OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_SWRS_PARTITION_ID,   "partition_id",   OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_SWRS_FEEDBACK_TIME,  "feedback_time",  OB_MYSQL_TYPE_VARCHAR),
};

ObShowSessionHandler::ObShowSessionHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), list_bucket_(0),
      cs_id_array_()
{
  SET_CS_HANDLER(&ObShowSessionHandler::dump_cs_details);
}

int ObShowSessionHandler::dump_cs_list_header()
{
  int ret = OB_SUCCESS;
  if (header_encoded_) {
    DEBUG_ICMD("header is already encoded, skip this");
  } else {
    if (OBPROXY_T_SUB_SESSION_LIST == sub_type_) {
      if (OB_FAIL(encode_header(LIST_COLUMN_ARRAY, OB_SLC_MAX_SLIST_COLUMN_ID))) {
        WDIAG_ICMD("fail to encode header", K(ret), K_(sub_type));
      } else {
        header_encoded_ = true;
      }
    } else if (OBPROXY_T_SUB_SESSION_LIST_INTERNAL == sub_type_) {
      if (OB_FAIL(encode_header(INTERNAL_LIST_COLUMN_ARRAY, OB_SILC_MAX_SLIST_COLUMN_ID))) {
        WDIAG_ICMD("fail to encode header", K(ret), K_(sub_type));
      } else {
        header_encoded_ = true;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      EDIAG_ICMD("it should not happened, unknown sub_type", K(ret), K_(sub_type));
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_attribute_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(ATTRIBUTE_COLUMN_ARRAY, OB_SAC_MAX_ATTRIBUTE_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_variable_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(VARIABLES_COLUMN_ARRAY, OB_SVC_MAX_VARIABLES_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_stat_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(STAT_COLUMN_ARRAY, OB_SSC_MAX_STAT_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_details(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  switch (sub_type_) {
    case OBPROXY_T_SUB_SESSION_ATTRIBUTE: {
      if (OB_FAIL(dump_cs_attribute_header())) {
        WDIAG_ICMD("fail to dump cs attribute header", K(ret));
      } else if (OB_FAIL(dump_cs_attribute(cs))) {
        WDIAG_ICMD("fail to dump cs attribute body", K(ret));
      }
      break;
    }
    case OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL: {
      if (OB_FAIL(dump_cs_variable_header())) {
        WDIAG_ICMD("fail to dump cs variable header", K(ret));
      } else if (OB_FAIL(dump_cs_variables_local(cs))) {
        WDIAG_ICMD("fail to dump cs variable local body", K(ret));
      }
      break;
    }
    case OBPROXY_T_SUB_SESSION_VARIABLES_ALL: {
      if (OB_FAIL(dump_cs_variable_header())) {
        WDIAG_ICMD("fail to dump cs variable header", K(ret));
      } else if (OB_FAIL(dump_cs_variables_all(cs))) {
        WDIAG_ICMD("fail to dump cs variable all body", K(ret));
      }
      break;
    }
    case OBPROXY_T_SUB_SESSION_STAT: {
      if (OB_FAIL(dump_cs_stat_header())) {
        WDIAG_ICMD("fail to dump cs stat header", K(ret));
      } else if (OB_FAIL(dump_cs_stat(cs))) {
        WDIAG_ICMD("fail to dump cs stat body", K(ret));
      }
      break;
    }
    case OBPROXY_T_SUB_SESSION_READ_STALE: {
      if (OB_FAIL(dump_cs_read_stale_replica_header())) {
        WDIAG_ICMD("fail to dump weak read stale replica", K(ret));
      } else if(OB_FAIL(dump_cs_read_stale_replica(cs))) {
        WDIAG_ICMD("fail to dump weak read stale replica body", K(ret));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      WDIAG_ICMD("unknown type to dump_cs_details", K(sub_type_), K(ret));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet",  K(ret));
    } else {
      DEBUG_ICMD("succ to dump cs details", K(sub_type_));
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_variables_local(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &client_info = cs.get_session_info();
  ObSEArray<ObSessionSysField, MAX_CHANGED_SYS_VAR_COUNT> sys_vars;
  if (OB_FAIL(client_info.get_changed_sys_vars(sys_vars))) {
    WDIAG_ICMD("fail to get changed sys vars", K(ret));
  } else if (OB_FAIL(dump_cs_variables_common(sys_vars, OB_FIELD_CHANGED_SYS_VAR))) {
    WDIAG_ICMD("fail to dump cs changed sys variables", K(ret));
  } else if (OB_FAIL(dump_cs_variables_user(cs))) {
    WDIAG_ICMD("fail to dump cs user variables", K(ret));
  } else {
    DEBUG_ICMD("succ to dump cs local variables", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_variables_all(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &client_info = cs.get_session_info();

  ObSEArray<ObSessionSysField, MAX_SYS_VAR_COUNT> sys_vars;
  if (OB_FAIL(client_info.get_all_sys_vars(sys_vars))) {
    WDIAG_ICMD("fail to get all sys vars", K(ret));
  } else if (OB_FAIL(dump_cs_variables_common(sys_vars, OB_FIELD_SYS_VAR))) {
    WDIAG_ICMD("fail to dump cs all sys variables", K(ret));
  } else if (OB_FAIL(dump_cs_variables_user(cs))) {
    WDIAG_ICMD("fail to dump cs user variables", K(ret));
  } else {
    DEBUG_ICMD("succ to dump cs all variables", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_variables_user(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &client_info = cs.get_session_info();
  ObSEArray<ObSessionBaseField, MAX_USER_VAR_COUNT> user_vars;
  if (OB_FAIL(client_info.get_all_user_vars(user_vars))) {
    WDIAG_ICMD("fail to get all user vars", K(ret));
  } else if (OB_FAIL(dump_cs_variables_common(user_vars, OB_FIELD_USER_VAR))) {
    WDIAG_ICMD("fail to dump cs all user variables", K(ret));
  } else {
    DEBUG_ICMD("succ to dump cs all user variables", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_variables_item(const ObSessionBaseField *field,
    const ObSessionVariableType var_type)
{
  int ret = OB_SUCCESS;
  char buf[PROXY_LIKE_NAME_MAX_SIZE] = {0};
  ObString warn_string("---value length is large then 128bytes---");
  int64_t pos = 0;
  if (OB_FIELD_USER_VAR == var_type) {
    // user field has store ' into value, no need to print ' again
    if (OB_FAIL(field->value_.print_plain_str_literal(buf, sizeof(buf), pos))) {
      WDIAG_ICMD("fail to print sql literal", K(ret));
    }
  } else {
    // sys field will use print_sql to add '
    if (OB_FAIL(field->value_.print_sql_literal(buf, sizeof(buf), pos))) {
      WDIAG_ICMD("fail to print sql literal", K(ret));
    }
  }

  if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
    MEMCPY(buf, warn_string.ptr(), warn_string.length());
    pos = warn_string.length();
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    ObNewRow row;
    ObObj cells[OB_SVC_MAX_VARIABLES_COLUMN_ID];
    cells[OB_SVC_NAME].set_varchar(field->name_, static_cast<int32_t>(field->name_len_));
    cells[OB_SVC_VALUE].set_varchar(buf, static_cast<int32_t>(pos));
    cells[OB_SVC_INFO].set_varchar(get_var_info_str(var_type));
    cells[OB_SVC_TYPE].set_varchar(get_var_modify_mod_str(field->modify_mod_));

    if (OB_FIELD_USER_VAR == var_type) {
      cells[OB_SVC_FLAG].set_varchar("");
    } else {
      const int32_t MAX_VAR_SCOPE_LENGTH = 80;
      char scope_array[MAX_VAR_SCOPE_LENGTH] = {'\0'};
      ObString scope_string(MAX_VAR_SCOPE_LENGTH, 0, scope_array);
      if (OB_FAIL(get_var_scope_str(reinterpret_cast<const ObSessionSysField *>(field), scope_string))) {
        WDIAG_ICMD("fail to get_var_scope_str", K(ret));
      } else {
        cells[OB_SVC_FLAG].set_varchar(scope_string);
      }
    }
    if (OB_SUCC(ret)) {
      row.cells_ = cells;
      row.count_ = OB_SVC_MAX_VARIABLES_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_stat(const ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < SESSION_STAT_COUNT; ++i) {
    if (common::match_like(g_mysql_stat_name[i], like_name_)) {
      DEBUG_ICMD("session stat name matched", K_(like_name), K(g_mysql_stat_name[i]));
      if (OB_FAIL(dump_cs_stat_item(g_mysql_stat_name[i], cs.get_session_stats().stats_[i]))) {
        WDIAG_ICMD("fail to dump cs stat item", K(ret));
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_stat_item(const char *name, const int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("name is null", K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_SSC_MAX_STAT_COLUMN_ID];
    cells[OB_SSC_NAME].set_varchar(name);
    cells[OB_SSC_VALUE].set_int(value);
    row.cells_ = cells;
    row.count_ = OB_SSC_MAX_STAT_COLUMN_ID;

    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_attribute(const ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObSqlString cs_info;
  char host_ip_buf[INET6_ADDRSTRLEN];
  uint16_t host_port = 0;
  ObNetVConnection *net_vc = cs.get_netvc();
  if (NULL != net_vc) {
    ops_ip_ntop(net_vc->get_remote_addr(), host_ip_buf, sizeof(host_ip_buf));
    host_port = net_vc->get_remote_port();
  } else {
    host_ip_buf[0] = '\0';
    host_port = 0;
  }

  ObClientSessionInfo &client_info = const_cast<ObClientSessionInfo &>(cs.get_session_info());
  const ObMysqlClientSession::ObSessionStats &session_stat = cs.get_session_stats();
  const ObProxySessionPrivInfo &priv_info = cs.get_session_info().get_priv_info();
  const ObString &user_name = priv_info.user_name_;
  const ObString &tenant_name = priv_info.tenant_name_;
  const ObString &cluster_name = priv_info.cluster_name_;
  ObString db_name;
  cs.get_session_info().get_database_name(db_name); // ignore ret, db_name maybe NULL
  if (db_name.empty()) {
    db_name = ObString::make_string("NULL");
  }
  ObString idc_name;
  char idc_name_buf[common::MAX_PROXY_IDC_LENGTH];
  if (cs.get_session_info().is_user_idc_name_set()) {
    if (cs.get_session_info().get_idc_name().empty()) {
      //do nothing
    } else {
      idc_name = cs.get_session_info().get_idc_name();
    }
  } else {
    obsys::CRLockGuard guard(get_global_proxy_config().rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(get_global_proxy_config().proxy_idc_name.str()));
    if (len < 0 || len > common::MAX_PROXY_IDC_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("proxy_idc_name's length is over size", K(len),
               "proxy_idc_name", get_global_proxy_config().proxy_idc_name.str(), K(ret));
    } else {
      memcpy(idc_name_buf, get_global_proxy_config().proxy_idc_name.str(), len);
      idc_name.assign(idc_name_buf, static_cast<int32_t>(len));
    }
  }
  const char *cs_common_info    = "cs common";
  const char *cs_stat_info      = "cs stat";
  const char *cs_var_info       = "cs var version";

  //dump common cs info
  if (OB_FAIL(dump_cs_attribute_item("proxy_sessid", static_cast<int64_t>(cs.get_proxy_sessid()), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("cs_id", static_cast<int64_t>(cs.get_cs_id()), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("cluster", cluster_name, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("tenant", tenant_name, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("user", user_name, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("host_ip", host_ip_buf, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("host_port", static_cast<int64_t>(host_port), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("db", db_name, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("total_trans_cnt", cs.get_transact_count(), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("svr_session_cnt", cs.get_svr_session_count(), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("active", (cs.active_ ? "true" : "false"), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("read_state", cs.get_read_state_str(), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("tid", cs.get_current_tid(), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("pid", static_cast<int64_t>(getpid()), cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else if (OB_FAIL(dump_cs_attribute_item("idc_name", idc_name, cs_common_info))) {
    WDIAG_ICMD("fail to dump attribute item", K(ret));
  } else { }

  //dump session stat
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dump_cs_attribute_item("modified_time", session_stat.modified_time_, cs_stat_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else if (OB_FAIL(dump_cs_attribute_item("reported_time", session_stat.reported_time_, cs_stat_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else {}
  }

  //dump session variables version
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dump_cs_attribute_item("hot_sys_var_version", client_info.get_hot_sys_var_version(), cs_var_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else if (OB_FAIL(dump_cs_attribute_item("sys_var_version", client_info.get_sys_var_version(), cs_var_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else if (OB_FAIL(dump_cs_attribute_item("user_var_version", client_info.get_user_var_version(), cs_var_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else if (OB_FAIL(dump_cs_attribute_item("last_insert_id_version", client_info.get_last_insert_id_version(), cs_var_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else if (OB_FAIL(dump_cs_attribute_item("db_name_version", client_info.get_db_name_version(), cs_var_info))) {
      WDIAG_ICMD("fail to dump attribute item", K(ret));
    } else {}
  }

  if (OB_SUCC(ret)) {
    bool lii_ss_found = false;
    int64_t lii_ss_id = -1;
    if (NULL != cs.get_lii_server_session()) {
      lii_ss_id = cs.get_lii_server_session()->ss_id_;
    } else {
      lii_ss_found = true;
    }

    const ObMysqlServerSession *svr_session = NULL;
    ObServerSessionType ss_type = OB_SST_INVAILED;
    //dump last used server session
    if (NULL != (svr_session = cs.get_server_session())) {
      if (!lii_ss_found && lii_ss_id == svr_session->ss_id_) {
        lii_ss_found = true;
        ss_type = OB_SST_LAST_USED_AND_LII;
      } else {
        ss_type = OB_SST_LAST_USED;
      }
      if (OB_FAIL(dump_cs_attribute_ss(*svr_session, ss_type))) {
        WDIAG_ICMD("fail to dump attribute item svr session", K(ss_type), K(ret));
      }
    }

    //dump current used server session
    if (OB_SUCC(ret) && NULL != (svr_session = cs.get_cur_server_session())) {
      if (!lii_ss_found && lii_ss_id == svr_session->ss_id_) {
        lii_ss_found = true;
        ss_type = OB_SST_CURRENT_AND_LII;
      } else {
        ss_type = OB_SST_CURRENT;
      }
      if (OB_FAIL(dump_cs_attribute_ss(*svr_session, ss_type))) {
        WDIAG_ICMD("fail to dump attribute item svr session", K(ss_type), K(ret));
      }
    }

    //dump server session pool
    if (OB_SUCC(ret)) {
      ObServerSessionPool::IPHashTable &ip_pool = const_cast<ObMysqlSessionManager &>(cs.get_session_manager()).get_session_pool().ip_pool_;
      ObServerSessionPool::IPHashTable::iterator spot = ip_pool.begin();
      ObServerSessionPool::IPHashTable::iterator last = ip_pool.end();
      for (int64_t i = 0; OB_SUCC(ret) && spot != last; ++spot, ++i) {
        if (!lii_ss_found && lii_ss_id == spot->ss_id_) {
          lii_ss_found = true;
          ss_type = OB_SST_SS_POOL_AND_LII;
        } else {
          ss_type = OB_SST_SS_POOL;
        }
        if (OB_FAIL(dump_cs_attribute_ss(*spot, ss_type, i))) {
          WDIAG_ICMD("fail to dump attribute item svr session", K(ss_type), K(i), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_attribute_item(const char *name, const int64_t value, const char *info)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_INT64_SIZE = 24;
  char int64_buffer[MAX_INT64_SIZE];
  int64_t length = snprintf(int64_buffer, sizeof(int64_buffer), "%ld", value);

  if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int64_t>(sizeof(int64_buffer)))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WDIAG("buf not enought", K(length), "int64_buffer length", sizeof(int64_buffer), K(value), K(ret));
  } else{
    ret = dump_cs_attribute_item(name, ObString::make_string(int64_buffer), info);
  }
  return ret;
}

inline int ObShowSessionHandler::dump_cs_attribute_item(const char *name, const char *value, const char *info)
{
  return dump_cs_attribute_item(name, ObString::make_string(value), info);
}

int ObShowSessionHandler::dump_cs_attribute_item(const char *name, const ObString &value, const char *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name) || OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("argument is null", K(name), K(info), K(ret));
  } else if (!common::match_like(name, like_name_)) {
    DEBUG_ICMD("no need dump it", K(like_name_), K(name), K(value), K(info));
  } else {
    ObNewRow row;
    ObObj cells[OB_SAC_MAX_ATTRIBUTE_COLUMN_ID];
    cells[OB_SAC_NAME].set_varchar(name);
    cells[OB_SAC_VALUE].set_varchar(value);
    cells[OB_SAC_INFO].set_varchar(info);
    row.cells_ = cells;
    row.count_ = OB_SAC_MAX_ATTRIBUTE_COLUMN_ID;

    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(name), K(value), K(info),K(ret));
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_attribute_ss(const ObMysqlServerSession &svr_session,
    const ObServerSessionType type, const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type <= OB_SST_INVAILED) || OB_UNLIKELY(type >= OB_SST_MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("argument is null", K(type), K(ret));
  } else {
    const int64_t MAX_SS_INFO_LENGTH = 32;
    char info_buf[MAX_SS_INFO_LENGTH] = {'\0'};
    static const char *ss_info[OB_SST_MAX_TYPE] = {
        "",
        "last used ss",
        "last used && lii ss",
        "curr used ss",
        "curr used && lii ss",
        "ss pool",
        "ss pool && lii ss",
    };

    const char *info = NULL;
    if (-1 != idx) {
      int32_t length = snprintf(info_buf, sizeof(info_buf), "%s [%ld]", ss_info[type], idx);
      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int32_t>(sizeof(info_buf)))) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("info_buf is not enough", K(length), "info_buf length", sizeof(info_buf),
                  K(info_buf), K(ret));
      } else {
        info = info_buf;
      }
    } else {
      info = ss_info[type];
    }

    if (OB_SUCC(ret)) {
      const ObServerSessionInfo &session_info = svr_session.get_session_info();
      char buf[INET6_ADDRSTRLEN];
      ops_ip_ntop(svr_session.server_ip_, buf, sizeof(buf));

      if (OB_FAIL(dump_cs_attribute_item("server_ip", buf, info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("server_port", static_cast<int64_t>((ntohs)(svr_session.server_ip_.port())), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("server_sessid", svr_session.server_sessid_, info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("ss_id", svr_session.ss_id_, info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("state", svr_session.get_state_str(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("transact_count", static_cast<int64_t>(svr_session.transact_count_), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("server_trans_stat", static_cast<int64_t>(svr_session.server_trans_stat_), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("hot_sys_var_version", session_info.get_hot_sys_var_version(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("sys_var_version", session_info.get_sys_var_version(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("user_var_version", session_info.get_user_var_version(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("last_insert_id_version", session_info.get_last_insert_id_version(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("db_name_version", session_info.get_db_name_version(), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("is_checksum_supported", static_cast<int64_t>(session_info.is_checksum_supported()), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("is_safe_read_weak_supported", static_cast<int64_t>(session_info.is_safe_read_weak_supported()), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("is_checksum_switch_supported", static_cast<int64_t>(session_info.is_checksum_switch_supported()), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("checksum_switch", static_cast<int64_t>(session_info.get_checksum_switch()), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      } else if (OB_FAIL(dump_cs_attribute_item("enable_extra_ok_packet_for_stats", static_cast<int64_t>(session_info.is_extra_ok_packet_for_stats_enabled()), info))) {
        WDIAG_ICMD("fail to dump attribute item", K(info), K(ret));
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_list(const ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObSqlString ip_port;
  char host_ip_buf[INET6_ADDRSTRLEN];
  uint16_t host_port = 0;
  ObNetVConnection *net_vc = cs.get_netvc();
  if (NULL != net_vc) {
    ops_ip_ntop(net_vc->get_remote_addr(), host_ip_buf, sizeof(host_ip_buf));
    host_port = net_vc->get_remote_port();
  } else {
    host_ip_buf[0] = '\0';
    host_port = 0;
  }
  const ObProxySessionPrivInfo &other_priv_info = cs.get_session_info().get_priv_info();
  const ObString &user_name = other_priv_info.user_name_;
  const ObString &tenant_name = other_priv_info.tenant_name_;
  const ObString &cluster_name = other_priv_info.cluster_name_;
  ObString db_name;
  cs.get_session_info().get_database_name(db_name); // ignore ret, db_name maybe NULL
  if (db_name.empty()) {
    db_name = ObString::make_string("NULL");
  }

  if (OB_FAIL(ip_port.append_fmt("%s:%d", host_ip_buf, static_cast<int32_t>(host_port)))) {
    WDIAG_ICMD("fail to append producer info", K(ret));
  } else if (OBPROXY_T_SUB_SESSION_LIST == sub_type_) {
    //the follow user can see this session:
    //1. has_all_privilege_, such as root@proxysys
    //2. SYS tenant user
    //3. same tenant && same user
    //4. same tenant && PROCESS privilege
    //5. same user for sharding
    if (session_priv_.has_all_privilege_
        || session_priv_.tenant_name_ == OB_SYS_TENANT_NAME
        || ((!cs.get_session_info().is_sharding_user() && session_priv_.is_same_tenant(other_priv_info))
            && (session_priv_.is_same_user(other_priv_info) || session_priv_.has_process_privilege()))
        || (cs.get_session_info().is_sharding_user() && session_priv_.is_same_logic_user(other_priv_info))) {
      ObNewRow row;
      ObObj cells[OB_SLC_MAX_SLIST_COLUMN_ID];
      cells[OB_SLC_ID].set_uint32(cs.get_cs_id());
      cells[OB_SLC_TENANT].set_varchar(tenant_name);
      cells[OB_SLC_USER].set_varchar(user_name);
      cells[OB_SLC_HOST].set_varchar(ip_port.string());
      cells[OB_SLC_DB].set_varchar(db_name);
      cells[OB_SLC_TRANS_COUNT].set_int(cs.get_transact_count());
      cells[OB_SLC_SS_COUNT].set_int(cs.get_svr_session_count());
      cells[OB_SLC_STATE].set_varchar(cs.get_read_state_str());
      cells[OB_SLC_TID].set_int(cs.get_current_tid());
      cells[OB_SLC_PID].set_mediumint(getpid());
      row.cells_ = cells;
      row.count_ = OB_SLC_MAX_SLIST_COLUMN_ID;
      const bool need_limit_size = false;//show processlist no need limit size
      if (OB_FAIL(encode_row_packet(row, need_limit_size))) {
        WDIAG_ICMD("fail to encode row packet", K_(sub_type), K(row), K(ret));
      }
    } else {
      DEBUG_ICMD("not the same user, no need to dump cs item", K(cluster_name), K(tenant_name),
                K(user_name), K(session_priv_));
    }
  } else {
    ObNewRow row;
    ObObj cells[OB_SILC_MAX_SLIST_COLUMN_ID];
    cells[OB_SILC_PROXY_SESSID].set_uint64(cs.get_proxy_sessid());
    cells[OB_SILC_ID].set_uint32(cs.get_cs_id());
    cells[OB_SILC_CLUSTER].set_varchar(cluster_name);
    cells[OB_SILC_TENANT].set_varchar(tenant_name);
    cells[OB_SILC_USER].set_varchar(user_name);
    cells[OB_SILC_HOST].set_varchar(ip_port.string());
    cells[OB_SILC_DB].set_varchar(db_name);
    cells[OB_SILC_TRANS_COUNT].set_int(cs.get_transact_count());
    cells[OB_SILC_SS_COUNT].set_int(cs.get_svr_session_count());
    cells[OB_SILC_STATE].set_varchar(cs.get_read_state_str());
    cells[OB_SILC_TID].set_int(cs.get_current_tid());
    cells[OB_SILC_PID].set_mediumint(getpid());
    cells[OB_SILC_USING_SSL].set_int(static_cast<ObUnixNetVConnection*>(cs.get_netvc())->using_ssl());
    row.cells_ = cells;
    row.count_ = OB_SILC_MAX_SLIST_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K_(sub_type), K(row), K(ret));
    }
  }
  return ret;
}

int ObShowSessionHandler::handle_cs_details(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  bool need_callback = true;
  bool is_proxy_conn_id = true;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    if (-1 == cs_id_) {
      //-1 means us currnet cs_id
      cs_id_ = session_priv_.cs_id_;
    }

    if (!is_conn_id_avail(cs_id_, is_proxy_conn_id)) {
      int errcode = OB_UNKNOWN_CONNECTION; //not found the specific session
      WDIAG_ICMD("cs_id is not avail", K(cs_id_), K(errcode));
      if (OB_FAIL(encode_err_packet(errcode, cs_id_))) {
        WDIAG_ICMD("fail to encode err resp packet", K(errcode), K_(cs_id), K(ret));
      }
    } else {
      if (is_proxy_conn_id) {
        //connection id got from obproxy
        int64_t thread_id = -1;
        if (OB_FAIL(extract_thread_id(static_cast<uint32_t>(cs_id_), thread_id))) {
          WDIAG_ICMD("fail to extract thread id, it should not happen", K(cs_id_), K(ret));
        } else if (thread_id == ethread->id_) {
          need_callback = false;
          event_ret = handle_cs_with_proxy_conn_id(EVENT_NONE, data);
        } else {
          SET_HANDLER(&ObInternalCmdHandler::handle_cs_with_proxy_conn_id);
          if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][thread_id]->schedule_imm(this))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            EDIAG_ICMD("fail to schedule self", K(thread_id), K(ret));
          } else {
            need_callback = false;
          }
        }
      } else {
        //connection id got from observer
        SET_HANDLER(&ObInternalCmdHandler::handle_cs_with_server_conn_id);
        if (OB_ISNULL(ethread->schedule_imm(this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          need_callback = false;
        }
      }
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

int ObShowSessionHandler::show_cs_list_in_thread(const ObEThread &ethread)
{
  int ret = OB_SUCCESS;
  ObMysqlClientSessionMap::IDHashMap &id_map = get_client_session_map(ethread).id_map_;
  ObMysqlClientSessionMap::IDHashMap::iterator spot = id_map.begin();
  ObMysqlClientSessionMap::IDHashMap::iterator end = id_map.end();
  for (;OB_SUCC(ret) && spot != end; ++spot) {
    // here we only read cs, no need try lock it
    if (enable_show_session(*spot)) {
      if (OB_FAIL(dump_cs_list(*spot))) {
        WDIAG_ICMD("fail to dump client session", K(spot->get_cs_id()));
      } else {
        DEBUG_ICMD("succ to dump client_session", K(spot->get_cs_id()));
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::show_cs_list(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool is_finished = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("cur ethread is null, it should not happened", K(ret));
  } else if (OB_FAIL(show_cs_list_in_thread(*ethread))) {
    WDIAG_ICMD("fail to do show_cs_list_in_thread", K(ret));
  } else {
    const int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (OB_LIKELY(NULL != submit_thread_) && next_id != submit_thread_->id_) {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule self", K(next_id), K(ret));
      } else {
        is_finished = false;
      }
    } else {
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      }
    }
  }

  if (is_finished) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

int ObShowSessionHandler::handle_cs_list(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  int errcode = OB_SUCCESS;
  bool need_callback = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else {
    if (need_privilege_check() && OB_FAIL(do_privilege_check(session_priv_, need_priv_, errcode))) {
      WDIAG_ICMD("fail to do privilege check", K(errcode), K(ret));
    }
    if (OB_SUCCESS != errcode) {
      WDIAG_ICMD("fail to check user privilege, try to response err packet", K(errcode), K(ret));
    } else if (OB_FAIL(dump_cs_list_header())) {
      WDIAG_ICMD("fail to dump list header, try to do internal_error_callback", K(ret));
    } else if (OB_ISNULL(ethread = this_ethread())) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("cur ethread is null, it should not happened", K(ret));
    } else {
      SET_HANDLER(&ObShowSessionHandler::show_cs_list);
      if (OB_ISNULL(ethread->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule self", K(ret));
      } else {
        need_callback = false;
      }
    }
  }

  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

ObString ObShowSessionHandler::get_var_info_str(ObSessionVariableType type) const
{
  ObString ret;
  switch (type) {
    case OB_FIELD_CHANGED_SYS_VAR: {
      ret = ObString::make_string("changed sys var");
      break;
    }
    case OB_FIELD_SYS_VAR: {
      ret = ObString::make_string("sys var");
      break;
    }
    case OB_FIELD_USER_VAR: {
      ret = ObString::make_string("user var");
      break;
    }
    default: {
      ret = ObString::make_empty_string();
      break;
    }
  }
  return ret;
}

ObString ObShowSessionHandler::get_var_modify_mod_str(ObSessionFieldModifyMod modify_mod) const
{
  ObString ret;
  switch (modify_mod) {
    case OB_FIELD_COLD_MODIFY_MOD: {
      ret = ObString::make_string("cold modified vars");
      break;
    }
    case OB_FIELD_HOT_MODIFY_MOD: {
      ret = ObString::make_string("hot modified vars");
      break;
    }
    case OB_FIELD_LAST_INSERT_ID_MODIFY_MOD: {
      ret = ObString::make_string("last insert id modified vars");
      break;
    }
    default: {
      ret = ObString::make_empty_string();
      break;
    }
  }
  return ret;
}

inline int ObShowSessionHandler::fill_scope_string(const char *string, const int32_t string_len,
    ObString &scope_string) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(string) || OB_UNLIKELY(string_len <= 0) || OB_UNLIKELY(scope_string.remain() < string_len)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument", K(string), K(string_len), "remain", scope_string.remain(), K(ret));
  } else if (OB_UNLIKELY(string_len != scope_string.write(string, string_len))) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("fail to write scopes string into buf", K(string), K(string_len), K(scope_string), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObShowSessionHandler::get_var_scope_str(const ObSessionSysField *field,
    ObString &scope_string) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(field) || OB_UNLIKELY(scope_string.remain() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument", K(field), "remain", scope_string.remain(), K(ret));
  } else {
    if (field->is_invisible()) {
      const char string[] = {" && invisible"};
      const int64_t string_len = static_cast<int64_t>(sizeof(string) - 1);
      if (OB_FAIL(fill_scope_string(string, string_len, scope_string))) {
        WDIAG_ICMD("fail to fill scope string", K(string), K(string_len), K(ret));
      }
    }
    if (OB_SUCC(ret) && field->is_global_scope()) {
      const char string[] = {" && global_scope"};
      const int64_t string_len = static_cast<int64_t>(sizeof(string) - 1);
      if (OB_FAIL(fill_scope_string(string, string_len, scope_string))) {
        WDIAG_ICMD("fail to fill scope string", K(string), K(string_len), K(ret));
      }
    }

    if (OB_SUCC(ret) && field->is_session_scope()) {
      const char string[] = {" && session_scope"};
      const int64_t string_len = static_cast<int64_t>(sizeof(string) - 1);
      if (OB_FAIL(fill_scope_string(string, string_len, scope_string))) {
        WDIAG_ICMD("fail to fill scope string", K(string), K(string_len), K(ret));
      }
    }

    if (OB_SUCC(ret) && field->is_readonly()) {
      const char string[] = {" && readonly"};
      const int64_t string_len = static_cast<int64_t>(sizeof(string) - 1);
      if (OB_FAIL(fill_scope_string(string, string_len, scope_string))) {
        WDIAG_ICMD("fail to fill scope string", K(string), K(string_len), K(ret));
      }
    }

    if (OB_SUCC(ret) && field->is_nullable()) {
      const char string[] = {" && nullable"};
      const int64_t string_len = static_cast<int64_t>(sizeof(string) - 1);
      if (OB_FAIL(fill_scope_string(string, string_len, scope_string))) {
        WDIAG_ICMD("fail to fill scope string", K(string), K(string_len), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      scope_string.reset();
    }
  }
  return ret;
}

static int show_session_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObEThread *ethread = NULL;
  ObShowSessionHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowSessionHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowSessionHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowSessionHandler");
  } else if (OB_FAIL(handler->session_priv_.deep_copy(info.session_priv_))) {
    WDIAG_ICMD("fail to deep copy session priv");
  } else if (OB_FAIL(ObProxyPrivilegeCheck::get_need_priv(sql::stmt::T_SHOW_PROCESSLIST,
      handler->session_priv_, handler->need_priv_))) {
    WDIAG_ICMD("fail to get need priv");
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    DEBUG_ICMD("schedule encode packet continuation");
    if (OBPROXY_T_SUB_SESSION_LIST == info.get_sub_cmd_type()
        || OBPROXY_T_SUB_SESSION_LIST_INTERNAL == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowSessionHandler::handle_cs_list);
    } else {
      SET_CONTINUATION_HANDLER(handler, &ObShowSessionHandler::handle_cs_details);
    }

    if (OB_SUCC(ret)) {
      action = &handler->get_action();
      if (OB_ISNULL(ethread->schedule_imm(handler, ET_NET))) {// use work thread
        ret = OB_ALLOCATE_MEMORY_FAILED;
        EDIAG_ICMD("fail to schedule ObShowSessionHandler", K(ret));
        action = NULL;
      } else {
        DEBUG_ICMD("succ to schedule ObShowSessionHandler", K(handler->session_priv_));
      }
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_session_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_PROCESSLIST,
                                                               &show_session_cmd_callback))) {
    WDIAG_ICMD("fail to OBPROXY_T_ICMD_SHOW_PROCESSLIST", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_SESSION,
                                                               &show_session_cmd_callback))) {
    WDIAG_ICMD("fail to OBPROXY_T_ICMD_SHOW_SESSION", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_read_stale_replica_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(READ_STALE_ARRAY, OB_SWRS_MAX_STALE_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_read_stale_replica(ObMysqlClientSession &cs)
{
  int ret = OB_SUCCESS;
  ObReadStaleProcessor &processor = get_global_read_stale_processor(); 
  DRWLock::RDLockGuard guard(processor.get_lock());
  ObVipReadStaleInfo *vip_read_stale_info = NULL;

  const ObProxySessionPrivInfo &priv_info = cs.get_session_info().get_priv_info();
  const ObString &tenant_name = priv_info.tenant_name_;
  const ObString &cluster_name = priv_info.cluster_name_;
  ObVipAddr vip_addr = cs.get_ct_info().vip_tenant_.vip_addr_;

  if (OB_FAIL(processor.acquire_vip_feedback_record(vip_addr, tenant_name, cluster_name, vip_read_stale_info))) {
    WDIAG_ICMD("fail to acquire vip read stale feedback map", K(ret));
  } 
  if (OB_SUCC(ret) && vip_read_stale_info != NULL) {
    DRWLock::RDLockGuard vip_guard(vip_read_stale_info->get_lock());
    ObVipReadStaleInfo::ObReadStaleFeedbackMap ::iterator it = vip_read_stale_info->read_stale_feedback_map_.begin();
    ObVipReadStaleInfo::ObReadStaleFeedbackMap::iterator end = vip_read_stale_info->read_stale_feedback_map_.end();
    for(; OB_SUCC(ret) && it != end; ++it) {
      ObReadStaleFeedback *feedback = it.value_;
      if (OB_FAIL(dump_cs_read_stale_replica_item(feedback))) {
        WDIAG_ICMD("fail to dump weak read stale replica item",K(feedback), K(ret));
      }
    }
  }
  return ret;
}

int ObShowSessionHandler::dump_cs_read_stale_replica_item(const ObReadStaleFeedback *feedback)
{
  int ret = OB_SUCCESS;
  ObObj cells[OB_SWRS_MAX_STALE_COLUMN_ID];
  ObNewRow row;
  char time_buf[OB_MAX_TIMESTAMP_LENGTH];
  int64_t pos = 0;
  ip_port_text_buffer server_ip;

  if (OB_FAIL(ops_ip_nptop(feedback->replica_.server_addr_, server_ip, sizeof(server_ip)))) {
    WDIAG_ICMD("fail to convert server addr", K(feedback->replica_.server_addr_), K(ret));
  } else if (OB_FAIL(ObTimeUtility::usec_to_str(feedback->feedback_time_, time_buf, OB_MAX_TIMESTAMP_LENGTH, pos))) {
    WDIAG_ICMD("fail to convert feedback time", K(feedback->replica_.server_addr_), K(ret));
  } else {
    cells[OB_SWRS_ADDR].set_varchar(server_ip);
    cells[OB_SWRS_TABLE_ID].set_int(feedback->replica_.table_id_);
    cells[OB_SWRS_PARTITION_ID].set_int(feedback->replica_.partition_id_);
    cells[OB_SWRS_FEEDBACK_TIME].set_varchar(time_buf);

    row.cells_ = cells;
    row.count_ = OB_SWRS_MAX_STALE_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}



} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

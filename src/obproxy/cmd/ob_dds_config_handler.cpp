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

#include "cmd/ob_dds_config_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_config_utils.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "lib/hash/ob_hashmap.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "obutils/ob_proxy_config_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
enum
{
  OB_VAR_NAME = 0,
  OB_VAR_VALUE,
  OB_CC_MAX_VAR_COLUMN_ID,
};
static const char* APP_NAME_COL    = "APP_NAME";
static const char* APP_VERSION_COL = "VERSION";
static const char* CONFIG_VAL      = "CONFIG_VAL";

const ObProxyColumnSchema SESSION_VAR_COLUMN_ARRAY[OB_CC_MAX_VAR_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_VAR_NAME, "Variable_name", OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_VAR_VALUE,    "Value",    OB_MYSQL_TYPE_VARCHAR),
};

typedef ObHashMap<common::ObString, common::ObString> SessionHash;

// used for map init
class SessionMap {
public:
  SessionMap() {
    str_session_map_.create(32, ObModIds::OB_HASH_BUCKET);
    str_session_map_.set_refactored(ObString::make_string("interactive_timeout"), ObString::make_string("28800"));
    str_session_map_.set_refactored(ObString::make_string("lower_case_table_names"), ObString::make_string("2"));
    str_session_map_.set_refactored(ObString::make_string("max_allowed_packet"), ObString::make_string("4194304"));
    str_session_map_.set_refactored(ObString::make_string("net_buffer_length"), ObString::make_string("16384"));
    str_session_map_.set_refactored(ObString::make_string("net_write_timeout"), ObString::make_string("60"));
    str_session_map_.set_refactored(ObString::make_string("query_cache_size"), ObString::make_string("1048576"));
    str_session_map_.set_refactored(ObString::make_string("wait_timeout"), ObString::make_string("28800"));
    str_session_map_.set_refactored(ObString::make_string("character_set_client"), ObString::make_string("utf8mb4"));
    str_session_map_.set_refactored(ObString::make_string("character_set_connection"), ObString::make_string("utf8mb4"));
    str_session_map_.set_refactored(ObString::make_string("character_set_results"), ObString::make_string("utf8mb4"));
    str_session_map_.set_refactored(ObString::make_string("character_set_server"), ObString::make_string("utf8mb4"));
    str_session_map_.set_refactored(ObString::make_string("init_connect"), ObString::make_string(""));
    str_session_map_.set_refactored(ObString::make_string("query_cache_type"), ObString::make_string("OFF"));
    str_session_map_.set_refactored(ObString::make_string("sql_mode"), ObString::make_string("STRICT_ALL_TABLES"));
    str_session_map_.set_refactored(ObString::make_string("system_time_zone"), ObString::make_string("+08:00"));
    str_session_map_.set_refactored(ObString::make_string("time_zone"), ObString::make_string("+08:00"));
    str_session_map_.set_refactored(ObString::make_string("transaction_isolation"), ObString::make_string("READ"));
    str_session_map_.set_refactored(ObString::make_string("tx_isolation"), ObString::make_string("READ-COMMITTED"));
  }
public:
  SessionHash str_session_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(SessionMap);
};
static SessionMap session_map;

ObDdsConfigHandler::ObDdsConfigHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), cmd_type_(info.get_cmd_type()),
    capability_(info.get_capability())
{
  sm_ = reinterpret_cast<ObMysqlSM *>(cont);
  SET_HANDLER(&ObDdsConfigHandler::main_handler);
}

int ObDdsConfigHandler::main_handler(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);
  if (OB_UNLIKELY(action_.cancelled_)) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("action canceled", K(ret));
    event_ret = internal_error_callback(ret);
  } else {
    switch (cmd_type_) {
    case OBPROXY_T_SHOW:
      event_ret = handle_show_variables();
      break;
    case OBPROXY_T_SET:
    case OBPROXY_T_SET_NAMES:
      event_ret = handle_set_variables();
      break;
    case OBPROXY_T_SELECT:
      event_ret = handle_select_variables();
      break;
    case OBPROXY_T_UPDATE:
      event_ret = handle_update_variables();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("unknown type", "cmd_type", get_print_stmt_name(cmd_type_));
      event_ret = internal_error_callback(ret);
    }
  }
  return event_ret;
}
int ObDdsConfigHandler::handle_show_variables()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObString sql = sm_->trans_state_.trans_info_.client_request_.get_sql();
  DEBUG_ICMD("handle_show_variables", K(sql));
  if (OB_FAIL(encode_header(SESSION_VAR_COLUMN_ARRAY, OB_CC_MAX_VAR_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  SessionHash& str_session_map = session_map.str_session_map_;
  for (SessionHash::iterator it = str_session_map.begin();
       OB_SUCC(ret) && it != str_session_map.end(); ++it) {
    ObNewRow row;
    ObObj cells[OB_CC_MAX_VAR_COLUMN_ID];
    cells[OB_VAR_NAME].set_varchar(it->first);
    cells[OB_VAR_VALUE].set_varchar(it->second);
    row.cells_ = cells;
    row.count_ = OB_CC_MAX_VAR_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode_eof_packet", K(ret));
    } else {
      DEBUG_ICMD("succ to encode_eof_packet");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObDdsConfigHandler::handle_set_variables()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  // here we direct send ok packet for simple
  ObString sql = sm_->trans_state_.trans_info_.client_request_.get_sql();
  DEBUG_ICMD("handle_set_variables", K(sql));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_ok_packet(0, capability_))) {
      WDIAG_ICMD("fail to encode_ok_packet", K(ret));
    } else {
      DEBUG_ICMD("succ to encode_ok_packet");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObDdsConfigHandler::handle_select_variables()
{
  int event_ret = EVENT_DONE;
  obutils::ObSqlParseResult& parse_result = sm_->trans_state_.trans_info_.client_request_.get_parse_result();
  if (parse_result.get_table_name().empty()) {
    //treat empty table name select session_var
    event_ret = handle_select_session_variables();
  } else {
    event_ret = handle_select_sql_variables();
  }
  return event_ret;
}

int ObDdsConfigHandler::handle_select_session_variables()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObString sql = sm_->trans_state_.trans_info_.client_request_.get_sql();
  char* pos = strcasestr(sql.ptr(), "SELECT");
  ObString select_key;
  if (pos != NULL) {
    ObString tmp_str(sql.length() - (pos + 6 - sql.ptr()), pos + 6);
    select_key = tmp_str.trim();
  }
  DEBUG_ICMD("handle_select_session_variables", K(sql), K(select_key));
  if (select_key.case_compare("@@session.auto_increment_increment") == 0) {
    if (OB_FAIL(handle_one_select_session_variable("@@session.auto_increment_increment", "1"))) {
      WDIAG_ICMD("handle_one_select_session_variable failed", K(select_key));
    }
  } else if (select_key.case_compare("@@session.tx_read_only") == 0) {
    if (OB_FAIL(handle_one_select_session_variable("@@session.tx_read_only", "0"))) {
      WDIAG_ICMD("handle_one_select_session_variable failed", K(select_key));
    }
  } else if (select_key.case_compare("@@session.tx_isolation") == 0) {
    if (OB_FAIL(handle_one_select_session_variable("@@session.tx_isolation", "READ-COMMITTED"))) {
      WDIAG_ICMD("handle_one_select_session_variable failed", K(select_key));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode_eof_packet", K(ret));
    } else {
      INFO_ICMD("succ to encode_eof_packet");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}
int ObDdsConfigHandler::handle_one_select_session_variable(const char* key, const char* val)
{
  int ret = OB_SUCCESS;
  const ObProxyColumnSchema COLUMN_ARRAY[1] = {
    ObProxyColumnSchema::make_schema(0, key, OB_MYSQL_TYPE_VARCHAR),
  };
  if (OB_FAIL(encode_header(COLUMN_ARRAY, 1))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  } else {
    ObNewRow row;
    ObObj cells[1];
    cells[0].set_varchar(val);
    row.cells_ = cells;
    row.count_ = 1;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}
int ObDdsConfigHandler::dump_select_result(const common::ObString& app_name, const common::ObString& app_block)
{
  int ret = OB_SUCCESS;
  common::ObSqlString config_val;
  ObProxyConfigType config_type = get_config_type_by_str(app_block);
  if (OB_FAIL(get_global_proxy_config_processor().get_app_config_string(app_name, config_type, config_val))) {
    WDIAG_ICMD("fail to get_app_config_string", K(ret));
  } else {
    const ObProxyColumnSchema COLUMN_ARRAY[1] = {
      ObProxyColumnSchema::make_schema(0, CONFIG_VAL, OB_MYSQL_TYPE_VARCHAR),
    };
    if (OB_FAIL(encode_header(COLUMN_ARRAY, 1))) {
      WDIAG_ICMD("fail to encode header", K(ret));
    } else {
      ObNewRow row;
      ObObj cells[1];
      cells[0].set_varchar(config_val.string());
      row.cells_ = cells;
      row.count_ = 1;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      } else if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      }
    }
  }
  return ret;
}

int ObDdsConfigHandler::handle_select_sql_variables()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  // sql ect: select  CONFIG_VAL from proxyconfig.init_config where APP_NAME = $appName;
  ObProxyMysqlRequest &client_request = sm_->trans_state_.trans_info_.client_request_;
  ObString table_name = client_request.get_parse_result().get_table_name();
  ObArenaAllocator allocator;
  ObExprParseResult expr_result;
  const char *pos1 = NULL;
  const char *pos2 = NULL;

  ObString sql = client_request.get_sql();
  // find select column
  if (NULL == (pos1 = strcasestr(sql.ptr(), "SELECT"))) {
    WDIAG_ICMD("invalid sql", K(sql));
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (pos2 = strcasestr(pos1, "FROM"))) {
     WDIAG_ICMD("invalid sql", K(sql));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString select_col = ObString(pos2 - (pos1 + 6), pos1 + 6).trim();
    if (select_col.case_compare(CONFIG_VAL) == 0 ||
        select_col.case_compare("*") == 0) {
    } else {
      WDIAG_ICMD("invalid select_col", K(select_col), K(sql));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(handle_parse_where_fields(&allocator, expr_result,
                     static_cast<ObCollationType>(sm_->get_client_session()->get_session_info().get_collation_connection())))) {
    WDIAG_ICMD("fail to parse where fileds", K(ret));
  } else {
    ObString app_block = table_name;
    ObString app_name;
    ObString app_version;
    ObString config_val;
    //here we only need app_name
    get_params_from_parse_result(expr_result, app_name, app_version, config_val);
    DEBUG_ICMD("after parse ", K(app_name), K(app_block));
    if (app_name.empty() || app_block.empty()) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("invalid argument", K(app_name), K(app_block));
    } else if (OB_FAIL(dump_select_result(app_name, app_block))) {
      WDIAG_ICMD("fail to dump_select_result", K(ret));
    } else {
      DEBUG_ICMD("succ to dump_select_result");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObDdsConfigHandler::handle_parse_where_fields(ObArenaAllocator* allocator, ObExprParseResult& expr_result,
                                                  ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;
  bool need_parse_fields = true;
  ObExprParseMode parse_mode = INVALID_PARSE_MODE;
  ObProxyMysqlRequest &client_request = sm_->trans_state_.trans_info_.client_request_;
  ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
  ObString sql = client_request.get_sql();
  if (sql_parse_result.is_select_stmt() || sql_parse_result.is_delete_stmt()) {
    // we treat delete as select
    parse_mode = SELECT_STMT_PARSE_MODE;
  } else if (sql_parse_result.is_insert_stmt() || sql_parse_result.is_replace_stmt()
             || sql_parse_result.is_update_stmt() || sql_parse_result.is_merge_stmt()) {
    parse_mode = INSERT_STMT_PARSE_MODE;
  } else {
    DEBUG_ICMD("no need parse sql fields", K(client_request.get_parse_sql()));
    need_parse_fields = false;
  }
  if (need_parse_fields) {
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("allocator is null", K(ret));
    } else {
      ObExprParser expr_parser(*allocator, parse_mode);
      expr_result.part_key_info_.key_num_ = 0;
      if (OB_FAIL(expr_parser.parse_reqsql(sql,  sql_parse_result.get_parsed_length(),
                                           expr_result, sql_parse_result.get_stmt_type(),
                                           connection_collation))) {
        WDIAG_ICMD("fail to do expr parse_reqsql", K(sql), K(ret));
      } else {
        DEBUG_ICMD("parse success:", K(sql), K(expr_result.all_relation_info_.relation_num_));
      }
    }
  }
  return ret;
}
void ObDdsConfigHandler::get_params_from_parse_result(const ObExprParseResult& expr_result,
    ObString& app_name, ObString& app_version, ObString& config_val)
{
  for (int i = 0; i < expr_result.all_relation_info_.relation_num_; i++) {
    ObProxyRelationExpr* relation_expr = expr_result.all_relation_info_.relations_[i];
    ObString name_str;
    ObString value_str;
    if (OB_ISNULL(relation_expr)) {
      WDIAG_ICMD("Got an empty relation_expr", K(i));
      continue;
    }
    if (relation_expr->left_value_ != NULL
        && relation_expr->left_value_->head_ != NULL
        && relation_expr->left_value_->head_->type_ == TOKEN_COLUMN) {
      if (relation_expr->left_value_->head_->column_name_.str_ != NULL) {
        name_str.assign_ptr(relation_expr->left_value_->head_->column_name_.str_,
                            relation_expr->left_value_->head_->column_name_.str_len_);
      } else {
        WDIAG_ICMD("get an empty column_name");
      }
    } else {
      WDIAG_ICMD("left value is null");
    }
    if (relation_expr->right_value_ != NULL) {
      ObProxyTokenNode *token = relation_expr->right_value_->head_;
      while (NULL != token) {
        switch (token->type_) {
        case TOKEN_STR_VAL:
          value_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
          break;
        default:
          DEBUG_ICMD("invalid token type", "token type", get_obproxy_token_type(token->type_));
          break;
        }
        token = token->next_;
      }
    } else {
      WDIAG_ICMD("right value is null");
    }
    if (name_str.case_compare(APP_NAME_COL) == 0) {
      app_name = value_str;
    } else if (name_str.case_compare(APP_VERSION_COL) == 0) {
      app_version = value_str;
    } else if (name_str.case_compare(CONFIG_VAL) == 0) {
      config_val = value_str;
    }
    DEBUG_ICMD("in relation", K(name_str), K(value_str));
  }
}
int ObDdsConfigHandler::update_dds_config_to_processor(
    const common::ObString& app_name,
    const common::ObString& app_version,
    const common::ObString& app_block,
    const common::ObString& config_val)
{
  int ret = OB_SUCCESS;
  ObProxyConfigType config_type = get_config_type_by_str(app_block);
  if (SECURITY_CONFIG == config_type) {
    if (OB_FAIL(get_global_proxy_config_processor().update_app_security_config(app_name, app_version, config_val, config_type))) {
      WDIAG_ICMD("fail to update app security config", K(ret), K(app_name), K(app_version));
    }
  } else if (OB_FAIL(get_global_proxy_config_processor().update_app_config(app_name, app_version, config_val, config_type))) {
    WDIAG_ICMD("fail to update app config", K(ret), K(app_name), K(app_version));
  }
  return ret;
}

int ObDdsConfigHandler::handle_update_variables()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  // sql etc: update proxyconfig.init_config set CONFIG_VAL = ${value} where APP_NAME = $appName and VERSION = $version;
  ObProxyMysqlRequest &client_request = sm_->trans_state_.trans_info_.client_request_;
  ObString table_name = client_request.get_parse_result().get_table_name();
  ObArenaAllocator allocator;
  ObExprParseResult expr_result;
  if (OB_FAIL(handle_parse_where_fields(&allocator, expr_result,
              static_cast<ObCollationType>(sm_->get_client_session()->get_session_info().get_collation_connection())))) {
    WDIAG_ICMD("fail to parse where fileds", K(ret));
  } else {
    ObString app_block = table_name;
    ObString app_name;
    ObString app_version;
    ObString config_val;
    // ObString sql = client_request.get_sql();
    // char* pos1 = strcasestr(sql.ptr(), "'");
    // char* pos2 = strcasestr(pos1+1, "'");
    get_params_from_parse_result(expr_result, app_name, app_version, config_val);
    // config_val = ObString(pos2 - pos1 - 1, pos1+1);
    DEBUG_ICMD("after parse", K(app_block), K(app_name), K(app_version), K(config_val));
    if (app_name.empty() || app_version.empty() || config_val.empty()) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("invalid argument", K(app_name), K(app_version), K(config_val));
    } else if (OB_FAIL(update_dds_config_to_processor(app_name, app_version, app_block, config_val))) {
      WDIAG_ICMD("fail to update_dds_config_to_processor", K(ret));
    } else if (OB_FAIL(encode_ok_packet(1, capability_))) {
      WDIAG_ICMD("fail to encode_ok_packet", K(ret));
    } else {
      DEBUG_ICMD("succ to encode_ok_packet");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

static int dds_config_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                   ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObDdsConfigHandler *handler = NULL;
  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObDdsConfigHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObDdsConfigHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObDdsConfigHandler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObDdsConfigHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObDdsConfigHandler");
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int dds_config_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_SHOW,
              &dds_config_cmd_callback, true))) {
    WDIAG_ICMD("fail to register_cmd for OBPROXY_T_SHOW", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_SET_NAMES,
                    &dds_config_cmd_callback, true))) {
    WDIAG_ICMD("fail to register_cmd for OBPROXY_T_SET_NAMES", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_SET,
                    &dds_config_cmd_callback, true))) {
    WDIAG_ICMD("fail to register_cmd for OBPROXY_T_SET", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_SELECT,
                    &dds_config_cmd_callback, true))) {
    WDIAG_ICMD("fail to register_cmd for OBPROXY_T_SELECT", K(ret));
  } else if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_UPDATE,
                    &dds_config_cmd_callback, true))) {
    WDIAG_ICMD("fail to register_cmd for OBPROXY_T_UPDATE", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

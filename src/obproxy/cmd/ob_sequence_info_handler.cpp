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
#include "cmd/ob_sequence_info_handler.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/ob_ldc_location.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "obutils/ob_hot_upgrade_processor.h"
#include "obutils/ob_vip_tenant_processor.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_proxy_sequence_entry.h"
#include "obutils/ob_proxy_sequence_entry_cont.h"
#include "obutils/ob_proxy_sequence_utils.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/shard/obproxy_shard_utils.h"



using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum
{
  OB_IC_SEQ_TIMESTAMP = 0,
  OB_IC_SEQ_DBTIMESTAMP,
  OB_IC_SEQ_GROUPID,
  OB_IC_SEQ_TABLEID,
  OB_IC_SEQ_EID,
  OB_IC_SEQ_NEXTVAL,
  OB_IC_SEQ_MAX_COLUMN_ID,
};

const ObProxyColumnSchema LIST_COLUMN_ARRAY[OB_IC_SEQ_MAX_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_TIMESTAMP,    "timestamp", obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_DBTIMESTAMP,    "dbtimestamp", obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_GROUPID,    "groupid", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_TABLEID,    "tableid", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_TABLEID,    "eid", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_SEQ_NEXTVAL,    "nextval", obmysql::OB_MYSQL_TYPE_LONGLONG),
};
ObSequenceInfoHandler::ObSequenceInfoHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
    const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), result_(NULL)
{
  sm_ = reinterpret_cast<ObMysqlSM *>(cont);
  seq_info_ = NULL;
  retry_time_ = 0;
  sequence_sql_.set_value(sm_->trans_state_.trans_info_.client_request_.get_sql());
  result_ = &(sm_->trans_state_.trans_info_.client_request_.get_parse_result().get_dual_result());
  table_id_ = 0;
  group_id_ = 0;
  eid_ = 0;
  param_.need_db_timestamp_ = false;
  param_.need_value_ = false;
  param_.only_need_db_timestamp_ = false;
  logic_tenant_name_.reset();
  logic_database_name_.reset();
  memset(err_msg_, 0, SEQUENCE_ERR_MSG_SIZE);
  SET_HANDLER(&ObSequenceInfoHandler::do_get_next_sequence);
}

int ObSequenceInfoHandler::handle_sequece_params()
{
  int ret = OB_SUCCESS;
  // conver to uppercase
  if (result_->select_fields_[0].seq_name_.length() >= MAX_SEQ_NAME_SIZE) {
    LOG_WARN("seq_name too long, unsupported", K(result_->select_fields_[0].seq_name_));
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "seq name more than %d", MAX_SEQ_NAME_SIZE);
    ret = OB_ERR_UNEXPECTED;
  } else {
    memcpy(seq_name_buf_, result_->select_fields_[0].seq_name_.ptr(), result_->select_fields_[0].seq_name_.length());
    seq_name_buf_[result_->select_fields_[0].seq_name_.length()] = '\0';
    string_to_upper_case(seq_name_buf_, result_->select_fields_[0].seq_name_.length());
    param_.seq_name_ = ObString::make_string(seq_name_buf_);
    param_.need_db_timestamp_ = result_->need_db_timestamp_;
    param_.need_value_ = result_->need_value_;
    param_.min_value_ = SEQUENCE_DEFAULT_MIN_VALUE;
    param_.max_value_ = SEQUENCE_DEFAULT_MAX_VALUE;
    param_.step_ = SEQUENCE_DEFAULT_STEP;
    param_.retry_count_ = SEQUENCE_DEFAULT_RETRY_COUNT;
    ObClientSessionInfo &session_info = sm_->client_session_->get_session_info();
    is_shard_seq_req_ = session_info.is_sharding_user();
    if (is_shard_seq_req_) {
      ret = handle_shard_sequence_params();
    } else {
      ret = handle_single_sequence_params();
    }
  }
  return ret;
}

int ObSequenceInfoHandler::handle_shard_sequence_params()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter handle_shard_sequence_params", K(sequence_sql_));
  // bool have_sharding_col = false;
  ObClientSessionInfo &session_info = sm_->client_session_->get_session_info();
  ObString logic_tenant_name;
  ObString logic_database_name;

  if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
    ret = OB_ERR_UNEXPECTED;
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "get logic tenant name fail");
    LOG_WARN("get_logic_tenant_name failed", K(sequence_sql_));
    return ret;
  }
  if (OB_FAIL(session_info.get_logic_database_name(logic_database_name))) {
    ret = OB_ERR_UNEXPECTED;
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "get logic databse name fail");
    LOG_WARN("get_logic_database_name failed", K(sequence_sql_));
    return ret;
  }
  LOG_DEBUG("logic info is ", K(logic_tenant_name), K(logic_database_name));
  ObSqlParseResult& parse_result = sm_->trans_state_.trans_info_.client_request_.get_parse_result();
  SqlFieldResult& sql_result = parse_result.get_sql_filed_result();
  for (int i = 0; i < sql_result.field_num_; i++) {
    LOG_DEBUG("SqlField is ", K(i), K(sql_result.fields_[i]));
  }
  ObDbConfigLogicDb* logic_db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name,
                                     logic_database_name);
  if (OB_ISNULL(logic_db_info)) {
    ret = OB_ERR_UNEXPECTED;
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "get logic dbinfo failed");
    LOG_WARN("logic_db_info not exist", K(logic_tenant_name), K(logic_database_name), K(sequence_sql_));
    return ret;
  }
  ObShardConnector* shard_conn = NULL;
  int64_t table_id = 0;
  int64_t group_id = 0;
  int64_t es_id = 0;
  ObString sequence_table_name = logic_db_info->get_sequence_table_name();
  ObSequenceParam sequence_param;
  if (OB_FAIL(logic_db_info->get_sequence_param(sequence_param))) {
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "get_sequence_param failed");
    LOG_WARN("fail to get_sequence_param", K(ret));
    return ret;
  }
  param_.min_value_ = sequence_param.min_value_;
  param_.max_value_ = sequence_param.max_value_;
  param_.step_ = sequence_param.step_;
  param_.retry_count_ = sequence_param.retry_count_;
  param_.tnt_id_ = parse_result.get_dbmesh_route_info().tnt_id_;
  param_.tnt_col_.set_value(sequence_param.tnt_col_);
  if (sequence_table_name.empty()) {
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "sequence_table_name is empty");
    LOG_ERROR("sequence table name is invalid", K(sequence_sql_));
    ret = OB_ERR_UNEXPECTED;
  } else if (!param_.tnt_id_.empty() && param_.tnt_col_.empty()) {
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "tnt_col is empty while tnt_id used");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObProxyShardUtils::get_real_info(*logic_db_info,
                     sequence_table_name,
                     parse_result,
                     shard_conn,
                     real_database_buf_,
                     OB_MAX_DATABASE_NAME_LENGTH,
                     real_table_name_buf_,
                     OB_MAX_TABLE_NAME_LENGTH,
                     &group_id,
                     &table_id,
                     &es_id))) {
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "Sequence table rule calculate error");
    LOG_WARN("get_real_info failed", K(ret), K(sequence_sql_));
  } else if (OB_ISNULL(shard_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid shard_conn", K(sequence_sql_));
  } else {
    LOG_DEBUG("shard_conn is ", K(*shard_conn));
    param_.shard_conn_ = shard_conn;
    param_.server_type_ = shard_conn->server_type_;
    param_.cluster_name_ = shard_conn->cluster_name_;
    param_.tenant_name_ = shard_conn->tenant_name_;
    param_.database_name_ = ObString::make_string(real_database_buf_);
    param_.table_name_ = ObString::make_string(real_table_name_buf_);
    param_.password_ = shard_conn->password_.config_string_;
    if (DB_OB_MYSQL == param_.server_type_ || DB_OB_ORACLE == param_.server_type_) {
      param_.username_ = shard_conn->username_;
    } else {
      param_.username_ = shard_conn->full_username_.config_string_;
    }
    logic_tenant_name_.set_value(logic_tenant_name);
    logic_database_name_.set_value(logic_database_name);
    LOG_DEBUG("hsr info", K(session_info.get_login_req().get_hsr_result()), K(param_.password_.hash()));
    table_id_ = table_id;
    group_id_ = group_id;
    eid_ = es_id;

    snprintf(seq_id_buf_, 2048, "%.*s-%.*s-%.*s-%.*s-%.*s-%.*s-%02ld-%02ld-%02ld",
             logic_tenant_name_.config_string_.length(), logic_tenant_name_.config_string_.ptr(),
             logic_database_name_.config_string_.length(), logic_database_name_.config_string_.ptr(),
             shard_conn->shard_name_.config_string_.length(), shard_conn->shard_name_.config_string_.ptr(),
             param_.table_name_.length(), param_.table_name_.ptr(),
             param_.seq_name_.length(), param_.seq_name_.ptr(),
             param_.tnt_id_.length(), param_.tnt_id_.ptr(),
             group_id_, table_id_, eid_);
    param_.seq_id_ = ObString(seq_id_buf_);
  }
  LOG_DEBUG("Left handle_shard_sequence_params", K(param_), K(table_id_), K(group_id_), K(eid_));
  return ret;
}

int ObSequenceInfoHandler::handle_single_sequence_params()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter handle_single_sequence_params", K(sequence_sql_));
  ObClientSessionInfo &session_info = sm_->client_session_->get_session_info();
  param_.server_type_ = DB_OB_MYSQL;
  param_.cluster_name_ = session_info.get_login_req().get_hsr_result().cluster_name_;
  param_.tenant_name_ = session_info.get_login_req().get_hsr_result().tenant_name_;
  param_.database_name_ = session_info.get_database_name();
  param_.table_name_ = ObProxySequenceUtils::get_default_sequence_table_name();
  param_.password_ = session_info.get_login_req().get_hsr_result().response_.get_auth_response();
  param_.username_ = session_info.get_login_req().get_hsr_result().user_name_;
  table_id_ = 0;
  group_id_ = 0;
  eid_ = 0;

  snprintf(seq_id_buf_, 2048, "%.*s-%.*s-%.*s-%.*s-%.*s-%.*s-%.*s-%02ld-%02ld-%02ld",
           logic_tenant_name_.config_string_.length(), logic_tenant_name_.config_string_.ptr(),
           logic_database_name_.config_string_.length(), logic_database_name_.config_string_.ptr(),
           param_.cluster_name_.length(), param_.cluster_name_.ptr(),
           param_.tenant_name_.length(), param_.tenant_name_.ptr(),
           param_.database_name_.length(), param_.database_name_.ptr(),
           param_.table_name_.length(), param_.table_name_.ptr(),
           param_.seq_name_.length(), param_.seq_name_.ptr(),
           group_id_, table_id_, eid_);
  LOG_DEBUG("Left handle_single_sequence_params", K(param_));
  return ret;
}

ObSequenceInfoHandler::~ObSequenceInfoHandler()
{
  result_ = NULL;
}

int ObSequenceInfoHandler::handle_sequence_done(int event, void *data)
{
  int event_ret = EVENT_NONE;
  DEBUG_ICMD("Enter handle_sequence_done");
  if (event != SEQUENCE_ENTRY_CREATE_COMPLETE_EVENT) {
    WARN_ICMD("invalid event ", K(event));
  }
  event_ret = process_sequence_info(data);
  return event_ret;
}

int ObSequenceInfoHandler::encode_err_packet(const int errcode)
{
  int ret = OB_SUCCESS;
  const int32_t MAX_MSG_BUF_SIZE = 256;
  char msg_buf[MAX_MSG_BUF_SIZE];
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    WARN_ICMD("it has not inited", K(ret));
  } else if (OB_FAIL(reset())) {//before encode_err_packet, we need clean buf
    WARN_ICMD("fail to do reset", K(errcode), K(ret));
  } else {
    int32_t length = 0;
    if (strlen(err_msg_) == 0) {
      length = snprintf(msg_buf, sizeof(msg_buf), "Select nextSeq error");
    } else {
      length = snprintf(msg_buf, sizeof(msg_buf), err_msg_);
    }
    if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= MAX_MSG_BUF_SIZE)) {
      ret = OB_BUF_NOT_ENOUGH;
      WARN_ICMD("msg_buf is not enough", K(length), K(err_msg_), K(ret));
    } else {}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet_buf(*internal_buf_, seq_, errcode, msg_buf))) {
      WARN_ICMD("fail to encode err packet", K(errcode), K(msg_buf), K(ret));
    } else {
      INFO_ICMD("succ to encode err packet", K(errcode), K(msg_buf));
    }
  }
  return ret;
}

int ObSequenceInfoHandler::process_sequence_info(void* data)
{
  DEBUG_ICMD("Enter process_sequence_info");
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObSequenceInfo* tmp_seq_info = reinterpret_cast<ObSequenceInfo *>(data);
  ObEThread *ethread = NULL;
  if (OB_ISNULL(tmp_seq_info)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("process_sequence_info get invalid argument", K(data));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (tmp_seq_info->errno_ != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err happend in cont", K(tmp_seq_info->errno_ ), K(tmp_seq_info->err_msg_ ));
    snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "%.*s", tmp_seq_info->err_msg_.config_string_.length(),
             tmp_seq_info->err_msg_.config_string_.ptr());
  } else if (param_.need_value_ && !tmp_seq_info->is_valid()) {
    LOG_INFO("is over need reschedule", K(*tmp_seq_info), K(retry_time_));
    op_free(tmp_seq_info);
    tmp_seq_info = NULL;
    if (retry_time_ < get_global_proxy_config().sequence_fail_retry_count) {
      event::ObAction* action = NULL;
      ++retry_time_;
      get_global_sequence_cache().get_next_sequence(param_,
          (process_seq_pfn)&ObSequenceInfoHandler::process_sequence_info, action);
    } else {
      snprintf(err_msg_, SEQUENCE_ERR_MSG_SIZE, "fail after retry, maybe step too small");
      LOG_WARN("fail after retry ", K(param_.seq_id_), K(retry_time_));
      ret = OB_SEQUENCE_ERROR;
    }
  } else {
    seq_info_ = tmp_seq_info;
    if (OB_FAIL(dump_header())) {
      WARN_ICMD("fail to dump_list_header", K(ret), K(*seq_info_));
    } else if (OB_FAIL(dump_body())) {
      WARN_ICMD("fail to dump_list_body sm", K(ret), K(*seq_info_));
    } else if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret), K(*seq_info_));
    } else {
      DEBUG_ICMD("succ to dump info", K(retry_time_), K(param_), KPC(seq_info_));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(OB_SEQUENCE_ERROR);
  }
  if (tmp_seq_info != NULL) {
    op_free(tmp_seq_info);
    tmp_seq_info = NULL;
  }
  LOG_DEBUG("Left process_sequence_info", K(ret), K(event_ret));
  return event_ret;
}
int ObSequenceInfoHandler::do_get_next_sequence(int event, void* data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(handle_sequece_params())) {
    WARN_ICMD("handle_sequece_params failed");
  } else {
    SET_HANDLER(&ObSequenceInfoHandler::handle_sequence_done);
    event::ObAction* action = NULL;
    param_.cont_ = this;
    DEBUG_ICMD("sequence_param is ", K(param_));
    get_global_sequence_cache().get_next_sequence(param_,
        (process_seq_pfn)&ObSequenceInfoHandler::process_sequence_info, action);
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(OB_SEQUENCE_ERROR);
  }
  return event_ret;
}
int ObSequenceInfoHandler::dump_header()
{
  int ret = OB_SUCCESS;
#define SEQ_BUF_SIZE 128
  char full_name[OB_IC_SEQ_MAX_COLUMN_ID][SEQ_BUF_SIZE];
  obmysql::EMySQLFieldType file_type[OB_IC_SEQ_MAX_COLUMN_ID];
  ObString cname[OB_IC_SEQ_MAX_COLUMN_ID];
  ObString dbp_runtime = ObString::make_string(OB_PROXY_DBP_RUNTIME_ENV);
#define MACRO_FOR_COLUMN_NAME_ARRAY(keyword, type) \
  if (result_->select_fields_[i].seq_field_.case_compare(keyword) == 0) { \
      if (!result_->select_fields_[i].alias_name_.empty()) { \
        cname[i] = result_->select_fields_[i].alias_name_; \
      } else { \
        if (0 == dbp_runtime.case_compare(get_global_proxy_config().runtime_env.str())) {\
          snprintf(full_name[i], SEQ_BUF_SIZE, "%.*s.%.*s",  \
          result_->select_fields_[i].seq_name_.length(), \
          result_->select_fields_[i].seq_name_.ptr(), \
          result_->select_fields_[i].seq_field_.length(), \
          result_->select_fields_[i].seq_field_.ptr()); \
        } else { \
          snprintf(full_name[i], SEQ_BUF_SIZE, "%.*s",  \
          result_->select_fields_[i].seq_field_.length(), \
          result_->select_fields_[i].seq_field_.ptr()); \
        } \
        cname[i] = ObString(full_name[i]); \
      } \
      file_type[i] = type; \
      continue; \
    }
  for (int i = 0; i < result_->select_fields_size_; i++) {
    MACRO_FOR_COLUMN_NAME_ARRAY("nextval", obmysql::OB_MYSQL_TYPE_LONGLONG);
    MACRO_FOR_COLUMN_NAME_ARRAY("timestamp", obmysql::OB_MYSQL_TYPE_VARCHAR);
    MACRO_FOR_COLUMN_NAME_ARRAY("dbtimestamp", obmysql::OB_MYSQL_TYPE_VARCHAR);
    MACRO_FOR_COLUMN_NAME_ARRAY("groupid", obmysql::OB_MYSQL_TYPE_LONGLONG);
    MACRO_FOR_COLUMN_NAME_ARRAY("tableid", obmysql::OB_MYSQL_TYPE_LONGLONG);
    MACRO_FOR_COLUMN_NAME_ARRAY("eid", obmysql::OB_MYSQL_TYPE_LONGLONG);
    MACRO_FOR_COLUMN_NAME_ARRAY("elasticid", obmysql::OB_MYSQL_TYPE_LONGLONG);
    LOG_ERROR("invalid seq_field:", K(i), K(result_->select_fields_[i].seq_field_));
    return OB_ERR_UNEXPECTED;
  }
  if (OB_FAIL(encode_header(cname, file_type, result_->select_fields_size_))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}
int ObSequenceInfoHandler::dump_body()
{
  int ret = OB_SUCCESS;
  ObSqlString name;
  ObNewRow row;
  ObObj cells[OB_IC_SEQ_MAX_COLUMN_ID];
  row.count_ = result_->select_fields_size_;
  row.cells_ = cells;
  char now_time_str[1024];
  now_time_str[0] = '\0';
  for (int i = 0; i < result_->select_fields_size_; i++) {
    if (result_->select_fields_[i].seq_field_.case_compare("nextval") == 0) {
      cells[i].set_int(seq_info_->local_value_);
    } else if (result_->select_fields_[i].seq_field_.case_compare("timestamp") == 0) {
      ObProxySequenceUtils::get_nowtime_string(now_time_str, 1024);
      cells[i].set_varchar(now_time_str);
    } else if (result_->select_fields_[i].seq_field_.case_compare("dbtimestamp") == 0) {
      if (seq_info_->db_timestamp_.is_valid()) {
        cells[i].set_varchar(seq_info_->db_timestamp_);
      } else {
        cells[i].set_varchar("");
      }
    } else if (result_->select_fields_[i].seq_field_.case_compare("groupid") == 0) {
      cells[i].set_int(group_id_);
    }  else if (result_->select_fields_[i].seq_field_.case_compare("tableid") == 0) {
      cells[i].set_int(table_id_);
    } else if (result_->select_fields_[i].seq_field_.case_compare("eid") == 0
               || result_->select_fields_[i].seq_field_.case_compare("elasticid") == 0) {
      cells[i].set_int(eid_);
    } else {
      LOG_ERROR("invalid seq_field:", K(result_->select_fields_[i].seq_field_));
    }
  }
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}
static int sequence_info_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                      ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObSequenceInfoHandler *handler = NULL;
  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObSequenceInfoHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObSequenceInfoHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObSequenceInfoHandler");
  } else {
    action = &handler->get_action();
    // if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
    if (OB_ISNULL(self_ethread().schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule handler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule handler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}
int sequence_info_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_DUAL,
              &sequence_info_cmd_callback))) {
    WARN_ICMD("fail to register OBPROXY_T_ICMD_DUAL CMD", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

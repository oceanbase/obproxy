/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_route_diagnosis.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static const char *get_diagnosis_type_name(const ObDiagType type) {
  const char *name = NULL;
  switch(type) { 
    case BASE:
      name = "BASE";
      break;
    
    case SQL_PARSE:
      name = "SQL_PARSE";
      break;
    
    case ROUTE_INFO:
      name = "ROUTE_INFO";
      break;
    
    case LOCATION_CACHE_LOOKUP:
      name = "LOCATION_CACHE_LOOKUP";
      break;

    case ROUTINE_ENTRY_LOOKUP:
      name = "ROUTINE_ENTRY_LOOKUP";
      break;
    
    case TABLE_ENTRY_LOOKUP:
      name = "TABLE_ENTRY_LOOKUP";
      break;

    case PARTITION_ID_CALC_DONE:
      name = "PARTITION_ID_CALC_DONE";
      break;
    
    case PARTITION_ENTRY_LOOKUP:
      name = "PARTITION_ENTRY_LOOKUP";
      break;

    case ROUTE_POLICY:
      name = "ROUTE_POLICY";
      break;

    case CONGESTION_CONTROL:
      name = "CONGESTION_CONTROL";
      break;
    
    case HANDLE_RESPONSE:
      name = "HANDLE_RESPONSE";
      break;
    
    case RETRY:
      name = "RETRY";
      break;
    
    case BASIC_NUM:
      name = "BASIC_NUM";
      break;
    
    default:
      name = "unknown diag type";
      break;
  }
  return name;
}
static const char *get_route_info_type_name(const ObDiagRouteInfo::RouteInfoType type)
{
  const char *name = NULL;
  switch(type) {
    case ObDiagRouteInfo::INVALID:
      name = "INVALID";
      break;
    
    case ObDiagRouteInfo::USE_INDEX:
      name = "USE_INDEX";
      break;

    case ObDiagRouteInfo::USE_OBPROXY_ROUTE_ADDR:
      name = "USE_OBPROXY_ROUTE_ADDR";
      break;
    
    case ObDiagRouteInfo::USE_CURSOR:
      name = "USE_CURSOR";
      break;

    case ObDiagRouteInfo::USE_PIECES:
      name = "USE_PIECES";
      break;

    case ObDiagRouteInfo::USE_CONFIG_TARGET_DB:
      name = "USE_CONFIG_TARGET_DB";
      break;

    case ObDiagRouteInfo::USE_COMMENT_TARGET_DB:
      name = "USE_COMMENT_TARGET_DB";
      break;
    
    case ObDiagRouteInfo::USE_TEST_SVR_ADDR:
      name = "USE_TEST_SVR_ADDR";
      break;

    case ObDiagRouteInfo::USE_LAST_SESSION:
      name = "USE_LAST_SESSION";
      break;
    
    case ObDiagRouteInfo::USE_PARTITION_LOCATION_LOOKUP:
      name = "USE_PARTITION_LOCATION_LOOKUP";
      break;

    case ObDiagRouteInfo::USE_COORDINATOR_SESSION:
      name = "USE_COODINATOR_SESSION";
      break;

    default:
      name = "unkonwn route type";
  }
  return name;
}
static const char *get_retry_type_name(const ObDiagRetry::RetryType type) {
  const char *name = NULL;
  switch(type) {
    case ObDiagRetry::INVALID:
      name = "INVALID";
      break;

    case ObDiagRetry::CMNT_TARGET_DB_SERVER:
      name = "CMNT_TARGET_DB_SERVER";
      break;
    
    case ObDiagRetry::CONF_TARGET_DB_SERVER:
      name = "CONF_TARGET_DB_SERVER";
      break;

    case ObDiagRetry::TEST_SERVER_ADDR:
      name = "TEST_SERVER_ADDR";
      break;
    
    case ObDiagRetry::USE_PARTITION_LOCATION_LOOKUP:
      name = "USE_PARTITION_LOCATION_LOOKUP";
      break;
    
    default:
      name = "unknown retry type";
  }
  return name;
}
int64_t ObDiagPoint::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  switch(base_.type_) {
    case SQL_PARSE: 
      pos += sql_parse_.to_string(buf + pos, buf_len - pos);
      break;
    case ROUTE_INFO:
      pos += route_info_.to_string(buf + pos, buf_len - pos);
      break;
    case LOCATION_CACHE_LOOKUP:
      pos += location_cache_lookup_.to_string(buf + pos, buf_len - pos);
      break;
    case ROUTINE_ENTRY_LOOKUP:
      pos += routine_entry_lookup_.to_string(buf + pos, buf_len - pos);
      break;
    case TABLE_ENTRY_LOOKUP:
      pos += table_entry_lookup_.to_string(buf + pos, buf_len - pos);
      break;
    case PARTITION_ID_CALC_DONE:
      pos += partition_id_calc_.to_string(buf + pos, buf_len - pos);
      break;
    case PARTITION_ENTRY_LOOKUP:
      pos += partition_entry_lookup_.to_string(buf + pos, buf_len - pos);
      break;
    case ROUTE_POLICY:
      pos += route_policy_.to_string(buf + pos, buf_len - pos);
      break;
    case CONGESTION_CONTROL:
      pos += congestion_control_.to_string(buf + pos, buf_len - pos);
      break;
    case HANDLE_RESPONSE:
      pos += handle_response_.to_string(buf + pos, buf_len - pos);
      break;
    case RETRY:
      pos += retry_connection_.to_string(buf + pos, buf_len - pos);
      break;
    default:
      pos += base_.to_string(buf + pos, buf_len - pos);
      break;
  }
  BUF_PRINTF(";");
  return pos;
}

int64_t ObDiagBase::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  BUF_PRINTF("Base");
  J_COLON();
  J_OBJ_START();
  J_KV(K_(type), K_(ret));
  J_OBJ_END();
  return pos;
}

void ObDiagBase::reset() {
  ret_ = 0;
  type_ = BASE;
  alloc_ = NULL;
}

void ObDiagBase::alloc_then_copy_string(ObString &src, ObString &dest) {
  char *c = NULL;
  const static char *fail = "fail_to_alloc_memory";
  size_t len = src.length();
  if (OB_ISNULL(alloc_)) {
    LOG_WARN("can not alloc mem for route diagnosis point since allocator is NULL");
    dest.assign_ptr(fail, (ObString::obstr_size_t) strlen(fail));
  } else if (len != 0 && NULL == (c = (char*) alloc_->alloc(len))) {
    LOG_WARN("fail to alloc mem for route diagnosis point", K(len));
    dest.assign_ptr(fail, (ObString::obstr_size_t) strlen(fail));
  } else {
    MEMCPY(c, src.ptr(), len);
    dest.assign_ptr(c, (ObString::obstr_size_t) len);
  }
}
int64_t ObDiagSqlParse::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("sql_cmd:\"%s\"", ObProxyParserUtils::get_sql_cmd_name(sql_cmd_));
    J_COMMA();
    J_KV("sql", sql_);
    J_COMMA();
    J_KV("table", table_);
  );
}

void ObDiagSqlParse::reset() {
  this->ObDiagBase::reset();
  sql_.reset();
  table_.reset();
  sql_cmd_ = obmysql::ObMySQLCmd(0);
}

int64_t ObDiagLocationCacheLookup::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    J_KV("mode", obutils::ObProxyConfig::get_routing_mode_str(mode_), 
         K_(need_partition_location_lookup)));
}

void ObDiagLocationCacheLookup::reset() {
  this->ObDiagBase::reset();
  mode_ = obutils::ObServerRoutingMode(0);
  need_partition_location_lookup_ = false;
}

int64_t ObDiagRoutineEntryLookup::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    J_KV(K_(routine_sql),
         K_(from_remote),
         K_(is_lookup_succ),
         "entry_state", ObRouteEntry::get_route_entry_state(entry_state_)));
}

void ObDiagRoutineEntryLookup::reset() {
  this->ObDiagBase::reset();
  routine_sql_.reset();
  from_remote_ = false;
  is_lookup_succ_ = false;
  entry_state_ = ObRouteEntry::ObRouteEntryState(0);
}
int64_t ObDiagTableEntryLookup::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    J_KV(K_(table),
         K_(table_id),
         K_(part_num),
         "table_type" , ob_table_type_str(table_type_),
         "entry_state", ObRouteEntry::get_route_entry_state(entry_state_),
         K_(entry_from_remote),
         K_(has_dup_replica),
         K_(is_lookup_succ)));
}

void ObDiagTableEntryLookup::reset() {
  this->ObDiagBase::reset();
  table_.reset();
  table_id_ = 0;
  part_num_ = 0;
  table_type_ = share::schema::ObTableType(0);
  entry_state_ = ObRouteEntry::ObRouteEntryState(0);
  entry_from_remote_ = false;
  has_dup_replica_ = false;
  is_lookup_succ_ = false;
}

int64_t ObDiagPartIDCalcDone::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("part_level:%d", static_cast<int>(level_));
    J_COMMA();
    if (level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE) {
      BUF_PRINTF("partitions:\"(p%ld)\"", part_idx_);
      J_COMMA();
    } else if (level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      BUF_PRINTF("partitions:\"(p%ldsp%ld)\"", part_idx_, sub_part_idx_);
      J_COMMA();
    }
    J_KV("parse_sql", parse_sql_);
    if (!part_name_.empty()) {
      J_COMMA();
      J_KV("part_name", part_name_);
    }
  );
}

void ObDiagPartIDCalcDone::reset() {
  this->ObDiagBase::reset();
  parse_sql_.reset();
  part_name_.reset();
  part_idx_ = 0;
  sub_part_idx_ = 0;
  level_ = share::schema::ObPartitionLevel(0);
}

int64_t ObDiagPartEntryLookup::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    J_KV(K_(part_id), 
         K_(from_remote),
         K_(has_dup_replica),
         "entry_state", ObRouteEntry::get_route_entry_state(entry_state_));
    if (leader_.server_.is_valid()) {
      J_COMMA();
      J_KV(K_(leader));
    }
  );
}

void ObDiagPartEntryLookup::reset() {
  this->ObDiagBase::reset();
  part_id_ = 0;
  from_remote_ = false;
  is_lookup_succ_ = false;
  has_dup_replica_ = false;
  entry_state_ = ObRouteEntry::ObRouteEntryState(0);
  leader_.reset();
}
int64_t ObDiagRouteInfo::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("route_info_type:\"%s\"", get_route_info_type_name(route_info_type_));
    if (svr_addr_.is_valid()) {
      J_COMMA();
      BUF_PRINTF("svr_addr:\"");
      pos += svr_addr_.to_string(buf + pos, buf_len - pos);
      BUF_PRINTF("\"");
    }
    J_COMMA();
    BUF_PRINTF("in_transaction:\"%s\"", STR_BOOL(in_transaction_));
    if (depent_func_) {
      J_COMMA();
      BUF_PRINTF("depent_func:\"true\"");
    }
    if (trans_specified_) {
      J_COMMA();
      BUF_PRINTF("trans_specified:\"true\"");
    }
  );
}

void ObDiagRouteInfo::reset() {
  this->ObDiagBase::reset();
  route_info_type_ = RouteInfoType(0);
  svr_addr_.reset();
  in_transaction_ = false;
  depent_func_ = false;
  trans_specified_ = false;
}

int64_t ObDiagRoutePolicy::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("consistency_level:\"%s\"", get_consistency_level_str(consistency_level_));
    if (!cur_idc_name_.empty()) {
      J_COMMA();
      J_KV("cur_idc_name", cur_idc_name_);
    }
    if (!primary_zone_.empty()) {
      J_COMMA();
      J_KV("primary_zone", primary_zone_);
    }
    if (!proxy_primary_zone_.empty()) {
      J_COMMA();
      J_KV("proxy_primary_zone", proxy_primary_zone_);
    }
    J_COMMA();
    BUF_PRINTF("route_policy:\"%s\"", get_route_policy_enum_string(route_policy_).ptr());
    J_COMMA();
    BUF_PRINTF("chosen_route_type:\"%s\"", get_route_type_string(chosen_route_type_).ptr());
    if (OB_NOT_NULL(chosen_server_.replica_) && chosen_server_.replica_->is_valid()) {
      J_COMMA();
      BUF_PRINTF("chosen_server:");
      pos += chosen_server_.to_string(buf + pos, buf_len - pos);
    }
  );
}

void ObDiagRoutePolicy::reset() {
  this->ObDiagBase::reset();
  cur_idc_name_.reset();
  primary_zone_.reset();
  proxy_primary_zone_.reset();
  consistency_level_ = ObConsistencyLevel(0);
  route_policy_ = ObRoutePolicyEnum(0);
  chosen_route_type_ = ObRouteType(0);
  chosen_server_.reset();
}
int64_t ObDiagCongestionControl::to_string(char *buf, const int64_t buf_len) const
{ 
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("force_retry_congested:\"%s\"", STR_BOOL(force_retry_congested_));
    if (alive_congested_) {
      BUF_PRINTF("alive_congested:\"%s\"", STR_BOOL(alive_congested_));
    }
    if (dead_congested_) {
      J_COMMA();
      BUF_PRINTF("dead_congested:\"%s\"", STR_BOOL(dead_congested_));
    }
    if (detect_congested_) {
      J_COMMA();
      BUF_PRINTF("detect_congested:\"%s\"", STR_BOOL(detect_congested_));
    }
    J_COMMA();
    BUF_PRINTF("need_congestion_lookup:\"%s\"", STR_BOOL(need_congestion_lookup_));
    J_COMMA();
    BUF_PRINTF("lookup_success:\"%s\"", STR_BOOL(lookup_success_));
    J_COMMA();
    BUF_PRINTF("entry_exist:\"%s\"", STR_BOOL(entry_exist_));
  );
}

void ObDiagCongestionControl::reset() {
  this->ObDiagBase::reset();
  alive_congested_ = false;
  dead_congested_ = false;
  detect_congested_ = false;
  force_retry_congested_ = false;
  need_congestion_lookup_ = false;
  lookup_success_ = false;
  entry_exist_ = false;
}

int64_t ObDiagHandleResponse::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    BUF_PRINTF("is_parititon_hit:\"%s\"", STR_BOOL(is_partition_hit_));
    J_COMMA();
    BUF_PRINTF("state:\"%s\"", ObMysqlTransact::get_server_state_name(state_));
    if (error_ != ObMysqlTransact::MIN_RESP_ERROR) {
      J_COMMA();
      BUF_PRINTF("error:\"%s\"", ObMysqlTransact::get_server_resp_error_name(error_));
    });
}

void ObDiagHandleResponse::reset() {
  this->ObDiagBase::reset();
  is_partition_hit_ = false;
  state_ = ObMysqlTransact::ObServerStateType(0);
  error_ = ObMysqlTransact::ObServerRespErrorType(0);
}
int64_t ObDiagRetry::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_POINT_PRINT(
    J_KV(K_(attempts), 
         "retry_status", ObMysqlTransact::get_retry_status_string(retry_status_), 
         "retry_type", get_retry_type_name(retry_type_), K_(retry_addr)));

}

void ObDiagRetry::reset() {
  this->ObDiagBase::reset();
  attempts_ = 0;
  retry_status_ = ObMysqlTransact::ObSSRetryStatus(0);
  retry_type_ = RetryType(0);
  retry_addr_.reset();
}
ObDiagPoint::ObDiagPoint() {
  MEMSET(this, 0, sizeof(ObDiagPoint));
}
ObDiagPoint::ObDiagPoint(const ObDiagPoint& odp) {
  *this = odp;
}

ObDiagPoint::~ObDiagPoint() {
  switch(base_.type_) {     
    case SQL_PARSE:
      sql_parse_.~ObDiagSqlParse();
      break;
    
    case ROUTE_INFO:
      route_info_.~ObDiagRouteInfo();
      break;
    
    case LOCATION_CACHE_LOOKUP:
      location_cache_lookup_.~ObDiagLocationCacheLookup();
      break;

    case ROUTINE_ENTRY_LOOKUP:
      routine_entry_lookup_.~ObDiagRoutineEntryLookup();
      break;
    
    case TABLE_ENTRY_LOOKUP:
      table_entry_lookup_.~ObDiagTableEntryLookup();
      break;

    case PARTITION_ID_CALC_DONE:
      partition_id_calc_.~ObDiagPartIDCalcDone();
      break;
    
    case PARTITION_ENTRY_LOOKUP:
      partition_entry_lookup_.~ObDiagPartEntryLookup();
      break;

    case ROUTE_POLICY:
      route_policy_.~ObDiagRoutePolicy();
      break;

    case CONGESTION_CONTROL:
      congestion_control_.~ObDiagCongestionControl();
      break;

    case HANDLE_RESPONSE:
      handle_response_.~ObDiagHandleResponse();
      break;
    
    case RETRY:
      retry_connection_.~ObDiagRetry();
      break;
    
    default:
      break;
  }
}
void ObDiagPoint::reset() {
  switch(this->base_.type_) {     
    case SQL_PARSE:
      this->sql_parse_.reset();
      break;
    
    case ROUTE_INFO:
      this->route_info_.reset();
      break;
    
    case LOCATION_CACHE_LOOKUP:
      this->location_cache_lookup_.reset();
      break;

    case ROUTINE_ENTRY_LOOKUP:
      this->routine_entry_lookup_.reset();
      break;
    
    case TABLE_ENTRY_LOOKUP:
      this->table_entry_lookup_.reset();
      break;

    case PARTITION_ID_CALC_DONE:
      this->partition_id_calc_.reset();
      break;
    
    case PARTITION_ENTRY_LOOKUP:
      this->partition_entry_lookup_.reset();
      break;

    case ROUTE_POLICY:
      this->route_policy_.reset();
      break;

    case CONGESTION_CONTROL:
      this->congestion_control_.reset();
      break;

    case HANDLE_RESPONSE:
      this->handle_response_.reset();
      break;
    
    case RETRY:
      this->retry_connection_.reset();
      break;

    default:
      MEMSET(this, 0, sizeof(ObDiagPoint));
      break;
  }
}
ObDiagPoint& ObDiagPoint::operator=(const ObDiagPoint& odp) {
  // reset memory of this
  this->reset();
  // copy other to the memory
  switch(odp.base_.type_) {     
    case SQL_PARSE:
      this->sql_parse_ = odp.sql_parse_;
      break;
    
    case ROUTE_INFO:
      this->route_info_ = odp.route_info_;
      break;
    
    case LOCATION_CACHE_LOOKUP:
      this->location_cache_lookup_ = odp.location_cache_lookup_;
      break;

    case ROUTINE_ENTRY_LOOKUP:
      this->routine_entry_lookup_ = odp.routine_entry_lookup_;
      break;
    
    case TABLE_ENTRY_LOOKUP:
      this->table_entry_lookup_ = odp.table_entry_lookup_;
      break;

    case PARTITION_ID_CALC_DONE:
      this->partition_id_calc_ = odp.partition_id_calc_;
      break;
    
    case PARTITION_ENTRY_LOOKUP:
      this->partition_entry_lookup_ = odp.partition_entry_lookup_;
      break;

    case ROUTE_POLICY:
      this->route_policy_ = odp.route_policy_;
      break;

    case CONGESTION_CONTROL:
      this->congestion_control_ =  odp.congestion_control_;
      break;

    case HANDLE_RESPONSE:
      this->handle_response_ = odp.handle_response_;
      break;
    
    case RETRY:
      this->retry_connection_ = odp.retry_connection_;
      break;
    
    default:
      this->base_ = odp.base_;
  }
  return *this;
}
void ObRouteDiagnosis::trans_first_sql_diagnosed(ObProxyMysqlRequest &cli_req) {
  // ObProxyMysqlRequest will not free the memory of request buf
  // ObRouteDiagnosis will do free after whole trx be diagnosed 
  // save the first sql
  if (cli_req.get_parse_result().has_explain_route()) {
    LOG_DEBUG("`explain route <sql>;` does not be treated as trans first sql");
  } else if (!cur_sql_diag_array_.empty() && SQL_PARSE == cur_sql_diag_array_[0].base_.type_) {
    trx_1st_sql_ = cur_sql_diag_array_[0].sql_parse_.sql_;
    cli_req.borrow_req_buf(trx_1st_req_buf_, trx_1st_req_buf_len_);
    LOG_DEBUG("borrow the trx 1st req buf", KP_(trx_1st_req_buf), K_(trx_1st_req_buf_len));
  } 
  trans_cur_sql_diagnosed();
}

void ObRouteDiagnosis::trans_cur_sql_diagnosed() {
  // clear cur sql diagnosis
  cur_sql_diag_array_.reuse();
  allocator_.reuse();
}

void ObRouteDiagnosis::trans_diagnosis_completed() {
  // free the memory
  if (OB_NOT_NULL(trx_1st_req_buf_) && 0 != trx_1st_req_buf_len_) {
    op_fixed_mem_free(trx_1st_req_buf_, trx_1st_req_buf_len_);
    LOG_DEBUG("free the trx 1st req buf", KP_(trx_1st_req_buf), K_(trx_1st_req_buf_len));
    trx_1st_req_buf_ = NULL;
    trx_1st_req_buf_len_ = 0;
    trx_1st_sql_.reset();
  }
  trans_cur_sql_diagnosed();
}
int64_t ObRouteDiagnosis::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const static char *line_separator = "/n";
  if (!trx_1st_sql_.empty()) {
    BUF_PRINTF("Trx 1st SQL: \"%s\"", trx_1st_sql_.ptr());
    BUF_PRINTF(line_separator);
  }

  BUF_PRINTF("Trx Cur SQL:");
  BUF_PRINTF(line_separator);
  for (int i = 0; i < cur_sql_diag_array_.count(); i++) {
    if (BASE != cur_sql_diag_array_[i].base_.type_) {
      FORMAT_PRINT(cur_sql_diag_array_[i]);
    }
  }

  return pos;
}
int64_t ObRouteDiagnosis::to_format_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const static char *line_separator = "\n";
  if (!trx_1st_sql_.empty()) {
    BUF_PRINTF("Trx 1st SQL: \"%s\"", trx_1st_sql_.ptr());
    BUF_PRINTF(line_separator);
    BUF_PRINTF("-----------------");
    BUF_PRINTF(line_separator);
  }
  BUF_PRINTF("Trx Cur SQL:");
  BUF_PRINTF(line_separator);
  BUF_PRINTF("-----------------");
  BUF_PRINTF(line_separator);
  for (int i = 0; i < cur_sql_diag_array_.count(); i++) {
    if (BASE != cur_sql_diag_array_[i].base_.type_) {
      FORMAT_PRINT(cur_sql_diag_array_[i]);
    }
  }
  return pos;
}

ObRouteDiagnosis::ObRouteDiagnosis() : 
  is_support_explain_route_(false), level_(ZERO),
  cur_sql_diag_array_(ObModIds::OB_PROXY_ROUTE_DIAGNOSIS_SE_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
  trx_1st_req_buf_(NULL), trx_1st_req_buf_len_(0), allocator_(ObModIds::OB_PROXY_ROUTE_DIAGNOSIS_STRING){
  cur_sql_diag_array_.prepare_allocate(BASIC_NUM);
}
ObRouteDiagnosis::~ObRouteDiagnosis() {
  if (OB_NOT_NULL(trx_1st_req_buf_) && 0 != trx_1st_req_buf_len_) {
    op_fixed_mem_free(trx_1st_req_buf_, trx_1st_req_buf_len_);
    trx_1st_req_buf_ = NULL;
    trx_1st_req_buf_len_ = 0;
  }
  cur_sql_diag_array_.reset();
  allocator_.reset();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
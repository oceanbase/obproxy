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
#include "share/part/ob_part_desc.h"
#include "lib/rowid/ob_urowid.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
void deep_copy_string(ObIAllocator *alloc, ObString &src, ObString &dest) {
  char *c = NULL;
  const static char *fail = "fail_to_alloc_memory";
  size_t len = src.length();
  if (OB_ISNULL(alloc)) {
    LOG_WDIAG("can not alloc mem for route diagnosis point because allocator is NULL");
    dest.assign_ptr(fail, (ObString::obstr_size_t) strlen(fail));
  } else if (len != 0 && NULL == (c = (char*) alloc->alloc(len))) {
    LOG_WDIAG("fail to alloc mem for route diagnosis point", K(len));
    dest.assign_ptr(fail, (ObString::obstr_size_t) strlen(fail));
  } else {
    MEMCPY(c, src.ptr(), len);
    dest.assign_ptr(c, (ObString::obstr_size_t) len);
  }
}
ObDiagnosisLevel get_type_level(ObDiagnosisType type) {
  ObDiagnosisLevel lv;
  switch(type) {
    case SQL_PARSE:
    case ROUTE_INFO:
    case LOCATION_CACHE_LOOKUP:
    case ROUTE_POLICY:
    case CONGESTION_CONTROL:
    case HANDLE_RESPONSE:
    case RETRY:
      lv = DIAGNOSIS_LEVEL_ONE;
      break;

    case ROUTINE_ENTRY_LOOKUP_DONE:
    case TABLE_ENTRY_LOOKUP_DONE:
    case PARTITION_ID_CALC_DONE:
    case PARTITION_ENTRY_LOOKUP_DONE:
    case TABLE_ENTRY_LOOKUP_START:
    case PARTITION_ID_CALC_START:
      lv = DIAGNOSIS_LEVEL_TWO;
      break;

    case FETCH_TABLE_RELATED_DATA:
    case EXPR_PARSE:
    case CALC_ROWID:
    case RESOLVE_EXPR:
    case CALC_PARTITION_ID:
      lv = DIAGNOSIS_LEVEL_THREE;
      break;

    case RESOLVE_TOKEN:
      lv = DIAGNOSIS_LEVEL_FOUR;
      break;

    default:
      lv = DIAGNOSIS_LEVEL_ZERO;
      LOG_WDIAG("log unexpected diagnosis type", K(type));
      break;
  }
  return lv;
}
static const char *get_diagnosis_type_name(const ObDiagnosisType type) {
  const char *name = NULL;

  switch(type) {
    case BASE_ROUTE_DIAGNOSIS_TYPE:
      name = "BASE_ROUTE_DIAGNOSIS_TYPE";
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

    case ROUTINE_ENTRY_LOOKUP_DONE:
      name = "ROUTINE_ENTRY_LOOKUP_DONE";
      break;

    case TABLE_ENTRY_LOOKUP_DONE:
      name = "TABLE_ENTRY_LOOKUP_DONE";
      break;

    case PARTITION_ID_CALC_DONE:
      name = "PARTITION_ID_CALC_DONE";
      break;

    case PARTITION_ENTRY_LOOKUP_DONE:
      name = "PARTITION_ENTRY_LOOKUP_DONE";
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

    case TABLE_ENTRY_LOOKUP_START:
      name = "TABLE_ENTRY_LOOKUP_START";
      break;

    case FETCH_TABLE_RELATED_DATA:
      name = "FETCH_TABLE_RELATED_DATA";
      break;

    case EXPR_PARSE:
      name = "EXPR_PARSE";
      break;

    case CALC_ROWID:
      name = "CALC_ROWID";
      break;

    case RESOLVE_TOKEN:
      name = "RESOLVE_TOKEN";
      break;

    case RESOLVE_EXPR:
      name = "RESOLVE_EXPR";
      break;

    case CALC_PARTITION_ID:
      name = "CALC_PARTITION_ID";
      break;

    case PARTITION_ID_CALC_START:
      name = "PARTITION_ID_CALC_START";
      break;

    default:
      name = "unknown diag type";
      break;
  }
  return name;
}
static const char *get_route_info_type_name(const ObRouteInfoType type)
{
  const char *name = NULL;
  switch(type) {
    case ObRouteInfoType::INVALID:
      name = "INVALID";
      break;

    case ObRouteInfoType::USE_GLOBAL_INDEX:
      name = "USE_GLOBAL_INDEX";
      break;

    case ObRouteInfoType::USE_OBPROXY_ROUTE_ADDR:
      name = "USE_OBPROXY_ROUTE_ADDR";
      break;

    case ObRouteInfoType::USE_CURSOR:
      name = "USE_CURSOR";
      break;

    case ObRouteInfoType::USE_PIECES_DATA:
      name = "USE_PIECES_DATA";
      break;

    case ObRouteInfoType::USE_CONFIG_TARGET_DB:
      name = "USE_CONFIG_TARGET_DB";
      break;

    case ObRouteInfoType::USE_COMMENT_TARGET_DB:
      name = "USE_COMMENT_TARGET_DB";
      break;

    case ObRouteInfoType::USE_TEST_SVR_ADDR:
      name = "USE_TEST_SVR_ADDR";
      break;

    case ObRouteInfoType::USE_LAST_SESSION:
      name = "USE_LAST_SESSION";
      break;

    case ObRouteInfoType::USE_LOCK_SESSION:
      name = "USE_LOCK_SESSION";
      break;

    case ObRouteInfoType::USE_CACHED_SESSION:
      name = "USE_CACHED_SESSION";
      break;

    case ObRouteInfoType::USE_SINGLE_LEADER:
      name = "USE_SINGLE_LEADER";
      break;

    case ObRouteInfoType::USE_PARTITION_LOCATION_LOOKUP:
      name = "USE_PARTITION_LOCATION_LOOKUP";
      break;

    case ObRouteInfoType::USE_COORDINATOR_SESSION:
      name = "USE_COODINATOR_SESSION";
      break;

    case ObRouteInfoType::USE_ROUTE_POLICY:
      name = "USE_ROUTE_POLICY";
      break;

    default:
      name = "unkonwn route type";
  }
  return name;
}
static const char *get_retry_type_name(const ObRetryType type) {
  const char *name = NULL;
  switch(type) {
    case INVALID:
      name = "INVALID";
      break;

    case CMNT_TARGET_DB_SERVER:
      name = "CMNT_TARGET_DB_SERVER";
      break;

    case CONF_TARGET_DB_SERVER:
      name = "CONF_TARGET_DB_SERVER";
      break;

    case TEST_SERVER_ADDR:
      name = "TEST_SERVER_ADDR";
      break;

    case PARTITION_LOCATION_LOOKUP:
      name = "PARTITION_LOCATION_LOOKUP";
      break;

    case REROUTE:
      name = "REROUTE";
      break;

    case NOT_RETRY:
      name = "NOT_RETRY";
      break;

    default:
      name = "unknown retry type";
  }
  return name;
}

int64_t ObDiagnosisPoint::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  int64_t pos = 0;
  switch(base_.type_) {
    case SQL_PARSE:
      pos += sql_parse_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case ROUTE_INFO:
      pos += route_info_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case LOCATION_CACHE_LOOKUP:
      pos += location_cache_lookup_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case ROUTINE_ENTRY_LOOKUP_DONE:
      pos += routine_entry_lookup_done_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case TABLE_ENTRY_LOOKUP_DONE:
      pos += table_entry_lookup_done_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case PARTITION_ID_CALC_DONE:
      pos += partition_id_calc_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case PARTITION_ENTRY_LOOKUP_DONE:
      pos += partition_entry_lookup_done_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case ROUTE_POLICY:
      pos += route_policy_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case CONGESTION_CONTROL:
      pos += congestion_control_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case HANDLE_RESPONSE:
      pos += handle_response_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case RETRY:
      pos += retry_connection_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case FETCH_TABLE_RELATED_DATA:
      pos += fetch_table_related_data_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case EXPR_PARSE:
      pos += expr_parse_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case CALC_ROWID:
      pos += calc_rowid_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case RESOLVE_TOKEN:
      pos += resolve_token_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case RESOLVE_EXPR:
      pos += resolve_expr_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    case CALC_PARTITION_ID:
      pos += calc_partition_id_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
    default:
      pos += base_.diagnose(buf + pos, buf_len - pos, warn, next_line);
      break;
  }
  return pos;
}
int64_t ObDiagnosisPoint::to_string(char *buf, const int64_t buf_len) const
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
    case ROUTINE_ENTRY_LOOKUP_DONE:
      pos += routine_entry_lookup_done_.to_string(buf + pos, buf_len - pos);
      break;
    case TABLE_ENTRY_LOOKUP_DONE:
      pos += table_entry_lookup_done_.to_string(buf + pos, buf_len - pos);
      break;
    case PARTITION_ID_CALC_DONE:
      pos += partition_id_calc_.to_string(buf + pos, buf_len - pos);
      break;
    case PARTITION_ENTRY_LOOKUP_DONE:
      pos += partition_entry_lookup_done_.to_string(buf + pos, buf_len - pos);
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
    case FETCH_TABLE_RELATED_DATA:
      pos += fetch_table_related_data_.to_string(buf + pos, buf_len - pos);
      break;
    case EXPR_PARSE:
      pos += expr_parse_.to_string(buf + pos, buf_len - pos);
      break;
    case CALC_ROWID:
      pos += calc_rowid_.to_string(buf + pos, buf_len - pos);
      break;
    case RESOLVE_TOKEN:
      pos += resolve_token_.to_string(buf + pos, buf_len - pos);
      break;
    case RESOLVE_EXPR:
      pos += resolve_expr_.to_string(buf + pos, buf_len - pos);
      break;
    case CALC_PARTITION_ID:
      pos += calc_partition_id_.to_string(buf + pos, buf_len - pos);
      break;
    default:
      pos += base_.to_string(buf + pos, buf_len - pos);
      break;
  }
  return pos;
}

int64_t ObDiagnosisBase::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(warn);
  UNUSED(next_line);
  return 0;
}

int64_t ObDiagnosisBase::to_string(char *buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return 0;
}

void ObDiagnosisBase::reset() {
  ret_ = 0;
  type_ = BASE_ROUTE_DIAGNOSIS_TYPE;
  alloc_ = NULL;
}

int64_t ObDiagnosisSqlParse::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (table_.empty()) {
      DIAGNOSE_INFO("Maybe can not get location cache of the query table because table name is empty");
    } else if (sql_.length() == get_global_proxy_config().request_buffer_length) {
      DIAGNOSE_INFO("Partition keys or table may not be completely parsed because it execeed the maxium request buffer length");
    }
  );
}
int64_t ObDiagnosisSqlParse::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("cmd", ObProxyParserUtils::get_sql_cmd_name(sql_cmd_));
    J_COMMA();
    J_KV(K_(table));
  );
}

void ObDiagnosisSqlParse::reset() {
  this->ObDiagnosisBase::reset();
  sql_.reset();
  table_.reset();
  sql_cmd_ = obmysql::OB_MYSQL_COM_SLEEP;
}
int64_t ObDiagnosisLocationCacheLookup::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(warn);
  UNUSED(next_line);
  return 0;
}
int64_t ObDiagnosisLocationCacheLookup::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("mode", obutils::ObProxyConfig::get_routing_mode_str(mode_));
    if (!need_partition_location_lookup_) {
      J_COMMA();
      J_KV(K_(need_partition_location_lookup));
    }
  );
}

void ObDiagnosisLocationCacheLookup::reset() {
  this->ObDiagnosisBase::reset();
  mode_ = obutils::OB_STANDARD_ROUTING_MODE;
  need_partition_location_lookup_ = false;
}

int64_t ObDiagnosisRoutineEntryLookupDone::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (!is_lookup_succ_) {
      DIAGNOSE_INFO("No available entry because routine entry lookup failed");
    } else if (entry_state_ == ObRouteEntry::DIRTY) {
      DIAGNOSE_WARN("Unexpected dirty entry");
    } else if (routine_sql_.empty()) {
      DIAGNOSE_WARN("Can not calculate partition location because without valid SQL stmt");
    }
  );
}

int64_t ObDiagnosisRoutineEntryLookupDone::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (entry_state_ == ObRouteEntry::DELETED) {
      J_KV("info", "entry not exists");
    } else {
      J_KV(K_(routine_sql));
      if (!is_lookup_succ_) {
        J_COMMA();
        J_KV(K_(is_lookup_succ));
      }
      J_COMMA();
      J_KV(K_(entry_from_remote));
    }
  );
}

void ObDiagnosisRoutineEntryLookupDone::reset() {
  this->ObDiagnosisBase::reset();
  routine_sql_.reset();
  entry_from_remote_ = false;
  is_lookup_succ_ = false;
  entry_state_ = ObRouteEntry::BORN;
}
int64_t ObDiagnosisTableEntryLookupDone::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (!is_lookup_succ_ || OB_ISNULL(table_entry_)) {
      DIAGNOSE_INFO("No available entry because table entry lookup failed");
    } else if (table_entry_->get_entry_state() == ObRouteEntry::DIRTY) {
      DIAGNOSE_WARN("Unexpected dirty entry");
    } else {
      if (table_entry_->get_part_num() == 1) {
        DIAGNOSE_INFO("Non-partition table will be routed by ROUTE_POLICY");
      }
      if (table_entry_->has_dup_replica()) {
        DIAGNOSE_INFO("Querying duplicated table");
      }
    }
  );
}
int64_t ObDiagnosisTableEntryLookupDone::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (OB_ISNULL(table_entry_) || table_entry_->get_entry_state() == ObRouteEntry::DELETED) {
      J_KV(K_(is_lookup_succ), K_(entry_from_remote));
    } else {
      BUF_PRINTF("table:\"%.*s\"",
                 table_entry_->get_table_name().length(),
                 table_entry_->get_table_name().ptr());
      J_COMMA();
      BUF_PRINTF("table_id:\"%ld\"", table_entry_->get_table_id());
      J_COMMA();
      BUF_PRINTF("table_type:\"%s\"", ob_table_type_str(table_entry_->get_table_type()));
      if (table_entry_->get_part_num() > 1) {
        J_COMMA();
        J_KV("partition_num", table_entry_->get_part_num());
      }
      if (table_entry_->has_dup_replica()) {
        J_COMMA();
        J_KV("has_dup_replica", table_entry_->has_dup_replica());
      }
      if (!entry_from_remote_) {
        J_COMMA();
        J_KV(K_(entry_from_remote));
      }
      if (!is_lookup_succ_) {
        J_COMMA();
        J_KV(K_(is_lookup_succ));
      }
    }
  );
}

void ObDiagnosisTableEntryLookupDone::reset() {
  this->ObDiagnosisBase::reset();
  entry_from_remote_ = false;
  is_lookup_succ_ = false;
  if (OB_NOT_NULL(table_entry_)) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }
}
int64_t ObDiagnosisPartIDCalcDone::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV(K_(partition_id));
    J_COMMA();
    BUF_PRINTF("level:%d", static_cast<int>(level_));
    if (!part_name_.empty()) {
      J_COMMA();
      J_KV("part_name", part_name_);
    } else {
      if (level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE) {
        if (part_idx_ != -1) {
          J_COMMA();
          BUF_PRINTF("partitions:\"(p%ld)\"", part_idx_);
        }
      } else if (level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
        if (part_idx_ != -1 && sub_part_idx_ != -1) {
          J_COMMA();
          BUF_PRINTF("partitions:\"(p%ldsp%ld)\"", part_idx_, sub_part_idx_);
        } else if (part_idx_ != -1 && sub_part_idx_ == -1) {
          J_COMMA();
          BUF_PRINTF("partitions:\"(p%ld)\"", part_idx_);
        }
      }
    }
    if (!parse_sql_.empty()) {
      J_COMMA();
      J_KV(K_(parse_sql));
    }
  );
}
int64_t ObDiagnosisPartIDCalcDone::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (!part_name_.empty() && partition_id_ == -1) {
      DIAGNOSE_WARN("Fail to use specified first partition name(%.*s) to get partition id", part_name_.length(), part_name_.ptr());
    } else if (!part_name_.empty() && partition_id_ != -1) {
      DIAGNOSE_INFO("Will route to specified partition(%.*s) id(%ld)", part_name_.length(), part_name_.ptr(), partition_id_);
    } else if (part_name_.empty() && part_idx_ == -1) {
      DIAGNOSE_WARN("Fail to use partition key value to calculate first part idx");
    } else if (part_name_.empty() && sub_part_idx_ == -1 && part_idx_ != -1 && level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      DIAGNOSE_WARN("Fail to use partition key value to calculate sub part idx");
    }
  );
}

void ObDiagnosisPartIDCalcDone::reset() {
  this->ObDiagnosisBase::reset();
  parse_sql_.reset();
  part_name_.reset();
  part_idx_ = 0;
  sub_part_idx_ = 0;
  partition_id_ = 0;
  level_ = share::schema::PARTITION_LEVEL_ZERO;
}

int64_t ObDiagnosisPartEntryLookupDone::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (!is_lookup_succ_) {
      DIAGNOSE_INFO("No available entry because partition entry lookup failed. Will routed by ROUTE_POLICY or cached server session(ROUTE_INFO)");
    } else if (entry_state_ == ObRouteEntry::DIRTY) {
      DIAGNOSE_WARN("Unexpected entry dirty");
    } else if (!leader_.is_valid()) {
      DIAGNOSE_WARN("Unexpected invalid leader replica");
    }
  )
}

int64_t ObDiagnosisPartEntryLookupDone::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (entry_state_ == ObRouteEntry::DELETED) {
      J_KV("info", "entry not exists");
    } else {
      if (leader_.server_.is_valid()) {
        J_KV("leader", leader_.server_);
      } else {
        J_KV("leader", "invalid leader");
      }
      if (has_dup_replica_) {
        J_COMMA();
        J_KV(K_(has_dup_replica));
      }
      if (!entry_from_remote_) {
        J_COMMA();
        J_KV(K_(entry_from_remote));
      }
    }
  );
}

void ObDiagnosisPartEntryLookupDone::reset() {
  this->ObDiagnosisBase::reset();
  entry_from_remote_ = false;
  is_lookup_succ_ = false;
  has_dup_replica_ = false;
  entry_state_ = ObRouteEntry::BORN;
  leader_.reset();
}
int64_t ObDiagnosisRouteInfo::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    char svr_buf[1 << 7] { 0 };
    svr_addr_.to_plain_string(svr_buf, 1 << 7);
    if (!svr_addr_.is_valid() &&
        (ObRouteInfoType::USE_CURSOR == route_info_type_ ||
         ObRouteInfoType::USE_PIECES_DATA == route_info_type_ ||
         ObRouteInfoType::USE_COMMENT_TARGET_DB == route_info_type_ ||
         ObRouteInfoType::USE_CONFIG_TARGET_DB == route_info_type_ ||
         ObRouteInfoType::USE_LAST_SESSION == route_info_type_ ||
         ObRouteInfoType::USE_COORDINATOR_SESSION == route_info_type_ ||
         ObRouteInfoType::USE_CACHED_SESSION == route_info_type_ ||
         ObRouteInfoType::USE_SINGLE_LEADER == route_info_type_)) {
      DIAGNOSE_WARN("Unexpected invalid server addr")
    } else if (ObRouteInfoType::USE_CURSOR == route_info_type_ || ObRouteInfoType::USE_PIECES_DATA == route_info_type_) {
      DIAGNOSE_INFO("Will route to starting server(%s) because use streaming data transfer", svr_buf);
    } else if (ObRouteInfoType::USE_CONFIG_TARGET_DB == route_info_type_ || ObRouteInfoType::USE_COMMENT_TARGET_DB == route_info_type_) {
      if (!net::ops_is_ip_loopback(svr_addr_)) {
        DIAGNOSE_INFO("Will route to target db server(%s)", svr_buf);
      } else {
        DIAGNOSE_INFO("Will route to target db server(%s) and it's loopback addr wouldn't do congestion control", svr_buf);
      }
    } else if (ObRouteInfoType::USE_PARTITION_LOCATION_LOOKUP == route_info_type_) {
      DIAGNOSE_INFO("Will do table partition location lookup to decide which OBServer to route to")
    } else if (ObRouteInfoType::USE_ROUTE_POLICY == route_info_type_) {
      DIAGNOSE_INFO("Will use route policy to decide which OBServer to route to")
    } else if (ObRouteInfoType::USE_GLOBAL_INDEX == route_info_type_) {
      DIAGNOSE_INFO("Will use global index table name as real table name to route")
    } else if (ObRouteInfoType::USE_TEST_SVR_ADDR == route_info_type_) {
      DIAGNOSE_INFO("Do NOT use deprecated feature test_server_addr")
    } else {
      if (ObRouteInfoType::USE_COORDINATOR_SESSION == route_info_type_) {
        if (trans_specified_) {
          DIAGNOSE_INFO("Will route to transaction coordinator(%s) because current query for session temporary table but last session not equals to coordinator", svr_buf);
        } else {
          DIAGNOSE_INFO("Will route to transaction coordinator(%s) because current query unable to be freely routed", svr_buf);
        }
      } else if (ObRouteInfoType::USE_LAST_SESSION == route_info_type_) {
        if (depent_func_) {
          DIAGNOSE_INFO("Will route to last connected server(%s) because 'select found_rows()/select row_count()/select connection_id()/show warnings/show errors/show trace' command and these queries depend on the last query session", svr_buf)
        } else if (trans_specified_) {
          DIAGNOSE_INFO("Will route to last connected server(%s) because query for session temporary table", svr_buf);
        } else if (in_transaction_) {
          DIAGNOSE_INFO("Will route to last connected server(%s) because current query in transaction", svr_buf);
        } else {
          DIAGNOSE_INFO("Will route to last connected server(%s)", svr_buf);
        }
      } else if (ObRouteInfoType::USE_CACHED_SESSION == route_info_type_) {
        DIAGNOSE_INFO("Will route to cached connected server(%s)", svr_buf);
      } else if (ObRouteInfoType::USE_SINGLE_LEADER == route_info_type_) {
        DIAGNOSE_INFO("Will route to tenant's single leader node(%s)", svr_buf);
      }
    }
  )
}
int64_t ObDiagnosisRouteInfo::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("route_info_type", get_route_info_type_name(route_info_type_));
    if (svr_addr_.is_valid()) {
      J_COMMA();
      BUF_PRINTF("svr_addr:");
      J_QUOTE();
      pos += svr_addr_.to_plain_string(buf + pos, buf_len - pos);
      J_QUOTE();
    }
    if (in_transaction_) {
      J_COMMA();
      J_KV(K_(in_transaction));
    }
    if (depent_func_) {
      J_COMMA();
      J_KV(K_(depent_func));
    }
    if (trans_specified_) {
      J_COMMA();
      J_KV(K_(trans_specified));
    }
  );
}

void ObDiagnosisRouteInfo::reset() {
  this->ObDiagnosisBase::reset();
  route_info_type_ = ObRouteInfoType::INVALID;
  svr_addr_.reset();
  in_transaction_ = false;
  depent_func_ = false;
  trans_specified_ = false;
}

int64_t ObDiagnosisRoutePolicy::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    ObString policy = get_route_policy_enum_string(route_policy_);
    ObString type = get_route_type_string(chosen_route_type_);
    const static char *route_type_prefix = "ROUTE_TYPE_";
    type.assign(type.ptr() + STRLEN(route_type_prefix), type.length() - static_cast<ObString::obstr_size_t>(STRLEN(route_type_prefix)));
    char svr_buf[1 << 7] { 0 };
    if (replica_.is_valid()) {
      replica_.server_.ip_port_to_string(svr_buf, 1 << 7);
    }
    if (proxy_idc_name_.empty() && chosen_route_type_ != ROUTE_TYPE_LEADER) {
      DIAGNOSE_INFO("All OBServers are treated as the SAME_IDC with OBProxy because 'proxy_idc_name' is not configured")
    }
    if (need_use_dup_replica_) {
      DIAGNOSE_INFO("Strong read duplicated table using route policy %.*s", policy.length(), policy.ptr());
    }
    if (route_policy_ != MAX_ROUTE_POLICY_COUNT && chosen_route_type_ == ROUTE_TYPE_LEADER) {
      DIAGNOSE_INFO("Will route to table's partition leader replica(%s) using route policy %.*s because query for %s read", svr_buf, policy.length(), policy.ptr(), get_consistency_level_str(trans_consistency_level_));
    } else if (chosen_route_type_ == ROUTE_TYPE_LEADER) {
      DIAGNOSE_INFO("Will route to table's partition leader replica(%s) using non route policy because query for %s read", svr_buf, get_consistency_level_str(trans_consistency_level_));
    } else if (!proxy_primary_zone_.empty() && !need_use_dup_replica_ && chosen_route_type_ == ROUTE_TYPE_PRIMARY_ZONE) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using proxy primary zone(%.*s) and route policy %.*s",
                    type.length(), type.ptr(), svr_buf, proxy_primary_zone_.length(), proxy_primary_zone_.ptr(), policy.length(), policy.ptr());
    } else if (!tenant_primary_zone_.empty() && !need_use_dup_replica_ && chosen_route_type_ == ROUTE_TYPE_PRIMARY_ZONE) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using tenant primary zone(%.*s) and route policy %.*s",
                    type.length(), type.ptr(), svr_buf, tenant_primary_zone_.length(), tenant_primary_zone_.ptr(), policy.length(), policy.ptr());
    } else if (route_policy_ == proxy_route_policy_ && !readonly_zone_exsits_ && trans_consistency_level_ == WEAK) {
      // Use proxy route policy because READONLY zone not exists and query for weak read
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using proxy route policy %.*s because query for WEAK read", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr());
    } else if (route_policy_ == MERGE_IDC_ORDER && !readonly_zone_exsits_ && trans_consistency_level_ == STRONG) {
      // Use default route policy sice READONLY zone not exists and query for strong read
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using default route policy %.*s because query for STRONG read", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr());
    } else if (route_policy_ == session_route_policy_ && readonly_zone_exsits_ && trans_consistency_level_ == WEAK) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using session route policy %.*s because READONLY zone exists and query for WEAK read", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr())
    } else if (route_policy_ == MERGE_IDC_ORDER && readonly_zone_exsits_ && !request_support_readonly_zone_) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using default route policy %.*s because READONLY zone exists but query not support to route to READONLY zone", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr());
    } else if (route_policy_ == session_route_policy_ && readonly_zone_exsits_ && request_support_readonly_zone_ &&
               session_consistency_level_ == WEAK) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using session route policy %.*s because READONLY zone exists and query for WEAK read", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr())
    } else if (route_policy_ == ONLY_READWRITE_ZONE && readonly_zone_exsits_ &&& request_support_readonly_zone_ &&
               session_consistency_level_ == STRONG) {
      DIAGNOSE_INFO("Will route to routing type(%.*s) matched replica(%s) using session route policy %.*s because READONLY zone exists and query for STRONG read", type.length(), type.ptr(), svr_buf, policy.length(), policy.ptr())
    }
  );
}

int64_t ObDiagnosisRoutePolicy::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (pos != 0) {
      J_COMMA();
    }
    J_KV("route_policy", get_route_policy_enum_string(opt_route_policy_));
    if (chosen_route_type_ == ROUTE_TYPE_LEADER) {
      J_COMMA();
      J_KV("chosen_route_type", get_route_type_string(chosen_route_type_));
    } else if (!replica_.is_valid()) {
      J_COMMA();
      J_KV("chosen_server", "Invalid");
    } else {
      J_COMMA();
      J_KV("replica", replica_.server_);
      J_COMMA();
      J_KV("idc_type", get_idc_type_string(chosen_server_.idc_type_));
      J_COMMA();
      J_KV("zone_type", zone_type_to_str(chosen_server_.zone_type_));
      J_COMMA();
      J_KV("role", replica_.get_role_type_string(replica_.role_));
      J_COMMA();
      J_KV("type", replica_.get_replica_type_string(replica_.replica_type_));
      if (replica_.is_dup_replica()) {
        J_COMMA();
        J_KV("is_dup_replica", replica_.is_dup_replica_);
      }
      if (chosen_server_.is_merging_) {
        J_COMMA();
        J_KV("is_merging", chosen_server_.is_merging_);
      }
      if (chosen_server_.is_partition_server_) {
        J_COMMA();
        J_KV("is_partition_server", chosen_server_.is_partition_server_);
      }
      if (chosen_server_.is_force_congested_) {
        J_COMMA();
        J_KV("is_force_congested", chosen_server_.is_force_congested_);
      }
      if (!proxy_primary_zone_.empty()) {
        J_COMMA();
        J_KV(K_(proxy_primary_zone));
      }
      if (chosen_route_type_ == ROUTE_TYPE_PRIMARY_ZONE && (!tenant_primary_zone_.empty() || !proxy_primary_zone_.empty())) {
        J_COMMA();
        J_KV("chosen_route_type", get_route_type_string(ROUTE_TYPE_PRIMARY_ZONE));
        if (!tenant_primary_zone_.empty()) {
          J_COMMA();
          J_KV(K_(tenant_primary_zone));
        }
      } else {
        J_COMMA();
        J_KV("chosen_route_type", get_route_type_string(chosen_route_type_));
        if (trans_consistency_level_ != INVALID_CONSISTENCY) {
          J_COMMA();
          J_KV("trans_consistency", get_consistency_level_str(trans_consistency_level_));
        }
        if (session_consistency_level_ != INVALID_CONSISTENCY) {
          J_COMMA();
          J_KV("session_consistency", get_consistency_level_str(session_consistency_level_));
        }
        if (!proxy_idc_name_.empty()) {
          J_COMMA();
          J_KV(K_(proxy_idc_name));
        }
      }
    }
  );
}

void ObDiagnosisRoutePolicy::reset() {
  this->ObDiagnosisBase::reset();
  need_use_dup_replica_ = false;
  need_check_merge_status_ = false;
  readonly_zone_exsits_ = false;
  request_support_readonly_zone_ = false;
  proxy_idc_name_.reset();
  tenant_primary_zone_.reset();
  proxy_primary_zone_.reset();
  session_consistency_level_ = INVALID_CONSISTENCY;
  trans_consistency_level_ = INVALID_CONSISTENCY;
  proxy_route_policy_ = MAX_ROUTE_POLICY_COUNT;
  session_route_policy_ = MAX_ROUTE_POLICY_COUNT;
  route_policy_ = MAX_ROUTE_POLICY_COUNT;
  opt_route_policy_ = MAX_ROUTE_POLICY_COUNT;
  chosen_server_.reset();
  chosen_route_type_ = ROUTE_TYPE_MAX;
  replica_.reset();
}

int64_t ObDiagnosisCongestionControl::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (need_congestion_lookup_) {
      if (!lookup_success_) {
        DIAGNOSE_WARN("Will disconnect because fail to get black/white list");
      } else if (!entry_exist_) {
        char svr_buf[1 << 7] { 0 };
        svr_addr_.to_plain_string(svr_buf, 1 << 7);
        DIAGNOSE_WARN("Will retry other replica because this replica(%s) not exists in black/white list", svr_buf);
      } else if (force_retry_congested_) {
        DIAGNOSE_WARN("Will force to use congested replica");
      } else if (dead_congested_ || detect_congested_ || alive_congested_) {
        char svr_buf[1 << 7] { 0 };
        svr_addr_.to_plain_string(svr_buf, 1 << 7);
        DIAGNOSE_WARN("Will retry because this replica(%s) is in black list", svr_buf);
      }
    } else {
      char svr_buf[1 << 7] { 0 };
      svr_addr_.to_plain_string(svr_buf, 1 << 7);
      DIAGNOSE_INFO("This replica(%s) is no need to pass congestion control", svr_buf);
    }
  )
}

int64_t ObDiagnosisCongestionControl::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (svr_addr_.is_valid()) {
      BUF_PRINTF("svr_addr:");
      J_QUOTE();
      pos += svr_addr_.to_plain_string(buf + pos, buf_len - pos);
      J_QUOTE();
    } else {
      J_KV("svr_addr", "Invalid");
    }
    if (force_retry_congested_) {
      J_COMMA();
      J_KV(K_(force_retry_congested));
    }
    if (need_congestion_lookup_) {
      if (alive_congested_) {
        J_COMMA();
        J_KV(K_(alive_congested));
      } else if (dead_congested_) {
        J_COMMA();
        J_KV(K_(dead_congested));
      } else if (detect_congested_) {
        J_COMMA();
        J_KV(K_(detect_congested));
      }
      if (!entry_exist_) {
        J_COMMA();
        J_KV(K_(entry_exist));
      }
      if (!lookup_success_) {
        J_COMMA();
        J_KV(K_(lookup_success));
      }
    } else {
      J_COMMA();
      J_KV(K_(need_congestion_lookup));
    }

  );
}

void ObDiagnosisCongestionControl::reset() {
  this->ObDiagnosisBase::reset();
  alive_congested_ = false;
  dead_congested_ = false;
  detect_congested_ = false;
  force_retry_congested_ = false;
  need_congestion_lookup_ = false;
  lookup_success_ = false;
  entry_exist_ = false;
  svr_addr_.reset();
}

int64_t ObDiagnosisHandleResponse::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (error_ != 0) {
      DIAGNOSE_INFO("OBServer respsonds with error code. Please check the manul")
    }
  )
}

int64_t ObDiagnosisHandleResponse::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    BUF_PRINTF("is_parititon_hit:\"%s\"", STR_BOOL(is_partition_hit_));
    J_COMMA();
    BUF_PRINTF("send_action:\"%s\"", ObMysqlTransact::get_send_action_name(send_action_));
    J_COMMA();
    BUF_PRINTF("state:\"%s\"", ObMysqlTransact::get_server_state_name(state_));
    if (error_ != ObMysqlTransact::MIN_RESP_ERROR) {
      J_COMMA();
      BUF_PRINTF("error:\"%s\"", ObMysqlTransact::get_server_resp_error_name(error_));
    });
}

void ObDiagnosisHandleResponse::reset() {
  this->ObDiagnosisBase::reset();
  is_partition_hit_ = false;
  send_action_ = ObMysqlTransact::SERVER_SEND_NONE;
  state_ = ObMysqlTransact::STATE_UNDEFINED;
  error_ = ObMysqlTransact::MIN_RESP_ERROR;
}

int64_t ObDiagnosisRetry::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(warn);
  UNUSED(next_line);
  return 0;
}

int64_t ObDiagnosisRetry::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("retry_status", ObMysqlTransact::get_retry_status_string(retry_status_),
         "retry_type", get_retry_type_name(retry_type_));
    if (retry_status_ == ObMysqlTransact::FOUND_EXISTING_ADDR) {
      J_COMMA();
      BUF_PRINTF("retry_addr:\"");
      pos += retry_addr_.to_plain_string(buf + pos, buf_len - pos);
      BUF_PRINTF("\"");
    }
    if (retry_status_ != ObMysqlTransact::NO_NEED_RETRY) {
      J_COMMA();
      J_KV(K_(attempts));
    }
  )
}

void ObDiagnosisRetry::reset()
{
  this->ObDiagnosisBase::reset();
  attempts_ = 0;
  retry_status_ = ObMysqlTransact::NO_NEED_RETRY;
  retry_type_ = INVALID;
  retry_addr_.reset();
}

int64_t ObDiagnosisFetchTableRelatedData::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (resp_error_ != 0) {
      if (fail_at_ == LOOKUP_TABLE_ENTRY_STATE) {
        DIAGNOSE_WARN("Fail to query __all_virtual_proxy_schema maybe table not exist with resp error(%ld)", resp_error_);
      } else if (fail_at_ == LOOKUP_PART_INFO_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to query __all_virtual_proxy_partition_info with  resp error(%ld)", resp_error_);
      } else if (fail_at_ == LOOKUP_FIRST_PART_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to query __all_virtual_proxy_partition with resp error(%ld)", resp_error_);
      } else if (fail_at_ == LOOKUP_SUB_PART_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to query __all_virtual_proxy_sub_partition with resp error(%ld)", resp_error_);
      } else if (fail_at_ == LOOKUP_BINLOG_ENTRY_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to query 'show binlog server' with resp error(%ld)", resp_error_);
      } else if (fail_at_ == LOOKUP_DONE_STATE) {
        DIAGNOSE_WARN("Unexpected state");
      }
    } else if (ret_ != 0) {
      if (fail_at_ == LOOKUP_TABLE_ENTRY_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to handle result of querying __all_virtual_proxy_schema with error(%d)", ret_);
      } else if (fail_at_ == LOOKUP_PART_INFO_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to handle result of querying __all_virtual_proxy_partition_info with error(%d)", ret_);
      } else if (fail_at_ == LOOKUP_FIRST_PART_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to handle result of querying __all_virtual_proxy_partition with error(%d)", ret_);
      } else if (fail_at_ == LOOKUP_SUB_PART_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to handle result of querying __all_virtual_proxy_sub_partition with error(%d)", ret_);
      } else if (fail_at_ == LOOKUP_BINLOG_ENTRY_STATE) {
        DIAGNOSE_WARN("Unexpected! Fail to handle result of querying 'show binlog server' with error(%d)", ret_);
      } else if (fail_at_ == LOOKUP_DONE_STATE) {
        DIAGNOSE_WARN("Unexpected state");
      }
    }
  );
}
int64_t ObDiagnosisFetchTableRelatedData::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (resp_error_ != 0) {
      J_KV(K_(resp_error));
      J_COMMA();
    }
    if (resp_error_ || ret_ != 0) {
      J_KV("fail_at", ObTableEntryCont::get_state_name(fail_at_));
      J_COMMA();
    }
    if (OB_NOT_NULL(table_entry_)) {
      ObProxyPartInfo *part = NULL;
      if (table_entry_->get_part_num() > 1 &&
          !table_entry_->is_dummy_entry() &&
          (part = table_entry_->get_part_info()) != NULL) {
        J_KV("part_level", part->get_part_level());
        if (part->has_unknown_part_key()) {
          J_COMMA();
          J_KV("has_unknown_part_key", part->has_unknown_part_key());
        }
        if (!part->get_part_columns().empty()) {
          J_COMMA();
          BUF_PRINTF("part_expr:\"");
          for (int i = 0; i < part->get_part_columns().count(); i++) {
            if (i != 0) {
              J_COMMA();
            }
            BUF_PRINTF("%.*s", part->get_part_columns().at(i).length(), part->get_part_columns().at(i).ptr());
          }
          BUF_PRINTF("\"");
        }
        if (!part->get_sub_part_columns().empty()) {
          J_COMMA();
          BUF_PRINTF("sub_part_expr:\"");
          for (int i = 0; i < part->get_sub_part_columns().count(); i++) {
            if (i != 0) {
              J_COMMA();
            }
            BUF_PRINTF("%.*s", part->get_sub_part_columns().at(i).length(), part->get_sub_part_columns().at(i).ptr());
          }
          BUF_PRINTF("\"");
        }
      } else if (table_entry_->get_part_num() == 1) {
        J_KV("table_entry", "not a partition table");
      } else if (table_entry_->get_part_info() == NULL) {
        J_KV("table_entry", "partition information does not exist");
      } else if (table_entry_->is_dummy_entry()) {
        J_KV("table_entry", "__all_dummy");
      }
    } else {
      J_KV("table_entry", "unexpected null value");
    }
  );
}

void ObDiagnosisFetchTableRelatedData::reset()
{
  this->ObDiagnosisBase::reset();
  fail_at_ = LookupState::LOOKUP_TABLE_ENTRY_STATE;
  if (OB_NOT_NULL(table_entry_)) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }
  resp_error_= 0;
}

int64_t ObDiagnosisExprParse::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(warn);
    UNUSED(next_line);
  )
}

int64_t ObDiagnosisExprParse::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (col_val_.empty()) {
      J_KV("col_val", "there are no part keys");
    } else {
      BUF_PRINTF("col_val:\"%.*s\"", col_val_.length(), col_val_.ptr());
    }
  )
}

void ObDiagnosisExprParse::reset()
{
  this->ObDiagnosisBase::reset();
  col_val_.reset();
}

static const char *get_row_id_calc_state_str(const ObRowIDCalcState state)
{
  const char *state_str = NULL;
  switch (state)
  {
  case SUCCESS:
    state_str = "SUCCESS";
    break;
  case RESOLVE_ROWID_TO_OBOBJ:
    state_str = "RESOLVE_ROWID_TO_OBOBJ";
    break;
  case DECODE_ROWID:
    state_str = "DECODE_ROWID";
    break;
  case GET_PART_ID_FROM_DECODED_ROWID:
    state_str = "GET_PART_ID_FROM_DECODED_ROWID";
    break;
  default:
    state_str = "unknown state";
    break;
  }
  return state_str;
}
static const char *get_row_id_version_str(const int16_t version)
{
  const char *version_str = NULL;
  switch (version)
  {
  case ObURowIDData::INVALID_ROWID_VERSION:
    version_str = "INVALID_ROWID_VERSION";
    break;
  case ObURowIDData::PK_ROWID_VERSION:
    version_str = "RESOLVE_ROWID_TO_OBOBJ";
    break;
  case ObURowIDData::NO_PK_ROWID_VERSION:
    version_str = "DECODE_ROWID";
    break;
  case ObURowIDData::HEAP_TABLE_ROWID_VERSION:
    version_str = "GET_PART_ID_FROM_DECODED_ROWID";
    break;
  case ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION:
    version_str = "EXT_HEAP_TABLE_ROWID_VERSION";
    break;
  default:
    version_str = "unknown version";
    break;
  }
  return version_str;
}

int64_t ObDiagnosisCalcRowid::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (state_ == RESOLVE_ROWID_TO_OBOBJ) {
      DIAGNOSE_WARN("Fail resolve rowid to obobj with error(%d)", ret_);
    } else if (state_ == DECODE_ROWID) {
      DIAGNOSE_WARN("Fail to decode rowid obj with error(%d)", ret_);
    } else if (state_ == GET_PART_ID_FROM_DECODED_ROWID) {
      DIAGNOSE_WARN("Fail to get partition id from decoded rowid with error(%d)", ret_);
    }
    if (version_ == ObURowIDData::HEAP_TABLE_ROWID_VERSION ||
        version_ == ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION) {
      DIAGNOSE_INFO("oceanbase 4x. Get tablet id as partition id from decoded rowid");
    } else {
      DIAGNOSE_INFO("oceanbase 3x. Get obobj from decoded rowid");
    }
  )
}

int64_t ObDiagnosisCalcRowid::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("state", get_row_id_calc_state_str(state_),
         "version", get_row_id_version_str(version_));
  )
}

void ObDiagnosisCalcRowid::reset()
{
  this->ObDiagnosisBase::reset();
  state_ = SUCCESS;
  version_ = 0;
}
int64_t ObDiagnosisResolveToken::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (token_type_ == TOKEN_FUNC && (expr_type_ == OB_PROXY_EXPR_TYPE_NONE || expr_type_ == OB_PROXY_EXPR_TYPE_MAX)) {
      DIAGNOSE_WARN("Not support to resolve expr func(%.*s)", token_.length(), token_.ptr());
    }
    if (generated_func_ != OB_PROXY_EXPR_TYPE_NONE &&
        generated_func_ != OB_PROXY_EXPR_TYPE_FUNC_SUBSTR) {
      DIAGNOSE_WARN("Not support to resolve generated partition key func(%s). Now only supports substr()", get_expr_type_name(generated_func_));
    }
    if (resolved_obj_.get_collation_type() == CS_TYPE_INVALID && resolved_obj_.is_string_type()) {
      DIAGNOSE_WARN("Fail to resolve string object's collation(%s)", ObCharset::collation_name(resolved_obj_.get_collation_type()));
    }
  )
}
int64_t ObDiagnosisResolveToken::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    J_KV("token_type", get_obproxy_token_type(token_type_));
    if (!resolved_obj_.is_null()) {
      J_COMMA();
      BUF_PRINTF("resolve:\"");
      BUF_PRINTF("%s:", ob_obj_type_str(resolved_obj_.get_type()));
      resolved_obj_.print_plain_str_literal(buf, buf_len, pos);
      if (resolved_obj_.is_string_type()) {
        BUF_PRINTF("<%s>", ObCharset::collation_name(resolved_obj_.get_collation_type()));
      }
      BUF_PRINTF("\"");
    }
    if (!token_.empty()) {
      J_COMMA();
      J_KV(K_(token));
    }
    if (expr_type_ != ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE) {
      J_COMMA();
      J_KV("expr_type", get_expr_type_name(expr_type_));
    }
    if (generated_func_ != ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE) {
      J_COMMA();
      J_KV("generated_func", get_generate_function_type(generated_func_));
    }
  )
}

void ObDiagnosisResolveToken::reset()
{
  this->ObDiagnosisBase::reset();
  resolved_obj_.reset();
  token_.reset();
  token_type_ = ObProxyTokenType::TOKEN_NONE;
  expr_type_ = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  generated_func_ = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
}

int64_t ObDiagnosisResolveExpr::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  return ObDiagnosisBase::diagnose(buf, buf_len, warn, next_line);
}

int64_t ObDiagnosisResolveExpr::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (!part_range_.empty()) {
      J_KV(K_(part_range));
    }
    if (!sub_part_range_.empty()) {
      if (!part_range_.empty()) {
        J_COMMA();
      }
      J_KV(K_(sub_part_range));
    }
  )
}

void ObDiagnosisResolveExpr::reset()
{
  this->ObDiagnosisBase::reset();
  part_range_.reset();
  sub_part_range_.reset();
}

int64_t ObDiagnosisCalcPartitionId::diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const
{
  DIAGNOSIS_PROMPTS_PRINT(
    if (NULL == part_desc_) {
      DIAGNOSE_WARN("Invalid partition description")
    }
  )
}

int64_t ObDiagnosisCalcPartitionId::to_string(char *buf, const int64_t buf_len) const
{
  DIAGNOSIS_DATA_PRINT(
    if (OB_NOT_NULL(part_desc_)) {
      BUF_PRINTF("part_description:\"");
      pos += part_desc_->to_plain_string(buf + pos, buf_len - pos);
      if (OB_NOT_NULL(sub_part_desc_)) {
        BUF_PRINTF(" ");
        pos += sub_part_desc_->to_plain_string(buf + pos, buf_len - pos);
      }
      BUF_PRINTF("\"");
    }
  )
}

void ObDiagnosisCalcPartitionId::reset()
{
  this->ObDiagnosisBase::reset();
  part_desc_ = NULL;
  sub_part_desc_ = NULL;
}

// WARNING: overwrite the vptr of the members
// do not call this func manually
ObDiagnosisPoint::ObDiagnosisPoint() {
  MEMSET(this, 0, sizeof(ObDiagnosisPoint));
}
ObDiagnosisPoint::ObDiagnosisPoint(const ObDiagnosisPoint& odp) {
  // construct at clean memory
  MEMSET(this, 0, sizeof(ObDiagnosisPoint));
  // call the actual copy constructor
  switch(odp.base_.type_) {
    case SQL_PARSE:
      new (&sql_parse_) ObDiagnosisSqlParse(odp.sql_parse_);
      break;

    case ROUTE_INFO:
      new (&route_info_) ObDiagnosisRouteInfo(odp.route_info_);
      break;

    case LOCATION_CACHE_LOOKUP:
      new (&location_cache_lookup_) ObDiagnosisLocationCacheLookup(odp.location_cache_lookup_);
      break;

    case ROUTINE_ENTRY_LOOKUP_DONE:
      new (&routine_entry_lookup_done_) ObDiagnosisRoutineEntryLookupDone(odp.routine_entry_lookup_done_);
      break;

    case TABLE_ENTRY_LOOKUP_DONE:
      new (&table_entry_lookup_done_) ObDiagnosisTableEntryLookupDone(odp.table_entry_lookup_done_);
      break;

    case PARTITION_ID_CALC_DONE:
      new (&partition_id_calc_) ObDiagnosisPartIDCalcDone(odp.partition_id_calc_);
      break;

    case PARTITION_ENTRY_LOOKUP_DONE:
      new (&partition_entry_lookup_done_) ObDiagnosisPartEntryLookupDone(odp.partition_entry_lookup_done_);
      break;

    case ROUTE_POLICY:
      new (&route_policy_) ObDiagnosisRoutePolicy(odp.route_policy_);
      break;

    case CONGESTION_CONTROL:
      new (&congestion_control_) ObDiagnosisCongestionControl(odp.congestion_control_);
      break;

    case HANDLE_RESPONSE:
      new (&handle_response_) ObDiagnosisHandleResponse(odp.handle_response_);
      break;

    case RETRY:
      new (&retry_connection_) ObDiagnosisRetry(odp.retry_connection_);
      break;

    case FETCH_TABLE_RELATED_DATA:
      new (&fetch_table_related_data_) ObDiagnosisFetchTableRelatedData(odp.fetch_table_related_data_);
      break;

    case EXPR_PARSE:
      new (&expr_parse_) ObDiagnosisExprParse(odp.expr_parse_);
      break;

    case CALC_ROWID:
      new (&calc_rowid_) ObDiagnosisCalcRowid(odp.calc_rowid_);
      break;

    case RESOLVE_TOKEN:
      new (&resolve_token_) ObDiagnosisResolveToken(odp.resolve_token_);
      break;

    case RESOLVE_EXPR:
      new (&resolve_expr_) ObDiagnosisResolveExpr(odp.resolve_expr_);
      break;

    case CALC_PARTITION_ID:
      new (&calc_partition_id_) ObDiagnosisCalcPartitionId(odp.calc_partition_id_);
      break;

    default:
      LOG_WDIAG("unexpected diagnosis point struct", K_(odp.base_.type));
      MEMSET(this, 0, sizeof(ObDiagnosisPoint));
      break;
  }
}
ObDiagnosisPoint::~ObDiagnosisPoint() {
  switch(base_.type_) {
    case BASE_ROUTE_DIAGNOSIS_TYPE:
    case PARTITION_ID_CALC_START:
    case TABLE_ENTRY_LOOKUP_START:
      base_.~ObDiagnosisBase();
      break;

    case SQL_PARSE:
      sql_parse_.~ObDiagnosisSqlParse();
      break;

    case ROUTE_INFO:
      route_info_.~ObDiagnosisRouteInfo();
      break;

    case LOCATION_CACHE_LOOKUP:
      location_cache_lookup_.~ObDiagnosisLocationCacheLookup();
      break;

    case ROUTINE_ENTRY_LOOKUP_DONE:
      routine_entry_lookup_done_.~ObDiagnosisRoutineEntryLookupDone();
      break;

    case TABLE_ENTRY_LOOKUP_DONE:
      table_entry_lookup_done_.~ObDiagnosisTableEntryLookupDone();
      break;

    case PARTITION_ID_CALC_DONE:
      partition_id_calc_.~ObDiagnosisPartIDCalcDone();
      break;

    case PARTITION_ENTRY_LOOKUP_DONE:
      partition_entry_lookup_done_.~ObDiagnosisPartEntryLookupDone();
      break;

    case ROUTE_POLICY:
      route_policy_.~ObDiagnosisRoutePolicy();
      break;

    case CONGESTION_CONTROL:
      congestion_control_.~ObDiagnosisCongestionControl();
      break;

    case HANDLE_RESPONSE:
      handle_response_.~ObDiagnosisHandleResponse();
      break;

    case RETRY:
      retry_connection_.~ObDiagnosisRetry();
      break;

    case FETCH_TABLE_RELATED_DATA:
      fetch_table_related_data_.~ObDiagnosisFetchTableRelatedData();
      break;

    case EXPR_PARSE:
      expr_parse_.~ObDiagnosisExprParse();
      break;

    case CALC_ROWID:
      calc_rowid_.~ObDiagnosisCalcRowid();
      break;

    case RESOLVE_TOKEN:
      resolve_token_.~ObDiagnosisResolveToken();
      break;

    case RESOLVE_EXPR:
      resolve_expr_.~ObDiagnosisResolveExpr();
      break;

    case CALC_PARTITION_ID:
      calc_partition_id_.~ObDiagnosisCalcPartitionId();
      break;

    default:
      LOG_WDIAG("destory unexpected diagnosis point struct", K_(base_.type));
      MEMSET(this, 0, sizeof(ObDiagnosisPoint));
      break;
  }
}
void ObDiagnosisPoint::reset()
{
  switch(base_.type_) {
    case BASE_ROUTE_DIAGNOSIS_TYPE:
      this->base_.reset();
      break;

    case SQL_PARSE:
      this->sql_parse_.reset();
      break;

    case ROUTE_INFO:
      this->route_info_.reset();
      break;

    case LOCATION_CACHE_LOOKUP:
      this->location_cache_lookup_.reset();
      break;

    case ROUTINE_ENTRY_LOOKUP_DONE:
      this->routine_entry_lookup_done_.reset();
      break;

    case TABLE_ENTRY_LOOKUP_DONE:
      this->table_entry_lookup_done_.reset();
      break;

    case PARTITION_ID_CALC_DONE:
      this->partition_id_calc_.reset();
      break;

    case PARTITION_ENTRY_LOOKUP_DONE:
      this->partition_entry_lookup_done_.reset();
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

   case FETCH_TABLE_RELATED_DATA:
      this->fetch_table_related_data_.reset();
      break;

    case EXPR_PARSE:
      this->expr_parse_.reset();
      break;

    case CALC_ROWID:
      this->calc_rowid_.reset();
      break;

    case RESOLVE_TOKEN:
      this->resolve_token_.reset();
      break;

    case RESOLVE_EXPR:
      this->resolve_expr_.reset();
      break;

    case CALC_PARTITION_ID:
      this->calc_partition_id_.reset();
      break;

    default:
      LOG_WDIAG("reset unexpected diagnosis point struct", K_(base_.type), K(lbt()));
      MEMSET(this, 0, sizeof(ObDiagnosisPoint));
      break;
  }
}
ObDiagnosisPoint& ObDiagnosisPoint::operator=(const ObDiagnosisPoint& odp)
{
  if (&odp != this) {
    // reset the old memeory before call assignment
    // because the part of the old memory maybe treated
    // as valid data member of the another union's data member
    reset();
    switch(odp.base_.type_) {
      case BASE_ROUTE_DIAGNOSIS_TYPE:
        this->base_ = odp.base_;
        LOG_WDIAG("copy assign base diagnosis point struct", K(lbt()));
        break;

      case SQL_PARSE:
        this->sql_parse_ = odp.sql_parse_;
        break;

      case ROUTE_INFO:
        this->route_info_ = odp.route_info_;
        break;

      case LOCATION_CACHE_LOOKUP:
        this->location_cache_lookup_ = odp.location_cache_lookup_;
        break;

      case ROUTINE_ENTRY_LOOKUP_DONE:
        this->routine_entry_lookup_done_ = odp.routine_entry_lookup_done_;
        break;

      case TABLE_ENTRY_LOOKUP_DONE:
        this->table_entry_lookup_done_ = odp.table_entry_lookup_done_;
        break;

      case PARTITION_ID_CALC_DONE:
        this->partition_id_calc_ = odp.partition_id_calc_;
        break;

      case PARTITION_ENTRY_LOOKUP_DONE:
        this->partition_entry_lookup_done_ = odp.partition_entry_lookup_done_;
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

     case FETCH_TABLE_RELATED_DATA:
        this->fetch_table_related_data_ = odp.fetch_table_related_data_;
        break;

      case EXPR_PARSE:
        this->expr_parse_ = odp.expr_parse_;
        break;

      case CALC_ROWID:
        this->calc_rowid_ = odp.calc_rowid_;
        break;

      case RESOLVE_TOKEN:
        this->resolve_token_ = odp.resolve_token_;
        break;

      case RESOLVE_EXPR:
        this->resolve_expr_ = odp.resolve_expr_;
        break;

      case CALC_PARTITION_ID:
        this->calc_partition_id_ = odp.calc_partition_id_;
        break;

      default:
        LOG_WDIAG("copy assign unexpected diagnosis point, do nothing", K(odp.base_.type_));
        break;
    }
  }
  return *this;
}
void ObRouteDiagnosis::trans_first_query_diagnosed(ObProxyMysqlRequest &cli_req) {
  // ObProxyMysqlRequest will not free the memory of request buf
  // ObRouteDiagnosis will do free after whole trx be diagnosed
  // save the first sql
  if (cli_req.get_parse_result().has_explain_route()) {
    LOG_DEBUG("`explain route <sql>;` does not be treated as trans first sql");
  } else if (!query_diagnosis_array_.empty() && SQL_PARSE == query_diagnosis_array_[0].base_.type_) {
    trx_1st_sql_ = query_diagnosis_array_[0].sql_parse_.sql_;
    cli_req.borrow_req_buf(trx_1st_req_buf_, trx_1st_req_buf_len_);
    LOG_DEBUG("borrow the trx 1st req buf", KP_(trx_1st_req_buf), K_(trx_1st_req_buf_len));
  }
  trans_cur_query_diagnosed();
}

void ObRouteDiagnosis::trans_cur_query_diagnosed() {
  // clear cur sql diagnosis
  query_diagnosis_array_.reuse();
  allocator_.reuse();
  is_log_warn_ = true;
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
  trans_cur_query_diagnosed();
}
int64_t ObRouteDiagnosis::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ObDiagnosisPoint any_point;
  const static char *next_line = "/n";
  BUF_PRINTF(next_line);
  if (!trx_1st_sql_.empty()) {
    J_KV("Trans First Query", trx_1st_sql_);
    BUF_PRINTF(next_line);
  }
  for (int i = 0; i < query_diagnosis_array_.count(); i++) {
    if (query_diagnosis_array_.at(i).base_.type_ == SQL_PARSE) {
      J_KV("Trans Current Query", query_diagnosis_array_.at(i).sql_parse_.sql_);
      BUF_PRINTF(next_line);
    }
  }
  BUF_PRINTF(next_line);
  BUF_PRINTF("Route Prompts");BUF_PRINTF(next_line);
  int warn = 0;
  for (int i = 0; i < query_diagnosis_array_.count() && warn == 0; i++) {
    pos += query_diagnosis_array_[i].diagnose(buf + pos, buf_len - pos, warn, next_line);
  }
  BUF_PRINTF(next_line);BUF_PRINTF(next_line);
  BUF_PRINTF("Route Plan");BUF_PRINTF(next_line);
  for (int i = 0; i < query_diagnosis_array_.count(); i++) {
    if (BASE_ROUTE_DIAGNOSIS_TYPE == query_diagnosis_array_[i].base_.type_) {
      /* do nothing */
    } else {
      if (FETCH_TABLE_RELATED_DATA == query_diagnosis_array_[i].base_.type_) {
        any_point.base_.type_ = TABLE_ENTRY_LOOKUP_START;
        FORMAT_PRINT(any_point);
      } else if (EXPR_PARSE == query_diagnosis_array_[i].base_.type_) {
        any_point.base_.type_ = PARTITION_ID_CALC_START;
        FORMAT_PRINT(any_point);
      }
      FORMAT_PRINT(query_diagnosis_array_[i]);
    }
  }

  return pos;
}
int64_t ObRouteDiagnosis::to_explain_route_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  ObDiagnosisPoint any_point;
  const static char *next_line = "\n";
  BUF_PRINTF(next_line);
  if (!trx_1st_sql_.empty()) {
    J_KV("Trans First Query", trx_1st_sql_);
    BUF_PRINTF(next_line);
  }
  for (int i = 0; i < query_diagnosis_array_.count(); i++) {
    if (query_diagnosis_array_.at(i).base_.type_ == SQL_PARSE) {
      J_KV("Trans Current Query", query_diagnosis_array_.at(i).sql_parse_.sql_);
      BUF_PRINTF(next_line);
    }
  }
  BUF_PRINTF(next_line);
  BUF_PRINTF("Route Prompts");BUF_PRINTF(next_line);
  BUF_PRINTF("-----------------");BUF_PRINTF(next_line);
  int warn = 0;
  for (int i = 0; i < query_diagnosis_array_.count() && warn == 0; i++) {
    pos += query_diagnosis_array_[i].diagnose(buf + pos, buf_len - pos, warn, next_line);
  }
  BUF_PRINTF(next_line);BUF_PRINTF(next_line);
  BUF_PRINTF("Route Plan");BUF_PRINTF(next_line);
  BUF_PRINTF("-----------------");BUF_PRINTF(next_line);
  for (int i = 0; i < query_diagnosis_array_.count(); i++) {
    if (BASE_ROUTE_DIAGNOSIS_TYPE == query_diagnosis_array_[i].base_.type_) {
      /* do nothing */
    } else {
      if (FETCH_TABLE_RELATED_DATA == query_diagnosis_array_[i].base_.type_) {
        any_point.base_.type_ = TABLE_ENTRY_LOOKUP_START;
        FORMAT_PRINT(any_point);
      } else if (EXPR_PARSE == query_diagnosis_array_[i].base_.type_) {
        any_point.base_.type_ = PARTITION_ID_CALC_START;
        FORMAT_PRINT(any_point);
      }
      FORMAT_PRINT(query_diagnosis_array_[i]);
    }
  }

  return pos;
}

ObRouteDiagnosis::ObRouteDiagnosis() :
  is_log_warn_(true), is_support_explain_route_(false), level_(DIAGNOSIS_LEVEL_ZERO),
  query_diagnosis_array_(ObModIds::OB_PROXY_ROUTE_DIAGNOSIS_SE_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE),
  trx_1st_req_buf_(NULL), trx_1st_req_buf_len_(0), allocator_(ObModIds::OB_PROXY_ROUTE_DIAGNOSIS_STRING){}
ObRouteDiagnosis::~ObRouteDiagnosis() {
  if (OB_NOT_NULL(trx_1st_req_buf_) && 0 != trx_1st_req_buf_len_) {
    op_fixed_mem_free(trx_1st_req_buf_, trx_1st_req_buf_len_);
    trx_1st_req_buf_ = NULL;
    trx_1st_req_buf_len_ = 0;
  }
  query_diagnosis_array_.reset();
  allocator_.reset();
}

void ObDiagnosisPointClearCallBack::operator()(ObDiagnosisPoint *ptr)
{
  // release the resource
  if (OB_NOT_NULL(ptr)) {
    switch (ptr->base_.type_)
    {
    case TABLE_ENTRY_LOOKUP_DONE:
      if (OB_NOT_NULL(ptr->table_entry_lookup_done_.table_entry_)) {
        ptr->table_entry_lookup_done_.table_entry_->dec_ref();
        ptr->table_entry_lookup_done_.table_entry_ = NULL;
      }
      break;
    case FETCH_TABLE_RELATED_DATA:
      if (OB_NOT_NULL(ptr->fetch_table_related_data_.table_entry_)) {
        ptr->fetch_table_related_data_.table_entry_->dec_ref();
        ptr->fetch_table_related_data_.table_entry_ = NULL;
      }
      break;
    default:
      // do nothing
      break;
    }
  }
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

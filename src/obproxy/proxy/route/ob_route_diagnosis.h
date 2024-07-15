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

#ifndef OBPROXY_OB_ROUTE_DIAGNOSIS_H
#define OBPROXY_OB_ROUTE_DIAGNOSIS_H

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"
#include "iocore/net/ob_inet.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_route_struct.h"
#include "common/ob_hint.h"
#include "ob_ldc_struct.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "lib/oblog/ob_log.h"
#include "lib/hash/ob_hashmap.h"
#include "proxy/route/ob_table_entry_cont.h"

#define MAX_QUERY_DIAGNOSIS_ARRAY_SIZE 1 << 7
#define EXPR_PARSE_MAX_LEN 1 << 9
#define RESOLVE_EXPR_MAX_LEN 1 << 9

/*
  @param rd: the ptr of ObRouteDiagnosis
  @param type: ObDiagnosisPointType
  @param name: samll case of ObDiagnosisXxxXXX.
               e.g. ObDiagnosisSqlParse -> sql_parse
  @param args: ret then else
*/
#define ROUTE_DIAGNOSIS(rd, type, name, args...)                                           \
  if (OB_NOT_NULL(rd) &&                                                                   \
      rd->is_diagnostic(type) &&                                                           \
      rd->get_query_diagnosis_count() < MAX_QUERY_DIAGNOSIS_ARRAY_SIZE) {                  \
    rd->diagnosis_##name(args);                                                            \
    LOG_DEBUG("route diagnosis point", K(type));                                           \
  }
#define _ROUTE_DIAGNOSIS_POINT(T, type, ret, args...)                             \
  T t(ret, &allocator_, args);                                                    \
  ObDiagnosisPoint *p = reinterpret_cast<ObDiagnosisPoint*>(&t);                  \
  query_diagnosis_array_.push_back(*p);

#define DIAGNOSIS_DATA_PRINT(content)                       \
  int64_t pos = 0;                                          \
  if (ret_ != OB_SUCCESS) {                                 \
    BUF_PRINTF("error:%d", ret_);                           \
    J_COMMA();                                              \
  }                                                         \
  content;                                                  \
  return pos;

#define DIAGNOSE_WARN(args...)                              \
  if (pos == 0) {                                           \
    BUF_PRINTF("> %s", get_diagnosis_type_name(type_));     \
    BUF_PRINTF(next_line);                                  \
  }                                                         \
  BUF_PRINTF("  [WARN] ");                                  \
  BUF_PRINTF(args);                                         \
  BUF_PRINTF(next_line);                                    \
  warn = 1;

#define DIAGNOSE_INFO(args...)                                \
  if (warn == 0) {                                            \
    if (pos == 0) {                                           \
      BUF_PRINTF("> %s", get_diagnosis_type_name(type_));     \
      BUF_PRINTF(next_line);                                  \
    }                                                         \
    BUF_PRINTF("  [INFO] ");                                  \
    BUF_PRINTF(args);                                         \
    BUF_PRINTF(next_line);                                    \
  }

#define DIAGNOSIS_PROMPTS_PRINT(content)                    \
  int64_t pos = 0;                                          \
  content;                                                  \
  return pos;

#define FORMAT_PRINT(diag_point)                                        \
  int lv = static_cast<int>(get_type_level(diag_point.base_.type_));    \
  for (int i = 1; i < lv; i++) {                                        \
    BUF_PRINTF("  ");                                                   \
  }                                                                     \
  BUF_PRINTF("> %s:{", get_diagnosis_type_name(diag_point.base_.type_));\
  pos += diag_point.to_string(buf + pos, buf_len - pos);                \
  BUF_PRINTF("}");                                                      \
  BUF_PRINTF(next_line);

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObTableEntry;
using namespace obutils;
enum ObDiagnosisLevel {
  DIAGNOSIS_LEVEL_ZERO = 0,
  DIAGNOSIS_LEVEL_ONE,
  DIAGNOSIS_LEVEL_TWO,
  DIAGNOSIS_LEVEL_THREE,
  DIAGNOSIS_LEVEL_FOUR,
};

#define VALID_DIAGNOSIS_TYPE_NUM RETRY
enum ObDiagnosisType {
  BASE_ROUTE_DIAGNOSIS_TYPE,                           // for mark empty
  SQL_PARSE,                      // level 1
  ROUTE_INFO,                     // level 1
  LOCATION_CACHE_LOOKUP,          // level 1
  ROUTINE_ENTRY_LOOKUP_DONE,      // level 2
  TABLE_ENTRY_LOOKUP_START,       // virtual
  FETCH_TABLE_RELATED_DATA,       // level 3
  TABLE_ENTRY_LOOKUP_DONE,        // level 2
  PARTITION_ID_CALC_START,        // virtual
  EXPR_PARSE,                     // level 3
  CALC_ROWID,                     // level 3
  RESOLVE_EXPR,                   // level 3
  RESOLVE_TOKEN,                  // level 4
  CALC_PARTITION_ID,              // level 3
  PARTITION_ID_CALC_DONE,         // level 2
  PARTITION_ENTRY_LOOKUP_DONE,    // level 2
  ROUTE_POLICY,                   // level 1
  CONGESTION_CONTROL,             // level 1
  HANDLE_RESPONSE,                // level 1
  RETRY,                          // level 1
};
extern void deep_copy_string(ObIAllocator *alloc, ObString &src, ObString &dest);
extern ObDiagnosisLevel get_type_level(ObDiagnosisType type);
/*
  if you want to add new ObDiagnosisXxxXxxx
  1. add your diagnosis type to ObDiagnosisType and get_type_level()
  2. your ObDiagnosisXxxXxxx class extend ObDiagnosisBase
  3. implement diagnose(), to_string(), reset() and ~ObDiagnosisXxxXxxx
  4. implement ObRouteDiagnosis::diagnosis_xxx_xxxx
  5. implement ObDiagnosisPoint::~ObDiagnosisPoint(). Call deconstructor of ObDiagnosisXxxXxxx
  6. implement ObDiagnosisPoint::reset(). Call ::reset()
  7. implement get_diagnosis_type_name()

  WARN: if your class's member refers to the resource from somewhere else
        then be careful when you implements ObDiagnosisXxxXxxx
  1. reset()
  2. deconstructor
  3. construct assign
  4. copy assgin

  usage: use marco ROUTE_DIAGNOSIS(rd, type, name, args...)
*/
class ObDiagnosisBase {
public:
  ObDiagnosisBase() : ret_(common::OB_INVALID_ARGUMENT) {}
  explicit ObDiagnosisBase(
    ObDiagnosisType type,
    int ret,
    ObIAllocator *alloc = NULL) : type_(type), ret_(ret), alloc_(alloc) {}
  ~ObDiagnosisBase() { reset(); }
  virtual int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual void reset();
  ObDiagnosisType type_;
  // ret value around the diagnosis point logged place
  int ret_;
  // refers to ObRouteDiagnosis::allocator_
  ObIAllocator *alloc_;
};
class ObDiagnosisSqlParse : public ObDiagnosisBase {
public:
  ObDiagnosisSqlParse(): ObDiagnosisBase() {}
  ~ObDiagnosisSqlParse() {
    sql_.reset();
    table_.reset();
    sql_cmd_ = obmysql::OB_MYSQL_COM_SLEEP;
  }
  explicit ObDiagnosisSqlParse(
    int ret,
    ObIAllocator *alloc,
    common::ObString sql,
    common::ObString table,
    obmysql::ObMySQLCmd sql_cmd)
    : ObDiagnosisBase(SQL_PARSE, ret, alloc), sql_(sql),
      table_(table), sql_cmd_(sql_cmd) {}
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString            sql_;
  ObString            table_;
  obmysql::ObMySQLCmd sql_cmd_;
};

enum class ObRouteInfoType {
  INVALID = 0,
  USE_GLOBAL_INDEX,
  USE_OBPROXY_ROUTE_ADDR,
  USE_CURSOR,
  USE_PIECES_DATA,
  USE_CONFIG_TARGET_DB,
  USE_COMMENT_TARGET_DB,
  USE_TEST_SVR_ADDR,
  USE_LAST_SESSION,
  USE_SHARD_TXN_SESSION,
  USE_LOCK_SESSION,
  USE_CACHED_SESSION,
  USE_SINGLE_LEADER,
  USE_PARTITION_LOCATION_LOOKUP,
  USE_COORDINATOR_SESSION,
  USE_ROUTE_POLICY
};

class ObDiagnosisRouteInfo : public ObDiagnosisBase {
public:
  ObDiagnosisRouteInfo() :
      ObDiagnosisBase(), route_info_type_(ObRouteInfoType::INVALID), svr_addr_(),
      in_transaction_(false), depent_func_(false), trans_specified_(false) {};
  explicit ObDiagnosisRouteInfo(
    int ret,
    ObIAllocator *alloc,
    ObRouteInfoType type,
    net::ObIpEndpoint &svr_addr,
    bool in_trans,
    bool depent_func,
    bool trans_specified)
    : ObDiagnosisBase(ROUTE_INFO, ret, alloc),
      route_info_type_(type), svr_addr_(svr_addr),
      in_transaction_(in_trans), depent_func_(depent_func), trans_specified_(trans_specified) {};
  ~ObDiagnosisRouteInfo() {
    route_info_type_ = ObRouteInfoType::INVALID;
    svr_addr_.reset();
    in_transaction_ = false;
    depent_func_ = false;
    trans_specified_ = false;
  }
public:
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
  ObRouteInfoType     route_info_type_;
  net::ObIpEndpoint svr_addr_;
  bool              in_transaction_;
  bool              depent_func_;
  bool              trans_specified_;
};
class ObDiagnosisLocationCacheLookup : public ObDiagnosisBase {
public:
  ObDiagnosisLocationCacheLookup() : ObDiagnosisBase() {};
  explicit ObDiagnosisLocationCacheLookup(
    int ret,
    ObIAllocator *alloc,
    obutils::ObServerRoutingMode mode,
    bool need_pl_lookup)
    : ObDiagnosisBase(ObDiagnosisType::LOCATION_CACHE_LOOKUP, ret, alloc),
      mode_(mode), need_partition_location_lookup_(need_pl_lookup) {};
  ~ObDiagnosisLocationCacheLookup() {
    mode_ = obutils::OB_STANDARD_ROUTING_MODE;
    need_partition_location_lookup_ = false;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  obutils::ObServerRoutingMode  mode_;
  bool                          need_partition_location_lookup_;
};

class ObDiagnosisRoutineEntryLookupDone : public ObDiagnosisBase {
public:
  ObDiagnosisRoutineEntryLookupDone() : ObDiagnosisBase() {};
  explicit ObDiagnosisRoutineEntryLookupDone(
    int ret,
    ObIAllocator *alloc,
    ObString sql,
    bool from_remote,
    bool is_lookup_succ,
    ObRouteEntry::ObRouteEntryState entry_state)
    : ObDiagnosisBase(ObDiagnosisType::ROUTINE_ENTRY_LOOKUP_DONE, ret, alloc),
      entry_from_remote_(from_remote), is_lookup_succ_(is_lookup_succ), entry_state_(entry_state) {
    deep_copy_string(alloc, sql, routine_sql_);
  };
  ~ObDiagnosisRoutineEntryLookupDone() {
    routine_sql_.reset();
    entry_from_remote_ = false;
    is_lookup_succ_ = false;
    entry_state_ = ObRouteEntry::BORN;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                         routine_sql_;
  bool                             entry_from_remote_;
  bool                             is_lookup_succ_;
  ObRouteEntry::ObRouteEntryState  entry_state_;
};

class ObDiagnosisTableEntryLookupDone : public ObDiagnosisBase {
public:
  ObDiagnosisTableEntryLookupDone() : ObDiagnosisBase() {};
  explicit ObDiagnosisTableEntryLookupDone(
    int ret,
    ObIAllocator *alloc,
    ObTableEntry *table_entry,
    bool entry_from_remote,
    bool is_lookup_succ)
    : ObDiagnosisBase(ObDiagnosisType::TABLE_ENTRY_LOOKUP_DONE, ret, alloc),
      table_entry_(table_entry), entry_from_remote_(entry_from_remote), is_lookup_succ_(is_lookup_succ) {
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->inc_ref();
    }
  };

  // The copy constructor initializes the new ObDiagnosisTableEntryLookupDone with an already existing one
  // So we consider the table_entry_ not refers to some resource
  ObDiagnosisTableEntryLookupDone(const ObDiagnosisTableEntryLookupDone& table_entry_lookup_done)
  : ObDiagnosisBase(table_entry_lookup_done.type_,
                    table_entry_lookup_done.ret_,
                    table_entry_lookup_done.alloc_) {
    entry_from_remote_ = table_entry_lookup_done.entry_from_remote_;
    is_lookup_succ_ = table_entry_lookup_done.is_lookup_succ_;
    table_entry_ = table_entry_lookup_done.table_entry_;
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->inc_ref();
    }
  }

  ObDiagnosisTableEntryLookupDone& operator=(const ObDiagnosisTableEntryLookupDone& table_entry_lookup_done) {
    if (this!= &table_entry_lookup_done) {
      if (OB_NOT_NULL(table_entry_)) {
        table_entry_->dec_ref();
        table_entry_ = NULL;
      }
      table_entry_ = table_entry_lookup_done.table_entry_;
      if (OB_NOT_NULL(table_entry_)) {
        table_entry_->inc_ref();
      }
      ret_ = table_entry_lookup_done.ret_;
      alloc_ = table_entry_lookup_done.alloc_;
      type_ = table_entry_lookup_done.type_;
      entry_from_remote_ = table_entry_lookup_done.entry_from_remote_;
      is_lookup_succ_ = table_entry_lookup_done.is_lookup_succ_;
    }
    return *this;
  }
  ~ObDiagnosisTableEntryLookupDone() {
    entry_from_remote_ = false;
    is_lookup_succ_ = false;
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->dec_ref();
      table_entry_ = NULL;
    }
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObTableEntry* table_entry_;
  bool          entry_from_remote_;
  bool          is_lookup_succ_;
};

class ObDiagnosisPartIDCalcDone : public ObDiagnosisBase {
public:
  ObDiagnosisPartIDCalcDone() : ObDiagnosisBase() {};
  explicit ObDiagnosisPartIDCalcDone(
    int ret,
    ObIAllocator *alloc,
    common::ObString parse_sql,
    common::ObString part_name,
    int64_t part_idx,
    int64_t sub_part_idx,
    int64_t partition_id,
    share::schema::ObPartitionLevel lv)
    : ObDiagnosisBase(ObDiagnosisType::PARTITION_ID_CALC_DONE, ret, alloc), parse_sql_(parse_sql), part_name_(part_name),
      part_idx_(part_idx), sub_part_idx_(sub_part_idx), partition_id_(partition_id), level_(lv) {};
  ~ObDiagnosisPartIDCalcDone() {
    parse_sql_.reset();
    part_name_.reset();
    part_idx_ = 0;
    sub_part_idx_ = 0;
    partition_id_ = 0;
    level_ = share::schema::PARTITION_LEVEL_ZERO;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                        parse_sql_;
  ObString                        part_name_;
  int64_t                         part_idx_;
  int64_t                         sub_part_idx_;
  int64_t                         partition_id_;
  share::schema::ObPartitionLevel level_;
};

class ObDiagnosisPartEntryLookupDone : public ObDiagnosisBase {
public:
  ObDiagnosisPartEntryLookupDone() : ObDiagnosisBase() {};
  explicit ObDiagnosisPartEntryLookupDone(
    int ret,
    ObIAllocator *alloc,
    bool from_remote,
    bool lookup_succ,
    bool has_dup_replica,
    ObRouteEntry::ObRouteEntryState entry_state,
    ObProxyReplicaLocation leader)
    : ObDiagnosisBase(ObDiagnosisType::PARTITION_ENTRY_LOOKUP_DONE, ret, alloc),
      entry_from_remote_(from_remote), is_lookup_succ_(lookup_succ),
      has_dup_replica_(has_dup_replica), entry_state_(entry_state), leader_(leader) {}
  ~ObDiagnosisPartEntryLookupDone() {
    entry_from_remote_ = false;
    is_lookup_succ_ = false;
    has_dup_replica_ = false;
    entry_state_ = ObRouteEntry::BORN;
    leader_.reset();
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  bool                            entry_from_remote_;
  bool                            is_lookup_succ_;
  bool                            has_dup_replica_;
  ObRouteEntry::ObRouteEntryState entry_state_;
  ObProxyReplicaLocation          leader_;
};
class ObDiagnosisRoutePolicy : public ObDiagnosisBase {
public:
  ObDiagnosisRoutePolicy() : ObDiagnosisBase() {};
  explicit ObDiagnosisRoutePolicy(
    int ret,
    ObIAllocator *alloc,
    bool need_use_dup_replica,
    bool need_check_merge_status,
    bool readonly_zone_exsits,
    bool request_support_readonly_zone,
    ObString &proxy_idc_name,
    ObString &tenant_primary_zone,
    ObString &proxy_primary_zone,
    ObConsistencyLevel session_consistency_level,
    ObConsistencyLevel trans_consistency_level,
    ObRoutePolicyEnum proxy_route_policy,
    ObRoutePolicyEnum session_route_policy,
    ObRoutePolicyEnum route_policy,
    ObRoutePolicyEnum opt_route_policy,
    ObLDCItem chosen_server,
    ObRouteType chosen_route_type)
    : ObDiagnosisBase(ObDiagnosisType::ROUTE_POLICY, ret, alloc),
      need_use_dup_replica_(need_use_dup_replica),
      need_check_merge_status_(need_check_merge_status),
      readonly_zone_exsits_(readonly_zone_exsits),
      request_support_readonly_zone_(request_support_readonly_zone),
      proxy_idc_name_(proxy_idc_name),
      proxy_primary_zone_(proxy_primary_zone),
      session_consistency_level_(session_consistency_level),
      trans_consistency_level_(trans_consistency_level),
      proxy_route_policy_(proxy_route_policy),
      session_route_policy_(session_route_policy),
      route_policy_(route_policy),
      opt_route_policy_(opt_route_policy),
      chosen_server_(chosen_server),
      chosen_route_type_(chosen_route_type) {
    if (OB_NOT_NULL(chosen_server_.replica_)) {
      replica_ = *chosen_server_.replica_;
      chosen_server_.replica_ = NULL;
    }
    deep_copy_string(alloc_, tenant_primary_zone, tenant_primary_zone_);
  }
  ~ObDiagnosisRoutePolicy() {
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
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  bool need_use_dup_replica_;
  bool need_check_merge_status_;
  bool readonly_zone_exsits_;
  bool request_support_readonly_zone_;
  ObString proxy_idc_name_; // proxy config proxy_id_name
  ObString tenant_primary_zone_;
  ObString proxy_primary_zone_;
  ObConsistencyLevel session_consistency_level_; // sys variable consistency level
  ObConsistencyLevel trans_consistency_level_; // sql hint + sys var + stmt type
  ObRoutePolicyEnum proxy_route_policy_;   // proxy config proxy_route_policy
  ObRoutePolicyEnum session_route_policy_; // session variable ob_route_policy
  ObRoutePolicyEnum route_policy_; 		  // before opti
  ObRoutePolicyEnum opt_route_policy_;     // after opti
  ObLDCItem chosen_server_;   // do not use chosen_server_.replica_ but use replica_
  ObProxyReplicaLocation replica_;
  ObRouteType chosen_route_type_;
 };

class ObDiagnosisCongestionControl : public ObDiagnosisBase {
public:
  ObDiagnosisCongestionControl() : ObDiagnosisBase() {};
  explicit ObDiagnosisCongestionControl(
    int ret,
    ObIAllocator *alloc,
    bool alive, bool dead, bool detect,
    bool force_retry_congested,
    bool need_congestion_lookup,
    bool lookup_success,
    bool entry_exist,
    net::ObIpEndpoint svr_addr)
    : ObDiagnosisBase(ObDiagnosisType::CONGESTION_CONTROL, ret, alloc),
      alive_congested_(alive), dead_congested_(dead), detect_congested_(detect),
      force_retry_congested_(force_retry_congested), need_congestion_lookup_(need_congestion_lookup),
      lookup_success_(lookup_success), entry_exist_(entry_exist), svr_addr_(svr_addr) {};
  ~ObDiagnosisCongestionControl() {
    alive_congested_ = false;
    dead_congested_ = false;
    detect_congested_ = false;
    force_retry_congested_ = false;
    need_congestion_lookup_ = false;
    lookup_success_ = false;
    entry_exist_ = false;
    svr_addr_.reset();
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  bool alive_congested_;
  bool dead_congested_;
  bool detect_congested_;
  bool force_retry_congested_;
  bool need_congestion_lookup_;
  bool lookup_success_;
  bool entry_exist_;
  net::ObIpEndpoint svr_addr_;
};
struct ObDiagnosisHandleResponse : public ObDiagnosisBase {
public:
  ObDiagnosisHandleResponse() : ObDiagnosisBase() {};
  explicit ObDiagnosisHandleResponse(
    int ret,
    ObIAllocator *alloc,
    bool is_partition_hit,
    ObMysqlTransact::ObServerSendActionType send_action,
    ObMysqlTransact::ObServerStateType state,
    ObMysqlTransact::ObServerRespErrorType error)
    : ObDiagnosisBase(HANDLE_RESPONSE, ret, alloc),
      is_partition_hit_(is_partition_hit), send_action_(send_action), state_(state), error_(error) {};
  ~ObDiagnosisHandleResponse() {
    is_partition_hit_ = false;
    send_action_ = ObMysqlTransact::SERVER_SEND_NONE;
    state_ = ObMysqlTransact::STATE_UNDEFINED;
    error_ = ObMysqlTransact::MIN_RESP_ERROR;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  bool                                   is_partition_hit_;
  ObMysqlTransact::ObServerSendActionType send_action_;
  ObMysqlTransact::ObServerStateType     state_;
  ObMysqlTransact::ObServerRespErrorType error_;
};

enum ObRetryType {
  INVALID = 0,
  CMNT_TARGET_DB_SERVER,
  CONF_TARGET_DB_SERVER,
  TEST_SERVER_ADDR,
  PARTITION_LOCATION_LOOKUP,
  TRANS_INTERNAL_ROUTING,
  REROUTE,
  NOT_RETRY,
};

class ObDiagnosisRetry : public ObDiagnosisBase {
public:
  ObDiagnosisRetry() : ObDiagnosisBase() {};
  explicit ObDiagnosisRetry(
    int ret,
    ObIAllocator *alloc,
    int64_t attempts,
    ObMysqlTransact::ObSSRetryStatus retry_status,
    ObRetryType retry_type,
    net::ObIpEndpoint &retry_addr)
    : ObDiagnosisBase(RETRY, ret, alloc),
      attempts_(attempts),
      retry_status_(retry_status),
      retry_type_(retry_type),
      retry_addr_(retry_addr) {};
  ~ObDiagnosisRetry() {
    attempts_ = 0;
    retry_status_ = ObMysqlTransact::NO_NEED_RETRY;
    retry_type_ = INVALID;
    retry_addr_.reset();
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  int64_t                          attempts_;
  ObMysqlTransact::ObSSRetryStatus retry_status_;
  ObRetryType                      retry_type_;
  net::ObIpEndpoint                retry_addr_;
};

typedef ObTableEntryLookupState LookupState;
class ObDiagnosisFetchTableRelatedData : public ObDiagnosisBase {
public:
  ObDiagnosisFetchTableRelatedData() : ObDiagnosisBase() {};
  explicit ObDiagnosisFetchTableRelatedData(
    int ret,
    ObIAllocator *alloc,
    int64_t resp_error,
    LookupState fail_at,
    ObTableEntry *table_entry)
    : ObDiagnosisBase(FETCH_TABLE_RELATED_DATA, ret, alloc),
      resp_error_(resp_error),
      fail_at_(fail_at),
      table_entry_(table_entry) {
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->inc_ref();
    }
  };
  // The copy constructor initializes the new ObDiagnosisFetchTableRelatedData with an already existing one
  // So we consider the table_entry_ not refers to some resource
  ObDiagnosisFetchTableRelatedData(const ObDiagnosisFetchTableRelatedData& fetch_table_related_data)
  : ObDiagnosisBase(fetch_table_related_data.type_,
                    fetch_table_related_data.ret_,
                    fetch_table_related_data.alloc_) {
    resp_error_ = fetch_table_related_data.resp_error_;
    fail_at_ = fetch_table_related_data.fail_at_;
    table_entry_ = fetch_table_related_data.table_entry_;
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->inc_ref();
    }
  }

  ObDiagnosisFetchTableRelatedData& operator=(const ObDiagnosisFetchTableRelatedData& fetch_table_related_data) {
    if (this!= &fetch_table_related_data) {
      if (OB_NOT_NULL(table_entry_)) {
        table_entry_->dec_ref();
        table_entry_ = NULL;
      }
      table_entry_ = fetch_table_related_data.table_entry_;
      if (OB_NOT_NULL(table_entry_)) {
        table_entry_->inc_ref();
      }
      ret_ = fetch_table_related_data.ret_;
      alloc_ = fetch_table_related_data.alloc_;
      type_ = fetch_table_related_data.type_;
      resp_error_ = fetch_table_related_data.resp_error_;
      fail_at_ = fetch_table_related_data.fail_at_;
    }
    return *this;
  }
  ~ObDiagnosisFetchTableRelatedData() {
    fail_at_ = LookupState::LOOKUP_TABLE_ENTRY_STATE;
    if (OB_NOT_NULL(table_entry_)) {
      table_entry_->dec_ref();
      table_entry_ = NULL;
    }
    resp_error_= 0;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  int64_t resp_error_;
  LookupState fail_at_;
  ObTableEntry *table_entry_;
};

class ObDiagnosisExprParse : public ObDiagnosisBase {
public:
  ObDiagnosisExprParse() : ObDiagnosisBase() {};
  explicit ObDiagnosisExprParse(
    int ret,
    ObIAllocator *alloc,
    ObString &col_val)
    : ObDiagnosisBase(EXPR_PARSE, ret, alloc),
      col_val_(col_val) {};
  ~ObDiagnosisExprParse() {
    col_val_.reset();
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString col_val_;
};

enum ObRowIDCalcState
{
  SUCCESS = 0,
  RESOLVE_ROWID_TO_OBOBJ,
  DECODE_ROWID,
  GET_PART_ID_FROM_DECODED_ROWID
};

class ObDiagnosisCalcRowid : public ObDiagnosisBase {
public:
  ObDiagnosisCalcRowid() : ObDiagnosisBase() {};
  explicit ObDiagnosisCalcRowid(
    int ret,
    ObIAllocator *alloc,
    ObRowIDCalcState state,
    int16_t version)
    : ObDiagnosisBase(CALC_ROWID, ret, alloc), state_(state), version_(version) {};
  ~ObDiagnosisCalcRowid() {
    state_ = SUCCESS;
    version_ = 0;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObRowIDCalcState state_;
  int16_t version_;
};

class ObDiagnosisResolveToken : public ObDiagnosisBase {
public:
  ObDiagnosisResolveToken() : ObDiagnosisBase() {};
  explicit ObDiagnosisResolveToken(
    int ret,
    ObIAllocator *alloc,
    ObProxyTokenType token_type,
    ObString &token,
    ObProxyExprType expr_type,
    ObProxyExprType generated_func,
    ObObj &obj)
    : ObDiagnosisBase(RESOLVE_TOKEN, ret, alloc) {
      token_type_ = token_type;
      deep_copy_string(alloc_, token, token_);
      expr_type_ = expr_type;
      generated_func_ = generated_func;
      if (obj.need_deep_copy()) {
        char *obj_buf = NULL;
        int64_t obj_buf_size = obj.get_deep_copy_size();
        int64_t pos = 0;
        if (OB_NOT_NULL(obj_buf = (char*) alloc_->alloc(obj_buf_size))) {
          resolved_obj_.deep_copy(obj, obj_buf, obj_buf_size, pos);
        }
      } else {
        resolved_obj_ = obj;
      }
    };
  ~ObDiagnosisResolveToken() {
    resolved_obj_.reset();
    token_.reset();
    token_type_ = ObProxyTokenType::TOKEN_NONE;
    expr_type_ = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
    generated_func_ = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObProxyTokenType token_type_;
  ObString token_;
  ObProxyExprType expr_type_;
  ObProxyExprType generated_func_;
  ObObj resolved_obj_;
};

class ObDiagnosisResolveExpr : public ObDiagnosisBase {
public:
  ObDiagnosisResolveExpr() : ObDiagnosisBase() {};
  explicit ObDiagnosisResolveExpr(
    int ret,
    ObIAllocator *alloc,
    ObString part_range,
    ObString sub_part_range)
    : ObDiagnosisBase(RESOLVE_EXPR, ret, alloc) {
      deep_copy_string(alloc, part_range, part_range_);
      deep_copy_string(alloc, sub_part_range, sub_part_range_);
    };
  ~ObDiagnosisResolveExpr() {
    part_range_.reset();
    sub_part_range_.reset();
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString part_range_;
  ObString sub_part_range_;
};

class ObDiagnosisCalcPartitionId : public ObDiagnosisBase {
public:
  ObDiagnosisCalcPartitionId() : ObDiagnosisBase() {};
  explicit ObDiagnosisCalcPartitionId(
    int ret,
    ObIAllocator *alloc,
    ObPartDesc *part_desc,
    ObPartDesc *sub_part_desc)
    : ObDiagnosisBase(CALC_PARTITION_ID, ret, alloc),
      part_desc_(part_desc), sub_part_desc_(sub_part_desc) {};
  ~ObDiagnosisCalcPartitionId() {
    part_desc_ = NULL;
    sub_part_desc_ = NULL;
  }
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObPartDesc *part_desc_;
  ObPartDesc *sub_part_desc_;
};

union ObDiagnosisPoint {
public:
  ObDiagnosisPoint();
  // we push diagnosis point to ObSEArray
  // called when the ObSEArray call use() or reset()
  // 1. for ObSEArray's local memory call ObDiagnosisPointClearCallBack to call this
  // 2. for ObSEArray's extra memory call this directly
  ~ObDiagnosisPoint();
  // called when push back to ObSEArray
  // this construct assign function will call operator=()
  ObDiagnosisPoint(const ObDiagnosisPoint& odp);
  ObDiagnosisPoint& operator=(const ObDiagnosisPoint& odp);
  int64_t diagnose(char *buf, const int64_t buf_len, int &warn, const char* next_line) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  // Do NOT let ObDiagnosisXXXX have a container member
  ObDiagnosisBase base_;
  ObDiagnosisSqlParse sql_parse_;
  ObDiagnosisRouteInfo route_info_;
  ObDiagnosisLocationCacheLookup location_cache_lookup_;
  ObDiagnosisRoutineEntryLookupDone routine_entry_lookup_done_;
  ObDiagnosisTableEntryLookupDone table_entry_lookup_done_;
  ObDiagnosisPartIDCalcDone partition_id_calc_;
  ObDiagnosisPartEntryLookupDone partition_entry_lookup_done_;
  ObDiagnosisRoutePolicy route_policy_;
  ObDiagnosisCongestionControl congestion_control_;
  ObDiagnosisHandleResponse handle_response_;
  ObDiagnosisRetry retry_connection_;
  ObDiagnosisFetchTableRelatedData fetch_table_related_data_;
  ObDiagnosisExprParse expr_parse_;
  ObDiagnosisCalcRowid calc_rowid_;
  ObDiagnosisResolveToken resolve_token_;
  ObDiagnosisResolveExpr resolve_expr_;
  ObDiagnosisCalcPartitionId calc_partition_id_;
};

// ObSEArray's CallBack for diagnosis point
// will be called in ObSEArray::use() and ::reset()
class ObDiagnosisPointClearCallBack
{
public:
  void operator()(ObDiagnosisPoint *ptr);
};

class ObRouteDiagnosis : public ObSharedRefCount
{
public:
  ObRouteDiagnosis();
  ~ObRouteDiagnosis();
  virtual void free() { op_free(this); }
  inline void set_level(ObDiagnosisLevel level) { level_ = level; }
  inline ObDiagnosisLevel get_level() const { return level_; }
  inline ObIAllocator* get_alloc() { return &allocator_;}
  inline const int64_t get_query_diagnosis_count() const { return query_diagnosis_array_.count(); }
  // get the last pushed diagnosis point and it's type matches
  ObDiagnosisBase* get_last_pushed_diagnosis_point(ObDiagnosisType type);
  // get the last matched diagnosis point ptr
  ObDiagnosisBase* get_last_matched_diagnosis_point(ObDiagnosisType type);
  // whether to generate a diagnosis point
  inline bool is_diagnostic(ObDiagnosisType type) const { return (is_support_explain_route_ || level_ >= get_type_level(type)) && is_request_diagnostic_;}
  /**
   * @brief
   * WARN level for
   * 1. trans first query @param is_trans_first_request
   * 2. distrubte transaction internal routing query @param is_trans_internal_routing
   * TRACE level for:
   * 1. trans query
   * 2. table name is empty [ObRouteDiagnosis::diagnosis_table_entry_lookup_done]
   * 3. query length execeed the maxium [ObRouteDiagnosis::diagnosis_sql_parse]
   * 4. fail to calc partition id [ObRouteDiagnosis::diagnosis_partition_id_calc]
   */
  inline bool need_log_warn(bool is_trans_first_request, bool is_in_trans, bool is_trans_internal_routing) {
    return is_log_warn_ && (is_trans_first_request || (is_in_trans && is_trans_internal_routing));
  }
  // whether need to be output to obproxy_diagnosis.log
  inline bool need_log_to_file(const ObRespAnalyzeResult &resp) {
    return level_ != 0 &&
           !is_support_explain_route_ &&
           is_request_diagnostic_ &&
           (!resp.is_partition_hit() || level_ == DIAGNOSIS_LEVEL_FOUR) &&
           !query_diagnosis_array_.empty();
  }
  // diagnostic request cmd:
  // COM_QUERY/COM_STMT_PREPARE_EXECUTE/COM_STMT_PREPARE/COM_STMT_SEND_PIECE_DATA/
  // COM_STMT_GET_PIECE_DATA/COM_STMT_FETCH/COM_STMT_SEND_LONG_DATA
  inline void set_is_request_diagnostic(const ObProxyMysqlRequest &cli_req);
  // `explain route <sql>` not support
  // COM_STMT_PREPARE/COM_STMT_PREPARE_EXECUTE/COM_STMT_CLOSE/
  // OBPROXY_T_TEXT_PS_PREPARE/OBPROXY_T_TEXT_PS_DROP
  inline void set_is_support_explain_route(const ObProxyMysqlRequest &cli_req);
  // whether need to explain route respsone packet
  inline bool is_support_explain_route() { return is_support_explain_route_; }
  // print to obproxy_diagnosis.log
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // print as result-set of executing 'explain route <your_sql>;'
  int64_t to_explain_route_string(char *buf, const int64_t buf_len) const;
  // call this after transaction's first sql completed,
  // hand the client request's req_buf_ to route diagnosis to
  // free the memory
  void trans_first_query_diagnosed(ObProxyMysqlRequest &);
  // call this after each sql completed
  void trans_cur_query_diagnosed();
  // call this after transaction completed
  void trans_diagnosis_completed();
  // to log kinds of diagnosis point info
  void diagnosis_sql_parse(
    int ret,
    common::ObString sql,
    common::ObString table,
    obmysql::ObMySQLCmd sql_cmd);
  void diagnosis_route_info(
    int ret,
    ObRouteInfoType type,
    net::ObIpEndpoint &svr_addr,
    bool in_trans,
    bool depent_func,
    bool trans_specified);
  void diagnosis_location_cache_lookup(
    int ret,
    obutils::ObServerRoutingMode mode,
    bool need_pl_lookup);
  void diagnosis_routine_entry_lookup_done(
    int ret,
    common::ObString sql,
    bool from_remote,
    bool is_lookup_succ,
    ObRouteEntry::ObRouteEntryState entry_state);
  void diagnosis_table_entry_lookup_done(
    int ret,
    ObTableEntry *table_entry,
    bool entry_from_remote,
    bool is_lookup_succ);
  void diagnosis_partition_id_calc(
    int ret,
    common::ObString parse_sql,
    common::ObString part_name,
    int64_t part_idx,
    int64_t sub_part_idx,
    int64_t partition_id,
    share::schema::ObPartitionLevel lv);
  void diagnosis_partition_entry_lookup_done(
    int ret,
    bool from_remote,
    bool lookup_succ,
    bool has_dup_replica,
    ObRouteEntry::ObRouteEntryState entry_state,
    ObProxyReplicaLocation leader);
  void diagnosis_route_policy(
    int ret,
    bool need_use_dup_replica,
    bool need_check_merge_status,
    bool readonly_zone_exsits,
    bool request_support_readonly_zone,
    ObString proxy_idc_name,
    ObString tenant_primary_zone,
    ObString proxy_primary_zone,
    ObConsistencyLevel session_consistency_level,
    ObConsistencyLevel trans_consistency_level,
    ObRoutePolicyEnum proxy_route_policy,
    ObRoutePolicyEnum session_route_policy,
    ObRoutePolicyEnum route_policy,
    ObRoutePolicyEnum opt_route_policy,
    ObLDCItem chosen_server,
    ObRouteType chosen_route_type);
  void diagnosis_congestion_control(
    int ret, bool alive,
    bool dead, bool detect,
    bool force_retry_congested,
    bool need_congestion_lookup,
    bool lookup_success,
    bool entry_exist,
    net::ObIpEndpoint svr_addr);
  void diagnosis_handle_response(
    int ret,
    bool is_partition_hit,
    ObMysqlTransact::ObServerSendActionType send_action,
    ObMysqlTransact::ObServerStateType state,
    ObMysqlTransact::ObServerRespErrorType error);
  void diagnosis_retry_connection(
    int ret, int64_t attempts,
    ObMysqlTransact::ObSSRetryStatus retry_status,
    ObRetryType retry_type,
    net::ObIpEndpoint &retry_addr);
  void diagnosis_fetch_table_related_data(
    int ret,
    bool resp_error,
    LookupState stage,
    ObTableEntry *table_entry);
  void diagnosis_expr_parse(int ret, ObString col_val);
  void diagnosis_calc_rowid(
    int ret,
    ObRowIDCalcState state,
    int16_t version);
  void diagnosis_resolve_token(
    int ret,
    ObProxyTokenType token_type,
    ObString &token,
    ObProxyExprType expr_type,
    ObProxyExprType generated_func_type,
    ObObj &obj);
  void diagnosis_resolve_expr(
    int ret,
    ObString &part_range,
    ObString &sub_part_range);
  void diagnosis_calc_partition_id(
    int ret,
    ObPartDesc *part_desc,
    ObPartDesc *sub_part_desc);
private:
  // log with WARN or TRACE level
  bool is_log_warn_;
  // if level_ > 0 and use `explain route` to show the most dedicated info
  bool is_support_explain_route_;
  // mark query's cmd whether be able to be diagnosed
  bool is_request_diagnostic_;
  // alter by proxy config 'route_diagnosis_level' [0-4]
  ObDiagnosisLevel level_;
  // only a few query will use below
  common::ObSEArray<
    ObDiagnosisPoint, VALID_DIAGNOSIS_TYPE_NUM,
    ModulePageAllocator, ObDiagnosisPointClearCallBack> query_diagnosis_array_;
  // save the transaction's first sql
  ObString trx_1st_sql_;
  char *trx_1st_req_buf_;
  int64_t trx_1st_req_buf_len_;
  ObArenaAllocator allocator_;
};
inline void ObRouteDiagnosis::diagnosis_sql_parse(
  int ret,
  common::ObString sql,
  common::ObString table,
  obmysql::ObMySQLCmd sql_cmd)
{
  is_log_warn_ = (sql.length() != get_global_proxy_config().request_buffer_length);
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisSqlParse, SQL_PARSE, ret, sql, table, sql_cmd);
}
inline void ObRouteDiagnosis::diagnosis_route_info(
  int ret,
  ObRouteInfoType type,
  net::ObIpEndpoint &svr_addr,
  bool in_trans,
  bool depent_func,
  bool trans_specified)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisRouteInfo, ROUTE_INFO, ret, type, svr_addr, in_trans, depent_func, trans_specified);
}
inline void ObRouteDiagnosis::diagnosis_location_cache_lookup(
  int ret,
  obutils::ObServerRoutingMode mode,
  bool need_pl_lookup)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisLocationCacheLookup, LOCATION_CACHE_LOOKUP, ret, mode, need_pl_lookup);
}
inline void ObRouteDiagnosis::diagnosis_routine_entry_lookup_done(
  int ret,
  common::ObString sql,
  bool from_remote,
  bool is_lookup_succ,
  ObRouteEntry::ObRouteEntryState entry_state)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisRoutineEntryLookupDone, ROUTINE_ENTRY_LOOKUP_DONE, ret,
                         sql, from_remote, is_lookup_succ, entry_state);
}
inline void ObRouteDiagnosis::diagnosis_table_entry_lookup_done(
  int ret,
  ObTableEntry *table_entry,
  bool entry_from_remote,
  bool is_lookup_succ)
{
  is_log_warn_ = (OB_NOT_NULL(table_entry) && !table_entry->get_table_name().empty());
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisTableEntryLookupDone, TABLE_ENTRY_LOOKUP_DONE, ret,
                         table_entry, entry_from_remote, is_lookup_succ);
}
inline void ObRouteDiagnosis::diagnosis_partition_id_calc(
  int ret,
  common::ObString parse_sql,
  common::ObString part_name,
  int64_t part_idx,
  int64_t sub_part_idx,
  int64_t partition_id,
  share::schema::ObPartitionLevel lv)
{
  is_log_warn_ = (partition_id != -1);
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisPartIDCalcDone, PARTITION_ID_CALC_DONE, ret,
                         parse_sql, part_name, part_idx, sub_part_idx, partition_id, lv);
}
inline void ObRouteDiagnosis::diagnosis_partition_entry_lookup_done(
  int ret,
  bool from_remote,
  bool lookup_succ,
  bool has_dup_replica,
  ObRouteEntry::ObRouteEntryState entry_state,
  ObProxyReplicaLocation leader)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisPartEntryLookupDone, PARTITION_ENTRY_LOOKUP_DONE, ret,
                         from_remote, lookup_succ,
                         has_dup_replica, entry_state, leader);
}
inline void ObRouteDiagnosis::diagnosis_route_policy(
  int ret,
  bool need_use_dup_replica,
  bool need_check_merge_status,
  bool readonly_zone_exsits,
  bool request_support_readonly_zone,
  ObString proxy_idc_name,
  ObString tenant_primary_zone,
  ObString proxy_primary_zone,
  ObConsistencyLevel session_consistency_level,
  ObConsistencyLevel trans_consistency_level,
  ObRoutePolicyEnum proxy_route_policy,
  ObRoutePolicyEnum session_route_policy,
  ObRoutePolicyEnum route_policy,
  ObRoutePolicyEnum opt_route_policy,
  ObLDCItem chosen_server,
  ObRouteType chosen_route_type)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisRoutePolicy, ROUTE_POLICY, ret,
                         need_use_dup_replica, need_check_merge_status,
                         readonly_zone_exsits, request_support_readonly_zone,
                         proxy_idc_name, tenant_primary_zone, proxy_primary_zone,
                         session_consistency_level, trans_consistency_level,
                         proxy_route_policy, session_route_policy, route_policy, opt_route_policy,
                         chosen_server, chosen_route_type);
}
inline void ObRouteDiagnosis::diagnosis_congestion_control(
  int ret, bool alive,
  bool dead, bool detect,
  bool force_retry_congested,
  bool need_congestion_lookup,
  bool lookup_success,
  bool entry_exist,
  net::ObIpEndpoint svr_addr)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisCongestionControl, CONGESTION_CONTROL, ret,
                         alive, dead, detect, force_retry_congested,
                         need_congestion_lookup, lookup_success, entry_exist, svr_addr);
}
inline void ObRouteDiagnosis::diagnosis_handle_response(
  int ret,
  bool is_partition_hit,
  ObMysqlTransact::ObServerSendActionType send_action,
  ObMysqlTransact::ObServerStateType state,
  ObMysqlTransact::ObServerRespErrorType error)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisHandleResponse, HANDLE_RESPONSE, ret,
                         is_partition_hit, send_action, state, error);
}
inline void ObRouteDiagnosis::diagnosis_retry_connection(
  int ret, int64_t attempts,
  ObMysqlTransact::ObSSRetryStatus retry_status,
  ObRetryType retry_type,
  net::ObIpEndpoint &retry_addr)
{
  _ROUTE_DIAGNOSIS_POINT(ObDiagnosisRetry, RETRY, ret, attempts, retry_status, retry_type, retry_addr);
}
inline void ObRouteDiagnosis::diagnosis_fetch_table_related_data(
  int ret,
  bool resp_error_code,
  LookupState stage,
  ObTableEntry *table_entry)
{
  _ROUTE_DIAGNOSIS_POINT(
    ObDiagnosisFetchTableRelatedData,
    FETCH_TABLE_RELATED_DATA,
    ret,
    resp_error_code,
    stage,
    table_entry);
}

inline void ObRouteDiagnosis::diagnosis_expr_parse(int ret, ObString col_val)
{
  ObDiagnosisExprParse t(ret, &allocator_, col_val);
  ObDiagnosisPoint *p = reinterpret_cast<ObDiagnosisPoint*>(&t);
  query_diagnosis_array_.push_back(*p);
}

inline void ObRouteDiagnosis::diagnosis_calc_rowid(
  int ret,
  ObRowIDCalcState state,
  int16_t version)
{
  _ROUTE_DIAGNOSIS_POINT(
    ObDiagnosisCalcRowid,
    CALC_ROWID,
    ret,
    state,
    version);
}

inline void ObRouteDiagnosis::diagnosis_resolve_token(
  int ret,
  ObProxyTokenType token_type,
  ObString &token,
  ObProxyExprType expr_type,
  ObProxyExprType generated_func_type,
  ObObj &obj)
{
  _ROUTE_DIAGNOSIS_POINT(
    ObDiagnosisResolveToken,
    RESOLVE_TOKEN,
    ret,
    token_type,
    token,
    expr_type,
    generated_func_type,
    obj);
}

inline void ObRouteDiagnosis::diagnosis_resolve_expr(
  int ret,
  ObString &part_range,
  ObString &sub_part_range)
{
  _ROUTE_DIAGNOSIS_POINT(
    ObDiagnosisResolveExpr,
    RESOLVE_EXPR,
    ret,
    part_range,
    sub_part_range);
}

inline void ObRouteDiagnosis::diagnosis_calc_partition_id(
  int ret,
  ObPartDesc *part_desc,
  ObPartDesc *sub_part_desc)
{
  _ROUTE_DIAGNOSIS_POINT(
    ObDiagnosisCalcPartitionId,
    CALC_PARTITION_ID,
    ret,
    part_desc,
    sub_part_desc);
}

inline void ObRouteDiagnosis::set_is_request_diagnostic(const ObProxyMysqlRequest &cli_req)
{
  is_request_diagnostic_ = !cli_req.is_internal_cmd() &&
                           ((cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_QUERY &&
                             cli_req.get_parse_result().get_stmt_type() != OBPROXY_T_TEXT_PS_DROP) ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_FETCH ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA);
}

inline void ObRouteDiagnosis::set_is_support_explain_route(const ObProxyMysqlRequest &cli_req)
{
  is_support_explain_route_ = cli_req.get_parse_result().has_explain_route() &&
                              !cli_req.is_internal_cmd() &&
                              !(cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE ||
                                cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE ||
                                cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_CLOSE ||
                                cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_RESET ||
                                cli_req.get_parse_result().get_stmt_type() == OBPROXY_T_TEXT_PS_PREPARE ||
                                cli_req.get_parse_result().get_stmt_type() == OBPROXY_T_TEXT_PS_DROP);
}
ObDiagnosisBase* ObRouteDiagnosis::get_last_pushed_diagnosis_point(ObDiagnosisType type) {
  ObDiagnosisBase* ret = NULL;
  if (query_diagnosis_array_.empty()) {
  } else {
    // pick up last one
    ObDiagnosisPoint &p = query_diagnosis_array_.at(query_diagnosis_array_.count() - 1);
    if (p.base_.type_ == type) {
      ret = reinterpret_cast<ObDiagnosisBase*>(&p);
    }
  }
  return ret;
}

ObDiagnosisBase* ObRouteDiagnosis::get_last_matched_diagnosis_point(ObDiagnosisType type) {
  ObDiagnosisBase* ret = NULL;
  if (query_diagnosis_array_.empty()) {
  } else {
    // pick up last one which matched the tyep
    for (int64_t i = query_diagnosis_array_.count() - 1; i >= 0; i--) {
      ObDiagnosisPoint &p = query_diagnosis_array_.at(i);
      if (p.base_.type_ == type) {
        ret = reinterpret_cast<ObDiagnosisBase*>(&p);
        break;
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif

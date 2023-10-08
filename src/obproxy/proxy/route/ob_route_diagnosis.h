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

#define MAX_ROUTE_DIAGNOSIS_ARRAY_COUNT 128

#define ROUTE_DIAGNOSIS(d, type, name, args...)                                           \
  if (OB_NOT_NULL(d) &&                                                                   \
      d->is_diagnostic(type) &&                                                           \
      d->get_diagnosis_point_count() < MAX_ROUTE_DIAGNOSIS_ARRAY_COUNT) {                 \
    d->diagnosis_##name(args);                                                            \
    LOG_DEBUG("route diagnosis point", K(type));                                          \
  }
#define _ROUTE_DIAGNOSIS_POINT(T, type, ret, args...)                             \
  T t(ret, &allocator_, args);                                                    \
  ObDiagPoint *p = reinterpret_cast<ObDiagPoint*>(&t);                            \
  cur_sql_diag_array_.push_back(*p);

#define DIAGNOSIS_POINT_PRINT(content)                      \
  int64_t pos = 0;                                          \
  BUF_PRINTF("%s", get_diagnosis_type_name(type_));         \
  J_COLON();                                                \
  J_OBJ_START();                                            \
  if (ret_ != OB_SUCCESS) {                                 \
    BUF_PRINTF("ret:%d", ret_);                             \
    J_COMMA();                                              \
  }                                                         \
  content;                                                  \
  J_OBJ_END();                                              \
  return pos;

#define FORMAT_PRINT(diag_point)                                     \
  int lv = static_cast<int>(get_type_level(diag_point.base_.type_)); \
  for (int i = 1; i < lv; i++) {                                     \
    BUF_PRINTF("  ");                                                \
  }                                                                  \
  BUF_PRINTF("> ");                                                  \
  pos += diag_point.to_string(buf + pos, buf_len - pos);             \
  BUF_PRINTF(line_separator);

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace obutils;
enum ObDiagLevel {
  ZERO = 0,
  ONE,
  TWO,
  THREE,
};

enum ObDiagType {
  BASE,
  SQL_PARSE,
  ROUTE_INFO,
  LOCATION_CACHE_LOOKUP,
  ROUTINE_ENTRY_LOOKUP,
  TABLE_ENTRY_LOOKUP,
  PARTITION_ID_CALC_DONE,
  PARTITION_ENTRY_LOOKUP,
  ROUTE_POLICY,
  CONGESTION_CONTROL,
  HANDLE_RESPONSE,
  RETRY,
  BASIC_NUM,
  MAX_TYPE,
};

static ObDiagLevel get_type_level(ObDiagType type) {
  ObDiagLevel lv;
  switch(type) {
    case SQL_PARSE:
    case ROUTE_INFO:
    case LOCATION_CACHE_LOOKUP:
    case ROUTE_POLICY:
    case CONGESTION_CONTROL:
    case HANDLE_RESPONSE:
    case RETRY:
      lv = ONE;
      break;
    
    case ROUTINE_ENTRY_LOOKUP:
    case TABLE_ENTRY_LOOKUP:
    case PARTITION_ID_CALC_DONE:
    case PARTITION_ENTRY_LOOKUP:
      lv = TWO;
      break;

    default:
      lv = ZERO;
      break;
  }
  return lv;
}
class ObDiagBase {
public:
  ObDiagBase() : ret_(common::OB_INVALID_ARGUMENT) {}
  explicit ObDiagBase(ObDiagType type, 
                      int ret,
                      ObIAllocator *alloc = NULL) : type_(type), ret_(ret), alloc_(alloc) {}
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual void reset();
  void alloc_then_copy_string(ObString &src, ObString &dest);

  ObDiagType type_;
  int ret_;
  ObIAllocator *alloc_;
};
class ObDiagSqlParse : public ObDiagBase {
public:
  ObDiagSqlParse(): ObDiagBase() {}
  explicit ObDiagSqlParse(int ret,
                          ObIAllocator *alloc,
                          common::ObString sql,
                          common::ObString table,
                          obmysql::ObMySQLCmd sql_cmd) 
    : ObDiagBase(SQL_PARSE, ret, alloc), sql_(sql), 
      table_(table), sql_cmd_(sql_cmd) {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString            sql_;
  ObString            table_;
  obmysql::ObMySQLCmd sql_cmd_;
};

class ObDiagRouteInfo : public ObDiagBase {
public:
  enum RouteInfoType {
    INVALID = 0,
    USE_INDEX,
    USE_OBPROXY_ROUTE_ADDR, 
    USE_CURSOR,
    USE_PIECES,
    USE_CONFIG_TARGET_DB,
    USE_COMMENT_TARGET_DB,
    USE_TEST_SVR_ADDR,
    USE_LAST_SESSION,
    USE_PARTITION_LOCATION_LOOKUP,
    USE_COORDINATOR_SESSION,
  };
  ObDiagRouteInfo() : 
      ObDiagBase(), route_info_type_(INVALID), svr_addr_(), 
      in_transaction_(false), depent_func_(false), trans_specified_(false) {};
  explicit ObDiagRouteInfo(int ret, 
                           ObIAllocator *alloc,
                           RouteInfoType type, 
                           net::ObIpEndpoint &svr_addr, 
                           bool in_trans,
                           bool depent_func,
                           bool trans_specified) 
    : ObDiagBase(ROUTE_INFO, ret, alloc),
      route_info_type_(type), svr_addr_(svr_addr), 
      in_transaction_(in_trans), depent_func_(depent_func), trans_specified_(trans_specified) {};
public:
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
  RouteInfoType     route_info_type_;
  net::ObIpEndpoint svr_addr_;
  bool              in_transaction_;
  bool              depent_func_;
  bool              trans_specified_;
};
class ObDiagLocationCacheLookup : public ObDiagBase {
public:
  ObDiagLocationCacheLookup() : ObDiagBase() {};
  explicit ObDiagLocationCacheLookup(int ret,
                                     ObIAllocator *alloc,
                                     obutils::ObServerRoutingMode mode,
                                     bool need_pl_lookup)
    : ObDiagBase(ObDiagType::LOCATION_CACHE_LOOKUP, ret, alloc),
      mode_(mode), need_partition_location_lookup_(need_pl_lookup) {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  obutils::ObServerRoutingMode  mode_;
  bool                          need_partition_location_lookup_;
};

class ObDiagRoutineEntryLookup : public ObDiagBase {
public:
  ObDiagRoutineEntryLookup() : ObDiagBase() {};
  explicit ObDiagRoutineEntryLookup(int ret, 
                                    ObIAllocator *alloctor,
                                    ObString sql,
                                    bool from_remote,
                                    bool is_lookup_succ,
                                    ObRouteEntry::ObRouteEntryState entry_state)
    : ObDiagBase(ObDiagType::ROUTINE_ENTRY_LOOKUP, ret, alloctor),
      from_remote_(from_remote), is_lookup_succ_(is_lookup_succ), entry_state_(entry_state) { 
    alloc_then_copy_string(sql, routine_sql_);
  };
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                         routine_sql_;
  bool                             from_remote_;
  bool                             is_lookup_succ_;
  ObRouteEntry::ObRouteEntryState  entry_state_;
};

class ObDiagTableEntryLookup : public ObDiagBase {
public:
  ObDiagTableEntryLookup() : ObDiagBase() {};
  explicit ObDiagTableEntryLookup(int ret, 
                                  ObIAllocator *alloc,
                                  common::ObString table,
                                  uint64_t table_id,
                                  int64_t part_num,
                                  share::schema::ObTableType table_type,
                                  ObRouteEntry::ObRouteEntryState entry_state,
                                  bool entry_from_remote,
                                  bool has_dup_replica,
                                  bool is_lookup_succ)
    : ObDiagBase(ObDiagType::TABLE_ENTRY_LOOKUP, ret, alloc),
      table_id_(table_id), part_num_(part_num), table_type_(table_type),
      entry_state_(entry_state), entry_from_remote_(entry_from_remote),
      has_dup_replica_(has_dup_replica), is_lookup_succ_(is_lookup_succ) {
    alloc_then_copy_string(table, table_);
  };
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                        table_;
  uint64_t                        table_id_;
  int64_t                         part_num_;
  share::schema::ObTableType      table_type_;
  ObRouteEntry::ObRouteEntryState entry_state_;
  bool                            entry_from_remote_;
  bool                            has_dup_replica_;
  bool                            is_lookup_succ_;
};

class ObDiagPartIDCalcDone : public ObDiagBase {
public:
  ObDiagPartIDCalcDone() : ObDiagBase() {};
  explicit ObDiagPartIDCalcDone(int ret,
                                ObIAllocator *alloc,
                                common::ObString parse_sql,
                                common::ObString part_name,
                                int64_t part_idx,
                                int64_t sub_part_idx,
                                share::schema::ObPartitionLevel lv)
    : ObDiagBase(ObDiagType::PARTITION_ID_CALC_DONE, ret, alloc), parse_sql_(parse_sql), part_name_(part_name),
      part_idx_(part_idx), sub_part_idx_(sub_part_idx), level_(lv) {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                        parse_sql_;
  ObString                        part_name_;
  int64_t                         part_idx_;
  int64_t                         sub_part_idx_;
  share::schema::ObPartitionLevel level_;
};

class ObDiagPartEntryLookup : public ObDiagBase {
public:
  ObDiagPartEntryLookup() : ObDiagBase() {};
  explicit ObDiagPartEntryLookup(int ret,
                                 ObIAllocator *alloc,
                                 uint64_t part_id,
                                 bool from_remote,
                                 bool lookup_succ,
                                 bool has_dup_replica,
                                 ObRouteEntry::ObRouteEntryState entry_state,
                                 ObProxyReplicaLocation leader)
    : ObDiagBase(ObDiagType::PARTITION_ENTRY_LOOKUP, ret, alloc),
      part_id_(part_id), from_remote_(from_remote), is_lookup_succ_(lookup_succ),
      has_dup_replica_(has_dup_replica), entry_state_(entry_state), leader_(leader) {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  uint64_t                        part_id_;
  bool                            from_remote_;
  bool                            is_lookup_succ_;
  bool                            has_dup_replica_;
  ObRouteEntry::ObRouteEntryState entry_state_;
  ObProxyReplicaLocation          leader_;
};
class ObDiagRoutePolicy : public ObDiagBase {
public:
  ObDiagRoutePolicy() : ObDiagBase() {};
  explicit ObDiagRoutePolicy(int ret,
                             ObIAllocator *alloc,
                             common::ObConsistencyLevel consistency_level,
                             common::ObString cur_idc_name,
                             common::ObString primary_zone,
                             common::ObString proxy_primary_zone,
                             ObRoutePolicyEnum route_policy, 
                             ObRouteType chosen_route_type,
                             ObLDCItem chosen_server)
    : ObDiagBase(ObDiagType::ROUTE_POLICY, ret, alloc),
      cur_idc_name_(cur_idc_name), primary_zone_(primary_zone),
      proxy_primary_zone_(proxy_primary_zone), consistency_level_(consistency_level),
      route_policy_(route_policy), chosen_route_type_(chosen_route_type), chosen_server_(chosen_server) {
    alloc_then_copy_string(cur_idc_name, cur_idc_name_);
    alloc_then_copy_string(primary_zone, primary_zone_);
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObString                   cur_idc_name_;
  ObString                   primary_zone_;
  ObString                   proxy_primary_zone_;
  ObConsistencyLevel         consistency_level_;
  ObRoutePolicyEnum          route_policy_;
  ObRouteType                chosen_route_type_;
  ObLDCItem                  chosen_server_;
 };

class ObDiagCongestionControl : public ObDiagBase {
public:
  ObDiagCongestionControl() : ObDiagBase() {};
  explicit ObDiagCongestionControl(int ret, 
                                   ObIAllocator *alloc,
                                   bool alive, bool dead, bool detect, 
                                   bool force_retry_congested,
                                   bool need_congestion_lookup,
                                   bool lookup_success,
                                   bool entry_exist)
    : ObDiagBase(ObDiagType::CONGESTION_CONTROL, ret, alloc),
      alive_congested_(alive), dead_congested_(dead), detect_congested_(detect), 
      force_retry_congested_(force_retry_congested), need_congestion_lookup_(need_congestion_lookup),
      lookup_success_(lookup_success), entry_exist_(entry_exist) {};
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
};
struct ObDiagHandleResponse : public ObDiagBase {
public:
  ObDiagHandleResponse() : ObDiagBase() {};
  explicit ObDiagHandleResponse(int ret,
                                ObIAllocator *alloc,
                                bool is_partition_hit,
                                ObMysqlTransact::ObServerStateType state,
                                ObMysqlTransact::ObServerRespErrorType error) 
    : ObDiagBase(HANDLE_RESPONSE, ret, alloc),
      is_partition_hit_(is_partition_hit), state_(state), error_(error) {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  bool                                   is_partition_hit_;
  ObMysqlTransact::ObServerStateType     state_;
  ObMysqlTransact::ObServerRespErrorType error_;
};

class ObDiagRetry : public ObDiagBase {
public:
  enum RetryType {
    INVALID = 0,
    CMNT_TARGET_DB_SERVER,
    CONF_TARGET_DB_SERVER,
    TEST_SERVER_ADDR,
    USE_PARTITION_LOCATION_LOOKUP,
    TRANS_INTERNAL_ROUTING,
    REROUTE,
  };
  ObDiagRetry() : ObDiagBase() {};
  explicit ObDiagRetry(int ret, 
                       ObIAllocator *alloc,
                       int64_t attempts,
                       ObMysqlTransact::ObSSRetryStatus retry_status,
                       RetryType retry_type,
                       net::ObIpEndpoint &retry_addr)
    : ObDiagBase(RETRY, ret, alloc),
      attempts_(attempts), 
      retry_status_(retry_status),
      retry_type_(retry_type),
      retry_addr_(retry_addr) {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  int64_t                          attempts_;
  ObMysqlTransact::ObSSRetryStatus retry_status_;
  RetryType                        retry_type_;
  net::ObIpEndpoint                retry_addr_;
};
union ObDiagPoint {
public:
  ObDiagPoint();
  ~ObDiagPoint();
  ObDiagPoint(const ObDiagPoint& odp);
  ObDiagPoint& operator=(const ObDiagPoint& odp);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public:
  ObDiagBase                base_;
  ObDiagSqlParse            sql_parse_;
  ObDiagRouteInfo           route_info_;
  ObDiagLocationCacheLookup location_cache_lookup_;
  ObDiagRoutineEntryLookup  routine_entry_lookup_;
  ObDiagTableEntryLookup    table_entry_lookup_;
  ObDiagPartIDCalcDone      partition_id_calc_;
  ObDiagPartEntryLookup     partition_entry_lookup_;
  ObDiagRoutePolicy         route_policy_;
  ObDiagCongestionControl   congestion_control_;
  ObDiagHandleResponse      handle_response_;
  ObDiagRetry               retry_connection_;
};

class ObRouteDiagnosis 
{
public:
  ObRouteDiagnosis();
  ~ObRouteDiagnosis();
  inline void set_level(ObDiagLevel level) { level_ = level; }
  inline ObDiagLevel get_level() const { return level_; }
  inline int64_t get_diagnosis_point_count() const { return cur_sql_diag_array_.count(); }
  // whether to generate a diagnosis point
  inline bool is_diagnostic(ObDiagType type) const { return (is_support_explain_route_ || level_ >= get_type_level(type)) && is_request_diagnostic_;}
  // whether need to be output to obproxy_diagnosis.log
  inline bool need_log() { return level_ != 0 && !is_support_explain_route_ && is_request_diagnostic_; }
  // diagnostic request cmd: 
  // OB_MYSQL_COM_QUERY/COM_STMT_PREPARE_EXECUTE/COM_STMT_PREPARE/COM_STMT_SEND_PIECE_DATA/
  // OB_MYSQL_COM_STMT_GET_PIECE_DATA/COM_STMT_FETCH/COM_STMT_SEND_LONG_DATA
  inline void set_is_request_diagnostic(const ObProxyMysqlRequest &cli_req);
  // `explain route <sql>` not support 
  // OB_MYSQL_COM_STMT_PREPARE/COM_STMT_PREPARE_EXECUTE/COM_STMT_CLOSE/
  // OBPROXY_T_TEXT_PS_PREPARE/OBPROXY_T_TEXT_PS_DROP
  inline void set_is_support_explain_route(const ObProxyMysqlRequest &cli_req);
  // whether need to explain route respsone packet
  inline bool is_support_explain_route() { return is_support_explain_route_; }
  // print to obproxy_diagnosis.log
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // print as result-set of executing 'explain route <your_sql>;'
  int64_t to_format_string(char *buf, const int64_t buf_len) const;
  // call this after transaction's first sql completed,
  // hand the client request's req_buf_ to route diagnosis to 
  // free the memory
  void trans_first_sql_diagnosed(ObProxyMysqlRequest &);
  // call this after each sql completed
  void trans_cur_sql_diagnosed();
  // call this after transaction completed
  void trans_diagnosis_completed();
  // to log kinds of diagnosis point info
  void diagnosis_sql_parse(int ret,
                           common::ObString sql,
                           common::ObString table,
                           obmysql::ObMySQLCmd sql_cmd);
  void diagnosis_route_info(int ret, 
                            ObDiagRouteInfo::RouteInfoType type, 
                            net::ObIpEndpoint &svr_addr, 
                            bool in_trans,
                            bool depent_func,
                            bool trans_specified);
  void diagnosis_location_cache_lookup(int ret,
                                       obutils::ObServerRoutingMode mode,
                                       bool need_pl_lookup); 
  void diagnosis_routine_entry_lookup(int ret, 
                                      common::ObString sql,
                                      bool from_remote,
                                      bool is_lookup_succ,
                                      ObRouteEntry::ObRouteEntryState entry_state);
  void diagnosis_table_entry_lookup(int ret, 
                                    common::ObString table_name,
                                    uint64_t table_id,
                                    int64_t part_num,
                                    share::schema::ObTableType table_type,
                                    ObRouteEntry::ObRouteEntryState entry_state,
                                    bool entry_from_remote,
                                    bool has_dup_replica,
                                    bool is_lookup_succ);
  void diagnosis_partition_id_calc(int ret,
                                   common::ObString parse_sql,
                                   common::ObString part_name,
                                   int64_t part_idx,
                                   int64_t sub_part_idx,
                                   share::schema::ObPartitionLevel lv);
  void diagnosis_partition_entry_lookup(int ret,
                                        uint64_t part_id,
                                        bool from_remote,
                                        bool lookup_succ,
                                        bool has_dup_replica,
                                        ObRouteEntry::ObRouteEntryState entry_state,
                                        ObProxyReplicaLocation leader); 
  void diagnosis_route_policy(int ret,
                              common::ObConsistencyLevel consistency_level,
                              common::ObString cur_idc_name,
                              common::ObString primary_zone,
                              common::ObString proxy_primary_zone,
                              ObRoutePolicyEnum route_policy,
                              ObRouteType chosen_route_type,
                              ObLDCItem chosen_server);
  void diagnosis_congestion_control(int ret, bool alive, 
                                    bool dead, bool detect, 
                                    bool force_retry_congested,
                                    bool need_congestion_lookup,
                                    bool lookup_success,
                                    bool entry_exist);
  void diagnosis_handle_response(int ret,
                                 bool is_partition_hit,
                                 ObMysqlTransact::ObServerStateType state,
                                 ObMysqlTransact::ObServerRespErrorType error);
  void diagnosis_retry_connection(int ret, int64_t attempts,
                                  ObMysqlTransact::ObSSRetryStatus retry_status,
                                  ObDiagRetry::RetryType retry_type,
                                  net::ObIpEndpoint &retry_addr);
private:
  // if level_ > 0 and use `explain route` to show the most dedicated info
  bool is_support_explain_route_;
  // mark query's cmd whether be able to be diagnosed
  bool is_request_diagnostic_;
  // alter by proxy config 'route_diagnosis_level' [0-4]
  ObDiagLevel level_;
  // only a few query will use below
  common::ObSEArray<ObDiagPoint, BASIC_NUM> cur_sql_diag_array_;
  // save the transaction's first sql
  ObString trx_1st_sql_;
  char *trx_1st_req_buf_;
  int64_t trx_1st_req_buf_len_;
  ObArenaAllocator allocator_;
};
inline void ObRouteDiagnosis::diagnosis_sql_parse(int ret,
                                                  common::ObString sql,
                                                  common::ObString table,
                                                  obmysql::ObMySQLCmd sql_cmd) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagSqlParse, SQL_PARSE, ret, sql, table, sql_cmd);
}
inline void ObRouteDiagnosis::diagnosis_route_info(int ret, 
                                                   ObDiagRouteInfo::RouteInfoType type, 
                                                   net::ObIpEndpoint &svr_addr, 
                                                   bool in_trans,
                                                   bool depent_func,
                                                   bool trans_specified) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagRouteInfo, ROUTE_INFO, ret, type, svr_addr, in_trans, depent_func, trans_specified);
}
inline void ObRouteDiagnosis::diagnosis_location_cache_lookup(int ret,
                                                              obutils::ObServerRoutingMode mode,
                                                              bool need_pl_lookup) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagLocationCacheLookup, LOCATION_CACHE_LOOKUP, ret, mode, need_pl_lookup);
}
inline void ObRouteDiagnosis::diagnosis_routine_entry_lookup(int ret, 
                                                             common::ObString sql,
                                                             bool from_remote,
                                                             bool is_lookup_succ,
                                                             ObRouteEntry::ObRouteEntryState entry_state) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagRoutineEntryLookup, ROUTINE_ENTRY_LOOKUP, ret,
                         sql, from_remote, is_lookup_succ, entry_state);
}
inline void ObRouteDiagnosis::diagnosis_table_entry_lookup(int ret, 
                                                           common::ObString table_name,
                                                           uint64_t table_id,
                                                           int64_t part_num,
                                                           share::schema::ObTableType table_type,
                                                           ObRouteEntry::ObRouteEntryState entry_state,
                                                           bool entry_from_remote,
                                                           bool has_dup_replica,
                                                           bool is_lookup_succ) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagTableEntryLookup, TABLE_ENTRY_LOOKUP, ret,
                         table_name, table_id, part_num, table_type, entry_state,
                         entry_from_remote, has_dup_replica, is_lookup_succ);
}
inline void ObRouteDiagnosis::diagnosis_partition_id_calc(int ret,
                                                          common::ObString parse_sql,
                                                          common::ObString part_name,
                                                          int64_t part_idx,
                                                          int64_t sub_part_idx,
                                                          share::schema::ObPartitionLevel lv) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagPartIDCalcDone, PARTITION_ID_CALC_DONE, ret,
                         parse_sql, part_name, part_idx, sub_part_idx, lv);
}
inline void ObRouteDiagnosis::diagnosis_partition_entry_lookup(int ret,
                                                               uint64_t part_id,
                                                               bool from_remote,
                                                               bool lookup_succ,
                                                               bool has_dup_replica,
                                                               ObRouteEntry::ObRouteEntryState entry_state,
                                                               ObProxyReplicaLocation leader) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagPartEntryLookup, PARTITION_ENTRY_LOOKUP, ret, 
                         part_id, from_remote, lookup_succ, 
                         has_dup_replica, entry_state, leader);
}
inline void ObRouteDiagnosis::diagnosis_route_policy(int ret,
                                                     common::ObConsistencyLevel consistency_level,
                                                     common::ObString cur_idc_name,
                                                     common::ObString primary_zone,
                                                     common::ObString proxy_primary_zone,
                                                     ObRoutePolicyEnum route_policy,
                                                     ObRouteType chosen_route_type,
                                                     ObLDCItem chosen_server) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagRoutePolicy, ROUTE_POLICY, ret, 
                         consistency_level, cur_idc_name, primary_zone, 
                         proxy_primary_zone, route_policy, chosen_route_type,
                         chosen_server);
}
inline void ObRouteDiagnosis::diagnosis_congestion_control(int ret, bool alive, 
                                                           bool dead, bool detect, 
                                                           bool force_retry_congested,
                                                           bool need_congestion_lookup,
                                                           bool lookup_success,
                                                           bool entry_exist) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagCongestionControl, CONGESTION_CONTROL, ret, 
                         alive, dead, detect, force_retry_congested, 
                         need_congestion_lookup, lookup_success, entry_exist);
}
inline void ObRouteDiagnosis::diagnosis_handle_response(int ret,
                                                        bool is_partition_hit,
                                                        ObMysqlTransact::ObServerStateType state,
                                                        ObMysqlTransact::ObServerRespErrorType error) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagHandleResponse, HANDLE_RESPONSE, ret,
                         is_partition_hit, state, error);
}
inline void ObRouteDiagnosis::diagnosis_retry_connection(int ret, int64_t attempts,
                                                         ObMysqlTransact::ObSSRetryStatus retry_status,
                                                         ObDiagRetry::RetryType retry_type,
                                                         net::ObIpEndpoint &retry_addr) {
  _ROUTE_DIAGNOSIS_POINT(ObDiagRetry, RETRY, ret, attempts, retry_status, retry_type, retry_addr);
}

inline void ObRouteDiagnosis::set_is_request_diagnostic(const ObProxyMysqlRequest &cli_req)
{
  is_request_diagnostic_ = (cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_QUERY || 
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_FETCH ||
                            cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA);
}

inline void ObRouteDiagnosis::set_is_support_explain_route(const ObProxyMysqlRequest &cli_req)
{
  is_support_explain_route_ = cli_req.get_parse_result().has_explain_route();
  if (is_support_explain_route_) {
    // for ps, only support execute
    is_support_explain_route_ = !(cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE ||
                          cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE ||
                          cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_CLOSE ||
                          cli_req.get_packet_meta().cmd_ == obmysql::OB_MYSQL_COM_STMT_RESET ||
                          cli_req.get_parse_result().get_stmt_type() == OBPROXY_T_TEXT_PS_PREPARE ||
                          cli_req.get_parse_result().get_stmt_type() == OBPROXY_T_TEXT_PS_DROP);
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif

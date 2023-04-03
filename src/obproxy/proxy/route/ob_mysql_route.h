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

#ifndef OBPROXY_MYSQL_ROUTE_H
#define OBPROXY_MYSQL_ROUTE_H

#include "iocore/eventsystem/ob_action.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_partition_entry.h"
#include "proxy/route/ob_routine_entry.h"
#include "obutils/ob_proxy_sql_parser.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObProxyCallInfo;
}
namespace proxy
{
class ObMysqlRoute;
class ObPartitionEntry;
class ObProxyMysqlRequest;
class ObClientSessionInfo;
class ObServerRoute;
typedef int (ObMysqlRoute::*MysqlRouteHandler)(int event, void *data);

class ObMysqlRouteResult
{
public:
  ObMysqlRouteResult()
    : table_entry_(NULL), part_entry_(NULL),
      is_table_entry_from_remote_(false),
      is_partition_entry_from_remote_(false),
      has_dup_replica_(false) {}
  ~ObMysqlRouteResult() {}
  void reset();
  void ref_reset();

  ObTableEntry *table_entry_;
  ObPartitionEntry *part_entry_;

  bool is_table_entry_from_remote_;
  bool is_partition_entry_from_remote_;
  bool has_dup_replica_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRouteResult);
};

inline void ObMysqlRouteResult::reset()
{
  table_entry_ = NULL;
  part_entry_ = NULL;
  is_table_entry_from_remote_ = false;
  is_partition_entry_from_remote_ = false;
  has_dup_replica_ = false;
}

inline void ObMysqlRouteResult::ref_reset()
{
  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }
  if (NULL != part_entry_) {
    part_entry_->dec_ref();
    part_entry_ = NULL;
  }
  is_table_entry_from_remote_ = false;
  is_partition_entry_from_remote_ = false;
  has_dup_replica_ = false;
}

class ObRouteParam
{
public:
  ObRouteParam()
    : cont_(NULL), name_(), force_renew_(false), use_lower_case_name_(false),
      is_partition_table_route_supported_(false), need_pl_route_(false), is_oracle_mode_(false),
      is_need_force_flush_(false), result_(), mysql_proxy_(NULL), client_request_(NULL), client_info_(NULL),
      route_(NULL), cr_version_(-1), cr_id_(-1), tenant_version_(0), timeout_us_(-1), current_idc_name_(),
      cluster_version_(0), cr_(NULL) {}
  ~ObRouteParam() { reset(); }

  void reset();
  bool is_valid() const;
  void set_cluster_resource(obutils::ObClusterResource *cr);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  event::ObContinuation *cont_;
  ObTableEntryName name_;
  // current, for partition table, force_renew will update
  // both table entry and partition entry from remote; indeedly,
  // only update partition entry is enough; TODO
  bool force_renew_;
  bool use_lower_case_name_;
  bool is_partition_table_route_supported_;
  bool need_pl_route_;// whether try pl route
  bool is_oracle_mode_;
  bool is_need_force_flush_;
  ObMysqlRouteResult result_;
  ObMysqlProxy *mysql_proxy_;
  ObProxyMysqlRequest *client_request_;
  ObClientSessionInfo *client_info_;
  ObServerRoute *route_;
  int64_t cr_version_;
  int64_t cr_id_;
  uint64_t tenant_version_;
  int64_t timeout_us_;
  common::ObString current_idc_name_;
  char current_idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];
  int64_t cluster_version_;

private:
  // for defense, ensure mysql_proxy_ is safely used
  obutils::ObClusterResource *cr_;

private:
   DISALLOW_COPY_AND_ASSIGN(ObRouteParam);
};

inline bool ObRouteParam::is_valid() const
{
  return (NULL != cont_)
          && (cr_version_ >= 0)
          && (cr_id_ >= 0)
          && (timeout_us_ > 0)
          && (name_.is_valid())
          && (NULL != mysql_proxy_);
}

inline void ObRouteParam::reset()
{
  cont_ = NULL;
  name_.reset();
  result_.reset();
  force_renew_ = false;
  use_lower_case_name_ = false;
  is_oracle_mode_ = false;
  is_need_force_flush_ = false;
  mysql_proxy_ = NULL;
  client_request_ = NULL;
  route_ = NULL;
  cr_version_ = -1;
  cr_id_ = -1;
  tenant_version_ = 0;
  timeout_us_ = -1;
  set_cluster_resource(NULL);
  current_idc_name_.reset();
  cluster_version_ = 0;
}

// all route related work, include table entry lookup, sql fast parse,
// part id calc, partition entry lookup, etc.
class ObMysqlRoute : public event::ObContinuation
{
public:
  enum ObMysqlRouteAction
  {
    ROUTE_ACTION_UNDEFINED = 0,

    ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_START,
    ROUTE_ACTION_ROUTINE_ENTRY_LOOKUP_DONE,
    ROUTE_ACTION_ROUTE_SQL_PARSE_START,
    ROUTE_ACTION_ROUTE_SQL_PARSE_DONE,

    ROUTE_ACTION_TABLE_ENTRY_LOOKUP_START,
    ROUTE_ACTION_TABLE_ENTRY_LOOKUP_DONE,
    ROUTE_ACTION_PARTITION_ID_CALC_START,
    ROUTE_ACTION_PARTITION_ID_CALC_DONE,
    ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_START,
    ROUTE_ACTION_PARTITION_ENTRY_LOOKUP_DONE,

    ROUTE_ACTION_NOTIFY_OUT,
    ROUTE_ACTION_TIMEOUT,

    ROUTE_ACTION_UNEXPECTED_NOOP,
  };

  ObMysqlRoute();
  virtual ~ObMysqlRoute() {}

  static int get_route_entry(ObRouteParam &route_param,
                             obutils::ObClusterResource *cr,
                             event::ObAction *&action);

private:
  static ObMysqlRoute *allocate_route();
  static void free_route(ObMysqlRoute *mysql_route);

  static const char *get_action_str(const ObMysqlRouteAction action);
  int deep_copy_route_param(ObRouteParam &param);
  void set_cluster_resource(obutils::ObClusterResource *cr) { param_.set_cluster_resource(cr); }
  void rewrite_route_names(const common::ObString &db_name, const common::ObString &table_name);
  int check_and_rebuild_call_params();

  int init(ObRouteParam &route_param);
  void kill_this();

  int state_route_start(int event, void *data);

  int main_handler(int event, void *data);
  void set_state_and_call_next(const ObMysqlRouteAction next_action);
  void call_next_action();

  void setup_routine_entry_lookup();
  int state_routine_entry_lookup(int event, void *data);
  void handle_routine_entry_lookup_done();

  void setup_route_sql_parse();
  int state_route_sql_parse(int event, void *data);
  void handle_route_sql_parse_done();

  void setup_table_entry_lookup();
  int state_table_entry_lookup(int event, void *data);
  void handle_table_entry_lookup_done();

  void setup_partition_id_calc();
  int state_partition_id_calc(int event, void *data);
  void handle_partition_id_calc_done();

  void setup_partition_entry_lookup();
  int state_partition_entry_lookup(int event, void *data);
  void handle_partition_entry_lookup_done();

  void handle_timeout();
  void notify_caller();
  int state_notify_caller(int event, void *data);

  // any error will enter this func
  void setup_error_route();

  int schedule_timeout_action();
  int cancel_pending_action();
  int cancel_timeout_action();

private:
  ObContMagic magic_;

  ObRouteParam param_;
  char *name_buf_;
  int64_t name_buf_len_;

  event::ObEThread *submit_thread_;
  event::ObAction action_;
  event::ObAction *pending_action_;
  event::ObAction *timeout_action_;

  ObRoutineEntry *routine_entry_;
  ObTableEntry *table_entry_;
  ObPartitionEntry *part_entry_;
  ObMysqlRouteAction next_action_;

  bool is_routine_entry_from_remote_;
  bool is_table_entry_from_remote_;
  bool is_part_entry_from_remote_;

  bool is_routine_entry_lookup_succ_;
  bool is_route_sql_parse_succ_;
  bool is_table_entry_lookup_succ_;
  bool is_part_id_calc_succ_;
  bool is_part_entry_lookup_succ_;
  bool terminate_route_;
  // observer use int64_t to store part id
  int64_t part_id_;
  obutils::ObSqlParseResult route_sql_result_;

  MysqlRouteHandler default_handler_;
  int32_t reentrancy_count_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlRoute);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_ROUTE_H

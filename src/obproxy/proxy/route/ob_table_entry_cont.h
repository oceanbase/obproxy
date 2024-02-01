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

#ifndef OBPROXY_TABLE_ENTRY_CONT_H
#define OBPROXY_TABLE_ENTRY_CONT_H

#include "iocore/eventsystem/ob_action.h"
#include "obutils/ob_async_common_task.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/client/ob_client_vc.h"


#define TABLE_ENTRY_LOOKUP_CACHE_EVENT (ROUTE_EVENT_EVENTS_START + 1)
#define TABLE_ENTRY_LOOKUP_REMOTE_EVENT (ROUTE_EVENT_EVENTS_START + 2)
#define TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT (ROUTE_EVENT_EVENTS_START + 3)
#define TABLE_ENTRY_NOTIFY_CALLER_EVENT (ROUTE_EVENT_EVENTS_START + 4)
#define TABLE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT (ROUTE_EVENT_EVENTS_START + 5)

namespace oceanbase
{
namespace obproxy
{
class ObResultSetFetcher;

namespace proxy
{
enum ObTableEntryLookupOp
{
  LOOKUP_MIN_OP = 0,
  LOOKUP_REMOTE_DIRECT_OP,
  LOOKUP_REMOTE_FOR_UPDATE_OP,
  LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP,
  RETRY_LOOKUP_GLOBAL_CACHE_OP,
  LOOKUP_PUSH_INTO_PENDING_LIST_OP,
  LOOKUP_GLOBAL_CACHE_HIT_OP,
  LOOKUP_FIRST_PART_OP,
};

enum ObTableEntryLookupState
{
  LOOKUP_TABLE_ENTRY_STATE = 0,
  LOOKUP_PART_INFO_STATE,
  LOOKUP_FIRST_PART_STATE,
  LOOKUP_SUB_PART_STATE,
  LOOKUP_BINLOG_ENTRY_STATE,
  LOOKUP_DONE_STATE,
};

enum
{
  OB_TABLE_ENTRY_CONT_MAGIC_ALIVE = 0xAABBCCDD,
  OB_TABLE_ENTRY_CONT_MAGIC_DEAD = 0xDDCCBBAA
};

struct ObRouteResult
{
  ObRouteResult() : target_entry_(NULL), target_old_entry_(NULL), is_from_remote_(false), is_need_force_flush_(false) {}
  ~ObRouteResult() {};
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObTableEntry *target_entry_;
  ObTableEntry *target_old_entry_;
  bool is_from_remote_;
  bool is_need_force_flush_;
};

class ObMysqlProxy;
class ObTableCache;
class ObTableRouteParam
{
public:
  ObTableRouteParam() : route_diagnosis_(NULL) { reset(); }
  ~ObTableRouteParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool need_fetch_remote() const { return force_renew_ && (!name_.is_sys_dummy()); }
  void set_route_diagnosis(ObRouteDiagnosis *route_diagnosis);


  event::ObContinuation *cont_;
  ObTableEntryName name_;
  int64_t cr_version_;
  int64_t cr_id_;
  int64_t cluster_version_;
  uint64_t tenant_version_;
  bool is_partition_table_route_supported_;
  bool force_renew_;
  bool is_oracle_mode_;
  bool is_need_force_flush_;
  ObRouteResult result_;
  ObMysqlProxy *mysql_proxy_;
  common::ObString current_idc_name_;
  char current_idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];
  common::ObString binlog_service_ip_;
  ObRouteDiagnosis *route_diagnosis_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRouteParam);
};

class ObTableEntryCont : public obutils::ObAsyncCommonTask
{
public:
  ObTableEntryCont();
  virtual ~ObTableEntryCont() {}
  virtual void destroy() { kill_this(); }
  void kill_this();

  int init(ObTableCache &table_cache, ObTableRouteParam &table_param, ObTableEntry *table_entry);
  void set_table_entry_op(const ObTableEntryLookupOp op) { te_op_ = op; }
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }
  static const char *get_state_name(const ObTableEntryLookupState state);

private:
  int main_handler(int event, void *data);
  static const char *get_event_name(const int64_t event);

  int schedule_imm(event::ObContinuation *cont, const int event);
  int schedule_in(event::ObContinuation *cont, const ObHRTime atimeout_in, const int event);

  int deep_copy_table_param(ObTableRouteParam &param);

  int lookup_entry_in_cache();

  int lookup_entry_remote(); // __all_virtual_proxy_schema
  int lookup_part_info_remote(); // __all_virtual_proxy_partition_info
  int lookup_first_part_remote(); // __all_virtural_proxy_partition
  int lookup_sub_part_remote(); // __all_virtual_proxy_sub_partition

  int set_next_state();

  int handle_client_resp(void *data);
  int handle_table_entry_resp(ObResultSetFetcher &rs_fetcher);
  int handle_part_info_resp(ObResultSetFetcher &rs_fetcher);
  int handle_first_part_resp(ObResultSetFetcher &rs_fetcher);
  int handle_sub_part_resp(ObResultSetFetcher &rs_fetcher);
  int handle_lookup_remote();
  int handle_lookup_remote_done();
  int handle_binlog_entry_resp(ObResultSetFetcher &rs_fetcher);

  int handle_lookup_remote_for_update();
  int add_to_global_cache(bool &add_succ);
  int replace_building_state_entry(bool &is_replaced);
  int handle_chain_notify_caller();
  int notify_caller();

  bool is_newest_table_entry_valid() const;
public:
  const static int64_t SCHEDULE_TABLE_ENTRY_LOOKUP_INTERVAL = HRTIME_MSECONDS(1); // 1ms

private:
  uint32_t magic_;
  ObTableRouteParam table_param_;
  char *name_buf_;
  int64_t name_buf_len_;
  ObTableEntryLookupOp te_op_; // next op, after lookup global cache
  ObTableEntryLookupState state_;
  ObTableEntry *newest_table_entry_; // the entry fetch from remote
  ObTableEntry *table_entry_; // the entry get from global cache
  ObTableCache *table_cache_;
  bool need_notify_;
  ObMysqlClient *mysql_client_;
  common::ObString binlog_service_ip_;

  DISALLOW_COPY_AND_ASSIGN(ObTableEntryCont);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TABLE_ENTRY_CONT_H

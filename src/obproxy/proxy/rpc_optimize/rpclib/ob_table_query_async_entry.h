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

#ifndef OB_TABLE_QUERY_ASYNC_ENTRY_H
#define OB_TABLE_QUERY_ASYNC_ENTRY_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/time/ob_hrtime.h"
#include "iocore/eventsystem/ob_thread.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_route_struct.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE     (RPC_QUERY_ASYNC_ENTRY_EVENT_EVENTS_START + 1)
#define RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT    (RPC_QUERY_ASYNC_ENTRY_EVENT_EVENTS_START + 2)
#define RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT    (RPC_QUERY_ASYNC_ENTRY_EVENT_EVENTS_START + 3)

class ObTableQueryAsyncEntry : public common::ObSharedRefCount
{
public:
  enum ObTableQueryAsyncEntryState
    {
      BORN = 0,
      BUILDING,
      AVAIL,
      DIRTY,
      UPDATING,
      DELETED
    };

  ObTableQueryAsyncEntry()
    : common::ObSharedRefCount(), client_query_session_id_(0), server_query_session_id_(0), current_position_(0),
      table_id_(0), data_table_id_(0), tablet_ids_(), timeout_ts_(0), first_query_(false), need_retry_(false),
      need_terminal_(false), global_index_query_(false), server_info_set_(false), partition_table_(false), server_info_(),
      state_(BORN), total_len_(0) {}
  ~ObTableQueryAsyncEntry() {}
  void reset()
  {
    client_query_session_id_ = 0;
    server_query_session_id_ = 0;
    current_position_ = 0;
    table_id_ = 0;
    data_table_id_ = 0;
    tablet_ids_.reset();
    timeout_ts_ = 0;
    need_retry_ = false;
    need_terminal_ = false;
    first_query_ = false;
    global_index_query_ = false;
    server_info_set_ = false;
  }

  void set_building_state() { state_ = BUILDING; }
  void set_avail_state() { state_ = AVAIL; }
  void set_dirty_state() { state_ = DIRTY; }
  void set_deleted_state() { state_ = DELETED; }
  void set_updating_state() { state_ = UPDATING; }
  bool is_building_state() const { return BUILDING == state_; }
  bool is_avail_state() const { return AVAIL == state_; }
  bool is_dirty_state() const { return DIRTY == state_; }
  bool is_updating_state() const { return UPDATING == state_; }
  bool is_deleted_state() const { return DELETED == state_; }

  void set_client_query_session_id(uint64_t client_query_session_id) { client_query_session_id_ = client_query_session_id; }
  void set_server_query_session_id(uint64_t server_query_session_id) { server_query_session_id_ = server_query_session_id; }
  void set_table_id(int64_t table_id) { table_id_ = table_id; }
  void set_data_table_id(int64_t data_table_id) { data_table_id_ = data_table_id; }
  void add_current_position() { current_position_++; }
  void reset_tablet_ids() { tablet_ids_.reset(); current_position_ = 0; }
  uint64_t get_client_query_session_id() const { return client_query_session_id_; }
  uint64_t get_server_query_session_id() const { return server_query_session_id_; }
  int64_t get_current_position() const { return current_position_; }
  int64_t get_table_id() const { return table_id_; }
  int64_t get_data_table_id() const { return data_table_id_; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
  common::ObIArray<int64_t> &get_tablet_ids() { return tablet_ids_; }
  const ObConnectionAttributes &get_server_info() const { return server_info_; }

  void set_server_info(const net::ObIpEndpoint &addr, const net::ObIpEndpoint &sql_addr) {
    server_info_.addr_ = addr;
    server_info_.sql_addr_ = sql_addr;
    server_info_set_ = true;
  }
  void reset_server_info() {
    server_info_.reset();
    server_info_set_ = false;
  }
  void set_first_query(bool first) { first_query_ = first; }
  void set_need_retry(bool need_retry) { need_retry_ = need_retry; }
  void set_need_terminal(bool need_terminal) { need_terminal_ = need_terminal; }
  void set_global_index_query(bool global_index_query) { global_index_query_ = global_index_query; }
  void set_partition_table(bool partition_table) { partition_table_ = partition_table; }

  bool is_first_query() const { return first_query_; }
  bool is_need_retry() const { return need_retry_; }
  bool is_need_terminal() const { return need_terminal_; }
  bool is_global_index_query() const { return global_index_query_; }
  bool is_server_info_set() const { return server_info_set_; }
  bool is_partition_table() const { return partition_table_; }
  bool is_single_query_request() const { return 1 >= tablet_ids_.count(); }
  bool is_last_tablet() const { return current_position_ == tablet_ids_.count() - 1; }

  void update_timeout_ts(int64_t rpc_pkt_timeout_ts) {
    timeout_ts_ = ObTimeUtility::current_time() + rpc_pkt_timeout_ts;
  }

  static int allocate(ObTableQueryAsyncEntry *&query_async_entry);
  int init(int64_t query_session_id);
  void free();

  int get_current_tablet_id(int64_t &tablet_id) const;

  uint64_t client_query_session_id_;
  uint64_t server_query_session_id_;
  int64_t current_position_;
  int64_t table_id_;
  int64_t data_table_id_;                     // 全局索引情况下使用
  common::ObSEArray<int64_t, 1> tablet_ids_;
  int64_t timeout_ts_;
  bool first_query_;
  bool need_retry_;
  bool need_terminal_;
  bool global_index_query_;
  bool server_info_set_;
  bool partition_table_;
  ObConnectionAttributes server_info_;
  ObTableQueryAsyncEntryState state_;
  int64_t total_len_;

  TO_STRING_KV(KP(this), K_(ref_count), K_(client_query_session_id), K_(server_query_session_id), K_(current_position),
               K_(table_id), K_(data_table_id), K_(tablet_ids), K_(timeout_ts), K_(first_query),
               K_(global_index_query), K_(server_info_set), K_(partition_table), K_(server_info),
               K_(state), K_(total_len));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncEntry);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_TABLE_QUERY_ASYNC_ENTRY_H
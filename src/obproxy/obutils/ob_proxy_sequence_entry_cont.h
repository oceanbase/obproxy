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

#ifndef OBPROXY_SEQUENCE_ENTRY_CONT_H
#define OBPROXY_SEQUENCE_ENTRY_CONT_H

#include "iocore/eventsystem/ob_event_system.h"
#include "lib/string/ob_string.h"
#include "lib/lock/tbrwlock.h"
#include "obutils/ob_proxy_sequence_entry.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_async_common_task.h"
#include "proxy/route/ob_table_entry_cont.h"


#define SEQUENCE_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 1)
#define SEQUENCE_ENTRY_LOOKUP_REMOTE_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 2)
#define SEQUENCE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 3)
#define SEQUENCE_ENTRY_NOTIFY_CALLER_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 4)
#define SEQUENCE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 5)
#define SEQUENCE_ENTRY_CREATE_COMPLETE_EVENT (SQUENCE_ENTRY_EVENT_EVENTS_START + 6)

namespace oceanbase
{
namespace obproxy
{
class ObResultSetFetcher;

namespace proxy
{
enum ObSequenceEntryLookupState
{
  LOOKUP_SEQUENCE_ENTRY_STATE = 0,
  LOOKUP_SEQUENCE_DBTIME_STATE,
  LOOKUP_SEQUENCE_UPDATE_STATE,
  LOOKUP_SEQUENCE_RETRY_STATE,
  LOOKUP_SEQUENCE_DONE_STATE,
};
enum
{
  OB_SEQUENCE_ENTRY_CONT_MAGIC_ALIVE = 0xAABBCCDD,
  OB_SEQUENCE_ENTRY_CONT_MAGIC_DEAD = 0xDDCCBBAA
};
enum ObSequenceContType
{
  OB_SEQUENCE_CONT_UNKOWN_TYPE = 0,
  OB_SEQUENCE_CONT_SYNC_TYPE,
  OB_SEQUENCE_CONT_ASYNC_TYPE,
  OB_SEQUENCE_CONT_TPE_MAX,
};
class ObMysqlProxy;

class ObProxySequenceEntryCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxySequenceEntryCont(ObContinuation *cb_cont, oceanbase::obproxy::event::ObEThread* submit_thread);
  int init(const ObSequenceRouteParam &param, ObSequenceEntry* sequence_entry, ObSequenceContType cont_type);
  virtual ~ObProxySequenceEntryCont() {}
  void kill_this();

  const static int64_t SCHEDULE_SHARD_ENTRY_LOOKUP_INTERVAL = HRTIME_MSECONDS(1); // 1ms

  int do_create_sequence_entry();
  void set_dbtimestamp(const common::ObString& dbtimestamp) {
    sequence_info_.db_timestamp_.set_value(dbtimestamp);
  }

  void set_errinfo(const common::ObString& err_msg) {
    sequence_info_.err_msg_.set_value(err_msg);
  }

  void set_errno(int64_t error) {
    sequence_info_.errno_ = error;
  }
private:
  virtual int main_handler(int event, void *data);
  static const char* get_event_name(const int64_t event);
  static const char* get_state_name(const ObSequenceEntryLookupState state);

  int do_create_oceanbase_sequence_entry();
  int do_create_normal_sequence_entry();
  inline int select_old_value_from_remote();
  inline int update_new_value_to_remote();
  bool need_create_cluster_resource();
  int set_next_state();
  int handle_create_cluster_resouce();
  int create_proxy(common::ObSharedRefCount *param, const common::ObString &username);
  int rebuild_proxy(ObMysqlProxy *proxy, common::ObSharedRefCount *param,
                    const common::ObString &username, const common::ObString &passwd);
  int handle_create_cluster_resource_complete(void *data);
  int handle_client_resp(void *data);
  int handle_select_old_value_resp(ObResultSetFetcher &rs_fetcher);
  int handle_update_new_value_resp(ObResultSetFetcher &rs_fetcher);

  int handle_lookup_remote();
  int handle_lookup_remote_done();
  int handle_chain_notify_caller();
  int handle_inform_out_event();
  int notify_caller();

  int schedule_in(event::ObContinuation *cont, const ObHRTime atimeout_in, const int event);
  int schedule_imm(ObContinuation *cont, const int event);
private:
  uint32_t magic_;
  dbconfig::ObShardConnector* shard_conn_;
  common::DBServerType server_type_;
  ObMysqlProxyEntry* mysql_proxy_entry_;
  char proxy_id_buf_[1024];
  oceanbase::obproxy::obutils::ObProxyConfigString cluster_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString tenant_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString database_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString real_table_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString username_;
  oceanbase::obproxy::obutils::ObProxyConfigString password_;
  oceanbase::obproxy::obutils::ObProxyConfigString seq_name_;
  int64_t min_value_;
  int64_t max_value_;
  int64_t step_;
  int64_t max_retry_count_;
  ObSequenceEntryLookupState state_;
  ObSequenceInfo sequence_info_;
  ObSequenceEntry* sequence_entry_;
  bool need_db_timestamp_;
  bool only_need_db_timestamp_;
  bool last_state_success_;
  bool need_insert_;
  int retry_time_;
  bool allow_insert_;
  ObSequenceContType cont_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxySequenceEntryCont);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SEQUENCE_ENTRY_CONT_H

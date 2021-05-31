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

#ifndef OBPROXY_STATE_PROCESSOR_H
#define OBPROXY_STATE_PROCESSOR_H
#include "obutils/ob_state_info.h"
#include "lib/container/ob_se_array.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "iocore/eventsystem/ob_lock.h"

#define REFRESH_ZONE_STATE_EVENT (SERVER_STATE_EVENT_EVENTS_START + 1)
#define REFRESH_SERVER_STATE_EVENT (SERVER_STATE_EVENT_EVENTS_START + 2)
#define DESTROY_SERVER_STATE_EVENT (SERVER_STATE_EVENT_EVENTS_START + 3)
#define REFRESH_CLUSTER_ROLE_EVENT (SERVER_STATE_EVENT_EVENTS_START + 4)
#define REFRESH_LDG_INFO_EVENT (SERVER_STATE_EVENT_EVENTS_START + 5)
#define REFRESH_ALL_TENANT_EVENT (SERVER_STATE_EVENT_EVENTS_START + 6)

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
class ObMysqlResultHandler;
}
namespace obutils
{
class ObClusterResource;
class ObCongestionManager;
class ObServerStateRefreshCont : public event::ObContinuation
{
public:
  ObServerStateRefreshCont()
    : ObContinuation(NULL), is_inited_(false), kill_this_(false), need_reset_in_error_(false),
      cur_job_event_(0), cluster_resource_(NULL),
      mysql_proxy_(NULL), congestion_manager_(NULL), pending_action_(NULL),
      ss_refresh_interval_us_(0), ss_refresh_failure_(0), set_interval_task_count_(0), cluster_id_(OB_INVALID_CLUSTER_ID), 
      cluster_name_(), last_zones_state_hash_(0), last_servers_state_hash_(0), last_server_list_hash_(0)
  {
    SET_HANDLER(&ObServerStateRefreshCont::main_handler);
  }
  virtual ~ObServerStateRefreshCont() {}

  int init(ObClusterResource *cr, int64_t ss_refresh_interval_us);

  int schedule_refresh_server_state(const bool imm = false);
  int set_server_state_refresh_interval(const int64_t interval);

  void kill_this();
  DECLARE_TO_STRING;

private:
  int main_handler(int event, void *data);
  int schedule_imm(const int event);

  int refresh_cluster_role();
  int refresh_server_state();
  int refresh_zone_state();
  int refresh_ldg_info();
  int refresh_all_tenant();

  int handle_cluster_role(void *data);
  int handle_zone_state(void *data);
  int handle_server_state(void *data);
  int handle_ldg_info(void *data);
  int handle_all_tenant(void *data);

  int handle_newest_zone(common::ObIArray<ObZoneStateInfo> &zones_state,
                         bool &is_zones_state_changed,
                         bool &has_invalid_zone);
  int handle_newest_server(common::ObIArray<ObServerStateInfo> &zones_state,
                           bool &is_servers_state_changed,
                           bool &has_invalid_server);

  int handle_rs_changed(const common::ObIArray<ObServerStateInfo> &servers_state);

  int do_update_zone(const ObZoneStateInfo &zs_info);
  int do_update_server(const ObServerStateInfo &ss_info);

  int handle_deleted_zone(common::ObIArray<ObZoneStateInfo> &zones_state);
  int handle_deleted_server(common::ObIArray<ObServerStateInfo> &servers_state);

  int add_refresh_rslist_task(const bool need_update_dummy_entry);
  int check_add_refresh_idc_list_task();
  int update_all_dummy_entry(const common::ObIArray<ObServerStateInfo> &servers_state);
  int update_last_zs_state(const bool is_zones_state_changed,
                           const bool is_servers_state_changed,
                           common::ObIArray<ObZoneStateInfo> &zones_state,
                           common::ObIArray<ObServerStateInfo> &servers_state);

  int update_safe_snapshot_manager(const common::ObIArray<ObServerStateInfo> &servers_state);
  int cancel_pending_action();

public:
  static const int64_t DEFAULT_ZONE_COUNT = 5;
  static const int64_t DEFAULT_SERVER_COUNT = 64;
  static const int64_t MAX_REFRESH_FAILURE = 10;
  static const int64_t MIN_REFRESH_FAILURE = 3;

private:
  bool is_inited_;
  bool kill_this_;
  bool need_reset_in_error_;
  int cur_job_event_;
  ObClusterResource *cluster_resource_;
  proxy::ObMysqlProxy *mysql_proxy_;
  ObCongestionManager *congestion_manager_;
  event::ObAction *pending_action_;
  int64_t ss_refresh_interval_us_; // us
  int64_t ss_refresh_failure_;
  volatile int64_t set_interval_task_count_; // inc when set_interval and dec when EVENT_IMMEDIATE has been done
  int64_t cluster_id_;
  common::ObString cluster_name_; // shallow copy, actual data buffer is in ObClusterResource

  common::ObSEArray<ObZoneStateInfo, DEFAULT_ZONE_COUNT> cur_zones_state_;
  uint64_t last_zones_state_hash_;
  uint64_t last_servers_state_hash_;
  uint64_t last_server_list_hash_;
  common::ObSEArray<ObZoneStateInfo,  DEFAULT_ZONE_COUNT> last_zones_state_;
  common::ObSEArray<ObServerStateInfo, DEFAULT_SERVER_COUNT> last_servers_state_;

  DISALLOW_COPY_AND_ASSIGN(ObServerStateRefreshCont);
};

class ObServerStateRefreshUtils
{
public:
  static int check_cluster_role(proxy::ObMysqlResultHandler &handler, int64_t &master_cluster_id);
  static int get_zone_state_info(proxy::ObMysqlResultHandler &result_handler,
                                 common::ObIArray<ObZoneStateInfo> &zone_info);

  static int get_server_state_info(proxy::ObMysqlResultHandler &result_handler,
                                   common::ObIArray<ObZoneStateInfo> &zones_state,
                                   common::ObIArray<ObServerStateInfo> &servers_state,
                                   bool &has_invalid_server);

  static ObZoneStateInfo *get_zone_info_ptr(common::ObIArray<ObZoneStateInfo> &zones_state,
                                            const common::ObString &zone_name);

  static uint64_t get_zones_state_hash(common::ObIArray<ObZoneStateInfo> &zones_state);
  static uint64_t get_servers_state_hash(common::ObIArray<ObServerStateInfo> &servers_state);
  static uint64_t get_servers_addr_hash(common::ObIArray<ObServerStateInfo> &servers_state);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_SERVER_STATE_PROCESSOR_H

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

#ifndef OCEANBASE_SHARE_OB_SERVER_STATUS_H_
#define OCEANBASE_SHARE_OB_SERVER_STATUS_H_

#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_zone.h"
#include "ob_lease_struct.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace share
{

struct ObServerStatus
{
  // server admin status
  enum ServerAdminStatus
  {
    OB_SERVER_ADMIN_NORMAL,
    OB_SERVER_ADMIN_DELETING,
    OB_SERVER_ADMIN_MAX,
  };

  // server heart beat status
  enum HeartBeatStatus
  {
    OB_HEARTBEAT_ALIVE,
    OB_HEARTBEAT_LEASE_EXPIRED,
    OB_HEARTBEAT_PERMANENT_OFFLINE,
    OB_HEARTBEAT_MAX,
  };

  // display status (display in __all_server table)
  enum DisplayStatus
  {
    OB_SERVER_INACTIVE,
    OB_SERVER_ACTIVE,
    OB_SERVER_DELETING,
    OB_DISPLAY_MAX,
  };

  ObServerStatus();
  virtual ~ObServerStatus();

  bool is_status_valid() const;

  static int server_admin_status_str(const ServerAdminStatus status, const char *&str);
  static int heartbeat_status_str(const HeartBeatStatus status, const char *&str);
  static int display_status_str(const DisplayStatus status, const char *&str);
  static int str2display_status(const char *str, DisplayStatus &status);
  static int str2display_status(const common::ObString& str, DisplayStatus &status);

  bool is_valid() const;
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool is_active() const { return is_alive() && OB_SERVER_ADMIN_NORMAL == admin_status_; }

  bool is_alive() const { return OB_HEARTBEAT_ALIVE == hb_status_; }
  bool is_permanent_offline() const { return OB_HEARTBEAT_PERMANENT_OFFLINE == hb_status_; }
  bool need_check_empty(const int64_t now) const { return !is_alive() && with_partition_
    && last_hb_time_ + GCONF.lease_time + common::OB_MAX_ADD_MEMBER_TIMEOUT < now; }

  DisplayStatus get_display_status() const;

  void block_migrate_in() { block_migrate_in_time_ = common::ObTimeUtility::current_time(); }
  void unblock_migrate_in() { block_migrate_in_time_ = 0; }
  bool is_migrate_in_blocked() const { return 0 != block_migrate_in_time_; }
  bool is_stopped() const { return 0 != stop_time_; }

  bool in_service() const { return 0 != start_service_time_; }

  uint64_t id_;
  common::ObZone zone_;
  char build_version_[common::OB_SERVER_VERSION_LENGTH];
  common::ObAddr server_;
  int64_t sql_port_;    // sql listen port
  int64_t register_time_;
  int64_t last_hb_time_;
  int64_t block_migrate_in_time_;
  int64_t stop_time_;
  int64_t start_service_time_;

  // Merged version report by server merge finish.
  // Can only be used to wakeup daily merge thread. (invalid between RS start and server merge finish)
  int64_t merged_version_;
  ServerAdminStatus admin_status_;
  HeartBeatStatus hb_status_;
  bool with_rootserver_;
  bool with_partition_;

  ObServerResourceInfo resource_info_;

private:
  static int get_status_str(const char *strs[],
      const int64_t strs_len, const int64_t status, const char *&str);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_SERVER_STATUS_H_

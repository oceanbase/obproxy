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

#ifndef OCEANBASE_SHARE_OB_ZONE_INFO_H_
#define OCEANBASE_SHARE_OB_ZONE_INFO_H_
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_print_utils.h"
#include "common/mysql_proxy/ob_mysql_proxy.h"
#include "common/mysql_proxy/ob_mysql_transaction.h"
#include "common/ob_zone.h"
#include "share/ob_zone_status.h"

namespace oceanbase
{
namespace share
{

class ObZoneItemTransUpdater;

struct ObZoneInfoItem : public common::ObDLinkBase<ObZoneInfoItem>
{
public:
  typedef common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> Info;
  typedef common::ObDList<ObZoneInfoItem> ItemList;

  ObZoneInfoItem(ItemList &list, const char *name, int64_t value, const char *info);
  ObZoneInfoItem(const ObZoneInfoItem &item);
  ObZoneInfoItem &operator = (const ObZoneInfoItem &item);

  bool is_valid() const { return NULL != name_ && value_ >= 0; }
  TO_STRING_KV(K_(name), K_(value), K_(info));
  operator int64_t() const { return value_; }

  // update %value_ and %info_ after execute sql success
  int update(common::ObISQLClient &sql_client, const common::ObZone &zone,
      const int64_t value, const Info &info);
  // update %value_ and %info_. (set %info_ to empty)
  int update(common::ObISQLClient &sql_client, const common::ObZone &zone,
      const int64_t value);

  // update %value_ and %info_ will be rollback if transaction rollback or commit failed.
  int update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
      const int64_t value, const Info &info);
  // update %value_ and %info_. (set %info_ to empty)
  int update(ObZoneItemTransUpdater &updater, const common::ObZone &zone,
      const int64_t value);

public:
  const char *name_;
  int64_t value_;
  Info info_;
};

// Update item in transaction, if transaction rollback or commit failed the item value
// value will be rollback too.
class ObZoneItemTransUpdater
{
public:
  ObZoneItemTransUpdater();
  ~ObZoneItemTransUpdater();

  int start(common::ObMySQLProxy &sql_proxy);
  int update(const common::ObZone &zone, ObZoneInfoItem &item,
      const int64_t value, const ObZoneInfoItem::Info &info);
  int end(const bool commit);
  common::ObMySQLTransaction &get_trans() { return trans_; }
private:
  const static int64_t PTR_OFFSET = sizeof(void *);

  bool started_;
  bool success_;

  common::ObArenaAllocator alloc_;
  ObZoneInfoItem::ItemList list_;
  common::ObMySQLTransaction trans_;
};

struct ObGlobalInfo
{
public:
  ObGlobalInfo();
  ObGlobalInfo(const ObGlobalInfo &other);
  ObGlobalInfo &operator = (const ObGlobalInfo &other);
  void reset();
  bool is_valid() const;
  DECLARE_TO_STRING;

public:
  const common::ObZone zone_; // always be default value
  ObZoneInfoItem::ItemList list_;

  ObZoneInfoItem cluster_;
  ObZoneInfoItem try_frozen_version_;
  ObZoneInfoItem frozen_version_;
  ObZoneInfoItem frozen_time_;
  ObZoneInfoItem global_broadcast_version_;
  ObZoneInfoItem last_merged_version_;
  ObZoneInfoItem privilege_version_;
  ObZoneInfoItem config_version_;
  ObZoneInfoItem lease_info_version_;
  ObZoneInfoItem is_merge_error_;
  ObZoneInfoItem merge_list_;
  ObZoneInfoItem merge_status_;
};

struct ObZoneInfo
{
public:
  enum MergeStatus
  {
    MERGE_STATUS_IDLE,
    MERGE_STATUS_MERGING,
    MERGE_STATUS_TIMEOUT,
    MERGE_STATUS_ERROR,
    MERGE_STATUS_INDEXING,
    MERGE_STATUS_MOST_MERGED,
    MERGE_STATUS_MAX
  };
  ObZoneInfo();
  ObZoneInfo(const ObZoneInfo &other);
  ObZoneInfo &operator =(const ObZoneInfo &other);
  void reset();
  bool is_valid() const;
  DECLARE_TO_STRING;

  const char *get_status_str() const;
  static const char *get_merge_status_str(const MergeStatus status);

  inline bool is_storage_merging() const { return broadcast_version_ != last_merged_version_; }
  bool is_merged(const int64_t global_broadcast_version) const;
  inline int64_t get_item_count() const { return list_.get_size(); }
public:
  common::ObZone zone_;
  ObZoneInfoItem::ItemList list_;

  ObZoneInfoItem status_;
  // Zone merging flag set by daily merge scheduler.
  // Set this flag before leader switching to notify leader coordinator the merging zones.
  ObZoneInfoItem is_merging_;
  ObZoneInfoItem broadcast_version_;
  ObZoneInfoItem last_merged_version_;
  ObZoneInfoItem last_merged_time_;
  ObZoneInfoItem all_merged_version_;
  ObZoneInfoItem merge_start_time_;
  ObZoneInfoItem is_merge_timeout_;
  ObZoneInfoItem suspend_merging_;
  ObZoneInfoItem merge_status_;
};


} // end namespace share
} // end namespace oceanbase

#endif  //OCEANBASE_SHARE_OB_CLUSTER_INFO_H_

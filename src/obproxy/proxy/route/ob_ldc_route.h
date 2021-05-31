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

#ifndef OBPROXY_LDC_ROUTE_H
#define OBPROXY_LDC_ROUTE_H
#include "proxy/route/ob_ldc_location.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObLDCRoute
{
public:
  ObLDCRoute() : location_(), policy_(MAX_ROUTE_POLICY_COUNT), disable_merge_status_check_(false),
                 curr_cursor_index_(0), next_index_in_site_(0)
  {}
  ~ObLDCRoute() {}
  void reset_cursor();
  void reset();
  const ObLDCItem *get_next_item();
  bool is_reach_end() const;
  bool is_follower_first_policy() const { return (policy_ >= FOLLOWER_FIRST && policy_ <= UNMERGE_FOLLOWER_FIRST_OPTIMIZED); }
  ObRouteType get_curr_route_type() const;
  ObRouteType get_route_type(const int64_t cursor_index) const;
  int64_t get_cursor_index(const ObRouteType route_type);

  static ObRouteType get_route_type(const ObRoutePolicyEnum policy, const int64_t cursor_index);
  static ObIDCType get_idc_type(const ObRouteType route_type);
  static bool is_same_merge_type(const ObRouteType route_type, const ObLDCItem &item);
  static bool is_same_zone_type(const ObRouteType route_type, const ObLDCItem &item);
  static bool is_same_partition_type(const ObRouteType route_type, const ObLDCItem &item);
  static bool is_same_role(const ObRouteType route_type, const ObLDCItem &item);

  TO_STRING_KV("policy", get_route_policy_enum_string(policy_), K_(disable_merge_status_check),
               K_(curr_cursor_index), K_(next_index_in_site), K_(location));

public:
  static const ObRouteType *route_order_cursor_[MAX_ROUTE_POLICY_COUNT];
  static int64_t route_order_size_[MAX_ROUTE_POLICY_COUNT];

  ObLDCLocation location_;
  ObRoutePolicyEnum policy_;
  bool disable_merge_status_check_;//if true, not care merge status
  int64_t curr_cursor_index_;
  int64_t next_index_in_site_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLDCRoute);
};

inline void ObLDCRoute::reset_cursor()
{
  curr_cursor_index_ = 0;
  next_index_in_site_ = 0;
  location_.reset_item_status();
}

inline void ObLDCRoute::reset()
{
  curr_cursor_index_ = 0;
  next_index_in_site_ = 0;
  policy_ = MAX_ROUTE_POLICY_COUNT;
  disable_merge_status_check_= true;
  location_.reset();
}

inline bool ObLDCRoute::is_reach_end() const
{
  return ROUTE_TYPE_MAX == get_curr_route_type();
}

inline ObRouteType ObLDCRoute::get_curr_route_type() const
{
  return get_route_type(curr_cursor_index_);
}

inline ObRouteType ObLDCRoute::get_route_type(const int64_t cursor_index) const
{
  ObRouteType ret_type = ROUTE_TYPE_MAX;
  if (is_route_policy_enum_valid(policy_)
      && 0 <= cursor_index
      && cursor_index < route_order_size_[policy_]) {
    ret_type = route_order_cursor_[policy_][cursor_index];
  }
  return ret_type;
}

inline ObRouteType ObLDCRoute::get_route_type(const ObRoutePolicyEnum policy, const int64_t cursor_index)
{
  ObRouteType ret_type = ROUTE_TYPE_MAX;
  if (is_route_policy_enum_valid(policy)
      && 0 <= cursor_index
      && cursor_index < route_order_size_[policy]) {
    ret_type = route_order_cursor_[policy][cursor_index];
  }
  return ret_type;
}

inline ObIDCType ObLDCRoute::get_idc_type(const ObRouteType route_type)
{
  return static_cast<ObIDCType>(route_type & MAX_IDC_VALUE);
}

inline bool ObLDCRoute::is_same_merge_type(const ObRouteType route_type,
    const ObLDCItem &item)
{
  return (ObRouteTypeCheck::is_merge_type(route_type) == item.is_merging_);
}

inline bool ObLDCRoute::is_same_zone_type(const ObRouteType route_type,
    const ObLDCItem &item)
{
  bool ret_bool = false;
  switch (route_type & MAX_ZONE_TYPE_VALUE) {
    case IS_READONLY_ZONE:
      ret_bool = (common::ZONE_TYPE_READONLY == item.zone_type_);
      break;
    case IS_READWRITE_ZONE:
      ret_bool = (common::ZONE_TYPE_READWRITE == item.zone_type_);
      break;
    case UNKNOWN_ZONE_TYPE:
      ret_bool = true;
      break;
    default:
      //do nothing
      break;
  }
  return ret_bool;
}

inline bool ObLDCRoute::is_same_partition_type(const ObRouteType route_type,
    const ObLDCItem &item)
{
  return (ObRouteTypeCheck::is_with_partition_type(route_type) == item.is_partition_server_);
}

inline bool ObLDCRoute::is_same_role(const ObRouteType route_type,
    const ObLDCItem &item)
{
  bool ret_bool = false;
  if (OB_NOT_NULL(item.replica_)) {
    switch (route_type & MAX_ROLE_TYPE_VALUE) {
      case IS_LEADER:
        ret_bool = (common::LEADER == item.replica_->role_);
        break;
      case IS_FOLLOWER:
        ret_bool = (common::FOLLOWER == item.replica_->role_);
        break;
      case UNKNOWN_ROLE:
        ret_bool = true;
        break;
      default:
        //do nothing
        break;
    }
  }
  return ret_bool;
}

inline int64_t ObLDCRoute::get_cursor_index(const ObRouteType route_type)
{
  int64_t idx = common::OB_INVALID_INDEX;
  if (is_route_policy_enum_valid(policy_) && ObRouteTypeCheck::is_route_type_valid(route_type)) {
    for (int64_t i = 0; common::OB_INVALID_INDEX == idx && i < route_order_size_[policy_]; i++) {
      if (route_type == route_order_cursor_[policy_][i]) {
        idx = i;
      }
    }
  }
  return idx;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_LDC_ROUTE_H */

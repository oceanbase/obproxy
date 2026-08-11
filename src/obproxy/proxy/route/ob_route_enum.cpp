/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX PROXY

#include "proxy/route/ob_route_enum.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const char *get_route_info_type_name(const ObRouteInfoType type)
{
  const char *name = NULL;
  switch(type) {
    case ObRouteInfoType::INVALID:
      name = "INVALID";
      break;

    case ObRouteInfoType::USE_GLOBAL_INDEX:
      name = "USE_GLOBAL_INDEX";
      break;

    case ObRouteInfoType::USE_OBPROXY_ROUTE_ADDR:
      name = "USE_OBPROXY_ROUTE_ADDR";
      break;

    case ObRouteInfoType::USE_CURSOR:
      name = "USE_CURSOR";
      break;

    case ObRouteInfoType::USE_PREPARE_EXECUTED_ADDR:
      name = "USE_PREPARE_EXECUTED_ADDR";
      break;

    case ObRouteInfoType::USE_PIECES_DATA:
      name = "USE_PIECES_DATA";
      break;

    case ObRouteInfoType::USE_CONFIG_TARGET_DB:
      name = "USE_CONFIG_TARGET_DB";
      break;

    case ObRouteInfoType::USE_COMMENT_TARGET_DB:
      name = "USE_COMMENT_TARGET_DB";
      break;

    case ObRouteInfoType::USE_TEST_SVR_ADDR:
      name = "USE_TEST_SVR_ADDR";
      break;

    case ObRouteInfoType::USE_LAST_SESSION:
      name = "USE_LAST_SESSION";
      break;

    case ObRouteInfoType::USE_LAST_INSERT_ID_SESSION:
      name = "USE_LAST_INSERT_ID_SESSION";
      break;

    case ObRouteInfoType::USE_LOCK_SESSION:
      name = "USE_LOCK_SESSION";
      break;

    case ObRouteInfoType::USE_CACHED_SESSION:
      name = "USE_CACHED_SESSION";
      break;

    case ObRouteInfoType::USE_SINGLE_LEADER:
      name = "USE_SINGLE_LEADER";
      break;

    case ObRouteInfoType::USE_SINGLE_LEADERS_FOLLOWER:
      name = "USE_SINGLE_LEADERS_FOLLOWER";
      break;

    case ObRouteInfoType::USE_PARTITION_LOCATION_LOOKUP:
      name = "USE_PARTITION_LOCATION_LOOKUP";
      break;

    case ObRouteInfoType::USE_COORDINATOR_SESSION:
      name = "USE_COODINATOR_SESSION";
      break;

    case ObRouteInfoType::USE_ROUTE_POLICY:
      name = "USE_ROUTE_POLICY";
      break;

    case ObRouteInfoType::USE_BINLOG_SERVICE_LOOKUP:
      name = "USE_BINLOG_SERVICE_LOOKUP";
      break;

    default:
      name = "unkonwn route type";
  }
  return name;
}


common::ObString get_route_policy_enum_string(const ObRoutePolicyEnum policy)
{
  static const common::ObString string_array[MAX_ROUTE_POLICY_COUNT] =
  {
      common::ObString::make_string("MERGE_IDC_ORDER"),
      common::ObString::make_string("READONLY_ZONE_FIRST"),
      common::ObString::make_string("ONLY_READONLY_ZONE"),
      common::ObString::make_string("UNMERGE_ZONE_FIRST"),
      common::ObString::make_string("ONLY_READWRITE_ZONE"),
      common::ObString::make_string("MERGE_IDC_ORDER_OPTIMIZED"),
      common::ObString::make_string("READONLY_ZONE_FIRST_OPTIMIZED"),
      common::ObString::make_string("ONLY_READONLY_ZONE_OPTIMIZED"),
      common::ObString::make_string("UNMERGE_ZONE_FIRST_OPTIMIZED"),
      common::ObString::make_string("ONLY_READWRITE_ZONE_OPTIMIZED"),
      common::ObString::make_string("FOLLOWER_FIRST"),
      common::ObString::make_string("UNMERGE_FOLLOWER_FIRST"),
      common::ObString::make_string("FOLLOWER_FIRST_OPTIMIZED"),
      common::ObString::make_string("UNMERGE_FOLLOWER_FIRST_OPTIMIZED"),
      common::ObString::make_string("DUP_REPLICA_FIRST"),
      common::ObString::make_string("FOLLOWER_ONLY"),
      common::ObString::make_string("FOLLOWER_ONLY_OPTIMIZED"),
      common::ObString::make_string("PROXY_PRIMARY_ZONE_NAME_ONLY"),
      common::ObString::make_string("TARGET_DB_SERVER_ONLY"),
      common::ObString::make_string("PRIMARY_ZONE_FIRST"),
      common::ObString::make_string("TARGET_REPLICA_TYPE_WITH_LEADER"),
      common::ObString::make_string("TARGET_REPLICA_TYPE_FOLLOWER_FIRST"),
      common::ObString::make_string("TARGET_REPLICA_TYPE_FOLLOWER_ONLY"),
      common::ObString::make_string("WEAKREAD_WEIGHT_LOAD_BALANCE"),
  };

  common::ObString string;
  if (OB_LIKELY(policy >= MERGE_IDC_ORDER) && OB_LIKELY(policy < MAX_ROUTE_POLICY_COUNT)) {
    string = string_array[policy];
  }
  return string;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
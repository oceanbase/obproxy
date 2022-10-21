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

#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "proxy/route/ob_ldc_route.h"
#include "obutils/ob_state_info.h"
#include "lib/container/ob_se_array.h"

#define TEST2_GET_NEXT_ITEM(idc, merge, is_partition_server, addr, port, type1) \
    item = test_ldc_route.get_next_item();\
    EXPECT_EQ(idc, item->idc_type_);\
    EXPECT_TRUE(merge == item->is_merging_);\
    EXPECT_TRUE(is_partition_server == item->is_partition_server_);\
    EXPECT_TRUE(item->is_used_);\
    EXPECT_FALSE(item->is_force_congested_);\
    EXPECT_EQ(ObAddr::convert_ipv4_addr(addr), item->replica_->server_.get_ipv4());\
    EXPECT_EQ(port, item->replica_->server_.get_port());\
    EXPECT_EQ(type1, test_ldc_route.get_curr_route_type());\
    EXPECT_TRUE(!test_ldc_route.is_reach_end());

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{


class TesLDCLocation : public ::testing::Test
{
public:
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info_;
  ObSEArray<ObProxyReplicaLocation, 20> replicas_z1_;
  ObSEArray<ObProxyReplicaLocation, 20> replicas_z2_;
  ObSEArray<ObProxyReplicaLocation, 20> replicas_z3_;
  ObSEArray<ObProxyReplicaLocation, 20> replicas_z4_;
  virtual void SetUp();
  virtual void TearDown();
  void start_tenant_server(ObTenantServer &ts,
                          const ObIArray<ObProxyReplicaLocation> &replicas,
                          const int64_t replica_count);
  int64_t hash(const ObLDCLocation &ldc);

  static void fill_server_info(ObString zone1_idc, ObString zone2_idc, ObString zone3_idc,
       ObString zone1_name, ObString zone2_name, ObString zone3_name,
       ObIArray<ObServerStateSimpleInfo> &ss_info_);
  static void test_get_next_item(ObLDCRoute &test_ldc_route,
                                 const ObIDCType idc,
                                 const bool is_merge,
                                 const ObZoneType zone_type,
                                 const bool is_partition,
                                 const char *ip_str,
                                 const int64_t port,
                                 const ObRouteType route_type);

  static void test_get_next_item(ObLDCRoute &test_ldc_route,
                                 const ObIDCType idc,
                                 const bool is_merge,
                                 const bool is_partition,
                                 const ObRole role,
                                 const char *ip_str,
                                 const int64_t port,
                                 const ObRouteType route_type);
};

void TesLDCLocation::fill_server_info(
    ObString zone1_idc,
    ObString zone2_idc,
    ObString zone3_idc,
    ObString zone1_name,
    ObString zone2_name,
    ObString zone3_name,
    ObIArray<ObServerStateSimpleInfo> &ss_info)
{
  for (int32_t i = 0; i < 10; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name(zone1_name);
    info.set_idc_name(zone1_idc);
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = false;
    info.zone_type_ = ZONE_TYPE_READWRITE;
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 15; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name(zone2_name);
    info.set_idc_name(zone2_idc);
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = false;
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name(zone3_name);
    info.set_idc_name(zone3_idc);
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = false;
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
}


void TesLDCLocation::SetUp()
{
  fill_server_info("z1", "z2", "z3", "z1", "z2", "z3", ss_info_);

  for (int32_t i = 0; i < 20; i++) {
    replicas_z1_.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1), FOLLOWER, REPLICA_TYPE_FULL));
    replicas_z2_.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1), FOLLOWER, REPLICA_TYPE_FULL));
    replicas_z3_.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1), FOLLOWER, REPLICA_TYPE_FULL));
    replicas_z4_.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "4.4.4.4", i + 1), FOLLOWER, REPLICA_TYPE_FULL));
  }
}

void TesLDCLocation::TearDown()
{
  ss_info_.destroy();
  replicas_z1_.destroy();
  replicas_z2_.destroy();
  replicas_z3_.destroy();
  replicas_z4_.destroy();
}

void TesLDCLocation::start_tenant_server(ObTenantServer &ts,
    const ObIArray<ObProxyReplicaLocation> &replicas, const int64_t replica_count)
{
  ts.destory();
  const int64_t alloc_size = static_cast<int64_t>(sizeof(ObProxyReplicaLocation)) * replicas.count();
  char *server_list_buf = NULL;
  if (NULL == (server_list_buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    LOG_WARN("fail to alloc mem", K(alloc_size));
  } else {
    ts.server_count_ = replicas.count();
    ts.server_array_ = new (server_list_buf) ObProxyReplicaLocation[replicas.count()];
    memcpy(ts.server_array_, &(replicas.at(0)), alloc_size);
    ts.replica_count_ = replica_count;
    ts.next_partition_idx_ = 0;
    if (replica_count <= 0) {
      ts.partition_count_ = 1;
    } else {
      ts.partition_count_ = static_cast<uint64_t>((ts.server_count_ / ts.replica_count_ + (0 == ts.server_count_ % ts.replica_count_ ? 0 : 1)));
    }
    ts.is_inited_ = true;
  }
}

void TesLDCLocation::test_get_next_item(ObLDCRoute &test_ldc_route,
                                        const ObIDCType idc,
                                        const bool is_merge,
                                        const ObZoneType zone_type,
                                        const bool is_partition,
                                        const char *ip_str,
                                        const int64_t port,
                                        const ObRouteType route_type)
{
  const ObLDCItem *item = NULL;
  item = test_ldc_route.get_next_item();
  EXPECT_TRUE(NULL != item);
  ASSERT_EQ(idc, item->idc_type_);
  ASSERT_TRUE(test_ldc_route.disable_merge_status_check_ || is_merge == item->is_merging_);
  ASSERT_TRUE(zone_type == item->zone_type_);
  ASSERT_TRUE(is_partition == item->is_partition_server_);
  ASSERT_EQ(ObAddr::convert_ipv4_addr(ip_str), item->replica_->server_.get_ipv4());
  ASSERT_EQ(port, item->replica_->server_.get_port());
  ASSERT_EQ(route_type, test_ldc_route.get_curr_route_type());
  ASSERT_TRUE(!test_ldc_route.is_reach_end());
}

void TesLDCLocation::test_get_next_item(ObLDCRoute &test_ldc_route,
                                        const ObIDCType idc,
                                        const bool is_merge,
                                        const bool is_partition,
                                        const ObRole role,
                                        const char *ip_str,
                                        const int64_t port,
                                        const ObRouteType route_type)
{
  const ObLDCItem *item = NULL;
  item = test_ldc_route.get_next_item();
  EXPECT_TRUE(NULL != item);
  ASSERT_EQ(idc, item->idc_type_);
  ASSERT_TRUE(is_merge == item->is_merging_);
  if (INVALID_ROLE != role) {
    ASSERT_TRUE(role == item->replica_->role_);
  }
  ASSERT_TRUE(is_partition == item->is_partition_server_);
  ASSERT_EQ(ObAddr::convert_ipv4_addr(ip_str), item->replica_->server_.get_ipv4());
  ASSERT_EQ(port, item->replica_->server_.get_port());
  ASSERT_EQ(route_type, test_ldc_route.get_curr_route_type());
  ASSERT_TRUE(!test_ldc_route.is_reach_end());
}

int64_t TesLDCLocation::hash(const ObLDCLocation &ldc)
{
  int64_t result = 1;
  if (!ldc.is_empty()) {
    const int64_t prime = 31;
    int64_t hash = 0;
    for (int64_t i = 0; i < ldc.count(); ++i) {
      hash = ldc.get_item(i)->replica_->server_.hash();
      result = prime * result + hash;
    }
  }
  return result;
}

TEST_F(TesLDCLocation, basic)
{
  EXPECT_TRUE(ObRouteTypeCheck::is_route_type_valid(0));
  EXPECT_TRUE(ObRouteTypeCheck::is_route_type_valid(118));
  EXPECT_TRUE(ObRouteTypeCheck::is_route_type_valid(254));
  EXPECT_TRUE(!ObRouteTypeCheck::is_route_type_valid(1024));
  EXPECT_TRUE(!ObRouteTypeCheck::is_route_type_valid(-1));

  EXPECT_EQ(13, ObLDCRoute::route_order_size_[0]);
  EXPECT_EQ(25, ObLDCRoute::route_order_size_[1]);
  EXPECT_EQ(13, ObLDCRoute::route_order_size_[2]);
  EXPECT_EQ(25, ObLDCRoute::route_order_size_[3]);
  EXPECT_EQ(13, ObLDCRoute::route_order_size_[4]);
  EXPECT_EQ(13, ObLDCRoute::route_order_size_[5]);
  EXPECT_EQ(25, ObLDCRoute::route_order_size_[6]);
  EXPECT_EQ(13, ObLDCRoute::route_order_size_[7]);
  EXPECT_EQ(25, ObLDCRoute::route_order_size_[8]);
  EXPECT_EQ(13, ObLDCRoute::route_order_size_[9]);
  EXPECT_EQ(19, ObLDCRoute::route_order_size_[10]);
  EXPECT_EQ(19, ObLDCRoute::route_order_size_[11]);
  EXPECT_EQ(19, ObLDCRoute::route_order_size_[12]);
  EXPECT_EQ(19, ObLDCRoute::route_order_size_[13]);

  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, ObLDCRoute::get_route_type(MERGE_IDC_ORDER, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MERGE_IDC_ORDER, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MERGE_IDC_ORDER, 13));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST, 24));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST, 25));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE, 13));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST, 24));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST, 25));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE, 13));

  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, ObLDCRoute::get_route_type(MERGE_IDC_ORDER_OPTIMIZED, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MERGE_IDC_ORDER_OPTIMIZED, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MERGE_IDC_ORDER_OPTIMIZED, 13));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST_OPTIMIZED, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST_OPTIMIZED, 24));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(READONLY_ZONE_FIRST_OPTIMIZED, 25));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE_OPTIMIZED, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE_OPTIMIZED, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READONLY_ZONE_OPTIMIZED, 13));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST_OPTIMIZED, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST_OPTIMIZED, 24));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(UNMERGE_ZONE_FIRST_OPTIMIZED, 25));

  EXPECT_TRUE(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE_OPTIMIZED, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE_OPTIMIZED, 12));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(ONLY_READWRITE_ZONE_OPTIMIZED, 13));


  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MAX_ROUTE_POLICY_COUNT, 0));
  EXPECT_TRUE(ROUTE_TYPE_MAX == ObLDCRoute::get_route_type(MAX_ROUTE_POLICY_COUNT, 13));

  EXPECT_TRUE(SAME_IDC == ObLDCRoute::get_idc_type(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL));
  EXPECT_TRUE(SAME_REGION == ObLDCRoute::get_idc_type(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION));
  EXPECT_TRUE(OTHER_REGION == ObLDCRoute::get_idc_type(ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE));
  EXPECT_TRUE(MAX_IDC_TYPE == ObLDCRoute::get_idc_type(ROUTE_TYPE_LEADER));

  ObLDCItem test_item;
  EXPECT_TRUE(!test_item.is_valid());
  test_item.is_merging_ = true;
  EXPECT_TRUE(ObLDCRoute::is_same_merge_type(ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE, test_item));
  EXPECT_TRUE(!ObLDCRoute::is_same_merge_type(ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL, test_item));
  test_item.zone_type_ = ZONE_TYPE_INVALID;
  EXPECT_TRUE(!ObLDCRoute::is_same_zone_type(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_item));
  EXPECT_TRUE(!ObLDCRoute::is_same_zone_type(ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL, test_item));
  EXPECT_TRUE(ObLDCRoute::is_same_zone_type(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_item));
  test_item.zone_type_ = ZONE_TYPE_READWRITE;
  EXPECT_TRUE(ObLDCRoute::is_same_zone_type(ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE, test_item));
  EXPECT_TRUE(!ObLDCRoute::is_same_zone_type(ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL, test_item));
  EXPECT_TRUE(ObLDCRoute::is_same_zone_type(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_item));
  test_item.is_partition_server_ = true;
  EXPECT_TRUE(ObLDCRoute::is_same_partition_type(ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE, test_item));
  EXPECT_TRUE(!ObLDCRoute::is_same_partition_type(ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL, test_item));
  EXPECT_TRUE(!test_item.is_valid());
  test_item.idc_type_ = SAME_IDC;
  EXPECT_TRUE(!test_item.is_valid());
  ObProxyReplicaLocation replica(ObAddr(ObAddr::IPV4, "6.6.6.6", 1), LEADER, REPLICA_TYPE_FULL);
  test_item.replica_ = &replica;
  EXPECT_TRUE(test_item.is_valid());



  ObLDCRoute test_ldc_route;
  test_ldc_route.curr_cursor_index_ = 0;
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  test_ldc_route.curr_cursor_index_ = 11;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  test_ldc_route.curr_cursor_index_ = 12;
  EXPECT_TRUE(test_ldc_route.is_reach_end());

  test_ldc_route.policy_ = UNMERGE_ZONE_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  test_ldc_route.curr_cursor_index_ = 23;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  test_ldc_route.curr_cursor_index_ = 24;
  EXPECT_TRUE(test_ldc_route.is_reach_end());

  EXPECT_TRUE(test_ldc_route.location_.get_idc_name().empty());
  EXPECT_TRUE(test_ldc_route.location_.is_empty());
  EXPECT_TRUE(0 == test_ldc_route.location_.count());
  EXPECT_TRUE(0 == test_ldc_route.location_.get_same_idc_count());
  EXPECT_TRUE(0 == test_ldc_route.location_.get_same_region_count());
  EXPECT_TRUE(0 == test_ldc_route.location_.get_other_region_count());


  ObProxyPartitionLocation pl;
  test_ldc_route.location_.set_partition(&pl);

  common::ObSEArray<ObProxyReplicaLocation, 20> replicas;
  for (int64_t i = 0; i < 5; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 5; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 5; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  for (int64_t i = 0; i < 5; i++) {
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  ASSERT_TRUE(!ts.is_valid());
  ASSERT_TRUE(ts.is_empty());
  start_tenant_server(ts, replicas, 4);
  ASSERT_TRUE(ts.is_valid());
  ASSERT_TRUE(!ts.is_empty());
  test_ldc_route.location_.set_tenant_server(&ts);
  EXPECT_TRUE(0 == test_ldc_route.location_.get_same_idc_count());

  test_ldc_route.reset_cursor();
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, assign)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  ASSERT_TRUE(!ts.is_valid());
  ASSERT_TRUE(ts.is_empty());
  ObString cluster_name;
  ASSERT_EQ(OB_INVALID_ARGUMENT, test_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(OB_INVALID_ARGUMENT, test_ldc.assign(NULL, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));

  start_tenant_server(ts, replicas, 4);
  ASSERT_TRUE(ts.is_valid());
  ASSERT_TRUE(!ts.is_empty());
  ASSERT_EQ(OB_INVALID_ARGUMENT, test_ldc.assign(&ts, ss_info_, ObString::make_string("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"), true, cluster_name, OB_DEFAULT_CLUSTER_ID));

  ObSEArray<ObServerStateSimpleInfo, 35> empty_ss_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, test_ldc.assign(&ts, empty_ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));

  //error name treat as do not use
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info_, ObString::make_string("z4"), true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(!test_ldc.is_ldc_used());
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(40, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(40, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(40, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(40, site_start_index_array[MAX_IDC_TYPE]);
  ASSERT_FALSE(test_ldc.get_idc_name().empty());
  ASSERT_FALSE(test_ldc.is_ldc_used());
  ASSERT_EQ(&ts, test_ldc.get_tenant_server());

  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = 0; i < 10; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 10; i < 25; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 25; i < 40; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }

  //empty name treat as do not use
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info_, ObString::make_empty_string(), true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(!test_ldc.is_ldc_used());
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(40, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());
  site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(40, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(40, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(40, site_start_index_array[MAX_IDC_TYPE]);
  ASSERT_TRUE(test_ldc.get_idc_name().empty());
  ASSERT_EQ(&ts, test_ldc.get_tenant_server());

  item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = 0; i < 10; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 10; i < 25; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 25; i < 40; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }

  //right name, but congestion is not available
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, empty_ss_info, idc_name, false, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(!test_ldc.is_ldc_used());
  ASSERT_EQ(60, test_ldc.count());
  ASSERT_EQ(60, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());
  site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(60, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(60, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(60, site_start_index_array[MAX_IDC_TYPE]);
  ASSERT_FALSE(test_ldc.get_idc_name().empty());
  ASSERT_FALSE(test_ldc.is_ldc_used());
  ASSERT_EQ(&ts, test_ldc.get_tenant_server());
  item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = 0; i < 15; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 15; i < 30; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 30; i < 45; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 45; i < 60; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("4.4.4.4"), item_array[i].replica_->server_.get_ipv4());
  }

  //right name
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(test_ldc.is_ldc_used());
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(10, test_ldc.get_same_idc_count());
  ASSERT_EQ(15, test_ldc.get_same_region_count());
  ASSERT_EQ(15, test_ldc.get_other_region_count());
  site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(10, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(25, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(40, site_start_index_array[MAX_IDC_TYPE]);
  ASSERT_EQ(idc_name, test_ldc.get_idc_name());
  ASSERT_EQ(&ts, test_ldc.get_tenant_server());
  item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    EXPECT_EQ(SAME_REGION, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    EXPECT_EQ(OTHER_REGION, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_IDC] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[SAME_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[OTHER_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_IDC] + 2; i < site_start_index_array[SAME_IDC] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION] + 2; i < site_start_index_array[SAME_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION] + 2; i < site_start_index_array[OTHER_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  //============================================
  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  LOG_DEBUG("begin", K(test_ldc_route));

  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  const ObLDCItem *item = NULL;

  TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_REGION, false, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  TEST2_GET_NEXT_ITEM(SAME_REGION, false, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "1.1.1.1", 4, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "1.1.1.1", 5, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_REGION, true, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_MERGE_REGION);
  TEST2_GET_NEXT_ITEM(SAME_REGION, true, true, "2.2.2.2", 4, ROUTE_TYPE_PARTITION_MERGE_REGION);
  TEST2_GET_NEXT_ITEM(SAME_REGION, true, true, "2.2.2.2", 5, ROUTE_TYPE_PARTITION_MERGE_REGION);

  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "1.1.1.1", 8, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  for (int64_t j = 8; j <= 15; j++) {
    TEST2_GET_NEXT_ITEM(SAME_REGION, false, false, "2.2.2.2", j, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  }
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, false, "1.1.1.1", 1, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, false, "1.1.1.1", 2, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_REGION, true, false, "2.2.2.2", 1, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  TEST2_GET_NEXT_ITEM(SAME_REGION, true, false, "2.2.2.2", 2, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  TEST2_GET_NEXT_ITEM(OTHER_REGION, false, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, false, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "3.3.3.3", 4, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "3.3.3.3", 5, ROUTE_TYPE_PARTITION_MERGE_REMOTE);

  for (int64_t j = 8; j <= 15; j++) {
    TEST2_GET_NEXT_ITEM(OTHER_REGION, false, false, "3.3.3.3", j, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  }

  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, false, "3.3.3.3", 1, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, false, "3.3.3.3", 2, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  item = test_ldc_route.get_next_item();
  EXPECT_TRUE(NULL == item);
  EXPECT_TRUE(test_ldc_route.is_reach_end());

  int64_t old_hash = hash(test_ldc);
  test_ldc.shuffle();
  int64_t new_hash = hash(test_ldc);
  EXPECT_NE(0, new_hash);
  EXPECT_NE(old_hash, new_hash);
}


TEST_F(TesLDCLocation, assign_other_region)
{
  ObString idc_name("z3");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
    replicas.push_back(replicas_z2_.at(i));
    replicas.push_back(replicas_z3_.at(i));
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));

  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(15, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(25, test_ldc.get_other_region_count());

  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(15, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(15, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(40, site_start_index_array[MAX_IDC_TYPE]);
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    EXPECT_EQ(OTHER_REGION, item_array[i].idc_type_);
    EXPECT_NE(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_IDC] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[OTHER_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_IDC] + 2; i < site_start_index_array[SAME_IDC] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION] + 2; i < site_start_index_array[OTHER_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  //============================================
  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  const ObLDCItem *item = NULL;
  LOG_DEBUG("begin", K(test_ldc_route));

  TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "3.3.3.3", 4, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "3.3.3.3", 5, ROUTE_TYPE_PARTITION_MERGE_LOCAL);

  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 8, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 11, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 12, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 13, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, false, "3.3.3.3", 1, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  TEST2_GET_NEXT_ITEM(SAME_IDC, true, false, "3.3.3.3", 2, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  TEST2_GET_NEXT_ITEM(OTHER_REGION, false, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, false, true, "1.1.1.1", 4, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);

  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  TEST2_GET_NEXT_ITEM(OTHER_REGION, true, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_MERGE_REMOTE);

  TEST2_GET_NEXT_ITEM(OTHER_REGION, false, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  item = test_ldc_route.get_next_item();
  EXPECT_TRUE(NULL != item);
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, assign_non_ldc)
{
  ObString idc_name("z4");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));

  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(!test_ldc.is_ldc_used());
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(40, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());

  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(40, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(40, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(40, site_start_index_array[MAX_IDC_TYPE]);
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = 0; i < 10; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 10; i < 25; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
  }
  for (int64_t i = 25; i < 40; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
  }

  for (int64_t i = 0; i < 10; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = 0; i < 40; i += 2) {
    item_array[i].is_partition_server_ = true;
  }

  //============================================
  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  const ObLDCItem *item = NULL;
  LOG_DEBUG("begin", K(test_ldc_route));

  for (int64_t j = 1; j <= 15; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "2.2.2.2", j, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  }
  for (int64_t j = 2; j <= 14; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, false, true, "3.3.3.3", j, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  }

  for (int64_t j = 1; j <= 9; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, true, true, "1.1.1.1", j, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  }

  for (int64_t j = 2; j <= 14; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "2.2.2.2", j, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  }
  for (int64_t j = 1; j <= 15; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, false, false, "3.3.3.3", j, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  }

  for (int64_t j = 2; j <= 10; j += 2) {
    TEST2_GET_NEXT_ITEM(SAME_IDC, true, false, "1.1.1.1", j, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  }

  item = test_ldc_route.get_next_item();
  EXPECT_TRUE(NULL == item);
  EXPECT_TRUE(test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_MAX, test_ldc_route.get_curr_route_type());
}

TEST_F(TesLDCLocation, fill_strong_read_location)
{
  ObString idc_name("z1");
  ObLDCLocation dummy_ldc;
  ObLDCLocation target_ldc;
  ObProxyPartitionLocation pl;
  common::ObSEArray<ObProxyReplicaLocation, 2> replicas2;
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 2), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 3), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 4), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 5), FOLLOWER, REPLICA_TYPE_FULL));
  pl.set_replicas(replicas2);

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
    replicas.push_back(replicas_z2_.at(i));
    replicas.push_back(replicas_z3_.at(i));
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;

  ObLDCItem leader_item;
  bool entry_need_update = true;
  bool is_only_readwrite_zone = false;
  bool need_use_dup_replica = false;
  ObSEArray<ObString, 5> region_names;
  ObSEArray<ObServerStateSimpleInfo, 5> servers_info;
  ObString proxy_primary_zone_name;
  bool need_skip_leader = false;
  bool is_random_route_mode = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLDCLocation::fill_strong_read_location(&pl, dummy_ldc, leader_item, target_ldc, 
            entry_need_update, is_only_readwrite_zone, need_use_dup_replica, need_skip_leader, is_random_route_mode,
            servers_info, region_names, proxy_primary_zone_name, ObString(), NULL));

  ASSERT_EQ(OB_SUCCESS, dummy_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(dummy_ldc.is_ldc_used());
  ASSERT_EQ(0, dummy_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(10, dummy_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(25, dummy_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, dummy_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
  const int64_t *site_start_index_array = dummy_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(dummy_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_IDC] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[SAME_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[OTHER_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_IDC] + 2; i < site_start_index_array[SAME_IDC] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION] + 2; i < site_start_index_array[SAME_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
    item_array[i].zone_type_ = ZONE_TYPE_READONLY;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION] + 2; i < site_start_index_array[OTHER_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }
  LOG_DEBUG("begin", KPC(dummy_ldc.get_tenant_server()));

  ASSERT_TRUE(!dummy_ldc.is_empty());

  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_strong_read_location(&pl, dummy_ldc, leader_item,
      target_ldc, entry_need_update, is_only_readwrite_zone, need_use_dup_replica, need_skip_leader,
      is_random_route_mode, servers_info, region_names, proxy_primary_zone_name, ObString(), NULL));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(!entry_need_update);
  ASSERT_TRUE(!leader_item.is_valid());
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(dummy_ldc.count(), target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }


  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 1), LEADER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 20), FOLLOWER, REPLICA_TYPE_FULL));
  pl.set_replicas(replicas2);

  is_only_readwrite_zone = true;
  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_strong_read_location(&pl, dummy_ldc, leader_item,
      target_ldc, entry_need_update, is_only_readwrite_zone, need_use_dup_replica, need_skip_leader,
      is_random_route_mode, servers_info, region_names, proxy_primary_zone_name, ObString(), NULL));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(entry_need_update);
  ASSERT_TRUE(leader_item.is_valid());
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(dummy_ldc.count() - 16, target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }


  is_only_readwrite_zone = false;
  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_strong_read_location(&pl, dummy_ldc, leader_item,
      target_ldc, entry_need_update, is_only_readwrite_zone, need_use_dup_replica, need_skip_leader,
      is_random_route_mode, servers_info, region_names, proxy_primary_zone_name, ObString(), NULL));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(entry_need_update);
  ASSERT_TRUE(leader_item.is_valid());
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(dummy_ldc.count() - 1, target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }
}

TEST_F(TesLDCLocation, set_ldc_location)
{
  ObLDCLocation target_ldc;
  ObLDCLocation dummy_ldc;
  ObProxyPartitionLocation pl;
  ObSEArray<ObLDCItem, 10> tmp_item_array;
  ObSEArray<ObLDCItem, 10> tmp_pz_item_array;
  ASSERT_EQ(OB_SUCCESS, target_ldc.set_ldc_location(&pl, dummy_ldc, tmp_item_array, tmp_pz_item_array));
  ASSERT_TRUE(!target_ldc.is_ldc_used());
  ASSERT_TRUE(target_ldc.is_empty());

  ObProxyReplicaLocation replica(ObAddr(ObAddr::IPV4, "2.2.2.2", 1), LEADER, REPLICA_TYPE_FULL);
  ObLDCItem same_idc_item(replica, false, SAME_IDC, ZONE_TYPE_READWRITE, false);
  ObLDCItem same_region_item(replica, true, SAME_REGION, ZONE_TYPE_READWRITE, false);
  ObLDCItem other_region_item(replica, false, OTHER_REGION, ZONE_TYPE_READWRITE, true);

  tmp_item_array.push_back(other_region_item);
  tmp_item_array.push_back(same_region_item);
  tmp_item_array.push_back(other_region_item);
  tmp_item_array.push_back(same_idc_item);
  tmp_item_array.push_back(same_region_item);
  tmp_item_array.push_back(other_region_item);
  tmp_item_array.push_back(same_idc_item);
  tmp_item_array.push_back(same_region_item);
  tmp_item_array.push_back(same_idc_item);
  tmp_item_array.push_back(other_region_item);
  tmp_item_array.push_back(same_idc_item);
  tmp_item_array.push_back(same_region_item);

  ASSERT_EQ(OB_SUCCESS, target_ldc.set_ldc_location(&pl, dummy_ldc, tmp_item_array, tmp_pz_item_array));
  ASSERT_TRUE(!target_ldc.is_ldc_used());
  ASSERT_TRUE(!target_ldc.is_empty());
  ASSERT_EQ(12, target_ldc.count());
  ASSERT_EQ(4, target_ldc.get_other_region_count());
  ASSERT_EQ(4, target_ldc.get_same_region_count());
  ASSERT_EQ(4, target_ldc.get_same_idc_count());
  ASSERT_TRUE(false == target_ldc.get_item(0)->is_merging_);
  ASSERT_TRUE(false == target_ldc.get_item(0)->is_force_congested_);
  ASSERT_TRUE(true == target_ldc.get_item(4)->is_merging_);
  ASSERT_TRUE(false == target_ldc.get_item(4)->is_force_congested_);
  ASSERT_TRUE(false == target_ldc.get_item(8)->is_merging_);
  ASSERT_TRUE(true == target_ldc.get_item(8)->is_force_congested_);
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }

}

TEST_F(TesLDCLocation, fill_weak_read_location)
{
  ObString idc_name("z1");
  ObLDCLocation dummy_ldc;
  ObLDCLocation target_ldc;
  ObProxyPartitionLocation pl;
  common::ObSEArray<ObProxyReplicaLocation, 2> replicas2;
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 2), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 3), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 4), FOLLOWER, REPLICA_TYPE_FULL));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 5), FOLLOWER, REPLICA_TYPE_FULL));
  pl.set_replicas(replicas2);

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
    replicas.push_back(replicas_z2_.at(i));
    replicas.push_back(replicas_z3_.at(i));
    replicas.push_back(replicas_z4_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;

  bool entry_need_update = true;
  bool is_only_readonly_zone = false;
  ObSEArray<ObString, 5> region_names;
  ObSEArray<ObServerStateSimpleInfo, 5> servers_info;
  ObString proxy_primary_zone_name;

  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLDCLocation::fill_weak_read_location(&pl, dummy_ldc,
      target_ldc, entry_need_update, is_only_readonly_zone, servers_info, region_names, proxy_primary_zone_name));

  ASSERT_EQ(OB_SUCCESS, dummy_ldc.assign(&ts, ss_info_, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(dummy_ldc.is_ldc_used());
  ASSERT_EQ(0, dummy_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(10, dummy_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(25, dummy_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, dummy_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
  const int64_t *site_start_index_array = dummy_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(dummy_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_IDC] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[SAME_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[OTHER_REGION] + 5; i++) {
    item_array[i].is_merging_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_IDC] + 2; i < site_start_index_array[SAME_IDC] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION] + 2; i < site_start_index_array[SAME_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].zone_type_ = ZONE_TYPE_READONLY;
  }

  for (int64_t i = site_start_index_array[OTHER_REGION] + 2; i < site_start_index_array[OTHER_REGION] + 7; i++) {
    item_array[i].is_partition_server_ = true;
  }
  LOG_DEBUG("begin", KPC(dummy_ldc.get_tenant_server()));

  ASSERT_TRUE(!dummy_ldc.is_empty());

  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_weak_read_location(&pl, dummy_ldc,
      target_ldc, entry_need_update, is_only_readonly_zone, servers_info, region_names, proxy_primary_zone_name));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(!entry_need_update);
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(dummy_ldc.count(), target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }

  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 1), FOLLOWER, REPLICA_TYPE_READONLY));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 6), FOLLOWER, REPLICA_TYPE_READONLY));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 7), FOLLOWER, REPLICA_TYPE_LOGONLY));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 8), FOLLOWER, REPLICA_TYPE_LOGONLY));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 9), FOLLOWER, REPLICA_TYPE_LOGONLY));
  replicas2.push_back(ObProxyReplicaLocation(ObAddr(ObAddr::IPV4, "2.2.2.2", 20), FOLLOWER, REPLICA_TYPE_FULL));
  pl.set_replicas(replicas2);

  is_only_readonly_zone = true;
  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_weak_read_location(&pl, dummy_ldc,
      target_ldc, entry_need_update, is_only_readonly_zone, servers_info, region_names, proxy_primary_zone_name));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(entry_need_update);
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(17, target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }

  is_only_readonly_zone = false;
  ASSERT_EQ(OB_SUCCESS, ObLDCLocation::fill_weak_read_location(&pl, dummy_ldc,
      target_ldc, entry_need_update, is_only_readonly_zone, servers_info, region_names, proxy_primary_zone_name));
  ASSERT_TRUE(target_ldc.is_ldc_used());
  ASSERT_TRUE(entry_need_update);
  ASSERT_EQ(dummy_ldc.get_idc_name(), target_ldc.get_idc_name());
  ASSERT_EQ(dummy_ldc.count() - 3, target_ldc.count());
  ASSERT_EQ(dummy_ldc.get_tenant_server(), target_ldc.get_tenant_server());
  ASSERT_EQ(&pl, target_ldc.get_partition_location());
  for (int64_t i = 0; i < target_ldc.count(); i++) {
    ASSERT_TRUE(target_ldc.get_item(i)->is_valid());
    ASSERT_FALSE(target_ldc.get_item(i)->is_used_);
  }
}


TEST_F(TesLDCLocation, set_logic_region)
{
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info;
  fill_server_info("ZUE", "ZTG", "ZUE","z1", "z2", "z3", ss_info);

  ObLDCLocation test_ldc;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 3);
  ObString idc_name("ZUE");

  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(25, test_ldc.get_same_idc_count());
  ASSERT_EQ(15, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());

  ASSERT_EQ(0, test_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(25, test_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
}

TEST_F(TesLDCLocation, set_logic_region2)
{
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info;
  fill_server_info("ZUE", "ZUE", "ZTG","z1", "z2", "z3", ss_info);

  ObLDCLocation test_ldc;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 3);
  ObString idc_name("ZUE");

  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(25, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(15, test_ldc.get_other_region_count());

  ASSERT_EQ(0, test_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(25, test_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(25, test_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
}

TEST_F(TesLDCLocation, set_logic_region3)
{
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info;
  fill_server_info("ZUE", "ZUE", "ZUE","z1", "z2", "z3", ss_info);

  ObLDCLocation test_ldc;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 3);
  ObString idc_name("ZUE");
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(40, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());

  ASSERT_EQ(0, test_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
}

TEST_F(TesLDCLocation, set_logic_region4)
{
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info;
  fill_server_info("default_idc", "default_idc", "default_idc","z1", "z12", "z13", ss_info);

  ObLDCLocation test_ldc;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 3);
  ObString idc_name("z1");
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(40, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(0, test_ldc.get_other_region_count());

  ASSERT_EQ(0, test_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
}

TEST_F(TesLDCLocation, set_logic_region5)
{
  ObSEArray<ObServerStateSimpleInfo, 45> ss_info;
  fill_server_info("default_idc", "default_idc", "default_idc","z1", "z12", "z3", ss_info);

  ObLDCLocation test_ldc;
  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 15; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 3);
  ObString idc_name("Z1"); // case unsensive

  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_EQ(40, test_ldc.count());
  ASSERT_EQ(25, test_ldc.get_same_idc_count());
  ASSERT_EQ(0, test_ldc.get_same_region_count());
  ASSERT_EQ(15, test_ldc.get_other_region_count());

  ASSERT_EQ(0, test_ldc.get_site_start_index_array()[SAME_IDC]);
  ASSERT_EQ(25, test_ldc.get_site_start_index_array()[SAME_REGION]);
  ASSERT_EQ(25, test_ldc.get_site_start_index_array()[OTHER_REGION]);
  ASSERT_EQ(40, test_ldc.get_site_start_index_array()[MAX_IDC_TYPE]);
}

TEST_F(TesLDCLocation, get_next_item)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;

  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  ASSERT_TRUE(!test_ldc.is_empty());
  ASSERT_TRUE(test_ldc.is_ldc_used());
  ASSERT_EQ(60, test_ldc.count());
  ASSERT_EQ(20, test_ldc.get_same_idc_count());
  ASSERT_EQ(20, test_ldc.get_same_region_count());
  ASSERT_EQ(20, test_ldc.get_other_region_count());
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  site_start_index_array = test_ldc.get_site_start_index_array();
  ASSERT_EQ(0, site_start_index_array[SAME_IDC]);
  ASSERT_EQ(20, site_start_index_array[SAME_REGION]);
  ASSERT_EQ(40, site_start_index_array[OTHER_REGION]);
  ASSERT_EQ(60, site_start_index_array[MAX_IDC_TYPE]);
  ASSERT_EQ(idc_name, test_ldc.get_idc_name());
  ASSERT_EQ(&ts, test_ldc.get_tenant_server());
  item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());
  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    EXPECT_EQ(SAME_IDC, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("1.1.1.1"), item_array[i].replica_->server_.get_ipv4());
    EXPECT_EQ((i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY), item_array[i].zone_type_);
    EXPECT_TRUE((i % 10 < 5) == item_array[i].is_merging_);
  }
  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    EXPECT_EQ(SAME_REGION, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("2.2.2.2"), item_array[i].replica_->server_.get_ipv4());
    EXPECT_EQ((i < (10 + site_start_index_array[SAME_REGION])? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY), item_array[i].zone_type_);
    EXPECT_TRUE((i % 10 < 5) == item_array[i].is_merging_);
  }
  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    EXPECT_EQ(OTHER_REGION, item_array[i].idc_type_);
    EXPECT_EQ(ObAddr::convert_ipv4_addr("3.3.3.3"), item_array[i].replica_->server_.get_ipv4());
    EXPECT_EQ((i < (10 + site_start_index_array[OTHER_REGION]) ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY), item_array[i].zone_type_);
    EXPECT_TRUE((i % 10 < 5) == item_array[i].is_merging_);
  }

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================
  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = READONLY_ZONE_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  ObRouteType route_type = ROUTE_TYPE_MAX;
  common::ObZoneType zone_type = ZONE_TYPE_INVALID;
  EXPECT_TRUE(test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "1.1.1.1", 16), ZONE_TYPE_INVALID));
  EXPECT_EQ(ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL, route_type);
  EXPECT_EQ(ZONE_TYPE_READONLY, zone_type);
  EXPECT_TRUE(test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "1.1.1.1", 16), ZONE_TYPE_READONLY));
  EXPECT_EQ(ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL, route_type);
  EXPECT_EQ(ZONE_TYPE_READONLY, zone_type);

  EXPECT_TRUE(test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "2.2.2.2", 16), ZONE_TYPE_INVALID));
  EXPECT_EQ(ROUTE_TYPE_NONPARTITION_UNMERGE_REGION, route_type);
  EXPECT_EQ(ZONE_TYPE_READONLY, zone_type);
  EXPECT_TRUE(test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "2.2.2.2", 16), ZONE_TYPE_READONLY));
  EXPECT_EQ(ROUTE_TYPE_NONPARTITION_UNMERGE_REGION, route_type);
  EXPECT_EQ(ZONE_TYPE_READONLY, zone_type);

  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "1.1.1.1", 13), ZONE_TYPE_INVALID));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);
  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "1.1.1.1", 13), ZONE_TYPE_READONLY));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);

  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "2.2.2.2", 13), ZONE_TYPE_INVALID));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);
  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "2.2.2.2", 13), ZONE_TYPE_READONLY));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);

  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "3.3.3.3", 16), ZONE_TYPE_INVALID));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);
  EXPECT_TRUE(!test_ldc_route.location_.is_in_same_region_unmerging(route_type, zone_type, ObAddr(ObAddr::IPV4, "3.3.3.3", 16), ZONE_TYPE_READONLY));
  EXPECT_EQ(ROUTE_TYPE_MAX, route_type);
  EXPECT_EQ(common::ZONE_TYPE_INVALID, zone_type);

}

TEST_F(TesLDCLocation, get_next_item0)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = READONLY_ZONE_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
  //ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
  //CNRP, CMRP, CNWP, CMWP
  //CNRT, CMRT, CNWT, CMWT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, get_next_item2)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = ONLY_READONLY_ZONE;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANRP, BNRP, AMRP, BMRP;
  //ANRT, BNRT, AMRT, BMRT;
  //CNRP, CMRP
  //CNRT, CMRT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, get_next_item3)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = UNMERGE_ZONE_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
  //ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
  //CNRP, CNWP, CMRP, CMWP
  //CNRT, CNWT, CMRT, CMWT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, get_next_item4)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANP, BNP, AMP, BMP;
  //ANT, BNT, AMT, BMT;
  //CNP, CMP;
  //CNT, CMT

  //ANP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);

  //BNP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_UNMERGE_REGION);

  //AMP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_MERGE_LOCAL);

  //BMP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_MERGE_REGION);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);

  //CMP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_MERGE_REMOTE);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}


TEST_F(TesLDCLocation, get_next_item42)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER;
  test_ldc_route.disable_merge_status_check_ = true;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //AP, BP
  //AT, BT
  //CP, CP;
  //CT, CT

  //AP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);

  //BP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_UNMERGE_REGION);

  //AT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //CP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);

  //CT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}

TEST_F(TesLDCLocation, get_next_item5)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));
  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = ONLY_READWRITE_ZONE;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANWP, BNWP, AMWP, BMWP;
  //ANWT, BNWT, AMWT, BMWT;
  //CNWP, CMWP
  //CNWT, CMWT

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);


  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}

TEST_F(TesLDCLocation, get_next_item52)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));
  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = READONLY_ZONE_FIRST_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANRP, BNRP, AMRP, BMRP, ANWP, BNWP, AMWP, BMWP;
  //CNRP, CMRP, CNWP, CMWP
  //ANRT, BNRT, AMRT, BMRT, ANWT, BNWT, AMWT, BMWT;
  //CNRT, CMRT, CNWT, CMWT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);


  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);


  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}

TEST_F(TesLDCLocation, get_next_item6)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }
  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));
  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = ONLY_READONLY_ZONE_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANRP, BNRP, AMRP, BMRP;
  //CNRP, CMRP
  //ANRT, BNRT, AMRT, BMRT;
  //CNRT, CMRT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}

TEST_F(TesLDCLocation, get_next_item7)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  LOG_DEBUG("begin", K(test_ldc));
  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = UNMERGE_ZONE_FIRST_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  //ANRP, BNRP, ANWP, BNWP, AMRP, BMRP, AMWP, BMWP;
  //CNRP, CNWP, CMRP, CMWP
  //ANRT, BNRT, ANWT, BNWT, AMRT, BMRT, AMWT, BMWT;
  //CNRT, CNWT, CMRT, CMWT

  //ANRP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_LOCAL);

  //BNRP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REGION);

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMRP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_LOCAL);

  //BMRP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);


  //CNRP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_READONLY_UNMERGE_REMOTE);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMRP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_READONLY_MERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //ANRT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_LOCAL);

  //BNRT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REGION);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMRT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_LOCAL);

  //BMRT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNRT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_READONLY_UNMERGE_REMOTE);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMRT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_READONLY_MERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);


  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}
TEST_F(TesLDCLocation, get_next_item8)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }
  LOG_DEBUG("begin", K(test_ldc));
  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = ONLY_READWRITE_ZONE_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANWP, BNWP, AMWP, BMWP;
  //CNWP, CMWP
  //ANWT, BNWT, AMWT, BMWT;
  //CNWT, CMWT

  //ANWP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REGION);

  //AMWP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_LOCAL);

  //BMWP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REGION);

  //CNWP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_READWRITE_MERGE_REMOTE);

  //ANWT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_LOCAL);

  //BNWT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REGION);

  //AMWT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_LOCAL);

  //BMWT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REGION);

  //CNWT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_READWRITE_UNMERGE_REMOTE);

  //CMWT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_READWRITE_MERGE_REMOTE);


  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}

TEST_F(TesLDCLocation, get_next_item9)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
  }
  LOG_DEBUG("begin", K(test_ldc));
  //=================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = MERGE_IDC_ORDER_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);
  //ANP, BNP, AMP, BMP;
  //CNP, CMP;
  //ANT, BNT, AMT, BMT;
  //CNT, CMT

  //ANP
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 6, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 7, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, true, "1.1.1.1", 8, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 16, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 17, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, true, "1.1.1.1", 18, ROUTE_TYPE_PARTITION_UNMERGE_LOCAL);

  //BNP
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 6, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 7, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, true, "2.2.2.2", 8, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 16, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 17, ROUTE_TYPE_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, true, "2.2.2.2", 18, ROUTE_TYPE_PARTITION_UNMERGE_REGION);

  //AMP
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 1, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 2, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, true, "1.1.1.1", 3, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 11, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 12, ROUTE_TYPE_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, true, "1.1.1.1", 13, ROUTE_TYPE_PARTITION_MERGE_LOCAL);

  //BMP
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 1, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 2, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, true, "2.2.2.2", 3, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 11, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 12, ROUTE_TYPE_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, true, "2.2.2.2", 13, ROUTE_TYPE_PARTITION_MERGE_REGION);

  //CNP
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 6, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 7, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, true, "3.3.3.3", 8, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 16, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 17, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, true, "3.3.3.3", 18, ROUTE_TYPE_PARTITION_UNMERGE_REMOTE);

  //CMP
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 1, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 2, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, true, "3.3.3.3", 3, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 11, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 12, ROUTE_TYPE_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, true, "3.3.3.3", 13, ROUTE_TYPE_PARTITION_MERGE_REMOTE);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READWRITE, false, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, ZONE_TYPE_READONLY, false, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READWRITE, false, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, ZONE_TYPE_READONLY, false, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READWRITE, false, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, ZONE_TYPE_READONLY, false, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READWRITE, false, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, ZONE_TYPE_READONLY, false, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READWRITE, false, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, ZONE_TYPE_READONLY, false, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READWRITE, false, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, ZONE_TYPE_READONLY, false, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());

}


TEST_F(TesLDCLocation, get_next_item10)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 0) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 1) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 2) ? LEADER : FOLLOWER);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = FOLLOWER_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANPF, BNPF, AMPF, BMPF;
  //ANPL, BNPL, AMPL, BMPL;
  //ANT, BNT, AMT, BMT;
  //CNPF, CMPF;
  //CNPL, CMPL;
  //CNT, CMT

  //ANPF
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);

  //BNPF
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false,  true, FOLLOWER, "2.2.2.2", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);

  //AMPF
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);

  //BMPF
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);

  //ANPL
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 6, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 16, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);

  //BNPL
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 7, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 17, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);

  //AMPL
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 1, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 11, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);

  //BMPL
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 2, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 12, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNPF
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);

  //CMPF
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);

  //CNPL
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 8, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 18, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);

  //CMPL
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 3, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 13, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}


TEST_F(TesLDCLocation, get_next_item11)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 0) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 1) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 2) ? LEADER : FOLLOWER);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = UNMERGE_FOLLOWER_FIRST;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANPF, BNPF
  //ANPL, BNPL
  //AMPF, BMPF;
  //AMPL, BMPL;
  //ANT, BNT, AMT, BMT;
  //CNPF, CNPL;
  //CMPF, CMPL;
  //CNT, CMT

  //ANPF
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);

  //BNPF
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false,  true, FOLLOWER, "2.2.2.2", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);

  //ANPL
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 6, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 16, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);

  //BNPL
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 7, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 17, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);

  //AMPF
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);

  //BMPF
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);

  //AMPL
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 1, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 11, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);

  //BMPL
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 2, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 12, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNPF
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);

  //CNPL
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 8, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 18, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);

  //CMPF
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);

  //CMPL
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 3, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 13, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}


TEST_F(TesLDCLocation, get_next_item12)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 0) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 1) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 2) ? LEADER : FOLLOWER);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = FOLLOWER_FIRST_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANPF, BNPF, AMPF, BMPF;
  //ANPL, BNPL, AMPL, BMPL;
  //CNPF, CMPF;
  //CNPL, CMPL;
  //ANT, BNT, AMT, BMT;
  //CNT, CMT

  //ANPF
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);

  //BNPF
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false,  true, FOLLOWER, "2.2.2.2", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);

  //AMPF
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);

  //BMPF
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);

  //ANPL
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 6, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 16, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);

  //BNPL
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 7, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 17, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);

  //AMPL
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 1, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 11, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);

  //BMPL
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 2, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 12, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);

  //CNPF
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);

  //CMPF
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);

  //CNPL
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 8, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 18, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);

  //CMPL
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 3, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 13, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}


TEST_F(TesLDCLocation, get_next_item13)
{
  ObString idc_name("z1");
  ObLDCRoute test_ldc_route;
  ObLDCLocation &test_ldc = test_ldc_route.location_;


  ObSEArray<ObServerStateSimpleInfo, 60> ss_info;
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z1");
    info.set_idc_name("z1");
    info.set_addr(ObAddr(ObAddr::IPV4, "1.1.1.1", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("HZ"));
    info.set_zone_name("z2");
    info.set_idc_name("z2");
    info.set_addr(ObAddr(ObAddr::IPV4, "2.2.2.2", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }
  for (int32_t i = 0; i < 20; i++) {
    ObServerStateSimpleInfo info;
    info.set_region_name(ObString::make_string("SH"));
    info.set_zone_name("z3");
    info.set_idc_name("z3");
    info.set_addr(ObAddr(ObAddr::IPV4, "3.3.3.3", i + 1));
    info.is_merging_ = (i % 10  < 5 ? true : false);
    info.zone_type_ = (i < 10 ? ZONE_TYPE_READWRITE : ZONE_TYPE_READONLY);
    ss_info.push_back(info);
  }

  common::ObSEArray<ObProxyReplicaLocation, 60> replicas;
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z1_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z2_.at(i));
  }
  for (int64_t i = 0; i < 20; i++) {
    replicas.push_back(replicas_z3_.at(i));
  }

  ObTenantServer ts;
  start_tenant_server(ts, replicas, 4);
  ObString cluster_name;
  ASSERT_EQ(OB_SUCCESS, test_ldc.assign(&ts, ss_info, idc_name, true, cluster_name, OB_DEFAULT_CLUSTER_ID));
  const int64_t *site_start_index_array = test_ldc.get_site_start_index_array();
  ObLDCItem *item_array = const_cast<ObLDCItem *>(test_ldc.get_item_array());

  for (int64_t i = site_start_index_array[SAME_IDC]; i < site_start_index_array[SAME_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 0) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[SAME_REGION]; i < site_start_index_array[OTHER_REGION]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 1) ? LEADER : FOLLOWER);
  }

  for (int64_t i = site_start_index_array[OTHER_REGION]; i < site_start_index_array[MAX_IDC_TYPE]; i++) {
    item_array[i].is_partition_server_ = (i % 5 < 3);
    const_cast<ObProxyReplicaLocation *>(item_array[i].replica_)->role_ = ((i % 5 == 2) ? LEADER : FOLLOWER);
  }

  LOG_DEBUG("begin", K(test_ldc));

  //============================================

  test_ldc_route.reset_cursor();
  test_ldc_route.policy_ = UNMERGE_FOLLOWER_FIRST_OPTIMIZED;
  EXPECT_TRUE(!test_ldc_route.is_reach_end());
  EXPECT_EQ(ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL, test_ldc_route.get_curr_route_type());
  EXPECT_EQ(0, test_ldc_route.next_index_in_site_);

  //ANPF, BNPF
  //ANPL, BNPL
  //AMPF, BMPF;
  //AMPL, BMPL;
  //CNPF, CNPL;
  //CMPF, CMPL;
  //ANT, BNT, AMT, BMT;
  //CNT, CMT

  //ANPF
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, FOLLOWER, "1.1.1.1", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_LOCAL);

  //BNPF
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 8, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, FOLLOWER, "2.2.2.2", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false,  true, FOLLOWER, "2.2.2.2", 18, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REGION);

  //ANPL
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 6, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, true, LEADER, "1.1.1.1", 16, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_LOCAL);

  //BNPL
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 7, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, true, LEADER, "2.2.2.2", 17, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REGION);

  //AMPF
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, FOLLOWER, "1.1.1.1", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_LOCAL);

  //BMPF
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 3, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, FOLLOWER, "2.2.2.2", 13, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REGION);

  //AMPL
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 1, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, true, LEADER, "1.1.1.1", 11, ROUTE_TYPE_LEADER_PARTITION_MERGE_LOCAL);

  //BMPL
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 2, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, true, LEADER, "2.2.2.2", 12, ROUTE_TYPE_LEADER_PARTITION_MERGE_REGION);

  //CNPF
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 6, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 7, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 16, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, FOLLOWER, "3.3.3.3", 17, ROUTE_TYPE_FOLLOWER_PARTITION_UNMERGE_REMOTE);

  //CNPL
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 8, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, true, LEADER, "3.3.3.3", 18, ROUTE_TYPE_LEADER_PARTITION_UNMERGE_REMOTE);

  //CMPF
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 1, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 2, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 11, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, FOLLOWER, "3.3.3.3", 12, ROUTE_TYPE_FOLLOWER_PARTITION_MERGE_REMOTE);

  //CMPL
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 3, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, true, LEADER, "3.3.3.3", 13, ROUTE_TYPE_LEADER_PARTITION_MERGE_REMOTE);

  //ANT
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, false, false, INVALID_ROLE, "1.1.1.1", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_LOCAL);

  //BNT
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, false, false, INVALID_ROLE, "2.2.2.2", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REGION);

  //AMT
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 4, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 5, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 14, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);
  test_get_next_item(test_ldc_route, SAME_IDC, true, false, INVALID_ROLE, "1.1.1.1", 15, ROUTE_TYPE_NONPARTITION_MERGE_LOCAL);

  //BMT
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 4, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 5, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 14, ROUTE_TYPE_NONPARTITION_MERGE_REGION);
  test_get_next_item(test_ldc_route, SAME_REGION, true, false, INVALID_ROLE, "2.2.2.2", 15, ROUTE_TYPE_NONPARTITION_MERGE_REGION);

  //CNT
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 9, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 10, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 19, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, false, false, INVALID_ROLE, "3.3.3.3", 20, ROUTE_TYPE_NONPARTITION_UNMERGE_REMOTE);

  //CMT
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 4, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 5, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 14, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);
  test_get_next_item(test_ldc_route, OTHER_REGION, true, false, INVALID_ROLE, "3.3.3.3", 15, ROUTE_TYPE_NONPARTITION_MERGE_REMOTE);

  EXPECT_TRUE(NULL == test_ldc_route.get_next_item());
  EXPECT_TRUE(test_ldc_route.is_reach_end());
}

}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

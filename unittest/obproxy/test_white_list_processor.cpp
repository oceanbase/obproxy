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
#include "obproxy/omt/ob_white_list_table_processor.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::omt;

class TestWhiteListProcessor : public ::testing::Test
{
public:
  void init_white_list_processor();

  ObWhiteListTableProcessor white_list_processor_;
};

void TestWhiteListProcessor::init_white_list_processor()
{
  ASSERT_EQ(OB_SUCCESS, white_list_processor_.addr_hash_map_array_[0].create(32, ObModIds::OB_HASH_BUCKET));
  ASSERT_EQ(OB_SUCCESS, white_list_processor_.addr_hash_map_array_[1].create(32, ObModIds::OB_HASH_BUCKET));
  ObString cluster_name = "cluster1";
  ObString tenant_name = "tenant1";
  ObString ip_list = "100.88.147.129/26";
  ASSERT_EQ(OB_SUCCESS, white_list_processor_.set_ip_list(cluster_name, tenant_name, ip_list));
  white_list_processor_.inc_index();
  ObString cluster_name2 = "cluster2";
  ObString tenant_name2 = "tenant2";
  ObString ip_list2 = "127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,127.0.0.1,168.1.1.1";
  ASSERT_EQ(OB_SUCCESS, white_list_processor_.set_ip_list(cluster_name2, tenant_name2, ip_list2));
  white_list_processor_.inc_index();
}

TEST_F(TestWhiteListProcessor, test_ip_net)
{
  init_white_list_processor();
  ObString cluster_name = "cluster1";
  ObString tenant_name = "tenant1";
  ObString user_name = "user1";
  char ip1[32] = "127.0.0.2";
  char ip2[32] = "127.0.0.3";
  char ip3[32] = "127.0.0.4";
  char ip4[32] = "127.0.0.5";
  ObAddr addr1(ObAddr::VER::IPV4, ip1, 0);
  ObAddr addr2(ObAddr::VER::IPV4, ip2, 0);
  ObAddr addr3(ObAddr::VER::IPV4, ip3, 0);
  ObAddr addr4(ObAddr::VER::IPV4, ip4, 0);
  struct sockaddr_storage ss = addr1.get_sockaddr();
  ASSERT_EQ(true, white_list_processor_.can_ip_pass(cluster_name, tenant_name, user_name, *reinterpret_cast<sockaddr*>(&ss)));
  ss = addr2.get_sockaddr();
  ASSERT_EQ(true, white_list_processor_.can_ip_pass(cluster_name, tenant_name, user_name, *reinterpret_cast<sockaddr*>(&ss)));
  ss = addr3.get_sockaddr();
  ASSERT_EQ(false, white_list_processor_.can_ip_pass(cluster_name, tenant_name, user_name, *reinterpret_cast<sockaddr*>(&ss)));
  ss = addr4.get_sockaddr();
  ASSERT_EQ(false, white_list_processor_.can_ip_pass(cluster_name, tenant_name, user_name, *reinterpret_cast<sockaddr*>(&ss)));
  ObString cluster_name2 = "cluster2";
  ObString tenant_name2 = "tenant2";
  ObString user_name2 = "user2";
  char ip5[32] = "127.0.0.1";
  char ip6[32] = "127.0.0.2";
  char ip7[32] = "127.0.0.3";
  ObAddr addr5(ObAddr::VER::IPV4, ip5, 0);
  ObAddr addr6(ObAddr::VER::IPV4, ip6, 0);
  ObAddr addr7(ObAddr::VER::IPV4, ip7, 0);
  ss = addr5.get_sockaddr();
  ASSERT_EQ(true, white_list_processor_.can_ip_pass(cluster_name2, tenant_name2, user_name2, *reinterpret_cast<sockaddr*>(&ss)));
  ss = addr6.get_sockaddr();
  ASSERT_EQ(false, white_list_processor_.can_ip_pass(cluster_name2, tenant_name2, user_name2, *reinterpret_cast<sockaddr*>(&ss)));
  ss = addr7.get_sockaddr();
  ASSERT_EQ(true, white_list_processor_.can_ip_pass(cluster_name2, tenant_name2, user_name2, *reinterpret_cast<sockaddr*>(&ss)));
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

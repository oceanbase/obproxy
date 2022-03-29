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
#include "obutils/ob_safe_snapshot_manager.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
TEST(test_safe_snapshot_manager, add_and_get)
{
  const static int64_t MAX_ADDR_NUM = 100;

  ObSafeSnapshotManager manager;
  ObAddr addrs[MAX_ADDR_NUM];
  int64_t last_ip = 1;
  for (int i = 0; i < MAX_ADDR_NUM; ++i) {
    ObRandomNumUtils::get_random_num(last_ip, INT32_MAX, last_ip);
    addrs[i].set_ipv4_addr(static_cast<int32_t>(last_ip), 2881); // observer use 2881 port
    manager.add(addrs[i]);
  }

  ASSERT_EQ(MAX_ADDR_NUM, manager.next_priority_);
  LOG_INFO("succ to add entry", K(manager));

  for (int i = 0; i < MAX_ADDR_NUM; ++i) {
    ASSERT_TRUE(NULL != manager.get(addrs[i]));
  }

  ObAddr not_exist_addr(ObAddr::IPV4, 0, 2881);
  ASSERT_TRUE(NULL == manager.get(not_exist_addr));
}

} // end of obutils
} // end of obproxy
} // end of oceanbase


int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

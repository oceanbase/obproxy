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
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "obproxy/obutils/ob_vip_tenant_cache.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

class TestVipTenantCache : public ::testing::Test
{
public:
  void init_vt_cache();

  ObVipTenantCache vt_cache_;
};

void TestVipTenantCache::init_vt_cache()
{
  ObVipTenant *vt1 = op_alloc(ObVipTenant);
  ObVipTenant *vt2 = op_alloc(ObVipTenant);

  if (NULL != vt1 && NULL != vt2) {
    vt1->vip_addr_.addr_.set_ipv4_addr("1.1.1.1", 111);
    vt1->vip_addr_.vid_ = 1;
    ObString tname1= ObString::make_string("tname1");
    ObString cname1 = ObString::make_string("cname1");
    vt2->vip_addr_.addr_.set_ipv4_addr("2.2.2.2", 222);
    vt2->vip_addr_.vid_ = 2;
    ObString tname2 = ObString::make_string("tname2");
    ObString cname2 = ObString::make_string("cname2");

    vt1->set_tenant_cluster(tname1, cname1);
    vt2->set_tenant_cluster(tname2, cname2);
    vt_cache_.set(*vt1);
    vt_cache_.set(*vt2);
  }
}

TEST_F(TestVipTenantCache, test_get)
{
  int ret = OB_SUCCESS;
  ObVipAddr addr;
  ObVipTenant vt;
  addr.addr_.set_ipv4_addr("1.1.1.1", 111);
  addr.vid_ = 1;
  ret = vt_cache_.get(addr, vt);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(vt.is_valid(), false);

  init_vt_cache();
  ret = vt_cache_.get(addr, vt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, vt.is_valid());

  ret = vt_cache_.update_cache_map();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = vt_cache_.get(addr, vt);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ASSERT_EQ(0, vt_cache_.get_vt_cache_count());
  vt_cache_.destroy();
  ASSERT_EQ(-1, vt_cache_.get_vt_cache_count());
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

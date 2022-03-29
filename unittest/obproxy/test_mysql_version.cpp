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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "common/ob_version.h"

namespace oceanbase
{
namespace obproxy
{
using namespace common;

class TestMysqlVersion : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
};

void TestMysqlVersion::SetUp()
{
}

void TestMysqlVersion::TearDown()
{
}

TEST_F(TestMysqlVersion, check_version_valid)
{
  const char *version_name_invalid[]=
  {
    ".5.6",
    "5.6."
    "5..6.25"
    "5.6.x",
    "x",
    "xyz",
    "x.y.z",
    ""
  };

  for (int i = 0; i < sizeof(version_name_invalid) / sizeof(version_name_invalid[0]); i++)
  {
    ASSERT_FALSE(check_version_valid(strlen(version_name_invalid[i]), version_name_invalid[i]));
  }

  const char *version_name_valid[]=
  {
    "5",
    "5625"
    "5.6"
    "5.6.25",
  };

  for (int i = 0; i < sizeof(version_name_valid) / sizeof(version_name_valid[0]); i++)
  {
    ASSERT_TRUE(check_version_valid(strlen(version_name_valid[i]), version_name_invalid[i]));
  }
}

}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

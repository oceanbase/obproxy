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
#define private public
#define protected public
#include <gtest/gtest.h>
#include <map>
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "obproxy/utils/ob_proxy_blowfish.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const static int64_t DBKEY_COUNT = 1;

class TestBlowFish : public ::testing::Test
{
public:
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

static const char enc_pwd[DBKEY_COUNT][128] = {
"38dac89a4a21522a"
};

static const char plain_pwd[DBKEY_COUNT][128] = {
"123"
};

TEST_F(TestBlowFish, decode)
{
  int ret = OB_SUCCESS;
  char dec_pwd[128];
  for (int64_t i = 0; OB_SUCC(ret) && i < DBKEY_COUNT; ++i) {
    memset(dec_pwd, 0, 128);
    if (OB_FAIL(ObBlowFish::decode(enc_pwd[i], strlen(enc_pwd[i]), dec_pwd, 128))) {
      LOG_WARN("fail to decode password", "pwd", enc_pwd[i], K(ret));
      ASSERT_EQ(1, 0);
    } else {
      LOG_INFO("succ to decode pwd", "pwd", enc_pwd[i], "dec_pwd", dec_pwd);
      ASSERT_EQ(0, memcmp(plain_pwd[i], dec_pwd, strlen(plain_pwd[i])));
    }
  }
}


}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

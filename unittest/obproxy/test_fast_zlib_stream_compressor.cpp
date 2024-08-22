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
#include "lib/ob_define.h"
#include "obproxy/utils/ob_fast_zlib_stream_compressor.h"
#include "lib/compress/zlib/ob_zlib_compressor.h"
#include "lib/oblog/ob_log.h"
#include "ob_mysql_test_utils.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;

class TestFastZlibStreamCompressor : public ::testing::Test
{
public:

};

TEST_F(TestFastZlibStreamCompressor, test_simple)
{
  ObFastZlibStreamCompressor compressor1;
  int ret = OB_SUCCESS;
  int64_t buff_len = 1024 * 1024;
  char *src_buf = new char[buff_len];
  char *second_src_buf = new char[buff_len];
  char *compress_buf = new char[buff_len];
  int64_t src_len = 0;
  int64_t filled_len = 0;
  int64_t filled_len2 = 0;

  for (int64_t i = 1; i < UINT16_MAX + 1; i++) {
    src_len = i;
    ObMysqlTestUtils::get_random_str(src_buf, src_len);
    ret = compressor1.add_compress_data(src_buf, src_len);
    if (src_len >= UINT16_MAX) {
      ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
      continue;
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    int j = 0;
    int64_t total_filled_len = 0;
    while (true) {
      filled_len = 0;
      // compress one byte by one byte
      ret = compressor1.compress(compress_buf + j, 1, filled_len);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_LE(filled_len, 1);
      total_filled_len += filled_len;
      if (filled_len < 1) {
        break;
      }
      j++;
    }
    ASSERT_LT(total_filled_len, buff_len);

    ObZlibCompressor compressor2;
    ret = compressor2.decompress(compress_buf, total_filled_len, second_src_buf, buff_len, filled_len2);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(filled_len2, src_len);
    ASSERT_TRUE(0 == memcmp(src_buf, second_src_buf, (uint32_t)(src_len)));

    LOG_INFO("one shot", K(src_len), K(total_filled_len), K(filled_len2));
  }

  delete []src_buf;
  src_buf = NULL;
  delete []second_src_buf;
  second_src_buf = NULL;
  delete []compress_buf;
  compress_buf = NULL;
};

TEST_F(TestFastZlibStreamCompressor, test_speed)
{
  ObFastZlibStreamCompressor compressor1;
  ObZlibCompressor compressor2(0);
  ObZlibCompressor compressor3(6);
  int ret = OB_SUCCESS;
  int64_t run_count = 1000;
  int64_t buff_len = 8 * 1024 * 1024;
  char *src_buf = new char[buff_len];
  char *second_src_buf = new char[buff_len];
  char *compress_buf = new char[buff_len];
  int64_t src_len = 0;
  int64_t filled_len = 0;
  int64_t filled_len2 = 0;

  int64_t start_time_us = 0;
  int64_t cost_time_us = 0;
  int64_t cost_time_us2 = 0;
  int64_t cost_time_us3 = 0;
  int64_t cost_time_us4 = 0;

  for (int64_t i = 128; i <= 32*1024 ; i = i * 2) {
    src_len = i;
    ObMysqlTestUtils::get_random_str(src_buf, src_len);
    // 1. fast compress
    start_time_us = ObMysqlTestUtils::get_us();
    for (int64_t j = 0; j < run_count; j++) {
      ret = compressor1.add_compress_data(src_buf, src_len);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = compressor1.compress(compress_buf, buff_len, filled_len);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    cost_time_us = ObMysqlTestUtils::get_us() - start_time_us;

    // 2. standard level 0 decompress
    start_time_us = ObMysqlTestUtils::get_us();
    for (int64_t j = 0; j < run_count; j++) {
      ret = compressor2.decompress(compress_buf, filled_len, second_src_buf, buff_len, filled_len2);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    cost_time_us2 = ObMysqlTestUtils::get_us() - start_time_us;

    // 3. standard level 0 compress
    start_time_us = ObMysqlTestUtils::get_us();
    for (int64_t j = 0; j < run_count; j++) {
      ret = compressor2.compress(src_buf, src_len, compress_buf, buff_len, filled_len2);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    cost_time_us3 = ObMysqlTestUtils::get_us() - start_time_us;

    // 4. standard level 6 compress
    start_time_us = ObMysqlTestUtils::get_us();
    for (int64_t j = 0; j < run_count; j++) {
      ret = compressor3.compress(src_buf, src_len, compress_buf, buff_len, filled_len2);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    cost_time_us4 = ObMysqlTestUtils::get_us() - start_time_us;
    LOG_INFO("one shot", "string len", src_len, "fast compress cost", cost_time_us,
             "standard level0 decompress cost", cost_time_us2,
             "standard level0 compress cost", cost_time_us3,
             "standard level6 compress cost", cost_time_us4);
  }

  delete []src_buf;
  src_buf = NULL;
  delete []second_src_buf;
  second_src_buf = NULL;
  delete []compress_buf;
  compress_buf = NULL;
};


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

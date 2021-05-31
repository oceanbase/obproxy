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
#include "obproxy/utils/ob_zlib_stream_compressor.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;

class TestZlibStreamCompressor : public ::testing::Test
{
public:
  const static char *get_test_text() {
    return "deflate() has a return value that can indicate errors, yet we do not check it here. "
      "Why not? Well, it turns out that deflate() can do no wrong here. Let's go through def"
      "late()'s return values and dispense with them one by one. The possible values are Z_OK, "
      "Z_STREAM_END, Z_STREAM_ERROR, or Z_BUF_ERROR. Z_OK is, well, ok. Z_STREAM_END is also ok "
      "and will be returned for the last call of deflate(). This is already guaranteed by calling "
      "deflate() with Z_FINISH until it has no more output. Z_STREAM_ERROR is only possible if the "
      "stream is not initialized properly, but we did initialize it properly. There is no harm in "
      "checking for Z_STREAM_ERROR here, for example to check for the possibility that some other "
      "part of the application inadvertently clobbered the memory containing the zlib state. "
      "Z_BUF_ERROR will be explained further below, but suffice it to say that this is simply an "
      "indication that deflate() could not consume more input or produce more output. deflate() "
      "can be called again with more output space or more available input, which it will be in "
      "this code.We often get questions about how the deflate() and inflate() functions should be"
      " used. Users wonder when they should provide more input, when they should use more output,"
      " what to do with a Z_BUF_ERROR, how to make sure the process terminates properly, and so "
      "on. So for those who have read zlib.h (a few times), and would like further edification, "
      "below is an annotated example in C of simple routines to compress and decompress from an "
      "input file to an output file using deflate() and inflate() respectively. The annotations "
      "are interspersed between lines of the code. So please read between the lines. We hope this "
      "helps explain some of the intricacies of zlib.";
  }
};

TEST_F(TestZlibStreamCompressor, test_simple)
{
  ObZlibStreamCompressor compressor1;
  int ret = OB_SUCCESS;
  const char *text = TestZlibStreamCompressor::get_test_text();
  int64_t text_len = strlen(text);
  int64_t len = text_len * 2;
  bool is_last_data = true;

  ret = compressor1.add_compress_data(text, text_len, is_last_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *buf = new char[len];
  int64_t filled_len = -1;
  ret = compressor1.compress(buf, len, filled_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(filled_len > 0);
  ASSERT_TRUE(len > filled_len);

  ObZlibStreamCompressor compressor2;
  ret = compressor2.add_decompress_data(buf, filled_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  char *buf2 = new char[len];
  int64_t filled_len2 = -1;
  ret = compressor2.decompress(buf2, len, filled_len2);
  ASSERT_TRUE(len > filled_len2);
  buf2[filled_len2] = '\0';
  ASSERT_STREQ(buf2, text);

  delete []buf;
  buf = NULL;
  delete []buf2;
  buf2 = NULL;
};

TEST_F(TestZlibStreamCompressor, test_compress_and_decomress_stream)
{
  int ret = OB_SUCCESS;
  const char *text = TestZlibStreamCompressor::get_test_text();
  int64_t text_len = strlen(text);
  int64_t len = text_len * 2;

  int64_t total_filled_len = 0;
  char *buf = new char[len];
  // 1. stream compress
  ObZlibStreamCompressor compressor1;
  for (int64_t i = 0; i < text_len; ++i) {
    // add data one by one
    ret = compressor1.add_compress_data(text + i, 1, (i == text_len - 1));
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t filled_len = -1;
    while (true) {
      int64_t buf_len = len - total_filled_len;
      char *buf_start = buf + total_filled_len;
      ret = compressor1.compress(buf_start, buf_len, filled_len);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(filled_len >= 0);
      total_filled_len += filled_len;
      if (filled_len < buf_len) {
        break;
      }
    }
  }
  int64_t compressed_len = total_filled_len;
  LOG_INFO("compress complete", K(compressed_len), "origin len", text_len);

  // 2.stream decompress
  int64_t len2 = len;
  char *buf2 = new char[len2];
  total_filled_len = 0;
  ObZlibStreamCompressor compressor2;
  for (int64_t i = 0; i < compressed_len; ++i) {
    // add data one by one
    ret = compressor2.add_decompress_data(buf + i, 1);
    ASSERT_EQ(OB_SUCCESS, ret);
    while (true) {
      int64_t filled_len = -1;
      int64_t buf_len = len2 - total_filled_len;
      char *buf_start = buf2 + total_filled_len;
      ret = compressor2.decompress(buf_start, buf_len, filled_len);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_TRUE(filled_len >= 0);
      total_filled_len += filled_len;
      if (filled_len < buf_len) {
        break;
      }
    }
  }
  LOG_INFO("decompress complete", "origin len", total_filled_len);
  buf2[total_filled_len] = '\0';
  ASSERT_STREQ(buf2, text);

  delete []buf;
  buf = NULL;
  delete []buf2;
  buf2 = NULL;
}

TEST_F(TestZlibStreamCompressor, test_compress_and_decomress_stream2)
{
  int ret = OB_SUCCESS;
  const char *text = TestZlibStreamCompressor::get_test_text();
  int64_t text_len = strlen(text);
  int64_t len = text_len * 2;

  int64_t total_filled_len = 0;
  char *buf = new char[len];
  ObZlibStreamCompressor compressor1;
  ret = compressor1.add_compress_data(text, text_len, true); // add all data at once
  ASSERT_EQ(OB_SUCCESS, ret);
  char tmp_char ='\0'; // get compressed data one by one
  while (true) {
    int64_t filled_len = -1;
    ret = compressor1.compress(&tmp_char, 1, filled_len);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (filled_len < 1) {
      break;
    } else {
      buf[total_filled_len] = tmp_char;
      ++total_filled_len;
    }
  }
  int64_t compressed_len = total_filled_len;
  LOG_INFO("compress complete", K(compressed_len), "origin len", text_len);

  int64_t len2 = len;
  char *buf2 = new char[len2];
  total_filled_len = 0;
  ObZlibStreamCompressor compressor2;
  ret = compressor2.add_decompress_data(buf, compressed_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  tmp_char = '\0';
  while (true) {
    int64_t filled_len = -1;
    ret = compressor2.decompress(&tmp_char, 1, filled_len);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (filled_len < 1) {
      break;
    } else {
      buf2[total_filled_len] = tmp_char;
      ++total_filled_len;
    }
  }

  buf2[total_filled_len] = '\0';
  ASSERT_STREQ(buf2, text);
  LOG_INFO("decompress complete", "origin len", total_filled_len);

  delete []buf;
  buf = NULL;
  delete []buf2;
  buf2 = NULL;
}

TEST_F(TestZlibStreamCompressor, test_unexpected)
{
  ObZlibStreamCompressor compressor;
  int ret = OB_SUCCESS;
  const char *data = "Action speak louder than words";
  int64_t len = strlen(data);
  int64_t filled_len = 0;
  ret = compressor.add_compress_data(data, len, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  char tmp_buf[100];
  // miss match
  ret = compressor.decompress(tmp_buf, 100, filled_len );
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ret = compressor.add_decompress_data(data, len);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ret = compressor.reset();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compressor.reset();
  ASSERT_EQ(OB_SUCCESS, ret);

  // not add decompressed data, just decompress
  ret = compressor.decompress(tmp_buf, 100, filled_len);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  // not add compressed data, just compress
  ret = compressor.compress(tmp_buf, 100, filled_len);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  ret = compressor.reset();
  ASSERT_EQ(OB_SUCCESS, ret);

  // normal
  ret = compressor.add_compress_data(data, len, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compressor.compress(tmp_buf, 100, filled_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(100 > filled_len);

  compressor.reset();
  char tmp_buf2[100];
  int64_t filled_len2 = 0;
  ret = compressor.add_decompress_data(tmp_buf, filled_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = compressor.decompress(tmp_buf2, 100, filled_len2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(100 > filled_len2);

  tmp_buf2[filled_len2] = '\0';
  ASSERT_STREQ(tmp_buf2, data);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

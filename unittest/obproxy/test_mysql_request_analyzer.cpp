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
#include <malloc.h>
#include <stdlib.h>
#include <gtest/gtest.h>
#define private public
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "obproxy/proxy/mysqllib/ob_mysql_request_analyzer.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

class TestEvent : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();

  void gen_mysql_packet(char *&data, const int64_t packet_size);

public:
  ObMysqlRequestAnalyzer *request_analyzer_;
};


void TestEvent::SetUp()
{
  request_analyzer_ = new ObMysqlRequestAnalyzer();
}

void TestEvent::TearDown()
{
  if (NULL != request_analyzer_) {
    delete request_analyzer_;
    request_analyzer_ = NULL;
  }
}

void TestEvent::gen_mysql_packet(char *&data, const int64_t packet_size)
{
  data = (char *) malloc(packet_size + MYSQL_NET_HEADER_LENGTH);
  ob_int3store(data, packet_size);
  int64_t size = 0;
  size = ob_uint3korr(data);
  LOG_INFO("gen mysql packet", K(size));
}

TEST_F(TestEvent, test_analyze_complete_mysql_request)
{
  char *packet_less_16m = NULL;
  int32_t packet_less_16m_size = (1<<20) * 16 - 2;

  char *packet_16m = NULL;
  int32_t packet_16m_size = (1<<20) * 16 - 1;

  char *packet_2m = NULL;
  int32_t packet_2m_size = (1<<20) * 2;

  char *packet_0b = NULL;
  int32_t packet_0b_size = 0;

  gen_mysql_packet(packet_less_16m, packet_less_16m_size);
  gen_mysql_packet(packet_16m, packet_16m_size);
  gen_mysql_packet(packet_2m, packet_2m_size);
  gen_mysql_packet(packet_0b, packet_0b_size);

  bool is_finish = false;
  ObRequestBuffer buffer;

  // case 0
  LOG_INFO("analyze less 16m");
  buffer.assign_ptr(packet_less_16m, packet_less_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  // case 1
  LOG_INFO("analyze 2m");
  buffer.assign_ptr(packet_2m, packet_2m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  // case 2
  LOG_INFO("analyze 16m + 0b");
  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_0b, packet_0b_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  // case 3
  LOG_INFO("analyze 16m + 2m");
  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_2m, packet_2m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  // case 4
  LOG_INFO("analyze 16m + 16m + 0b");
  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_0b, packet_0b_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  // case 5
  LOG_INFO("analyze 16m + 16m + 2m");
  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_16m, packet_16m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(packet_2m, packet_2m_size + MYSQL_NET_HEADER_LENGTH);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  request_analyzer_->reuse();
  buffer.reset();

  free(packet_16m);
  free(packet_2m);
  free(packet_0b);
}

TEST_F(TestEvent, test_analyze_10b_mysql_request_by_1_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = 10;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(1);

  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();


  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_10b_mysql_request_by_2_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = 10;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(2);

  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < 4; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_0b_mysql_request_by_1_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(1);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();


  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 0b
  packet_len = 0;

  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);


  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_0b_mysql_request_by_2_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(2);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (((1<<20) * 8) - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  buffer.reset();
  buffer.assign_ptr(tmp_buf, 1);

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);


  // 0b
  packet_len = 0;
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  buffer.reset();

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_2m_mysql_request_by_1_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(1);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();


  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 2m
  packet_len = (1<<20) * 2;

  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i=0; i<packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  buffer.reset();

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_2m_mysql_request_by_2_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(2);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (((1<<20) * 8) - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  buffer.reset();
  buffer.assign_ptr(tmp_buf, 1);

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 2m
  packet_len = (1<<20) * 2;
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (packet_len/2 - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_16m_2m_mysql_request_by_1_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(1);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();


  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 16m
  packet_len = (1<<20) * 16 - 1;
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();


  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 2m
  packet_len = (1<<20) * 2;

  *((uchar *)tmp_buf) = (uchar) (packet_len);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 1);
  for (int32_t i = 0; i < packet_len; i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 1);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);
  buffer.reset();

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_16m_2m_mysql_request_by_2_byte)
{
  char *tmp_buf = NULL;
  int32_t packet_len = (1<<20) * 16 - 1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *)malloc(2);

  // 16m
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (((1<<20) * 8) - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  buffer.reset();
  buffer.assign_ptr(tmp_buf, 1);

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 16m
  packet_len = (1<<20) * 16 - 1;
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (((1<<20) * 8) - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  buffer.reset();
  buffer.assign_ptr(tmp_buf, 1);

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);

  // 2m
  packet_len = (1<<20) * 2;
  *((uchar *)tmp_buf) = (uchar) (packet_len);
  *(((uchar *)tmp_buf) + 1) = (uchar) (packet_len >> 8);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  *((uchar *)tmp_buf) = (uchar) (packet_len >> 16);
  buffer.assign_ptr(tmp_buf, 2);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_FALSE(is_finish);
  buffer.reset();

  buffer.assign_ptr(tmp_buf, 2);
  for (int32_t i = 0; i < (packet_len/2 - 1); i++) {
    request_analyzer_->is_request_finished(buffer, is_finish);
    ASSERT_FALSE(is_finish);
  }

  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);

  request_analyzer_->reuse();
  buffer.reset();

  free(tmp_buf);
}

TEST_F(TestEvent, test_analyze_16m_0b_in_one_buffer)
{
  char *tmp_buf = NULL;
  int32_t packet_len_16m = (1<<20) * 16 - 1;
  int32_t packet_len_0b = 0;

  int64_t size = -1;

  bool is_finish = false;
  ObRequestBuffer buffer;

  tmp_buf = (char *) malloc(MYSQL_NET_HEADER_LENGTH + packet_len_16m + MYSQL_NET_HEADER_LENGTH + packet_len_0b);
  ob_int3store(tmp_buf, packet_len_16m);
  size = ob_uint3korr(tmp_buf);
  LOG_INFO("gen mysql packet", K(size));

  ob_int3store(tmp_buf + (MYSQL_NET_HEADER_LENGTH + packet_len_16m), packet_len_0b);
  size = ob_uint3korr(tmp_buf + (MYSQL_NET_HEADER_LENGTH + packet_len_16m));
  LOG_INFO("gen mysql packet", K(size));

  buffer.assign_ptr(tmp_buf, MYSQL_NET_HEADER_LENGTH + packet_len_16m + MYSQL_NET_HEADER_LENGTH + packet_len_0b);
  request_analyzer_->is_request_finished(buffer, is_finish);
  ASSERT_TRUE(is_finish);

  buffer.reset();
  free(tmp_buf);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

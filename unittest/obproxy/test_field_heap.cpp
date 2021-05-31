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
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/number/ob_number_v2.h"
#include "obproxy/proxy/mysqllib/ob_field_heap.h"
#include "obproxy/proxy/mysqllib/ob_session_field_mgr.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

TEST(TestFieldHeader, test_init)
{
  ObFieldHeader header;
  ASSERT_TRUE(HEAP_OBJ_EMPTY == header.type_);
  ASSERT_EQ(0U, header.length_);
  header.init(HEAP_OBJ_STR_BLOCK, 129);
  ASSERT_TRUE(HEAP_OBJ_STR_BLOCK == header.type_);
  ASSERT_EQ(129U, header.length_);
  char a[100];
  header.to_string(a, 100);
  LOG_INFO("field_header", K(header));
};

TEST(TestStrHeapDesc, test_reset)
{
  StrHeapDesc heap_desc;
  heap_desc.reset();
};

TEST(TestFieldHeap, test_new_field_heap)
{
  ObFieldHeap *heap = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObFieldHeapUtils::new_field_heap(ObFieldHeap::get_hdr_size(), heap));
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE + 1, heap));
  heap->destroy();
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  heap->destroy();
};

TEST(TestFieldHeap, test_new_field_str_heap)
{
  ObFieldHeap *heap = NULL;
  char *buf = NULL;
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  ASSERT_EQ(OB_INVALID_ARGUMENT, heap->allocate_str(0, buf));
  ASSERT_TRUE(OB_SUCCESS == (heap->allocate_str(ObFieldHeapUtils::STR_HEAP_DEFAULT_SIZE + 1, buf)));
  heap->destroy();
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  ASSERT_TRUE(OB_SUCCESS == (heap->allocate_str(ObFieldHeapUtils::STR_HEAP_DEFAULT_SIZE + 1, buf)));
};

TEST(TestFieldHeap, test_alloc_and_dealloc_obj)
{
  ObFieldHeap heap_tmp(10);
  ObFieldHeader *new_block = NULL;
  void *obj = NULL;
  int64_t size = 2048;
  ASSERT_EQ(OB_NOT_INIT, heap_tmp.allocate_block(size, obj));
  ASSERT_EQ(OB_NOT_INIT, heap_tmp.deallocate_block(new_block));
  ObFieldHeap *heap = NULL;
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  ASSERT_EQ(OB_INVALID_ARGUMENT, heap->allocate_block(size, obj));
  ASSERT_EQ(OB_INVALID_ARGUMENT, heap->deallocate_block(new_block));
  size = 1024;
  ASSERT_EQ(OB_SUCCESS, heap->allocate_block(size, obj));
  ASSERT_TRUE(NULL != obj);
  ASSERT_EQ(OB_SUCCESS, heap->allocate_block(size, obj));
  ASSERT_TRUE(NULL != obj);
  ASSERT_EQ(OB_SUCCESS, heap->allocate_block(size, obj));

  ASSERT_TRUE(NULL != (new_block = new(obj) ObFieldHeader()));
  ASSERT_EQ(OB_SUCCESS, heap->deallocate_block(new_block));
};

TEST(TestFieldHeap, test_duplicate_str)
{
  const char *in_str = NULL;
  uint16_t in_len = 4;
  uint16_t out_len = 0;
  ObFieldHeap *heap = NULL;
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  const char *out_str = NULL;
  ASSERT_EQ(OB_INVALID_ARGUMENT, heap->duplicate_str(in_str, in_len, out_str, out_len));
  in_str = "yyyy";
  ASSERT_EQ(OB_SUCCESS, heap->duplicate_str(in_str, in_len, out_str, out_len));
  ObString in_string(in_len, in_str);
  ObString out_string(out_len, out_str);
  ASSERT_TRUE(in_string == out_string);
  heap->free_string(out_str, out_len);
  ASSERT_EQ(heap->dirty_string_space_, 4);
}

TEST(TestFieldHeap, test_duplicate_obj)
{
  //duplicate_obj basic type, varchar type, numberic type
  ObFieldHeap *heap = NULL;
  ASSERT_EQ(OB_SUCCESS, ObFieldHeapUtils::new_field_heap(ObFieldHeap::FIELD_HEAP_DEFAULT_SIZE, heap));
  ObObj out_obj_int;
  ObObj int_obj;
  int_obj.set_int(1024);
  ASSERT_EQ(OB_SUCCESS, heap->duplicate_obj(int_obj, out_obj_int));
  ASSERT_EQ(int_obj, out_obj_int);
  heap->free_obj(out_obj_int);

  ObObj varchar_obj;
  ObObj out_obj_varchar;
  varchar_obj.set_varchar("yyy");
  varchar_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  LOG_INFO("varchar obj:", K(varchar_obj));
  ASSERT_EQ(OB_SUCCESS, heap->duplicate_obj(varchar_obj, out_obj_varchar));
  ASSERT_EQ(varchar_obj, out_obj_varchar);
  heap->free_obj(out_obj_varchar);
  ASSERT_EQ(heap->dirty_string_space_, 3);

  number::ObNumber number;
  char buf[1024];
  ModuleArena arena;
  sprintf(buf, "100");
  int ret = number.from(buf, arena);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("number", K(number));

  ObObj number_obj;
  ObObj out_obj_number;
  number_obj.set_number(number);
  ASSERT_EQ(number_obj.get_deep_copy_size(), 4);
  ASSERT_EQ(OB_SUCCESS, heap->duplicate_obj(number_obj, out_obj_number));
  ASSERT_EQ(number_obj, out_obj_number);
  heap->free_obj(out_obj_number);
  ASSERT_EQ(heap->dirty_string_space_, 7);
};

TEST(TestFieldHeap, test_reform_mem)
{
  ObSessionFieldMgr session_mgr;
  ASSERT_EQ(OB_SUCCESS, session_mgr.init());
  ObObj value;
  static const int64_t BUF_SIZE = 1025;
  char buf[BUF_SIZE];
  for (int64_t i = 0; i < BUF_SIZE; ++i) {
    buf[i] = 'y';
  }
  buf[BUF_SIZE - 1] = '\0';
  value.set_varchar(buf);
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  LOG_INFO("replace 1");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("1"), value));
  LOG_INFO("remove 1");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.remove_user_variable(ObString::make_string("1")));
  LOG_INFO("NEXT REFORM");
  LOG_INFO("replace 2");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("2"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 3");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("3"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 4");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("4"), value));
  LOG_INFO("NEXT DEMOTE");
  LOG_INFO("replace 5");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("5"), value));
  LOG_INFO("NEXT DEMOTE AND REFROM");
  LOG_INFO("replace 6");
  ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("6"), value));
  //LOG_INFO("replace 7");
  ////here will demote and reform
  //ASSERT_TRUE(OB_SUCCESS == session_mgr.replace_user_variable(ObString::make_string("7"), value));
}
}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace 
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


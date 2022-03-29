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

#define USING_LOG_PREFIX PROXY_EVENT

#include <gtest/gtest.h>
#include <pthread.h>
#define private public
#define protected public
#include "test_eventsystem_api.h"
#include "ob_io_buffer.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

#define MAX_LOCATION_STRING_LENGTH  120
#define MAX_BUFFER_STRING_SIZE      150

char g_input_buf[] = "OceanBase was originally designed to solve the problem"
                   " of large-scale data of Taobao in ALIBABA GROUP.";
int64_t g_size = sizeof(g_input_buf);

class TestIOBuffer : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void check_data_alloc(ObIOBufferData *data_ptr, int64_t size);
  void check_block_clear(ObIOBufferBlock *block);
  void check_block_reset(ObIOBufferBlock *block);
  void check_buffer_clear(ObMIOBuffer *buffer);
  void init_miobuffer(int64_t size, bool empty_miobuffer = false);
  void init_miobuffer_alloc_readers(int64_t reader_num, bool write_not_init = false);
  void check_multi_reader_read(int64_t reader_num);
  void common_two_buffer_init();
  void check_two_buffer_memcpy(int64_t reader_num);
  void check_two_buffer_remove_append(int64_t reader_num);
  void check_two_buffer_write_and_transfer(int64_t reader_num);
  void check_accessor_and_new_buffer(ObMIOBufferAccessor *mio_accessor);

public:
  ObIOBufferData *data_ptr_;
  ObIOBufferBlock *block_ptr_;
  ObMIOBuffer *buffer_ptr_;
  ObMIOBuffer *des_buffer_ptr_;
};

void TestIOBuffer::SetUp()
{
  data_ptr_ = NULL;
  block_ptr_ = NULL;
  buffer_ptr_ = NULL;
  des_buffer_ptr_ = NULL;
}

void TestIOBuffer::TearDown()
{
  if (NULL != data_ptr_) {
    data_ptr_->free();
    block_ptr_ = NULL;
  } else {}

  if (NULL != block_ptr_) {
    block_ptr_->free();
    block_ptr_ = NULL;
  } else {}

  if (NULL != buffer_ptr_) {
    free_miobuffer(buffer_ptr_);
    buffer_ptr_ = NULL;
  } else {}

  if (NULL != des_buffer_ptr_) {
    des_buffer_ptr_->destroy();
    delete des_buffer_ptr_;
    des_buffer_ptr_ = NULL;
  } else {}
}

void TestIOBuffer::check_data_alloc(ObIOBufferData *data_ptr, int64_t size)
{
  ASSERT_TRUE(NULL != data_ptr);
  ASSERT_EQ(size, data_ptr->size_);
  ASSERT_EQ(DEFAULT_ALLOC, data_ptr->mem_type_);
  ASSERT_TRUE(NULL != data_ptr->data_);
}

void TestIOBuffer::check_block_clear(ObIOBufferBlock *block)
{
  ASSERT_TRUE(NULL == block->start_);
  ASSERT_TRUE(NULL == block->end_);
  ASSERT_TRUE(NULL == block->buf_end_);
}

void TestIOBuffer::check_block_reset(ObIOBufferBlock *block)
{
  ASSERT_TRUE(block->start_ == block->data_->data_);
  ASSERT_TRUE(block->end_ == block->start_);
  ASSERT_TRUE(block->buf_end_ == (block->start_ + block->data_->size_));
}

void TestIOBuffer::check_buffer_clear(ObMIOBuffer *buffer)
{
  ASSERT_EQ(0, buffer->size_);
  ASSERT_TRUE(NULL == buffer->writer_);
  ASSERT_EQ(0, buffer->water_mark_);
  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; i++) {
    ASSERT_TRUE(NULL == buffer->readers_[i].accessor_);
    ASSERT_TRUE(NULL == buffer->readers_[i].block_);
    ASSERT_TRUE(NULL == buffer->readers_[i].mbuf_);
    ASSERT_TRUE(0 == buffer->readers_[i].start_offset_);
  }
}

void TestIOBuffer::init_miobuffer(int64_t size, bool empty_miobuffer)
{
#ifdef TRACK_BUFFER_USER
  char location[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/");
#endif

  if (!empty_miobuffer) {
#ifdef TRACK_BUFFER_USER
    buffer_ptr_ = new_miobuffer_internal(location, size);
    ASSERT_EQ(0, strcmp(buffer_ptr_->location_, location));
    ASSERT_EQ(0, strcmp(buffer_ptr_->writer_->location_, location));
    ASSERT_EQ(0, strcmp(buffer_ptr_->writer_->data_->location_, location));
#else
    buffer_ptr_ = new_miobuffer_internal(size);
#endif
    ASSERT_FALSE(buffer_ptr_->empty());
  } else {
#ifdef TRACK_BUFFER_USER
    buffer_ptr_ = new_empty_miobuffer_internal(location, size);
    ASSERT_EQ(0, strcmp(buffer_ptr_->location_, location));
#else
    buffer_ptr_ = new_empty_miobuffer_internal(size);
#endif
    ASSERT_TRUE(buffer_ptr_->empty());
  }
  ASSERT_EQ(size, buffer_ptr_->size_);
  ASSERT_EQ(size, buffer_ptr_->get_block_size());
}

void TestIOBuffer::init_miobuffer_alloc_readers(int64_t reader_num, bool write_not_init)
{
  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; i++) {
    ASSERT_FALSE(buffer_ptr_->readers_[i].is_allocated());
  }
  for (int64_t i = 0; i < reader_num && reader_num <= MAX_MIOBUFFER_READERS; i++) {
    buffer_ptr_->alloc_reader();
    ASSERT_TRUE(buffer_ptr_->readers_[i].is_allocated());
    ASSERT_TRUE(NULL == buffer_ptr_->readers_[i].accessor_);
    ASSERT_TRUE(0 == buffer_ptr_->readers_[i].start_offset_);
    ASSERT_TRUE(buffer_ptr_->readers_[i].get_current_block() == buffer_ptr_->get_current_block());
    if (!write_not_init) {
      ASSERT_TRUE(buffer_ptr_->writer_->start() == buffer_ptr_->readers_[i].start());
      ASSERT_TRUE(buffer_ptr_->writer_->end() == buffer_ptr_->readers_[i].end());
    } else {}
  }
}

void TestIOBuffer::common_two_buffer_init()
{
  des_buffer_ptr_ = new(std::nothrow) ObMIOBuffer(MAX_BUFFER_STRING_SIZE);
  ASSERT_TRUE(NULL != des_buffer_ptr_);
  ASSERT_EQ(MAX_BUFFER_STRING_SIZE, des_buffer_ptr_->size_);
#ifdef TRACK_BUFFER_USER
  ASSERT_TRUE(NULL == des_buffer_ptr_->location_);
#endif

  init_miobuffer(g_size / MAX_MIOBUFFER_READERS, true);
  init_miobuffer_alloc_readers(MAX_MIOBUFFER_READERS, true);
  int64_t written_len = 0;
  buffer_ptr_->write(g_input_buf, g_size, written_len);//need add_block
}

void TestIOBuffer::check_two_buffer_memcpy(int64_t reader_num)
{
  char output_buf[MAX_BUFFER_STRING_SIZE] = {};
  int64_t base_offset = g_size / reader_num;
  int64_t last_max_block_count = 0;
  int64_t written_len = 0;

  for (int64_t i = 0; i < reader_num - 1; ++i) {
    buffer_ptr_->readers_[i].copy(output_buf, base_offset, i * base_offset);
    ASSERT_EQ(0, memcmp(g_input_buf + i * base_offset, output_buf, base_offset));
    ASSERT_EQ(g_size, buffer_ptr_->readers_[i].read_avail());
    ASSERT_TRUE(buffer_ptr_->readers_[i].is_read_avail_more_than(g_size - 1));
    ASSERT_FALSE(buffer_ptr_->readers_[i].is_read_avail_more_than(g_size));

    last_max_block_count = des_buffer_ptr_->max_block_count();
    des_buffer_ptr_->write(&(buffer_ptr_->readers_[i]), base_offset, written_len, i * base_offset);
    ASSERT_TRUE(des_buffer_ptr_->writer_->buf_end() == des_buffer_ptr_->writer_->end());
    des_buffer_ptr_->alloc_reader();
    ASSERT_EQ(last_max_block_count + 1, des_buffer_ptr_->max_block_count());
    memset(output_buf, 0, MAX_BUFFER_STRING_SIZE);
  }

  buffer_ptr_->readers_[0].copy(output_buf, base_offset - 1,
      (reader_num - 1) * base_offset);
  ASSERT_EQ(0, memcmp(g_input_buf + (reader_num - 1) * base_offset, output_buf,
      base_offset - 1));

  last_max_block_count = des_buffer_ptr_->max_block_count();
  des_buffer_ptr_->write(&(buffer_ptr_->readers_[0]), base_offset - 1, written_len,
      (reader_num - 1) * base_offset);
  ASSERT_TRUE(des_buffer_ptr_->writer_->buf_end() == des_buffer_ptr_->writer_->end());
  des_buffer_ptr_->alloc_reader();
  ASSERT_EQ(last_max_block_count + 1, des_buffer_ptr_->max_block_count());
}

void TestIOBuffer::check_two_buffer_remove_append(int64_t reader_num)
{
  int64_t written_len = 0;
  des_buffer_ptr_->alloc_reader();
  buffer_ptr_->readers_[0].start_offset_ = g_size / reader_num;
  ASSERT_EQ(0, des_buffer_ptr_->max_block_count());
  des_buffer_ptr_->remove_append(&(buffer_ptr_->readers_[0]), written_len);
  ASSERT_TRUE(NULL == buffer_ptr_->writer_);
  ASSERT_EQ(reader_num, des_buffer_ptr_->max_block_count());
}

void TestIOBuffer::check_two_buffer_write_and_transfer(int64_t reader_num)
{
  int64_t written_len = 0;
  des_buffer_ptr_->alloc_reader();
  int64_t last_max_block_count = des_buffer_ptr_->max_block_count();

  ASSERT_EQ(g_size, buffer_ptr_->readers_[0].read_avail());
  ASSERT_EQ(OB_SUCCESS, des_buffer_ptr_->write_and_transfer_left_over_space(
                        &(buffer_ptr_->readers_[0]), g_size, written_len, 0));
  ASSERT_EQ(g_size, written_len);
  ASSERT_EQ(last_max_block_count + reader_num + !!(g_size % reader_num),
      des_buffer_ptr_->max_block_count());
  ASSERT_TRUE(buffer_ptr_->writer_->buf_end() == buffer_ptr_->writer_->end());
  ASSERT_TRUE(des_buffer_ptr_->writer_->buf_end()
      == des_buffer_ptr_->writer_->data_->data() + des_buffer_ptr_->writer_->get_block_size());
}

void TestIOBuffer::check_accessor_and_new_buffer(ObMIOBufferAccessor *mio_accessor)
{
  ASSERT_TRUE(NULL == mio_accessor->mbuf_);
  ASSERT_TRUE(NULL == mio_accessor->entry_);

  mio_accessor->writer_for(des_buffer_ptr_);
  ASSERT_TRUE(des_buffer_ptr_ == mio_accessor->mbuf_);
  ASSERT_TRUE(NULL == mio_accessor->entry_);

  mio_accessor->reader_for(des_buffer_ptr_);
  ASSERT_TRUE(des_buffer_ptr_ == mio_accessor->mbuf_);
  ASSERT_TRUE(NULL == mio_accessor->entry_);

  des_buffer_ptr_ = new(std::nothrow) ObMIOBuffer((void *)g_input_buf, g_size - 1, g_size / 2);
  ASSERT_TRUE(NULL != des_buffer_ptr_);
  ASSERT_EQ(g_size - 1, des_buffer_ptr_->size_);
  ASSERT_EQ(g_size / 2, des_buffer_ptr_->water_mark_);
#ifdef TRACK_BUFFER_USER
  ASSERT_TRUE(NULL == des_buffer_ptr_->location_);
#endif
  mio_accessor->reader_for(des_buffer_ptr_);
  ASSERT_TRUE(des_buffer_ptr_ == mio_accessor->mbuf_);
  ASSERT_TRUE(des_buffer_ptr_->readers_ + 0 == mio_accessor->entry_);
  ASSERT_TRUE(des_buffer_ptr_->readers_[0].mbuf_ == des_buffer_ptr_);
  ASSERT_TRUE(des_buffer_ptr_->readers_[0].accessor_ == mio_accessor);

  mio_accessor->reader_for(&(des_buffer_ptr_->readers_[0]));
  ASSERT_TRUE(mio_accessor->entry_ == mio_accessor->reader());
  ASSERT_TRUE(mio_accessor->mbuf_ == mio_accessor->writer());

  des_buffer_ptr_->alloc_accessor(mio_accessor);
  mio_accessor->reader_for(&(des_buffer_ptr_->readers_[1]));
  ASSERT_TRUE(mio_accessor->entry_ == &(des_buffer_ptr_->readers_[1]));
  des_buffer_ptr_->readers_[0].accessor_ = NULL;
  des_buffer_ptr_->readers_[1].accessor_ = mio_accessor;
}

TEST_F(TestIOBuffer, test_ObIOBufferData_simple)
{
  LOG_DEBUG("test_ObIOBufferData_simple");
  ObIOBufferData io_data;
  ASSERT_EQ(0, io_data.size_);//check construct
  ASSERT_TRUE(NULL == io_data.data_);
  ASSERT_TRUE(NO_ALLOC == io_data.mem_type_);
#ifdef TRACK_BUFFER_USER
  ASSERT_TRUE(NULL == io_data.location_);
#endif

  int64_t size = DEFAULT_LARGE_BUFFER_SIZE;
  //check alloc
  io_data.alloc(size);
  check_data_alloc(&io_data, size);
  size = 1;
  io_data.alloc(size);
  check_data_alloc(&io_data, size);
  //check member func
  ASSERT_EQ(io_data.size_, io_data.get_block_size());
  ASSERT_EQ(0, memcmp(io_data.data_, io_data.data(), size));
  ASSERT_EQ(0, memcmp(io_data.data_, *(&io_data), size));
  io_data.dealloc();
  ASSERT_TRUE(NULL == io_data.data_);
  ASSERT_EQ(0, io_data.size_);
  ASSERT_EQ(NO_ALLOC, io_data.mem_type_);
}

TEST_F(TestIOBuffer, test_new_iobufferdata_internal)
{
  LOG_DEBUG("test_new_iobufferdata_internal");
  //check new_iobufferdata_internal(size)
#ifdef TRACK_BUFFER_USER
  char location[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/Data/");
  data_ptr_ = new_iobufferdata_internal(location);
  ASSERT_EQ(0, strcmp(data_ptr_->location_, location));
#else
  data_ptr_ = new_iobufferdata_internal();
#endif
  check_data_alloc(data_ptr_, DEFAULT_LARGE_BUFFER_SIZE);
  data_ptr_->free();
  data_ptr_ = NULL;

  //check new_iobufferdata_internal(buf, size)
  char buf[] = "hello oceanbase";
  int64_t size = sizeof(buf);
#ifdef TRACK_BUFFER_USER
  data_ptr_ = new_iobufferdata_internal(location, (void *)buf, size);
  ASSERT_EQ(0, strcmp(data_ptr_->location_, location));
#else
  data_ptr_ = new_iobufferdata_internal((void *)buf, size);
#endif
  ASSERT_TRUE(NULL != data_ptr_);
  ASSERT_EQ(size, data_ptr_->size_);
  ASSERT_EQ(CONSTANT, data_ptr_->mem_type_);
  ASSERT_EQ(0, memcmp(buf, data_ptr_->data_, size));

  data_ptr_->free();
  data_ptr_ = NULL;
}

TEST_F(TestIOBuffer, test_ObIOBufferBlock_simple)
{
  LOG_DEBUG("test_ObIOBufferBlock_simple");
  ObIOBufferBlock io_block;//check construct
  check_block_clear(&io_block);
#ifdef TRACK_BUFFER_USER
  ASSERT_TRUE(NULL == io_block.location_);
  char location[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/Block/");
  io_block.location_ = location;
#endif

  io_block.alloc();//check alloc
  ASSERT_TRUE(NULL != io_block.data_);
  ASSERT_TRUE(NULL != io_block.buf());
  check_block_reset(&io_block);
#ifdef TRACK_BUFFER_USER
  ASSERT_EQ(0, strcmp(location, io_block.data_->location_));
#endif

  //check member func
  ASSERT_TRUE(io_block.start_ == io_block.start());
  ASSERT_TRUE(io_block.end_ == io_block.end());
  ASSERT_EQ(0, io_block.size());
  ASSERT_EQ(0, io_block.read_avail());
  ASSERT_EQ(DEFAULT_LARGE_BUFFER_SIZE, io_block.write_avail());
  ASSERT_EQ(DEFAULT_LARGE_BUFFER_SIZE, io_block.get_block_size());

  char buf[] = "hello oceanbase";
  int64_t buf_len = sizeof(buf);
  memcpy(io_block.end(), buf, buf_len);
  io_block.fill(buf_len);
  ASSERT_EQ(0, memcmp(buf, io_block.start(), buf_len));

  io_block.consume(buf_len);
  ASSERT_TRUE(io_block.start() == io_block.end());

  io_block.reset();
  check_block_reset(&io_block);

  io_block.dealloc();
  ASSERT_TRUE(NULL == io_block.data_);
  ASSERT_TRUE(NULL == io_block.next_.ptr_);
  check_block_clear(&io_block);
}

TEST_F(TestIOBuffer, test_new_iobufferblock_internal)
{
  LOG_DEBUG("test_new_iobufferblock_internal");
  char buf[] = "hello oceanbase";
  int64_t size = sizeof(buf);

  //check new_iobufferblock_internal(data, len, offset)
#ifdef TRACK_BUFFER_USER
  char location_data[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/Data/");
  data_ptr_ = new_iobufferdata_internal(location_data, (void *)buf, size);
  char location[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/Block/");
  block_ptr_ = new_iobufferblock_internal(location, data_ptr_, size/2);
  ASSERT_EQ(0, strcmp(block_ptr_->location_, location));
#else
  data_ptr_ = new_iobufferdata_internal((void *)buf, size);
  block_ptr_ = new_iobufferblock_internal(data_ptr_, size / 2);
#endif
  ASSERT_TRUE(NULL != block_ptr_);
  ASSERT_TRUE(data_ptr_ == block_ptr_->data_);
  ASSERT_TRUE(block_ptr_->start_ == data_ptr_->data_);
  ASSERT_TRUE(block_ptr_->end_ == (block_ptr_->start_ + size/2));
  ASSERT_TRUE(block_ptr_->buf_end_ == (block_ptr_->start_ + data_ptr_->size_));
  ASSERT_EQ(0, memcmp(block_ptr_->end_, buf + size/2, size/2));
  ASSERT_EQ(0, memcmp(block_ptr_->start_, buf, size/2));

  //check new_iobufferblock_internal()
#ifdef TRACK_BUFFER_USER
  block_ptr_->next_ = new_iobufferblock_internal(location);
  ASSERT_EQ(0, strcmp(block_ptr_->next_->location_, location));
#else
  block_ptr_->next_ = new_iobufferblock_internal();
#endif
  ASSERT_TRUE(NULL != block_ptr_->next_);
  block_ptr_->next_->alloc();
  ASSERT_TRUE(NULL != block_ptr_->next_->data_);
  check_block_reset(block_ptr_->next_);
#ifdef TRACK_BUFFER_USER
  ASSERT_EQ(0, strcmp(location, block_ptr_->next_->data_->location_));
#endif

  block_ptr_->free();
  block_ptr_ = NULL;
  data_ptr_ = NULL;
}

TEST_F(TestIOBuffer, test_ObIOBufferBlock_realloc)
{
  LOG_DEBUG("test_ObIOBufferBlock_realloc");
  ObIOBufferBlock io_block;
  io_block.alloc();
  char buf[] = "hello oceanbase";
  int64_t buf_len = sizeof(buf);
  memcpy(io_block.end(), buf, buf_len);
  io_block.fill(buf_len);
  ASSERT_EQ(0, memcmp(buf, io_block.start(), buf_len));

#ifdef TRACK_BUFFER_USER
  char location[MAX_LOCATION_STRING_LENGTH] = RES_PATH("memory/ObIOBuffer/Block/");
  block_ptr_ = new_iobufferblock_internal(location);
  ASSERT_EQ(0, strcmp(block_ptr_->location_, location));
#else
  block_ptr_ = new_iobufferblock_internal();
#endif
  block_ptr_->alloc();
  io_block.next_ = block_ptr_;

  //check clone
  ObIOBufferBlock *block_clone = io_block.clone();
  block_ptr_->next_ = block_clone;
  common::ObPtr<ObIOBufferBlock> tmp_io_block = block_ptr_->next_;
  ASSERT_TRUE(block_clone->data_ == io_block.data_);
  ASSERT_TRUE(block_clone->start_ == io_block.start_);
  ASSERT_TRUE(block_clone->end_ == io_block.end_);
  ASSERT_TRUE(block_clone->buf_end_ == io_block.end_);
#ifdef TRACK_BUFFER_USER
  ASSERT_TRUE(block_clone->location_ == io_block.location_);
#endif

  int64_t size = buf_len + 1;
  io_block.realloc(size);
  block_ptr_ = NULL;
  ASSERT_EQ(size, io_block.data_->size_);
  ASSERT_TRUE(DEFAULT_ALLOC == io_block.data_->mem_type_);
  ASSERT_TRUE(io_block.end_ == (io_block.start_ + buf_len));
  ASSERT_TRUE(io_block.buf_end_ == (io_block.start_ + size));
  ASSERT_EQ(0, memcmp(io_block.start_, buf, buf_len));
  ASSERT_TRUE(NULL == io_block.next_.ptr_);
  ASSERT_TRUE(NULL != tmp_io_block);
  tmp_io_block = NULL;

  io_block.realloc(buf, size + 1);
  ASSERT_EQ(size + 1, io_block.data_->size_);
  ASSERT_TRUE(io_block.end_ == (io_block.start_ + buf_len));
  ASSERT_TRUE(io_block.buf_end_ == (io_block.start_ + size + 1));
  ASSERT_EQ(0, memcmp(io_block.start_, buf, buf_len));//hello
  ASSERT_TRUE(NULL == io_block.next_.ptr_);

  io_block.dealloc();
}

TEST_F(TestIOBuffer, test_ObMIOBuffer_two_buffer_memcpy)
{
  LOG_DEBUG("test_ObMIOBuffer_two_buffer_memcpy");
  common_two_buffer_init();
  check_two_buffer_memcpy(MAX_MIOBUFFER_READERS);
}

TEST_F(TestIOBuffer, check_two_buffer_remove_append)
{
  LOG_DEBUG("check_two_buffer_remove_append");
  common_two_buffer_init();
  check_two_buffer_remove_append(MAX_MIOBUFFER_READERS);
}

TEST_F(TestIOBuffer, check_two_buffer_write_and_transfer)
{
  LOG_DEBUG("check_two_buffer_write_and_transfer");
  common_two_buffer_init();
  check_two_buffer_write_and_transfer(MAX_MIOBUFFER_READERS);
}

TEST_F(TestIOBuffer, test_ObMIOBuffer_clone_memchr)
{
  LOG_DEBUG("test_ObMIOBuffer_clone_memchr");
  init_miobuffer(g_size, true);
  buffer_ptr_->set(g_input_buf, g_size);
  ASSERT_EQ(0, buffer_ptr_->write_avail());
  init_miobuffer_alloc_readers(1);

  //clone_reader
  buffer_ptr_->clone_reader(&buffer_ptr_->readers_[0]);//clone readers_[0] as readers_[1]
  ASSERT_TRUE(buffer_ptr_->readers_[1].block_ == buffer_ptr_->readers_[0].block_);
  ASSERT_TRUE(buffer_ptr_->readers_[1].mbuf_ == buffer_ptr_);

  //clone
  buffer_ptr_->readers_[0].clone();//clone readers_[0] as readers_[2]
  ASSERT_TRUE(buffer_ptr_->readers_[2].block_ == buffer_ptr_->readers_[0].block_);
  ASSERT_TRUE(buffer_ptr_->readers_[2].mbuf_ == buffer_ptr_);

  //append_allocated
  ASSERT_EQ(1, buffer_ptr_->max_block_count());
  ASSERT_EQ(g_size, buffer_ptr_->max_read_avail());
  buffer_ptr_->append_allocated(g_input_buf, g_size / 2);
  ASSERT_EQ(2, buffer_ptr_->max_block_count());
  ASSERT_EQ(g_size + g_size / 2, buffer_ptr_->max_read_avail());

  buffer_ptr_->readers_[0].dealloc();
}

TEST_F(TestIOBuffer, test_ObMIOBuffer_accessor_puts)
{
  LOG_DEBUG("test_ObMIOBuffer_accessor_puts");
  ObMIOBufferAccessor mio_accessor;
  check_accessor_and_new_buffer(&mio_accessor);
  ASSERT_TRUE(des_buffer_ptr_->readers_[1].is_high_water());
  ASSERT_TRUE(des_buffer_ptr_->readers_[1].is_low_water());
  ASSERT_FALSE(des_buffer_ptr_->readers_[1].is_current_low_water());

  des_buffer_ptr_->destroy();
  delete des_buffer_ptr_;
  des_buffer_ptr_ = NULL;
  mio_accessor.destroy();
}

TEST_F(TestIOBuffer, test_ObMIOBuffer_size_and_check_block_free)
{
  LOG_DEBUG("test_ObMIOBuffer_size_and_check_block_free");

  for (int64_t i = BUFFER_SIZE_INDEX_512; i <= BUFFER_SIZE_INDEX_COUNT; ++i) {
    int64_t buf_size = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);
    int64_t input_length = 0;
    int64_t written_len = 0;

    init_miobuffer(buf_size);
    init_miobuffer_alloc_readers(1);
    buffer_ptr_->init_readers();
    buffer_ptr_->water_mark_ = buf_size;//set it big enough for call check_add_block()
    ASSERT_EQ(1, buffer_ptr_->readers_[0].get_block_count());

    //check check_add_block()
    buffer_ptr_->check_add_block();
    ASSERT_EQ(2, buffer_ptr_->readers_[0].get_block_count());

    ASSERT_EQ(buf_size * 2, buffer_ptr_->current_write_avail());
    while (input_length < buf_size) {
      buffer_ptr_->write(g_input_buf, g_size, written_len);
      input_length += written_len;
    }
    ASSERT_EQ(input_length, buffer_ptr_->max_read_avail());//limit_size

    buffer_ptr_->destroy();
    check_buffer_clear(buffer_ptr_);
    op_reclaim_free(buffer_ptr_);
    buffer_ptr_ = NULL;
  }
}

TEST_F(TestIOBuffer, test_OBMIOBufferReader_replace_with_char)
{
  LOG_DEBUG("test_ObMIOBufferReader_replace_with_char");

  int64_t buf_size = g_size / MAX_MIOBUFFER_READERS;
  int64_t written_len = 0;

  init_miobuffer(buf_size);
  init_miobuffer_alloc_readers(1);
  buffer_ptr_->init_readers();
  buffer_ptr_->water_mark_ = buf_size;//set it big enough for call check_add_block()
  ObIOBufferReader *reader = buffer_ptr_->alloc_reader();
  ASSERT_EQ(1, reader->get_block_count());

  buffer_ptr_->write(g_input_buf, g_size, written_len); // need add block
  ASSERT_EQ(g_size, written_len);
  ASSERT_EQ(4, reader->get_block_count());
  ASSERT_EQ(g_size, reader->read_avail());

  char target_buf[g_size] = "\0";
  char mark = '*';
  memset(target_buf, mark, g_size);
  
  int64_t start_offset = 0;
  int64_t delta_size = 5;
  reader->replace_with_char(mark, buf_size + delta_size, start_offset);
  ASSERT_EQ(buf_size, reader->block_read_avail());
  ObIOBufferBlock *b = reader->block_;
  ASSERT_EQ(0, memcmp(target_buf, b->start() + start_offset, buf_size));
  b = b->next_;
  ASSERT_EQ(0, memcmp(target_buf, b->start(), delta_size));
  ASSERT_NE(0, memcmp(target_buf, b->start(), buf_size));

  buffer_ptr_->destroy();
  check_buffer_clear(buffer_ptr_);
  op_reclaim_free(buffer_ptr_);
  buffer_ptr_ = NULL;
}

TEST_F(TestIOBuffer, test_OBMIOBufferReader_replace)
{
  LOG_DEBUG("test_ObMIOBufferReader_replace");

  int64_t buf_size = g_size / MAX_MIOBUFFER_READERS;
  int64_t written_len = 0;

  init_miobuffer(buf_size);
  init_miobuffer_alloc_readers(1);
  buffer_ptr_->init_readers();
  buffer_ptr_->water_mark_ = buf_size;//set it big enough for call check_add_block()
  ObIOBufferReader *reader = buffer_ptr_->alloc_reader();
  ASSERT_EQ(1, reader->get_block_count());

  buffer_ptr_->write(g_input_buf, g_size, written_len); // need add block
  ASSERT_EQ(g_size, written_len);
  ASSERT_EQ(4, reader->get_block_count());
  ASSERT_EQ(g_size, reader->read_avail());

  char *target_buf = new(std::nothrow) char[g_size];
  char mark = '*';
  memset(target_buf, mark, g_size);
  
  int64_t start_offset = 0;
  reader->replace(target_buf, g_size, start_offset);
  ASSERT_EQ(buf_size, reader->block_read_avail());
  ASSERT_EQ(g_size, reader->read_avail());
  ObIOBufferBlock *b = reader->block_;
  int64_t bytes = 0;
  int64_t remain_len = g_size;
  char *buf = target_buf;
  while (NULL != b) {
    ASSERT_EQ(buf_size, reader->block_read_avail());
    bytes = buf_size < remain_len ? buf_size : remain_len;
    ASSERT_EQ(0, memcmp(buf, b->start(), bytes));
    buf += bytes;
    remain_len -= bytes;
    b = b->next_;
  }

  delete []target_buf;
  target_buf = NULL;
  buffer_ptr_->destroy();
  check_buffer_clear(buffer_ptr_);
  op_reclaim_free(buffer_ptr_);
  buffer_ptr_ = NULL;
}

} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

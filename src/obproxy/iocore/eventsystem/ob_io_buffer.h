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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * *************************************************************
 *
 * I/O Buffer classes
 *
 * Watermarks can be used as an interface between the data transferring
 * layer (ObVConnection) and the user layer (a state machine).  Watermarks
 * should be used when you need to have at least a certain amount of data
 * to make some determination.  For example, when parsing a string, one
 * might wish to ensure that an entire line will come in before consuming
 * the data.  In such a case, the water_mark should be set to the largest
 * possible size of the string. (appropriate error handling should take
 * care of excessively long strings).
 *
 * In all other cases, especially when all data will be consumed, the
 * water_mark should be set to 0 (the default).
 */

#ifndef OBPROXY_IOBUFFER_H
#define OBPROXY_IOBUFFER_H

#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_resource_tracker.h"
#include "iocore/eventsystem/ob_thread_allocator.h"
#include "iocore/eventsystem/ob_ethread.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

struct ObMIOBufferAccessor;
class ObMIOBuffer;
class ObIOBufferReader;
class ObVIO;

//#if !defined(TRACK_BUFFER_USER)
//#define TRACK_BUFFER_USER 1
//#endif

enum ObAllocType
{
  NO_ALLOC,
  DEFAULT_ALLOC,
  CONSTANT
};

/**
 * A reference counted wrapper around fast allocated or malloced memory.
 * The ObIOBufferData class provides two basic services around a portion
 * of allocated memory.
 * First, it is a reference counted object and ...
 *
 * @remarks The ObAllocType enum, is used to define the type of allocation
 * for the memory this ObIOBufferData object manages.
 *
 * NO_ALLOC
 * DEFAULT_ALLOC
 * CONSTANT
 */
class ObIOBufferData : public common::ObRefCountObj
{
public:
  /**
   * Constructor. Initializes state for a ObIOBufferData object. Do not use
   * this method. Use one of the functions with the 'new_' prefix instead.
   */
  ObIOBufferData()
      : size_(0), mem_type_(NO_ALLOC), data_(NULL)
#ifdef TRACK_BUFFER_USER
      , location_(NULL)
#endif
  { }

  virtual ~ObIOBufferData() { }

  /**
   * The size of the memory allocated by this ObIOBufferData. Calculates
   * the amount of memory allocated by this ObIOBufferData.
   *
   * @return number of bytes allocated for the 'data_' member.
   */
  int64_t get_block_size() const { return size_; }

  /**
   * Provides access to the allocated memory. Returns the address of the
   * allocated memory handled by this ObIOBufferData.
   *
   * @return address of the memory handled by this ObIOBufferData.
   */
  char *data() { return data_; }

  /**
   * Cast operator. Provided as a convenience, the cast to a char* applied
   * to the ObIOBufferData returns the address of the memory handled by the
   * ObIOBuffer data. In this manner, objects of this class can be used as
   * parameter to functions requiring a char*.
   *
   * @return
   */
  operator char *() { return data_; }

  /**
   * Allocates memory and sets this ObIOBufferData to point to it.
   * Allocates memory according to the size and type
   * parameters. Any previously allocated memory pointed to by
   * this ObIOBufferData is deallocated.
   *
   * @param size
   * @param type of allocation to use; see remarks section.
   */
  int alloc(const int64_t size);

  /**
   * Frees the memory managed by this ObIOBufferData.  Deallocates the
   * memory previously allocated by this ObIOBufferData object. It frees
   * the memory pointed to by 'data' according to the 'mem_type_' and
   * 'size_' members.
   */
  void dealloc();

  /**
   * Frees the ObIOBufferData object and its underlying memory. Deallocates
   * the memory managed by this ObIOBufferData and then frees itself. You
   * should not use this object or reference after this call.
   */
  virtual void free();

  int64_t size_;

  /**
   * Type of allocation used for the managed memory. Stores the type of
   * allocation used for the memory currently managed by the ObIOBufferData
   * object. Do not set or modify this value directly. Instead use the
   * alloc or dealloc methods.
   */
  ObAllocType mem_type_;

  /**
   * Points to the allocated memory. This member stores the address of
   * the allocated memory. You should not modify its value directly,
   * instead use the alloc or dealloc methods.
   */
  char *data_;

#ifdef TRACK_BUFFER_USER
  const char *location_;
#endif

private:
  // declaration only
  DISALLOW_COPY_AND_ASSIGN(ObIOBufferData);
};

/**
 * A linkable portion of ObIOBufferData. ObIOBufferBlock is a chainable
 * buffer block descriptor. The ObIOBufferBlock represents both the used
 * and available space in the underlying block. The ObIOBufferBlock is not
 * sharable between buffers but rather represents what part of the data
 * block is both in use and usable by the ObMIOBuffer it is attached to.
 */
class ObIOBufferBlock : public common::ObRefCountObj
{
public:
  /**
   * Constructor of a ObIOBufferBlock. Do not use it to create a new object,
   * instead call new_IOBufferBlock
   */
  ObIOBufferBlock();

  virtual ~ObIOBufferBlock() { }

  /**
   * Access the actual data. Provides access to the     underlying data
   * managed by the ObIOBufferData.
   *
   * @return pointer to the underlying data.
   */
  char *buf() { return data_->data_; }

  /**
   * Beginning of the inuse section. Returns the position in the buffer
   * where the inuse area begins.
   *
   * @return pointer to the start of the inuse section.
   */
  char *start() { return start_; }

  /**
   * End of the used space. Returns a pointer to end of the used space
   * in the data buffer represented by this block.
   *
   * @return pointer to the end of the inuse portion of the block.
   */
  char *end() { return end_; }

  /**
   * End of the data buffer. Returns a pointer to end of the data buffer
   * represented by this block.
   *
   * @return
   */
  char *buf_end() { return buf_end_; }

  /**
   * Size of the inuse area. Returns the size of the current inuse area.
   *
   * @return bytes occupied by the inuse area.
   */
  int64_t size() const { return static_cast<int64_t>(end_ - start_); }

  /**
   * Size of the data available for reading. Returns the size of the data
   * available for reading in the inuse area.
   *
   * @return bytes available for reading from the inuse area.
   */
  int64_t read_avail() const { return static_cast<int64_t>(end_ - start_); }

  /**
   * Space available in the buffer. Returns the number of bytes that can
   * be written to the data buffer.
   *
   * @return space available for writing in this ObIOBufferBlock.
   */
  int64_t write_avail() const { return static_cast<int64_t>(buf_end_ - end_); }

  /**
   * Size of the memory allocated by the underlying ObIOBufferData.
   * Computes the size of the entire block, which includes the used and
   * available areas. It is the memory allocated by the ObIOBufferData
   * referenced by this ObIOBufferBlock.
   *
   * @return bytes allocated to the ObIOBufferData referenced by this
   *         ObIOBufferBlock.
   */
  int64_t get_block_size() const { return data_->get_block_size(); }

  /**
   * Decrease the size of the inuse area. Moves forward the start of
   * the inuse area. This also decreases the number of available bytes
   * for reading.
   *
   * @param len    bytes to consume or positions to skip for the start of
   *               the inuse area.
   */
  int consume(const int64_t len);

  /**
   * Increase the inuse area of the block. Adds 'len' bytes to the inuse
   * area of the block. Data should be copied into the data buffer by
   * using end() to find the start of the free space in the data buffer
   * before calling fill()
   *
   * @param len    bytes to increase the inuse area. It must be less than
   *               or equal to the value of write_avail().
   */
  int fill(const int64_t len);

  /**
   * Trim the last 'len' bytes.
   *
   * @param len    bytes to trim. It must be less than
   *               or equal to the value of reserved size.
   * @return       reamin untrimed bytes
   */
  int64_t trim(const int64_t len);

  /**
   * Reset the inuse area. The start and end of the inuse area are reset
   * but the actual ObIOBufferData referenced by this ObIOBufferBlock is not
   * modified.  This effectively reduces the number of bytes available
   * for reading to zero, and the number of bytes available for writing
   * to the size of the entire buffer.
   */
  void reset();

  /**
   * Create a copy of the ObIOBufferBlock. Creates and returns a copy of this
   * ObIOBufferBlock that references the same data that this ObIOBufferBlock
   * (it does not allocate an another buffer). The cloned block will not
   * have a writable space since the original ObIOBufferBlock mantains the
   * ownership for writing data to the block.
   *
   * @return copy of this ObIOBufferBlock.
   */
  ObIOBufferBlock *clone();

  /**
   * Clear the ObIOBufferData this ObIOBufferBlock handles. Clears this
   * ObIOBufferBlock's reference to the data buffer (ObIOBufferData). You can
   * use alloc after this call to allocate an ObIOBufferData associated to
   * this ObIOBufferBlock.
   */
  void destroy();

  /**
   * Allocate a data buffer.
   *
   * @param size
   */
  int alloc(const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

  /**
   * Clear the ObIOBufferData this ObIOBufferBlock handles. Clears this
   * ObIOBufferBlock's reference to the data buffer (ObIOBufferData).
   */
  void dealloc();

  /**
   * Frees the ObIOBufferBlock object and its underlying memory.
   * Removes the reference to the ObIOBufferData object and then frees
   * itself. You should not use this object or reference after this
   * call.
   */
  virtual void free();

  /**
   * Set or replace this ObIOBufferBlock's ObIOBufferData member. Sets this
   * ObIOBufferBlock's ObIOBufferData member to point to the ObIOBufferData
   * passed in. You can optionally specify the inuse area with the 'len'
   * argument and an offset for the start.
   *
   * @param d      new ObIOBufferData this ObIOBufferBlock references.
   * @param len    in use area to set. It must be less than or equal to the
   *               length of the block size *ObIOBufferData).
   * @param offset bytes to skip from the beginning of the ObIOBufferData
   *               and to mark its start.
   */
  int set_internal(void *b, const int64_t len);
  void set(ObIOBufferData *d, const int64_t len = 0, const int64_t offset = 0);
  int realloc_set_internal(void *b, const int64_t buf_size);
  int realloc(void *b, const int64_t buf_size);
  int realloc(const int64_t size);

  char *start_;
  char *end_;
  char *buf_end_;

#ifdef TRACK_BUFFER_USER
  const char *location_;
#endif

  /**
   * The underlying reference to the allocated memory. A reference to a
   * ObIOBufferData representing the memory allocated to this buffer. Do
   * not set or modify its value directly.
   */
  common::ObPtr<ObIOBufferData> data_;

  /**
   * Reference to another ObIOBufferBlock. A reference to another
   * ObIOBufferBlock that allows this object to link to other.
   */
  common::ObPtr<ObIOBufferBlock> next_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIOBufferBlock);
};

/**
 * An independent reader from an ObMIOBuffer. A reader for a set of
 * ObIOBufferBlocks. The ObIOBufferReader represents the place where a given
 * consumer of buffer data is reading from. It provides a uniform interface
 * for easily accessing the data contained in a list of ObIOBufferBlocks
 * associated with the ObIOBufferReader.
 *
 * ObIOBufferReaders are the abstraction that determine when data blocks
 * can be removed from the buffer.
 */
class ObIOBufferReader
{
public:
  ObIOBufferReader()
      : accessor_(NULL), mbuf_(NULL), start_offset_(0), reserved_size_(0)
  { }

  ~ObIOBufferReader() { }

  /**
   * Start of unconsumed data. Returns a pointer to first unconsumed data
   * on the buffer for this reader. A null pointer indicates no data is
   * available. It uses the current start_offset value.
   *
   * @return pointer to the start of the unconsumed data.
   */
  char *start();

  /**
   * End of inuse area of the first block with unconsumed data. Returns a
   * pointer to the end of the first block with unconsumed data for this
   * reader. A NULL pointer indicates there are no blocks with unconsumed
   * data for this reader.
   *
   * @return pointer to the end of the first block with unconsumed data.
   */
  char *end();

  /**
   * Amount of data available across all of the ObIOBufferBlocks. Returns the
   * number of unconsumed bytes of data available to this reader across
   * all remaining ObIOBufferBlocks. It subtracts the current start_offset
   * value from the total.
   *
   * @return bytes of data available across all the buffers.
   */
  int64_t read_avail() const;

  /**
   * Check if there is more than size bytes available to read.
   *
   * @param size
   *
   * @return true if more than size byte are available.
   */
  bool is_read_avail_more_than(const int64_t size) const;

  /**
   * Amount of data available in the first buffer with data for this
   * reader.  Returns the number of unconsumed bytes of data available
   * on the first ObIOBufferBlock with data for this reader.
   *
   * @return number of unconsumed bytes of data available in the first
   *         buffer.
   */
  int64_t block_read_avail();

  /**
   * Number of ObIOBufferBlocks with data in the block list. Returns the
   * number of ObIOBufferBlocks on the block list with data remaining for
   * this reader.
   *
   * @return number of blocks with data for this reader.
   */
  int64_t get_block_count() const;

  void skip_empty_blocks();

  /**
   * whether all readable data is in one single IOBlockBuffer
   */
  bool is_in_single_buffer_block();

  /**
   * Clears all fields in this ObIOBuffeReader, rendering it unusable. Drops
   * the reference to the ObIOBufferBlock list, the accessors, ObMIOBuffer and
   * resets this reader's state. You have to set those fields in order
   * to use this object again.
   */
  void destroy();

  /**
   * Instruct the reader to reset the ObIOBufferBlock list. Resets the
   * reader to the point to the start of the block where new data will
   * be written. After this call, the start_offset field is set to zero
   * and the list of ObIOBufferBlocks is set using the associated ObMIOBuffer.
   */
  void reset();

  /**
   * Consume a number of bytes from this reader's ObIOBufferBlock
   * list. Advances the current position in the ObIOBufferBlock list of
   * this reader by n bytes.
   *
   * @param len    number of bytes to consume. It must be less than or equal
   *               to read_avail().
   */
  int consume(const int64_t len);
  int consume_internal(const int64_t len);

  /**
   * Consume read_avail() bytes from this reader's ObIOBufferBlock list.
   */
  int consume_all();

  /**
   * Create another reader with access to the same data as this
   * ObIOBufferReader. Allocates a new reader with the same state as this
   * ObIOBufferReader. This means that the new reader will point to the same
   * list of ObIOBufferBlocks and to the same buffer position as this reader.
   *
   * @return new reader with the same state as this.
   */
  ObIOBufferReader *clone();

  /**
   * Deallocate this reader. Removes and deallocates this reader from
   * the underlying ObMIOBuffer. This ObIOBufferReader object must not be
   * used after this call.
   */
  void dealloc();

  /**
   * Consult this reader's ObMIOBuffer writable space. Queries the ObMIOBuffer
   * associated with this reader about the amount of writable space
   * available without adding any blocks on the buffer and returns true
   * if it is less than the water mark.
   *
   * @return true if the ObMIOBuffer associated with this ObIOBufferReader
   *         returns true in ObMIOBuffer::is_current_low_water().
   */
  bool is_current_low_water() const;

  /**
   * Queries the underlying ObMIOBuffer about. Returns true if the amount
   * of writable space after adding a block on the underlying ObMIOBuffer
   * is less than its water mark. This function call may add blocks to
   * the ObMIOBuffer (see ObMIOBuffer::is_low_water()).
   *
   * @return result of ObMIOBuffer::is_low_water() on the ObMIOBuffer for
   *         this reader.
   */
  bool is_low_water() const;

  /**
   * To see if the amount of data available to the reader is greater than
   * the ObMIOBuffer's water mark. Indicates whether the amount of data
   * available to this reader exceeds the water mark for this reader's
   * ObMIOBuffer.
   *
   * @return true if the amount of data exceeds the ObMIOBuffer's water mark.
   */
  bool is_high_water() const;

  /**
   * Copy data but do not consume it. Copies 'len' bytes of data from
   * the current buffer into the supplied buffer. The copy skips the
   * number of bytes specified by 'offset' beyond the current point of
   * the reader. It also takes into account the current start_offset value.
   *
   * @param buf    in which to place the data. The pointer is modified after
   *               the call and points one position after the end of the data copied.
   * @param len    bytes to copy. If len exceeds the bytes available to the
   *               reader or INT64_MAX is passed in, the number of bytes available is
   *               used instead. No data is consumed from the reader in this operation.
   * @param offset bytes to skip from the current position. The parameter
   *               is modified after the call.
   *
   * @return pointer to one position after the end of the data copied. The
   *         parameter buf is set to this value also.
   */
  char *copy(char *dest_buf, const int64_t copy_len = INT64_MAX, const int64_t start_offset = 0);

  /**
   *  Replace data but do not consume it. Replace 'replace_len' bytes of data in the reader buffer.
   *  The replace skips the number of bytes specified by 'offset' beyond the current point
   *  of the reader. It also takes into account the current start_offset value.
   *
   *  @param src_buf       a buffer which stores bytes to be written into the reader
   *  @param replace_len   length of bytes to be written.
   *  @param start_offset  bytes to skip from the current position.
   */
  void replace(const char *src_buf, const int64_t replace_len, const int64_t start_offset = 0);

  /**
   *  Replace data but do not consume it. Replace 'replace_len' bytes of data in the reader buffer with specified char.
   *  The replace skips the number of bytes specified by 'offset' beyond the current point
   *  of the reader. It also takes into account the current start_offset value.
   *
   *  @param mark          a specified char to be written into the reader.
   *  @param replace_len   length of bytes to be written.
   *  @param start_offset  bytes to skip from the current position.
   */
  void replace_with_char(const char mark, const int64_t replace_len, const int64_t start_offset = 0);
  /**
   * Get a pointer to the first block with data. Returns a pointer to
   * the first ObIOBufferBlock in the block chain with data available for
   * this reader
   *
   * @return pointer to the first ObIOBufferBlock in the list with data
   *         available for this reader.
   */
  ObIOBufferBlock *get_current_block();

  ObMIOBuffer *writer() const { return mbuf_; }

  bool is_allocated() const { return NULL != mbuf_; }


  ObMIOBufferAccessor *accessor_;  // pointer back to the accessor

  /**
   * Back pointer to this object's ObMIOBuffer. A pointer back to the
   * ObMIOBuffer this reader is allocated from.
   */
  ObMIOBuffer *mbuf_;
  common::ObPtr<ObIOBufferBlock> block_;

  /**
   * Offset beyond the shared start(). The start_offset is used in the
   * calls that copy or consume data and is an offset at the beginning
   * of the available data.
   */
  int64_t start_offset_;
  int64_t reserved_size_;
};

/**
 * A multiple reader, single writer memory buffer. ObMIOBuffers are at
 * the center of all IOCore data transfer. ObMIOBuffers are the data
 * buffers used to transfer data to and from VConnections. A ObMIOBuffer
 * points to a list of ObIOBufferBlocks which in turn point to ObIOBufferData
 * structures that in turn point to the actual data. ObMIOBuffer allows one
 * producer and multiple consumers. The buffer fills up according the
 * amount of data outstanding for the slowest consumer. Thus, ObMIOBuffer
 * implements automatic flow control between readers of different speeds.
 * Data on ObIOBuffer is immutable. Once written it cannot be modified, only
 * deallocated once all consumers have finished with it. Immutability is
 * necessary since data can be shared between buffers, which means that
 * multiple ObIOBufferBlock objects may reference the same data but only
 * one will have ownership for writing.
 */
class ObMIOBuffer
{
public:
  ObMIOBuffer(void *b, const int64_t buf_size, const int64_t water_mark);
  explicit ObMIOBuffer(const int64_t default_size);
  ObMIOBuffer();

  ~ObMIOBuffer() { }

  /**
   * Increase writer's inuse area. Instructs the writer associated with
   * this ObMIOBuffer to increase the inuse area of the block by as much as
   * 'len' bytes.
   *
   * @param len    number of bytes to add to the inuse area of the block.
   */
  int fill(const int64_t len);

  /**
   * Trim the last 'len' bytes, will move the writer_ pointer if the last
   * block is not enough to trim
   *
   * @param buf_reader  the buffer reader contains the prev block
   * @param len         number of bytes to trim
   * @return            return OB_SUCCESS if all bytes has been trimed,
   *                    otherwise will return OB_BUF_NOT_ENOUGH
   */
  int trim(ObIOBufferReader &buf_reader, const int64_t len);

  /**
   * Get the priv block of current block
   *
   * @param buf_reader  use to find prev block
   * @param cur_block   current block
   * @return            return priv block, if not found return NULL
   */
  ObIOBufferBlock *get_prev_block(ObIOBufferReader &buf_reader, ObIOBufferBlock *cur_block);

  /**
   * Adds a block to the end of the block list. The block added to list
   * must be writable by this buffer and must not be writable by any
   * other buffer.
   *
   * @param b
   */
  int append_block(ObIOBufferBlock *b);

  /**
   * Adds a new block to the end of the block list. The size is determined
   * by asize. See the remarks section for a mapping of indexes to
   * buffer block sizes.
   *
   * @param asize
   */
  int append_block(const int64_t size);

  /**
   * Adds new block to the end of block list using the block size for
   * the buffer specified when the buffer was allocated.
   */
  int add_block();

  int add_block(const int64_t block_count);

  /**
   * Adds by reference len bytes of data pointed to by b to the end of the
   * buffer. b MUST be a pointer to the beginning of block allocated from
   * op_reclaim_alloc(type). The data will be deallocated by the buffer once
   * all readers on the buffer have consumed it.
   *
   * @param b
   * @param len
   */
  int append_allocated(void *b, const int64_t len);

  int append_block_internal(ObIOBufferBlock *b);

  int set(void *b, const int64_t len);
  int alloc(const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

  void realloc(const int64_t size) { writer_->realloc(size); }

  void realloc(void *b, const int64_t buf_size) { writer_->realloc(b, buf_size); }

  /**
   * Adds the nbytes worth of data pointed by rbuf to the buffer. The
   * data is copied into the buffer. write() does not respect watermarks
   * or buffer size limits. Users of write must implement their own flow
   * control. Returns the number of bytes added.
   *
   * @param rbuf
   * @param nbytes
   *
   * @return
   */
  int write(const char *src_buf, const int64_t towrite_len, int64_t &written_len);

  /**
   * Add by data from ObIOBufferReader r to the this buffer by reference. If
   * len is INT64_MAX, all available data on the reader is added. If len is
   * less than INT64_MAX, the smaller of len or the amount of data on the
   * buffer is added. If offset is greater than zero, than the offset
   * bytes of data at the front of the reader are skipped. Bytes skipped
   * by offset reduce the number of bytes available on the reader used
   * in the amount of data to add computation. write() does not respect
   * watermarks or buffer size limits. Users of write must implement
   * their own flow control. Returns the number of bytes added. Each
   * write() call creates a new ObIOBufferBlock, even if it is for one
   * byte. As such, it's necessary to exercise caution in any code that
   * repeatedly transfers data from one buffer to another, especially if
   * the data is being read over the network as it may be coming in very
   * small chunks. Because deallocation of outstanding buffer blocks is
   * recursive, it's possible to overrun the stack if too many blocks
   * have been added to the buffer chain. It's imperative that users
   * both implement their own flow control to prevent too many bytes
   * from becoming outstanding on a buffer that the write() call is
   * being used and that care be taken to ensure the transfers are of a
   * minimum size. Should it be necessary to make a large number of small
   * transfers, it's preferable to use a interface that copies the data
   * rather than sharing blocks to prevent a build of blocks on the buffer.
   *
   * @param r
   * @param len
   * @param offset
   *
   * @return
   */
  int write(ObIOBufferReader *r, const int64_t towrite_len, int64_t &written_len,
            const int64_t start_offset = 0);

  /**
   * Same functionality as write but for the one small difference. The
   * space available in the last block is taken from the original and
   * this space becomes available to the copy.
   *
   * @param r
   * @param len
   * @param offset
   *
   * @return
   */
  int write_and_transfer_left_over_space(ObIOBufferReader *r,
                                         const int64_t towrite_len,
                                         int64_t &written_len,
                                         const int64_t start_offset = 0);

  int remove_append(ObIOBufferReader *r, int64_t &append_len);

  /**
   * Returns a pointer to the first writable block on the block chain.
   * Returns NULL if there are not currently any writable blocks on the
   * block list.
   *
   * @return
   */
  ObIOBufferBlock *first_write_block()
  {
    ObIOBufferBlock *ret = NULL;

    if (NULL != writer_) {
      if (NULL != writer_->next_ && 0 == writer_->write_avail()) {
        ret = writer_->next_;
      } else {
        if (OB_UNLIKELY(NULL != writer_->next_) && OB_UNLIKELY(0 != writer_->next_->read_avail())) {
          PROXY_EVENT_LOG(ERROR, "next block read avail must be 0",
                          "next_block_read_avail", writer_->next_->read_avail());
        } else {
          ret = writer_;
        }
      }
    }

    return ret;
  }

  char *buf()
  {
    ObIOBufferBlock *b = first_write_block();

    return NULL != b ? b->buf() : NULL;
  }

  char *buf_end() { return first_write_block()->buf_end(); }

  char *start() { return first_write_block()->start(); }

  char *end() { return first_write_block()->end(); }

  // the write_avail_buf must be successive, and buf's len must be (0, 8KB]
  int get_write_avail_buf(char *&start, int64_t &len);
  // make sure that current write block has the reserved_len of successive buf
  int reserve_successive_buf(const int64_t reserved_len);

  /**
   * Returns the amount of space of available for writing on the first
   * writable block on the block chain (the one that would be returned
   * by first_write_block()).
   *
   * @return
   */
  int64_t block_write_avail();

  /**
   * Returns the amount of space of available for writing on all writable
   * blocks currently on the block chain.  Will NOT add blocks to the
   * block chain.
   *
   * @return
   */
  int64_t current_write_avail() const;

  /**
   * Adds blocks for writing if the watermark criteria are met. Returns
   * the amount of space of available for writing on all writable blocks
   * on the block chain after a block due to the watermark criteria.
   *
   * @return
   */
  int64_t write_avail();

  int64_t write_avail(const int64_t total_size);

  /**
   * Returns the default data block size for this buffer.
   *
   * @return
   */
  int64_t get_block_size() const { return size_; }

  void set_block_size(const int64_t size) { size_ = size; }

  /**
   * Returns true if amount of the data outstanding on the buffer exceeds
   * the watermark.
   *
   * @return
   */
  bool is_high_water() const { return max_read_avail() > water_mark_; }

  /**
   * Returns true if the amount of writable space after adding a block on
   * the buffer is less than the water mark. Since this function relies
   * on write_avail() it may add blocks.
   *
   * @return
   */
  bool is_low_water() { return write_avail() <= water_mark_; }

  /**
   * Returns true if amount the amount writable space without adding and
   * blocks on the buffer is less than the water mark.
   *
   * @return
   */
  bool is_current_low_water() const { return max_read_avail() + current_write_avail() <= water_mark_; }

  /**
   * Allocates a new ObIOBuffer reader and sets it's its 'accessor' field
   * to point to 'accessor'.
   *
   * @param accessor
   *
   * @return
   */
  ObIOBufferReader *alloc_accessor(ObMIOBufferAccessor *accessor);

  /**
   * Allocates an ObIOBufferReader for this buffer. ObIOBufferReaders hold
   * data on the buffer for different consumers. ObIOBufferReaders are
   * REQUIRED when using buffer. alloc_reader() MUST ONLY be a called
   * on newly allocated buffers.
   *
   * Attention!!! Calling on a buffer with data already
   * placed on it will result in the reader starting at an indeterminate
   * place on the buffer.
   *
   * @return
   */
  ObIOBufferReader *alloc_reader();

  /**
   * Allocates a new reader on this buffer and places it's starting
   * point at the same place as reader r. r MUST be a pointer to a reader
   * previous allocated from this buffer.
   *
   * @param r
   *
   * @return
   */
  ObIOBufferReader *clone_reader(ObIOBufferReader *r);

  /**
   * Deallocates reader e from this buffer. e MUST be a pointer to a reader
   * previous allocated from this buffer. Reader need to allocated when a
   * particularly consumer is being removed from the buffer but the buffer
   * is still in use. Deallocation is not necessary when the buffer is
   * being freed as all outstanding readers are automatically deallocated.
   *
   * @param e
   */
  void dealloc_reader(ObIOBufferReader *e);

  /**
   * Deallocates all outstanding readers on the buffer.
   */
  void dealloc_all_readers();

  bool empty() const { return NULL == writer_; }

  int64_t max_read_avail() const;
  int64_t max_block_count() const;
  int check_add_block();
  int check_add_block(const int64_t total_size);
  ObIOBufferBlock *get_current_block();

  void reset()
  {
    if (NULL != writer_) {
      writer_->reset();
    }

    for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; ++i) {
      if (readers_[i].is_allocated()) {
        readers_[i].reset();
      }
    }
  }

  void init_readers()
  {
    for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; ++i) {
      if (readers_[i].is_allocated() && NULL == readers_[i].block_) {
        readers_[i].block_ = writer_;
      }
    }
  }

  void dealloc()
  {
    writer_ = NULL;
    dealloc_all_readers();
  }

  void destroy()
  {
    dealloc();
    size_ = 0;
    water_mark_ = 0;
  }

  int64_t size_;

  /**
   * Determines when to stop writing or reading. The watermark is the
   * level to which the producer (filler) is required to fill the buffer
   * before it can expect the reader to consume any data.  A watermark
   * of zero means that the reader will consume any amount of data,
   * no matter how small.
   */
  int64_t water_mark_;

  common::ObPtr<ObIOBufferBlock> writer_;
  ObIOBufferReader readers_[MAX_MIOBUFFER_READERS];

#ifdef TRACK_BUFFER_USER
  const char *location_;
#endif
};

/**
 * A wrapper for either a reader or a writer of an ObMIOBuffer.
 */
struct ObMIOBufferAccessor
{
  ObMIOBufferAccessor()
      : mbuf_(NULL), entry_(NULL)
  { }

  ~ObMIOBufferAccessor() { }

  ObIOBufferReader *reader() { return entry_; }

  ObMIOBuffer *writer() { return mbuf_; }

  int64_t get_block_size() const { return mbuf_->get_block_size(); }

  void reader_for(ObIOBufferReader *reader);
  void reader_for(ObMIOBuffer *buf);
  void writer_for(ObMIOBuffer *buf);

  void destroy()
  {
    mbuf_ = NULL;
    entry_ = NULL;
  }

private:
  ObMIOBuffer *mbuf_;
  ObIOBufferReader *entry_;

  DISALLOW_COPY_AND_ASSIGN(ObMIOBufferAccessor);
};

extern ObMIOBuffer *new_miobuffer_internal(
#ifdef TRACK_BUFFER_USER
    const char *loc,
#endif
    const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

#ifdef TRACK_BUFFER_USER
class ObMIOBufferTracker
{
public:
  explicit ObMIOBufferTracker(const char *loc) : loc_(loc) { }

  ~ObMIOBufferTracker() { }

  ObMIOBuffer *operator ()(const int64_t size = DEFAULT_LARGE_BUFFER_SIZE)
  {
    return new_miobuffer_internal(loc_, size);
  }

private:
  const char *loc_;

  DISALLOW_COPY_AND_ASSIGN(ObMIOBufferTracker);
};
#endif

extern ObMIOBuffer *new_empty_miobuffer_internal(
#ifdef TRACK_BUFFER_USER
    const char *loc,
#endif
    const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

#ifdef TRACK_BUFFER_USER
class ObEmptyMIOBufferTracker
{
public:
  explicit ObEmptyMIOBufferTracker(const char *loc) : loc_(loc) { }

  ~ObEmptyMIOBufferTracker() { }

  ObMIOBuffer *operator ()(const int64_t size = DEFAULT_LARGE_BUFFER_SIZE)
  {
    return new_empty_miobuffer_internal(loc_, size);
  }

private:
  const char *loc_;

  DISALLOW_COPY_AND_ASSIGN(ObEmptyMIOBufferTracker);
};
#endif

// ObMIOBuffer allocator/deallocator
#ifdef TRACK_BUFFER_USER
#define new_miobuffer               ObMIOBufferTracker(RES_PATH("memory/ObIOBuffer/"))
#define new_empty_miobuffer         ObEmptyMIOBufferTracker(RES_PATH("memory/ObIOBuffer/"))
#else
#define new_miobuffer               new_miobuffer_internal
#define new_empty_miobuffer         new_empty_miobuffer_internal
#endif
extern void free_miobuffer(ObMIOBuffer *mio);

extern ObIOBufferBlock *new_iobufferblock_internal(
#ifdef TRACK_BUFFER_USER
    const char *loc
#endif
    );

extern ObIOBufferBlock *new_iobufferblock_internal(
#ifdef TRACK_BUFFER_USER
    const char *loc,
#endif
    ObIOBufferData *d, const int64_t len = 0, const int64_t offset = 0);

#ifdef TRACK_BUFFER_USER
class ObIOBufferBlockTracker
{
public:
  ObIOBufferBlockTracker(const char *loc) : loc_(loc) { }

  ~ObIOBufferBlockTracker() { }

  ObIOBufferBlock *operator ()()
  {
    return new_iobufferblock_internal(loc_);
  }

  ObIOBufferBlock *operator ()(ObIOBufferData *d, const int64_t len = 0,
                               const int64_t offset = 0)
  {
    return new_iobufferblock_internal(loc_, d, len, offset);
  }

private:
  const char *loc_;

  DISALLOW_COPY_AND_ASSIGN(ObIOBufferBlockTracker);
};
#endif

// ObIOBufferBlock allocator
#ifdef TRACK_BUFFER_USER
#define new_iobufferblock  ObIOBufferBlockTracker(RES_PATH("memory/ObIOBuffer/"))
#else
#define new_iobufferblock  new_iobufferblock_internal
#endif

extern ObIOBufferData *new_iobufferdata_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

extern ObIOBufferData *new_iobufferdata_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    void *b, const int64_t size = DEFAULT_LARGE_BUFFER_SIZE);

#ifdef TRACK_BUFFER_USER
class ObIOBufferDataTracker
{
public:
  ObIOBufferDataTracker(const char *loc) : loc_(loc)
  { }

  ~ObIOBufferDataTracker() { }

  ObIOBufferData *operator ()(const int64_t size = DEFAULT_LARGE_BUFFER_SIZE)
  {
    return new_iobufferdata_internal(loc_, size);
  }

private:
  const char *loc_;

  DISALLOW_COPY_AND_ASSIGN(ObIOBufferDataTracker);
};
#endif

#ifdef TRACK_BUFFER_USER
#define new_iobufferdata ObIOBufferDataTracker(RES_PATH("memory/ObIOBuffer/"))
#else
#define new_iobufferdata new_iobufferdata_internal
#endif

#ifdef TRACK_BUFFER_USER
inline int iobuffer_mem_common(const char *loc, int64_t size)
{
  int ret = common::OB_SUCCESS;
  UNUSED(loc);
  UNUSED(size);
#ifdef OB_HAS_MEMORY_TRACKER
  if (size >= -DEFAULT_MAX_BUFFER_SIZE && size <= DEFAULT_MAX_BUFFER_SIZE && 0 != size) {
    if (OB_ISNULL(loc)) {
      loc = "memory/IOBuffer/UNKNOWN-LOCATION";
    }
    if (OB_FAIL(ObMemoryResourceTracker::increment(loc, size))) {
      PROXY_EVENT_LOG(WARN, "fail to increment", K(loc), K(size), K(ret));
    }
  } else {
    ret = common::OB_ERROR_OUT_OF_RANGE;
    PROXY_EVENT_LOG(WARN, "size is out of range", K(loc), K(size), K(ret));
  }
#endif //OB_HAS_MEMORY_TRACKER
  return ret;
}

inline int iobuffer_mem_inc(const char *loc, int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(iobuffer_mem_common(loc, size))) {
    PROXY_EVENT_LOG(WARN, "fail to iobuffer_mem_inc", K(loc), K(size), K(ret));
  }
  return ret;
}

inline int iobuffer_mem_dec(const char *loc, int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(iobuffer_mem_common(loc, -size))) {
    PROXY_EVENT_LOG(WARN, "fail to iobuffer_mem_dec", K(loc), K(size), K(ret));
  }
  return ret;
}
#endif

// class ObIOBufferData
inline ObIOBufferData *new_iobufferdata_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    void *b, int64_t size)
{
  ObIOBufferData *data = NULL;
  if (OB_ISNULL(b) || OB_UNLIKELY(size <= 0)) {
    PROXY_EVENT_LOG(WARN, "invalid argument", K(b), K(size));
  } else {
    data = op_thread_alloc(ObIOBufferData, get_io_data_allocator(), NULL);
    if (OB_ISNULL(data)) {
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData");
    } else {
      data->size_ = size;
      data->data_ = reinterpret_cast<char *>(b);
      data->mem_type_ = CONSTANT; //other alloced, we do not dealloc it

#ifdef TRACK_BUFFER_USER
      data->location_ = location;
#endif
    }
  }
  return data;
}

inline ObIOBufferData *new_iobufferdata_internal(
#ifdef TRACK_BUFFER_USER
    const char *loc,
#endif
    int64_t size)
{
  ObIOBufferData *data = NULL;
  if (OB_UNLIKELY(size <= 0)) {
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size));
  } else {
    data = op_thread_alloc(ObIOBufferData, get_io_data_allocator(), NULL);
    if (OB_ISNULL(data)) {
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData");
    } else {
#ifdef TRACK_BUFFER_USER
      data->location_ = loc;
#endif

      if (common::OB_SUCCESS != data->alloc(size)) {
        PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData");
        op_thread_free(ObIOBufferData, data, get_io_data_allocator());
        data = NULL;
      }
    }
  }
  return data;
}

// IRIX has a compiler bug which prevents this function
// from being compiled correctly at -O3
// so it is DUPLICATED in IOBuffer.cc
inline int ObIOBufferData::alloc(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size), K(ret));
  } else {
    if (NULL != data_) {
      dealloc();
    }

    size_ = size;
    mem_type_ = DEFAULT_ALLOC;

#ifdef TRACK_BUFFER_USER
    iobuffer_mem_inc(location_, size_);
#endif
    data_ = static_cast<char *>(op_fixed_mem_alloc(size_));
    if (OB_ISNULL(data_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData", K(ret));
      size_ = 0;
      mem_type_ = NO_ALLOC;
    }
  }
  return ret;
}

inline void ObIOBufferData::dealloc()
{
  if (DEFAULT_ALLOC == mem_type_ && size_ > 0) {
#ifdef TRACK_BUFFER_USER
    iobuffer_mem_dec(location_, size_);
#endif
    op_fixed_mem_free(data_, size_);
  }

  data_ = NULL;
  size_ = 0;
  mem_type_ = NO_ALLOC;
}

inline void ObIOBufferData::free()
{
  dealloc();
  op_thread_free(ObIOBufferData, this, get_io_data_allocator());
}

// class ObIOBufferBlock -- functions definitions
inline ObIOBufferBlock *new_iobufferblock_internal(
#ifdef TRACK_BUFFER_USER
    const char *location
#endif
    )
{
  ObIOBufferBlock *b = op_thread_alloc(ObIOBufferBlock, get_io_block_allocator(), NULL);
  if (OB_ISNULL(b)) {
    PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferBlock");
  } else {
#ifdef TRACK_BUFFER_USER
    b->location_ = location;
#endif
  }
  return b;
}

inline ObIOBufferBlock *new_iobufferblock_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    ObIOBufferData *d, const int64_t len, const int64_t offset)
{
  ObIOBufferBlock *b = NULL;
  if (OB_ISNULL(d) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(offset < 0)
      || len > d->get_block_size()) {
    PROXY_EVENT_LOG(WARN, "invalid argument", K(d), K(len), K(offset));
  } else {
    b = op_thread_alloc(ObIOBufferBlock, get_io_block_allocator(), NULL);
    if (OB_ISNULL(b)) {
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferBlock");
    } else {
#ifdef TRACK_BUFFER_USER
      b->location_ = location;
#endif
      b->set(d, len, offset);
    }
  }
  return b;
}

inline ObIOBufferBlock::ObIOBufferBlock() :
    start_(NULL),
    end_(NULL),
    buf_end_(NULL)
#ifdef TRACK_BUFFER_USER
    , location_(NULL)
#endif
{ }

inline int ObIOBufferBlock::consume(const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(end_ - start_ >= len)) {
    start_ += len;
  } else {
    ret = common::OB_SIZE_OVERFLOW;
    PROXY_EVENT_LOG(WARN, "consume length overflow",
                    "actual_size", end_ - start_, K(len), K(ret));
  }
  return ret;
}

inline int ObIOBufferBlock::fill(const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(buf_end_ - end_ >= len)) {
    end_ += len;
  } else {
    ret = common::OB_SIZE_OVERFLOW;
    PROXY_EVENT_LOG(WARN, "fill length overflow",
                    "remain_size", buf_end_ - end_, K(len), K(ret));
  }
  return ret;
}

inline int64_t ObIOBufferBlock::trim(const int64_t len)
{
  // min(len, end_ - start_)
  int64_t trim_len = end_ - start_ > len ? len : end_ - start_;
  end_ -= trim_len;
  return trim_len;
}

inline void ObIOBufferBlock::reset()
{
  start_ = buf();
  end_ = start_;
  buf_end_ = start_ + data_->get_block_size();
}

inline int ObIOBufferBlock::alloc(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size), K(ret));
  } else {
#ifdef TRACK_BUFFER_USER
    data_ = new_iobufferdata_internal(location_, size);
#else
    data_ = new_iobufferdata_internal(size);
#endif
    if (OB_ISNULL(data_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData", K(ret));
    } else {
      reset();
    }
  }
  return ret;
}

inline void ObIOBufferBlock::destroy()
{
  ObIOBufferBlock *p = next_;
  ObIOBufferBlock *next = NULL;
  data_ = NULL;

  while (NULL != p && p->refcount_dec() <= 0) {
    next = p->next_.ptr_;
    p->next_.ptr_ = NULL;
    p->free();
    p = next;
  }

  next_.ptr_ = NULL;
  buf_end_ = NULL;
  end_ = NULL;
  start_ = NULL;
}

inline ObIOBufferBlock *ObIOBufferBlock::clone()
{
#ifdef TRACK_BUFFER_USER
  ObIOBufferBlock *block = new_iobufferblock_internal(location_);
#else
  ObIOBufferBlock *block = new_iobufferblock_internal();
#endif

  if (OB_ISNULL(block)) {
    PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferBlock");
  } else {
    block->data_ = data_;
    block->start_ = start_;
    block->end_ = end_;
    block->buf_end_ = end_;
#ifdef TRACK_BUFFER_USER
    block->location_ = location_;
#endif
  }
  return block;
}

inline void ObIOBufferBlock::dealloc()
{
  destroy();
}

inline void ObIOBufferBlock::free()
{
  dealloc();
  op_thread_free(ObIOBufferBlock, this, get_io_block_allocator());
}

inline int ObIOBufferBlock::set_internal(void *b, const int64_t len)
{
  int ret = common::OB_SUCCESS;
#ifdef TRACK_BUFFER_USER
  data_ = new_iobufferdata_internal(location_, b, len);
#else
  data_ = new_iobufferdata_internal(b, len);
#endif
  if (OB_ISNULL(data_)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData", K(ret));
  } else {
    reset();
    end_ = start_ + len;
  }
  return ret;
}

inline void ObIOBufferBlock::set(ObIOBufferData *d, const int64_t len, const int64_t offset)
{
  data_ = d;
  start_ = buf() + offset;
  end_ = start_ + len;
  buf_end_ = buf() + d->get_block_size();
}

inline int ObIOBufferBlock::realloc_set_internal(void *b, const int64_t buf_size)
{
  int ret = common::OB_SUCCESS;
  int64_t data_size = size();
  if (OB_UNLIKELY(data_size > buf_size) || OB_ISNULL(b) || OB_UNLIKELY(buf_size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(data_size), K(buf_size), K(b), K(ret));
  } else {
    MEMCPY(b, start_, data_size);
    dealloc();
    if (OB_SUCC(set_internal(b, buf_size))) {
      end_ = start_ + data_size;
    }
  }
  return ret;
}

inline int ObIOBufferBlock::realloc(void *b, const int64_t buf_size)
{
  return realloc_set_internal(b, buf_size);
}

inline int ObIOBufferBlock::realloc(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  void *b = NULL;
  if (OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size), K(ret));
  } else if (size != data_->size_) {
    if (OB_ISNULL((b = op_fixed_mem_alloc(size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObIOBufferData data", K(ret));
    } else if (OB_FAIL(realloc_set_internal(b, size))) {
      op_fixed_mem_free(b, size);
      b = NULL;
      PROXY_EVENT_LOG(WARN, "fail to realloc_set_internal", K(size), K(ret));
    } else {
      data_->mem_type_ = DEFAULT_ALLOC;
    }
  }
  return ret;
}

// class ObIOBufferReader -- functions definitions
inline void ObIOBufferReader::skip_empty_blocks()
{
  while (NULL != block_->next_ && block_->next_->read_avail() > 0
         && start_offset_ >= block_->size()) {
    start_offset_ -= block_->size();
    block_ = block_->next_;
  }
}

inline bool ObIOBufferReader::is_low_water() const
{
  return mbuf_->is_low_water();
}

inline bool ObIOBufferReader::is_high_water() const
{
  return read_avail() > mbuf_->water_mark_;
}

inline bool ObIOBufferReader::is_current_low_water() const
{
  return mbuf_->is_current_low_water();
}

inline ObIOBufferBlock *ObIOBufferReader::get_current_block()
{
  return block_;
}

inline char *ObIOBufferReader::start()
{
  char *ret = NULL;
  if (OB_LIKELY(NULL != block_)) {
    skip_empty_blocks();
    ret = block_->start() + start_offset_;
  }
  return ret;
}

inline char *ObIOBufferReader::end()
{
  char *ret = NULL;
  if (OB_LIKELY(NULL != block_)) {
    skip_empty_blocks();
    ret = block_->end();
  }
  return ret;
}

inline int64_t ObIOBufferReader::block_read_avail()
{
  int64_t ret = 0;
  if (OB_LIKELY(NULL != block_)) {
    skip_empty_blocks();
    ret = static_cast<int64_t>(block_->end() - (block_->start() + start_offset_));
  }
  return ret;
}

inline int64_t ObIOBufferReader::get_block_count() const
{
  int64_t count = 0;
  ObIOBufferBlock *b = block_;

  while (NULL != b) {
    ++count;
    b = b->next_;
  }

  return count;
}

inline int64_t ObIOBufferReader::read_avail() const
{
  int64_t ret = 0;
  ObIOBufferBlock *b = block_;

  while (NULL != b) {
    ret += b->read_avail();
    b = b->next_;
  }

  ret -= start_offset_;
  ret -= reserved_size_;

  return ret;
}

inline bool ObIOBufferReader::is_read_avail_more_than(const int64_t size) const
{
  int64_t total = -start_offset_;
  ObIOBufferBlock *b = block_;
  bool ret = false;

  while (NULL != b && !ret) {
    total += b->read_avail();
    if (total - reserved_size_ > size) {
      ret = true;
    } else {
      b = b->next_;
    }
  }

  return ret;
}

inline int ObIOBufferReader::consume_internal(const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL != block_)) {
    int64_t r = block_->read_avail();
    start_offset_ += len;
    while (r <= start_offset_ && NULL != block_->next_ && block_->next_->read_avail() > 0) {
      start_offset_ -= r;
      block_ = block_->next_;
      r = block_->read_avail();
    }
  }
  return ret;
}

inline int ObIOBufferReader::consume(const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(read_avail() < len) || OB_UNLIKELY(len < 0)) {
    ret = common::OB_SIZE_OVERFLOW;
    PROXY_EVENT_LOG(WARN, "consume length is overflow", "read_avail",
                    read_avail(), K(len), K(ret));
  } else {
    consume_internal(len);
  }
  return ret;
}

inline int ObIOBufferReader::consume_all()
{
  return consume_internal(read_avail());
}

inline bool ObIOBufferReader::is_in_single_buffer_block()
{
  return (read_avail() == block_read_avail());
}

inline void ObIOBufferReader::destroy()
{
  accessor_ = NULL;
  block_ = NULL;
  mbuf_ = NULL;
  start_offset_ = 0;
  reserved_size_ = 0;
}

inline void ObIOBufferReader::reset()
{
  block_ = mbuf_->writer_;
  start_offset_ = 0;
  reserved_size_ = 0;
}

// This constructor accepts a pre-allocated memory buffer,
// wraps if in a ObIOBufferData and ObIOBufferBlock structures
// and sets it as the current block.
// NOTE that in this case the memory buffer will not be freed
// by the ObMIOBuffer class. It is the user responsibility to
// free the memory buffer. The wrappers (ObIOBufferBlock and
// ObIOBufferData) will be freed by this class.
inline ObMIOBuffer::ObMIOBuffer(void *b, const int64_t buf_size, const int64_t water_mark)
{
#ifdef TRACK_BUFFER_USER
  location_ = NULL;
#endif
  set(b, buf_size);
  water_mark_ = water_mark;
  size_ = buf_size;
}

inline ObMIOBuffer::ObMIOBuffer(const int64_t default_size)
{
  destroy();
  size_ = default_size;
#ifdef TRACK_BUFFER_USER
  location_ = NULL;
#endif
}

inline ObMIOBuffer::ObMIOBuffer()
{
  destroy();
#ifdef TRACK_BUFFER_USER
  location_ = NULL;
#endif
}

inline ObMIOBuffer *new_miobuffer_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    const int64_t size)
{
  ObMIOBuffer *b = op_thread_alloc(ObMIOBuffer, get_mio_allocator(), NULL);
  if (OB_ISNULL(b)) {
    PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObMIOBuffer");
  } else {
#ifdef TRACK_BUFFER_USER
    b->location_ = location;
#endif
    b->alloc(size);
  }
  return b;
}

inline void free_miobuffer(ObMIOBuffer *mio)
{
  if (OB_LIKELY(NULL != mio)) {
    mio->dealloc();
    op_thread_free(ObMIOBuffer, mio, get_mio_allocator());
  }
}

inline ObMIOBuffer *new_empty_miobuffer_internal(
#ifdef TRACK_BUFFER_USER
    const char *location,
#endif
    const int64_t size)
{
  ObMIOBuffer *b = op_thread_alloc(ObMIOBuffer, get_mio_allocator(), NULL);
  if (OB_ISNULL(b)) {
    PROXY_EVENT_LOG(ERROR, "fail to allocate memory for ObMIOBuffer");
  } else {
    b->size_ = size;
#ifdef TRACK_BUFFER_USER
    b->location_ = location;
#endif
  }
  return b;
}

inline ObIOBufferReader *ObMIOBuffer::alloc_accessor(ObMIOBufferAccessor *accessor)
{
  ObIOBufferReader *ret = NULL;
  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS && NULL == ret; ++i) {
    if (!readers_[i].is_allocated()) {
     ret = &readers_[i];
    }
  }

  if (OB_LIKELY(NULL != ret)) {
    ret->mbuf_ = this;
    ret->reset();
    ret->accessor_ = accessor;
  }
  return ret;
}

inline ObIOBufferReader *ObMIOBuffer::alloc_reader()
{
  ObIOBufferReader *ret = NULL;
  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS && NULL == ret; ++i) {
    if (!readers_[i].is_allocated()) {
      ret = &readers_[i];
    }
  }

  if (OB_LIKELY(NULL != ret)) {
    ret->mbuf_ = this;
    ret->reset();
    ret->accessor_ = NULL;
  }
  return ret;
}

inline ObIOBufferReader *ObMIOBuffer::clone_reader(ObIOBufferReader *r)
{
  ObIOBufferReader *ret = NULL;

  if (OB_LIKELY(NULL != r)) {
    for (int64_t i = 0; i < MAX_MIOBUFFER_READERS && NULL == ret; ++i) {
      if (!readers_[i].is_allocated()) {
        ret = &readers_[i];
      }
    }

    if (OB_LIKELY(NULL != ret)) {
      ret->mbuf_ = this;
      ret->accessor_ = NULL;
      ret->block_ = r->block_;
      ret->start_offset_ = r->start_offset_;
      ret->reserved_size_ = r->reserved_size_;
    }
  }

  return ret;
}

inline int64_t ObMIOBuffer::block_write_avail()
{
  ObIOBufferBlock *b = first_write_block();
  return NULL != b ? b->write_avail() : 0;
}

// Appends a block to writer->next and make it the current
// block.
// Note that the block is not appended to the end of the list.
// That means that if writer->next was not null before this
// call then the block that writer->next was pointing to will
// have its reference count decremented and writer->next
// will have a new value which is the new block.
// In any case the new appended block becomes the current
// block.
inline int ObMIOBuffer::append_block_internal(ObIOBufferBlock *b)
{
  int ret = common::OB_SUCCESS;
  // It would be nice to remove an empty buffer at the beginning,
  // but this breaks the process.
  // if (!writer_ || !writer_->read_avail())
  if (NULL == writer_) {
    writer_ = b;
    init_readers();
  } else if(NULL != writer_->next_ && OB_UNLIKELY(0 != writer_->next_->read_avail())) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_EVENT_LOG(ERROR, "append_block_internal, invalid miobuffer writer",
                    "read_avail", writer_->next_->read_avail(), K(ret));
  } else {
    writer_->next_ = b;
    while (NULL != b && b->read_avail() > 0) {
      writer_ = b;
      b = b->next_;
    }
  }

  if (OB_SUCC(ret)) {
    while (NULL != writer_->next_ && 0 == writer_->write_avail()
           && writer_->next_->read_avail() > 0) {
      writer_ = writer_->next_;
    }
  }
  return ret;
}

inline int ObMIOBuffer::append_block(ObIOBufferBlock *b)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(b)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(b), K(ret));
  } else if (OB_UNLIKELY(b->read_avail() <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument, block read avail must be greater than 0",
                    "read_avail", b->read_avail(), K(ret));
  } else if (OB_FAIL(append_block_internal(b))) {
    PROXY_EVENT_LOG(WARN, "failed to append block internal", K(b), K(ret));
  }
  return ret;
}

// Allocate a block, appends it to current->next
// and make the new block the current block (writer).
inline int ObMIOBuffer::append_block(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size), K(ret));
  } else {
#ifdef TRACK_BUFFER_USER
    ObIOBufferBlock *b = new_iobufferblock_internal(location_);
#else
    ObIOBufferBlock *b = new_iobufferblock_internal();
#endif

    if (OB_ISNULL(b)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "failed to allocate iobuffer block", K(b), K(ret));
    } else if (OB_FAIL(b->alloc(size))) {
      b->free();
      PROXY_EVENT_LOG(WARN, "failed to allocate iobuffer data", K(b), K(size), K(ret));
    } else if (OB_FAIL(append_block_internal(b))) {
      b->free();
      PROXY_EVENT_LOG(WARN, "failed to append block internal", K(b), K(ret));
    }
  }
  return ret;
}

inline int ObMIOBuffer::add_block()
{
  return append_block(size_);
}

inline int ObMIOBuffer::add_block(const int64_t block_count)
{
  int ret = common::OB_SUCCESS;
  ObIOBufferBlock *b = NULL;
  ObIOBufferBlock *head_block = NULL;

  if (OB_UNLIKELY(block_count <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(block_count), K(ret));
  } else {
    for (int64_t i = block_count; (i > 0) && OB_SUCC(ret); --i) {
#ifdef TRACK_BUFFER_USER
      b = new_iobufferblock_internal(location_);
#else
      b = new_iobufferblock_internal();
#endif
      if (OB_ISNULL(b)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_EVENT_LOG(ERROR, "failed to allocate iobuffer block", K(b), K(i), K(ret));
      } else if (OB_FAIL(b->alloc(size_))) {
        PROXY_EVENT_LOG(WARN, "failed to allocate iobuffer data", K(b), K(size_), K(i), K(ret));
      } else {
        b->next_ = head_block;
        head_block = b;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_block_internal(head_block))) {
        PROXY_EVENT_LOG(WARN, "failed to append block internal", K(head_block), K(ret));
      }
    }
    if (OB_FAIL(ret) && NULL != head_block) {
      head_block->free();
      head_block = NULL;
    }
  }
  return ret;
}

inline int ObMIOBuffer::check_add_block()
{
  int ret = common::OB_SUCCESS;
  if (!is_high_water() && is_current_low_water()) {
    ret = add_block();
  }
  return ret;
}

inline int ObMIOBuffer::check_add_block(const int64_t total_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(total_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(total_size), K(ret));
  } else {
    if (!is_high_water() && is_current_low_water()) {

      int64_t add_block_count = (water_mark_ - max_read_avail() - current_write_avail()) / size_ + 1;
      int64_t need_add_block_count = total_size / size_ + 1;

      if (add_block_count > need_add_block_count) {
        add_block_count = need_add_block_count;
      }

      if (add_block_count > 0) {
        if (OB_FAIL(add_block(add_block_count))) {
          PROXY_EVENT_LOG(WARN, "fail to add_block", K(add_block_count), K(ret));
        }
      }
    }
  }
  return ret;
}

inline ObIOBufferBlock *ObMIOBuffer::get_current_block()
{
  return first_write_block();
}

// returns the total space available in all blocks.
// This function is different than write_avail() because
// it will not append a new block if there is no space
// or below the watermark space available.
inline int64_t ObMIOBuffer::current_write_avail() const
{
  int64_t ret = 0;
  ObIOBufferBlock *b = writer_;
  while (NULL != b) {
    ret += b->write_avail();
    b = b->next_;
  }
  return ret;
}

// returns the number of bytes available in the current block.
// If there is no current block or not enough free space in
// the current block then a new block is appended.
inline int64_t ObMIOBuffer::write_avail()
{
  int64_t ret = 0;
  if (OB_LIKELY(common::OB_SUCCESS == check_add_block())) {
    ret = current_write_avail();
  }
  return ret;
}

inline int64_t ObMIOBuffer::write_avail(const int64_t total_size)
{
  int64_t ret = 0;
  if (OB_LIKELY(common::OB_SUCCESS == check_add_block(total_size))) {
    ret = current_write_avail();
  }
  return ret;
}

inline int ObMIOBuffer::fill(const int64_t len)
{
  int ret = common::OB_SUCCESS;
  int64_t tofill_len = writer_->write_avail();
  int64_t remain_len = len;

  while (tofill_len < remain_len && OB_SUCC(ret)) {
    if (OB_SUCC(writer_->fill(tofill_len))) {
      remain_len -= tofill_len;
      if (remain_len > 0) {
        writer_ = writer_->next_;
      }

      tofill_len = writer_->write_avail();
    }
  }

  if (OB_SUCC(ret)) {
    ret = writer_->fill(remain_len);
  }
  return ret;
}

inline int ObMIOBuffer::trim(ObIOBufferReader &buf_reader, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  int64_t remain_len = len;
  int64_t trimed_len = 0;
  ObIOBufferBlock *cur_block = writer_;

  while (remain_len > 0 && NULL != cur_block) {
    trimed_len = cur_block->trim(remain_len);
    remain_len -= trimed_len;
    if (remain_len > 0) {
      cur_block = get_prev_block(buf_reader, cur_block);
    }
  }

  // not trim all
  if (remain_len > 0 || NULL == cur_block) {
    ret = common::OB_BUF_NOT_ENOUGH;
  } else {
    writer_ = cur_block;
  }
  return ret;
}

inline ObIOBufferBlock *ObMIOBuffer::get_prev_block(ObIOBufferReader &buf_reader,
                                                    ObIOBufferBlock *cur_block)
{
  ObIOBufferBlock *prev_block = NULL;
  if (NULL == cur_block) {
    // do nothing
    prev_block = NULL;
  } else {
    // find beginning block from reader
    ObIOBufferBlock *block = buf_reader.get_current_block();
    while (NULL != block && NULL == prev_block) {
      if (cur_block == block->next_) {
        prev_block = block;
      } else {
        block = block->next_;
      }
    }
  }
  return prev_block;
}

inline int64_t ObMIOBuffer::max_block_count() const
{
  int64_t ret = 0;
  int64_t count = 0;

  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; ++i) {
    if (readers_[i].is_allocated()) {
      count = readers_[i].get_block_count();
      if (count > ret) {
        ret = count;
      }
    }
  }

  return ret;
}

inline int64_t ObMIOBuffer::max_read_avail() const
{
  int64_t ret = 0;
  int64_t read_avail = 0;
  bool found = false;

  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; ++i) {
    if (readers_[i].is_allocated()) {
      read_avail = readers_[i].read_avail();
      if (read_avail > ret) {
        ret = read_avail;
      }
      found = true;
    }
  }

  if (!found && NULL != writer_) {
    ret = writer_->read_avail();
  }

  return ret;
}

inline int ObMIOBuffer::set(void *b, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(b) || OB_UNLIKELY(len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(b), K(len), K(ret));
  } else {
#ifdef TRACK_BUFFER_USER
    writer_ = new_iobufferblock_internal(location_);
#else
    writer_ = new_iobufferblock_internal();
#endif
    if (OB_ISNULL(writer_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "failed to allocate iobuffer block", K(b), K(ret));
    } else if (OB_FAIL(writer_->set_internal(b, len))) {
      writer_ = NULL; // free writer block
      PROXY_EVENT_LOG(WARN, "failed to set internal block", K(b), K(ret));
    } else {
      init_readers();
    }
  }
  return ret;
}

inline int ObMIOBuffer::append_allocated(void *b, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(b) || OB_UNLIKELY(len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(b), K(len), K(ret));
  } else {
#ifdef TRACK_BUFFER_USER
    ObIOBufferBlock *block = new_iobufferblock_internal(location_);
#else
    ObIOBufferBlock *block = new_iobufferblock_internal();
#endif

    if (OB_ISNULL(block)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "failed to allocate iobuffer block", K(b), K(ret));
    } else if (OB_FAIL(block->set_internal(b, len))) {
      block->free();
      block = NULL;
      PROXY_EVENT_LOG(WARN, "failed to set internal block", K(b), K(ret));
    } else if (OB_FAIL(append_block_internal(block))) {
      block->free();
      block = NULL;
      PROXY_EVENT_LOG(WARN, "failed to append block internal", K(b), K(ret));
    }
  }
  return ret;
}

inline int ObMIOBuffer::alloc(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WARN, "invalid argument", K(size), K(ret));
  } else {
#ifdef TRACK_BUFFER_USER
    writer_ = new_iobufferblock_internal(location_);
#else
    writer_ = new_iobufferblock_internal();
#endif
    if (OB_ISNULL(writer_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_EVENT_LOG(ERROR, "failed to allocate iobuffer block", K(ret));
    } else if (OB_FAIL(writer_->alloc(size))) {
      writer_ = NULL; // free writer block
      PROXY_EVENT_LOG(WARN, "failed to set internal block", K(size), K(ret));
    } else {
      size_ = size;
      init_readers();
    }
  }
  return ret;
}

inline void ObMIOBuffer::dealloc_reader(ObIOBufferReader *e)
{
  if (NULL != e->accessor_) {
    if (OB_UNLIKELY(e->accessor_->writer() != this) || OB_UNLIKELY(e->accessor_->reader() != e)) {
      PROXY_EVENT_LOG(ERROR, "accessor writer must be this, accessor reader must be e",
                      "accessor_writer", e->accessor_->writer(),
                      K(this), "accessor_reader", e->accessor_->reader(), K(e));
    }
    e->accessor_->destroy();
  }

  e->destroy();
}

inline ObIOBufferReader *ObIOBufferReader::clone()
{
  return mbuf_->clone_reader(this);
}

inline void ObIOBufferReader::dealloc()
{
  mbuf_->dealloc_reader(this);
}

inline void ObMIOBuffer::dealloc_all_readers()
{
  for (int64_t i = 0; i < MAX_MIOBUFFER_READERS; ++i) {
    if (readers_[i].is_allocated()) {
      dealloc_reader(&readers_[i]);
    }
  }
}

inline void ObMIOBufferAccessor::reader_for(ObMIOBuffer *buf)
{
  mbuf_ = buf;
  if (NULL != buf) {
    entry_ = mbuf_->alloc_accessor(this);
  } else {
    entry_ = NULL;
  }
}

inline void ObMIOBufferAccessor::reader_for(ObIOBufferReader *reader)
{
  if (entry_ != reader) {
    mbuf_ = reader->mbuf_;
    entry_ = reader;
  }
}

inline void ObMIOBufferAccessor::writer_for(ObMIOBuffer *buf)
{
  mbuf_ = buf;
  entry_ = NULL;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_IOBUFFER_H

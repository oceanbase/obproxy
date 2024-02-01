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
 */

#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{

int ObMIOBuffer::remove_append(ObIOBufferReader *r, int64_t &append_len)
{
  int ret = OB_SUCCESS;
  ObIOBufferBlock *b = NULL;
  append_len = 0;

  while (NULL != r->block_ && OB_SUCC(ret)) {
    b = r->block_;
    b->start_ += r->start_offset_;
    if (b->start_ >= b->end_) {
      r->start_offset_ = b->start_ - b->end_; // equal to offset - (end - start)
      r->block_ = r->block_->next_;
    } else {
      if (OB_FAIL(append_block(b))) {
        PROXY_EVENT_LOG(WDIAG, "failed to append block", K(ret));
      } else {
        r->start_offset_ = 0;
        append_len += r->read_avail();
        r->block_ = NULL;
      }
    }
  }

  if (OB_SUCC(ret)) {
    r->mbuf_->writer_ = NULL;
  }
  return ret;
}

int ObMIOBuffer::write(const char *src_buf, const int64_t towrite_len, int64_t &written_len)
{
  int ret = OB_SUCCESS;
  const char *buf = src_buf;
  int64_t remain_len = towrite_len;
  int64_t tofill_len = 0;
  written_len = 0;

  if (OB_ISNULL(src_buf) || OB_UNLIKELY(towrite_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WDIAG, "invalid argument", K(src_buf), K(towrite_len), K(ret));
  }

  while (remain_len > 0 && OB_SUCC(ret)) {
    //size maybe 0 at first time, without have create writer_
    if (NULL == writer_ && OB_FAIL(add_block())) {
      PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
    }

    if (OB_SUCC(ret) && OB_LIKELY(NULL != writer_)) {
      tofill_len = writer_->write_avail();
      tofill_len = tofill_len < remain_len ? tofill_len : remain_len;
      if (tofill_len > 0) {
        MEMCPY(writer_->end(), buf, tofill_len);
        if (OB_FAIL(writer_->fill(tofill_len))) {
          PROXY_EVENT_LOG(WDIAG, "failed to fill iobuffer", K(tofill_len), K(ret));
        } else {
          buf += tofill_len;
          remain_len -= tofill_len;
        }
      }

      if (remain_len > 0 && OB_SUCC(ret)) {
        if (NULL == writer_->next_) {
          if (OB_FAIL(add_block())) {
            PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
          }
        } else {
          writer_ = writer_->next_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    written_len = towrite_len - remain_len;
  }

  return ret;
}

int ObMIOBuffer::get_write_avail_buf(char *&start, int64_t &len)
{
  int ret = OB_SUCCESS;
  start = NULL;
  len = 0;

  while (OB_SUCC(ret) && (0 == len)) {
    //size maybe 0 at first time, without have create writer_
    if (NULL == writer_ && OB_FAIL(add_block())) {
      PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
    }

    if (OB_SUCC(ret) && OB_LIKELY(NULL != writer_)) {
      len = writer_->write_avail();
      if (len > 0) {
        start = writer_->end();
      }
    }

    if (OB_SUCC(ret) && (0 == len)) {
      if (NULL == writer_->next_) {
        if (OB_FAIL(add_block())) {
          PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
        }
      } else {
        writer_ = writer_->next_;
      }
    }
  }

  return ret;
}

int ObMIOBuffer::reserve_successive_buf(const int64_t reserved_len)
{
  int ret = OB_SUCCESS;
  if (reserved_len <= 0 || reserved_len > get_block_size()) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WDIAG, "invalid input value", K(reserved_len), K(get_block_size()), K(ret));
  } else {
    bool found = false;
    if ((NULL == writer_) && OB_FAIL(add_block())) {
      PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
    } else {
      while ((NULL != writer_) && !found && OB_SUCC(ret)) {
        if (writer_->write_avail() >= reserved_len) {
          found = true;
        } else {
          // do not use the remain sapce, and try next IOBlock
          writer_->buf_end_ = writer_->end_;
          if (NULL == writer_->next_) {
            if (OB_FAIL(add_block())) {
              PROXY_EVENT_LOG(WDIAG, "failed to add block", K(ret));
            }
          } else {
            writer_ = writer_->next_;
          }
        }
      }
    }
  }

  return ret;
}

/**
 * Same functionality as write but for the one small difference.
 * The space available in the last block is taken from the original
 * and this space becomes available to the copy.
 */
int ObMIOBuffer::write_and_transfer_left_over_space(
    ObIOBufferReader *r, const int64_t towrite_len,
    int64_t &written_len, const int64_t start_offset)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(write(r, towrite_len, written_len, start_offset))) {
    PROXY_EVENT_LOG(WDIAG, "failed to wirte data from buffer reader",
                    K(r), K(towrite_len), K(ret));
  } else {
    // reset the end markers of the original so that it cannot
    // make use of the space in the current block
    if (NULL != r->mbuf_->writer_) {
      r->mbuf_->writer_->buf_end_ = r->mbuf_->writer_->end_;
    }

    // reset the end marker of the clone so that it can make
    // use of the space in the current block
    if (NULL != writer_) {
      writer_->buf_end_ = writer_->data_->data() + writer_->get_block_size();
    }
  }

  return ret;
}

int ObMIOBuffer::write(ObIOBufferReader *r, const int64_t towrite_len,
                       int64_t &written_len, const int64_t start_offset)
{
  int ret = OB_SUCCESS;
  int64_t remain_len = towrite_len;
  ObIOBufferBlock *b = NULL;
  int64_t offset = 0;
  int64_t max_bytes = 0;
  int64_t bytes = 0;
  ObIOBufferBlock *bb = NULL;
  written_len = 0;

  if (OB_ISNULL(r) || OB_UNLIKELY(towrite_len <= 0) || OB_UNLIKELY(start_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(WDIAG, "invalid argument", K(r), K(towrite_len), K(start_offset), K(ret));
  } else {
    b = r->block_;
    offset = start_offset + r->start_offset_;
  }

  while (NULL != b && remain_len > 0 && OB_SUCC(ret)) {
    max_bytes = b->read_avail();
    max_bytes -= offset;
    if (max_bytes <= 0) {
      offset = -max_bytes;
      b = b->next_;
    } else {
      bytes = remain_len >= max_bytes ? max_bytes : remain_len;
      bb = b->clone();
      if (OB_ISNULL(bb)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_EVENT_LOG(EDIAG, "failed to allocate memory for iobuffer block", K(ret));
      } else {
        bb->start_ += offset;
        bb->end_ = bb->start_ + bytes;
        bb->buf_end_ = bb->end_;
        if (OB_FAIL(append_block(bb))) {
          bb->free();
          PROXY_EVENT_LOG(WDIAG, "failed to append block", K(bb), K(ret));
        } else {
          offset = 0;
          remain_len -= bytes;
          b = b->next_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    written_len = towrite_len - remain_len;
  }

  return ret;
}

char *ObIOBufferReader::copy(char *dest_buf, const int64_t copy_len,
                             const int64_t start_offset)
{
  char *buf = dest_buf;
  ObIOBufferBlock *b = block_;
  int64_t remain_len = copy_len;
  int64_t offset = start_offset + start_offset_;
  int64_t max_bytes = 0;
  int64_t bytes = 0;

  if (OB_LIKELY(NULL != dest_buf) && copy_len > 0 && start_offset >= 0) {
    while (NULL != b && remain_len > 0) {
      max_bytes = b->read_avail();
      max_bytes -= offset;
      if (max_bytes <= 0) {
        offset = -max_bytes;
        b = b->next_;
      } else {
        bytes = remain_len >= max_bytes ? max_bytes : remain_len;
        MEMCPY(buf, b->start() + offset, bytes);
        buf += bytes;
        remain_len -= bytes;
        b = b->next_;
        offset = 0;
      }
    }
  }

  return buf;
}

void ObIOBufferReader::replace(const char *src_buf,
                               const int64_t replace_len,
                               const int64_t start_offset)
{
  const char *buf = src_buf;
  ObIOBufferBlock *b = block_;
  int64_t remain_len = replace_len;
  int64_t offset = start_offset + start_offset_;
  int64_t max_bytes = 0;
  int64_t bytes = 0;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(replace_len > 0) && OB_LIKELY(start_offset >= 0)) {
    while (NULL != b && remain_len > 0) {
      max_bytes = b->read_avail();
      max_bytes -= offset;
      if (max_bytes <= 0) {
        offset = -max_bytes;
        b = b->next_;
      } else {
        bytes = remain_len >= max_bytes ? max_bytes : remain_len;
        MEMCPY(b->start() + offset, buf, bytes);
        buf += bytes;
        remain_len -= bytes;
        b = b->next_;
        offset = 0;
      }
    }
  }
}

void ObIOBufferReader::replace_with_char(const char mark,
                                         const int64_t replace_len,
                                         const int64_t start_offset)
{
  ObIOBufferBlock *b = block_;
  int64_t remain_len = replace_len;
  int64_t offset = start_offset + start_offset_;
  int64_t max_bytes = 0;
  int64_t bytes = 0;
  if (OB_LIKELY(replace_len > 0) && OB_LIKELY(start_offset >= 0)) {
    while (NULL != b && remain_len > 0) {
      max_bytes = b->read_avail();
      max_bytes -= offset;
      if (max_bytes <= 0) {
        offset = -max_bytes;
        b = b->next_;
      } else {
        bytes = remain_len >= max_bytes ? max_bytes : remain_len;
        MEMSET(b->start() + offset, mark, bytes);
        remain_len -= bytes;
        b = b->next_;
        offset = 0;
      }
    }
  }
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

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

#ifndef OBPROXY_MYSQL_TRACE_STATS_H
#define OBPROXY_MYSQL_TRACE_STATS_H

#include "lib/net/ob_addr.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObTraceRecord
{
public:
  ObTraceRecord() { reset(); }
  ~ObTraceRecord() { }
  void reset()
  {
    memset(this, 0, sizeof(ObTraceRecord));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  int8_t attempts_;
  int8_t pl_attempts_;
  int8_t server_state_;
  int8_t send_action_;
  int8_t resp_error_;
  int32_t cost_time_us_;
  common::ObAddr addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTraceRecord);
};

class ObTraceBlock
{
public:
  ObTraceBlock() : next_(NULL), next_idx_(0), block_idx_(0)
  {
    memset(stats_, 0, sizeof(stats_));
  }
  ~ObTraceBlock() { }

  void reuse()
  {
    next_idx_ = 0;
  };

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  //max attempts for one transaction:
  //  max(2*replica_count + 1, connect_observer_max_retries_)-->current it is 7
  //min steps for one attempt connect:
  //  1.handshake, 2.saved login, 3.sync var/last_ii/transaction 4.use db, 5.send req
  //max steps for one attempt connect:
  //  1.handshake, 2.saved login, 3.sync var, 4.sync db, 5.sync last_insert_id, 6.start transaction, 7.send req
  //here we use the min steps for one attempt connect: 5
  //
  static const int32_t TRACE_BLOCK_ENTRIES = 5;
  ObTraceRecord stats_[TRACE_BLOCK_ENTRIES];
  ObTraceBlock *next_;
  int16_t next_idx_;
  int16_t block_idx_;

private:

  DISALLOW_COPY_AND_ASSIGN(ObTraceBlock);
};

class ObTraceStats
{
public:
  ObTraceStats() : last_trace_end_time_(0), first_block_(), current_block_(&first_block_) { }
  ~ObTraceStats() { }

  void destory();
  void mark_trace_stats_need_reuse() { last_trace_end_time_ = 0; }
  bool need_reuse_trace_stats() const { return 0 == last_trace_end_time_; }
  bool is_trace_stats_used() const { return 0 != last_trace_end_time_; }

  void reuse()
  {
    ObTraceBlock *b = &first_block_;
    while (NULL != b) {
      b->reuse();
      b = b->next_;
    }
    last_trace_end_time_ = 0;
    current_block_ = &first_block_;
  }

  int append_new_trace_block();
  int get_current_record(ObTraceRecord *&record);

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  //max connect attempts is 7
  //max record for one attempts is 7
  //TRACE_BLOCK_ENTRIES is 5
  //TRACE_STATS_ENTRIES = 7*7/5  + 1
  static const int32_t TRACE_STATS_ENTRIES = 10;
  ObHRTime last_trace_end_time_;
  ObTraceBlock first_block_;
  ObTraceBlock *current_block_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTraceStats);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_TRACE_STATS_H

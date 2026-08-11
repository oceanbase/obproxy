/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_TABLE_QUERY_ASYNC_PROCESSOR_H
#define OB_TABLE_QUERY_ASYNC_PROCESSOR_H

#include "proxy/rpc/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc/rpclib/ob_table_query_async_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObTableQueryAsyncResult
{
  ObTableQueryAsyncResult() : target_entry_(NULL) {}
  ~ObTableQueryAsyncResult() {}
  void reset() { target_entry_ = NULL; }

  TO_STRING_KV(K_(target_entry));

  ObTableQueryAsyncEntry *target_entry_;
};

class ObTableQueryAsyncParam
{
public:
  ObTableQueryAsyncParam()
    : cont_(NULL), new_async_query_(false), key_(0), result_() {}
  ~ObTableQueryAsyncParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void deep_copy(ObTableQueryAsyncParam &other);

  event::ObContinuation *cont_;
  bool new_async_query_;
  uint64_t key_;
  ObTableQueryAsyncResult result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncParam);
};

inline bool ObTableQueryAsyncParam::is_valid() const
{
  return NULL != cont_;
}

inline void ObTableQueryAsyncParam::reset()
{
  new_async_query_ = false;
  cont_ = NULL;
  result_.reset();
  key_ = 0;
}

class ObTableQueryAsyncProcessor
{
  class RpcQuerySessionIdGenerator
  {
  public:
    static uint64_t rpc_query_sessionid_generator_;
  };
public:
  ObTableQueryAsyncProcessor() {}
  ~ObTableQueryAsyncProcessor() {}

  static int get_table_query_async_entry(ObTableQueryAsyncParam &param, event::ObAction *&action);

private:
  static int get_table_query_async_entry_from_thread_cache(ObTableQueryAsyncParam &param,
                                                           ObTableQueryAsyncEntry *&entry);
  static int alloc_new_table_query_async_entry(ObTableQueryAsyncParam &param);
  static uint64_t get_new_query_session_id();
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_TABLE_QUERY_ASYNC_PROCESSOR_H
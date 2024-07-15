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

#ifndef OBPROXY_INDEX_PROCESSOR_H
#define OBPROXY_INDEX_PROCESSOR_H

#include "proxy/route/ob_index_entry.h"
#include "proxy/route/ob_index_cache.h"
#include "proxy/route/ob_table_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObIndexResult
{
  ObIndexResult() : target_entry_(NULL),  target_old_entry_(NULL), is_from_remote_(false) {}
  ~ObIndexResult() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObIndexEntry *target_entry_;
  ObIndexEntry *target_old_entry_;
  bool is_from_remote_;
};

inline void ObIndexResult::reset()
{
  is_from_remote_ = false;
  target_entry_ = NULL;
  target_old_entry_ = NULL;
}

class ObMysqlProxy;
class ObIndexParam
{
public:
  ObIndexParam()
    : cont_(NULL), name_(), name_buf_(NULL), name_buf_len_(0),
      result_(), cluster_version_(-1), cluster_id_(common::OB_INVALID_CLUSTER_ID) {}
  ~ObIndexParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void deep_copy(ObIndexParam &other);

  event::ObContinuation *cont_;
  ObIndexEntryName name_;
  char *name_buf_;
  int64_t name_buf_len_;
  ObIndexResult result_;
  int64_t cluster_version_;
  int64_t cluster_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexParam);
};

inline bool ObIndexParam::is_valid() const
{
  return (NULL != cont_)
          && (name_.is_valid())
          && cluster_version_ >= 0 && cluster_id_ >= 0;
}

inline void ObIndexParam::reset()
{
  cont_ = NULL;
  result_.reset();
  name_.reset();
  cluster_id_ = -1;
  cluster_version_ = common::OB_INVALID_CLUSTER_ID;

  if (NULL != name_buf_ && name_buf_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
}


class ObIndexProcessor
{
public:
  ObIndexProcessor() {}
  ~ObIndexProcessor() {}

  static int get_index_entry(ObIndexParam &param, event::ObAction *&action);

private:
  static int get_index_entry_from_thread_cache(ObIndexParam &param,
                                                   ObIndexEntry *&entry);
  DISALLOW_COPY_AND_ASSIGN(ObIndexProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_INDEX_PROCESSOR_H

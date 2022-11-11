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

#ifndef OBPROXY_ROUTINE_PROCESSOR_H
#define OBPROXY_ROUTINE_PROCESSOR_H

#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_routine_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObRoutineResult
{
  ObRoutineResult() : target_entry_(NULL), is_from_remote_(false) {}
  ~ObRoutineResult() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObRoutineEntry *target_entry_;
  bool is_from_remote_;
};

inline void ObRoutineResult::reset()
{
  is_from_remote_ = false;
  target_entry_ = NULL;
}

class ObMysqlProxy;
class ObRoutineParam
{
public:
  ObRoutineParam() : name_buf_(NULL), name_buf_len_(0)
  { reset(); }
  ~ObRoutineParam() { reset(); }
  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool need_fetch_from_remote() const { return force_renew_; }
  void set_table_entry(ObTableEntry *entry);
  int deep_copy(ObRoutineParam &other);

  event::ObContinuation *cont_;
  ObRoutineEntryName name_;
  char *name_buf_;
  int64_t name_buf_len_;

  bool force_renew_;
  ObMysqlProxy *mysql_proxy_;
  ObRoutineResult result_;
  int64_t cr_version_;
  int64_t cr_id_;
  int64_t cluster_version_;
  common::ObString current_idc_name_;
  char current_idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];

private:
  DISALLOW_COPY_AND_ASSIGN(ObRoutineParam);
};

inline bool ObRoutineParam::is_valid() const
{
  return (NULL != cont_)
          && (cr_version_ >= 0)
          && (cr_id_ >= 0)
          && (name_.is_valid())
          && (NULL != mysql_proxy_);
}

inline void ObRoutineParam::reset()
{
  cont_ = NULL;
  name_.reset();
  result_.reset();
  mysql_proxy_ = NULL;
  force_renew_ = false;
  cr_version_ = 0;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
  cluster_version_ = 0;
  current_idc_name_.reset();

  if (NULL != name_buf_ && name_buf_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
}

class ObRoutineProcessor
{
public:
  ObRoutineProcessor() {}
  ~ObRoutineProcessor() {}

  static int get_routine_entry(ObRoutineParam &param, event::ObAction *&action);

private:
  static int get_routine_entry_from_thread_cache(ObRoutineParam &param,
      ObRoutineEntry *&entry);
  DISALLOW_COPY_AND_ASSIGN(ObRoutineProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_ROUTINE_PROCESSOR_H

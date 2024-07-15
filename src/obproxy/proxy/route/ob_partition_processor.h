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

#ifndef OBPROXY_PARTITION_PROCESSOR_H
#define OBPROXY_PARTITION_PROCESSOR_H

#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_table_entry.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObPartitionResult
{
  ObPartitionResult() : target_entry_(NULL),  target_old_entry_(NULL), is_from_remote_(false) {}
  ~ObPartitionResult() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObPartitionEntry *target_entry_;
  ObPartitionEntry *target_old_entry_;
  bool is_from_remote_;
};

inline void ObPartitionResult::reset()
{
  is_from_remote_ = false;
  target_entry_ = NULL;
  target_old_entry_ = NULL;
}

class ObMysqlProxy;
class ObPartitionParam
{
public:
  ObPartitionParam()
    : cont_(NULL), partition_id_(common::OB_INVALID_ID),
      force_renew_(false), is_need_force_flush_(false),
      result_(), mysql_proxy_(NULL),
      current_idc_name_(), tenant_version_(0),
      cluster_version_(0), table_entry_(NULL) {}
  ~ObPartitionParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool need_fetch_from_remote() const { return force_renew_; }
  void set_table_entry(ObTableEntry *entry);
  ObTableEntry *get_table_entry() const { return table_entry_; }
  void deep_copy(ObPartitionParam &other);

  event::ObContinuation *cont_;
  uint64_t partition_id_;
  bool force_renew_;
  bool is_need_force_flush_;
  ObPartitionResult result_;
  ObMysqlProxy *mysql_proxy_;
  common::ObString current_idc_name_;
  char current_idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];
  uint64_t tenant_version_;
  int64_t cluster_version_;

private:
  ObTableEntry *table_entry_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionParam);
};

class ObBatchPartitionParam
{
public:
  ObBatchPartitionParam()
    : cont_(NULL), force_renew_(false), is_need_force_flush_(false),
      mysql_proxy_(NULL), current_idc_name_(), tenant_version_(0),
      cluster_version_(0), table_entry_(NULL) {}
  ~ObBatchPartitionParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool need_fetch_from_remote() const { return force_renew_; }
  void set_table_entry(ObTableEntry *entry);
  ObTableEntry *get_table_entry() const { return table_entry_; }
  void deep_copy(ObBatchPartitionParam &other);

  event::ObContinuation *cont_; //maybe not to use
  // uint64_t partition_id_;
  bool force_renew_;
  bool is_need_force_flush_;
  // ObPartitionResult result_;
  ObMysqlProxy *mysql_proxy_;
  common::ObString current_idc_name_;
  char current_idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];
  uint64_t tenant_version_;
  int64_t cluster_version_;

private:
  ObTableEntry *table_entry_;
  DISALLOW_COPY_AND_ASSIGN(ObBatchPartitionParam);
};

inline bool ObPartitionParam::is_valid() const
{
  return (NULL != cont_)
          && (common::OB_INVALID_ID != partition_id_)
          && (NULL != get_table_entry())
          && (NULL != mysql_proxy_);
}

inline void ObPartitionParam::reset()
{
  cont_ = NULL;
  result_.reset();
  partition_id_ = common::OB_INVALID_ID;
  mysql_proxy_ = NULL;
  force_renew_ = false;
  is_need_force_flush_ = false;
  set_table_entry(NULL);
  current_idc_name_.reset();
  tenant_version_ = 0;
  cluster_version_ = 0;
}

inline void ObPartitionParam::set_table_entry(ObTableEntry *entry)
{
  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }
  table_entry_ = entry;
  if (NULL != table_entry_) {
    table_entry_->inc_ref();
  }
}

inline bool ObBatchPartitionParam::is_valid() const
{
  return //(NULL != cont_)
          (NULL != get_table_entry())
          && (NULL != mysql_proxy_);
}

inline void ObBatchPartitionParam::reset()
{
  cont_ = NULL;
  // result_.reset();
  mysql_proxy_ = NULL;
  force_renew_ = false;
  is_need_force_flush_ = false;
  set_table_entry(NULL);
  current_idc_name_.reset();
  tenant_version_ = 0;
  cluster_version_ = 0;
}

inline void ObBatchPartitionParam::set_table_entry(ObTableEntry *entry)
{
  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }
  table_entry_ = entry;
  if (NULL != table_entry_) {
    table_entry_->inc_ref();
  }
}

class ObPartitionProcessor
{
public:
  ObPartitionProcessor() {}
  ~ObPartitionProcessor() {}

  static int get_partition_entry(ObPartitionParam &param, event::ObAction *&action);

  static int put_and_check_schedule_async_batch_fetch(ObTableEntry *entry, uint64_t partition_id);

private:
  static int get_partition_entry_from_thread_cache(ObPartitionParam &param,
                                                   ObPartitionEntry *&entry);
  DISALLOW_COPY_AND_ASSIGN(ObPartitionProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_PARTITION_PROCESSOR_H

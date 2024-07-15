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

#ifndef OBPROXY_TABLEGROUP_PROCESSOR_H
#define OBPROXY_TABLEGROUP_PROCESSOR_H

#include "proxy/rpc_optimize/rpclib/ob_tablegroup_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObTableGroupResult
{
  ObTableGroupResult() : target_entry_(NULL),  target_old_entry_(NULL), is_from_remote_(false) {}
  ~ObTableGroupResult() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObTableGroupEntry *target_entry_;
  ObTableGroupEntry *target_old_entry_;
  bool is_from_remote_;
};

inline void ObTableGroupResult::reset()
{
  is_from_remote_ = false;
  target_entry_ = NULL;
  target_old_entry_ = NULL;
}

class ObMysqlProxy;
class ObTableGroupParam
{
public:
  ObTableGroupParam()
    : cont_(NULL), tenant_id_(OB_INVALID_TENANT_ID),
      force_renew_(false), mysql_proxy_(NULL),
      tablegroup_name_(), database_name_(),
      result_(), tenant_version_(0), cr_version_(0),
      cr_id_(common::OB_INVALID_CLUSTER_ID), cluster_version_(0) {}
  ~ObTableGroupParam() { reset(); }

  void reset();
  bool need_fetch_from_remote() const { return force_renew_; }
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void deep_copy(ObTableGroupParam &other);

  event::ObContinuation *cont_;
  int64_t tenant_id_;
  bool force_renew_;
  ObMysqlProxy *mysql_proxy_;
  common::ObString tablegroup_name_;
  common::ObString database_name_;
  char tablegroup_name_buf_[OB_MAX_TABLEGROUP_NAME_LENGTH];
  char database_name_buf_[OB_MAX_DATABASE_NAME_LENGTH];
  ObTableGroupResult result_;
  uint64_t tenant_version_;
  int64_t cr_version_;
  int64_t cr_id_;
  int64_t cluster_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupParam);
};

inline bool ObTableGroupParam::is_valid() const
{
  return (NULL != cont_)
          && (tenant_id_ != OB_INVALID_TENANT_ID)
          && (!tablegroup_name_.empty())
          && (!database_name_.empty())
          && cr_version_ >= 0 && cr_id_ >= 0
          && OB_NOT_NULL(mysql_proxy_);
}

inline void ObTableGroupParam::reset()
{
  cont_ = NULL;
  result_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablegroup_name_.reset();
  database_name_.reset();
  tenant_version_ = 0;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
  cr_version_ = 0;
  cluster_version_ = 0;
  mysql_proxy_ = NULL;
}


class ObTableGroupProcessor
{
public:
  ObTableGroupProcessor() {}
  ~ObTableGroupProcessor() {}

  static int get_tablegroup_entry(ObTableGroupParam &param, event::ObAction *&action);

private:
  static int get_tablegroup_entry_from_thread_cache(ObTableGroupParam &param,
                                                   ObTableGroupEntry *&entry);
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TABLEGROUP_PROCESSOR_H

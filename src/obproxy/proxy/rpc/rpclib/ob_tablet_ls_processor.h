/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_TABLET_LS_PROCESSOR_H
#define OBPROXY_TABLET_LS_PROCESSOR_H

#include "proxy/rpc/rpclib/ob_tablet_ls_entry.h"
#include "proxy/rpc/rpclib/ob_tablet_ls_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObTabletLsResult
{
  ObTabletLsResult() : target_entry_(NULL),  target_old_entry_(NULL), is_from_remote_(false) {}
  ~ObTabletLsResult() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  ObTabletLsEntry *target_entry_;
  ObTabletLsEntry *target_old_entry_;
  bool is_from_remote_;
};

inline void ObTabletLsResult::reset()
{
  is_from_remote_ = false;
  target_entry_ = NULL;
  target_old_entry_ = NULL;
}

class ObMysqlProxy;
class ObTabletLsParam
{
public:
  ObTabletLsParam()
    : cont_(NULL), tenant_id_(OB_INVALID_TENANT_ID), table_id_(0),
      force_renew_(false), mysql_proxy_(NULL),
      result_(), tenant_version_(0), cr_version_(0),
      cr_id_(common::OB_INVALID_CLUSTER_ID), cluster_version_(0) {}
  ~ObTabletLsParam() { reset(); }

  void reset();
  bool need_fetch_from_remote() const { return force_renew_; }
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void deep_copy(ObTabletLsParam &other);

  event::ObContinuation *cont_;
  int64_t tenant_id_;
  int64_t table_id_;
  bool force_renew_;
  ObMysqlProxy *mysql_proxy_;
  ObTabletLsResult result_;
  uint64_t tenant_version_;
  int64_t cr_version_;
  int64_t cr_id_;
  int64_t cluster_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletLsParam);
};

inline bool ObTabletLsParam::is_valid() const
{
  return (NULL != cont_)
          && (tenant_id_ != OB_INVALID_TENANT_ID)
          && (table_id_ != 0)
          && cr_version_ >= 0 && cr_id_ >= 0
          && OB_NOT_NULL(mysql_proxy_);
}

inline void ObTabletLsParam::reset()
{
  cont_ = NULL;
  result_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = 0;
  tenant_version_ = 0;
  cr_id_ = common::OB_INVALID_CLUSTER_ID;
  cr_version_ = 0;
  cluster_version_ = 0;
  mysql_proxy_ = NULL;
}


class ObTabletLsProcessor
{
public:
  ObTabletLsProcessor() {}
  ~ObTabletLsProcessor() {}

  static int get_tablet_ls_entry(ObTabletLsParam &param, event::ObAction *&action);

private:
  static int get_tablet_ls_entry_from_thread_cache(ObTabletLsParam &param,
                                                   ObTabletLsEntry *&entry);
  DISALLOW_COPY_AND_ASSIGN(ObTabletLsProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_TABLET_LS_PROCESSOR_H

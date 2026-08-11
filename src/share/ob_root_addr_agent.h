/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_
#define OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_

#include "lib/container/ob_iarray.h"
#include "partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace share
{
typedef ObReplicaLocation ObRootAddr;
typedef common::ObIArray<ObRootAddr> ObRootAddrList;

// store and fetch root server address list interface.
class ObRootAddrAgent
{
public:
  ObRootAddrAgent() : inited_(false), config_(NULL) {}
  virtual ~ObRootAddrAgent() {}

  virtual int init(common::ObServerConfig &config);
  virtual bool is_valid();

  virtual int store(const ObRootAddrList &addr_list, const bool force) = 0;
  virtual int fetch(ObRootAddrList &add_list) = 0;

protected:
  bool inited_;
  common::ObServerConfig *config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootAddrAgent);
};

inline int ObRootAddrAgent::init(common::ObServerConfig &config)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WDIAG, "init twice", K(ret));
  } else {
    config_ = &config;
    inited_ = true;
  }
  return ret;
};

inline bool ObRootAddrAgent::is_valid()
{
  return inited_;
}

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_

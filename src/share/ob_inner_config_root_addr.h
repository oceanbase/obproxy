/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_
#define OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_

#include "ob_root_addr_agent.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{

// store and dispatch root server address list by oceanbase config mechanism.
// same with oceanbase 0.5
class ObInnerConfigRootAddr : public ObRootAddrAgent
{
public:
  ObInnerConfigRootAddr() : inited_(false), proxy_(NULL) {}
  virtual ~ObInnerConfigRootAddr() {}

  int init(common::ObMySQLProxy &sql_proxy, common::ObServerConfig &config);

  virtual int store(const ObRootAddrList &addr_list, const bool force);
  virtual int fetch(ObRootAddrList &add_list);

private:
  static int parse_rs_addr(char *addr_buf, common::ObAddr &addr, int64_t &sql_port);

private:
  bool inited_;
  common::ObMySQLProxy *proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObInnerConfigRootAddr);
};

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_INNER_CONFIG_ROOT_ADDR_H_

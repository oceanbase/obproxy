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

#ifndef OBPROXY_RELOAD_CONFIG_H
#define OBPROXY_RELOAD_CONFIG_H

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyConfig;
class ObIProxyReloadConfig
{
public:
  ObIProxyReloadConfig() {};
  virtual ~ObIProxyReloadConfig() {};
  virtual int do_reload_config(ObProxyConfig &config) = 0;
};

class ObProxyReloadConfig
{
public:
  explicit ObProxyReloadConfig(ObIProxyReloadConfig *reloader)
    : reloader_(reloader) {}
  ~ObProxyReloadConfig() {}
  int operator()(ObProxyConfig &config);

private:
  ObIProxyReloadConfig *reloader_;
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_RELOAD_CONFIG_H */

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

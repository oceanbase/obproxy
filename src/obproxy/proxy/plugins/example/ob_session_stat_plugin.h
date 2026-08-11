/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SESSION_STAT_PLUGIN_H
#define OBPROXY_SESSION_STAT_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_session_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObSessionStatPlugin : public ObSessionPlugin
{
public:
  static ObSessionStatPlugin *alloc(ObProxyClientSession *ssn)
  {
    return op_reclaim_alloc_args(ObSessionStatPlugin, ssn);
  }

  explicit ObSessionStatPlugin(ObProxyClientSession *ssn) : ObSessionPlugin(ssn)
  {
    register_hook(HOOK_SSN_START);
    register_hook(HOOK_SSN_CLOSE);
  }

  virtual void destroy()
  {
    ObSessionPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_ssn_start(ObProxyClientSession *ssn)
  {
    _OB_LOG(DEBUG, "new client session");
    ObApiPlugin::handle_ssn_start(ssn);
  }

  virtual void handle_ssn_close(ObProxyClientSession *ssn)
  {
    _OB_LOG(DEBUG, "close client session");
    ObApiPlugin::handle_ssn_close(ssn);
  }
};

class ObSessionPluginInstaller : public ObGlobalPlugin
{
public:
  static ObSessionPluginInstaller *alloc()
  {
    return op_reclaim_alloc(ObSessionPluginInstaller);
  }

  ObSessionPluginInstaller() : ObGlobalPlugin()
  {
    register_hook(ObApiPlugin::HOOK_SSN_START);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_ssn_start(ObProxyClientSession *ssn)
  {
    _OB_LOG(DEBUG, "ObSessionPluginInstaller::handle_ssn_start");
    ssn->add_plugin(ObSessionStatPlugin::alloc(ssn));

    ObApiPlugin::handle_ssn_start(ssn);
  }
};

inline void init_session_stat_plugin()
{
  _OB_LOG(DEBUG, "init session stat plugin");
  ObSessionPluginInstaller *plugin = ObSessionPluginInstaller::alloc();
  UNUSED(plugin);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase


#endif // OBPROXY_SESSION_STAT_PLUGIN_H

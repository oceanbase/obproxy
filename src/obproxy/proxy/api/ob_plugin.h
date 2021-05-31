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

#ifndef OBPROXY_PLUGIN_H
#define OBPROXY_PLUGIN_H

#include "utils/ob_proxy_lib.h"
#include "lib/list/ob_intrusive_list.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObPluginRegInfo
{
  ObPluginRegInfo();
  ~ObPluginRegInfo();

  bool plugin_registered_;
  char *plugin_path_;

  char *plugin_name_;
  char *vendor_name_;
  char *support_email_;

  LINK(ObPluginRegInfo, link_);
};

// Plugin registration vars
extern common::DLL<ObPluginRegInfo> g_plugin_reg_list;
extern ObPluginRegInfo *g_plugin_reg_current;

int plugin_init();

/**
 * Abstract interface class for plugin based continuations.
 *
 * The primary intended use of this is for logging so that continuations
 * that generate logging messages can generate plugin local data in a
 * generic way.
 *
 * The core will at appropriate times dynamically cast the continuation
 * to this class and if successful access the plugin data via these
 * methods.
 *
 * Plugins should mix this in to continuations for which it is useful.
 * The default implementations return empty / invalid responses and should
 * be overridden by the plugin.
 */
class ObPluginIdentity
{
 public:
  /// Make sure destructor is virtual.
  virtual ~ObPluginIdentity() {}

  /**
   * Get the plugin tag.
   * The returned string must have a lifetime at least as long as the plugin.
   *
   * @return A string identifying the plugin or NULL.
   */
  virtual char const* get_plugin_tag() const { return NULL; }
  /**
   * Get the plugin instance ID.
   * A plugin can create multiple subsidiary instances. This is used as the
   * identifier for those to distinguish the instances.
   *
   * @return
   */
  virtual int64_t get_plugin_id() const { return 0; }
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PLUGIN_H

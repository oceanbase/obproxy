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

#ifndef _OCEABASE_LIB_OB_PROXY_WORKER_H_
#define _OCEABASE_LIB_OB_PROXY_WORKER_H_
namespace oceanbase
{
namespace lib
{

// used to check compatibility mode.
class ObProxyRuntimeContext
{
public:
  ObProxyRuntimeContext()
  : is_oralce_mode_(false)
  {}
  bool is_oralce_mode_;
};

inline ObProxyRuntimeContext &get_ob_proxy_runtime_context()
{
  static __thread ObProxyRuntimeContext *ob_proxy_runtime_context = NULL;
  if (OB_ISNULL(ob_proxy_runtime_context)) {
    ob_proxy_runtime_context = new (std::nothrow) ObProxyRuntimeContext();
  }
  return *ob_proxy_runtime_context;
}

inline void set_oracle_mode(bool is_oracle_mode)
{
  get_ob_proxy_runtime_context().is_oralce_mode_ = is_oracle_mode;
}

inline bool is_oracle_mode()
{
  return get_ob_proxy_runtime_context().is_oralce_mode_;
}
inline bool is_mysql_mode()
{
  return !is_oracle_mode();
}

}
}
#endif // _OCEABASE_LIB_OB_PROXY_WORKER_H_

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

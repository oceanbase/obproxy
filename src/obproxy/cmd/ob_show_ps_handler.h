/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_PS_HANDLER_H
#define OBPROXY_SHOW_PS_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
  class ObEThread;
}
namespace proxy
{
class ObMysqlClientSession;
class ObBasePsEntry;


class ObShowPSHandler : public ObInternalCmdHandler
{
public:
  ObShowPSHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowPSHandler();

private:
  int main_handler(int event, void *data);
  int dump_header();
  int handle_ps_cache(int event, void* data);
  int handle_ps_cache_for_cs();
  int dump_ps_cache_for_cs(ObMysqlClientSession &cs);
  int dump_ps_cache_for_one_cs(proxy::ObMysqlClientSession &cs, bool need_check_prvilige = true/*true*/);
  int handle_ps_cache_for_tenant(int event, void* data);
  int dump_ps_cache_for_tenant_in_thread(const event::ObEThread& ethread);
  int handle_ps_cache_all_for_global();
  int handle_ps_cache_all_for_thread(int event, void* data);
  int dump_ps_cache_all_in_thread(const event::ObEThread& ethread);
  int dump_one_ps_entry(proxy::ObBasePsEntry& ps_entry,  const ObString& cluster_name = "",
                        const ObString& tenant_name = "",  int64_t cs_id = -1,
                        int64_t ps_id = -1, const ObString& ps_name = "");
  static int dump_one_ps_entry(ObBasePsEntry& ps_entry, va_list args);
  int dump_empty_ps_entry();

  int dump_cumulative_ps_entry();
  bool is_match_tenant(const proxy::ObMysqlClientSession &cs) const;
  bool enable_dump_ps_cache(const proxy::ObMysqlClientSession &cs) const;
  bool enable_dump_all_ps_cache() const;

public:
  ObProxySessionPrivInfo session_priv_;
private:
  static const int BUF_LEN = OB_MALLOC_NORMAL_BLOCK_SIZE;
  bool need_dump_all_ps_cache_;
  bool need_output_detail_info_;
  ObProxyBasicStmtSubType sub_type_;
  char *row_buf_;
  common::ObString tenant_name_;
  char tenant_str_[common::OB_MAX_TENANT_NAME_LENGTH + 1];
  DISALLOW_COPY_AND_ASSIGN(ObShowPSHandler);
};

int show_ps_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_TOPOLOGY_HANDLER_H */

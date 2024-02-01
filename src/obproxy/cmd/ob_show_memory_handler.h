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

#ifndef OBPROXY_SHOW_MEMORY_HANDLER_H
#define OBPROXY_SHOW_MEMORY_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
class ObObjFreeList;
}
namespace obproxy
{
namespace obutils
{
class ObShowMemoryHandler : public ObInternalCmdHandler
{
public:
  ObShowMemoryHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowMemoryHandler() {}
  int handle_show_memory(int event, void *data);
  int handle_show_objpool(int event, void *data);

private:
  int format_int_to_str(const int64_t value, common::ObSqlString &string);
  int dump_mod_memory_header();
  int dump_mod_memory(const char *name, const char *type, const int64_t hold,
                      const int64_t used, const int64_t count, const ObString& backtrace = ObString(""));

  int dump_objpool_header();
  int dump_objpool_memory(const common::ObObjFreeList *fl, const ObString& backtrace = ObString(""));

private:
  int64_t backtrace_count_;
  const ObProxyBasicStmtSubType sub_type_;

  DISALLOW_COPY_AND_ASSIGN(ObShowMemoryHandler);
};

int show_memory_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_MEMORY_HANDLER_H */

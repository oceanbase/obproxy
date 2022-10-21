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

#ifndef OBPROXY_SHOW_STAT_HANDLER_H
#define OBPROXY_SHOW_STAT_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShowStatHandler : public ObInternalCmdHandler
{
public:
  ObShowStatHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowStatHandler() {}

private:
  int handle_show_stat(int event, void *data);
  int dump_stat_header();
  int dump_stat_item(const obproxy::ObRecRecord *record);
  const common::ObString get_persist_type_str(const obproxy::ObRecPersistType type) const;

private:
  const ObProxyBasicStmtSubType sub_type_;

  DISALLOW_COPY_AND_ASSIGN(ObShowStatHandler);
};

int show_stat_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_STAT_HANDLER_H */


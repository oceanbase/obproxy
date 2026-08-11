/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_INFO_HANDLER_H
#define OBPROXY_SHOW_INFO_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
class ObNewRow;
}
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
struct ObServerStateSimpleInfo;
}
namespace proxy
{
class ObShowInfoHandler : public ObInternalCmdHandler
{
public:
  ObShowInfoHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowInfoHandler() {};

private:
  int main_handler(int event, void *data);
  int dump_header();
  int dump_body();
  int dump_idc_body();
  int dump_resource_idc_info(const obutils::ObClusterResource &cr,
                             const common::ObString &idc_name,
                             common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
                             char *buf,
                             const int64_t buf_len);
private:
  ObProxyBasicStmtSubType sub_type_;
  DISALLOW_COPY_AND_ASSIGN(ObShowInfoHandler);
};

int show_info_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHOW_INFO_HANDLER_H


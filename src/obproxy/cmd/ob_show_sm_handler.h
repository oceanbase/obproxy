/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_SM_HANDLER_H
#define OBPROXY_SHOW_SM_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/rpc/ob_rpc_req_debug_names.h"
// #include "proxy/rpc/ob_rpc_request_sm.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace obproxy
{
namespace proxy
{
class ObShowSMHandler : public ObInternalCmdHandler
{
public:
  ObShowSMHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowSMHandler() { got_id_set_.destroy(); }
  int dump_smlist();
  int init_hash_set(); //now only show smlist need init
  bool need_init_hash_set() const { return sm_id_ < 0; }
  int handle_smdetails(int event, void *data);
  int handle_smlist(int event, void *data);
  int handle_rpc_smlist(int event, void *data);
  int handle_rpc_smdetails(int event, void *data);

private:

  int dump_header();
  int dump_common_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_tunnel_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_history_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_sm_internal(const ObMysqlSM &sm);
  int dump_rpc_sm_internal(const ObRpcRequestSM &sm);
  int dump_rpc_history_info(const ObRpcRequestSM &sm, common::ObSqlString &sm_info);

private:
  static const int64_t BUCKET_SIZE = 1021;

  const ObProxyBasicStmtSubType sub_type_;
  bool is_hash_set_inited_;
  int64_t list_bucket_;
  const int64_t sm_id_;
  common::hash::ObHashSet<int64_t, hash::NoPthreadDefendMode> got_id_set_;

  DISALLOW_COPY_AND_ASSIGN(ObShowSMHandler);
};

int show_sm_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHOW_SM_HANDLER_H


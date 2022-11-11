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

#ifndef OBPROXY_SHOW_SM_HANDLER_H
#define OBPROXY_SHOW_SM_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_sm.h"

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

private:
  int handle_smdetails(int event, void *data);
  int handle_smlist(int event, void *data);

  int dump_header();
  int dump_common_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_tunnel_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_history_info(const ObMysqlSM &sm, common::ObSqlString &sm_info);
  int dump_sm_internal(const ObMysqlSM &sm);

private:
  static const int64_t BUCKET_SIZE = 1021;

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


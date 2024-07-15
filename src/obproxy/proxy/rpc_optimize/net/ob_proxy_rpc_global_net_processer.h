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
#ifndef OBPROXY_RESOURCES_POOL_PROCESSOR_H
#define OBPROXY_RESOURCES_POOL_PROCESSOR_H

#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "proxy/rpc_optimize/net/ob_rpc_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObProxyRpcNetProcessor
{
  int add_client_entry(ObRpcVCEntry* entry);
  int remove_client_entry(ObRpcVCEntry* entry);

  int add_server_entry(ObRpcVCEntry* entry);
  int remove_server_entry(ObRpcVCEntry* entry);

  int do_client_io_read(ObRpcVCEntry* entry, ObReq *&req);
  int do_client_io_send(ObRpcVCEntry* entry, const ObReq *req);
  int do_server_io_read(ObRpcVCEntry* entry, ObReq *&req);
  int do_server_io_send(ObRpcVCEntry* entry, const ObReq *req);

  int do_client_io_close(...);
  int do_client_io_shutdown(...);
  int do_client_io_reenable(...);

  int do_server_io_close(...);
  int do_server_io_shutdown(...);
  int do_server_io_reenable(...);

  int add_server_table_entry(ObAddr add);
  int free_server_table_entry(ObAddr addr);

  // ObHashMap<event::ObVConnection *, ObRpcVCEntry*>  client_entry_table_;  //保存所有client entry对象；
//   ObHashMap<int64_t, ObRpcVCEntry *> client_entry_table_;  //保存所有client entry对象，key：ip+port组成的数字， value：client entry对象；
//   ObHashMap<int64_t, ObRpcVCServerTableEntry *>  server_entry_table_;  //保存所有 server entry对象； key是 ip + port组成的数字，value是 一组sever entry对象。
  common::hash::ObHashMap<ObAddr, ObRpcNetHandler*> client_entry_table_;
  common::hash::ObHashMap<ObAddr, ObRpcNetHandler*> server_entry_table_;

//   ObHashMap<event::ObVConnection *, ObRpcVCEntry *>  all_entry_table_; //保存所有entry信息；
};

extern ObProxyRpcNetProcessor g_rpc_net_processor;
inline ObProxyRpcNetProcessor &get_global_rpc_net_processor()
{
    return g_rpc_net_processor;
}

}
}
}

#endif /* OBPROXY_RESOURCES_POOL_PROCESSOR_H */
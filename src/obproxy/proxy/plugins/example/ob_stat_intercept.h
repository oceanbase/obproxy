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

#ifndef OBPROXY_STAT_INTERCEPT_H
#define OBPROXY_STAT_INTERCEPT_H

#include "cmd/ob_show_sm_handler.h"
#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_intercept_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObStatIntercept : public ObInterceptPlugin
{
public:
  static ObStatIntercept *alloc(ObApiTransaction &transaction)
  {
    return op_reclaim_alloc_args(ObStatIntercept, transaction);
  }

  explicit ObStatIntercept(ObApiTransaction &transaction) : ObInterceptPlugin(transaction) { }

  virtual void destroy()
  {
    ObInterceptPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void consume(event::ObIOBufferReader *reader)
  {
    _OB_LOG(DEBUG, "ObStatIntercept, read client request finish, reader=%p", reader);
  }

  virtual void handle_input_complete()
  {
    _OB_LOG(DEBUG, "ObStatIntercept, handle_input_complete");
    event::ObMIOBuffer *buf = event::new_empty_miobuffer();
    event::ObIOBufferReader *reader = buf->alloc_reader();
    ObShowSMHandler show_sm(
        NULL, buf, 0, (uint8_t)sm_->trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_);
    show_sm.dump_smlist();
    (void)show_sm.fill_external_buf();
    ObInterceptPlugin::produce(reader);
    ObInterceptPlugin::set_output_complete();
    buf->dealloc_reader(reader);
    free_miobuffer(buf);
  }
};

class ObInterceptInstaller : public ObGlobalPlugin
{
public:
  static ObInterceptInstaller *alloc()
  {
    return op_reclaim_alloc(ObInterceptInstaller);
  }

  ObInterceptInstaller() : ObGlobalPlugin()
  {
    register_hook(ObApiPlugin::HOOK_READ_REQUEST);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_request(ObApiTransaction &transaction)
  {
    _OB_LOG(DEBUG, "ObInterceptInstaller::handle_read_request");
    if (transaction.is_internal_cmd()) {
      transaction.add_plugin(ObStatIntercept::alloc(transaction));
      _OB_LOG(DEBUG, "Added stat intercept");
    }

    transaction.resume();
  }
};

inline void init_stat_intercept()
{
  _OB_LOG(DEBUG, "init stat intercept");
  ObInterceptInstaller *intercept = ObInterceptInstaller::alloc();
  UNUSED(intercept);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase


#endif // OBPROXY_STAT_INTERCEPT_H

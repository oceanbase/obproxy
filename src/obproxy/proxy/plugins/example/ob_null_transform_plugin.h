/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_NULL_TRANSFORM_PLUGIN_H
#define OBPROXY_NULL_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObNullTransformationPlugin : public ObTransformationPlugin
{
public:
  static ObNullTransformationPlugin *alloc(
      ObApiTransaction &transaction, ObTransformationPlugin::ObTransformType xform_type)
  {
    return op_reclaim_alloc_args(ObNullTransformationPlugin, transaction, xform_type);
  }

  ObNullTransformationPlugin(ObApiTransaction &transaction,
                             ObTransformationPlugin::ObTransformType xform_type)
      : ObTransformationPlugin(transaction, xform_type)
  {
    register_hook((ObTransformationPlugin::REQUEST_TRANSFORMATION == xform_type) ?
                 HOOK_SEND_REQUEST : HOOK_SEND_RESPONSE);
  }

  virtual void destroy()
  {
    ObTransformationPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_send_request(ObApiTransaction &transaction)
  {
    transaction.resume();
  }

  virtual void handle_send_response(ObApiTransaction &transaction)
  {
    transaction.resume();
  }

  virtual int consume(event::ObIOBufferReader *reader)
  {
    produce(reader);
  }

  virtual void handle_input_complete()
  {
    set_output_complete();
  }
};

class ObGlobalHookPlugin : public ObGlobalPlugin
{
public:
  static ObGlobalHookPlugin *alloc()
  {
    return op_reclaim_alloc(ObGlobalHookPlugin);
  }

  ObGlobalHookPlugin()
  {
    register_hook(HOOK_READ_REQUEST);
    register_hook(HOOK_READ_RESPONSE);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_request(ObApiTransaction &transaction)
  {
    ObMysqlSM *sm = transaction.get_sm();
    if (!transaction.is_internal_cmd() && !sm->trans_state_.is_handshake_req_phase()) {
      transaction.add_plugin(ObNullTransformationPlugin::alloc(
          transaction, ObTransformationPlugin::REQUEST_TRANSFORMATION));
    }
    transaction.resume();
  }

  virtual void handle_read_response(ObApiTransaction &transaction)
  {
    ObMysqlSM *sm = transaction.get_sm();
    if (!transaction.is_internal_cmd() && !sm->trans_state_.is_handshake_req_phase()
        && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_) {
      transaction.add_plugin(ObNullTransformationPlugin::alloc(
          transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION));
    }
    transaction.resume();
  }
};

inline void init_null_transform()
{
  _OB_LOG(DEBUG, "init null transformation plugin");
  ObGlobalHookPlugin *null_transform = ObGlobalHookPlugin::alloc();
  UNUSED(null_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NULL_TRANSFORM_PLUGIN_H

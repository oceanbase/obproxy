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
 *
 * **************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OBPROXY_TRANSFORMATION_PLUGIN_H
#define OBPROXY_TRANSFORMATION_PLUGIN_H

#include "proxy/api/ob_api_transaction.h"
#include "proxy/api/ob_transaction_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObTransformationPluginState;

/**
 * @brief The interface used when you wish to transform Request or Response body content.
 *
 * Transformations are deceptively simple, transformations are chained so the output
 * of one ObTransformationPlugin becomes the input of another ObTransformationPlugin. As
 * data arrives it will fire a consume() and when all the data has been sent
 * you will receive a handle_input_complete(). Data can be sent to the next ObTransformationPlugin
 * in the chain by calling produce() and when the transformation has no data left to send
 * it will fire a setOutputCompete().
 *
 * Since a ObTransformationPlugin is a type of ObTransactionPlugin you can call register_hook() and
 * establish any hook for a ObApiTransaction also; however, remember that you must implement
 * the appropriate callback for any hooks you register.
 *
 * A simple example of how to use the ObTransformationPlugin interface follows, this is an example
 * of a Response transformation, the avialable options are REQUEST_TRANSFORMATION and RESPONSE_TRANSFORMATION
 * which are defined in ObTransformType.
 *
 * This example is a Null Transformation, meaning it will just spit out the content it receives without
 * actually doing any work on it.
 * @code
 * class ObNullTransformationPlugin : public ObTransformationPlugin
 * {
 * public:
 *   static ObNullTransformationPlugin *alloc(ObApiTransaction &transaction)
 *   {
 *     return op_reclaim_alloc_args(ObNullTransformationPlugin, transaction);
 *   }
 *   virtual void destroy()
 *   {
 *     ObTransactionPlugin::destroy();
 *     op_reclaim_free(this);
 *   }
 *   void handle_send_response(ObApiTransaction &transaction)
 *   {
 *     transaction.resume();
 *   }
 *   void consume(event::ObIOBufferReader *reader)
 *   {
 *     produce(reader);
 *   }
 *   void handle_input_complete()
 *   {
 *     set_output_complete();
 *   }
 * private:
 *   ObNullTransformationPlugin(ObApiTransaction &transaction)
 *       : ObTransformationPlugin(transaction, RESPONSE_TRANSFORMATION)
 *   {
 *     register_hook(HOOK_SEND_RESPONSE_HEADERS);
 *   }
 * };
 * @endcode
 * @see ObApiPlugin
 * @see ObTransactionPlugin
 * @see ObTransformType
 * @see ObHookType
 */
class ObTransformationPlugin : public ObTransactionPlugin
{
  friend class ObApiUtilsInternal;
public:
  virtual void destroy();

  /**
   * The available types of Transformations.
   */
  enum ObTransformType
  {
    REQUEST_TRANSFORMATION = 0, // Transform the Request content
    RESPONSE_TRANSFORMATION     // Transform the Response content
  };

  /**
   * A method that you must implement when writing a ObTransformationPlugin, this method will be
   * fired whenever an upstream ObTransformationPlugin has produced output.
   *
   * @param data
   */
  virtual int consume(event::ObIOBufferReader *reader) = 0;

  /**
   * A method that you must implement when writing a ObTransformationPlugin, this method
   * will be fired whenever the upstream ObTransformationPlugin has completed writing data.
   */
  virtual void handle_input_complete() = 0;

protected:
  /**
   * a ObTransformationPlugin must implement this interface, it cannot be constructed directly
   *
   * @param transaction
   * @param type
   */
  ObTransformationPlugin(ObApiTransaction &transaction, ObTransformType type);

  /**
   * This method is how a ObTransformationPlugin will produce output for the downstream
   * transformation plugin, if you need to produce binary data this can still be
   * done by constructing a ObIOBufferReader.
   *
   * @return
   */
  int64_t produce(event::ObIOBufferReader *reader, const int64_t produce_size = 0);

  /**
   * This is the method that you must call when you're done producing output for
   * the downstream ObTransformationPlugin.
   *
   * @return
   */
  int64_t set_output_complete();

  int get_write_ntodo(int64_t &to_write);

private:
  int handle_transform_read(ObContInternal *contp);
  int handle_event(ObEventType event, void *edata);

  DISALLOW_COPY_AND_ASSIGN(ObTransformationPlugin);
  ObVConnInternal *vconn_;
  ObApiTransaction *transaction_;
  ObTransformType type_;
  event::ObVIO *output_vio_; // this gets initialized on an output().
  event::ObMIOBuffer *output_buffer_;
  event::ObIOBufferReader *output_buffer_reader_;
  int64_t bytes_written_;
  int64_t transform_bytes_;

  // We can only send a single WRITE_COMPLETE even though
  // we may receive an immediate event after we've sent a
  // write complete, so we'll keep track of whether or not we've
  // sent the input end our write complete.
  bool input_complete_dispatched_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TRANSFORMATION_PLUGIN_H

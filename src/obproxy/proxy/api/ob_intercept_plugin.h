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

#ifndef OBPROXY_INTERCEPT_PLUGIN_H
#define OBPROXY_INTERCEPT_PLUGIN_H

#include "proxy/api/ob_api_transaction.h"
#include "proxy/api/ob_transaction_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

/**
 * Allows a plugin to act as a server and return the response. This
 * plugin can be created in read request hook.
 */
class ObInterceptPlugin : public ObTransactionPlugin
{
  friend class ObApiUtilsInternal;
public:
  /**
   * A method that you must implement when writing an ObInterceptPlugin, this method will be
   * invoked whenever client request data is read.
   */
  virtual void consume(event::ObIOBufferReader *reader) = 0;

  /**
   * A method that you must implement when writing an ObInterceptPlugin, this method
   * will be invoked when the client request is deemed complete.
   */
  virtual void handle_input_complete() = 0;

  virtual void destroy();

protected:
  // a plugin must implement this interface, it cannot be constructed directly
  explicit ObInterceptPlugin(ObApiTransaction &transaction);

  /**
   * This method is how an ObInterceptPlugin will send output back to
   * the client.
   */
  int produce(event::ObIOBufferReader *reader);

  int set_output_complete();

private:
  void destroy_cont();
  int do_read();
  int handle_event_internal(ObEventType event, void *edata);
  int handle_event(ObEventType event, void *data);

  ObContInternal *intercept_cont_;
  event::ObVConnection *net_vc_;

  struct IoHandle
  {
    event::ObVIO *vio_;
    event::ObMIOBuffer *buffer_;
    event::ObIOBufferReader *reader_;
    IoHandle() : vio_(NULL), buffer_(NULL), reader_(NULL) { };

    void destroy()
    {
      if (NULL != reader_) {
        reader_->mbuf_->dealloc_reader(reader_);
      }

      if (NULL != buffer_) {
        event::free_miobuffer(buffer_);
      }
    }
  };

  IoHandle input_;
  IoHandle output_;

  int64_t num_bytes_written_;

  // these two fields to be used by the continuation callback only
  ObEventType saved_event_;
  void *saved_edata_;
  event::ObAction *timeout_action_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_INTERCEPT_PLUGIN_H

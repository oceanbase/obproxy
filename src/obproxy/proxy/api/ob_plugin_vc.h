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
 * *************************************************************
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

#ifndef OBPROXY_PLUGIN_VC_H
#define OBPROXY_PLUGIN_VC_H

#include "iocore/net/ob_net.h"
#include "proxy/api/ob_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObPluginVCCore;

struct ObPluginVCState
{
  ObPluginVCState();
  event::ObVIO vio_;
  bool shutdown_;
};

inline ObPluginVCState::ObPluginVCState()
    : vio_(), shutdown_(false)
{
}

enum ObPluginVCType
{
  PLUGIN_VC_UNKNOWN,
  PLUGIN_VC_ACTIVE,
  PLUGIN_VC_PASSIVE
};

// For the id in set_data/get_data
enum
{
  PLUGIN_VC_DATA_LOCAL,
  PLUGIN_VC_DATA_REMOTE
};

enum
{
  PLUGIN_VC_MAGIC_ALIVE = 0xaabbccdd,
  PLUGIN_VC_MAGIC_DEAD = 0xaabbdead
};

class ObPluginVC : public net::ObNetVConnection, public ObPluginIdentity
{
  friend class ObPluginVCCore;

public:

  ObPluginVC(ObPluginVCCore *core_obj);
  virtual ~ObPluginVC();

  virtual event::ObVIO *do_io_read(event::ObContinuation *c = NULL,
                                   const int64_t nbytes = INT64_MAX,
                                   event::ObMIOBuffer *buf = 0);

  virtual event::ObVIO *do_io_write(event::ObContinuation *c = NULL,
                                    const int64_t nbytes = INT64_MAX,
                                    event::ObIOBufferReader *buf = 0);

  virtual void do_io_close(const int lerrno = -1);
  virtual void do_io_shutdown(const event::ShutdownHowToType howto);

  // Reenable a given vio. The public interface is through ObVIO::reenable
  virtual void reenable(event::ObVIO *vio);
  virtual void reenable_re(event::ObVIO *vio);

  // Timeouts
  virtual int set_active_timeout(ObHRTime timeout_in);
  virtual void set_inactivity_timeout(ObHRTime timeout_in);
  virtual void cancel_active_timeout();
  virtual void cancel_inactivity_timeout();
  virtual void add_to_keep_alive_lru();
  virtual void remove_from_keep_alive_lru();
  virtual ObHRTime get_active_timeout() const;
  virtual ObHRTime get_inactivity_timeout() const;

  // Pure virtual functions we need to compile
  virtual int get_socket();
  virtual int set_local_addr();
  virtual bool set_remote_addr();
  virtual int set_virtual_addr() { return common::OB_SUCCESS; }
  virtual int get_conn_fd() { return 0; }
  virtual int set_tcp_init_cwnd(int init_cwnd);
  virtual int apply_options();

  virtual bool get_data(const int32_t id, void *data);
  virtual bool set_data(const int32_t id, void *data);

  virtual ObPluginVC *get_other_side() { return other_side_; }

  // @name Plugin identity.
  // Override for ObPluginIdentity.
  virtual char const *get_plugin_tag() const { return plugin_tag_; }

  // Override for ObPluginIdentity.
  virtual int64_t get_plugin_id() const { return plugin_id_; }

  // Setter for plugin tag.
  virtual void setPluginTag(char const *tag) { plugin_tag_ = tag; }

  // Setter for plugin id.
  virtual void setPluginId(int64_t id) { plugin_id_ = id; }

  int main_handler(int event, void *data);

private:
  void process_read_side(bool);
  void process_write_side(bool);
  void process_close();
  void process_timeout(event::ObEvent **e, int event_to_send);

  void setup_event_cb(ObHRTime in, event::ObEvent **e_ptr);

  void update_inactive_time();
  int64_t transfer_bytes(event::ObMIOBuffer *transfer_to,
                         event::ObIOBufferReader *transfer_from, int64_t act_on);

  uint32_t magic_;
  ObPluginVCType vc_type_;
  ObPluginVCCore *core_obj_;

  ObPluginVC *other_side_;

  ObPluginVCState read_state_;
  ObPluginVCState write_state_;

  bool need_read_process_;
  bool need_write_process_;

  volatile bool closed_;
  event::ObEvent *sm_lock_retry_event_;
  event::ObEvent *core_lock_retry_event_;

  bool deletable_;
  int reentrancy_count_;

  ObHRTime active_timeout_;
  event::ObEvent *active_event_;

  ObHRTime inactive_timeout_;
  ObHRTime inactive_timeout_at_;
  event::ObEvent *inactive_event_;

  char const *plugin_tag_;
  int64_t plugin_id_;
};

class ObPluginVCCore : public event::ObContinuation
{
  friend class ObPluginVC;

public:
  ObPluginVCCore();
  virtual ~ObPluginVCCore();

  static ObPluginVCCore *alloc();
  void init();
  void set_accept_cont(event::ObContinuation *c);

  int state_send_accept(int event, void *data);
  int state_send_accept_failed(int event, void *data);

  void attempt_delete();

  ObPluginVC *connect();
  int connect_re(event::ObContinuation *c, event::ObAction *&acion);
  void kill_no_connect();

  // Set the active address.
  // IPv4 address in host order.
  // IP Port in host order.
  void set_active_addr(in_addr_t ip, int port);

  // Set the active address and port.
  void set_active_addr(const sockaddr &ip);

  // Set the passive address.
  // IPv4 address in host order.
  // IP port in host order.
  void set_passive_addr(in_addr_t ip, int port);

  // Set the passive address.
  void set_passive_addr(const sockaddr &ip);

  void set_active_data(void *data);
  void set_passive_data(void *data);

  // Set the plugin ID for the internal VCs.
  void set_plugin_id(int64_t id);

  // Set the plugin tag for the internal VCs.
  void set_plugin_tag(char const *tag);

  // The active vc is handed to the initiator of
  // connection. The passive vc is handled to
  // receiver of the connection
  ObPluginVC active_vc_;
  ObPluginVC passive_vc_;

private:
  void destroy();

  event::ObContinuation *connect_to_;
  bool connected_;

  event::ObMIOBuffer *p_to_a_buffer_;
  event::ObIOBufferReader *p_to_a_reader_;

  event::ObMIOBuffer *a_to_p_buffer_;
  event::ObIOBufferReader *a_to_p_reader_;

  net::ObIpEndpoint passive_addr_struct_;
  net::ObIpEndpoint active_addr_struct_;

  void *passive_data_;
  void *active_data_;

  static volatile int64_t g_nextid;
  int64_t id_;
};

inline ObPluginVCCore::ObPluginVCCore()
    : active_vc_(this),
      passive_vc_(this),
      connect_to_(NULL),
      connected_(false),
      p_to_a_buffer_(NULL),
      p_to_a_reader_(NULL),
      a_to_p_buffer_(NULL),
      a_to_p_reader_(NULL),
      passive_data_(NULL),
      active_data_(NULL),
      id_(0)
{
  memset(&active_addr_struct_, 0, sizeof active_addr_struct_);
  memset(&passive_addr_struct_, 0, sizeof passive_addr_struct_);

  id_ = ATOMIC_FAA(&g_nextid, 1);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PLUGIN_VC_H

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

#ifndef OB_DRIVER_CLIENT_H_
#define OB_DRIVER_CLIENT_H_

#include "proxy/client/ob_client_utils.h"
#include "iocore/net/ob_net_vconnection.h"
#include "iocore/eventsystem/ob_ethread.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObDriverClient
{
public:
  ObDriverClient();
  virtual ~ObDriverClient();
  int init();
  int sync_connect();
  int sync_connect_with_timeout();
  int sync_recv_with_timeout(char *buf, int64_t buf_len, int64_t &recv_len);
  int sync_send_with_timeout(const char *buf, int64_t buf_len, int64_t &send_len);
  inline void set_connect_timeout(const int64_t timeout) { connect_timeout_ = timeout; }
  inline void set_cs_id(const int64_t cs_id) { cs_id_ = cs_id; }
  void destroy();

  int64_t get_connect_timeout() const { return connect_timeout_; }
  int64_t get_recv_timeout() const { return recv_timeout_; }
  void set_recv_timeout(const int64_t timeout) { recv_timeout_ = timeout; }
  int64_t get_send_timeout() const { return send_timeout_; }
  void set_send_timeout(const int64_t timeout) { send_timeout_ = timeout; }

private:
  inline int64_t get_current_time();
  int sync_recv_internal(char *buf, int64_t buf_len, int64_t &recv_len, int recv_flags);
  int sync_send_internal(const char *buf, int64_t buf_len, int64_t &send_len, int send_flags);
  int sync_wait_io_or_timeout(bool is_recv);

private:
  bool is_inited_;
  int unix_fd_;
  int64_t connect_timeout_;
  int64_t recv_timeout_;
  int64_t send_timeout_;
  int64_t cs_id_;
private:
DISALLOW_COPY_AND_ASSIGN(ObDriverClient);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif

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
#ifndef OB_TARGET_DB_SERVER_H
#define OB_TARGET_DB_SERVER_H

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/utility/ob_print_utils.h"
#include "iocore/net/ob_inet.h"
#include "lib/string/ob_string.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
typedef common::ObSEArray<common::ObString, 4L> ObTargetDbServerArray;
class ObTargetDbServer
{
public:
  ObTargetDbServer() : 
      is_init_(false), is_rand_(false), last_failed_idx_(-1),
      target_db_server_buf_(NULL), target_db_server_buf_len_(0) {}
 
  ObTargetDbServer& operator=(const ObTargetDbServer &other) {
    if (this != &other) {
      reset();
      // other.target_db_server_buf_len_ - 1 to minus last '\0'
      init(other.target_db_server_buf_, other.target_db_server_buf_len_ - 1);
    }
    return *this;
  }

  ~ObTargetDbServer() {
    reset();
  }

  int init(const char* target_db_server_buf, uint64_t target_db_server_buf_len); 
  // get the server with the highest priority execpt failed servers
  int get(net::ObIpEndpoint &dest_addr);
  // mark the previous visit server as failed then call get()
  int get_next(net::ObIpEndpoint &dest_addr);
  // num of target db server
  inline int64_t num() const { return is_init_? target_db_server_.count() : 0; }
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool contains(net::ObIpEndpoint &addr);

  inline bool is_init() const { return is_init_; }
  inline bool is_empty() const { return is_init_? target_db_server_.empty() : true; }
  inline void reuse() { is_rand_ = false; last_failed_idx_ = -1; }

private:
  bool is_init_;
  bool is_rand_;
  int64_t last_failed_idx_;
  char* target_db_server_buf_;
  uint64_t target_db_server_buf_len_;

  ObTargetDbServerArray  target_db_server_;
  common::ObSEArray<int8_t, 4L> target_db_server_weight_;
};

} // end of namespace obproxy
} // end of namespace oceanbase
#endif
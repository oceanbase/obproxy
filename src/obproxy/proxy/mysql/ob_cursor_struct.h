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

#ifndef OBPROXY_CURSOR_STRUCT_H
#define OBPROXY_CURSOR_STRUCT_H

#include "lib/hash/ob_build_in_hashmap.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define CURSOR_ID_START 1LL << 31

// stored in client session info
class ObCursorIdAddr
{
public:
  ObCursorIdAddr() : cursor_id_(0), addr_() {}
  ObCursorIdAddr(uint32_t cursor_id, const sockaddr &addr)
      : cursor_id_(cursor_id), addr_(addr) {}
  ~ObCursorIdAddr() {}

  static int alloc_cursor_id_addr(uint32_t cursor_id, const struct sockaddr &addr, ObCursorIdAddr *&cursor_id_addr);

  void destroy();
  bool is_valid() const { return 0 != cursor_id_ && addr_.is_valid(); }

  uint32_t get_cursor_id() { return cursor_id_; }
  net::ObIpEndpoint &get_addr() { return addr_; }
  void set_addr(const struct sockaddr &addr) { addr_.assign(addr); }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t cursor_id_; // client cursor id
  net::ObIpEndpoint addr_;

  LINK(ObCursorIdAddr, cursor_id_addr_link_);
};

// cursor_id ----> ObCursorIdAddr
struct ObCursorIdAddrHashing
{
  typedef const uint32_t &Key;
  typedef ObCursorIdAddr Value;
  typedef ObDLList(ObCursorIdAddr, cursor_id_addr_link_) ListHead;

  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
  static Key key(Value const *value) { return value->cursor_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

typedef common::hash::ObBuildInHashMap<ObCursorIdAddrHashing> ObCursorIdAddrMap;

// stored in server session info
class ObCursorIdPair
{
public:
  ObCursorIdPair() : client_cursor_id_(0),
                     server_cursor_id_(0) {}
  ObCursorIdPair(uint32_t client_cursor_id, uint32_t server_cursor_id)
      : client_cursor_id_(client_cursor_id), server_cursor_id_(server_cursor_id) {}
  ~ObCursorIdPair() {}

  static int alloc_cursor_id_pair(uint32_t client_cursor_id, uint32_t server_cursor_id, ObCursorIdPair *&cursor_id_pair);

  void destroy();
  bool is_valid() const { return 0 != client_cursor_id_ && 0 != server_cursor_id_; }

  uint32_t get_client_cursor_id() const { return client_cursor_id_; }
  void set_client_cursor_id(uint32_t client_cursor_id) { client_cursor_id_ = client_cursor_id; }

  uint32_t get_server_cursor_id() const { return server_cursor_id_; }
  void set_server_cursor_id(uint32_t server_cursor_id) { server_cursor_id_ = server_cursor_id; }

  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t client_cursor_id_;
  uint32_t server_cursor_id_;
  LINK(ObCursorIdPair, cursor_id_pair_link_);
};

// client_cursor_id ----> ObCusorIdPair
struct ObCursorIdPairHashing
{
  typedef const uint32_t &Key;
  typedef ObCursorIdPair Value;
  typedef ObDLList(ObCursorIdPair, cursor_id_pair_link_) ListHead;

  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
  static Key key(Value const *value) { return value->client_cursor_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

typedef common::hash::ObBuildInHashMap<ObCursorIdPairHashing> ObCursorIdPairMap;

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CURSOR_STRUCT_H

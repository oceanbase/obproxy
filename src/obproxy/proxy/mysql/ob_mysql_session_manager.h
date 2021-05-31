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

#ifndef OBPROXY_MYSQL_SESSION_MANAGER_H
#define OBPROXY_MYSQL_SESSION_MANAGER_H

#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysql/ob_mysql_server_session.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlClientSession;
class ObMysqlSM;
class ObMysqlSessionManagerNew;

// A pool of server sessions.
//
// This is a continuation so that it can get callbacks from the server sessions.
// This is used to track remote closes on the sessions so they can be cleaned up.
//
// @internal Cleanup is the real reason we will always need an IP address mapping for the
// sessions. The I/O callback will have only the NetVC and thence the remote IP address
// for the closed session and we need to be able find it based on that.
class ObServerSessionPool : public event::ObContinuation
{
public:
  // Default constructor.
  // Constructs an empty pool.
  ObServerSessionPool(event::ObProxyMutex *mutex = NULL);
  virtual ~ObServerSessionPool() { }

  // Handle events from server sessions.
  int event_handler(int event, void *data);

public:
  static const int64_t HASH_BUCKET_SIZE = 16;

public:
  // Interface class for IP map.
  struct ObIPHashing
  {
    typedef const ObMysqlServerSessionHashKey Key;
    typedef ObMysqlServerSession Value;
    typedef ObDLList(ObMysqlServerSession, ip_hash_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key.server_ip_->sa_, sizeof(sockaddr), 0); }
    static Key key(Value const *value)
    {
      ObMysqlServerSessionHashKey key;
      if (OB_NOT_NULL(value)) {
        key = value->get_server_session_hash_key();
      }
      return key;
    }

    static bool equal(Key lhs, Key rhs) { return lhs.equal(rhs); }
  };

  typedef common::hash::ObBuildInHashMap<ObIPHashing, HASH_BUCKET_SIZE> IPHashTable; // Sessions by IP address.

public:
  // Get a session from the pool.
  // The session is selected based on address. If found the session
  // is removed from the pool.
  //
  // @return A pointer to the session or NULL if not matching session was found.
  ObMysqlServerSession *acquire_session(const ObMysqlServerSessionHashKey &key);
  ObMysqlServerSession *acquire_session(const sockaddr &addr, const ObString &auth_user);

  // Get a session by index. If found the session is kept in the pool
  ObMysqlServerSession *get_server_session(const int64_t index);

  int acquire_random_session(ObMysqlServerSession *&server_session);

  // Release a session to pool.
  int release_session(ObMysqlServerSession &ss);
  int64_t get_svr_session_count() const { return ip_pool_.count(); }

  // Close all sessions and then clear the table.
  void purge();
  void destroy();

  void set_hash_key(const ObString &hash_key);
  const ObString &get_hash_key() const { return hash_key_; }
  void set_session_manager(ObMysqlSessionManagerNew *session_manager) { session_manager_ = session_manager; }
  void set_delete_when_empty(bool is_delete_when_empty) { is_delete_when_empty_ = is_delete_when_empty; }

public:
  // Pools of server sessions.
  // Note that each server session is stored in both pools.
  IPHashTable ip_pool_;

  LINK(ObServerSessionPool, session_pool_hash_link_);

private:
  bool is_delete_when_empty_;
  ObMysqlSessionManagerNew *session_manager_;
  ObString hash_key_;
  char hash_key_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerSessionPool);
};

class ObMysqlSessionManager
{
public:
  ObMysqlSessionManager() { }
  ~ObMysqlSessionManager() { }

  int release_session(ObMysqlServerSession &to_release);
  void purge_keepalives() { session_pool_.purge(); }
  ObMysqlServerSession *get_server_session(const int64_t index) { return session_pool_.get_server_session(index); }
  int64_t get_svr_session_count() const { return session_pool_.get_svr_session_count(); }
  ObServerSessionPool &get_session_pool() { return session_pool_; }
  int acquire_server_session(const sockaddr &addr, const ObString &auth_user,
                             ObMysqlServerSession *&server_session);
  void set_mutex(event::ObProxyMutex *mutex) { session_pool_.mutex_ = mutex; }
  bool is_session_pool_full() const { return get_svr_session_count() >= MAX_SERVER_SESSION_COUNT; }

  //NOTE::client_session_ also hold one server session
  static const int64_t MAX_SERVER_SESSION_COUNT = 9;
private:
  ObServerSessionPool session_pool_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlSessionManager);
};

class ObMysqlSessionManagerNew
{
public:
  ObMysqlSessionManagerNew() { }
  ~ObMysqlSessionManagerNew() { mutex_.release(); }

public:
  static const int64_t HASH_BUCKET_SIZE = 16;

  // Interface class for IP map.
  struct ObServerSessionPoolHashing
  {
    typedef const ObString &Key;
    typedef ObServerSessionPool Value;
    typedef ObDLList(ObServerSessionPool, session_pool_hash_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(key.ptr(), key.length(), 0); }
    static Key key(Value const *value) {
      return value->get_hash_key();
    }
    static bool equal(Key lhs, Key rhs) {
      if (0 != lhs.case_compare(rhs)) {
        return false;
      }

      return true;
    }
  };
  typedef common::hash::ObBuildInHashMap<ObServerSessionPoolHashing, HASH_BUCKET_SIZE> SessionPoolHashTable; // Sessions by IP address.

public:
  SessionPoolHashTable &get_session_pool_hash() { return session_pool_hash_; }
  int release_session(const ObString &hash_key, ObMysqlServerSession &to_release);
  void purge_keepalives();
  int64_t get_svr_session_count();
  int acquire_server_session(const ObString &hash_key, const sockaddr &addr, const ObString &auth_user,
                             ObMysqlServerSession *&server_session);
  int acquire_random_session(const ObString &hash_key, ObMysqlServerSession *&server_session);
  void set_mutex(event::ObProxyMutex *mutex) { mutex_ = mutex; }

private:
  SessionPoolHashTable session_pool_hash_;
  common::ObPtr<event::ObProxyMutex> mutex_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlSessionManagerNew);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SESSION_MANAGER_H

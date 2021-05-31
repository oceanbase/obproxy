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

#define USING_LOG_PREFIX PROXY
#include "proxy/mysql/ob_mysql_session_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObServerSessionPool::ObServerSessionPool(ObProxyMutex *mutex)
   : ObContinuation(mutex), is_delete_when_empty_(false),
     session_manager_(NULL), hash_key_()
{
  MEMSET(hash_key_buf_, 0, sizeof(hash_key_buf_));
  SET_HANDLER(&ObServerSessionPool::event_handler);
}

void ObServerSessionPool::purge()
{
  IPHashTable::iterator last = ip_pool_.end();
  IPHashTable::iterator tmp_iter;
  for (IPHashTable::iterator spot = ip_pool_.begin(); spot != last;) {
    tmp_iter = spot;
    ++spot;
    tmp_iter->do_io_close();
  }
  ip_pool_.reset();

  if (is_delete_when_empty_) {
    destroy();
  }
}

void ObServerSessionPool::destroy()
{
  session_manager_ = NULL;
  hash_key_.reset();
  op_free(this);
}

ObMysqlServerSession *ObServerSessionPool::acquire_session(const ObMysqlServerSessionHashKey &key)
{
  return ip_pool_.remove(key);
}

ObMysqlServerSession *ObServerSessionPool::acquire_session(const sockaddr &addr, const ObString &auth_user)
{
  ObMysqlServerSessionHashKey key;
  ObIpEndpoint server_ip;
  ops_ip_copy(server_ip, addr);
  key.server_ip_ = &server_ip;
  key.auth_user_ = &auth_user;

  return acquire_session(key);

}

ObMysqlServerSession *ObServerSessionPool::get_server_session(const int64_t index)
{
  ObMysqlServerSession *ss_ret = NULL;
  if (OB_LIKELY(index >= 0) && OB_LIKELY(index < ip_pool_.count())) {
    IPHashTable::iterator last = ip_pool_.end();
    int64_t i = 0;
    bool found = false;
    for (IPHashTable::iterator spot = ip_pool_.begin(); !found && spot != last; ++spot) {
      if (i++ == index) {
        ss_ret = &(*spot);
        found = true;
      }
    }
  }

  return ss_ret;
}

int ObServerSessionPool::release_session(ObMysqlServerSession &ss)
{
  int ret = OB_SUCCESS;
  ss.state_ = MSS_KA_SHARED;
  ObNetVConnection *server_vc = NULL;
  // Now we need to issue a read on the connection to detect
  // if it closes on us. We will get called back in the
  // continuation for this bucket, ensuring we have the lock
  // to remove the connection from our lists
  if (OB_ISNULL(ss.do_io_read(this, INT64_MAX, ss.read_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("do_io_read error", K(ret));
  } else if (OB_ISNULL(ss.do_io_write(this, 0, NULL))) {
    // Transfer control of the write side as well
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("do_io_write error", K(ret));
  } else if(OB_ISNULL(server_vc = ss.get_netvc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server vc is null", K(ret));
  } else {
    // we probably don't need the active timeout set, but will leave it for now
    server_vc->set_inactivity_timeout(server_vc->get_inactivity_timeout());
    server_vc->set_active_timeout(server_vc->get_active_timeout());

    // put it in the pools.
    ret = ip_pool_.set_refactored(&ss);
    if (OB_SUCCESS == ret || OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("[release session] server session placed into shared pool",
                "ss_id", ss.ss_id_);
    } else {
      LOG_WARN("fail to release server session into shared pool", K(ret));
    }
  }

  return ret;
}

int ObServerSessionPool::acquire_random_session(ObMysqlServerSession *&server_session)
{
  int ret = OB_SUCCESS;
  server_session = NULL;
  ObMysqlServerSessionHashKey hash_key;
  int64_t idx = 0;
  const int64_t max_idx = get_svr_session_count() - 1;
  if (OB_FAIL(ObRandomNumUtils::get_random_num(0, max_idx, idx))) {
    LOG_DEBUG("fail to get random num", K(idx), K(max_idx), K(ret));
  } else {
    ObServerSessionPool::IPHashTable::iterator begin = ip_pool_.begin();
    ObServerSessionPool::IPHashTable::iterator end = ip_pool_.end();
    int64_t i = 0;
    while (i < idx && begin != end) {
      ++begin;
      ++i;
    }
    if (begin == end) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it arrived the end, it should not happened", K(idx), K(i), K(ret));
    } else {
      hash_key.local_ip_ = &begin->local_ip_;
      hash_key.server_ip_ = &begin->server_ip_;
      hash_key.auth_user_ = &begin->auth_user_;
      LOG_DEBUG("need use this server", K(hash_key), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(server_session = acquire_session(hash_key))) {
      ret = OB_SESSION_NOT_FOUND;
      LOG_WARN("fail to acquire session, it should not happened", K(hash_key));
    }
  }
  return ret;
}

// Called from the ObNetProcessor to let us know that a
// connection has closed down
int ObServerSessionPool::event_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  ObNetVConnection *net_vc = NULL;
  ObMysqlServerSession *ss = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data is null", K(ret));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
        // The server sent us data. This is unexpected so
        // close the connection
        // fallthrough
      case VC_EVENT_EOS:
      case VC_EVENT_ERROR:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
        net_vc = static_cast<ObNetVConnection*>((static_cast<ObVIO*>(data))->vc_server_);
        break;

      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid event", K(event), K(ret));
        break;
    }
  }

  if (OB_LIKELY(NULL != net_vc)) {
    ObMysqlServerSessionHashKey hash_key;
    ObIpEndpoint local_ip;
    ObIpEndpoint server_ip;
    ops_ip_copy(local_ip, net_vc->get_local_addr());
    ops_ip_copy(server_ip, net_vc->get_remote_addr());
    hash_key.local_ip_ = &local_ip;
    hash_key.server_ip_ = &server_ip;

    bool found = false;
    if (OB_LIKELY(NULL != (ss = ip_pool_.get(hash_key))) && OB_LIKELY(ss->get_netvc() == net_vc)) {
      // We've found our server session. Remove it from
      // our lists and close it down
      found = true;
      LOG_DEBUG("[session_pool] session received io notice ",
          K(event), "ss_id", ss->ss_id_, K(ss));
      if (OB_LIKELY(MSS_KA_SHARED == ss->state_)) {
        // Out of the pool! Now!
        if (OB_ISNULL(ip_pool_.remove(hash_key))) {
          //impossible happen here
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no server_session found in shared pool", K(ret));
        }
        // Drop connection on this end.
        ss->do_io_close();
      }
    }

    if (OB_UNLIKELY(!found)) {
      // We failed to find our session.  This can only be the result
      // of a programming flaw
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Connection leak from mysql keep-alive system", K(ret));
    }
  }

  if (0 == ip_pool_.count() && is_delete_when_empty_ && NULL != session_manager_) {
    session_manager_->get_session_pool_hash().remove(hash_key_);
    destroy();
  }

  return VC_EVENT_NONE;
}

void ObServerSessionPool::set_hash_key(const ObString &hash_key)
{
  const int32_t min_len = std::min(hash_key.length(), static_cast<int32_t>(OB_PROXY_FULL_USER_NAME_MAX_LEN));
  MEMCPY(hash_key_buf_, hash_key.ptr(), min_len);
  hash_key_.assign_ptr(hash_key_buf_, min_len);
}

int ObMysqlSessionManager::acquire_server_session(const sockaddr &addr, const ObString &auth_user,
                                                  ObMysqlServerSession *&server_session)
{
  int ret = OB_SESSION_NOT_FOUND;
  server_session = NULL;
  if (NULL != (server_session = session_pool_.acquire_session(addr, auth_user))) {
    ret = OB_SUCCESS;
    LOG_DEBUG("[acquire session] pool search successful");
  }
  return ret;
}

int ObMysqlSessionManager::release_session(ObMysqlServerSession &to_release)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(session_pool_.release_session(to_release))) {
    LOG_WARN("fail to release session to shared pool", K(ret));
  }
  return ret;
}

int64_t ObMysqlSessionManagerNew::get_svr_session_count()
{
  int64_t session_count = 0;

  if (session_pool_hash_.count() > 0) {
    SessionPoolHashTable::iterator last = session_pool_hash_.end();
    SessionPoolHashTable::iterator spot = session_pool_hash_.begin();
    for (; spot != last; ++spot) {
      session_count += spot->get_svr_session_count();
    }
  }

  return session_count;
}

int ObMysqlSessionManagerNew::acquire_server_session(const ObString &hash_key,
                                                       const sockaddr &addr, const ObString &auth_user,
                                                       ObMysqlServerSession *&server_session)
{
  int ret = OB_SESSION_NOT_FOUND;
  server_session = NULL;
  ObServerSessionPool *session_pool = NULL;

  if (OB_SUCCESS == session_pool_hash_.get_refactored(hash_key, session_pool)) {
    if (NULL != (server_session = session_pool->acquire_session(addr, auth_user))) {
      ret = OB_SUCCESS;
      LOG_DEBUG("[acquire session] pool search successful");
    }
  }
  return ret;
}

int ObMysqlSessionManagerNew::acquire_random_session(const ObString &hash_key,
                                                       ObMysqlServerSession *&server_session)
{
  int ret = OB_SESSION_NOT_FOUND;
  server_session = NULL;
  ObServerSessionPool *session_pool = NULL;

  if (OB_SUCCESS == session_pool_hash_.get_refactored(hash_key, session_pool)) {
    if (OB_SUCCESS == session_pool->acquire_random_session(server_session)) {
      ret = OB_SUCCESS;
      LOG_DEBUG("[acquire random session] pool search successful");
    }
  }
  return ret;
}

int ObMysqlSessionManagerNew::release_session(const ObString &hash_key, ObMysqlServerSession &to_release)
{
  int ret = OB_SUCCESS;
  ObServerSessionPool *session_pool = NULL;
  if (OB_FAIL(session_pool_hash_.get_refactored(hash_key, session_pool))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(session_pool = op_alloc_args(ObServerSessionPool, mutex_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else if (FALSE_IT(session_pool->set_delete_when_empty(true))) {
      } else if (FALSE_IT(session_pool->set_session_manager(this))) {
      } else if (FALSE_IT(session_pool->set_hash_key(hash_key))) {
      } else if (OB_FAIL(session_pool_hash_.unique_set(session_pool))) {
        LOG_WARN("fail to set session pool to hash table", K(ret));
      }
    } else {
      LOG_WARN("fail to get session pool", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(session_pool->release_session(to_release))) {
      LOG_WARN("fail to release session to shared pool", K(ret));
    }
  }
  return ret;
}

void ObMysqlSessionManagerNew::purge_keepalives()
{
  if (session_pool_hash_.count() > 0) {
    SessionPoolHashTable::iterator last = session_pool_hash_.end();
    SessionPoolHashTable::iterator iter = session_pool_hash_.begin();
    SessionPoolHashTable::iterator tmp_iter;
    for (; iter != last;) {
      tmp_iter = iter;
      ++iter;
      tmp_iter->purge();
    }
    session_pool_hash_.reset();
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

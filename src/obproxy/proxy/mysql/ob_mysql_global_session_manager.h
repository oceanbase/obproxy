/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_MYSQL_GLOBAL_SESSION_MANAGER_H_
#define OB_MYSQL_GLOBAL_SESSION_MANAGER_H_

#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "proxy/mysql/ob_mysql_server_session.h"
#include "proxy/mysql/ob_mysql_session_manager.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "obutils/ob_proxy_config.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlClientSession;
class ObMysqlClientSessionMap;
class ObMysqlSM;
class ObMysqlServerSession;
class ObMysqlServerSessionListPool;
/*
 * <dbkey, <serveraddr, SessionList>> use for store free server_session list
 * local_ip_pool used for show all session in pool
 */
class ObMysqlServerSessionList : public event::ObContinuation, public common::ObSharedRefCount
{
public:
  ObMysqlServerSessionList();
  virtual ~ObMysqlServerSessionList();
  void destroy() {op_free(this);}
  virtual void free() {destroy();}
  int init();
  int main_handler(int event, void *data);
  void purge_session_list();
  void do_kill_session();
  void reset();
  //add when server_session create
  // int add_server_session(ObMysqlServerSession* server_session);
  // remove when server_ession do_io_close()
  // int remove_server_session(const ObMysqlServerSession* server_session);
  void remove_from_list_and_pool(ObMysqlServerSession* server_session);
  ObMysqlServerSession* acquire_first_from_list();
  ObMysqlServerSession* acquire_matched_from_list(const ObServerSessionMatchRules &r);
  int release_to_list(ObMysqlServerSession& server_session);
  int do_pool_log(const ObProxySchemaKey& schema_key, bool force_log = false);
  common::DRWLock &get_ss_list_rwlock() { return ss_list_rwlock_; }
public:
  static const int64_t HASH_BUCKET_SIZE = 16;
  struct ObLocalIPHashing
  {
    typedef const ObMysqlServerSessionHashKey Key;
    typedef ObMysqlServerSession Value;
    typedef ObDLList(ObMysqlServerSession, dbkey_local_ip_hash_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key.local_ip_->sa_, sizeof(sockaddr), 0); }
    static Key key(Value const *value) {
      ObMysqlServerSessionHashKey key;
      if (OB_NOT_NULL(value)) {
        key = value->get_server_session_hash_key();
      }
      return key;
    }
    static bool equal(Key lhs, Key rhs) { return lhs.equal(rhs); }
  };
  //client ip HashTable,using for show all the session
  typedef common::hash::ObBuildInHashMap<ObLocalIPHashing, HASH_BUCKET_SIZE> LocalIPHashTable;
private:
 // 不能对 ss_list_rwlock_ 加写锁后, 向 mutex_ 加锁, 会导致锁依赖死锁
 // 因为 main_handler 执行会先加 mutex_ 锁, 再加 ss_lit_rwlock_
  common::DRWLock  ss_list_rwlock_;
public:
  net::ObIpEndpoint server_ip_;
  net::ObIpEndpoint local_ip_;
  oceanbase::obproxy::obutils::ObProxyConfigString auth_user_;
  ObCommonAddr common_addr_;
  common::ObAtomicList server_session_list_;
  LocalIPHashTable local_ip_pool_;
  int64_t idle_count_; // live_count is all count, include now is being used
  int64_t last_log_time_;
  event::ObProxyMutex m_;

  ObMysqlServerSessionListPool *pool_; // 持有当前 List 的 ObMysqlServerSessionListPool 指针
public:
  LINK(ObMysqlServerSessionList, ip_hash_link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlServerSessionList);
};
class ObMysqlServerSessionListPool : public common::ObSharedRefCount
{
public:
  ObMysqlServerSessionListPool();
  ~ObMysqlServerSessionListPool();
  int init(const ObProxySchemaKey& schema_key);
  void destroy() {op_free(this);}
  virtual void free() {destroy();}
  int acquire_ss_list(const ObCommonAddr& key, ObMysqlServerSessionList* &ss_list);
  int acquire_server_session(const ObCommonAddr &key,
                             ObMysqlServerSession* &server_session,
                             ObServerSessionMatchRules *rules);
  int release_server_session(ObMysqlServerSession &ss);
  int purge_session_list_pool();
  int do_kill_session();
  int do_kill_session_by_ssid(int64_t ss_id);
  int64_t incr_idle_session_count();
  int64_t decr_idle_session_count();
  int64_t get_current_session_conn_count(const ObCommonAddr& key);
  int add_server_addr_if_not_exist(const ObCommonAddr& common_addr);
  int add_server_addr_if_not_exist(const common::ObString& server_ip, int32_t server_port, bool is_physical);
  int remove_server_addr_if_exist(const common::ObString& server_ip, int32_t server_port, bool is_physical);
  int remove_server_addr_if_exist(const ObCommonAddr& common_addr);
  int incr_fail_count(const ObCommonAddr& addr);
  void reset_fail_count(const ObCommonAddr& addr);
  int get_fail_count(const ObCommonAddr& addr);
public:
  static const int64_t HASH_BUCKET_SIZE = 32;
  // Interface class for IP map.
  struct ObIPHashing
  {
    typedef const ObCommonAddr &Key;
    typedef ObMysqlServerSessionList Value;
    typedef ObDLList(ObMysqlServerSessionList, ip_hash_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash();}
    static Key key(Value const *value) { return value->common_addr_; }
    static bool equal(Key lhs, Key rhs) {
      return lhs.equals(rhs);
    }
  };
  TO_STRING_KV(K_(idle_session_count), K_(schema_key));
  typedef common::hash::ObBuildInHashMap<ObIPHashing, HASH_BUCKET_SIZE> IPHashTable; // Sessions by Server IP address.
  common::DRWLock  rwlock_;
  common::DRWLock  sig_rwlock_;
  IPHashTable server_session_list_pool_;
  ObProxySchemaKey schema_key_;
  int64_t idle_session_count_;
  ObMysqlSchemaServerAddrInfo* schema_server_addr_info_;
  LINK(ObMysqlServerSessionListPool, session_list_pool_);
};

class ObMysqlGlobalSessionManager
{
public:
  static const int64_t SESSIONPOOLLIST_HASH_BUCKET_SIZE = 64;
  static const int64_t HASH_BUCKET_SIZE = 16;
  ObMysqlGlobalSessionManager() {};
  ~ObMysqlGlobalSessionManager();
  //add when server_session create
  // int add_server_session(ObMysqlServerSession& ss);
  // remove when server_ession do_io_close()
  // int remove_server_session(const ObMysqlServerSession& ss);

  int acquire_server_session(const ObProxySchemaKey& schema_key,
                             const ObCommonAddr& addr,
                             ObMysqlServerSession *&server_session,
                             ObServerSessionMatchRules *rules = NULL);
  int release_server_session(ObMysqlServerSession &to_release);
  int purge_session_manager_keepalives(const common::ObString& dbkey);
  int64_t get_current_session_conn_count(const common::ObString& dbkey,
                                         const ObCommonAddr& common_addr);
  ObMysqlServerSessionListPool* get_server_session_list_pool(const common::ObString& dbkey);

  int add_schema_if_not_exist(const ObProxySchemaKey& schema_key,
                              ObMysqlServerSessionListPool* &server_session_list_pool);
  int remove_schema_if_exist(const ObProxySchemaKey& schema_key);

  int add_server_addr_if_not_exist(const ObProxySchemaKey& schema_key,
                                   const common::ObString& server_ip,
                                   int32_t server_port,
                                   bool is_physical = true);

  int incr_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr);
  void reset_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr);
  int32_t get_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr);

  ObMysqlSchemaServerAddrInfo* acquire_scheme_server_addr_info(const ObProxySchemaKey& schema_key);
  int get_all_session_list_pool(common::ObIArray<ObMysqlServerSessionListPool*> &all_session_list_pool);
public:
  struct ObSessionPoolListHashing
  {
    typedef const common::ObString &Key;
    typedef ObMysqlServerSessionListPool Value;
    typedef ObDLList(ObMysqlServerSessionListPool, session_list_pool_) ListHead;
    static uint64_t hash(Key key) {return key.hash();}
    static Key key(Value const *value) {return value->schema_key_.dbkey_.config_string_;}
    static bool equal(Key lhs, Key rhs) {return lhs == rhs;}
  };
  common::DRWLock rwlock_;
  typedef common::hash::ObBuildInHashMap<ObSessionPoolListHashing,
          SESSIONPOOLLIST_HASH_BUCKET_SIZE> SessionPoolListHashTable;
  SessionPoolListHashTable global_session_pool_;
};
class SchemaKeyConnInfo
{
public:
  SchemaKeyConnInfo() {}
  ~SchemaKeyConnInfo() {}
public:
  ObProxySchemaKey schema_key_;
  proxy::ObCommonAddr addr_;
  int64_t conn_count_;
  DECLARE_TO_STRING;
  LINK(SchemaKeyConnInfo, link_);
};

template <typename T>
class ObMysqlContJobList
{
public:
  ObMysqlContJobList();
  ~ObMysqlContJobList();
  int init();
  int32_t count() {return list_count_;}
  void push(T* elem);
  T* pop();
public:
  int32_t list_count_;
  common::ObAtomicList pending_list_;
};
template <typename T>
ObMysqlContJobList<T>::ObMysqlContJobList() :list_count_(0)
{
  init();
}
template <typename T>
ObMysqlContJobList<T>::~ObMysqlContJobList()
{

}
template <typename T>
int ObMysqlContJobList<T>::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pending_list_.init("job pending list",
                                        reinterpret_cast<int64_t>(&(reinterpret_cast<T*>(0))->link_)))) {
    PROXY_LOG(WDIAG, "fail to init pending_list", K(ret));
  }
  return ret;
}
template <typename T>
void ObMysqlContJobList<T>::push(T* elem)
{
  if (OB_ISNULL(elem)) {
    PROXY_LOG(WDIAG, "elem should not null");
    return;
  }
  if (list_count_ >= oceanbase::obproxy::obutils::get_global_proxy_config().max_pending_num) {
    PROXY_LOG(WDIAG, "list is full, drop it");
    op_free(elem);
  }
  pending_list_.push(elem);
  ATOMIC_INC(&list_count_);
}
template <typename T>
T* ObMysqlContJobList<T>::pop()
{
  T* elem = (T*)pending_list_.pop();
  if (!OB_ISNULL(elem)) {
    ATOMIC_DEC(&list_count_);
  }
  return elem;
}
extern ObMysqlGlobalSessionManager& get_global_session_manager();
extern ObMysqlContJobList<ObProxySchemaKey>& get_global_schema_key_job_list();
extern ObMysqlContJobList<SchemaKeyConnInfo>& get_global_server_conn_job_list();


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_MYSQL_GLOBAL_SESSION_MANAGER_H_

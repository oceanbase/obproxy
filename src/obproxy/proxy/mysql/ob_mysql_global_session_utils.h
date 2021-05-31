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

#ifndef OB_MYSQL_GLOBAL_SESSION_UTILS_H_
#define OB_MYSQL_GLOBAL_SESSION_UTILS_H_
#include "lib/net/ob_addr.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/lock/ob_drw_lock.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_proxy_string_utils.h"
#include "iocore/net/ob_inet.h"
#include "dbconfig/ob_proxy_db_config_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const char* DEFAULT_LOGIC_TENANT_NAME = "DEFAULT";
class ObMysqlClientSession;

/*
 * struct store informaton for ob no sharding
 */
enum ConnectorType {
  TYPE_SINGLE_CONNECTOR = 0,
  TYPE_SHARD_CONNECTOR,
  TYPE_CONNECTOR_MAX
};
/*
 * struct store the informaton for dbkey
 */
class ObProxySchemaKey
{
public:
  ObProxySchemaKey();
  ~ObProxySchemaKey();
  void reset();
  ObProxySchemaKey& operator=(const ObProxySchemaKey& key);
  void set_shard_connector(dbconfig::ObShardConnector* shard_conn, ConnectorType type);

  inline common::ObString get_shard_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->shard_name_;
    }
    return str;
  }

  inline common::ObString get_cluster_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->cluster_name_;
    }
    return str;
  }

  inline common::ObString get_tenant_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->tenant_name_;
    }
    return str;
  }

  inline common::ObString get_full_user_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->full_username_;
    }
    return str;
  }
  inline common::ObString get_user_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->username_;
    }
    return str;
  }

  inline common::ObString get_database_name() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->database_name_;
    }
    return str;
  }

  inline common::ObString get_password() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->password_;
    }
    return str;
  }
  inline common::ObString get_physical_ip() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->physic_addr_;
    }
    return str;
  }

  inline common::ObString get_pyhsical_port() const
  {
    common::ObString str;
    if (NULL != shard_conn_) {
      str = shard_conn_->physic_port_;
    }
    return str;
  }

  inline common::DBServerType get_db_server_type() const
  {
    common::DBServerType type = common::DB_MAX;
    if (NULL != shard_conn_) {
      type = shard_conn_->server_type_;
    }
    return type;
  }

  inline ConnectorType get_connector_type() const {
    return connector_type_;
  }

  obutils::ObProxyVariantString logic_tenant_name_;
  obutils::ObProxyVariantString logic_database_name_;
  obutils::ObProxyVariantString dbkey_; //used for map key
  ConnectorType connector_type_;
  dbconfig::ObShardConnector* shard_conn_;
  int64_t last_access_time_;
  bool init_;
  LINK(ObProxySchemaKey, link_);
  DECLARE_TO_STRING;
};

class ObCommonAddr
{
public:
  ObCommonAddr() {reset(); }
  ~ObCommonAddr() { reset(); }
public:
  void reset()
  {
    is_physical_ = true;
    addr_.reset();
    port_ = 0;
    ip_endpoint_.reset();
  }
  uint64_t hash() const
  {
    uint64_t hash_val;
    if (is_physical_) {
      hash_val = ip_endpoint_.hash();
    } else {
      hash_val = addr_.config_string_.hash();
    }
    return hash_val;
  }
  ObCommonAddr& operator=(const ObCommonAddr& common_addr)
  {
    if (this != &common_addr) {
      is_physical_ = common_addr.is_physical_;
      if (is_physical_) {
        ip_endpoint_ = common_addr.ip_endpoint_;
      } else {
        addr_.set_value(common_addr.addr_.config_string_);
        port_ = common_addr.port_;
      }
    }
    return *this;
  }
  int get_sockaddr(sockaddr& sa);
  int assign(const sockaddr &addr);
  int assign(const common::ObString& ip, const common::ObString& port, bool is_physical);
  int assign(const common::ObString& ip, int32_t port, bool is_physical);

  bool equals(const ObCommonAddr& rv) const
  {
    if (is_physical_ != rv.is_physical_) {
      return false;
    }
    if (is_physical_) {
      if (!net::ops_ip_addr_port_eq(ip_endpoint_.sa_, rv.ip_endpoint_.sa_)) {
        return false;
      }
    } else {
      if (!addr_.equals(rv.addr_)) {
        return false;
      }
      if (port_ != rv.port_) {
        return false;
      }
    }
    return true;
  }
  TO_STRING_KV(K_(is_physical), K_(addr), K_(port), K_(ip_endpoint));
public:
  bool is_physical_;
  obutils::ObProxyVariantString addr_;
  int32_t port_;
  net::ObIpEndpoint ip_endpoint_;
};
class ObServerAddrInfo
{
public:
  ObServerAddrInfo();
  ~ObServerAddrInfo()
  {
    reset();
  }
  void reset()
  {
    addr_.reset();
    fail_count_= 0;
  }
  bool operator==(const ObServerAddrInfo &rv) const {
    bool bret = true;
    if (!addr_.equals(rv.addr_)) {
      bret = false;
    } else if (fail_count_ != rv.fail_count_) {
      bret = false;
    }
    return bret;
  }
  TO_STRING_KV(K_(addr), K_(fail_count));
  void incr_fail_count();
  void reset_fail_count() {fail_count_ = 0;}
  bool reach_max_fail_count();
  int32_t get_fail_count() {return fail_count_;}
  ObCommonAddr addr_;
  int32_t fail_count_;
  LINK(ObServerAddrInfo, server_addr_link_);
};

/*
 * dbkey server addr infos;
 * use a map store all the server addr for dbkey
 * */
class ObMysqlSchemaServerAddrInfo: public common::ObSharedRefCount
{
public:
  ObMysqlSchemaServerAddrInfo(const ObProxySchemaKey& schema_key);
  ~ObMysqlSchemaServerAddrInfo();
  void destroy() {op_free(this);}
  virtual void free() {destroy();}
  int add_server_addr_if_not_exist(const common::ObString& server_ip, int32_t server_port, bool is_physical = true);
  int add_server_addr_if_not_exist(const ObCommonAddr& addr);

  int remove_server_addr_if_exist(const ObCommonAddr& addr);
  int remove_server_addr_if_exist(const common::ObString& server_ip, int32_t server_port, bool is_physical = true);

  int incr_fail_count(const ObCommonAddr& addr);
  void reset_fail_count(const ObCommonAddr& addr);
  int32_t get_fail_count(const ObCommonAddr& addr);

public:
  static const int64_t HASH_BUCKET_SIZE = 16;
  struct ObServerAddrHashing
  {
    typedef const ObCommonAddr &Key;
    typedef ObServerAddrInfo Value;
    typedef ObDLList(ObServerAddrInfo, server_addr_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value const *value) { return value->addr_; }
    static bool equal(Key lhs, Key rhs) { return lhs.equals(rhs); }
  };
  typedef common::hash::ObBuildInHashMap<ObServerAddrHashing, HASH_BUCKET_SIZE> ServerAddrHashTable;
  ObProxySchemaKey schema_key_;
  ServerAddrHashTable server_addr_map_;
  common::DRWLock rwlock_;
  LINK(ObMysqlSchemaServerAddrInfo, server_addr_info_link_);
};

class ObMysqlSessionUtils {
public:
  static int64_t get_session_max_conn(const ObProxySchemaKey& schema_key);
  static int64_t get_session_min_conn(const ObProxySchemaKey& schema_key);
  static int64_t get_session_idle_timeout_ms(const ObProxySchemaKey& schema_key);
  static int64_t get_session_blocking_timeout_ms(const ObProxySchemaKey& schema_key);
  static bool get_session_prefill(const ObProxySchemaKey& schema_key);
  static int make_full_username(char* buf, int buf_len,
                                const common::ObString& user_name,
                                const common::ObString& tenant_name,
                                const common::ObString& cluster_name);

  static int make_session_dbkey(ObProxySchemaKey& schema_key, char* dbkey_buf, int buf_len);

  static int format_full_username(ObProxySchemaKey& schema_key, char* buf, int buf_len);

  static int init_schema_key_value(ObProxySchemaKey& schema_key,
                                   const common::ObString& user_name,
                                   const common::ObString& tenant_name,
                                   const common::ObString& cluster_name,
                                   const common::ObString& database_name,
                                   const common::ObString& logic_tenant_name);

  static int init_schema_key_value(ObProxySchemaKey& schema_key,
                                   const common::ObString& logic_tenant_name,
                                   const common::ObString& logic_database_name,
                                   dbconfig::ObShardConnector* shard_conn);

  static int init_schema_key_with_client_session(ObProxySchemaKey& schema_key,
      ObMysqlClientSession* client_session);

  static int init_common_addr_with_client_session(ObCommonAddr& common_addr,
      const net::ObIpEndpoint& server_ip,
      ObMysqlClientSession* client_session);

  static int get_session_prop(const ObProxySchemaKey& schema_key, dbconfig::ObShardProp* & shard_prop);

  static int get_sockaddr_by_ip_port(const common::ObString& ip_str, int32_t port, bool is_physical, sockaddr& addr);

};
class GlobalSingleConnectorMap {
public:
  GlobalSingleConnectorMap() { }
  ~GlobalSingleConnectorMap() { destroy(); }
  void destroy();
  struct ObSingleConnectorHashing
  {
    typedef const common::ObString &Key;
    typedef dbconfig::ObShardConnector Value;
    typedef ObDLList(dbconfig::ObShardConnector, link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->shard_name_.config_string_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  int add_connector_if_not_exists(dbconfig::ObShardConnector* connector);
  int get_connector(const common::ObString& name, dbconfig::ObShardConnector* &connector);
  typedef common::hash::ObBuildInHashMap<ObSingleConnectorHashing, dbconfig::ObDbConfigChild::DB_HASH_BUCKET_SIZE> SingleConnectorHashMap;
  SingleConnectorHashMap single_connector_map_;
  common::DRWLock rwlock_;
private:
  DISALLOW_COPY_AND_ASSIGN(GlobalSingleConnectorMap);
};

extern GlobalSingleConnectorMap& get_global_single_connector_map();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_MYSQL_GLOBAL_SESSION_UTILS_H_

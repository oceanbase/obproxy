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
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/time/ob_hrtime.h"
#include "iocore/net/ob_inet.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_client_session.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const int MAX_FAIL_COUNT = 3;

ObProxySchemaKey::ObProxySchemaKey(): shard_conn_(NULL)
{
  reset();
}
DEF_TO_STRING(ObProxySchemaKey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(logic_tenant_name), K_(logic_database_name),
       K_(dbkey), K_(connector_type), KPC(shard_conn_), K_(last_access_time), K_(init));
  J_OBJ_END();
  return pos;
}
ObProxySchemaKey& ObProxySchemaKey::operator=(const ObProxySchemaKey& key)
{
  if (this != &key) {
    logic_tenant_name_.set_value(key.logic_tenant_name_);
    logic_database_name_.set_value(key.logic_database_name_);
    dbkey_.set_value(key.dbkey_);
    set_shard_connector(key.shard_conn_, key.connector_type_);
    last_access_time_ = key.last_access_time_;
    init_ = key.init_;
  }
  return *this;
}

void ObProxySchemaKey::set_shard_connector(dbconfig::ObShardConnector* shard_conn, ConnectorType type) {
  connector_type_ = type;
  if (shard_conn_ != shard_conn) {
   if (NULL != shard_conn_) {
      shard_conn_->dec_ref();
      shard_conn_ = NULL;
    }
    if (NULL != shard_conn) {
      shard_conn_ = shard_conn;
      shard_conn_->inc_ref();
    }
  }
}
void ObProxySchemaKey::reset()
{
  logic_tenant_name_.reset();
  logic_database_name_.reset();
  dbkey_.reset();
  connector_type_ = TYPE_CONNECTOR_MAX;
  last_access_time_ = 0;
  init_ = false;
  if (NULL != shard_conn_) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }
}
ObProxySchemaKey::~ObProxySchemaKey()
{
  reset();
}
int ObCommonAddr::assign(const sockaddr &addr)
{
  int ret = common::OB_SUCCESS;
  is_physical_ = true;
  ip_endpoint_.assign(addr);
  return ret;
}
int ObCommonAddr::assign(const common::ObString& ip_str, const common::ObString& port_str, bool is_physical)
{
  int ret = OB_SUCCESS;
  int64_t port = 0;
  if (OB_FAIL(get_int_value(port_str, port))) {
    LOG_WDIAG("get_int_value failed", K(ret), K(port_str));
  } else {
    ret = assign(ip_str, static_cast<int32_t>(port), is_physical);
  }
  return ret;
}

int ObCommonAddr::assign(const common::ObString& ip_str, const int32_t port, bool is_physical)
{
  int ret = OB_SUCCESS;
  if (is_physical) {
    sockaddr sa;
    if (OB_FAIL(ObMysqlSessionUtils::get_sockaddr_by_ip_port(ip_str, port, true, sa))) {
      LOG_WDIAG("get_sockaddr_by_ip_port failed", K(ret));
    } else {
      ip_endpoint_.assign(sa);
    }
  } else {
    addr_.set_value(ip_str);
    port_ = static_cast<int32_t>(port);
  }
  is_physical_ = is_physical;
  return ret;
}
int ObCommonAddr::get_sockaddr(sockaddr& sa)
{
  int ret = OB_SUCCESS;
  if (is_physical_) {
    sa = ip_endpoint_.sa_;
  } else if (OB_FAIL(ObMysqlSessionUtils::get_sockaddr_by_ip_port(addr_.config_string_, port_, false, sa))) {
    LOG_WDIAG("get_sockaddr_by_ip_port failed", K(ret));
  }
  return ret;
}

ObMysqlSchemaServerAddrInfo::ObMysqlSchemaServerAddrInfo(const ObProxySchemaKey& schema_key)
{
  schema_key_ = schema_key;
}
ObMysqlSchemaServerAddrInfo::~ObMysqlSchemaServerAddrInfo()
{
  ServerAddrHashTable::iterator last = server_addr_map_.end();
  for (ServerAddrHashTable::iterator spot = server_addr_map_.begin(); spot != last; ++spot) {
    ObServerAddrInfo* addr_info = &(*spot);
    op_free(addr_info);
  }
  server_addr_map_.reset();
}

ObServerAddrInfo::ObServerAddrInfo()
{
  reset();
}
bool ObServerAddrInfo::reach_max_fail_count()
{
  return fail_count_ >= MAX_FAIL_COUNT;
}
void ObServerAddrInfo::incr_fail_count()
{
  ATOMIC_AAF(&fail_count_, 1);
}

int ObMysqlSchemaServerAddrInfo::remove_server_addr_if_exist(const common::ObString& server_ip,
    int32_t server_port, bool is_physical)
{
  int ret = OB_SUCCESS;
  ObCommonAddr common_addr;
  common_addr.assign(server_ip, server_port, is_physical);
  ret = remove_server_addr_if_exist(common_addr);
  return ret;
}

int ObMysqlSchemaServerAddrInfo::remove_server_addr_if_exist(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(rwlock_);
  ObServerAddrInfo* addr_info = NULL;
  if (OB_ISNULL(addr_info = server_addr_map_.remove(addr))) {
    LOG_INFO("not in map", K(addr), K(schema_key_.dbkey_));
  } else {
    LOG_DEBUG("remove success", K(schema_key_.dbkey_), K(addr));
    op_free(addr_info);
    addr_info= NULL;
  }
  return ret;
}

int ObMysqlSchemaServerAddrInfo::add_server_addr_if_not_exist(const common::ObString& server_ip,
  int32_t server_port, bool is_physical)
{
  int ret = OB_SUCCESS;
  ObCommonAddr common_addr;
  common_addr.assign(server_ip, server_port, is_physical);
  ret = add_server_addr_if_not_exist(common_addr);
  return ret;
}

int ObMysqlSchemaServerAddrInfo::add_server_addr_if_not_exist(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  ObServerAddrInfo* addr_info = NULL;
  if (OB_FAIL(server_addr_map_.get_refactored(addr, addr_info))) {
    DRWLock::WRLockGuard guard(rwlock_);
    if (OB_FAIL(server_addr_map_.get_refactored(addr, addr_info))) {
      if (OB_ISNULL(addr_info = op_alloc(ObServerAddrInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate ObServerAddrInfo", K(schema_key_.dbkey_), K(addr));
      } else {
        addr_info->addr_ = addr;
        if (OB_FAIL(server_addr_map_.unique_set(addr_info))) {
          LOG_WDIAG("add to map failed", K(addr), K(ret));
          ret = OB_ERR_UNEXPECTED;
          op_free(addr_info);
          addr_info = NULL;
        } else {
          LOG_DEBUG("add to server_addr_map_ succ", K(schema_key_.dbkey_), K(addr));
        }
      }
    } else {
      LOG_DEBUG("already exist, no need add addr", K(schema_key_.dbkey_), K(addr));
    }
  } else {
    LOG_DEBUG("already exist, no need add addr", K(schema_key_.dbkey_), K(addr));
  }
  return ret;
}

int ObMysqlSchemaServerAddrInfo::incr_fail_count(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  ObServerAddrInfo* addr_info = NULL;
  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_FAIL(server_addr_map_.get_refactored(addr, addr_info))) {
    LOG_WDIAG("not in map", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(addr_info)) {
    LOG_WDIAG("addr_info can not be null here", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    addr_info->incr_fail_count();
    LOG_DEBUG("now fail count is", K(addr_info->fail_count_));
  }
  return ret;
}

void ObMysqlSchemaServerAddrInfo::reset_fail_count(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  ObServerAddrInfo* addr_info = NULL;
  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_FAIL(server_addr_map_.get_refactored(addr, addr_info))) {
    LOG_WDIAG("not in map", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(addr_info)) {
    LOG_WDIAG("addr_info can not be null here", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    addr_info->reset_fail_count();
    LOG_DEBUG("now fail count is", K(addr_info->fail_count_));
  }
}
int32_t ObMysqlSchemaServerAddrInfo::get_fail_count(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  int32_t fail_count = 0;
  ObServerAddrInfo* addr_info = NULL;
  DRWLock::RDLockGuard guard(rwlock_); // maybe not need lock but use for safe
  if (OB_FAIL(server_addr_map_.get_refactored(addr, addr_info))) {
    LOG_WDIAG("not in map", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(addr_info)) {
    LOG_WDIAG("addr_info can not be null here", K(schema_key_.dbkey_), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    fail_count = addr_info->get_fail_count();
  }
  LOG_DEBUG("now fail count is", K(fail_count));
  return fail_count;
}

int ObMysqlSessionUtils::get_session_prop(const ObProxySchemaKey& schema_key, dbconfig::ObShardProp* & shard_prop)
{
  const ObString& logic_tenant_name = schema_key.logic_tenant_name_.config_string_;
  const ObString& logic_database_name = schema_key.logic_database_name_.config_string_;
  const ObString& shard_name = schema_key.get_shard_name();
  if (TYPE_SHARD_CONNECTOR == schema_key.get_connector_type()) {
    return get_global_dbconfig_cache().get_shard_prop(logic_tenant_name,
            logic_database_name, shard_name, shard_prop);
  } else {
    return OB_SHARD_CONF_INVALID;
  }
}
int64_t ObMysqlSessionUtils::get_session_max_conn(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  int64_t max_conn = 0;
  if (!get_global_proxy_config().use_local_session_prop) {
    dbconfig::ObShardProp* shard_prop = NULL;
    if (OB_SUCC(get_session_prop(schema_key, shard_prop))) {
      max_conn = shard_prop->get_max_conn();
      shard_prop->dec_ref();
      shard_prop = NULL;
    } else {
      //for no sharding
      max_conn = get_global_proxy_config().session_pool_default_max_conn;
    }
  } else {
    max_conn = get_global_proxy_config().session_pool_default_max_conn;
  }
  return max_conn;
}
int64_t ObMysqlSessionUtils::get_session_min_conn(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  int64_t min_conn = 0;
  if (!get_global_proxy_config().use_local_session_prop) {
    dbconfig::ObShardProp* shard_prop = NULL;
    if (OB_SUCC(get_session_prop(schema_key, shard_prop))) {
      min_conn = shard_prop->get_min_conn();
      shard_prop->dec_ref();
      shard_prop = NULL;
    } else {
      // for no shrading
      min_conn = get_global_proxy_config().session_pool_default_min_conn;
    }
  } else {
    min_conn = get_global_proxy_config().session_pool_default_min_conn;
  }
  return min_conn;
}
int64_t ObMysqlSessionUtils::get_session_blocking_timeout_ms(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  int64_t blocking_timeout = 0;
  if (!get_global_proxy_config().use_local_session_prop) {
    dbconfig::ObShardProp* shard_prop = NULL;
    if (OB_SUCC(get_session_prop(schema_key, shard_prop))) {
      blocking_timeout = HRTIME_MSECONDS(shard_prop->get_blocking_timeout());
      shard_prop->dec_ref();
      shard_prop = NULL;
    } else {
      //for no sharding
      blocking_timeout = HRTIME_USECONDS(get_global_proxy_config().session_pool_default_blocking_timeout);
    }
  } else {
    blocking_timeout = HRTIME_USECONDS(get_global_proxy_config().session_pool_default_blocking_timeout);
  }
  return blocking_timeout;
}

int64_t ObMysqlSessionUtils::get_session_idle_timeout_ms(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  int64_t idle_timeout = 0;
  if (!get_global_proxy_config().use_local_session_prop) {
    dbconfig::ObShardProp* shard_prop = NULL;
    if (OB_SUCC(get_session_prop(schema_key, shard_prop))) {
      idle_timeout = HRTIME_MSECONDS(shard_prop->get_idle_timeout());
      shard_prop->dec_ref();
      shard_prop = NULL;
    } else {
      idle_timeout = HRTIME_USECONDS(get_global_proxy_config().session_pool_default_idle_timeout);
    }
  } else {
    idle_timeout = HRTIME_USECONDS(get_global_proxy_config().session_pool_default_idle_timeout);
  }
  return idle_timeout;
}
bool ObMysqlSessionUtils::get_session_prefill(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  bool prefill = false;
  if (!get_global_proxy_config().use_local_session_prop) {
    dbconfig::ObShardProp* shard_prop = NULL;
    if (OB_SUCC(get_session_prop(schema_key, shard_prop))) {
      prefill = shard_prop->get_need_prefill();
      shard_prop->dec_ref();
      shard_prop = NULL;
    } else {
      // for no sharding can not prefill
    }
  } else {
    prefill = get_global_proxy_config().session_pool_default_prefill;
  }
  return prefill;
}
int ObMysqlSessionUtils::make_full_username(char* buf, int buf_len,
                                            const ObString& user_name,
                                            const ObString& tenant_name,
                                            const ObString& cluster_name)
{
  int ret = OB_SUCCESS;
  snprintf(buf, buf_len, "%.*s@%.*s#%.*s",
           user_name.length(), user_name.ptr(),
           tenant_name.length(), tenant_name.ptr(),
           cluster_name.length(), cluster_name.ptr());
  return ret;
}
int ObMysqlSessionUtils::make_session_dbkey(ObProxySchemaKey& schema_key, char* dbkey_buf, int buf_len)
{
  int ret = OB_SUCCESS;
  ObShardConnector* shard_conn = schema_key.shard_conn_;
  if (OB_ISNULL(shard_conn)) {
      LOG_WDIAG("shard_conn is null");
      ret = OB_ERR_UNEXPECTED;
  } else if (TYPE_SHARD_CONNECTOR == schema_key.connector_type_) {
    common::ObString& logic_database_name = schema_key.logic_database_name_.config_string_;
    common::ObString& logic_tenant_name = schema_key.logic_tenant_name_.config_string_;
    common::ObString& shard_name = shard_conn->shard_name_.config_string_;
    snprintf(dbkey_buf, buf_len, "%.*s-%.*s-%.*s",
           logic_tenant_name.length(), logic_tenant_name.ptr(),
           logic_database_name.length(), logic_database_name.ptr(),
           shard_name.length(), shard_name.ptr());
  } else if (TYPE_SINGLE_CONNECTOR == schema_key.connector_type_) {
    common::ObString& user_name = shard_conn->username_;
    common::ObString& tenant_name = shard_conn->tenant_name_;
    common::ObString& cluster_name = shard_conn->cluster_name_;
    make_full_username(dbkey_buf, buf_len, user_name, tenant_name, cluster_name);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("type is invalid", K(schema_key.connector_type_));
  }
  return ret;
}
/*
 * user@tenant#cluster_name
 * is used for session auth_user format
 */
int ObMysqlSessionUtils::format_full_username(ObProxySchemaKey& schema_key, char* buf, int buf_len) {
  int ret = OB_SUCCESS;
  ObString user_name = schema_key.get_user_name();
  ObString tenant_name = schema_key.get_tenant_name();
  ObString cluster_name = schema_key.get_cluster_name();
  snprintf(buf, buf_len, "%.*s@%.*s#%.*s",
           user_name.length(), user_name.ptr(),
           tenant_name.length(), tenant_name.ptr(),
           cluster_name.length(), cluster_name.ptr());
  return ret;
}
int ObMysqlSessionUtils::init_schema_key_value(ObProxySchemaKey& schema_key,
    const common::ObString& user_name,
    const common::ObString& tenant_name,
    const common::ObString& cluster_name,
    const common::ObString& database_name,
    const common::ObString& logic_tenant_name)
{
  int ret = OB_SUCCESS;
  UNUSED(database_name);
  char full_user_buf[1024];
  snprintf(full_user_buf, 1024, "%.*s:%.*s:%.*s",
           cluster_name.length(), cluster_name.ptr(),
           tenant_name.length(), tenant_name.ptr(),
           user_name.length(), user_name.ptr());
  ObShardConnector* shard_conn = NULL;
  ObString full_user_name = ObString::make_string(full_user_buf);
  if (OB_SUCC(get_global_single_connector_map().get_connector(full_user_name, shard_conn))) {
    LOG_DEBUG("get_connector from map succ", K(full_user_name));
  } else if (OB_ISNULL(shard_conn = op_alloc(ObShardConnector))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("alloc ObSingleConnector failed");
  } else {
    shard_conn->set_shard_name(full_user_name);
    shard_conn->set_full_username(full_user_name);
    shard_conn->inc_ref();
    get_global_single_connector_map().add_connector_if_not_exists(shard_conn);
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    schema_key.set_shard_connector(shard_conn, TYPE_SINGLE_CONNECTOR);
    char dbkey_buf[2048];
    schema_key.logic_tenant_name_.set_value(logic_tenant_name);
    ObMysqlSessionUtils::make_session_dbkey(schema_key, dbkey_buf, 2048);
    ObString dbkey(dbkey_buf);
    schema_key.dbkey_.set_value(dbkey);
    schema_key.init_ = true;
    shard_conn->dec_ref();
    shard_conn = NULL;
    LOG_DEBUG("init_schema_key_value succ", K(schema_key));
  }
  return ret;
}

int ObMysqlSessionUtils::init_schema_key_value(ObProxySchemaKey& schema_key,
    const ObString& logic_tenant_name,
    const ObString& logic_database_name,
    dbconfig::ObShardConnector* shard_conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(shard_conn)) {
    LOG_WDIAG("shard_conn is null");
    ret = OB_ERR_UNEXPECTED;
  } else {
    schema_key.set_shard_connector(shard_conn, TYPE_SHARD_CONNECTOR);
    char dbkey_buf[2048];
    schema_key.logic_tenant_name_.set_value(logic_tenant_name);
    schema_key.logic_database_name_.set_value(logic_database_name);
    ObMysqlSessionUtils::make_session_dbkey(schema_key, dbkey_buf, 2048);
    ObString dbkey(dbkey_buf);
    schema_key.dbkey_.set_value(dbkey);
    schema_key.init_ = true;
    LOG_DEBUG("init_schema_key_value succ", K(schema_key));
  }
  return ret;
}

int ObMysqlSessionUtils::init_schema_key_with_client_session(ObProxySchemaKey& schema_key,
    ObMysqlClientSession* client_session)
{
  int ret = OB_SUCCESS;
  // proxy session pool
  if (client_session->schema_key_.init_ && client_session->is_proxy_mysql_client_
      && client_session->is_session_pool_client()) {
    schema_key = client_session->schema_key_;
    LOG_DEBUG("init with client_session schema_key:", K(client_session->schema_key_));
    return ret;
  }
  ObClientSessionInfo& session_info = client_session->get_session_info();
  ObString logic_tenant_name;
  ObString logic_database_name;
  client_session->get_session_info().get_logic_database_name(logic_database_name);
  client_session->get_session_info().get_logic_tenant_name(logic_tenant_name);
  if (logic_tenant_name.empty()) {
    logic_tenant_name = ObString::make_string(DEFAULT_LOGIC_TENANT_NAME);
  }
  bool is_sharding_user = client_session->get_session_info().is_sharding_user();
  if (is_sharding_user) {
    dbconfig::ObShardConnector * shard_conn = session_info.get_shard_connector(); 
    if (OB_FAIL(init_schema_key_value(schema_key, logic_tenant_name, logic_database_name, shard_conn))) {
      LOG_WDIAG("init_schema_key_value failed", K(ret));
    }
  } else {
    ObString& user_name = session_info.get_login_req().get_hsr_result().user_name_;
    ObString& tenant_name = session_info.get_login_req().get_hsr_result().tenant_name_;
    ObString& cluster_name = session_info.get_login_req().get_hsr_result().cluster_name_;
    ObString database_name = session_info.get_database_name();
    if (database_name.empty()) {
      database_name = client_session->get_session_info().get_login_req().get_hsr_result().response_.get_database();
    }
    LOG_DEBUG("init_schema_key_with_client_session", K(user_name), K(tenant_name),
            K(cluster_name), K(database_name), K(logic_tenant_name), K(client_session));
    ret = init_schema_key_value(schema_key, user_name, tenant_name, cluster_name, database_name, logic_tenant_name);
  }
  return ret;
}

int ObMysqlSessionUtils::init_common_addr_with_client_session(ObCommonAddr& common_addr,
      const net::ObIpEndpoint& server_ip,
      ObMysqlClientSession* client_session)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo& session_info = client_session->get_session_info();
  if (client_session->is_proxy_mysql_client_) {
    common_addr = client_session->common_addr_;
  } else if (session_info.is_sharding_user() &&
              NULL != session_info.get_shard_connector() &&
              common::DB_MYSQL == session_info.get_shard_connector()->server_type_ &&
              !session_info.get_shard_connector()->is_physic_ip_) {
    common_addr.assign(session_info.get_shard_connector()->physic_addr_,
      session_info.get_shard_connector()->physic_port_,
      false);
  } else {
    common_addr.assign(server_ip.sa_);
  }
  LOG_DEBUG("init_common_addr_with_client_session", K(common_addr));
  return ret;
}
int ObMysqlSessionUtils::get_sockaddr_by_ip_port(const common::ObString& ip_str, int32_t port,
  bool is_physical, sockaddr& addr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyPbUtils::get_physic_ip(ip_str, is_physical, addr))) {
    LOG_WDIAG("get_physic_ip failed", K(ret));
  } else {
    ops_ip_port_cast(addr) = (htons)(static_cast<uint16_t>(port));
  }
  return ret;
}

int GlobalSingleConnectorMap::add_connector_if_not_exists(dbconfig::ObShardConnector* connector)
{
  int ret = OB_SUCCESS;
  // connector should null ,not check
  {
    //code block for lock
    DRWLock::WRLockGuard guard(rwlock_);
    if (OB_SUCC(single_connector_map_.unique_set(connector))) {
      // if succ inc_ref
      connector->inc_ref();
    }
  }
  if (ret == OB_HASH_EXIST) {
    LOG_DEBUG("already exist");
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    LOG_WDIAG("add_connector_if_not_exists failed", K(ret));
  }
  return ret;
}
int GlobalSingleConnectorMap::get_connector(const common::ObString& name, dbconfig::ObShardConnector* &connector)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_SUCC(single_connector_map_.get_refactored(name, connector))) {
    connector->inc_ref();
  }
  return ret;
}
void GlobalSingleConnectorMap::destroy() {
  DRWLock::WRLockGuard guard(rwlock_);
  for (SingleConnectorHashMap::iterator it = single_connector_map_.begin();
      it != single_connector_map_.end(); ++it) {
    dbconfig::ObShardConnector* shard_conn = &(*it);
    shard_conn->dec_ref();
    shard_conn = NULL;
  }
}
GlobalSingleConnectorMap& get_global_single_connector_map()
{
  static GlobalSingleConnectorMap g_global_single_connector_map;
  return g_global_single_connector_map;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
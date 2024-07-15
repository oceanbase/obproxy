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
#ifndef OBPROXY_RPC_NET_CLIENT_INFO_H
#define OBPROXY_RPC_NET_CLIENT_INFO_H

#include "lib/container/ob_vector.h"
#include "lib/net/ob_addr.h"
#include "lib/random/ob_mysql_random.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_cached_variables.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/route/ob_ldc_struct.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "proxy/mysql/ob_cursor_struct.h"
#include "proxy/mysql/ob_piece_info.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "rpc/obmysql/packet/ompk_ssl_request.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "utils/ob_proxy_privilege_check.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyConfigString;
}
class ObDefaultSysVarSet;
namespace proxy
{

class ObRpcClientNetSessionInfo
{
public:
  ObRpcClientNetSessionInfo();
  ~ObRpcClientNetSessionInfo();

  /* RPC if has ps_id <-> ps_entry , need add them to here */


public:
  int init();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t get_cluster_id() const { return cluster_id_; }
  const common::ObString &get_meta_cluster_name() const { return real_meta_cluster_name_; }
  void set_cluster_id(const int64_t cluster_id) { cluster_id_ = cluster_id; }
  int set_cluster_info(const bool enable_cluster_checkout,
                       const common::ObString &cluster_name,
                       const obutils::ObProxyConfigString &real_meta_cluster_name,
                       const int64_t cluster_id,
                       bool &need_delete_cluster);
  void free_real_meta_cluster_name();

  bool is_partition_table_supported() const { return true; }
  bool is_oracle_mode() const {
    return is_oracle_mode_;
  }
  void set_oracle_mode(const bool is_oracle_mode) {
    is_oracle_mode_ = is_oracle_mode;
    if (is_oracle_mode_) {
      server_type_ = DB_OB_ORACLE;
    }
  }

  //set and get methord
  int set_cluster_name(const common::ObString &cluster_name);
  int set_tenant_name(const common::ObString &tenant_name);
  int set_vip_addr_name(const common::ObAddr &vip_addr);
  int set_database_name(const common::ObString &database_name, const bool inc_db_version = true);
  int set_user_name(const common::ObString &user_name);
  int set_ldg_logical_cluster_name(const common::ObString &cluster_name);
  int set_ldg_logical_tenant_name(const common::ObString &tenant_name);
  int set_logic_tenant_name(const common::ObString &logic_tenant_name);
  int set_logic_database_name(const common::ObString &logic_database_name);
  // int set_rpc_login_req(ObProxyRpcAuthRequest &auth_req) { rpc_auth_req_ = auth_req; }

  int get_cluster_name(common::ObString &cluster_name) const;
  int get_tenant_name(common::ObString &tenant_name) const;
  int get_vip_addr_name(common::ObString &vip_addr_name) const;

  common::ObString get_full_username();

  int get_user_name(common::ObString &user_name) const;
  int get_ldg_logical_cluster_name(common::ObString &cluster_name) const;
  int get_ldg_logical_tenant_name(common::ObString &tenant_name) const;
  int get_logic_tenant_name(common::ObString &logic_tenant_name) const;
  int get_logic_database_name(common::ObString &logic_database_name) const;
  int get_database_name(ObString &database_name) const;
  ObString get_database_name() const;

  bool is_user_idc_name_set() const { return is_user_idc_name_set_; }
  bool is_proxy_route_policy_set() const { return is_proxy_route_policy_set_; }

  // return the raw timeout value in session
  int get_session_timeout(const char *timeout_name, int64_t &timeout) const;

  ObProxySessionPrivInfo &get_priv_info() { return priv_info_; }
  const ObProxySessionPrivInfo &get_priv_info() const { return priv_info_; }

  // safe weak read sanpshot (the value is set when we receive by ok packet)
  int64_t get_safe_read_snapshot() { return safe_read_snapshot_; }
  void set_safe_read_snapshot(int64_t snapshot) { safe_read_snapshot_ = snapshot; }


  // route policy
  int64_t get_route_policy() const { return route_policy_; }
  void set_route_policy(int64_t route_policy) { route_policy_ = route_policy; }
  int64_t get_read_consistency() const { return cs_read_consistency_; }
  void set_read_consistency(int64_t read_consistency) { cs_read_consistency_ = read_consistency; }

  //proxy route policy
  ObProxyRoutePolicyEnum get_proxy_route_policy() const { return proxy_route_policy_; }
  void set_proxy_route_policy(ObProxyRoutePolicyEnum policy) {
    proxy_route_policy_ = policy;
    is_proxy_route_policy_set_ = true;
  }

  // the initial value of this flag is false,
  // when we specifies next transaction characteristic(set transaction xxx), this flag will be set;
  // we will clear this flag when the next comming transaction commit successfully;

  void set_user_identity(const ObProxyLoginUserType identity) { user_identity_ = identity; }
  ObProxyLoginUserType get_user_identity() const { return user_identity_; }

  bool is_sharding_user() const { return USER_TYPE_SHARDING == user_identity_; }
  bool is_metadb_user() const { return USER_TYPE_METADB == user_identity_; }
  bool is_proxysys_user() const { return USER_TYPE_PROXYSYS == user_identity_; }
  bool is_rootsys_user() const { return USER_TYPE_ROOTSYS == user_identity_; }
  bool is_proxyro_user() const { return USER_TYPE_PROXYRO == user_identity_; }
  bool is_inspector_user() const { return USER_TYPE_INSPECTOR == user_identity_; }
  bool is_proxysys_tenant() const { return (is_proxysys_user() || is_inspector_user()); }

  int create_scramble(common::ObMysqlRandom &random);
  common::ObString &get_scramble_string() { return scramble_string_; }
  common::ObString &get_idc_name() { return idc_name_; }
  common::ObString get_idc_name() const { return idc_name_; }
  void set_idc_name(const ObString &name);
  common::ObString &get_client_host() { return client_host_; }
  void set_client_host(const common::ObAddr &host);
  common::ObString &get_origin_username() { return origin_username_; }
  int set_origin_username(const common::ObString &username);

  void set_user_priv_set(const int64_t user_priv_set) { priv_info_.user_priv_set_ = user_priv_set; }


  void set_obproxy_route_addr(int64_t ip_port) { obproxy_route_addr_ = ip_port; }
  int64_t get_obproxy_route_addr() const { return obproxy_route_addr_; }


  // mark the ob_route_policy_set has set
  bool is_read_consistency_set() const { return is_read_consistency_set_; }
  void set_read_consistency_set_flag(const bool flag) { is_read_consistency_set_ = flag; }

  //bool enable_shard_authority() const { return enable_shard_authority_; }
  //void set_enable_shard_authority() { enable_shard_authority_ = true; }


  ObConsistencyLevel get_consistency_level_prop() const {return consistency_level_prop_;}
  void set_consistency_level_prop(ObConsistencyLevel level) {consistency_level_prop_ = level;}

  void destroy();

  DBServerType get_server_type() const { return server_type_; }
  void set_server_type(DBServerType server_type) { server_type_ = server_type; }

  common::ObArenaAllocator &get_rpc_clientinfo_allocator() { return allocator_; };

  inline bool is_oceanbase_server() const { return DB_OB_MYSQL == server_type_ || DB_OB_ORACLE == server_type_; }
  void set_allow_use_last_session(const bool is_allow_use_last_session) { is_allow_use_last_session_ = is_allow_use_last_session; }
  bool is_allow_use_last_session() { return is_allow_use_last_session_; }
  int remove_database_name() { return field_mgr_.remove_database_name(); }

  void set_group_id(const int64_t group_id) { group_id_ = group_id; }
  int64_t get_group_id() { return group_id_; }

  void set_is_read_only_user(bool is_read_only_user)
  {
    is_read_only_user_ = is_read_only_user;
  }
  void set_is_request_follower_user(bool is_request_follower_user)
  {
    is_request_follower_user_ = is_request_follower_user;
  }

  void set_is_in_shard_query(bool is_in_shard_query)
  {
    is_in_shard_query_ = is_in_shard_query;
  }
  void set_is_in_fetch_query(bool is_in_fetch_query)
  {
    is_in_fetch_query_ = is_in_fetch_query;
  }

  void set_last_request_is_shard_query(bool is_shard_query)
  {
    last_request_is_shard_query_ = is_shard_query;
  }

  bool is_read_only_user() const { return is_read_only_user_; }
  bool is_request_follower_user() const { return is_request_follower_user_; }
  bool is_in_shard_query() const { return is_in_shard_query_; }
  bool is_in_fetch_query() const { return is_in_fetch_query_; }
  bool last_request_is_shard_query() const { return last_request_is_shard_query_; }
  ObRpcRequestConfigInfo &get_config_info() { return config_info_; }

public:
  ObSessionFieldMgr field_mgr_; // just used clust / tenant / db name version etc(not used any user's defined variables)
  bool is_session_pool_client_; // used for ObRpcClient

  common::ObString full_name_;
  common::ObString user_tenant_name_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString user_name_;
  common::ObString database_name_;
  int64_t cluster_id_;
  bool is_clustername_from_default_;
  bool has_tenant_username_;
  bool has_cluster_username_;
  int64_t name_len_;
  char *name_buf_;
  static const int SCHEMA_LENGTH = 100;
  char full_name_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];
  char schema_name_buf_[SCHEMA_LENGTH];
  ObRpcRequestConfigInfo config_info_; //TODO maybe it need a map to store cluster#tenant#vaddr's level config info in client or thread cache

private:

  bool is_inited_;
  //when user set proxy_idc_name, set it true;
  bool is_user_idc_name_set_;
  //when user set ob_route_policy, set it true;
  bool is_read_consistency_set_;
  // is oracle mode
  bool is_oracle_mode_;
  //when user set proxy_route_policy, set it true;
  bool is_proxy_route_policy_set_;

  // ob route policy
  int64_t route_policy_;

  ObProxyRoutePolicyEnum proxy_route_policy_;

  // login packet will be used to next time(include raw packet data and analyzed result)
  // ObProxyRpcAuthRequest rpc_auth_req_;

  // privilege info of current client session
  ObProxySessionPrivInfo priv_info_;

  ObProxyLoginUserType user_identity_;
  // obproxy_route_addr
  int64_t obproxy_route_addr_;

  int64_t cs_read_consistency_;

  int64_t safe_read_snapshot_;

  common::ObString real_meta_cluster_name_;
  char *real_meta_cluster_name_str_;

  common::ObString scramble_string_;
  char scramble_buf_[obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE + 1];

  common::ObString idc_name_;
  char idc_name_buf_[OB_PROXY_MAX_IDC_NAME_LENGTH];

  DBServerType server_type_;
  //dbconfig::ObShardConnector *shard_conn_;
  //dbconfig::ObShardProp *shard_prop_;
  int64_t group_id_;
  bool is_allow_use_last_session_;

  common::ObString client_host_;
  char client_host_buf_[common::MAX_IP_ADDR_LENGTH];

  common::ObString origin_username_;
  char username_buf_[common::OB_MAX_USER_NAME_LENGTH];

  // consistency_level_prop in shard_conn_prop
  ObConsistencyLevel consistency_level_prop_;

  bool is_read_only_user_;
  bool is_request_follower_user_;
  bool is_in_shard_query_;
  bool is_in_fetch_query_;
  bool last_request_is_shard_query_;

  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObRpcClientNetSessionInfo);
};

inline int ObRpcClientNetSessionInfo::set_origin_username(const common::ObString &username)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(username.length() > common::OB_MAX_USER_NAME_LENGTH)) {
    ret = common::OB_SIZE_OVERFLOW;
    PROXY_LOG(WDIAG, "username buf is not enough", K(username), K(ret));
  } else {
    MEMCPY(username_buf_, username.ptr(), username.length());
    origin_username_.assign_ptr(username_buf_, username.length());
  }
  return ret;
}

inline void ObRpcClientNetSessionInfo::set_client_host(const common::ObAddr &host)
{
  if (host.ip_to_string(client_host_buf_, common::MAX_IP_ADDR_LENGTH)) {
    client_host_.assign(client_host_buf_, static_cast<int32_t>(strlen(client_host_buf_)));
  }
}

inline void ObRpcClientNetSessionInfo::set_idc_name(const ObString &name)
{
  const int64_t len = min(name.length(), OB_PROXY_MAX_IDC_NAME_LENGTH);
  MEMCPY(idc_name_buf_, name.ptr(), len);
  idc_name_.assign(idc_name_buf_, static_cast<int32_t>(len));
  is_user_idc_name_set_ = true;
}

inline int ObRpcClientNetSessionInfo::create_scramble(common::ObMysqlRandom &random)
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(random.create_random_string(scramble_buf_, sizeof(scramble_buf_)))) {
    scramble_string_.assign_ptr(scramble_buf_, static_cast<int32_t>(STRLEN(scramble_buf_)));
  }
  return ret;
}

int64_t ObRpcClientNetSessionInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(priv_info),
       K_(is_user_idc_name_set),
       K_(is_read_consistency_set), K_(idc_name), K_(cluster_id), K_(real_meta_cluster_name),
       K_(route_policy),
       K_(proxy_route_policy), K_(user_identity),
       K_(is_read_only_user), K_(is_request_follower_user));
  J_OBJ_END();
  return pos;
}

void ObRpcClientNetSessionInfo::destroy()
{
  if (is_inited_)  {
    // rpc_auth_req_.destroy();
    // field_mgr_.destroy();
    is_inited_ = false;
  }
  //is_trans_specified_ = false;
  is_user_idc_name_set_ = false;
  is_read_consistency_set_ = false;
  is_oracle_mode_ = false;
  is_proxy_route_policy_set_ = false;

  is_read_only_user_ = false;
  is_request_follower_user_ = false;

  obproxy_route_addr_ = 0;
  route_policy_ = 1;
  proxy_route_policy_ = MAX_PROXY_ROUTE_POLICY;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  // free_real_meta_cluster_name();

  server_type_ = DB_OB_MYSQL;
  group_id_ = OBPROXY_MAX_DBMESH_ID;
  is_allow_use_last_session_ = true;
}

}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase
#endif /* OBPROXY_RPC_NET_CLIENT_INFO_H */

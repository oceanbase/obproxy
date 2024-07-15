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

#include "proxy/rpc_optimize/net/ob_proxy_rpc_session_info.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log.h"
#include "utils/ob_proxy_privilege_check.h"
#include "proxy/mysqllib/ob_sys_var_set_processor.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_resource_pool_processor.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const int64_t SESSION_ITEM_NUM = 256;

ObRpcClientNetSessionInfo::ObRpcClientNetSessionInfo()
    : field_mgr_(), is_session_pool_client_(true), full_name_(), user_tenant_name_(),
      cluster_name_(), tenant_name_(), user_name_(), database_name_(), cluster_id_(OB_INVALID_CLUSTER_ID),
      is_clustername_from_default_(false), has_tenant_username_(false), has_cluster_username_(false),
      name_len_(0), name_buf_(NULL), config_info_(), is_inited_(false),
      is_user_idc_name_set_(false), is_read_consistency_set_(false), is_oracle_mode_(false),
      is_proxy_route_policy_set_(false),
      //enable_shard_authority_(false), enable_reset_db_(true), cap_(0), safe_read_snapshot_(0),
      route_policy_(1), proxy_route_policy_(MAX_PROXY_ROUTE_POLICY),
      user_identity_(USER_TYPE_NONE),
      obproxy_route_addr_(0),
      real_meta_cluster_name_(), real_meta_cluster_name_str_(NULL),
      server_type_(DB_OB_MYSQL),
      group_id_(OBPROXY_MAX_DBMESH_ID), is_allow_use_last_session_(true),
      consistency_level_prop_(INVALID_CONSISTENCY),
      is_read_only_user_(false),
      is_request_follower_user_(false),
      is_in_shard_query_(false),
      is_in_fetch_query_(false),
      last_request_is_shard_query_(false),
      allocator_()
{
  MEMSET(scramble_buf_, 0, sizeof(scramble_buf_));
  MEMSET(idc_name_buf_, 0, sizeof(idc_name_buf_));
  MEMSET(client_host_buf_, 0, sizeof(client_host_buf_));
  MEMSET(username_buf_, 0, sizeof(username_buf_));
}

ObRpcClientNetSessionInfo::~ObRpcClientNetSessionInfo()
{
  destroy();
}

// int64_t ObRpcClientNetSessionInfo::to_string(char *buf, const int64_t buf_len) const
// {
//   int64_t pos = 0;
//   J_OBJ_START();
//   J_KV(K_(is_inited), K_(priv_info),
//        K_(is_user_idc_name_set),
//        K_(is_read_consistency_set), K_(idc_name), K_(cluster_id), K_(real_meta_cluster_name),
//        K_(route_policy),
//        K_(proxy_route_policy), K_(user_identity),
//        K_(is_read_only_user), K_(is_request_follower_user));
//   J_OBJ_END();
//   return pos;
// }

int ObRpcClientNetSessionInfo::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("client session is inited", K(ret));
  } else if (OB_FAIL(field_mgr_.init())) {
    LOG_WDIAG("fail to init field_mgr", K(ret));
  //} else if (OB_FAIL(sess_info_hash_map_.create(32, ObModIds::OB_PROXY_SESS_SYNC))) {
  //  LOG_WDIAG("create hash map failed", K(ret));
  } else {
    LOG_DEBUG("init session info success", K(ret));
    is_inited_ = true;
  }
  return ret;
}

int ObRpcClientNetSessionInfo::get_cluster_name(ObString &cluster_name) const
{
  return field_mgr_.get_cluster_name(cluster_name);
}

int ObRpcClientNetSessionInfo::get_tenant_name(ObString &tenant_name) const
{
  return field_mgr_.get_tenant_name(tenant_name);
}

int ObRpcClientNetSessionInfo::get_logic_tenant_name(ObString &logic_tenant_name) const
{
  return field_mgr_.get_logic_tenant_name(logic_tenant_name);
}

int ObRpcClientNetSessionInfo::get_logic_database_name(ObString &logic_database_name) const
{
  return field_mgr_.get_logic_database_name(logic_database_name);
}

// int ObRpcClientNetSessionInfo::get_database_name(ObString &database_name) const
// {
//   return field_mgr_.get_database_name(database_name);
// }

int ObRpcClientNetSessionInfo::get_vip_addr_name(ObString &vip_addr_name) const
{
  return field_mgr_.get_vip_addr_name(vip_addr_name);
}

// ObString ObRpcClientNetSessionInfo::get_database_name() const
// {
//   ObString database_name;
//   field_mgr_.get_database_name(database_name);
//   return database_name;
// }

int ObRpcClientNetSessionInfo::get_user_name(ObString &user_name) const
{
  return field_mgr_.get_user_name(user_name);
}

int ObRpcClientNetSessionInfo::get_ldg_logical_cluster_name(ObString &cluster_name) const
{
  return field_mgr_.get_ldg_logical_cluster_name(cluster_name);
}

int ObRpcClientNetSessionInfo::get_ldg_logical_tenant_name(ObString &tenant_name) const
{
  return field_mgr_.get_ldg_logical_tenant_name(tenant_name);
}


int ObRpcClientNetSessionInfo::set_cluster_info(const bool enable_cluster_checkout,
    const ObString &cluster_name, const obutils::ObProxyConfigString &real_meta_cluster_name,
    const int64_t cluster_id, bool &need_delete_cluster)
{
  int ret = OB_SUCCESS;
  cluster_id_ = cluster_id;
  if (cluster_name == OB_META_DB_CLUSTER_NAME) {
    free_real_meta_cluster_name();
    if (OB_UNLIKELY(real_meta_cluster_name.empty())) {
      if (OB_LIKELY(enable_cluster_checkout)) {
        // we need check delete cluster when the follow happened:
        // 1. this is OB_META_DB_CLUSTER_NAME
        // 2. enable cluster checkout
        // 3. real meta cluster do not exist
        need_delete_cluster = true;
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("real meta cluster name is empty, it should not happened, proxy need rebuild meta cluster", K(ret));
      }
    } else {
      if (OB_ISNULL(real_meta_cluster_name_str_ = static_cast<char *>(op_fixed_mem_alloc(real_meta_cluster_name.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for real_meta_cluster_name", "size", real_meta_cluster_name.length(), K(ret));
      } else {
        MEMCPY(real_meta_cluster_name_str_, real_meta_cluster_name.ptr(), real_meta_cluster_name.length());
        real_meta_cluster_name_.assign_ptr(real_meta_cluster_name_str_, real_meta_cluster_name.length());
      }
    }
  }
  return ret;
}

void ObRpcClientNetSessionInfo::free_real_meta_cluster_name()
{
  if (NULL != real_meta_cluster_name_str_) {
    op_fixed_mem_free(real_meta_cluster_name_str_, real_meta_cluster_name_.length());
    real_meta_cluster_name_str_ = NULL;
  }
  real_meta_cluster_name_.reset();
}

int ObRpcClientNetSessionInfo::set_cluster_name(const ObString &cluster_name)
{
  return field_mgr_.set_cluster_name(cluster_name);
}

int ObRpcClientNetSessionInfo::set_tenant_name(const ObString &tenant_name)
{
  return field_mgr_.set_tenant_name(tenant_name);
}

int ObRpcClientNetSessionInfo::set_vip_addr_name(const common::ObAddr &vip_addr)
{
  int ret = OB_SUCCESS;
  char vip_name[MAX_IP_ADDR_LENGTH];
  if (OB_UNLIKELY(!vip_addr.ip_to_string(vip_name, static_cast<int32_t>(sizeof(vip_name))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to covert ip to string", K(vip_name), K(ret));
  } else {
    return field_mgr_.set_vip_addr_name(vip_name);
  }
  return ret;
}

int ObRpcClientNetSessionInfo::set_logic_tenant_name(const ObString &logic_tenant_name)
{
  return field_mgr_.set_logic_tenant_name(logic_tenant_name);
}

int ObRpcClientNetSessionInfo::set_logic_database_name(const ObString &logic_database_name)
{
  return field_mgr_.set_logic_database_name(logic_database_name);
}

ObString ObRpcClientNetSessionInfo::get_full_username()
{
  if (is_oceanbase_server()) {
    return full_name_;
  } else {
    return ObString::make_empty_string();
  }
}

//int get_database_name(ObString &database_name) const
int ObRpcClientNetSessionInfo::get_database_name(common::ObString &database_name) const
{
  database_name = database_name_;
  return OB_SUCCESS;
}
common::ObString ObRpcClientNetSessionInfo::get_database_name() const
{
  return database_name_;
}

int ObRpcClientNetSessionInfo::set_user_name(const ObString &user_name)
{
  return field_mgr_.set_user_name(user_name);
}

int ObRpcClientNetSessionInfo::set_ldg_logical_cluster_name(const ObString &cluster_name)
{
  return field_mgr_.set_ldg_logical_cluster_name(cluster_name);
}

int ObRpcClientNetSessionInfo::set_ldg_logical_tenant_name(const ObString &tenant_name)
{
  return field_mgr_.set_ldg_logical_tenant_name(tenant_name);
}

//int ObRpcClientNetSessionInfo::extract_changed_schema(ObRpcClientNetSessionInfo &server_info,
//                                                ObString &db_name)
//{
//  int ret = OB_SUCCESS;
//  if (OB_UNLIKELY(!is_inited_)) {
//    ret = OB_NOT_INIT;
//    LOG_WDIAG("client session is not inited", K(ret));
//  } else if (!need_reset_database(server_info)) {
//    // do nothing
//  } else if (OB_FAIL(get_database_name(db_name))) {
//    LOG_EDIAG("fail to get database name", K(*this), K(server_info), K(ret));
//  } else {}
//  return ret;
//}

// void ObRpcClientNetSessionInfo::destroy()
// {
//   if (is_inited_)  {
//     rpc_auth_req_.destroy();
//     field_mgr_.destroy();
//     is_inited_ = false;
//   }
//   //is_trans_specified_ = false;
//   is_user_idc_name_set_ = false;
//   is_read_consistency_set_ = false;
//   is_oracle_mode_ = false;
//   is_proxy_route_policy_set_ = false;

//   is_read_only_user_ = false;
//   is_request_follower_user_ = false;

//   obproxy_route_addr_ = 0;
//   route_policy_ = 1;
//   proxy_route_policy_ = MAX_PROXY_ROUTE_POLICY;
//   cluster_id_ = OB_INVALID_CLUSTER_ID;
//   free_real_meta_cluster_name();

//   server_type_ = DB_OB_MYSQL;
//   group_id_ = OBPROXY_MAX_DBMESH_ID;
//   is_allow_use_last_session_ = true;
// }

}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase

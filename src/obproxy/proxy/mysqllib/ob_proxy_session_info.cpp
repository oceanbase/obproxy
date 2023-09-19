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

#include "proxy/mysqllib/ob_proxy_session_info.h"
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
ObServerSessionInfo::ObServerSessionInfo() :
    cap_(0), compatible_capability_(0), checksum_switch_(CHECKSUM_ON), is_inited_(false),
    server_type_(DB_OB_MYSQL), shard_conn_(NULL),
    ps_id_(0), ps_id_pair_map_(), cursor_id_pair_map_(), allocator_(), text_ps_version_set_()
{
  const int BUCKET_SIZE = 8;
  text_ps_version_set_.create(BUCKET_SIZE);
}

ObServerSessionInfo::~ObServerSessionInfo()
{
  reset();
  text_ps_version_set_.destroy();
  if (NULL != shard_conn_) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }
}

int ObServerSessionInfo::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("server session is inited", K(ret));
  } else if (OB_FAIL(field_mgr_.init())) {
    LOG_WARN("fail to init field_mgr", K(ret));
  } else {
    field_mgr_.set_allow_var_not_found(true);
    LOG_DEBUG("init session info success");
    is_inited_ = true;
    ObDefaultSysVarSet *default_sysvar_set = NULL;

    if (OB_ISNULL(default_sysvar_set = get_global_resource_pool_processor().get_default_sysvar_set())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_sysvar_set is null", K(ret));
    } else if (OB_FAIL(field_mgr_.set_sys_var_set(default_sysvar_set))) {
      LOG_WARN("fail to add sys var set", K(ret));
    }
  }
  return ret;
}

ObString ObServerSessionInfo::get_database_name() const
{
  ObString database_name;
  field_mgr_.get_database_name(database_name);
  return database_name;
}

int ObServerSessionInfo::get_database_name(ObString &database_name) const
{
  return field_mgr_.get_database_name(database_name);
}

int ObServerSessionInfo::set_database_name(const ObString &database_name, const bool is_string_to_lower_case)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(field_mgr_.set_database_name(database_name))) {
    LOG_WARN("fail to set database name", K(database_name), K(ret));
  } else if (OB_LIKELY(is_string_to_lower_case)) {
    ObString db_name;
    if (OB_FAIL(get_database_name(db_name))) {
      LOG_WARN("fail to get db name", K(db_name), K(database_name), K(ret));
    } else {
      string_to_lower_case(db_name.ptr(), db_name.length());
    }
  }

  return ret;
}

void ObServerSessionInfo::reset()
{
  if (is_inited_) {
    field_mgr_.destroy();
    is_inited_ = false;
  }

  destroy_ps_id_pair_map();
  destroy_cursor_id_pair_map();
  ob_server_.reset();
  version_.reset();
  cap_ = 0;
  compatible_capability_.capability_ = 0;
  checksum_switch_ = CHECKSUM_ON;
  reuse_text_ps_version_set();
}

void ObServerSessionInfo::destroy_ps_id_pair_map()
{
  ObPsIdPairMap::iterator last = ps_id_pair_map_.end();
  ObPsIdPairMap::iterator tmp_iter;
  for (ObPsIdPairMap::iterator ps_iter = ps_id_pair_map_.begin(); ps_iter != last;) {
    tmp_iter = ps_iter;
    ++ps_iter;
    tmp_iter->destroy();
  }
  ps_id_pair_map_.reset();
}

void ObServerSessionInfo::destroy_cursor_id_pair_map()
{
  ObCursorIdPairMap::iterator last = cursor_id_pair_map_.end();
  ObCursorIdPairMap::iterator tmp_iter;
  for (ObCursorIdPairMap::iterator cursor_iter = cursor_id_pair_map_.begin(); cursor_iter != last;) {
    tmp_iter = cursor_iter;
    ++cursor_iter;
    tmp_iter->destroy();
  }
  cursor_id_pair_map_.reset();
}

int ObServerSessionInfo::set_text_ps_version(const int64_t text_ps_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(text_ps_version_set_.set_refactored(text_ps_version))) {
    LOG_WARN("set refactored failed", K(ret), K(text_ps_version));
  } else {
    LOG_DEBUG("set refactored succed", K(text_ps_version));
  }

  return ret;
}

int ObServerSessionInfo::remove_text_ps_version(const int64_t text_ps_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(text_ps_version_set_.erase_refactored(text_ps_version))) {
    LOG_WARN("remove refactored failed", K(ret), K(text_ps_version));
  } else {
    LOG_DEBUG("remove refactored succed", K(text_ps_version));
  }

  return ret;
}


int64_t ObServerSessionInfo::get_sess_field_version(int16_t type)
{
  int64_t version = 0;
  int64_t count = sess_info_field_version_.count();
  for (int i = 0; i < count; i++) {
    SessionInfoFieldVersion &field_version = sess_info_field_version_.at(i);
    if (field_version.get_sess_info_type() == type) {
      version = field_version.get_version();
      break;
    }
  }
  return version;
}


int ObServerSessionInfo::update_sess_info_field_version(int16_t type, int64_t version)
{
  int ret = OB_SUCCESS;
  bool is_type_exist = false;
  int64_t count = sess_info_field_version_.count();
  for (int i = 0; i < count; i++) {
    SessionInfoFieldVersion &field_version = sess_info_field_version_.at(i);
    if (field_version.get_sess_info_type() == type) {
      is_type_exist = true;
      field_version.set_version(version);
      break;
    }
  }
  if (!is_type_exist) {
    if (OB_FAIL(sess_info_field_version_.push_back(SessionInfoFieldVersion(type, version)))) {
      LOG_WARN("fail to record sess info field version", K(type), K(version), K(ret));
    }
  }
  return ret;
}  

void ObServerSessionInfo::reuse_text_ps_version_set()
{
  text_ps_version_set_.reuse();
}

ObClientSessionInfo::ObClientSessionInfo()
    : is_inited_(false), is_trans_specified_(false), is_global_vars_changed_(false),
      is_user_idc_name_set_(false), is_read_consistency_set_(false), is_oracle_mode_(false),
      is_proxy_route_policy_set_(false),
      enable_shard_authority_(false), enable_reset_db_(true), client_cap_(0), server_cap_(0),
      safe_read_snapshot_(0),
      syncing_safe_read_snapshot_(0), route_policy_(1), proxy_route_policy_(MAX_PROXY_ROUTE_POLICY),
      user_identity_(USER_TYPE_NONE), cached_variables_(),
      global_vars_version_(OB_INVALID_VERSION), obproxy_route_addr_(0),
      var_set_processor_(NULL), cluster_id_(OB_INVALID_CLUSTER_ID),
      real_meta_cluster_name_(), real_meta_cluster_name_str_(NULL),
      server_type_(DB_OB_MYSQL), shard_conn_(NULL), shard_prop_(NULL),
      group_id_(OBPROXY_MAX_DBMESH_ID), table_id_(OBPROXY_MAX_DBMESH_ID), es_id_(OBPROXY_MAX_DBMESH_ID),
      is_allow_use_last_session_(true),
      consistency_level_prop_(INVALID_CONSISTENCY),
      recv_client_ps_id_(0), ps_id_(0), ps_entry_(NULL), ps_id_entry_(NULL), ps_id_entry_map_(),
      text_ps_name_entry_(NULL), text_ps_name_entry_map_(), cursor_id_(0), cursor_id_addr_map_(),
      ps_id_addrs_map_(), request_send_addrs_(), is_read_only_user_(false), is_request_follower_user_(false),
      obproxy_force_parallel_query_dop_(1), ob_max_read_stale_time_(-1), last_server_addr_(),
      last_server_sess_id_(0), sync_conf_sys_var_(false), init_sql_()
{
  is_session_pool_client_ = true;
  MEMSET(scramble_buf_, 0, sizeof(scramble_buf_));
  MEMSET(idc_name_buf_, 0, sizeof(idc_name_buf_));
  MEMSET(client_host_buf_, 0, sizeof(client_host_buf_));
  MEMSET(username_buf_, 0, sizeof(username_buf_));

  ob20_request_.reset();
}

ObClientSessionInfo::~ObClientSessionInfo()
{
  destroy();
}

int64_t ObClientSessionInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(priv_info), K_(version), K_(hash_version), K_(val_hash), K_(global_vars_version),
       K_(is_global_vars_changed), K_(is_trans_specified), K_(is_user_idc_name_set),
       K_(is_read_consistency_set), K_(idc_name), K_(cluster_id), K_(real_meta_cluster_name),
       K_(safe_read_snapshot), K_(syncing_safe_read_snapshot), K_(route_policy),
       K_(proxy_route_policy), K_(user_identity), K_(global_vars_version),
       K_(is_read_only_user), K_(is_request_follower_user), K_(obproxy_force_parallel_query_dop),
       K_(ob20_request), K_(client_cap), K_(server_cap), K_(last_server_addr), K_(last_server_sess_id),
       K_(init_sql));
  J_OBJ_END();
  return pos;
}

int ObClientSessionInfo::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("client session is inited", K(ret));
  } else if (OB_FAIL(field_mgr_.init())) {
    LOG_WARN("fail to init field_mgr", K(ret));
  } else {
    LOG_DEBUG("init session info success", K(cached_variables_));
    is_inited_ = true;
  }
  return ret;
}

int ObClientSessionInfo::add_sys_var_set(ObDefaultSysVarSet &set)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(field_mgr_.set_sys_var_set(&set))) {
    LOG_WARN("fail to set sys var set", K(ret));
  } else if (OB_FAIL(load_all_cached_variable())) {
    LOG_WARN("fail to load cached variables", K(ret));
    // if failed, we should set var set back to NULL;
    if (OB_FAIL(field_mgr_.set_sys_var_set(NULL))) {
      LOG_WARN("fail to set sys var set", K(ret));
    }
    is_inited_ = true;
  }

  return ret;
}

int ObClientSessionInfo::set_cluster_info(const bool enable_cluster_checkout,
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
        LOG_WARN("real meta cluster name is empty, it should not happened, proxy need rebuild meta cluster", K(ret));
      }
    } else {
      if (OB_ISNULL(real_meta_cluster_name_str_ = static_cast<char *>(op_fixed_mem_alloc(real_meta_cluster_name.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mem for real_meta_cluster_name", "size", real_meta_cluster_name.length(), K(ret));
      } else {
        MEMCPY(real_meta_cluster_name_str_, real_meta_cluster_name.ptr(), real_meta_cluster_name.length());
        real_meta_cluster_name_.assign_ptr(real_meta_cluster_name_str_, real_meta_cluster_name.length());
      }
    }
  }
  return ret;
}

void ObClientSessionInfo::free_real_meta_cluster_name()
{
  if (NULL != real_meta_cluster_name_str_) {
    op_fixed_mem_free(real_meta_cluster_name_str_, real_meta_cluster_name_.length());
    real_meta_cluster_name_str_ = NULL;
  }
  real_meta_cluster_name_.reset();
}

int ObClientSessionInfo::set_cluster_name(const ObString &cluster_name)
{
  return field_mgr_.set_cluster_name(cluster_name);
}

int ObClientSessionInfo::set_tenant_name(const ObString &tenant_name)
{
  return field_mgr_.set_tenant_name(tenant_name);
}

int ObClientSessionInfo::set_vip_addr_name(const common::ObAddr &vip_addr)
{
  int ret = OB_SUCCESS;
  char vip_name[MAX_IP_ADDR_LENGTH];
  if (OB_UNLIKELY(!vip_addr.ip_to_string(vip_name, static_cast<int32_t>(sizeof(vip_name))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to covert ip to string", K(vip_name), K(ret));
  } else {
    return field_mgr_.set_vip_addr_name(vip_name);
  }
  return ret;
}

int ObClientSessionInfo::set_logic_tenant_name(const ObString &logic_tenant_name)
{
  return field_mgr_.set_logic_tenant_name(logic_tenant_name);
}

int ObClientSessionInfo::set_logic_database_name(const ObString &logic_database_name)
{
  return field_mgr_.set_logic_database_name(logic_database_name);
}

int ObClientSessionInfo::set_database_name(const ObString &database_name,
                                           const bool inc_db_version/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(field_mgr_.set_database_name(database_name))) {
    LOG_WARN("fail to set database name", K(database_name), K(ret));
  } else if (OB_LIKELY(!is_oracle_mode())) {
    ObString db_name;
    if (OB_FAIL(get_database_name(db_name))) {
      LOG_WARN("fail to get db name", K(db_name), K(database_name), K(ret));
    } else if (need_use_lower_case_names()) {
      string_to_lower_case(db_name.ptr(), db_name.length());
    } else {/*do nothing*/}
  }

  if (OB_SUCC(ret)) {
    if (inc_db_version) {
      version_.inc_db_name_version();
    }
  }
  return ret;
}

int ObClientSessionInfo::set_user_name(const ObString &user_name)
{
  return field_mgr_.set_user_name(user_name);
}

int ObClientSessionInfo::set_ldg_logical_cluster_name(const ObString &cluster_name)
{
  return field_mgr_.set_ldg_logical_cluster_name(cluster_name);
}

int ObClientSessionInfo::set_ldg_logical_tenant_name(const ObString &tenant_name)
{
  return field_mgr_.set_ldg_logical_tenant_name(tenant_name);
}

int ObClientSessionInfo::update_sess_sync_info(const ObString& sess_info, const bool is_error_packet, ObServerSessionInfo& server_info, common::ObSimpleTrace<4096> &trace_log)
{
  int ret = OB_SUCCESS;
  const char *buf = sess_info.ptr();
  const int64_t len = sess_info.length();
  const char *end = buf + len;
  // decode sess_info
  const int MAX_TYPE_RECORD = 32;
  int64_t type_record = 0;
  if (NULL != sess_info.ptr()) {
    while (OB_SUCC(ret) && buf < end) {
      int16_t info_type = 0;
      int32_t info_len = 0;
      ObMySQLUtil::get_int2(buf, info_type);
      ObMySQLUtil::get_int4(buf, info_len);
      if(info_type < MAX_TYPE_RECORD) {
        type_record |= 1 << info_type;
      }
      if (buf + info_len > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("data is error", K(info_type), K(info_len), K(len), K(ret));
      } else {
        LOG_DEBUG("extra info", K(info_type), K(info_len), K(len));
        char* info_value = NULL;
        ObString info_value_string;
        int64_t version = 0;
        if (OB_ISNULL(info_value = static_cast<char*>(op_fixed_mem_alloc(info_len + 6)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(info_len), K(ret));
        } else if (FALSE_IT(MEMCPY(info_value, buf - 6, info_len + 6))) {
        } else if (FALSE_IT(info_value_string.assign_ptr(info_value, info_len + 6))) {
        } else if (OB_FAIL(update_sess_info_field(info_type, info_value_string, version))) {
          LOG_WARN("fail to set to hash map", K(info_type), K(ret));
          op_fixed_mem_free(info_value, info_len + 6);
          info_value = NULL;
        } else if (is_error_packet && OB_FAIL(server_info.update_sess_info_field_version(info_type, version))) {
          // if proxy receive error packet, treat all types that server not delivered sync failed, only push up the version of the type that server delivered
          // and duplicate sync the types server not delivered next time
          LOG_WARN("fail to update server sess info type version", K(info_type), K(version), K(ret));
        } else {
          buf += info_len;
        }
      }
    }
    trace_log.log_it("[get_sess]", "type", type_record);
    if (OB_SUCC(ret)) {
      version_.inc_sess_info_version();
      LOG_DEBUG("update sess info succ", K(sess_info));
    }
  }
  return ret;
}

int ObClientSessionInfo::update_server_sess_info_version(ObServerSessionInfo &server_info, const bool is_error_packet) {
  int ret = OB_SUCCESS;
  int64_t sess_info_count = get_sess_info().count();
  if (!is_error_packet) {
   // not error packet,  push up all server sess info type version
    for (int64_t i = 0; i < sess_info_count && OB_SUCC(ret); ++i) {
      SessionInfoField &field = get_sess_info().at(i);
      int16_t sess_info_type = field.get_sess_info_type();
      int64_t client_version = field.get_version();
      int64_t server_version = server_info.get_sess_field_version(sess_info_type);
      if (client_version > server_version) {
        if (OB_FAIL(server_info.update_sess_info_field_version(sess_info_type, client_version))) {
          LOG_WARN("fail to set sess field versoin", K(sess_info_type), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      server_info.set_sess_info_version(get_sess_info_version());
    }
  } else {
    // error packet, check if necessary to push up totoal version
    bool need_update_global_version = true;
    for (int64_t i = 0; i < sess_info_count && OB_SUCC(ret); ++i) {
      SessionInfoField &field = get_sess_info().at(i);
      int16_t sess_info_type = field.get_sess_info_type();
      int64_t client_version = field.get_version();
      int64_t server_version = server_info.get_sess_field_version(sess_info_type);
      if (client_version > server_version) {
        need_update_global_version = false;
      }
    }
    if (OB_SUCC(ret) && need_update_global_version) {
      server_info.set_sess_info_version(get_sess_info_version());
    }
  }
  return ret;
}

int ObClientSessionInfo::update_server_sess_info_version_not_dup_sync(ObServerSessionInfo &server_info, const bool is_error_packet) {
  // 4.1 early version, internal routing trans not support sync session info duplicatly
  // here keep session info version pusing up for only internal routing trans
  int ret = OB_SUCCESS;
  bool need_update_global_version = true;
  int64_t sess_info_count = get_sess_info().count();
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < sess_info_count && OB_SUCC(ret); ++i) {
      SessionInfoField &field = get_sess_info().at(i);
      int16_t sess_info_type = field.get_sess_info_type();
      int64_t client_version = field.get_version();

      if (is_error_packet && !ObProto20Utils::is_trans_related_sess_info(sess_info_type)) {
        int64_t server_version = server_info.get_sess_field_version(sess_info_type);
        if (client_version > server_version) {
          // exist sess info need duplicate sync, don't update total version
          need_update_global_version = false;
        }
      } else if (OB_FAIL(server_info.update_sess_info_field_version(sess_info_type, client_version))) {
        // push up all version of internal routing transaction sess info
        LOG_WARN("fail to set sess field versoin", K(sess_info_type), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && need_update_global_version) {
    server_info.set_sess_info_version(get_sess_info_version());
  }
  return ret;
}

int ObClientSessionInfo::get_cluster_name(ObString &cluster_name) const
{
  return field_mgr_.get_cluster_name(cluster_name);
}

int ObClientSessionInfo::get_tenant_name(ObString &tenant_name) const
{
  return field_mgr_.get_tenant_name(tenant_name);
}

int ObClientSessionInfo::get_logic_tenant_name(ObString &logic_tenant_name) const
{
  return field_mgr_.get_logic_tenant_name(logic_tenant_name);
}

int ObClientSessionInfo::get_logic_database_name(ObString &logic_database_name) const
{
  return field_mgr_.get_logic_database_name(logic_database_name);
}

int ObClientSessionInfo::get_database_name(ObString &database_name) const
{
  return field_mgr_.get_database_name(database_name);
}

int ObClientSessionInfo::get_vip_addr_name(ObString &vip_addr_name) const
{
  return field_mgr_.get_vip_addr_name(vip_addr_name);
}

ObString ObClientSessionInfo::get_database_name() const
{
  ObString database_name;
  field_mgr_.get_database_name(database_name);
  return database_name;
}

int ObClientSessionInfo::get_user_name(ObString &user_name) const
{
  return field_mgr_.get_user_name(user_name);
}

int ObClientSessionInfo::get_ldg_logical_cluster_name(ObString &cluster_name) const
{
  return field_mgr_.get_ldg_logical_cluster_name(cluster_name);
}

int ObClientSessionInfo::get_ldg_logical_tenant_name(ObString &tenant_name) const
{
  return field_mgr_.get_ldg_logical_tenant_name(tenant_name);
}

ObString ObClientSessionInfo::get_full_username()
{
  if (is_oceanbase_server()) {
    return login_req_.get_hsr_result().full_name_;
  } else {
    if (OB_NOT_NULL(shard_conn_)) {
      return shard_conn_->full_username_.config_string_;
    } else {
      return ObString::make_empty_string();
    }
  }
}

int ObClientSessionInfo::update_cached_variable(const ObString &var_name,
                                                ObSessionSysField *field)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(OB_FIELD_LAST_INSERT_ID_MODIFY_MOD != field->modify_mod_)) {
    // update cached variables
    ObCachedVariableType type = CACHED_VAR_MAX;
    if (CACHED_VAR_MAX != (type = cached_variables_.get_type(var_name))) {
      if (OB_FAIL(cached_variables_.update_var(type, field->value_))) {
        LOG_WARN("update cached variable failed ", K(var_name), KPC(field), K(type), K(ret));
      }
    }
  }

  return ret;
}

int ObClientSessionInfo::update_common_sys_variable(const ObString &var_name, const ObObj &value,
                                                    const bool is_need_insert, const bool is_oceanbase)
{
  int ret = OB_SUCCESS;
  ObSessionSysField *field = NULL;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.update_common_sys_variable(var_name, value, field, is_need_insert, is_oceanbase))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("filed to update comomn sys variable", K(var_name), K(ret));
    }
  } else if (OB_ISNULL(field)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filed is null after update_system_variable, it should not happened", K(var_name), K(ret));
  } else {
    switch (field->modify_mod_) {
      case OB_FIELD_HOT_MODIFY_MOD: {
        version_.inc_common_hot_sys_var_version();
        break;
      }
      case OB_FIELD_COLD_MODIFY_MOD: {
        if (!field->is_readonly() && field->is_session_scope()) {
          version_.inc_common_sys_var_version();
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error modify_mod_ type, it should not happened", K(field->modify_mod_), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_cached_variable(var_name, field))) {
        LOG_WARN("fail to update cached variable ", K(var_name), KPC(field), K(ret));
      }
    }
  }
  return ret;
}

int ObClientSessionInfo::update_common_sys_variable(const ObString &var_name, const ObString &value,
                                                    const bool is_need_insert, const bool is_oceanbase)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_varchar(value);
  if (OB_FAIL(update_common_sys_variable(var_name, obj, is_need_insert, is_oceanbase))) {
    LOG_WARN("fail to update common sys variable", K(var_name), K(ret));
  }
  return ret;
}

int ObClientSessionInfo::update_mysql_sys_variable(const ObString &var_name, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(update_common_sys_variable(var_name, value, false))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ObSessionSysField *field = NULL;
      if (OB_FAIL(field_mgr_.update_mysql_system_variable(var_name, value, field))){
        LOG_WARN("fail to update system variable", K(var_name), K(ret));
      } else if (OB_ISNULL(field)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filed is null after update_system_variable, it should not happened", K(var_name), K(ret));
      } else {
        switch (field->modify_mod_) {
          case OB_FIELD_HOT_MODIFY_MOD:
            version_.inc_mysql_hot_sys_var_version();
            break;
          case OB_FIELD_COLD_MODIFY_MOD:
            if (!field->is_readonly() && field->is_session_scope()) {
              version_.inc_mysql_sys_var_version();
            }
            break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error modify_mod_ type, it should not happened", K(field->modify_mod_), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(update_cached_variable(var_name, field))) {
            LOG_WARN("fail to update cached variable ", K(var_name), KPC(field), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObClientSessionInfo::update_mysql_sys_variable(const ObString &var_name,
                                                   const ObString &value)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_varchar(value);
  if (OB_FAIL(update_mysql_sys_variable(var_name, obj))) {
    LOG_WARN("fail to update sys variable", K(var_name), K(ret));
  }
  return ret;
}

int ObClientSessionInfo::update_sys_variable(const ObString &var_name, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(update_common_sys_variable(var_name, value, false))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ObSessionSysField *field = NULL;
      if (OB_FAIL(field_mgr_.update_system_variable(var_name, value, field))){
        LOG_WARN("fail to update system variable", K(var_name), K(ret));
      } else if (OB_ISNULL(field)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filed is null after update_system_variable, it should not happened", K(var_name), K(ret));
      } else {
        switch (field->modify_mod_) {
          case OB_FIELD_LAST_INSERT_ID_MODIFY_MOD:
            version_.inc_last_insert_id_version();
            break;
          case OB_FIELD_HOT_MODIFY_MOD:
            version_.inc_hot_sys_var_version();
            break;
          case OB_FIELD_COLD_MODIFY_MOD:
            if (!field->is_readonly() && field->is_session_scope()) {
              version_.inc_sys_var_version();
            }
            break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error modify_mod_ type, it should not happened", K(field->modify_mod_), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(update_cached_variable(var_name, field))) {
            LOG_WARN("fail to update cached variable ", K(var_name), KPC(field), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObClientSessionInfo::update_sys_variable(const ObString &var_name, const ObString &value)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_varchar(value);
  if (OB_FAIL(update_sys_variable(var_name, obj))) {
    LOG_WARN("fail to update sys variable", K(var_name), K(ret));
  }
  return ret;
}

int ObClientSessionInfo::get_sys_variable(const ObString &var_name, ObSessionSysField *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.get_sys_variable(var_name, value))) {
    LOG_WARN("fail to get sys variable", K(var_name), K(ret));
  }
  return  ret;
}

int ObClientSessionInfo::get_sys_variable_value(const ObString &var_name, ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.get_sys_variable_value(var_name, value))) {
    LOG_WARN("fail to get sys variable value", K(var_name), K(ret));
  }
  return  ret;
}

int ObClientSessionInfo::get_sys_variable_value(const char *name, ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("variable name is NULL", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ObString::make_string(name)), K(ret));
  } else if (OB_FAIL(field_mgr_.get_sys_variable_value(name, value))) {
    LOG_WARN("fail to get sys variable value", K(ObString::make_string(name)), K(ret));
  }
  return ret;
}

int ObClientSessionInfo::sys_variable_exists(const ObString &var_name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.sys_variable_exists(var_name, is_exist))) {
    LOG_WARN("fail to check if sys variable exists", K(var_name), K(ret));
  }
  return  ret;
}

// @synopsis get variable type by name
int ObClientSessionInfo::get_sys_variable_type(const ObString &var_name, ObObjType &type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.get_sys_variable_type(var_name, type))) {
    LOG_WARN("fail to get sys variable type", K(var_name), K(ret));
  }
  return  ret;
}

//sys variables related methords
int ObClientSessionInfo::replace_user_variable(const ObString &var_name, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.replace_user_variable(var_name, val))) {
    LOG_WARN("fail to replace user variable", K(ret));
  } else {
    version_.inc_user_var_version();
  }
  return  ret;
}

int ObClientSessionInfo::replace_user_variable(const ObString &name,
                                               const ObString &val)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  obj.set_varchar(val);
  if (OB_FAIL(replace_user_variable(name, obj))) {
    LOG_WARN("fail to replace user variable", K(name), K(obj), K(ret));
  } else {
    LOG_DEBUG("succ to replace user variable", K(name), K(obj));
  }
  return ret;
}

int ObClientSessionInfo::remove_user_variable(const ObString &var_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.remove_user_variable(var_name))) {
    LOG_WARN("fail to remove user variable", K(var_name), K(ret));
  } else {
    version_.inc_user_var_version();
  }
  return  ret;
}

int ObClientSessionInfo::remove_all_user_variable()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(field_mgr_.remove_all_user_vars())) {
    LOG_WARN("fail to remove all user variable", K(ret));
  }
  return ret;
}

int ObClientSessionInfo::get_user_variable(const ObString &var_name, ObSessionUserField *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(var_name), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.get_user_variable(var_name, value))) {
    LOG_WARN("fail to get user variable", K(var_name), K(ret));
  }
  return  ret;
}

int ObClientSessionInfo::get_user_variable_value(const ObString &var_name, ObObj &value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.get_user_variable_value(var_name, value))) {
    LOG_WARN("fail to get user variable value", K(var_name), K(ret));
  }
  return  ret;
}

int ObClientSessionInfo::user_variable_exists(const ObString &var_name, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(var_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("var name is empty", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(var_name), K(ret));
  } else if (OB_FAIL(field_mgr_.user_variable_exists(var_name, is_exist))) {
    LOG_WARN("fail to check if user variable exist", K(var_name), K(ret));
  }
  return  ret;
}

int ObClientSessionInfo::extract_all_variable_reset_sql(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ret));
  } else if (OB_FAIL(sql.append_fmt("SET"))) {
    LOG_WARN("fail to append_fmt 'SET'", K(ret));
    // reset all session variable,
    // including hot/cold sys variable ,user variable, last_insert_id
  } else if (OB_FAIL(field_mgr_.format_all_var(sql))) {
    LOG_WARN("fail to format_all_sys_var.", K(sql), K(*this), K(ret));
  } else {
    *(sql.ptr() + sql.length() - 1) = ';'; //replace ',' with ';'
  }
  return ret;
}

int ObClientSessionInfo::extract_user_variable_reset_sql(ObServerSessionInfo &server_info,
                                                         ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ret));
  } else {
    bool need_reset = false;
    if (OB_FAIL(sql.append_fmt("SET"))) {
      LOG_WARN("fail to append_fmt 'SET'", K(ret));
    } else if (!is_oceanbase_server()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extract user variable reset sql only for oceanbase server");
    } else { /* do nothing */ }

    //reset user variable
    if (OB_SUCC(ret)) {
      if (need_reset_user_session_vars(server_info)) {
        need_reset = true;
        if (OB_FAIL(field_mgr_.format_user_var(sql))) {
          LOG_WARN("fail to format_user_var.", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_reset) {
        *(sql.ptr() + sql.length() - 1) = ';'; //replace ',' with ';'
      } else {
        sql.reset();
      }
    } else {
      sql.reset();
    }
  }
  return ret;
}

int ObClientSessionInfo::extract_variable_reset_sql(ObServerSessionInfo &server_info,
                                                    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ret));
  } else {
    bool need_reset = false;
    if (OB_FAIL(sql.append_fmt("SET"))) {
      LOG_WARN("fail to append_fmt 'SET'", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (is_oceanbase_server()) {
        if (OB_FAIL(extract_oceanbase_variable_reset_sql(server_info, sql, need_reset))) {
          LOG_WARN("fail to extract_oceanbase_variable_reset_sql", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      } else {
        if (OB_FAIL(extract_mysql_variable_reset_sql(server_info, sql, need_reset))) {
          LOG_WARN("fail to extract_mysql_variable_reset_sql", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      }
    }

    // Attention!! need first set OB or MySQL var, then set common var
    // because OB or MySQL var set maybe have same var with common var set. But common var set is neweset
    //reset cold common sys variable
    if (OB_SUCC(ret)) {
      if (need_reset || need_reset_common_cold_session_vars(server_info)) {
        need_reset = true;
        if (OB_FAIL(field_mgr_.format_common_sys_var(sql))) {
          LOG_WARN("fail to format_common_sys_var.", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      }
    }

    //reset hot common sys variable
    if (OB_SUCC(ret)) {
      if (need_reset || need_reset_common_hot_session_vars(server_info)) {
        need_reset = true;
        if (OB_FAIL(field_mgr_.format_common_hot_sys_var(sql))) {
          LOG_WARN("fail to format_common_hot_sys_var.", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      }
    }

    //reset user variable
    if (OB_SUCC(ret)) {
      if (need_reset_user_session_vars(server_info)) {
        need_reset = true;
        if (OB_FAIL(field_mgr_.format_user_var(sql))) {
          LOG_WARN("fail to format_user_var.", K(sql), K(*this),
                   K(server_info), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (need_reset) {
        *(sql.ptr() + sql.length() - 1) = ';'; //replace ',' with ';'
      } else {
        sql.reset();
      }
    } else {
      sql.reset();
    }
  }
  return ret;
}

int ObClientSessionInfo::extract_mysql_variable_reset_sql(ObServerSessionInfo &server_info,
                                                          ObSqlString &sql, bool &need_reset)
{
  int ret = OB_SUCCESS;
  //reset cold mysql sys variable
  if (OB_SUCC(ret)) {
    if (need_reset_mysql_cold_session_vars(server_info)) {
      need_reset = true;
      if (OB_FAIL(field_mgr_.format_mysql_sys_var(sql))) {
        LOG_WARN("fail to format_mysql_sys_var.", K(sql), K(*this),
                 K(server_info), K(ret));
      }
    }
  }

  //reset hot mysql sys variable
  if (OB_SUCC(ret)) {
    if (need_reset_mysql_hot_session_vars(server_info)) {
      need_reset = true;
      if (OB_FAIL(field_mgr_.format_mysql_hot_sys_var(sql))) {
        LOG_WARN("fail to format_mysql_hot_sys_var.", K(sql), K(*this),
                 K(server_info), K(ret));
      }
    }
  }

  return ret;
}

int ObClientSessionInfo::extract_oceanbase_variable_reset_sql(ObServerSessionInfo &server_info,
                                                              ObSqlString &sql, bool &need_reset)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (need_reset_safe_read_snapshot(server_info)) {
      need_reset = true;
      if (OB_FAIL(sql.append_fmt(" @@%s = %ld,",
                                 OB_SV_SAFE_WEAK_READ_SNAPSHOT,
                                 syncing_safe_read_snapshot_))) {
        LOG_WARN("fail to append_fmt safe read_snapshot", K(ret));
      } else {
        LOG_DEBUG("will sync safe snapshot ", K_(syncing_safe_read_snapshot));
      }
    }
  }

  //reset cold sys variable
  if (OB_SUCC(ret)) {
    if (need_reset_cold_session_vars(server_info)) {
      need_reset = true;
      if (OB_FAIL(field_mgr_.format_sys_var(sql))) {
        LOG_WARN("fail to format_sys_var.", K(sql), K(*this),
                 K(server_info), K(ret));
      }
    }
  }

  //reset hot sys variable
  if (OB_SUCC(ret)) {
    if (need_reset_hot_session_vars(server_info)) {
      need_reset = true;
      if (OB_FAIL(field_mgr_.format_hot_sys_var(sql))) {
        LOG_WARN("fail to format_hot_sys_var.", K(sql), K(*this),
                 K(server_info), K(ret));
      }
    }
  }

  //reset last insert id
  if (OB_SUCC(ret)) {
    if (need_reset_last_insert_id(server_info)) {
      need_reset = true;
      if (OB_FAIL(field_mgr_.format_last_insert_id(sql))) {
          LOG_WARN("fail to format_last_insert_id.", K(sql),
                   K(*this), K(server_info), K(ret));
      }
    }
  }

  return ret;
}

int ObClientSessionInfo::extract_last_insert_id_reset_sql(ObServerSessionInfo &server_info,
                                                          ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ret));
  } else if (!need_reset_last_insert_id(server_info)) {
    // do nothing
  } else if (OB_FAIL(sql.append_fmt("SET"))) {
    LOG_WARN("fail to append_fmt ", K(ret));
  } else if (OB_FAIL(field_mgr_.format_last_insert_id(sql))) {
    LOG_WARN("fail to format_last_insert_id.", K(sql), K(ret));
  } else {
    *(sql.ptr() + sql.length() - 1) = ';'; //replace ',' with ';'
  }
  return ret;
}


int ObClientSessionInfo::extract_changed_schema(ObServerSessionInfo &server_info,
                                                ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("client session is not inited", K(ret));
  } else if (!need_reset_database(server_info)) {
    // do nothing
  } else if (OB_FAIL(get_database_name(db_name))) {
    LOG_ERROR("fail to get database name", K(*this), K(server_info), K(ret));
  } else {}
  return ret;
}

int ObClientSessionInfo::is_equal_with_snapshot(const ObString &name,
                                                const ObString &value, bool &is_equal)
{
  ObObj obj_value;
  obj_value.set_varchar(value);
  return field_mgr_.is_equal_with_snapshot(name, obj_value, is_equal);
}

int ObClientSessionInfo::get_all_user_var_names(ObIArray<ObString> &names)
{
  return field_mgr_.get_all_user_var_names(names);
}

int ObClientSessionInfo::get_changed_sys_var_names(ObIArray<ObString> &names)
{
  return field_mgr_.get_changed_sys_var_names(names);
}

int ObClientSessionInfo::get_all_sys_var_names(ObIArray<ObString> &names)
{
  return field_mgr_.get_all_sys_var_names(names);
}

int ObClientSessionInfo::get_changed_sys_vars(ObIArray<ObSessionSysField> &fileds)
{
  return field_mgr_.get_all_changed_sys_vars(fileds);
}

int ObClientSessionInfo::get_all_sys_vars(ObIArray<ObSessionSysField> &fileds)
{
  return field_mgr_.get_all_sys_vars(fileds);
}

int ObClientSessionInfo::get_all_user_vars(ObIArray<ObSessionBaseField> &fileds)
{
  return field_mgr_.get_all_user_vars(fileds);
}

int ObClientSessionInfo::get_session_timeout(const char *timeout_name, int64_t &timeout) const
{
  int ret = OB_SUCCESS;
  ObObj timeout_obj;
  if (OB_FAIL(get_sys_variable_value(timeout_name, timeout_obj))) {
    LOG_WARN("fail to get sys variable", "name", timeout_name, K(ret));
  } else if (OB_FAIL(timeout_obj.get_int(timeout))) {
    LOG_WARN("fail to get timeout", K(ret));
  }
  return ret;
}

int ObClientSessionInfo::set_start_trans_sql(const ObString &sql)
{
  int ret = OB_SUCCESS;
  char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(sql.length()));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(sql.length()), K(ret));
  } else {
    // if we have save start_trans_sql before, reset it (and free memory buf)
    // TODO: reuse it?
    reset_start_trans_sql();
    saved_start_trans_sql_.assign_buffer(buf, sql.length());
    int32_t writed_size = saved_start_trans_sql_.write(sql.ptr(), sql.length());
    if (OB_UNLIKELY(sql.length() != writed_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("writed unexpected", K(writed_size), K(sql.length()), K(ret));
    }
  }
  return ret;
}

void ObClientSessionInfo::reset_start_trans_sql()
{
  // free mem
  if (NULL != saved_start_trans_sql_.ptr()) {
    op_fixed_mem_free(saved_start_trans_sql_.ptr(), saved_start_trans_sql_.size());
  }
  saved_start_trans_sql_.reset();
}

int ObClientSessionInfo::load_all_cached_variable()
{
  int ret = OB_SUCCESS;
  ObString name;
  ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < CACHED_VAR_MAX; ++i) {
    ObCachedVariableType type = static_cast<ObCachedVariableType>(i);
    name = ObCachedVariables::get_name(type);
    if (OB_FAIL(field_mgr_.get_sys_variable_value(name, obj))) {
      LOG_WARN("get system variable value failed", K(name), K(obj), K(ret));
    } else if (OB_FAIL(cached_variables_.update_var(type, obj))) {
      LOG_WARN("update cached variable failed ", K(name), K(type), K(obj), K(ret));
    } else {}
  }
  return ret;
}

int ObClientSessionInfo::revalidate_sys_var_set(ObSysVarSetProcessor &var_set_processor)
{
  int ret = OB_SUCCESS;
  bool need_update_var_set = false;

  if (NULL == var_set_processor_ || &var_set_processor != var_set_processor_) {
    need_update_var_set = true;
    var_set_processor_ = &var_set_processor;
  }

  ObDefaultSysVarSet *cur_var_set = field_mgr_.get_sys_var_set();
  ObDefaultSysVarSet *newest_var_set = NULL;
  if (OB_ISNULL(newest_var_set = var_set_processor_->acquire())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to acquire sys var set", K(newest_var_set), K(ret));
  } else if (need_update_var_set
             || (NULL == cur_var_set)
             || (newest_var_set->get_sys_var_count() != cur_var_set->get_sys_var_count())) {

    LOG_DEBUG("will add sys var set", "cur_var_set[ptr=", cur_var_set, "count",
              ((NULL == cur_var_set) ? 0 : cur_var_set->get_sys_var_count()),
              "version", ((NULL == cur_var_set) ? 0 : cur_var_set->get_last_modified_time()),
              "newest_var_set[ptr", newest_var_set, "count", newest_var_set->get_sys_var_count(),
              "version", newest_var_set->get_last_modified_time());

    // revalidate new var set
    // add_sys_var_set will inc_ref newest_var_set and dec_ref cur_var_set
    if (OB_FAIL(add_sys_var_set(*newest_var_set))) {
      LOG_WARN("fail to add sys var set", K(ret));
    }
  }
  if (OB_LIKELY(NULL != newest_var_set)) {
    // release newest_var_set
    var_set_processor_->release(newest_var_set);
  }

  return ret;
}

void ObClientSessionInfo::destroy()
{
  if (is_inited_)  {
    field_mgr_.destroy();
    login_req_.destroy();
    destroy_sess_info_list();
    is_inited_ = false;
  }
  if (NULL != var_set_processor_) {
    ObDefaultSysVarSet *sys_set = field_mgr_.get_sys_var_set();
    if (NULL != sys_set) {
      // here we should release the the sys_var_set
      var_set_processor_->release(sys_set);
      sys_set = NULL;
    }
    var_set_processor_ = NULL;
  }
  reset_start_trans_sql();
  clear_init_sql();
 
  destroy_ps_id_entry_map();
  destroy_cursor_id_addr_map();
  destroy_ps_id_addrs_map();
  destroy_piece_info_map();
  destroy_text_ps_name_entry_map();
  is_trans_specified_ = false;
  is_global_vars_changed_ = false;
  is_user_idc_name_set_ = false;
  is_read_consistency_set_ = false;
  is_oracle_mode_ = false;
  is_proxy_route_policy_set_ = false;

  enable_shard_authority_ = false;
  enable_reset_db_ = true;

  is_read_only_user_ = false;
  is_request_follower_user_ = false;
  obproxy_force_parallel_query_dop_ = 1;
  ob_max_read_stale_time_ = 0;

  global_vars_version_ = OB_INVALID_VERSION;
  obproxy_route_addr_ = 0;
  safe_read_snapshot_ = 0;
  syncing_safe_read_snapshot_ = 0;
  route_policy_ = 1;
  proxy_route_policy_ = MAX_PROXY_ROUTE_POLICY;
  client_cap_ = 0;
  server_cap_ = 0;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  free_real_meta_cluster_name();

  server_type_ = DB_OB_MYSQL;
  if (NULL != shard_conn_) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }
  if (NULL != shard_prop_) {
    shard_prop_->dec_ref();
    shard_prop_ = NULL;
  }
  group_id_ = OBPROXY_MAX_DBMESH_ID;
  table_id_ = OBPROXY_MAX_DBMESH_ID;
  es_id_ = OBPROXY_MAX_DBMESH_ID;
  is_allow_use_last_session_ = true;
  up_info_.reset();

  last_server_addr_.reset();
  last_server_sess_id_ = 0;
}

void ObClientSessionInfo::destroy_ps_id_entry_map()
{
  ObPsIdEntryMap::iterator last = ps_id_entry_map_.end();
  ObPsIdEntryMap::iterator tmp_iter;
  for (ObPsIdEntryMap::iterator ps_iter = ps_id_entry_map_.begin(); ps_iter != last;) {
    tmp_iter = ps_iter;
    ++ps_iter;
    tmp_iter->destroy();
  }
  ps_id_entry_map_.reset();
}

void ObClientSessionInfo::destroy_cursor_id_addr_map()
{
  ObCursorIdAddrMap::iterator last = cursor_id_addr_map_.end();
  ObCursorIdAddrMap::iterator tmp_iter;
  for (ObCursorIdAddrMap::iterator cursor_iter = cursor_id_addr_map_.begin(); cursor_iter != last;) {
    tmp_iter = cursor_iter;
    ++cursor_iter;
    tmp_iter->destroy();
  }
  cursor_id_addr_map_.reset();
}

void ObClientSessionInfo::destroy_piece_info_map()
{
  ObPieceInfoMap::iterator last = piece_info_map_.end();
  ObPieceInfoMap::iterator tmp_iter;
  for (ObPieceInfoMap::iterator iter = piece_info_map_.begin(); iter != last;) {
    tmp_iter = iter;
    ++iter;
    op_free(&(*tmp_iter));
  }
  piece_info_map_.reset();
}

void ObClientSessionInfo::destroy_ps_id_addrs_map()
{
  ObPsIdAddrsMap::iterator last = ps_id_addrs_map_.end();
  ObPsIdAddrsMap::iterator tmp_iter;
  for (ObPsIdAddrsMap::iterator ps_id_addrs_iter = ps_id_addrs_map_.begin(); ps_id_addrs_iter != last;) {
    tmp_iter = ps_id_addrs_iter;
    ++ps_id_addrs_iter;
    tmp_iter->destroy();
  }
  ps_id_addrs_map_.reset();
}

void ObClientSessionInfo::destroy_text_ps_name_entry_map()
{
  ObTextPsNameEntryMap::iterator last = text_ps_name_entry_map_.end();
  ObTextPsNameEntryMap::iterator tmp_iter;
  for (ObTextPsNameEntryMap::iterator ps_iter = text_ps_name_entry_map_.begin(); ps_iter != last;) {
    tmp_iter = ps_iter;
    ++ps_iter;
    tmp_iter->destroy();
  }
  text_ps_name_entry_map_.reset();
}

void ObClientSessionInfo::destroy_sess_info_list()
{
  int64_t sess_info_count = sess_info_list_.count();
  for (int64_t i = 0; i < sess_info_count; ++i) {
    SessionInfoField &field = sess_info_list_.at(i);
    field.reset_sess_info_value();
  }
  sess_info_list_.destroy();
}

ObTextPsNameEntry *ObClientSessionInfo::get_text_ps_name_entry(const common::ObString &text_ps_name) const
{
  int ret = OB_SUCCESS;
  ObTextPsNameEntry *text_ps_name_entry = NULL;
  if (OB_FAIL(text_ps_name_entry_map_.get_refactored(text_ps_name, text_ps_name_entry))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get text ps name enrtry with client stmt name", K(ret), K(text_ps_name));
    }
  } else if (OB_ISNULL(text_ps_name_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text ps name entry is null", K(ret), K(text_ps_name));
  }
  return text_ps_name_entry;
}

int ObClientSessionInfo::add_text_ps_name_entry(ObTextPsNameEntry *text_ps_name_entry)
{
  LOG_DEBUG("add text ps name entry", K(text_ps_name_entry->text_ps_name_));
  return text_ps_name_entry_map_.unique_set(text_ps_name_entry);
}

int ObClientSessionInfo::delete_text_ps_name_entry(ObString& text_ps_name)
{
  ObTextPsNameEntry *tmp = text_ps_name_entry_map_.remove(text_ps_name);
  if (NULL == tmp) {
    LOG_WARN("unexpected", K(text_ps_name));
  } else {
    tmp->destroy();
  }
  return OB_SUCCESS;
}

ObCursorIdAddr *ObClientSessionInfo::get_cursor_id_addr(uint32_t client_cursor_id) const
{
  ObCursorIdAddr *cursor_id_addr = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(cursor_id_addr_map_.get_refactored(client_cursor_id, cursor_id_addr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get cursor_id_addr with client cursor id", K(ret), K(client_cursor_id));
    }
  } else if (OB_ISNULL(cursor_id_addr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor_id_addr is null", K(ret), K(client_cursor_id));
  }
  return cursor_id_addr;
}

ObPsIdAddrs *ObClientSessionInfo::get_ps_id_addrs(uint32_t client_ps_id) const
{
  ObPsIdAddrs *ps_id_addrs = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ps_id_addrs_map_.get_refactored(client_ps_id, ps_id_addrs))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get ps_id_addrs with client ps id", K(ret), K(client_ps_id));
    }
  } else if (OB_ISNULL(ps_id_addrs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ps_id_addrs is null", K(ret), K(client_ps_id));
  }
  return ps_id_addrs;
}

int ObClientSessionInfo::update_sess_info_field(int16_t type, ObString &sess_info_value, int64_t &version)
{
  int ret = OB_SUCCESS;
  version = 0;
  bool is_field_exist = false;
  int64_t count = sess_info_list_.count();
  for(int64_t i = 0; i < count; i ++) {
    SessionInfoField &field = sess_info_list_.at(i);
    if (field.get_sess_info_type() == type) {
      is_field_exist = true;
      field.reset_sess_info_value();
      field.inc_version();
      field.set_sess_info_value(sess_info_value);
      version = field.get_version();
      break;
    }
  }
  if (!is_field_exist) {
    version = 1;
    if (OB_FAIL(sess_info_list_.push_back(SessionInfoField(sess_info_value, type, version)))) {
      LOG_WARN("fail to record sess info field version", K(type), K(sess_info_value), K(ret));
    }
  }
  return ret;
}


}//end of namespace proxy
}//end of namespace obproxy
}//end of namespace oceanbase

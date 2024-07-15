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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY

#include "lib/ob_errno.h"
#include "omt/ob_proxy_config_table_processor.h"
#include "obutils/ob_config_processor.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_reload_config.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "cmd/ob_internal_cmd_processor.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "opsql/parser/ob_proxy_parser.h"
#include "obproxy/cmd/ob_show_config_handler.h"

static const char *EXECUTE_SQL =
    "replace into proxy_config(vip, vid, vport, cluster_name, tenant_name, name, value, config_level) values("
    "'%.*s', %ld, %ld, '%.*s', '%.*s', '%s', '%s', '%.*s')";

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::opsql;

#define GetProxyConfigVariableStr(name)\
do {\
  ObConfigItem item;\
  if (OB_SUCC(ret) && OB_FAIL(get_global_config_processor().get_proxy_config( \
  vip_info_.vip_addr_, vip_info_.cluster_name_.ptr(), vip_info_.tenant_name_.ptr(), #name, item, false))) {\
    PROXY_LOG(WDIAG, "fail to get " #name , K_(vip_info_.vip_addr), K_(vip_info_.cluster_name), K_(vip_info_.tenant_name), K(ret)); }  \
  else {\
    name##_.rewrite(item.str(), strlen(item.str()));\
  }} while(0)

#define GetProxyConfigWithType(name, fun_type, var_type)\
do {\
  ObConfig##var_type##Item item;\
  if (OB_SUCC(ret) && OB_FAIL(get_global_config_processor().get_proxy_config##fun_type( \
  vip_info_.vip_addr_, vip_info_.cluster_name_.ptr(), vip_info_.tenant_name_.ptr(), #name, item, false))) {\
    PROXY_LOG(WDIAG, "fail to get " #name , K_(vip_info_.vip_addr), K_(vip_info_.cluster_name), K_(vip_info_.tenant_name), K(ret)); }  \
  else {\
    name##_ = item.get_value();\
  }} while(0)
  
  #define GetProxyConfigInt(name)   GetProxyConfigWithType(name, _int_item, Int)
  #define GetProxyConfigBool(name)  GetProxyConfigWithType(name, _bool_item, Bool)
  #define GetProxyConfigTime(name)  GetProxyConfigWithType(name, , Time)

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

ObProxyConfigItem* ObProxyConfigItem::clone()
{
  ObProxyConfigItem *ret = NULL;
  if (OB_ISNULL(ret = op_alloc(ObProxyConfigItem))) {
    LOG_WDIAG("op_alloc ObProxyConfigItem failed");
  } else {
    ret->vip_info_ = vip_info_;
    ret->config_level_ = config_level_;
    ret->config_item_ = config_item_;
  }

  return ret;
}

ObProxyConfigItem::ObProxyConfigItem(const ObProxyConfigItem& item)
{
  vip_info_ = item.vip_info_;
  config_level_ = item.config_level_;
  config_item_ = item.config_item_;
}

ObProxyConfigItem& ObProxyConfigItem::operator =(const ObProxyConfigItem &item)
{
  if (this != &item) {
    vip_info_ = item.vip_info_;
    config_level_ = item.config_level_;
    config_item_ = item.config_item_;
  }
  return *this;
}

int64_t ObProxyConfigItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(vip_info_.vip_addr), K_(vip_info_.tenant_name), K_(vip_info_.cluster_name), K_(config_level), K_(config_item));
  J_OBJ_END();
  return pos;
}

uint64_t ObProxyConfigItem::get_hash() const
{
  uint64_t hash = 0;
  hash = vip_info_.get_hash();
  hash = murmurhash(config_item_.name(), static_cast<int32_t>(strlen(config_item_.name())), hash);
  return hash;
}

void ObProxyMultiLevelConfig::free()
{
  op_free(this);
}

uint64_t ObVipInfo::get_hash() const
{
  uint64_t hash = 0;
  hash = vip_addr_.hash(0);
  hash = tenant_name_.hash(hash);
  hash = cluster_name_.hash(hash);
  return hash;
}

bool ObVipInfo::operator==(const ObVipInfo &other) const
{
  bool bret = true;
  if (this != &other) {
    bret = (vip_addr_ == other.vip_addr_
            && tenant_name_ == other.tenant_name_
            && cluster_name_ == other.cluster_name_);
  }
  return bret;
}

uint64_t ObProxyMultiLevelConfig::get_hash() const
{
  return vip_info_.get_hash();
}

int ObProxyConfigTableProcessor::update_proxy_multi_config(
                                 const ObVipInfo &key,
                                 ObProxyMultiLevelConfig* &cur_config)
{
  int ret = OB_SUCCESS;
  const obutils::ObVipAddr &vip_addr = key.vip_addr_;
  const ObConfigVariableString &tenant_name = key.tenant_name_;
  const ObConfigVariableString &cluster_name = key.cluster_name_;
  // 传入的cur_config必须为NULL，然后生成一份最新的config传出
  // 1. 先直接创建config、并set_config()
  // 2. 加写锁判断Hashmap中是否需要插入，最后更新HashMap，返回cur_config
  if (OB_UNLIKELY(NULL != cur_config)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "cur_config ptr must is nullpt", K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
  } else {
    DRWLock::WRLockGuard guard(proxy_config_lock_);
    ObProxyMultiLevelConfig *hashmap_config = NULL;
    bool will_insert_hashmap = false;

    if (OB_FAIL(proxy_multi_level_config_map_.get_refactored(key, hashmap_config))) {
      if (OB_HASH_NOT_EXIST != ret) {
        PROXY_LOG(WDIAG, "fail to find multi level config from HashMap",
                K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        ret = OB_SUCCESS;
        will_insert_hashmap = true;
      }
    } else if (OB_NOT_NULL(hashmap_config)) { // HashMap已经存在
      if (OB_UNLIKELY(config_version_ < hashmap_config->config_version_)) {
        // 异常情况，理论不会小于。除非溢出？
        ret = OB_ERR_UNEXPECTED;
        PROXY_LOG(WDIAG, "unexcepect global config version less than HashMap config version",
                  K_(config_version), K_(hashmap_config->config_version), K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
      } else if (config_version_ == hashmap_config->config_version_) {
        // 已有其他线程更新，直接复用; 此时外部要引用，引用计数+1
        hashmap_config->inc_ref();
        cur_config = hashmap_config;
      } else {
        // 删除Hash老的config，并引用计数减一
        proxy_multi_level_config_map_.remove(hashmap_config->vip_info_);
        hashmap_config->dec_ref();
        will_insert_hashmap = true;
      }
    } else {
      // impossiable
      ret = OB_ERR_UNEXPECTED;
      PROXY_LOG(WDIAG, "multi proxyconfig nullptr is nullptr from HashMap",
                K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
    }

    if (OB_SUCC(ret) && will_insert_hashmap) {
      // 创建config，构造函数中引用计数+1
      if (OB_ISNULL(cur_config = op_alloc(ObProxyMultiLevelConfig))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WDIAG, "fail to alloc memory for ObProxyMultiLevelConfig",
                  K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        cur_config->vip_info_ = key;
        if (OB_FAIL(cur_config->set_config(config_version_))) {
        PROXY_LOG(WDIAG, "fail to set multi-level config", K(ret), K(vip_addr),
                  K(cluster_name), K(tenant_name));
        } else if (OB_FAIL(proxy_multi_level_config_map_.unique_set(cur_config))) {
          PROXY_LOG(WDIAG, "fail to insert ObProxyMultiLevelConfig to HashMap",
                    K(ret), K(vip_addr), K(cluster_name), K(tenant_name));
        } else {
          // 成功插入，此时外部要引用，引用计数+1
          cur_config->inc_ref();
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(cur_config)) {
    cur_config->dec_ref();
    cur_config = NULL;
  }
  return ret;
}

int ObProxyConfigTableProcessor::get_proxy_multi_config(const obutils::ObVipAddr &vip_addr,
                                                        const common::ObString &cluster_name, 
                                                        const common::ObString &tenant_name, 
                                                        const uint64_t global_version,
                                                        ObProxyMultiLevelConfig* &old_config)
{
  int ret = OB_SUCCESS;
  ObProxyMultiLevelConfig* cur_config = NULL;
  ObVipInfo key;
  key.vip_addr_ = vip_addr;
  key.cluster_name_.rewrite(cluster_name);
  key.tenant_name_.rewrite(tenant_name);
  // 注意在全局对象中调用！
  // 先读数据是否存在，存在则：判断数据版本是否等于全局版本，不匹配从Hash中删除
  // 如果不等于全局版本，或者不存在（都用cur_config == NULL判断），并创建config并插入Hash
  {
    DRWLock::RDLockGuard guard(proxy_config_lock_);
    if (OB_FAIL(proxy_multi_level_config_map_.get_refactored(key, cur_config))) {
      if (OB_HASH_NOT_EXIST == ret) {
        cur_config = NULL;
        ret = OB_SUCCESS;
      } else {
        PROXY_LOG(WDIAG, "fail to get multi level config from global HashMap",
                  K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
      }
    } 
    if (OB_SUCC(ret) && OB_NOT_NULL(cur_config)) {
      if (global_version <= cur_config->config_version_) {
        cur_config->inc_ref();
      } else {
        cur_config = NULL;
      }
    }
  }
  
  // 需要更新cur_config
  if (OB_SUCC(ret) && OB_ISNULL(cur_config)) {
    if (OB_FAIL(update_proxy_multi_config(key, cur_config))) {
      PROXY_LOG(WDIAG, "fail to update multi-level config", K(vip_addr), K(cluster_name), K(tenant_name), K(ret));
    }
  }

  // 更新配置
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(cur_config)) {
      // impossible
      PROXY_LOG(WDIAG, "multi level config is nullptr from HashMap", K(ret), K(vip_addr), K(cluster_name), K(tenant_name));
    } else {
      if (OB_NOT_NULL(old_config)) {
        old_config->dec_ref();
        old_config = NULL;
      }
      // update_proxy_multi_config中已增加引用计数;
      old_config = cur_config;
    }
  }
  return ret;
}

int ObProxyMultiLevelConfig::set_config(const uint64_t global_version)
{
  // 注意：由于外面加了写锁，函数中每次调用get_proxy_config，都必须调用无锁的方法，否则会死锁
  int ret = OB_SUCCESS;
  const obutils::ObVipAddr &addr = vip_info_.vip_addr_;
  const ObConfigVariableString &tenant_name = vip_info_.tenant_name_;
  const ObConfigVariableString &cluster_name = vip_info_.cluster_name_;

  GetProxyConfigVariableStr(proxy_route_policy);
  GetProxyConfigVariableStr(proxy_idc_name);
  GetProxyConfigVariableStr(proxy_primary_zone_name);
  GetProxyConfigVariableStr(mysql_version);
  GetProxyConfigVariableStr(binlog_service_ip);
  GetProxyConfigVariableStr(init_sql);    // init sql
  GetProxyConfigVariableStr(target_db_server);
  GetProxyConfigVariableStr(compression_algorithm);
  GetProxyConfigBool(enable_cloud_full_username);
  GetProxyConfigBool(enable_client_ssl);
  GetProxyConfigBool(enable_server_ssl);
  GetProxyConfigBool(enable_read_write_split);
  GetProxyConfigBool(enable_transaction_split);
  GetProxyConfigTime(read_stale_retry_interval);
  GetProxyConfigInt(obproxy_read_only);
  GetProxyConfigInt(obproxy_read_consistency);
  GetProxyConfigInt(ob_max_read_stale_time);
  GetProxyConfigInt(obproxy_force_parallel_query_dop);
  GetProxyConfigBool(enable_weak_reroute);
  GetProxyConfigBool(enable_single_leader_node_routing);
  GetProxyConfigInt(route_diagnosis_level);
  GetProxyConfigTime(observer_query_timeout_delta);
  GetProxyConfigTime(query_digest_time_threshold);
  GetProxyConfigTime(slow_query_time_threshold);
  // ssl_attributes
  if (OB_SUCC(ret)) {
    ObConfigItem item;
    SSLAttributes ssl_attributes;
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
      addr, cluster_name.ptr(), tenant_name.ptr(), "ssl_attributes", item, false))) {
      PROXY_LOG(WDIAG, "get ssl attributes failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
    } else if (0 != strlen(item.str())) {
      if (OB_FAIL(ObProxyConfigTableProcessor::parse_ssl_attributes(item, ssl_attributes))) {
        PROXY_LOG(WDIAG, "fail to parse ssl attributes", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ssl_attributes_ = ssl_attributes;
    }
  }
  // mysql_config_params中的Time要做转换，乘1000
  observer_query_timeout_delta_ *= 1000;
  query_digest_time_threshold_ *= 1000;
  slow_query_time_threshold_ *= 1000;

  // 更新版本号
  if (OB_SUCC(ret)) {
    config_version_ = global_version;
  }
  return ret;
}

int ObProxyConfigTableProcessor::execute(void *arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("arg is null unexpected", K(ret));
  } else {
    ObFnParams *params = reinterpret_cast<ObFnParams*>(arg);
    if (NULL == params->fields_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fields is null unexpected", K(ret));
    } else if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().set_proxy_config(params->fields_, true, params->row_index_))) {
        LOG_WDIAG("set proxy config failed", K(ret));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().delete_proxy_config(params->fields_, true))) {
        LOG_WDIAG("delete proxy config failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected stmt type", K(params->stmt_type_), K(ret));
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::excute_sync_master(void *arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer arg, excute_sync_master failed", K(ret));
  } else {
    ObFnParams *params = static_cast<ObFnParams*>(arg);
    if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().set_proxy_config(
              params->fields_, false, params->row_index_))) {
        LOG_WDIAG("set proxy config failed", K(ret));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().delete_proxy_config(
              params->fields_, false))) {
        LOG_WDIAG("delete proxy config failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected stmt type", K(params->stmt_type_), K(ret));
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::commit(void *arg, bool is_backup_success)
{
  const ObFnParams &params = *static_cast<ObFnParams*>(arg);
  if (is_backup_success) {
    get_global_proxy_config_table_processor().inc_index();
    get_global_proxy_config_table_processor().inc_config_version();
    if (params.sync_master_failed_) {
      get_global_proxy_config_table_processor().set_need_rebuild_config_map(true);
    }
  } else {
    get_global_proxy_config_table_processor().set_need_rebuild_config_map(true);
    LOG_WDIAG("proxy config commit failed", K(is_backup_success));
  }

  get_global_proxy_config_table_processor().clear_execute_sql();

  return OB_SUCCESS;
}

int ObProxyConfigTableProcessor::before_commit(void *proxy_config_db, void *arg,
                                               bool &is_success, int64_t row_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_config_db) || OB_ISNULL(arg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer from sqlite", K(proxy_config_db), K(arg), K(ret));
  } else {
    sqlite3 *db = static_cast<sqlite3*>(proxy_config_db);
    is_success = true;
    // 同步全局配置，执行sql
    if (OB_FAIL(get_global_proxy_config_table_processor().commit_execute_sql(db))) {
      is_success = false;
      LOG_WDIAG("fail to commit execute sql", K(ret));
    } else {
      ObFnParams* params = static_cast<ObFnParams*>(arg);
      for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
        params->row_index_ = i;
        if (OB_FAIL(get_global_proxy_config_table_processor().excute_sync_master(params))) {
          params->sync_master_failed_ = true;
          LOG_WDIAG("fail to execute before commit to sync master", K(row_num),
                    K(i), K(ret));
        }
      }
    }
  }

  return ret;
}

void ObProxyConfigTableProcessor::inc_index()
{
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  index_ = (index_ + 1) % 2;
}

ObProxyConfigTableProcessor &get_global_proxy_config_table_processor()
{
  static ObProxyConfigTableProcessor g_proxy_config_table_processor;
  return g_proxy_config_table_processor;
}

int ObProxyConfigTableProcessor::init()
{
  int ret = OB_SUCCESS;
  ObConfigHandler handler;
  handler.execute_func_ = &ObProxyConfigTableProcessor::execute;
  handler.commit_func_ = &ObProxyConfigTableProcessor::commit;
  handler.before_commit_func_ = &ObProxyConfigTableProcessor::before_commit;
  if (OB_FAIL(get_global_config_processor().register_callback("proxy_config", handler))) {
    LOG_WDIAG("register proxy config table callback failed", K(ret));
  }

   return ret;
}

void ObProxyConfigTableProcessor::clean_hashmap(ProxyConfigHashMap &config_map)
{
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  clean_hashmap_with_lock(config_map);
}

void ObProxyConfigTableProcessor::clean_hashmap_with_lock(ProxyConfigHashMap &config_map)
{
  ProxyConfigHashMap::iterator last = config_map.end();
  ProxyConfigHashMap::iterator tmp;
  for (ProxyConfigHashMap::iterator it = config_map.begin(); it != last;) {
    tmp = it;
    ++it;
    tmp->destroy();
  }
  config_map.reset();
}

int ObProxyConfigTableProcessor::backup_hashmap_with_lock()
{
  int ret = OB_SUCCESS;
  int64_t backup_index = (index_ + 1) % 2;
  ProxyConfigHashMap &backup_map = proxy_config_map_array_[backup_index];
  ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];
  clean_hashmap_with_lock(backup_map);
  ProxyConfigHashMap::iterator last = current_map.end();
  for (ProxyConfigHashMap::iterator it = current_map.begin(); OB_SUCC(ret) && it != last; ++it) {
    ObProxyConfigItem *item = static_cast<ObProxyConfigItem*>(&(*it))->clone();
    if (NULL == item) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("clone ObProxyConfigItem failed", K(ret));
    } else if (OB_FAIL(backup_map.unique_set(item))) {
      LOG_WDIAG("backup map set refactored failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    clean_hashmap_with_lock(backup_map);
  } else {
    set_need_rebuild_config_map(false);
  }

  return ret;
}

int ObProxyConfigTableProcessor::set_proxy_config(void *arg, const bool is_backup, int64_t row_index)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem *item = NULL;
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (need_rebuild_config_map_ && OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WDIAG("backup hashmap failed", K(ret));
  } else if (OB_ISNULL(item = op_alloc(ObProxyConfigItem))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("op alloc ObProxyConfigItem failed", K(ret));
  } else {
    ObString vip;
    int64_t vport = 0;
    int64_t vid = -1;
    ObString name;
    SqlFieldResult *sql_fields = static_cast<SqlFieldResult*>(arg);
    for (int i = 0; OB_SUCC(ret) && i < sql_fields->field_num_; i++) {
      SqlField &sql_field = *(sql_fields->fields_.at(i));
      // 这里可能内存越界，但是下面立即判断，不会真的访问到非法内存
      SqlColumnValue &sql_column = sql_field.column_values_.at(row_index);  
      if (row_index >= sql_field.column_values_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("index out of range, invalid value for proxy_config",
                  K(row_index), K(sql_field.column_values_.count()), K_(sql_field.column_name), K(ret));
      } else if (0 == sql_field.column_name_.config_string_.case_compare("vip")) {
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else {
          vip = sql_column.column_value_;
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("vport")) {
        if (TOKEN_STR_VAL == sql_column.value_type_) {
          vport = atoi(sql_column.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_column.value_type_) {
          vport = sql_column.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid value type", K(sql_column.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("vid")) {
        if (TOKEN_STR_VAL == sql_column.value_type_) {
          vid = atoi(sql_column.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_column.value_type_) {
          vid = sql_column.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid value type", K(sql_column.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("config_level")) {
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else {
          item->config_level_.rewrite(sql_column.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("tenant_name")) {
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else {
          item->vip_info_.tenant_name_.rewrite(sql_column.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("cluster_name")) {
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else {
          item->vip_info_.cluster_name_.rewrite(sql_column.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("name")) {
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else {
          ObString &name = sql_column.column_value_.config_string_;
          item->config_item_.set_name(name.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("value")) {
        int32_t val_len = sql_column.column_value_.config_string_.length();
        if (TOKEN_STR_VAL != sql_column.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpeted type", K(sql_column.value_type_), K(ret));
        } else if (val_len >= OB_MAX_CONFIG_VALUE_LEN) {
          ret = OB_ERR_VARCHAR_TOO_LONG;
          LOG_WDIAG("proxy config length of value should less than 4096", K(val_len), K(OB_MAX_CONFIG_VALUE_LEN), K(ret));
        } else {
          item->config_item_.set_value(sql_column.column_value_.config_string_);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected column name", K(sql_field.column_name_.config_string_));
      }
    }
    if (OB_SUCC(ret)) {
      item->vip_info_.vip_addr_.set(vip.ptr(), static_cast<int32_t>(vport), vid);
      // 检查配置的值是否合法，仅在execute时，写backup阶段检查;
      if ((0 != strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())
           && !is_config_in_service(item->config_item_.name()))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WDIAG("multi level config not supported", KPC(item), K(ret));
      } else if (is_backup && !is_config_vaild(item->config_item_)) {
        ret = OB_INVALID_CONFIG;
        LOG_WDIAG("fail to check value, config value is invaild", KPC(item), K(ret));
      } else {
        int64_t index = is_backup ? (index_ + 1) % 2 : index_;
        ProxyConfigHashMap &config_map = proxy_config_map_array_[index];
        // Unique_set needs to be guaranteed to be the last step,
        // and the previous failure needs to release the item memory
        ObProxyConfigItem* tmp_item = config_map.remove(*item);
        if (NULL != tmp_item) {
          tmp_item->destroy();
          tmp_item = NULL;
        }

        if (0 == strcasecmp("compression_algorithm", item->config_item_.name())
            && NULL != item->config_item_.str()
            && '\0' != *item->config_item_.str()) {
          ObString val(item->config_item_.str());
          val = val.trim();
          // only support 'zlib:0~9' or empty
          ObString algo_str = val.split_on(':').trim();
          ObString level_str = val.trim();
          int64_t level = 0;
          if (0 != algo_str.case_compare("zlib")) {
            ret = OB_NOT_SUPPORTED;
          } else if (0 == level_str.case_compare("0")) {
            // valid value '0'
          } else if (0 == (level = atoi(level_str.ptr()))) {
            // fail to convert
            ret = OB_NOT_SUPPORTED;
          } else if (level < 0 || level > 9) {
            // out of range
            ret = OB_NOT_SUPPORTED;
          }
        }

        if (0 == strcasecmp("init_sql", item->config_item_.name())
            && NULL != item->config_item_.str()
            && '\0' != *item->config_item_.str()) {
          ObArenaAllocator allocator;
          ParseResult parse_result;
          ObSEArray<ObString, 4> sql_array;
          const int64_t EXTRA_NUM = 2;
          char buf[OB_MAX_CONFIG_VALUE_LEN + EXTRA_NUM];
          memset(buf, 0, sizeof(buf));
          MEMCPY(buf, item->config_item_.str(), strlen(item->config_item_.str()));
          if (OB_FAIL(ObProxySqlParser::split_multiple_stmt(buf, sql_array))) {
            LOG_WDIAG("fail to split multiple stmt", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < sql_array.count(); i++) {
              char tmp_buf[OB_MAX_CONFIG_VALUE_LEN + EXTRA_NUM];
              memset(tmp_buf, 0, sizeof(tmp_buf));
              MEMCPY(tmp_buf, sql_array.at(i).ptr(), sql_array.at(i).length());
              ObString parse_sql(sql_array.at(i).length() + EXTRA_NUM, tmp_buf);
              ObProxyParser obproxy_parser(allocator, NORMAL_PARSE_MODE);
              if (OB_FAIL(obproxy_parser.obparse(parse_sql, parse_result))) {
                LOG_WDIAG("fail to parse sql", K(buf), K(parse_sql), K(ret));
              } else if (OB_ISNULL(parse_result.result_tree_)
                  || OB_ISNULL(parse_result.result_tree_->children_)
                  || OB_ISNULL(parse_result.result_tree_->children_[0])
                  || OB_ISNULL(parse_result.result_tree_->children_[0]->children_)
                  || OB_ISNULL(parse_result.result_tree_->children_[0]->children_[0])
                  || (T_VARIABLE_SET != parse_result.result_tree_->children_[0]->type_
                      && T_ALTER_SYSTEM_SET_PARAMETER != parse_result.result_tree_->children_[0]->type_)) {
                ret = OB_NOT_SUPPORTED;
                LOG_WDIAG("init sql is not expected", K(ret));
              }
            }
          }
        }

        // need_sync_to_file_表示不是alter proxyconfig设置的命令，而是通过proxy_config设置的配置项
        if (OB_SUCC(ret) && need_sync_to_file_) {
          if ((0 == strcasecmp("obproxy_sys_password", item->config_item_.name())
              || 0 == strcasecmp("observer_sys_password", item->config_item_.name())
              || 0 == strcasecmp("observer_sys_password1", item->config_item_.name()))
              && (NULL != item->config_item_.str() && '\0' != *item->config_item_.str())) {
            char value_str[common::OB_MAX_CONFIG_VALUE_LEN + 1];
            char passwd_staged1_buf[ENC_STRING_BUF_LEN];
            ObString tmp_value_string;
            ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
            if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(item->config_item_.str(), passwd_string))) {
              LOG_WDIAG("encrypt_passwd_to_stage1 failed", K(ret));
            } else {
              MEMCPY(value_str, passwd_staged1_buf + 1, 40);
              value_str[40] = '\0';
              tmp_value_string.assign(value_str, 40);
              item->config_item_.set_value(tmp_value_string);
              char sql[1024];
              int64_t len = static_cast<int64_t>(snprintf(sql, 1024, EXECUTE_SQL, vip.length(), vip.ptr(), vid, vport,
                                                item->vip_info_.cluster_name_.size(), item->vip_info_.cluster_name_.ptr(),
                                                item->vip_info_.tenant_name_.size(), item->vip_info_.tenant_name_.ptr(),
                                                item->config_item_.name(), item->config_item_.str(),
                                                item->config_level_.size(), item->config_level_.ptr()));
              if (OB_UNLIKELY(len <= 0 || len >= 1024)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WDIAG("get execute sql failed", K(len), K(ret));
              } else {
                ObProxyVariantString buf_string;
                buf_string.set_value(sql);
                if (OB_FAIL(execute_sql_array_.push_back(buf_string))) {
                  LOG_WDIAG("execute_sql_array push back failed", K(ret));
                }
              }
            }
          }
        }

        SSLAttributes ssl_attributes;
        if (OB_SUCC(ret)
            && 0 == strcasecmp("ssl_attributes", item->config_item_.name())
            && NULL != item->config_item_.str()
            && '\0' != *item->config_item_.str()
            && OB_FAIL(ObProxyConfigTableProcessor::parse_ssl_attributes(item->config_item_, ssl_attributes))) {
          LOG_WDIAG("parse ssl attributes failed", KPC(item), K(ret));
        }
        // 检查配置设置时的主键信息和 level 是否匹配
        if (OB_SUCC(ret)) {
          if (0 == strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid || !item->vip_info_.cluster_name_.is_empty() || !item->vip_info_.tenant_name_.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          } else if (0 == strcasecmp("LEVEL_CLUSTER", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid || !item->vip_info_.tenant_name_.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          } else if (0 == strcasecmp("LEVEL_TENANT", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          }
        }

        // 检查配置设置时的主键信息和 level 是否匹配
        if (OB_SUCC(ret)) {
          if (0 == strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid || !item->vip_info_.cluster_name_.is_empty() || !item->vip_info_.tenant_name_.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          } else if (0 == strcasecmp("LEVEL_CLUSTER", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid || !item->vip_info_.tenant_name_.is_empty() || item->vip_info_.cluster_name_.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          } else if (0 == strcasecmp("LEVEL_TENANT", item->config_level_.ptr())) {
            if (!vip.empty() || 0 != vport || -1 != vid || item->vip_info_.cluster_name_.is_empty() || item->vip_info_.tenant_name_.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("set config info failed", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          }
        }

        // GLOBAL级别配置同步，统一到before_commit中执行
        if (OB_SUCC(ret)) {
          if (is_backup && need_sync_to_file_ && 0 == strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())) {
            ObProxyVariantString config_name;
            ObProxyVariantString config_value;
            if (OB_UNLIKELY(!config_name.set_value(item->config_item_.name()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("fail to set global config name", K(vip), K(vport), K(vid), KPC(item), K(ret));
            } else if (OB_UNLIKELY(!config_value.set_value(item->config_item_.str()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("fail to set global config value", K(vip), K(vport), K(vid), KPC(item), K(ret));
            } else if (OB_FAIL(global_config_array_.push_back(ObConfigKV(config_name, config_value)))) {
              LOG_WDIAG("fail to push global config value to array", K(vip), K(vport), K(vid), KPC(item), K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(config_map.unique_set(item))) {
            LOG_WDIAG("backup map unique_set failed", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != item) {
        item->destroy();
        item = NULL;
      }
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::delete_proxy_config(void *arg, const bool is_backup)
{
  // todo: Intercept and delete global configuration items
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (need_rebuild_config_map_ && OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WDIAG("backup hashmap failed", K(ret));
  } else {
    SqlFieldResult *fields = static_cast<SqlFieldResult*>(arg);
    int64_t index = is_backup ? (index_ + 1) % 2 : index_;
    ProxyConfigHashMap &config_map = proxy_config_map_array_[index];
    ProxyConfigHashMap::iterator last = config_map.end();
    for (ProxyConfigHashMap::iterator it = config_map.begin(); OB_SUCC(ret) && it != last;) {
      ObProxyConfigItem &item = *it;
      bool need_delete = true;
      ++it;
      for (int i = 0; OB_SUCC(ret) && need_delete && i < fields->field_num_; i++) {
        SqlField* sql_field = fields->fields_.at(i);
        if (0 == sql_field->column_name_.config_string_.case_compare("vip")) {
          if (ObVipAddr::VTOA_VIP_ADDR == item.vip_info_.vip_addr_.vip_addr_type_) {
            char ip_buf[256];
            item.vip_info_.vip_addr_.addr_.ip_to_string(ip_buf, 256);
            if (TOKEN_STR_VAL != sql_field->value_type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
            } else if (0 != strcasecmp(ip_buf, sql_field->column_value_.config_string_.ptr())) {
              need_delete = false;
            }
          } else if (ObVipAddr::VPC_VIP_ADDR == item.vip_info_.vip_addr_.vip_addr_type_) {
            ObString vpc;
            vpc.assign_ptr(item.vip_info_.vip_addr_.vpc_info_.ptr(), static_cast<int32_t>(item.vip_info_.vip_addr_.vpc_info_.len()));
            if (TOKEN_STR_VAL != sql_field->value_type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
            } else if (vpc != sql_field->column_value_.config_string_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("vport")) {
          int64_t vport = 0;
          if (TOKEN_STR_VAL == sql_field->value_type_) {
            vport = atoi(sql_field->column_value_.config_string_.ptr());
          } else if (TOKEN_INT_VAL == sql_field->value_type_) {
            vport = sql_field->column_int_value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vport != item.vip_info_.vip_addr_.addr_.port_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("vid")) {
          int64_t vid = 0;
          if (TOKEN_STR_VAL == sql_field->value_type_) {
            vid = atoi(sql_field->column_value_.config_string_.ptr());
          } else if (TOKEN_INT_VAL == sql_field->value_type_) {
            vid = sql_field->column_int_value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vid != item.vip_info_.vip_addr_.vid_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("config_level")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_level_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("tenant_name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.vip_info_.tenant_name_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("cluster_name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.vip_info_.cluster_name_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.name(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("value")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.str(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected column name", K(sql_field->column_name_));
        }
      }

      if (OB_SUCC(ret) && need_delete) {
        const char *config_level_str = item.config_level_.ptr();
        if (0 == strcasecmp("LEVEL_GLOBAL", config_level_str)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("delete global config unsupported", K(ret));
        } else {
          config_map.remove(&item);
          item.destroy();
        }
      }
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::get_config_item_without_lock(const obutils::ObVipAddr &addr,
                                                 const ObString &cluster_name,
                                                 const ObString &tenant_name,
                                                 const common::ObString &name,
                                                 ObProxyConfigItem &item)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem *proxy_config_item = NULL;
  ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];

  ObProxyConfigItem key;
  key.vip_info_.vip_addr_ = addr;
  key.config_item_.set_name(name.ptr());
  if (OB_FAIL(key.vip_info_.cluster_name_.rewrite(cluster_name))) {
    LOG_WDIAG("assign cluster_name failed", K(ret));
  } else if (OB_FAIL(key.vip_info_.tenant_name_.rewrite(tenant_name))) {
    LOG_WDIAG("assign tenant name failed", K(ret));
  } else if (OB_FAIL(current_map.get_refactored(key, proxy_config_item))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WDIAG("get config item failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
    }
  } else if (OB_ISNULL(proxy_config_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("proxy config item is null unexpected", K(ret));
  } else {
    item = *proxy_config_item;
  }
  return ret;
}

int ObProxyConfigTableProcessor::get_config_item(const obutils::ObVipAddr &addr,
                                                 const ObString &cluster_name,
                                                 const ObString &tenant_name,
                                                 const common::ObString &name,
                                                 ObProxyConfigItem &item,
                                                 const bool lock_required/*true*/)
{
  int ret = OB_SUCCESS;
  if (lock_required) {
    DRWLock::RDLockGuard guard(proxy_config_lock_);
    if (OB_FAIL(get_config_item_without_lock(addr, cluster_name, tenant_name, name, item))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WDIAG("fail to get config item with lock", K(ret));
      }
    }
  } else if (OB_FAIL(get_config_item_without_lock(addr, cluster_name, tenant_name, name, item))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WDIAG("fail to get config item without lock", K(ret));
    }
  }
  
  return ret;
}

int ObProxyConfigTableProcessor::alter_proxy_config()
{
  int ret = OB_SUCCESS;
  bool has_update_config = false;
  bool has_reload_config = false;
  ObProxyReloadConfig *reload_config = NULL;
  if (OB_ISNULL(reload_config = get_global_internal_cmd_processor().get_reload_config())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WDIAG("fail to get reload config", K(ret));
  }
  const int64_t global_config_len = global_config_array_.count();
  // copy old value
  common::ObSEArray<ObConfigVariableString, 4> global_old_value_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < global_config_len; ++i) {
    const ObString &key = global_config_array_.at(i).first.config_string_;
    ObConfigVariableString old_value;
    if (OB_FAIL(get_global_proxy_config().get_config_value(key, old_value))) {
      LOG_WDIAG("fail to get old config value", K(key), K(ret));
    } else if (OB_FAIL(global_old_value_array.push_back(old_value))) {
      LOG_WDIAG("fail to push back old config to array", K(key), K(old_value), K(ret));
    }
  }
  // update new value
  if (OB_SUCC(ret)) {
    has_update_config = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < global_config_len; ++i) {
      const ObString &key = global_config_array_.at(i).first.config_string_;
      const ObString &value = global_config_array_.at(i).second.config_string_;
      if (OB_FAIL(get_global_proxy_config().update_config_item(key, value))) {
        LOG_WDIAG("fail to sync update global config", K(key), K(value), K(ret));
      } else {
        // do nothing
      }
    }
  }

  // check
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_global_proxy_config().check_proxy_serviceable())) {
      LOG_WDIAG("fail to check proxy serviceable", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
      WDIAG_ICMD("fail to reload config", K(ret));
    } else {
      has_reload_config = true;
      DEBUG_ICMD("succ to update config", K(global_config_len));
    }
  }
  // rollback
  if (OB_FAIL(ret)) {
    int tmp_ret = ret;
    ret = OB_SUCCESS;
    if (has_update_config) {
      for (int64_t i = 0; OB_SUCC(ret) && i < global_config_len; ++i) {
        const ObString &key = global_config_array_.at(i).first.config_string_;
        const ObString old_value = global_old_value_array.at(i);
        if (OB_FAIL(get_global_proxy_config().update_config_item(key, old_value))) {
          LOG_WARN("rollback global config to original config failed", K(key), K(old_value), K(ret));
        } else {
          LOG_DEBUG("succ to back to old config", K(key), K(old_value));
        }
      }
    }
    if (has_reload_config && OB_SUCC(ret)) {
      if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
        WDIAG_ICMD("fail to reload old config", K(ret));
      } else {
        LOG_DEBUG("succ to reload old config");
      }
    }
    ret = tmp_ret;
  }

  return ret;
}

bool ObProxyConfigTableProcessor::is_config_vaild(const ObBaseConfigItem &config_item)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  // 无需对get_global_proxy_config加锁，即使读到脏数据，但只需check传入的value
  ObConfigItem *const *global_config_item = NULL;
  ObConfigItem *new_item = NULL;
  const char *name = config_item.name();
  const char *value = config_item.str();
  if (OB_ISNULL(global_config_item = get_global_proxy_config().get_container().get(ObConfigStringKey(name)))) {
    LOG_WDIAG("Invalid config string, no such config item", K(name), K(value));
  } else if (OB_ISNULL(new_item = (*global_config_item)->clone())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to clone global app config item to new app item", K(name), K(value), K(ret));
  } else if (!new_item->set_value(value)) {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("Invalid config value", K(name), K(value), K(ret));
  } else if (!new_item->check()) {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("Invalid config, value out of range", K(name), K(value), K(ret));
  } else {
    bret = true;
  }

  if (OB_NOT_NULL(new_item)) {
    new_item->free(); // item was allocate by op_alloc in clone
    new_item = NULL;
  }
  return bret;
}

bool ObProxyConfigTableProcessor::is_config_in_service(const ObString &config_name)
{
  bool is_in_service = false;
  // 无需对global_proxy_config加锁，CONFIG_LEVLE不会改变
  ObConfigItem* const* item = NULL;
  ObConfigStringKey key(config_name);

  if (OB_NOT_NULL(item = get_global_proxy_config().get_container().get(key))
      && ObString::make_string((*item)->config_level_to_str()) == common::OB_CONFIG_MULTI_LEVEL_VIP) {
    is_in_service = true;
  }

  return is_in_service;
}

int ObProxyConfigTableProcessor::commit_execute_sql(sqlite3 *db)
{
  int ret = OB_SUCCESS;
  // 同步全局配置
  if (global_config_array_.count() > 0 && OB_FAIL(alter_proxy_config())) {
    LOG_WDIAG("fail to sync global config", K(global_config_array_.count()), K(ret));
  } else if (execute_sql_array_.count() > 0) {
    // 执行sql
    if (OB_UNLIKELY(NULL == db)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("sqlite db is null unexpected", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < execute_sql_array_.count(); i++) {
      char *err_msg = NULL;
      ObString sql = execute_sql_array_.at(i).config_string_;
      int sqlite_err_code = SQLITE_OK;
      if (SQLITE_OK != (sqlite_err_code = sqlite3_exec(db, sql.ptr(), NULL, 0, &err_msg))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("sqlite exec failed", K(sqlite_err_code), K(sql), K(ret), "err_msg", err_msg);
      }

      if (NULL != err_msg) {
        sqlite3_free(err_msg);
      }
    }
  }

  return ret;
}

void ObProxyConfigTableProcessor::clear_execute_sql()
{
  execute_sql_array_.reset();
  global_config_array_.reset();
}

int ObProxyConfigTableProcessor::dump_config_item(obutils::ObShowConfigHandler &show_config_all,
                     const ObString &like_name)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(proxy_config_lock_);
  omt::ObProxyConfigTableProcessor::ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];
  omt::ObProxyConfigTableProcessor::ProxyConfigHashMap::iterator iter = current_map.begin();
  omt::ObProxyConfigTableProcessor::ProxyConfigHashMap::iterator iter_end = current_map.end();
  for (; OB_SUCC(ret) && iter != iter_end; ++iter) {
    if (common::match_like(iter->config_item_.name(), like_name)
        && OB_FAIL(show_config_all.dump_all_config_item(*iter))) {
      LOG_WDIAG("fail to dump ObProxyConfigItem item", K(*iter), K(like_name), K(ret));
    }
  }
  return ret;
}

int ObProxyConfigTableProcessor::parse_ssl_attributes(const ObBaseConfigItem &config_item, SSLAttributes &ssl_attributes)
{
  int ret = OB_SUCCESS;
  Parser parser;
  json::Value *json_value = NULL;
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WDIAG("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(config_item.str(), strlen(config_item.str()), json_value))) {
    LOG_WDIAG("json parse failed", K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, json::JT_OBJECT))) {
    LOG_WDIAG("check config info type failed", K(ret));
  } else {
    DLIST_FOREACH(p, json_value->get_object()) {
      if (0 == p->name_.case_compare("using_ssl")) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
          LOG_WDIAG("check config info type failed, using_ssl need string type", K(p->value_), K(ret));
        } else if (0 == p->value_->get_string().case_compare("ENABLE_FORCE")) {
          ssl_attributes.force_using_ssl_ = true;
        } else if (0 == p->value_->get_string().case_compare("DISABLE_FORCE")) {
          ssl_attributes.force_using_ssl_ = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("unsupport using_ssl config", K(p->value_->get_string()), K(ret));
        }
      } else if (0 == p->name_.case_compare("min_tls_version")) {
        long options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
          LOG_WDIAG("check config info type failed, min_tls_version need string type", K(p->value_), K(ret));
        } else if (0 == p->value_->get_string().case_compare("NONE")) {
        } else if (0 == p->value_->get_string().case_compare("TLSV1")) {
        } else if (0 == p->value_->get_string().case_compare("TLSV1.1")) {
          options |= SSL_OP_NO_TLSv1;
        } else if (0 == p->value_->get_string().case_compare("TLSV1.2")) {
          options |= (SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
        } else if (0 == p->value_->get_string().case_compare("TLSV1.3")) {
          options |= (SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2);
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("unsupport tls version", K(p->value_->get_string()), K(ret));
        }

        if (OB_SUCC(ret)) {
          ssl_attributes.options_ = options;
        }
      }
    }
  }

  return ret;
}

} // end of omt
} // end of obprxy
} // end of oceanbase

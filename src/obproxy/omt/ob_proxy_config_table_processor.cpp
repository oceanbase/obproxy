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

#include "omt/ob_proxy_config_table_processor.h"
#include "obutils/ob_config_processor.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_reload_config.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "cmd/ob_internal_cmd_processor.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "opsql/parser/ob_proxy_parser.h"

static const char *config_name_array[] = {"proxy_route_policy", "proxy_idc_name", "enable_cloud_full_username",
                                          "enable_client_ssl", "enable_server_ssl",
                                          "obproxy_read_consistency", "obproxy_read_only",
                                          "proxy_tenant_name", "rootservice_cluster_name",
                                          "enable_read_write_split", "enable_transaction_split",
                                          "target_db_server", "observer_sys_password",
                                          "observer_sys_password1", "obproxy_force_parallel_query_dop",
                                          "read_stale_retry_interval", "ob_max_read_stale_time",
                                          "ssl_attributes", "init_sql"};

static const char *EXECUTE_SQL = 
    "replace into proxy_config(vip, vid, vport, cluster_name, tenant_name, name, value, config_level) values("
    "'%.*s', %ld, %ld, '%.*s', '%.*s', '%s', '%s', '%.*s')";

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::opsql;

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
    LOG_WARN("op_alloc ObProxyConfigItem failed");
  } else {
    ret->vip_addr_ = vip_addr_;
    ret->tenant_name_ = tenant_name_;
    ret->cluster_name_ = cluster_name_;
    ret->config_level_ = config_level_;
    ret->config_item_ = config_item_;
  }

  return ret;
}

ObProxyConfigItem::ObProxyConfigItem(const ObProxyConfigItem& item)
{
  vip_addr_ = item.vip_addr_;
  cluster_name_ = item.cluster_name_;
  tenant_name_ = item.tenant_name_;
  config_level_ = item.config_level_;
  config_item_ = config_item_;
}

ObProxyConfigItem& ObProxyConfigItem::operator =(const ObProxyConfigItem &item)
{
  if (this != &item) {
    vip_addr_ = item.vip_addr_;
    cluster_name_ = item.cluster_name_;
    tenant_name_ = item.tenant_name_;
    config_level_ = item.config_level_;
    config_item_ = item.config_item_;
  }
  return *this;
}

int64_t ObProxyConfigItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(vip_addr), K_(tenant_name), K_(cluster_name), K_(config_level), K_(config_item));
  J_OBJ_END();
  return pos;
}

uint64_t ObProxyConfigItem::get_hash() const
{
  uint64_t hash = 0;
  void *data = (void*)(&vip_addr_);
  hash = murmurhash(data, sizeof(ObVipAddr), 0);
  hash = tenant_name_.hash(hash);
  hash = cluster_name_.hash(hash);
  hash = murmurhash(config_item_.name(), static_cast<int32_t>(strlen(config_item_.name())), hash);
  return hash;
}

int ObProxyConfigTableProcessor::execute(void *arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is null unexpected", K(ret));
  } else {
    ObCloudFnParams *params = reinterpret_cast<ObCloudFnParams*>(arg);
    if (NULL == params->fields_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null unexpected", K(ret));
    } else if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().set_proxy_config(params->fields_, true))) {
        LOG_WARN("set proxy config failed", K(ret));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().delete_proxy_config(params->fields_, true))) {
        LOG_WARN("delete proxy config failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(params->stmt_type_), K(ret));
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::commit(void *arg, bool is_success)
{
  int ret = OB_SUCCESS;
  if (is_success) {
    get_global_proxy_config_table_processor().inc_config_version();
    ObCloudFnParams *params = reinterpret_cast<ObCloudFnParams*>(arg);
    if (NULL == params->fields_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null unexpected", K(ret));
    } else if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().set_proxy_config(params->fields_, false))) {
        LOG_WARN("set proxy config failed", K(ret));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().delete_proxy_config(params->fields_, false))) {
        LOG_WARN("delete proxy config failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(params->stmt_type_), K(ret));
    }
    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      get_global_proxy_config_table_processor().inc_index();
      get_global_proxy_config_table_processor().set_need_rebuild_config_map(true);
    }
  } else {
    get_global_proxy_config_table_processor().set_need_rebuild_config_map(true);
    LOG_WARN("proxy config commit failed", K(is_success));
  }

  get_global_proxy_config_table_processor().clear_execute_sql();

  return OB_SUCCESS;
}

int ObProxyConfigTableProcessor::before_commit(void *arg)
{
  sqlite3 *db = (sqlite3*)arg;
  return get_global_proxy_config_table_processor().commit_execute_sql(db);
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
    LOG_WARN("register proxy config table callback failed", K(ret));
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
      LOG_WARN("clone ObProxyConfigItem failed", K(ret));
    } else if (OB_FAIL(backup_map.unique_set(item))) {
      LOG_WARN("backup map set refactored failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    clean_hashmap_with_lock(backup_map);
  }

  return ret;
}

int ObProxyConfigTableProcessor::set_proxy_config(void *arg, const bool is_backup)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem *item = NULL;
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (need_rebuild_config_map_ && OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WARN("backup hashmap failed", K(ret));
  } else if (OB_ISNULL(item = op_alloc(ObProxyConfigItem))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op alloc ObProxyConfigItem failed", K(ret));
  } else {
    ObString vip;
    int64_t vport = 0;
    int64_t vid = -1;
    ObString name;
    SqlFieldResult *sql_fields = static_cast<SqlFieldResult*>(arg);
    for (int i = 0; OB_SUCC(ret) && i < sql_fields->field_num_; i++) {
      SqlField &sql_field = *(sql_fields->fields_.at(i));
      if (0 == sql_field.column_name_.config_string_.case_compare("vip")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          vip = sql_field.column_value_.config_string_;
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("vport")) {
        if (TOKEN_STR_VAL == sql_field.value_type_) {
          vport = atoi(sql_field.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_field.value_type_) {
          vport = sql_field.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid value type", K(sql_field.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("vid")) {
        if (TOKEN_STR_VAL == sql_field.value_type_) {
          vid = atoi(sql_field.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_field.value_type_) {
          vid = sql_field.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid value type", K(sql_field.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("config_level")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->config_level_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("tenant_name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->tenant_name_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("cluster_name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->cluster_name_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          ObString &name = sql_field.column_value_.config_string_;
          item->config_item_.set_name(name.ptr());
        }
      } else if (0 == sql_field.column_name_.config_string_.case_compare("value")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->config_item_.set_value(sql_field.column_value_.config_string_);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column name", K(sql_field.column_name_.config_string_));
      }
    }

    if (OB_SUCC(ret)) {
      item->vip_addr_.set(vip.ptr(), static_cast<int32_t>(vport), vid);
      if (0 != strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())
          && !is_config_in_service(item->config_item_.name())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("multi level config not supported", KPC(item), K(ret));
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
            LOG_WARN("fail to split multiple stmt", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < sql_array.count(); i++) {
              char tmp_buf[OB_MAX_CONFIG_VALUE_LEN + EXTRA_NUM];
              memset(tmp_buf, 0, sizeof(tmp_buf));
              MEMCPY(tmp_buf, sql_array.at(i).ptr(), sql_array.at(i).length());
              ObString parse_sql(sql_array.at(i).length() + EXTRA_NUM, tmp_buf);
              ObProxyParser obproxy_parser(allocator, NORMAL_PARSE_MODE);
              if (OB_FAIL(obproxy_parser.obparse(parse_sql, parse_result))) {
                LOG_WARN("fail to parse sql", K(buf), K(parse_sql), K(ret));
              } else if (OB_ISNULL(parse_result.result_tree_)
                  || OB_ISNULL(parse_result.result_tree_->children_)
                  || OB_ISNULL(parse_result.result_tree_->children_[0])
                  || OB_ISNULL(parse_result.result_tree_->children_[0]->children_)
                  || OB_ISNULL(parse_result.result_tree_->children_[0]->children_[0])
                  || (T_VARIABLE_SET != parse_result.result_tree_->children_[0]->type_
                      && T_ALTER_SYSTEM_SET_PARAMETER != parse_result.result_tree_->children_[0]->type_)) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("init sql is not expected", K(ret));
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
              LOG_WARN("encrypt_passwd_to_stage1 failed", K(ret));
            } else {
              MEMCPY(value_str, passwd_staged1_buf + 1, 40);
              value_str[40] = '\0';
              tmp_value_string.assign(value_str, 40);
              item->config_item_.set_value(tmp_value_string);
              char sql[1024];
              int64_t len = static_cast<int64_t>(snprintf(sql, 1024, EXECUTE_SQL, vip.length(), vip.ptr(), vid, vport,
                                                item->cluster_name_.size(), item->cluster_name_.ptr(), 
                                                item->tenant_name_.size(), item->tenant_name_.ptr(),
                                                item->config_item_.name(), item->config_item_.str(),
                                                item->config_level_.size(), item->config_level_.ptr()));
              if (OB_UNLIKELY(len <= 0 || len >= 1024)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get execute sql failed", K(len), K(ret));
              } else {
                ObProxyVariantString buf_string;
                buf_string.set_value(sql);
                if (OB_FAIL(execute_sql_array_.push_back(buf_string))) {
                  LOG_WARN("execute_sql_array push back failed", K(ret));
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
          LOG_WARN("parse ssl attributes failed", KPC(item), K(ret));
        }

        if (OB_SUCC(ret)) {
          if (!is_backup && need_sync_to_file_ && 0 == strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr()) &&
              OB_FAIL(alter_proxy_config(item->config_item_.name(), item->config_item_.str()))) {
            LOG_WARN("alter proxyconfig failed", K(ret));
          } else if (OB_FAIL(config_map.unique_set(item))) {
            LOG_WARN("backup map unique_set failed", K(ret));
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
    LOG_WARN("invalid argument", K(ret));
  } else if (need_rebuild_config_map_ && OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WARN("backup hashmap failed", K(ret));
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
          char ip_buf[256];
          item.vip_addr_.addr_.ip_to_string(ip_buf, 256);
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(ip_buf, sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("vport")) {
          int64_t vport = 0;
          if (TOKEN_STR_VAL == sql_field->value_type_) {
            vport = atoi(sql_field->column_value_.config_string_.ptr());
          } else if (TOKEN_INT_VAL == sql_field->value_type_) {
            vport = sql_field->column_int_value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vport != item.vip_addr_.addr_.port_) {
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
            LOG_WARN("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vid != item.vip_addr_.vid_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("config_level")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_level_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("tenant_name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.tenant_name_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("cluster_name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.cluster_name_.ptr(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("name")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.name(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("value")) {
          if (TOKEN_STR_VAL != sql_field->value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field->value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.str(), sql_field->column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column name", K(sql_field->column_name_));
        }
      }

      if (OB_SUCC(ret) && need_delete) {
        const char *config_level_str = item.config_level_.ptr();
        if (0 == strcasecmp("LEVEL_GLOBAL", config_level_str)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("delete global config unsupported", K(ret));
        } else {
          config_map.remove(&item);
          item.destroy();
        }
      }
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::get_config_item(const obutils::ObVipAddr &addr,
                                                 const ObString &cluster_name,
                                                 const ObString &tenant_name,
                                                 const common::ObString &name,
                                                 ObProxyConfigItem &item)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(proxy_config_lock_);
  ObProxyConfigItem *proxy_config_item = NULL;
  ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];

  ObProxyConfigItem key;
  key.vip_addr_ = addr;
  key.config_item_.set_name(name.ptr());
  if (OB_FAIL(key.cluster_name_.assign(cluster_name))) {
    LOG_WARN("assign cluster_name failed", K(ret));
  } else if (OB_FAIL(key.tenant_name_.assign(tenant_name))) {
    LOG_WARN("assign tenant name failed", K(ret));
  } else if (OB_FAIL(current_map.get_refactored(key, proxy_config_item))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get config item failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
    }
  } else if (OB_ISNULL(proxy_config_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy config item is null unexpected", K(ret));
  } else {
    item = *proxy_config_item;
  }

  return ret;
}

int ObProxyConfigTableProcessor::alter_proxy_config(const common::ObString &key_string,
                                                    const common::ObString &value_string)
{
  int ret = OB_SUCCESS;
  char *old_value = NULL;
  bool has_update_config = false;
  bool has_dump_config = false;
  ObProxyReloadConfig *reload_config = NULL;
  ObString tmp_value_string = value_string;
  if (OB_UNLIKELY(key_string.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_string), K(value_string), K(ret));
  } else {
    if (OB_ISNULL(reload_config = get_global_internal_cmd_processor().get_reload_config())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get reload config", K(ret));
    } else if (OB_ISNULL(old_value = static_cast<char *>(op_fixed_mem_alloc(OB_MAX_CONFIG_VALUE_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for old_value", "size", OB_MAX_CONFIG_VALUE_LEN, K(ret));
    } else if (OB_FAIL(get_global_proxy_config().get_old_config_value(key_string, old_value, OB_MAX_CONFIG_VALUE_LEN))) {
      LOG_WARN("fail to get old config value", K(key_string), K(ret));
    } else if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, tmp_value_string))) {
      LOG_WARN("fail to update config", K(key_string), K(tmp_value_string), K(ret));
    } else {
      has_update_config = true;
      LOG_DEBUG("succ to update config", K(key_string), K(value_string), K(old_value));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_global_proxy_config().check_proxy_serviceable())) {
        LOG_WARN("fail to check proxy serviceable", K(ret));
      } else if (OB_FAIL(get_global_proxy_config().dump_config_to_local())) {
        LOG_WARN("fail to dump_config_to_local", K(ret));
      } else {
        has_dump_config = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
        WARN_ICMD("fail to reload config, but config has already dumped!!", K(ret));
      } else {
        DEBUG_ICMD("succ to update config", K(key_string), K(value_string));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (has_update_config) {
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().update_config_item(
            key_string, ObString::make_string(old_value))))) {
          LOG_WARN("fail to back to old config", K(key_string), K(old_value), K(tmp_ret));
        } else {
          LOG_DEBUG("succ to back to old config", K(key_string), K(old_value));
        }
      }
      if (has_dump_config && OB_LIKELY(OB_SUCCESS == tmp_ret)) {
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().dump_config_to_local()))) {
          LOG_WARN("fail to dump old config", K(tmp_ret));
        } else {
          LOG_DEBUG("succ to dump old config");
        }
      }
    }
  }

  return ret;
}

bool ObProxyConfigTableProcessor::is_config_in_service(const ObString &config_name)
{
  bool is_in_service = false;
  for(int64_t i = 0; i < (sizeof(config_name_array) / sizeof(const char*)); i++) {
    if (config_name == config_name_array[i]) {
      is_in_service = true;
      break;
    }
  }

  return is_in_service;
}

int ObProxyConfigTableProcessor::commit_execute_sql(sqlite3 *db)
{
  int ret = OB_SUCCESS;
  if (execute_sql_array_.count() > 0) {
    if (OB_UNLIKELY(NULL == db)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sqlite db is null unexpected", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < execute_sql_array_.count(); i++) {
      char *err_msg = NULL;
      ObString sql = execute_sql_array_.at(i).config_string_;
      if (SQLITE_OK != sqlite3_exec(db, sql.ptr(), NULL, 0, &err_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec failed", K(ret), "err_msg", err_msg);
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
}

int ObProxyConfigTableProcessor::parse_ssl_attributes(const ObConfigItem &config_item, SSLAttributes &ssl_attributes)
{
  int ret = OB_SUCCESS;
  Parser parser;
  json::Value *json_value = NULL;
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(config_item.str(), strlen(config_item.str()), json_value))) {
    LOG_WARN("json parse failed", K(ret));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, json::JT_OBJECT))) {
    LOG_WARN("check config info type failed", K(ret));
  } else {
    DLIST_FOREACH(p, json_value->get_object()) {
      if (0 == p->name_.case_compare("using_ssl")) {
        if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
          LOG_WARN("check config info type failed, using_ssl need string type", K(p->value_), K(ret));
        } else if (0 == p->value_->get_string().case_compare("ENABLE_FORCE")) {
          ssl_attributes.force_using_ssl_ = true;
        } else if (0 == p->value_->get_string().case_compare("DISABLE_FORCE")) {
          ssl_attributes.force_using_ssl_ = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupport using_ssl config", K(p->value_->get_string()), K(ret));
        }
      }
    }
  }

  return ret;
}

} // end of omt
} // end of obprxy
} // end of oceanbase

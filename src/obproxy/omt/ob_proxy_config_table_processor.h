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

#ifndef OB_PROXY_CONFIG_TABLE_PROCESSOR_H_
#define OB_PROXY_CONFIG_TABLE_PROCESSOR_H_

#include <sqlite/sqlite3.h>

#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_vip_tenant_cache.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/config/ob_config.h"
#include "obutils/ob_proxy_string_utils.h"
#include <utility>

namespace oceanbase
{
namespace obproxy
{
class ObTargetDbServer;
namespace obutils
{
class ObShowConfigHandler;
}
namespace omt
{

#define MAX_CONFIG_LEVEL_LENGTH 20

struct ObVipInfo
{
public:
  ObVipInfo() : vip_addr_(), tenant_name_(), cluster_name_() {}
  bool operator==(const ObVipInfo &other) const;
  uint64_t get_hash() const;
  obutils::ObVipAddr vip_addr_;
  ObConfigVariableString tenant_name_;
  ObConfigVariableString cluster_name_;
};

struct SSLAttributes
{
  SSLAttributes(): force_using_ssl_(false), options_(SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3) {}
  ~SSLAttributes() {
    force_using_ssl_ = false;
    options_ = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
  }
  bool force_using_ssl_;
  uint64_t options_;
};

class ObProxyConfigItem
{
public:
  ObProxyConfigItem() : vip_info_(), config_level_(), config_item_() {}
  ~ObProxyConfigItem() {}
  ObProxyConfigItem(const ObProxyConfigItem &item);
  ObProxyConfigItem& operator =(const ObProxyConfigItem &item);
  void destroy()
  {
    op_free(this);
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObProxyConfigItem* clone();
  uint64_t get_hash() const;
  ObVipInfo vip_info_;
  ObConfigVariableString config_level_;
  ObVariableLenConfigItem config_item_;

  LINK(ObProxyConfigItem, proxy_config_item_link_);
};

class ObProxyMultiLevelConfig: public ObSharedRefCount
{
public:
  ObProxyMultiLevelConfig(): proxy_route_policy_(), proxy_idc_name_(), proxy_primary_zone_name_(),
                             mysql_version_(), binlog_service_ip_(), init_sql_(),
                             target_db_server_(), compression_algorithm_(),
                             enable_cloud_full_username_(false),
                             enable_client_ssl_(false),  enable_server_ssl_(false),
                             enable_read_write_split_(false), enable_transaction_split_(false),
                             enable_weak_reroute_(false), enable_single_leader_node_routing_(false),
                             read_stale_retry_interval_(0),
                             ssl_attributes_(), observer_query_timeout_delta_(0),
                             query_digest_time_threshold_(0), route_diagnosis_level_(0),
                             slow_query_time_threshold_(0),
                             config_version_(0), vip_info_()
  {
    ObSharedRefCount::inc_ref();
  }
  ~ObProxyMultiLevelConfig() {}
  uint64_t get_hash() const;
  int set_config(const uint64_t global_version);
  virtual void free() override;   // 所有堆上成员，都要在free中释放
  // 注意，配置项的成员多级别获取，会使用宏拼接变量名的方式
  // mysqlSm中的多级配置，vip级别
  ObConfigVariableString proxy_route_policy_;
  ObConfigVariableString proxy_idc_name_;
  ObConfigVariableString proxy_primary_zone_name_;
  ObConfigVariableString mysql_version_;
  ObConfigVariableString binlog_service_ip_;
  ObConfigVariableString init_sql_;
  ObConfigVariableString target_db_server_;
  ObConfigVariableString compression_algorithm_;
  bool enable_cloud_full_username_;
  bool enable_client_ssl_;
  bool enable_server_ssl_;
  bool enable_read_write_split_;
  bool enable_transaction_split_;
  bool enable_weak_reroute_;
  bool enable_single_leader_node_routing_;
  int64_t read_stale_retry_interval_;
  int64_t obproxy_read_only_;
  int64_t obproxy_read_consistency_;
  int64_t ob_max_read_stale_time_;
  int64_t obproxy_force_parallel_query_dop_;
  SSLAttributes ssl_attributes_;
  // 移植ObMysqlConfigParams中的配置项
  int64_t observer_query_timeout_delta_;
  int64_t query_digest_time_threshold_;
  int64_t route_diagnosis_level_;
  int64_t slow_query_time_threshold_;

  // 非配置项信息
  uint64_t config_version_;		// 记录全局ObProxyConfigTableProcessor中的版本，用以更新判断
  ObVipInfo vip_info_;
  LINK(ObProxyMultiLevelConfig, proxy_mutil_level_config_link_);
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyMultiLevelConfig);
};

class ObProxyConfigTableProcessor
{
public:
  struct ObProxyConfigItemHashing
  {
    typedef const ObProxyConfigItem& Key;
    typedef ObProxyConfigItem Value;
    typedef ObDLList(ObProxyConfigItem, proxy_config_item_link_) ListHead;

    static uint64_t hash(Key key)
    {
      return key.get_hash();
    }

    static Key key(Value *value) { return *value; }

    static bool equal(Key lhs, Key rhs)
    {
      return lhs.vip_info_ == rhs.vip_info_
             && 0 == strcasecmp(lhs.config_item_.name(), rhs.config_item_.name());
    }
  };
  struct ObProxyMultiLevelConfigHashing
  {
    typedef const ObVipInfo& Key;
    typedef ObProxyMultiLevelConfig Value;
    typedef ObDLList(ObProxyMultiLevelConfig, proxy_mutil_level_config_link_) ListHead;

    static uint64_t hash(Key key)
    {
      return key.get_hash();
    }

    static Key key(Value *value) { return value->vip_info_; }
    static bool equal(Key lhs, Key rhs)
    {
      return lhs == rhs;
    }
  };
  typedef common::hash::ObBuildInHashMap<ObProxyConfigItemHashing, 1024> ProxyConfigHashMap;
  typedef common::hash::ObBuildInHashMap<ObProxyMultiLevelConfigHashing, 32> ProxyMultiLevelConfigHashMap;
public:
  ObProxyConfigTableProcessor() : index_(0), proxy_config_lock_(obsys::WRITE_PRIORITY),
                                  config_version_(0), need_sync_to_file_(false),
                                  execute_sql_array_(), need_rebuild_config_map_(false) {}
  ~ObProxyConfigTableProcessor() {}

  int init();
  void inc_index();
  // 写备份或主内存失败，会设置为true；backup_hashmap_with_lock成功后，会重新设为false
  void set_need_rebuild_config_map(const bool need_rebuld) { need_rebuild_config_map_ = need_rebuld; }
  void clean_hashmap(ProxyConfigHashMap &map);
  int set_proxy_config(void *arg, const bool is_backup, int64_t row_index);
  int delete_proxy_config(void *arg, const bool is_backup);
  int get_config_item_without_lock(const obutils::ObVipAddr &addr, const ObString &cluster_name,
                                   const ObString &tenant_name, const common::ObString &name,
                                   ObProxyConfigItem &item);
  int get_config_item(const obutils::ObVipAddr &addr, const common::ObString &cluster_name,
                      const common::ObString &teannt_name, const common::ObString &name,
                      ObProxyConfigItem &item, const bool lock_required = true);
  const uint64_t get_config_version() const { return config_version_; }
  void inc_config_version() { config_version_++; }
  void set_need_sync_to_file(const bool bvalue) { need_sync_to_file_ = bvalue; }
  ProxyConfigHashMap& get_backup_hashmap() { return proxy_config_map_array_[(index_ + 1) % 2]; }
  int commit_execute_sql(sqlite3 *db);
  void clear_execute_sql();
  int dump_config_item(obutils::ObShowConfigHandler &show_config_all,
                       const ObString &like_name);

  static int parse_ssl_attributes(const ObBaseConfigItem &config_item, SSLAttributes &ssl_attributes);
  static bool is_config_vaild(const ObBaseConfigItem &config_item);
  int backup_hashmap_with_lock();
  int get_proxy_multi_config(const obutils::ObVipAddr &vip_addr, const common::ObString &cluster_name,
                            const common::ObString &tenant_name, const uint64_t global_version,
                            ObProxyMultiLevelConfig* &old_config);
private:
  void clean_hashmap_with_lock(ProxyConfigHashMap &map);
  int alter_proxy_config();
  bool is_config_in_service(const common::ObString &config_name);

  int update_proxy_multi_config(const ObVipInfo &key,
                                ObProxyMultiLevelConfig* &cur_config);
  int excute_sync_master(void *arg);
  static int execute(void *arg);
  static int commit(void* arg, bool is_success);
  static int before_commit(void * proxy_config_db, void *arg, bool &is_success, int64_t row_num);

private:
  ProxyConfigHashMap proxy_config_map_array_[2];
  ProxyMultiLevelConfigHashMap proxy_multi_level_config_map_;
  int64_t index_;
  common::DRWLock proxy_config_lock_;
  uint64_t config_version_;
  volatile bool need_sync_to_file_;
  common::ObSEArray<obutils::ObProxyVariantString, 4> execute_sql_array_;
  typedef ::std::pair<obutils::ObProxyVariantString, obutils::ObProxyVariantString> ObConfigKV;
  common::ObSEArray<ObConfigKV, 4> global_config_array_;
  bool need_rebuild_config_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigTableProcessor);
};

extern ObProxyConfigTableProcessor &get_global_proxy_config_table_processor();

} // end of omt
} // end of obproxy
} // end of oceanbase

#endif

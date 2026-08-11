/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include <utility>

#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_vip_tenant_cache.h"
#include "lib/string/ob_fixed_length_string.h"
#include "share/config/ob_config.h"
#include "obutils/ob_proxy_string_utils.h"
#include "obutils/ob_proxy_config_processor.h"

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
  ObVipInfo() : vip_addr_(), tenant_name_(), cluster_name_(), service_name_() {}
  bool operator==(const ObVipInfo &other) const;
  uint64_t get_hash() const;
  obutils::ObVipAddr vip_addr_;
  ObConfigVariableString tenant_name_;
  ObConfigVariableString cluster_name_;
  ObConfigVariableString service_name_;
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
  ObProxyConfigItem() : vip_info_(), config_level_(), config_item_(), version_(0) {}
  ~ObProxyConfigItem() {}
  ObProxyConfigItem(const ObProxyConfigItem &item);
  ObProxyConfigItem& operator =(const ObProxyConfigItem &item);
  void destroy()
  {
    op_free(this);
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int parse_json_info(ObString &info, bool &is_exist_version);

  ObProxyConfigItem* clone();
  uint64_t get_hash() const;
  ObVipInfo vip_info_;
  ObConfigVariableString config_level_;
  ObVariableLenConfigItem config_item_;
  int64_t version_;

  LINK(ObProxyConfigItem, proxy_config_item_link_);
};

// 此类用于delete配置时，存储SqlFiled的数据，然后和每一个配置做比较，好处是：
//  1. 引入bool成员变量，可以在O(1)的时间判断是否要删除的配置，无需每次都比较配置的字符串
//  2. 父类仅设置删除的值，但是没法判断这个值是否需要删除（如删除的值恰好是默认值）
class ObProxyDeleteConfigItem: public ObProxyConfigItem
{
public:
  ObProxyDeleteConfigItem(): has_vip_addr_(0), has_vid_(0), has_vport_(0),
        has_config_level_(0), has_valule_(0), has_version_(0),
        has_tenant_name_(0), has_cluster_name_(0), has_name_(0)  {}
  ~ObProxyDeleteConfigItem() {}
  bool compare_config(const ObProxyConfigItem& item) const;
  int set_for_sql_field(const obutils::SqlFieldResult &fields);
public:
  static const int64_t NEED_DELETE_BITS_SHIFT = 0;
  static const int64_t IS_NOT_EQUAL_BITS_SHIFT = 1;
  static const int64_t WITH_NEED_DELETE = 1LL << NEED_DELETE_BITS_SHIFT;
  static const int64_t WITH_IS_NOT_EQUAL = 1LL << IS_NOT_EQUAL_BITS_SHIFT;

  bool is_not_equal(const uint16_t val) const { return val & WITH_IS_NOT_EQUAL; }
  bool is_need_delete(const uint16_t val) const { return val & WITH_NEED_DELETE; }

public:
  uint16_t has_vip_addr_:           2;
  uint16_t has_vid_:                2;
  uint16_t has_vport_:              2;
  uint16_t has_config_level_:       2;
  uint16_t has_valule_:             2;
  uint16_t has_version_:            2;
  uint16_t has_tenant_name_:        2;
  uint16_t has_cluster_name_:       2;
  uint16_t has_name_:               2;
  uint16_t:                         0;
};

class ObZoneWeakReadWeight
{
public:
  ObZoneWeakReadWeight(): zone_array_(), weight_array_() {}
  ObZoneWeakReadWeight(const ObZoneWeakReadWeight& other): zone_array_(other.zone_array_), weight_array_(other.weight_array_) {}
  ObZoneWeakReadWeight& operator=(const ObZoneWeakReadWeight& other);
  ~ObZoneWeakReadWeight() {}
  static int parse_weight_zone(const ObConfigItem& item, ObZoneWeakReadWeight &weight_zone);
  bool is_valid() const { return !zone_array_.empty();}
public:
  common::ObSEArray<ObConfigVariableString, 8> zone_array_;
  common::ObSEArray<int64_t, 8> weight_array_;
};

class ObTargetReplicaType
{
public:
  ObTargetReplicaType(): replica_type_(0) {}
  static int find_replica_index(const ObString &replica_str);
  void parse_target_replica_type(const ObConfigItem& item);
  /*
    |---- 1 bits ---|--- 1 bits ---|--- 1 bits ---|
    |- ColumnStore--|-- ReadOnly --|---- Full ----|
  */
  enum ObReplicaType
  {
    Full = 0,
    ReadOnly,
    ColumnStore,
  };
  static const int64_t FULL_BITS_SHIFT = 0;
  static const int64_t READONLY_BITS_SHIFT = 1;
  static const int64_t COLUMN_STORE_BITS_SHIFT = 2;
  static const int64_t WITH_FULL = 1LL << FULL_BITS_SHIFT;
  static const int64_t WITH_READONLY = 1LL << READONLY_BITS_SHIFT;
  static const int64_t WITH_COLUMN_STORE = 1LL << COLUMN_STORE_BITS_SHIFT;

  bool is_exist_full_replica() const { return replica_type_ & WITH_FULL; };
  bool is_exist_readonly_replica() const { return replica_type_ & WITH_READONLY; };
  bool is_exist_column_store_replica() const { return replica_type_ & WITH_COLUMN_STORE; };
  bool is_column_store_replica_only() const { return replica_type_ == WITH_COLUMN_STORE; };
  void set_full_replica() { replica_type_ = replica_type_ | WITH_FULL; };
  void set_readonly_replica() { replica_type_ = replica_type_ | WITH_READONLY; };
  void set_column_store_replica() { replica_type_ = replica_type_ | WITH_COLUMN_STORE; };
  void set_all_weakread_replica();
  TO_STRING_KV(K_(replica_type));
public:
  int64_t replica_type_;
};

class ObProxyMultiLevelConfig: public ObSharedRefCount
{
public:
  ObProxyMultiLevelConfig(): proxy_route_policy_(), proxy_idc_name_(), proxy_primary_zone_name_(),
                             mysql_version_(), binlog_service_ip_(), init_sql_(),
                             target_db_server_(), compression_algorithm_(),
                             rootservice_cluster_name_(), proxy_tenant_name_(),
                             enable_cloud_full_username_(false),
                             enable_client_ssl_(false), enable_server_ssl_(false),
                             enable_read_write_split_(false), enable_transaction_split_(false),
                             enable_weak_reroute_(false), enable_single_leader_node_routing_(false),
                             enable_standby_read_write_split_(false), enable_check_cluster_name_(false),
                             read_stale_retry_interval_(0),
                             ssl_attributes_(), weakread_weight_zone_(), limit_config_(), route_target_replica_type_(),
                             observer_query_timeout_delta_(0), query_digest_time_threshold_(0),
                             route_diagnosis_level_(0), slow_query_time_threshold_(0),
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
  ObConfigVariableString rootservice_cluster_name_;
  ObConfigVariableString proxy_tenant_name_;
  bool enable_cloud_full_username_;
  bool enable_client_ssl_;
  bool enable_server_ssl_;
  bool enable_read_write_split_;
  bool enable_transaction_split_;
  bool enable_weak_reroute_;
  bool enable_single_leader_node_routing_;
  bool enable_standby_read_write_split_;
  bool enable_check_cluster_name_;
  int64_t read_stale_retry_interval_;
  int64_t obproxy_read_only_;
  int64_t obproxy_read_consistency_;
  int64_t ob_max_read_stale_time_;
  int64_t obproxy_force_parallel_query_dop_;
  SSLAttributes ssl_attributes_;
  ObZoneWeakReadWeight weakread_weight_zone_;
  obutils::ObProxyLimitControlConfig limit_config_;

  ObTargetReplicaType route_target_replica_type_;
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
  static int is_replica_type_config_valid(const ObProxyConfigItem &item);
  static int is_weigth_zone_config_valid(const ObProxyConfigItem &item);
  bool can_write_to_sqlite(const bool is_backup);
  int parse_item_for_sql_fileds(const obutils::SqlFieldResult &sql_fields,
                                const int64_t row_index, ObString &vip,
                                int64_t &vport, int64_t &vid,
                                ObProxyConfigItem &item);
  // 多级别配置校验统一放到下面这个函数
  int check_multi_level_config_valid(ObProxyConfigItem &item, const ObString &vip,
                                     const int64_t vport, const int64_t vid,
                                     const bool is_backup);
  int rewrite_service_name_config(const bool is_backup,
                                           ObProxyConfigItem &item,
                                           const ObString &vip,
                                           const int64_t vport,
                                           const int64_t vid);

  int backup_hashmap_with_lock();
  int get_proxy_multi_config(const obutils::ObVipAddr &vip_addr, const common::ObString &cluster_name,
                            const common::ObString &tenant_name, const uint64_t global_version,
                            ObProxyMultiLevelConfig* &old_config,
                            const common::ObString &service_name);
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

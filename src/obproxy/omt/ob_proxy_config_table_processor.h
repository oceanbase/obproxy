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

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

#define MAX_CONFIG_LEVEL_LENGTH 20

class ObProxyConfigItem
{
public:
  ObProxyConfigItem() : vip_addr_(), tenant_name_(), cluster_name_(), config_level_(), config_item_() {}
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
  obutils::ObVipAddr vip_addr_;
  common::ObFixedLengthString<oceanbase::common::OB_MAX_TENANT_NAME_LENGTH> tenant_name_;
  common::ObFixedLengthString<OB_PROXY_MAX_CLUSTER_NAME_LENGTH> cluster_name_;
  common::ObFixedLengthString<MAX_CONFIG_LEVEL_LENGTH> config_level_;
  common::ObConfigItem config_item_;

  LINK(ObProxyConfigItem, proxy_config_item_link_);
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
      return lhs.vip_addr_ == rhs.vip_addr_
             && lhs.tenant_name_ == rhs.tenant_name_
             && lhs.cluster_name_ == rhs.cluster_name_
             && 0 == strcasecmp(lhs.config_item_.name(), rhs.config_item_.name());
    }
  };
  typedef common::hash::ObBuildInHashMap<ObProxyConfigItemHashing, 1024> ProxyConfigHashMap;

public:
  struct SSLAttributes {
    SSLAttributes() : force_using_ssl_(false) {}
    ~SSLAttributes() {
      force_using_ssl_ = false;
    }
    bool force_using_ssl_;
  };

public:
  ObProxyConfigTableProcessor() : index_(0), proxy_config_lock_(obsys::WRITE_PRIORITY),
                                  config_version_(0), need_sync_to_file_(false),
                                  execute_sql_array_(), need_rebuild_config_map_(false) {}
  ~ObProxyConfigTableProcessor() {}

  int init();
  void inc_index();
  void set_need_rebuild_config_map(const bool need_rebuld) { need_rebuild_config_map_ = need_rebuld; }
  void clean_hashmap(ProxyConfigHashMap &map);
  int set_proxy_config(void *arg, const bool is_backup);
  int delete_proxy_config(void *arg, const bool is_backup);
  int get_config_item(const obutils::ObVipAddr &addr, const common::ObString &cluster_name,
                      const common::ObString &teannt_name, const common::ObString &name,
                      ObProxyConfigItem &item);
  const uint64_t get_config_version() const { return config_version_; }
  void inc_config_version() { config_version_++; }
  void set_need_sync_to_file(const bool bvalue) { need_sync_to_file_ = bvalue; }
  ProxyConfigHashMap& get_backup_hashmap() { return proxy_config_map_array_[(index_ + 1) % 2]; }
  int commit_execute_sql(sqlite3 *db);
  void clear_execute_sql();

  static int parse_ssl_attributes(const ObConfigItem &config_item, SSLAttributes &ssl_attributes);
  int backup_hashmap_with_lock();
private:
  void clean_hashmap_with_lock(ProxyConfigHashMap &map);
  int alter_proxy_config(const common::ObString &key_string, const common::ObString &value_string);
  bool is_config_in_service(const common::ObString &config_name);
  static int execute(void *arg);
  static int commit(void* arg, bool is_success);
  static int before_commit(void * arg);

private:
  ProxyConfigHashMap proxy_config_map_array_[2];
  int64_t index_;
  common::DRWLock proxy_config_lock_;
  uint64_t config_version_;
  bool need_sync_to_file_;
  common::ObSEArray<obutils::ObProxyVariantString, 4> execute_sql_array_;
  bool need_rebuild_config_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigTableProcessor);
};

extern ObProxyConfigTableProcessor &get_global_proxy_config_table_processor();

} // end of omt
} // end of obproxy
} // end of oceanbase

#endif

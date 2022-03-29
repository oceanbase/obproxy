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

#ifndef OBPROXY_TENANT_PROCESSOR_H
#define OBPROXY_TENANT_PROCESSOR_H

#include "obutils/ob_config_processor.h"
#include "lib/lock/ob_drw_lock.h"
#include "ob_vip_tenant_conn.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObResourceUnitTableProcessor
{
public:
  ObResourceUnitTableProcessor();
  virtual ~ObResourceUnitTableProcessor() { destroy(); }

  int init();
  void destroy();
  static int execute(void* cloud_params);
  static int commit(bool is_success);

  // Handling vip tenant connection related
  bool check_and_inc_conn(common::ObString& cluster_name,
      common::ObString& tenant_name, common::ObString& ip_name);

  int inc_conn(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& ip_name,
      int64_t& cur_used_connections);

  void dec_conn(common::ObString& cluster_name,
      common::ObString& tenant_name, common::ObString& ip_name);

  int build_tenant_cluster_vip_name(const common::ObString &tenant_name,
      const common::ObString &cluster_name, const common::ObString &vip_name,
      common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF>& key_string);

  int get_vt_conn_object(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name, ObVipTenantConn*& vt_conn);

  int alloc_and_init_vt_conn(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name, uint32_t max_connections, ObVipTenantConn*& vt_conn);

  int fill_local_vt_conn_cache(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name);

  int backup_local_vt_conn_cache();

  int handle_replace_config(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& name_str, common::ObString& value_str);
  int handle_delete_config(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& name_str);

  int conn_handle_replace_config(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& name_str, common::ObString& value_str);
  int conn_handle_delete_config(common::ObString& cluster_name, common::ObString& tenant_name);

  ObVipTenantConnCache &get_vt_conn_cache() { return vt_conn_cache_; }
  int rollback();
  void set_backup_status(bool status) { backup_status_ = status; }

  int64_t get_conn_map_count() const { return vt_conn_cache_.get_conn_map_count(); }
  ObVipTenantConnCache::VTHashMap* get_conn_map() { return vt_conn_cache_.get_conn_map(); }
  ObVipTenantConnCache::VTHashMap& get_conn_map_replica() { return vt_conn_cache_.get_conn_map_replica(); }

  int create_used_conn(common::ObString& key_name, ObUsedConn*& used_conn, int64_t& cur_used_connections);
  int get_used_conn(common::ObString& key_name, bool is_need_inc_used_connections, ObUsedConn*& used_conn, int64_t& cur_used_connections);
  int erase_used_conn(common::ObString& key_name);
  int get_or_create_used_conn(common::ObString& key_name, ObUsedConn*& used_conn, int64_t& cur_used_connections);

  TO_STRING_KV(K_(is_inited), K_(backup_status));

private:
  bool is_inited_;
  bool backup_status_;   // false: backup fail; true: backup success

  common::DRWLock rwlock_;
  ObVipTenantConnCache vt_conn_cache_;

  common::DRWLock used_conn_rwlock_;
  ObUsedConnCache used_conn_cache_;

  DISALLOW_COPY_AND_ASSIGN(ObResourceUnitTableProcessor);
};

ObResourceUnitTableProcessor &get_global_resource_unit_table_processor();

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_TENANT_PROCESSOR_H */

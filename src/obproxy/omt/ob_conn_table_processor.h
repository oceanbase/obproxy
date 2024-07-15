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

#ifndef OBPROXY_CONN_TABLE_PROCESSOR_H
#define OBPROXY_CONN_TABLE_PROCESSOR_H

#include "obutils/ob_config_processor.h"
#include "lib/lock/ob_drw_lock.h"
#include "ob_vip_tenant_conn.h"
#include "ob_vip_tenant_cpu.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObConnTableProcessor
{
public:
  ObConnTableProcessor() : conn_backup_status_(false), rwlock_(), used_conn_rwlock_() {}
  virtual ~ObConnTableProcessor() { destroy(); }
  void destroy();
  int commit(bool is_success);

  bool check_and_inc_conn(common::ObString& cluster_name,
      common::ObString& tenant_name, common::ObString& ip_name);

  int inc_conn(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& ip_name,
      int64_t& cur_used_connections);

  void dec_conn(common::ObString& cluster_name,
      common::ObString& tenant_name, common::ObString& ip_name);

  int get_vt_conn_object(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name, ObVipTenantConn*& vt_conn);

  int alloc_and_init_vt_conn(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name, uint32_t max_connections, ObVipTenantConn*& vt_conn);

  int fill_local_vt_conn_cache(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& vip_name);

  int backup_local_vt_conn_cache();

  int conn_handle_replace_config(common::ObString& cluster_name, common::ObString& tenant_name,
      common::ObString& name_str, common::ObString& value_str, const bool need_to_backup);
  int conn_handle_delete_config(common::ObString& cluster_name, common::ObString& tenant_name, const bool need_to_backup);

  ObVipTenantConnCache &get_vt_conn_cache() { return vt_conn_cache_; }
  int conn_rollback();

  int64_t get_conn_map_count() const { return vt_conn_cache_.get_conn_map_count(); }
  ObVipTenantConnCache::VTHashMap* get_conn_map() { return vt_conn_cache_.get_conn_map(); }
  ObVipTenantConnCache::VTHashMap& get_conn_map_replica() { return vt_conn_cache_.get_conn_map_replica(); }

  int create_used_conn(common::ObString& key_name, ObUsedConn*& used_conn, int64_t& cur_used_connections);
  int get_used_conn(common::ObString& key_name, bool is_need_inc_used_connections, ObUsedConn*& used_conn, int64_t& cur_used_connections);
  int erase_used_conn(common::ObString& key_name, ObUsedConn* used_conn);
  int get_or_create_used_conn(common::ObString& key_name, ObUsedConn*& used_conn, int64_t& cur_used_connections);
  
  TO_STRING_KV(K_(conn_backup_status));

private:
  bool conn_backup_status_;   // false: backup failed; true: backup successful

  common::DRWLock rwlock_;
  ObVipTenantConnCache vt_conn_cache_;

  common::DRWLock used_conn_rwlock_;
  ObUsedConnCache used_conn_cache_;

  DISALLOW_COPY_AND_ASSIGN(ObConnTableProcessor);
};

ObConnTableProcessor &get_global_conn_table_processor();

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_CONN_TABLE_PROCESSOR_H */

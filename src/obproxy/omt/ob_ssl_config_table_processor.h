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

#ifndef OB_SSL_CONFIG_TABLE_PROCESSOR_H_
#define OB_SSL_CONFIG_TABLE_PROCESSOR_H_

#include "obutils/ob_proxy_string_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

struct SSLConfigInfo
{
  SSLConfigInfo() { reset(); }
  bool is_key_info_valid_;

  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

class ObSSLConfigTableProcessor
{
public:
  ObSSLConfigTableProcessor() : index_(0), ssl_config_lock_(obsys::WRITE_PRIORITY), delete_info_() {}
  ~ObSSLConfigTableProcessor() {}

  int init();
  void inc_index();
  void handle_delete();
  int set_ssl_config(const common::ObString &cluster_name, const common::ObString &tenant_name,
                     const common::ObString &name, const common::ObString &value, const bool need_to_backup);
  int delete_ssl_config(common::ObString &cluster_name, common::ObString &tenant_name, const bool need_to_backup);
  bool is_ssl_key_info_valid(const common::ObString &cluster_name, const common::ObString &tenant_name);
  void print_config();
private:
  int backup_hash_map();
  static int execute(void *arg);
  static int commit(void* arg, bool is_success);

private:
  typedef common::hash::ObHashMap<common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH>, SSLConfigInfo> SSLConfigHashMap;
  SSLConfigHashMap ssl_config_map_array_[2];
  int64_t index_;
  common::DRWLock ssl_config_lock_;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> delete_info_;

private:
DISALLOW_COPY_AND_ASSIGN(ObSSLConfigTableProcessor);
};

extern ObSSLConfigTableProcessor &get_global_ssl_config_table_processor();

} // end of omt
} // end of obproxy
} // end of oceanbase

#endif

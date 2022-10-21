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

#ifndef OB_WHITE_LIST_TABLE_PROCESSOR_H_
#define OB_WHITE_LIST_TABLE_PROCESSOR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_drw_lock.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

struct AddrStruct
{
  uint32_t ip_;
  uint32_t net_;

  int64_t to_string(char *buf, const int64_t buf_len) const;
};

class ObWhiteListTableProcessor
{
typedef common::hash::ObHashMap<common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH>, common::ObSEArray<AddrStruct, 4> > WhiteListHashMap;
public:
  ObWhiteListTableProcessor() : index_(0), white_list_lock_(obsys::WRITE_PRIORITY) {}
  ~ObWhiteListTableProcessor() {}

  int init();
  bool can_ip_pass(common::ObString &cluster_name, common::ObString &tenant_name,
                   common::ObString &user_name, uint32_t src_ip, bool is_big_endian);
  bool can_ip_pass(common::ObString &cluster_name, common::ObString &tenant_name, common::ObString &user_name, char *ip);
  int set_ip_list(common::ObString &cluster_name, common::ObString &tenant_name, common::ObString &ip_list);
  int delete_ip_list(common::ObString &cluster_name, common::ObString &tenant_name);
  void inc_index();
  void print_config();
  WhiteListHashMap& get_backup_hashmap() { return addr_hash_map_array_[(index_ + 1) % 2]; }
  void clean_hashmap(WhiteListHashMap& whitelist_map);
private:
  int32_t ip_to_int(char *addr, uint32_t &value);
  int backup_hash_map();
  uint32_t to_little_endian(uint32_t value);
private:
  static int execute(void *arg);
  static int commit(void* arg, bool is_success);

private:
  WhiteListHashMap addr_hash_map_array_[2];
  int64_t index_;
private:
  common::DRWLock white_list_lock_;
};

extern ObWhiteListTableProcessor &get_global_white_list_table_processor();

} // end of omt
} // end of obproxy
} // end of oceanbase
#endif

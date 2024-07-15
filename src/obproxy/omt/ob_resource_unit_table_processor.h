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

#ifndef OBPROXY_RESOURCE_UNIT_TABLE_PROCESSOR_H
#define OBPROXY_RESOURCE_UNIT_TABLE_PROCESSOR_H

#include "obutils/ob_config_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObResourceUnitTableProcessor
{
public:
  ObResourceUnitTableProcessor() : is_inited_(false), name_type_(0) {}
  virtual ~ObResourceUnitTableProcessor() {}
  static int execute(void* args);
  static int commit(void* args, bool is_success);
  int init();
  static int get_config_params(void* args, common::ObString& cluster_str, common::ObString& tenant_str,
      common::ObString& name_str, common::ObString& value_str, ObProxyBasicStmtType& stmt_type);
  int handle_replace_config(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& name_str, common::ObString& value_str, const bool need_to_backup);
  int handle_delete_config(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& name_str, const bool need_to_backup);
  TO_STRING_KV(K_(is_inited));
  uint8_t get_name_type() { return name_type_; }
  void reset_name_type() { name_type_ = 0; }

private:
  bool is_inited_;
  // 表示sql中插入了哪些name的种类。第1个bit表示conn_name,第二个bit表示cpu_name
  uint8_t name_type_;
  DISALLOW_COPY_AND_ASSIGN(ObResourceUnitTableProcessor);
};

ObResourceUnitTableProcessor &get_global_resource_unit_table_processor();
int build_tenant_cluster_vip_name(const common::ObString &tenant_name,
    const common::ObString &cluster_name, const common::ObString &vip_name,
    common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH>& key_string);
} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_RESOURCE_UNIT_TABLE_PROCESSOR_H */

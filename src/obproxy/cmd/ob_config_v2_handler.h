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

#ifndef OB_CONFIG_V2_HANDLER_H_
#define OB_CONFIG_V2_HANDLER_H_

#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "lib/container/ob_se_array.h"
#include "obutils/ob_proxy_string_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
  class ObMysqlSM;
}
class ObConfigV2Handler : public ObInternalCmdHandler
{
public:
  ObConfigV2Handler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  ~ObConfigV2Handler() {}
  int main_handler(int event, void *data);

private:
  int handle_select_stmt();
  int handle_dml_stmt();

public:
  common::ObSEArray<obutils::ObFixSizeString<OB_MAX_COLUMN_NAME_LENGTH>, 4> sqlite3_column_name_;
  common::ObSEArray<common::ObSEArray<obutils::ObProxyVariantString, 4>, 4> sqlite3_column_value_;
public:
  static int config_v2_cmd_callback(event::ObContinuation *cont, ObInternalCmdInfo &info,
                                    event::ObMIOBuffer *buf, event::ObAction *&action);
private:
  proxy::ObMysqlSM* sm_;
  ObProxyBasicStmtType cmd_type_;
  obmysql::ObMySQLCapabilityFlags capability_;
  
  DISALLOW_COPY_AND_ASSIGN(ObConfigV2Handler);
};

} // end of obproxy
} // end of oceanbase

#endif

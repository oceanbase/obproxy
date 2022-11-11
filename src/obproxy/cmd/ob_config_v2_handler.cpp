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

#include "cmd/ob_config_v2_handler.h"
#include "obutils/ob_config_processor.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "rpc/obmysql/ob_mysql_global.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{

ObConfigV2Handler::ObConfigV2Handler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
        : ObInternalCmdHandler(cont, buf, info), sqlite3_column_name_(),
          sqlite3_column_value_(), cmd_type_(info.get_cmd_type()),
          capability_(info.get_capability())
{
  sm_ = reinterpret_cast<ObMysqlSM *>(cont);
  SET_HANDLER(&ObConfigV2Handler::main_handler);
}

int ObConfigV2Handler::main_handler(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  switch (cmd_type_) {
    case OBPROXY_T_SELECT:
      event_ret = handle_select_stmt();
      break;
    case OBPROXY_T_REPLACE:
    case OBPROXY_T_DELETE:
      event_ret = handle_dml_stmt();
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      WARN_ICMD("unknown type", "cmd_type", get_print_stmt_name(cmd_type_), K(ret));
      event_ret = internal_error_callback(ret);
  }

  return event_ret;
}

int ObConfigV2Handler::handle_select_stmt()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObString sql = sm_->trans_state_.trans_info_.client_request_.get_sql();
  DEBUG_ICMD("handle select stmt", K(sql));
  if (OB_FAIL(get_global_config_processor().execute(sql, cmd_type_, this))) {
    WARN_ICMD("execute sql faield", K(sql), K(ret));
  } else {
    const int64_t column_num = sqlite3_column_name_.count();
    if (OB_UNLIKELY(column_num > OB_PROXY_MAX_INNER_TABLE_COLUMN_NUM)) {
      ret = OB_ERR_UNEXPECTED;
      WARN_ICMD("column num is bigger than OB_PROXY_MAX_INNER_TABLE_COLUMN_NUM", K(ret), K(column_num));
    } else {
      ObProxyColumnSchema schema_array[OB_PROXY_MAX_INNER_TABLE_COLUMN_NUM];
      for (int i = 0; i < column_num; i++) {
        schema_array[i] = ObProxyColumnSchema::make_schema(0, sqlite3_column_name_.at(i).string_, OB_MYSQL_TYPE_VARCHAR);
      }

      if (0 == column_num) {
        if (OB_FAIL(encode_ok_packet(0, capability_))) {
          WARN_ICMD("fail to encode eof packet", K(ret));
        } else {
          event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
        }
      } else if (OB_FAIL(encode_header(schema_array, column_num))) {
        WARN_ICMD("fail to encode header", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sqlite3_column_value_.count(); i++) {
          ObIArray<ObProxyVariantString> &tmp_array = sqlite3_column_value_.at(i);
          ObNewRow row;
          ObObj cells[64];
          for (int64_t j = 0; j < column_num; j++) {
            cells[j].set_varchar(tmp_array.at(j).ptr());
          }
          row.cells_ = cells;
          row.count_ = column_num;
          if (OB_FAIL(encode_row_packet(row))) {
            WARN_ICMD("fail to encode row packet", K(row), K(ret));
          }
        }
        sqlite3_column_name_.reset();
        sqlite3_column_value_.reset();

        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(encode_eof_packet())) {
          WARN_ICMD("fail to encode eof packet", K(ret));
        } else {
          event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }

  return event_ret;
}

int ObConfigV2Handler::handle_dml_stmt()
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObString sql = sm_->trans_state_.trans_info_.client_request_.get_sql();
  DEBUG_ICMD("handle dml stmt", K(sql));
  if (OB_FAIL(get_global_config_processor().execute(sql, cmd_type_, this))) {
    WARN_ICMD("fail to execute sql", K(sql), K(ret));
  } else if (OB_FAIL(encode_ok_packet(0, capability_))) {
    WARN_ICMD("fail to encode_ok_packet", K(ret));
  } else {
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }

  return event_ret;
}

int ObConfigV2Handler::config_v2_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                              ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObConfigV2Handler *handler = NULL;
  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObConfigV2Handler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObConfigV2Handler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObConfigV2Handler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObConfigV2Handler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObConfigV2Handler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }

  return ret;
}

} // end of obproxy
} // end of oceanbase

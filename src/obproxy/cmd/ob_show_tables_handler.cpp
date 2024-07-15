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
 */

#define USING_LOG_PREFIX PROXY_CMD

#include "cmd/ob_show_tables_handler.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "lib/container/ob_array_iterator.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
enum
{
  OB_CC_NAME = 0,
  OB_CC_TYPE,
  OB_CC_MAX_TABLE_COLUMN_ID,
};

ObShardingShowTablesHandler::ObShardingShowTablesHandler(ObMIOBuffer *buf, ObCmdInfo &info, ObProxyBasicStmtSubType sub_type)
  : ObCmdHandler(buf, info), sub_type_(sub_type)
{
}

int ObShardingShowTablesHandler::handle_show_tables(const ObString &logic_tenant_name, const ObString &logic_database_name,
                                            ObString &logic_table_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_table_header(logic_database_name))) {
    WARN_CMD("fail to dump tables header", K(ret));
  } else if (OB_FAIL(dump_table(logic_tenant_name, logic_database_name, logic_table_name))) {
    WARN_CMD("fail to dump tables", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_CMD("fail to encode eof packet", K(ret));
  } else {
    INFO_CMD("succ to build show tables");
  }

  if (OB_FAIL(ret)) {
    if (OB_FAIL(encode_err_packet(ret))) {
      WARN_CMD("fail to encode internal err packet, callback", K(ret));
    } else {
      INFO_CMD("succ to encode internal err packet, callback");
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_external_buf())) {
      WARN_CMD("fail to fill_external_buf", K(ret));
    }
  }
  return ret;
}

int ObShardingShowTablesHandler::dump_table_header(const ObString &logic_database_name)
{
  int ret = OB_SUCCESS;

  ObProxyColumnSchema SHOW_TABLES_ARRAY[OB_CC_MAX_TABLE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CC_NAME,   "",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_TYPE,   "Table_type",   OB_MYSQL_TYPE_VARCHAR),
  };

  char column_name[OB_MAX_DATABASE_NAME_LENGTH] = "Tables_in_";
  logic_database_name.to_string(column_name + strlen(column_name), OB_MAX_DATABASE_NAME_LENGTH - strlen(column_name));
  SHOW_TABLES_ARRAY[0].cname_ = ObString(column_name);

  int64_t size = OB_CC_MAX_TABLE_COLUMN_ID;
  if (OBPROXY_T_SUB_SHOW_TABLES == sub_type_) {
    size = OB_CC_MAX_TABLE_COLUMN_ID - 1;
  }

  if (OB_FAIL(encode_header(SHOW_TABLES_ARRAY, size))) {
    WARN_CMD("fail to encode header", K(ret));
  }

  return ret;
}

int ObShardingShowTablesHandler::dump_table(const ObString &logic_tenant_name, const ObString &logic_database_name,
                                    ObString &logic_table_name)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObString> ObStringArray;
  ObStringArray table_names;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_TABLE_COLUMN_ID];
  int64_t size = OB_CC_MAX_TABLE_COLUMN_ID;
  if (OBPROXY_T_SUB_SHOW_TABLES == sub_type_) {
    size = OB_CC_MAX_TABLE_COLUMN_ID - 1;
  }

  if (OB_FAIL(ObProxyShardUtils::get_all_schema_table(logic_tenant_name, logic_database_name, table_names))) {
    WARN_CMD("fail to get all tables", K(logic_tenant_name), K(ret));
  } else {
    string_to_upper_case(logic_table_name.ptr(), logic_table_name.length());
    for (ObStringArray::iterator it = table_names.begin(); OB_SUCC(ret) && it != table_names.end(); it++) {
      if (common::match_like(*it, logic_table_name)) {
        cells[OB_CC_NAME].set_varchar(*it);
        if (OBPROXY_T_SUB_SHOW_FULL_TABLES == sub_type_) {
          cells[OB_CC_TYPE].set_varchar("BASE TABLE");
        }

        row.cells_ = cells;
        row.count_ = size;
        if (OB_FAIL(encode_row_packet(row))) {
          WARN_CMD("fail to encode row packet", K(row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShardingShowTablesHandler::show_tables_cmd_callback(ObMIOBuffer *buf, ObCmdInfo &info,
                                                  ObProxyBasicStmtSubType sub_type,
                                                  const ObString &logic_tenant_name,
                                                  const ObString &logic_database_name,
                                                  ObString &logic_table_name)
{
  int ret = OB_SUCCESS;
  ObShardingShowTablesHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShardingShowTablesHandler(buf, info, sub_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShardingShowTablesHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShardingShowTablesHandler");
  } else if (OB_FAIL(handler->handle_show_tables(logic_tenant_name, logic_database_name, logic_table_name))) {
    DEBUG_CMD("succ to schedule ObShardingShowTablesHandler");
  }

  if (OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

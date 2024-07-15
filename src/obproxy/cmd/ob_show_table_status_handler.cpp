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

#include "cmd/ob_show_table_status_handler.h"
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
  OB_STS_NAME = 0,
  OB_STS_ENGINE,
  OB_STS_VERSION,
  OB_STS_ROW_FORMAT,
  OB_STS_ROWS,
  OB_STS_AVG_ROW_LENGTH,
  OB_STS_DATA_LENGTH,
  OB_STS_MAX_DATA_LENGTH,
  OB_STS_INDEX_LENGTH,
  OB_STS_DATA_FREE,
  OB_STS_AUTO_INCREMENT,
  OB_STS_CREATE_TIME,
  OB_STS_UPDATE_TIME,
  OB_STS_CHECK_TIME,
  OB_STS_COLLATION,
  OB_STS_CHECKSUM,
  OB_STS_CREATE_OPTIONS,
  OB_STS_COMMENT,
  OB_STS_MAX_COLUMN_ID,
};

const ObProxyColumnSchema SHOW_TABLE_STATUS_ARRAY[OB_STS_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_STS_NAME,             "Name",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_ENGINE,           "Engine",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_VERSION,          "Version",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_ROW_FORMAT,       "Row_format",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_ROWS,             "Rows",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_AVG_ROW_LENGTH,   "Avg_row_length",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_DATA_LENGTH,      "Data_length",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_MAX_DATA_LENGTH,  "Max_data_length", OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_INDEX_LENGTH,     "Index_length",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_DATA_FREE,        "Data_free",       OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_AUTO_INCREMENT,   "Auto_increment",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_CREATE_TIME,      "Create_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_UPDATE_TIME,      "Update_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_CHECK_TIME,       "Check_time",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_COLLATION,        "Collation",       OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_CHECKSUM,         "Checksum",        OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_CREATE_OPTIONS,   "Create_options",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_STS_COMMENT,          "Comment",         OB_MYSQL_TYPE_VARCHAR),
};

ObShardingShowTableStatusHandler::ObShardingShowTableStatusHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShardingShowTableStatusHandler::handle_show_table_status(const ObString &logic_tenant_name, const ObString &logic_database_name,
                                                       ObString &logic_table_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(encode_header(SHOW_TABLE_STATUS_ARRAY, OB_STS_MAX_COLUMN_ID))) {
    WARN_CMD("fail to dump tables header", K(ret));
  } else if (OB_FAIL(dump_table(logic_tenant_name, logic_database_name, logic_table_name))) {
    WARN_CMD("fail to dump tables", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_CMD("fail to encode eof packet", K(ret));
  } else {
    INFO_CMD("succ to build show table status");
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

int ObShardingShowTableStatusHandler::dump_table(const ObString &logic_tenant_name, const ObString &logic_database_name,
                                         ObString &logic_table_name)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObString> ObStringArray;
  ObStringArray table_names;
  ObNewRow row;
  ObObj cells[OB_STS_MAX_COLUMN_ID];

  if (OB_FAIL(ObProxyShardUtils::get_all_schema_table(logic_tenant_name, logic_database_name, table_names))) {
    WARN_CMD("fail to get all tables", K(logic_tenant_name), K(ret));
  } else {
    string_to_upper_case(logic_table_name.ptr(), logic_table_name.length());
    for (ObStringArray::iterator it = table_names.begin(); OB_SUCC(ret) && it != table_names.end(); it++) {
      if (common::match_like(*it, logic_table_name)) {
        cells[OB_STS_NAME].set_varchar(*it);
        cells[OB_STS_ENGINE].set_varchar("Innodb");
        cells[OB_STS_VERSION].set_varchar("10");
        cells[OB_STS_ROW_FORMAT].set_varchar("Dynamic");
        cells[OB_STS_ROWS].set_varchar("0");
        cells[OB_STS_AVG_ROW_LENGTH].set_varchar("0");
        cells[OB_STS_DATA_LENGTH].set_varchar("0");
        cells[OB_STS_MAX_DATA_LENGTH].set_varchar("0");
        cells[OB_STS_INDEX_LENGTH].set_varchar("0");
        cells[OB_STS_DATA_FREE].set_varchar("0");
        cells[OB_STS_AUTO_INCREMENT].set_varchar("NULL");
        cells[OB_STS_CREATE_TIME].set_varchar("NULL");
        cells[OB_STS_UPDATE_TIME].set_varchar("NULL");
        cells[OB_STS_CHECK_TIME].set_varchar("NULL");
        cells[OB_STS_COLLATION].set_varchar("");
        cells[OB_STS_CHECKSUM].set_varchar("");
        cells[OB_STS_CREATE_OPTIONS].set_varchar("");
        cells[OB_STS_COMMENT].set_varchar("");

        row.cells_ = cells;
        row.count_ = OB_STS_MAX_COLUMN_ID;
        if (OB_FAIL(encode_row_packet(row))) {
          WARN_CMD("fail to encode row packet", K(row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShardingShowTableStatusHandler::show_table_status_cmd_callback(ObMIOBuffer *buf, ObCmdInfo &info,
                                                             const ObString &logic_tenant_name, const ObString &logic_database_name,
                                                             ObString &logic_table_name)
{
  int ret = OB_SUCCESS;
  ObShardingShowTableStatusHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShardingShowTableStatusHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShardingShowTableStatusHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShardingShowTableStatusHandler");
  } else if (OB_FAIL(handler->handle_show_table_status(logic_tenant_name, logic_database_name, logic_table_name))) {
    DEBUG_CMD("succ to schedule ObShardingShowTableStatusHandler");
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

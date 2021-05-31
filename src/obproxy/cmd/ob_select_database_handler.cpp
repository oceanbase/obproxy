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

#include "cmd/ob_select_database_handler.h"

using namespace oceanbase::obproxy::event;
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
  OB_CC_MAX_COLUMN_ID,
};

const static ObString &column_name = ObString::make_string("database()");
const static EMySQLFieldType column_type = OB_MYSQL_TYPE_VARCHAR;

ObSelectDatabaseHandler::ObSelectDatabaseHandler(ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit)
  : ObCmdHandler(buf, pkg_seq, memory_limit)
{
}

int ObSelectDatabaseHandler::handle_select_database(const ObString &logic_database_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_header())) {
    WARN_CMD("fail to dump header", K(ret));
  } else if (OB_FAIL(dump_payload(logic_database_name))) {
    WARN_CMD("fail to dump database", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_CMD("fail to encode eof packet", K(ret));
  } else {
    INFO_CMD("succ to build select database");
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

int ObSelectDatabaseHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(&column_name, &column_type, OB_CC_MAX_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }

  return ret;
}

int ObSelectDatabaseHandler::dump_payload(const ObString &logic_database_name)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_COLUMN_ID];
  if (!logic_database_name.empty()) {
    cells[OB_CC_NAME].set_varchar(logic_database_name);
  } else {
    cells[OB_CC_NAME].set_null();
  }

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_CMD("fail to encode row packet", K(row), K(ret));
  }

  return ret;
}

int ObSelectDatabaseHandler::select_database_cmd_callback(ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit, const ObString &logic_database_name)
{
  int ret = OB_SUCCESS;
  ObSelectDatabaseHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObSelectDatabaseHandler(buf, pkg_seq, memory_limit))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObSelectDatabaseHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObSelectDatabaseHandler");
  } else if (OB_FAIL(handler->handle_select_database(logic_database_name))) {
    DEBUG_CMD("succ to schedule ObSelectDatabaseHandler");
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

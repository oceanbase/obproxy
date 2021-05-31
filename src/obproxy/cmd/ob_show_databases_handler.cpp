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

#include "cmd/ob_show_databases_handler.h"
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
  OB_CC_MAX_DATABASE_COLUMN_ID,
};

const static ObString &column_name = ObString::make_string("Database");
const static EMySQLFieldType column_type = OB_MYSQL_TYPE_VARCHAR;

ObShowDatabasesHandler::ObShowDatabasesHandler(ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit)
  : ObCmdHandler(buf, pkg_seq, memory_limit)
{
}

int ObShowDatabasesHandler::handle_show_databases(const ObString &logic_tenant_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_database_header())) {
    WARN_CMD("fail to dump databases header", K(ret));
  } else if (OB_FAIL(dump_database(logic_tenant_name))) {
    WARN_CMD("fail to dump databases", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_CMD("fail to encode eof packet", K(ret));
  } else {
    INFO_CMD("succ to build show databases");
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

int ObShowDatabasesHandler::dump_database_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(&column_name, &column_type, OB_CC_MAX_DATABASE_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }

  return ret;
}

int ObShowDatabasesHandler::dump_database(const ObString &logic_tenant_name)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObString> ObStringArray;
  ObStringArray db_names;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_DATABASE_COLUMN_ID];

  if (OB_FAIL(ObProxyShardUtils::get_all_database(logic_tenant_name, db_names))) {
    WARN_CMD("fail to get all databases", K(logic_tenant_name), K(ret));
  } else {
    for (ObStringArray::iterator it = db_names.begin(); it != db_names.end(); it++) {
      cells[OB_CC_NAME].set_varchar(*it);

      row.cells_ = cells;
      row.count_ = OB_CC_MAX_DATABASE_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WARN_CMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObShowDatabasesHandler::show_databases_cmd_callback(ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit, const ObString &logic_tenant_name)
{
  int ret = OB_SUCCESS;
  ObShowDatabasesHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShowDatabasesHandler(buf, pkg_seq, memory_limit))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShowDatabasesHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShowDatabasesHandler");
  } else if (OB_FAIL(handler->handle_show_databases(logic_tenant_name))) {
    DEBUG_CMD("succ to schedule ObShowDatabasesHandler");
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

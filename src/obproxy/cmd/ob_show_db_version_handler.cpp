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

#include "cmd/ob_show_db_version_handler.h"
#include "proxy/shard/obproxy_shard_utils.h"

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
  OB_CC_DATABASE_NAME,
  OB_CC_MAX_DATABASE_COLUMN_ID,
};

static const ObString column_name[OB_CC_MAX_DATABASE_COLUMN_ID] = {
    ObString::make_string("database"),
    ObString::make_string("version")
};

static const EMySQLFieldType column_type[OB_CC_MAX_DATABASE_COLUMN_ID] = {
    OB_MYSQL_TYPE_VARCHAR,
    OB_MYSQL_TYPE_VARCHAR
};

ObShowDBVersionHandler::ObShowDBVersionHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShowDBVersionHandler::handle_show_db_version(const ObString &logic_tenant_name, const ObString &logic_db_name)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_db_version_header())) {
    WARN_CMD("fail to dump databases header", K(ret));
  } else if (OB_FAIL(dump_db_version(logic_tenant_name, logic_db_name))) {
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

int ObShowDBVersionHandler::dump_db_version_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(column_name, column_type, OB_CC_MAX_DATABASE_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }

  return ret;
}

int ObShowDBVersionHandler::dump_db_version(const ObString &logic_tenant_name, const ObString &logic_db_name)
{
  int ret = OB_SUCCESS;
  ObString db_version;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_DATABASE_COLUMN_ID];

  if (OB_FAIL(ObProxyShardUtils::get_db_version(logic_tenant_name, logic_db_name, db_version))) {
    WARN_CMD("fail to get db version", K(logic_tenant_name), K(logic_db_name), K(ret));
  } else {
    cells[OB_CC_NAME].set_varchar(logic_db_name);
    cells[OB_CC_DATABASE_NAME].set_varchar(db_version);
    row.cells_ = cells;
    row.count_ = OB_CC_MAX_DATABASE_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_CMD("fail to encode row packet", K(row), K(ret));
    }
  }

  return ret;
}

int ObShowDBVersionHandler::show_db_version_cmd_callback(ObMIOBuffer *buf, ObCmdInfo &info,
                                                         const ObString &logic_tenant_name,
                                                         const ObString &logic_db_name)
{
  int ret = OB_SUCCESS;
  ObShowDBVersionHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShowDBVersionHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShowDBVersionHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShowDBVersionHandler");
  } else if (OB_FAIL(handler->handle_show_db_version(logic_tenant_name, logic_db_name))) {
    DEBUG_CMD("succ to schedule ObShowDBVersionHandler");
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

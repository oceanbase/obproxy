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
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obproxy/proxy/mysqllib/ob_proxy_session_info.h"

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

ObShardingShowDatabasesHandler::ObShardingShowDatabasesHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShardingShowDatabasesHandler::handle_show_databases(const ObString &logic_tenant_name,
                                                  ObMysqlClientSession &client_session)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dump_database_header())) {
    WARN_CMD("fail to dump databases header", K(ret));
  } else if (OB_FAIL(dump_database(logic_tenant_name, client_session))) {
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

int ObShardingShowDatabasesHandler::dump_database_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(&column_name, &column_type, OB_CC_MAX_DATABASE_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }

  return ret;
}

int ObShardingShowDatabasesHandler::dump_database(const ObString &logic_tenant_name,
                                          ObMysqlClientSession &client_session)
{
  int ret = OB_SUCCESS;
  typedef ObArray<ObString> ObStringArray;

  ObStringArray db_names;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_DATABASE_COLUMN_ID];

  if (OB_FAIL(ObProxyShardUtils::get_all_database(logic_tenant_name, db_names))) {
    WARN_CMD("fail to get all databases", K(logic_tenant_name), K(ret));
  } else {
    for (ObStringArray::iterator it = db_names.begin(); OB_SUCC(ret) && it != db_names.end(); it++) {
      ObString db_name = *it;
      if (OB_FAIL(ObProxyShardUtils::check_logic_db_priv_for_cur_user(logic_tenant_name,
                                                                      client_session, db_name))) {
        ret = OB_SUCCESS;
        LOG_DEBUG("no privilege to show this db", K(db_name), K(ret));
      } else {
        cells[OB_CC_NAME].set_varchar(*it);
        row.cells_ = cells;
        row.count_ = OB_CC_MAX_DATABASE_COLUMN_ID;
        if (OB_FAIL(encode_row_packet(row))) {
          WARN_CMD("fail to encode row packet", K(row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShardingShowDatabasesHandler::show_databases_cmd_callback(ObMIOBuffer *buf, ObCmdInfo &info,
                                                        const ObString &logic_tenant_name,
                                                        ObMysqlClientSession &client_session)
{
  int ret = OB_SUCCESS;
  ObShardingShowDatabasesHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShardingShowDatabasesHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShardingShowDatabasesHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShardingShowDatabasesHandler");
  } else if (OB_FAIL(handler->handle_show_databases(logic_tenant_name, client_session))) {
    DEBUG_CMD("succ to schedule ObShardingShowDatabasesHandler");
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

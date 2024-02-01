// Copyright 2014-2016 Alibaba Inc. All Rights Reserved.
// Author:
//    luoxiaohu.lxh <luoxiaohu.lxh@oceanbase.com>
// Normalizer:
//    luoxiaohu.lxh <luoxiaohu.lxh@oceanbase.com>
//


#define USING_LOG_PREFIX PROXY_CMD

#include "cmd/ob_show_create_table_handler.h"
#include "dbconfig/ob_proxy_db_config_info.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
//CreateTableID for sharding
enum
{
  OB_CT_TABLE = 0,
  OB_CT_CREATE_TABLE,
  OB_CT_SHARD_MAX_COLUMN_ID,
};

const ObProxyColumnSchema SHOW_CREATE_TABLE_ARRAY[OB_CT_SHARD_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CT_TABLE,          "Table",           OB_MYSQL_TYPE_VAR_STRING),
    ObProxyColumnSchema::make_schema(OB_CT_CREATE_TABLE,   "Create Table",    OB_MYSQL_TYPE_LONG_BLOB),
};

ObShowCreateTableHandler::ObShowCreateTableHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShowCreateTableHandler::show_create_table_cmd_callback(ObMIOBuffer *buf,
                                                             ObCmdInfo &info,
                                                             const ObString &logic_tenant_name,
                                                             const ObString &logic_database_name,
                                                             const ObString &logic_table_name)
{
  int ret = OB_SUCCESS;
  ObShowCreateTableHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShowCreateTableHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShowCreateTableHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShowCreateTableHandler");
  } else if (OB_FAIL(handler->handle_show_create_table(logic_tenant_name, logic_database_name, logic_table_name))){
    WARN_CMD("fail to handle show create table", K(logic_tenant_name), K(logic_database_name),
                                                 K(logic_table_name), K(ret));
  }

  if (OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int ObShowCreateTableHandler::handle_show_create_table(const ObString &tenant_name,
                                                       const ObString &db_name,
                                                       const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(SHOW_CREATE_TABLE_ARRAY, OB_CT_SHARD_MAX_COLUMN_ID))) {
    WARN_CMD("fail to dump_header", K(ret));
  } else {
    ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
    ObDbConfigLogicDb *db_info = NULL;
    ObShardRule *shard_rule = NULL;
    if (OB_ISNULL(db_info = dbconfig_cache.get_exist_db_info(tenant_name, db_name))) {
      ret = OB_ERR_BAD_DATABASE;
      WARN_CMD("logic database not exist", K(tenant_name), K(db_name), K(ret));
    } else if (OB_FAIL(db_info->get_shard_rule(shard_rule, table_name))) {
      // no logic table
      ret = OB_TABLE_NOT_EXIST;
    } else {
      ObNewRow row;
      ObObj cells[OB_CT_SHARD_MAX_COLUMN_ID];
      cells[OB_CT_TABLE].set_varchar(table_name);
      cells[OB_CT_CREATE_TABLE].set_varchar(shard_rule->create_table_sql_);
      row.cells_ = cells;
      row.count_ = OB_CT_SHARD_MAX_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WARN_CMD("fail to encode row packet for shard show create table", K(row), K(ret));
      }
    }

    if (NULL != db_info) {
      db_info->dec_ref();
      db_info = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_CMD("fail to encode eof packet", K(ret));
    }
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

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

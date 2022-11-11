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

#include "cmd/ob_show_topology_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

//TopologyColumnID
enum
{
  OB_TC_EID = 0,
  OB_TC_MAX_COLUMN_ID
};

static const ObString column_name[OB_TC_MAX_COLUMN_ID] = {
    ObString::make_string("elastic_id")
};

static const EMySQLFieldType column_type[OB_TC_MAX_COLUMN_ID] = {
    OB_MYSQL_TYPE_LONG
};

ObShowTopologyHandler::ObShowTopologyHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShowTopologyHandler::handle_show_topology(const ObString &tenant_name,
                                                const ObString &db_name,
                                                const ObString &group_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dump_topology_header())) {
    WARN_CMD("fail to dump_header", K(ret));
  } else {
    ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
    ObDbConfigLogicDb *db_info = NULL;
    ObShardTpo *shard_tpo = NULL;
    bool es_id_array[OB_MAX_ELASTIC_ID_COUNT];
    memset(es_id_array, 0, sizeof(es_id_array));
    if (OB_ISNULL(db_info = dbconfig_cache.get_exist_db_info(tenant_name, db_name))) {
      ret = OB_ERR_BAD_DATABASE;
      WARN_CMD("logic database not exist", K(tenant_name), K(db_name), K(ret));
    } else if (OB_FAIL(db_info->get_shard_tpo(shard_tpo))) {
      // no shard router, return success
      ret = OB_SUCCESS;
    } else if (!group_name.empty()) {
      ObGroupCluster *gc_info = NULL;
      shard_tpo->get_group_cluster(group_name, gc_info);
      if (NULL != gc_info) {
        for (int64_t i = 0; i < gc_info->es_array_.count(); ++i) {
          const ObElasticInfo &es_info = gc_info->es_array_.at(i);
          if (es_info.eid_ >= 0 && es_info.eid_ < OB_MAX_ELASTIC_ID_COUNT) {
            es_id_array[es_info.eid_] = true;
          }
        }
      }
    } else {
      ObShardTpo::GCHashMap &map = const_cast<ObShardTpo::GCHashMap &>(shard_tpo->gc_map_);
      for (ObShardTpo::GCHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
        for (int64_t i = 0; i < it->es_array_.count(); ++i) {
          const ObElasticInfo &es_info = it->es_array_.at(i);
          if (es_info.eid_ >= 0 && es_info.eid_ < OB_MAX_ELASTIC_ID_COUNT) {
            es_id_array[es_info.eid_] = true;
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < OB_MAX_ELASTIC_ID_COUNT; ++i) {
      if (es_id_array[i]) {
        if (OB_FAIL(dump_elastic_id(i))) {
          WARN_CMD("fail to dump elastic id", K(ret));
        }
      }
    }
    if (NULL != shard_tpo) {
      shard_tpo->dec_ref();
      shard_tpo = NULL;
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

int ObShowTopologyHandler::dump_elastic_id(const int64_t eid)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_TC_MAX_COLUMN_ID];
  cells[OB_TC_EID].set_int(eid);
  row.cells_ = cells;
  row.count_ = OB_TC_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_CMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowTopologyHandler::dump_topology_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(column_name, column_type, OB_TC_MAX_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowTopologyHandler::show_topology_cmd_callback(ObMIOBuffer *buf,
                                                      ObCmdInfo &info,
                                                      const ObString &logic_tenant_name,
                                                      const ObString &logic_database_name,
                                                      const ObString &group_name)
{
  int ret = OB_SUCCESS;
  ObShowTopologyHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShowTopologyHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShowTopologyHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShowTopologyHandler");
  } else if (OB_FAIL(handler->handle_show_topology(logic_tenant_name, logic_database_name, group_name))){
    WARN_CMD("fail to handle show topology", K(logic_tenant_name), K(logic_database_name), K(group_name), K(ret));
  }

  if (OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

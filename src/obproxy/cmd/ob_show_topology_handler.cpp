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
#include "dbconfig/ob_proxy_db_config_info.h"
#include "iocore/eventsystem/ob_event_processor.h"
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

//TopologyColumnID for sharding
enum
{
  OB_TC_ID = 0,
  OB_TC_GROUP_NAME,
  OB_TC_TABLE_NAME,
  OB_TC_SCHEMA_TYPE,
  OB_TC_SHARD_KEY,
  OB_TC_SHARD_RULE,
  OB_TC_SHARD_MAX_COLUMN_ID,
};

const ObProxyColumnSchema SHOW_TOPOLOGY_ARRAY[OB_TC_SHARD_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_TC_ID,           "id",            OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_TC_GROUP_NAME,   "group_name",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_TABLE_NAME,   "table_name",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_SCHEMA_TYPE,  "schema_type",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_SHARD_KEY,    "shard_key",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_TC_SHARD_RULE,   "shard_rule",    OB_MYSQL_TYPE_VARCHAR),
};

ObShowTopologyHandler::ObShowTopologyHandler(ObMIOBuffer *buf, ObCmdInfo &info)
  : ObCmdHandler(buf, info)
{
}

int ObShowTopologyHandler::handle_show_elastic_id(const ObString &tenant_name,
                                                  const ObString &db_name,
                                                  const ObString &group_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dump_elastic_id_header())) {
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

int ObShowTopologyHandler::dump_elastic_id_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(column_name, column_type, OB_TC_MAX_COLUMN_ID))) {
    WARN_CMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowTopologyHandler::show_elastic_id_cmd_callback(ObMIOBuffer *buf,
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
  } else if (OB_FAIL(handler->handle_show_elastic_id(logic_tenant_name, logic_database_name, group_name))){
    WARN_CMD("fail to handle show topology", K(logic_tenant_name), K(logic_database_name), K(group_name), K(ret));
  }

  if (OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}


int ObShowTopologyHandler::show_topology_cmd_callback(ObMIOBuffer *buf,
                                                      ObCmdInfo &info,
                                                      const ObString &logic_tenant_name,
                                                      const ObString &logic_database_name,
                                                      const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObShowTopologyHandler *handler = NULL;

  if (OB_ISNULL(handler = new(std::nothrow) ObShowTopologyHandler(buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_CMD("fail to new ObShowTopologyHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_CMD("fail to init for ObShowTopologyHandler");
  } else if (OB_FAIL(handler->handle_show_topology(logic_tenant_name, logic_database_name, table_name))){
    WARN_CMD("fail to handle show topology", K(logic_tenant_name), K(logic_database_name), K(ret));
  }

  if (OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int ObShowTopologyHandler::handle_show_topology(const ObString &tenant_name,
                                                const ObString &db_name,
                                                const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(SHOW_TOPOLOGY_ARRAY, OB_TC_SHARD_MAX_COLUMN_ID))) {
    WARN_CMD("fail to dump_header", K(ret));
  } else {
    ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
    ObDbConfigLogicDb *db_info = NULL;
    ObShardRule *shard_rule = NULL;
    if (OB_ISNULL(db_info = dbconfig_cache.get_exist_db_info(tenant_name, db_name))) {
      ret = OB_ERR_BAD_DATABASE;
      WARN_CMD("logic database not exist", K(tenant_name), K(db_name), K(ret));
    } else if (OB_UNLIKELY(db_info->is_single_shard_db_table())) {
      if (OB_FAIL(dump_shard_topology_for_single_db(table_name))) {
        WARN_CMD("fail to encode topology packet for single db", K(tenant_name), K(db_name), K(ret));
      }
    } else if (OB_FAIL(db_info->get_shard_rule(shard_rule, table_name))) {
      // no logic table
      ret = OB_TABLE_NOT_EXIST;
    } else if (OB_UNLIKELY((1 == shard_rule->db_size_) && (1 == shard_rule->tb_size_))) {
      if (OB_FAIL(dump_shard_topology_for_shard_db_single_tb(table_name))) {
        WARN_CMD("fail to encode topology packet for shard_db_single_tb", K(tenant_name), K(db_name), K(shard_rule->db_size_), K(shard_rule->tb_size_), K(ret));
      }
    } else if (OB_UNLIKELY(1 == shard_rule->tb_size_)) {
      if (OB_FAIL(dump_shard_topology_for_shard_db_non_shard_tb(table_name, shard_rule))) {
        WARN_CMD("fail to encode topology packet for shard_db_non_shard_tb", K(tenant_name), K(db_name), K(shard_rule->db_size_), K(shard_rule->tb_size_), K(ret));
      }
    } else {
      if (OB_FAIL(dump_shard_topology_for_shard_db_shard_tb(shard_rule))) {
        WARN_CMD("fail to encode topology packet for shard_db_shard_tb", K(tenant_name), K(db_name), K(shard_rule->db_size_), K(shard_rule->tb_size_), K(ret));
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

int ObShowTopologyHandler::dump_shard_topology_for_single_db(ObString table_name)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_TC_SHARD_MAX_COLUMN_ID];
  cells[OB_TC_ID].set_int(0);
  cells[OB_TC_GROUP_NAME].set_varchar("group_00");
  cells[OB_TC_TABLE_NAME].set_varchar(table_name);
  cells[OB_TC_SCHEMA_TYPE].set_varchar("SINGLE");
  cells[OB_TC_SHARD_KEY].set_null();
  cells[OB_TC_SHARD_RULE].set_null();
  row.cells_ = cells;
  row.count_ = OB_TC_SHARD_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_CMD("fail to encode row packet for shard", K(row), K(ret));
  }
  return ret;
}

int ObShowTopologyHandler::dump_shard_topology_for_shard_db_single_tb(ObString table_name)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_TC_SHARD_MAX_COLUMN_ID];
  cells[OB_TC_ID].set_int(0);
  cells[OB_TC_GROUP_NAME].set_varchar("group_00");
  cells[OB_TC_TABLE_NAME].set_varchar(table_name);
  cells[OB_TC_SCHEMA_TYPE].set_varchar("SHARD");
  cells[OB_TC_SHARD_KEY].set_null();
  cells[OB_TC_SHARD_RULE].set_null();
  row.cells_ = cells;
  row.count_ = OB_TC_SHARD_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_CMD("fail to encode row packet for shard", K(row), K(ret));
  }
  return ret;
}

int ObShowTopologyHandler::dump_shard_topology_for_shard_db_non_shard_tb(ObString table_name, ObShardRule *shard_rule)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_TC_SHARD_MAX_COLUMN_ID];
  int size = OB_TC_SHARD_MAX_COLUMN_ID;
  ObString real_group_name;
  ObString real_table_name = table_name;
  char group_name_buf[OB_MAX_TABLEGROUP_NAME_LENGTH];
  ObString schema_type("SHARD");
  char shard_key_buf[MAX_RULE_BUF_SIEZ];
  char shard_rule_buf[MAX_RULE_BUF_SIEZ];
  ObString shard_key_str = get_shard_key_str(*shard_rule, shard_key_buf, MAX_RULE_BUF_SIEZ);
  ObString shard_rule_str = get_shard_rule_str(*shard_rule, shard_rule_buf, MAX_RULE_BUF_SIEZ);

  int64_t count = shard_rule->db_size_;
  for(int64_t index = 0; index < count; ++index) {
    int64_t group_index = index;
    shard_rule->get_real_name_by_index(shard_rule->db_size_,
                                        shard_rule->db_suffix_len_, group_index,
                                        shard_rule->db_prefix_.config_string_,
                                        shard_rule->db_tail_.config_string_,
                                        group_name_buf, OB_MAX_TABLEGROUP_NAME_LENGTH);
    real_group_name = ObString::make_string(group_name_buf);
    cells[OB_TC_ID].set_int(index);
    cells[OB_TC_GROUP_NAME].set_varchar(real_group_name);
    cells[OB_TC_TABLE_NAME].set_varchar(real_table_name);
    cells[OB_TC_SCHEMA_TYPE].set_varchar(schema_type);
    cells[OB_TC_SHARD_KEY].set_varchar(shard_key_str);
    cells[OB_TC_SHARD_RULE].set_varchar(shard_rule_str);
    row.cells_ = cells;
    row.count_ = size;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_CMD("fail to encode row packet for shard", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowTopologyHandler::dump_shard_topology_for_shard_db_shard_tb(ObShardRule *shard_rule)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY((0 == shard_rule->db_size_) || (shard_rule->tb_size_ < shard_rule->db_size_))) {
    WARN_CMD("wrong shard rule for shard_db_shard_tb", K(shard_rule->db_size_), K(shard_rule->tb_size_), K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_TC_SHARD_MAX_COLUMN_ID];
    int size = OB_TC_SHARD_MAX_COLUMN_ID;
    ObString real_group_name;
    ObString real_table_name;
    char group_name_buf[OB_MAX_TABLEGROUP_NAME_LENGTH];
    char table_name_buf[OB_MAX_USER_TABLE_NAME_LENGTH];
    ObString schema_type("SHARD");
    char shard_key_buf[MAX_RULE_BUF_SIEZ];
    char shard_rule_buf[MAX_RULE_BUF_SIEZ];
    ObString shard_key_str = get_shard_key_str(*shard_rule, shard_key_buf, MAX_RULE_BUF_SIEZ);
    ObString shard_rule_str = get_shard_rule_str(*shard_rule, shard_rule_buf, MAX_RULE_BUF_SIEZ);

    int64_t count = shard_rule->tb_size_;
    for(int64_t index = 0; index < count; ++index) {
      int64_t group_index = index / (shard_rule->tb_size_ / shard_rule->db_size_);
      shard_rule->get_real_name_by_index(shard_rule->db_size_,
                                          shard_rule->db_suffix_len_, group_index,
                                          shard_rule->db_prefix_.config_string_,
                                          shard_rule->db_tail_.config_string_,
                                          group_name_buf, OB_MAX_TABLEGROUP_NAME_LENGTH);
      shard_rule->get_real_name_by_index(shard_rule->tb_size_,
                                          shard_rule->tb_suffix_len_, index,
                                          shard_rule->tb_prefix_.config_string_,
                                          shard_rule->tb_tail_.config_string_,
                                          table_name_buf, OB_MAX_USER_TABLE_NAME_LENGTH);
      real_group_name = ObString::make_string(group_name_buf);
      real_table_name = ObString::make_string(table_name_buf);
      cells[OB_TC_ID].set_int(index);
      cells[OB_TC_GROUP_NAME].set_varchar(real_group_name);
      cells[OB_TC_TABLE_NAME].set_varchar(real_table_name);
      cells[OB_TC_SCHEMA_TYPE].set_varchar(schema_type);
      cells[OB_TC_SHARD_KEY].set_varchar(shard_key_str);
      cells[OB_TC_SHARD_RULE].set_varchar(shard_rule_str);
      row.cells_ = cells;
      row.count_ = size;
      if (OB_FAIL(encode_row_packet(row))) {
        WARN_CMD("fail to encode row packet for shard", K(row), K(ret));
      }
    }
  }
  return ret;
}

ObString ObShowTopologyHandler::get_shard_key_str(dbconfig::ObShardRule& shard_rule,
                                                  char * const buf,
                                                  const int64_t buf_len)
{
  ObString ret_str = ObString::make_string("empty shard key");

  if (OB_UNLIKELY(0 >= shard_rule.shard_key_columns_.count())) {
    // nothing
  } else if (OB_LIKELY(1 == shard_rule.shard_key_columns_.count())) {
    ret_str = shard_rule.shard_key_columns_[0];
  } else {
    int pos = 0;
    int write_len = 0;
    for (int64_t i = 0; i < shard_rule.shard_key_columns_.count(); ++i) {
      const ObString& tmp_shard_key = shard_rule.shard_key_columns_.at(i);
      write_len = snprintf(buf + pos, buf_len, "%.*s,", tmp_shard_key.length(), tmp_shard_key.ptr());
      if (write_len <= 0) {
        break;
      }
      pos += write_len;
    }

    if (OB_LIKELY(pos > 0)) {
      buf[pos] = '\0';
      ret_str.assign_ptr(buf, pos);
    }
  }

  return ret_str;
}

ObString ObShowTopologyHandler::get_shard_rule_str(dbconfig::ObShardRule& shard_rule,
                                                   char * const buf,
                                                   const int64_t buf_len)
{
  ObString ret_str = ObString::make_string("empty shard rule");

  if (OB_UNLIKELY(0 >= shard_rule.tb_rules_.count())) {
    // nothing
  } else if (OB_LIKELY(1 == shard_rule.tb_rules_.count())) {
    ret_str = shard_rule.tb_rules_.at(0).shard_rule_str_.config_string_;
  } else {
    int pos = 0;
    int write_len = 0;
    for (int64_t i = 0; i < shard_rule.tb_rules_.count(); ++i) {
      const ObString& tmp_shard_rule = shard_rule.tb_rules_.at(i).shard_rule_str_.config_string_;
      write_len = snprintf(buf + pos, buf_len, "%.*s;", tmp_shard_rule.length(), tmp_shard_rule.ptr());
      if (write_len <= 0) {
        break;
      }
      pos += write_len;
    }

    if (OB_LIKELY(pos > 0)) {
      buf[pos] = '\0';
      ret_str.assign_ptr(buf, pos);
    }
  }

  return ret_str;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

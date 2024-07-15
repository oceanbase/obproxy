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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_show_route_handler.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_task.h"
#include "iocore/eventsystem/ob_event_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

//RouteColumn
enum
{
  OB_RC_CNAME = 0,
  OB_RC_TENAME,
  OB_RC_DNAME,
  OB_RC_TABLE_NAME,
  OB_RC_STATE,
  OB_RC_PART_NUM,
  OB_RC_REPLICA_NUM,
  OB_RC_TABLE_ID,
  OB_RC_CR_VERSION,
  OB_RC_SCHEMA_VERSION,
  OB_RC_FROM_RSLIST,
  OB_RC_CREATE,
  OB_RC_LAST_VALID,
  OB_RC_LAST_ACCESS,
  OB_RC_LAST_UPDATE,
  OB_RC_EXPIRE_TIME,
  OB_RC_RELATIVE_EXPIRE_TIME,
  OB_RC_SERVER_ADDR,
  OB_RC_MAX_ROUTE_COLUMN_ID,
};


const ObProxyColumnSchema ROUTE_COLUMN_ARRAY[OB_RC_MAX_ROUTE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_RC_CNAME,           "cluster_name",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_TENAME,          "tenant_name",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_DNAME,           "database_name",        OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_TABLE_NAME,      "table_name",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_STATE,           "state",                OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_PART_NUM,        "partition_num",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RC_REPLICA_NUM,     "replica_num",          OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RC_TABLE_ID,        "table_id",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RC_CR_VERSION,      "cluster_version",      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RC_SCHEMA_VERSION,  "schema_version",       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RC_FROM_RSLIST,     "from_rslist",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_CREATE,          "create_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_LAST_VALID,      "last_valid_time",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_LAST_ACCESS,     "last_access_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_LAST_UPDATE,     "last_update_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_EXPIRE_TIME,     "expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_RELATIVE_EXPIRE_TIME,     "relative_expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RC_SERVER_ADDR,     "server addr",          OB_MYSQL_TYPE_VARCHAR),
};

//RoutePartitionColumn
enum
{
  OB_RPC_TABLE_ID = 0,
  OB_RPC_PARTITION_ID,
  OB_RPC_STATE,
  OB_RPC_CR_VERSION,
  OB_RPC_SCHEMA_VERSION,
  OB_RPC_CREATE,
  OB_RPC_LAST_VALID,
  OB_RPC_LAST_ACCESS,
  OB_RPC_LAST_UPDATE,
  OB_RPC_EXPIRE_TIME,
  OB_RPC_RELATIVE_EXPIRE_TIME,
  OB_RPC_SERVER_ADDR,
  OB_RPC_MAX_ROUTE_COLUMN_ID,
};

const ObProxyColumnSchema ROUTE_PARTITION_COLUMN_ARRAY[OB_RPC_MAX_ROUTE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_RPC_TABLE_ID,        "table_id",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RPC_PARTITION_ID,    "partition_id",         OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RPC_STATE,           "state",                OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_CR_VERSION,      "cluster_version",      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RPC_SCHEMA_VERSION,  "schema_version",       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RPC_CREATE,          "create_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_LAST_VALID,      "last_valid_time",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_LAST_ACCESS,     "last_access_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_LAST_UPDATE,     "last_update_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_EXPIRE_TIME,     "expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_RELATIVE_EXPIRE_TIME,     "relative_expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RPC_SERVER_ADDR,     "server addr",          OB_MYSQL_TYPE_VARCHAR),
};


//RouteRoutineColumn
enum
{
  OB_RRC_CNAME = 0,
  OB_RRC_TENAME,
  OB_RRC_DNAME,
  OB_RRC_PNAME,
  OB_RRC_RNAME,
  OB_RRC_STATE,
  OB_RRC_ROUTINE_TYPE,
  OB_RRC_ROUTINE_ID,
  OB_RRC_CR_VERSION,
  OB_RRC_SCHEMA_VERSION,
  OB_RRC_CREATE,
  OB_RRC_LAST_VALID,
  OB_RRC_LAST_ACCESS,
  OB_RRC_LAST_UPDATE,
  OB_RRC_EXPIRE_TIME,
  OB_RRC_RELATIVE_EXPIRE_TIME,
  OB_RRC_ROUTE_SQL,
  OB_RRC_MAX_ROUTE_COLUMN_ID,
};

const ObProxyColumnSchema ROUTE_ROUTINE_COLUMN_ARRAY[OB_RRC_MAX_ROUTE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_RRC_CNAME,           "cluster_name",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_TENAME,          "tenant_name",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_DNAME,           "database_name",        OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_PNAME,           "package_name",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_RNAME,           "routine_name",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_STATE,           "state",                OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_ROUTINE_TYPE,    "routine_num",          OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RRC_ROUTINE_ID,      "routine_id",           OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RRC_CR_VERSION,      "cluster_version",      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RRC_SCHEMA_VERSION,  "schema_version",       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_RRC_CREATE,          "create_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_LAST_VALID,      "last_valid_time",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_LAST_ACCESS,     "last_access_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_LAST_UPDATE,     "last_update_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_EXPIRE_TIME,     "expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_RELATIVE_EXPIRE_TIME,     "relative_expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_RRC_ROUTE_SQL,       "route_sql",            OB_MYSQL_TYPE_VARCHAR),
};

//GlobalIndexColumn
enum
{
  OB_GIC_TABLE_ID = 0,
  OB_GIC_DATA_TABLE_ID,
  OB_GIC_INDEX_TABLE_NAME,
  OB_GIC_STATE,
  OB_GIC_CR_VERSION,
  OB_GIC_SCHEMA_VERSION,
  OB_GIC_CREATE,
  OB_GIC_LAST_VALID,
  OB_GIC_LAST_ACCESS,
  OB_GIC_LAST_UPDATE,
  OB_GIC_EXPIRE_TIME,
  OB_GIC_RELATIVE_EXPIRE_TIME,
  OB_GIC_MAX_GLOBAL_INDEX_COLUMN_ID,
};

const ObProxyColumnSchema GLOBAL_INDEX_COLUMN_ARRAY[OB_RPC_MAX_ROUTE_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_GIC_TABLE_ID,        "table_id",             OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_GIC_DATA_TABLE_ID,   "data_table_id",        OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_GIC_INDEX_TABLE_NAME,"index_table_name",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_STATE,           "state",                OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_CR_VERSION,      "cluster_version",      OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_GIC_SCHEMA_VERSION,  "schema_version",       OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_GIC_CREATE,          "create_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_LAST_VALID,      "last_valid_time",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_LAST_ACCESS,     "last_access_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_LAST_UPDATE,     "last_update_time",     OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_EXPIRE_TIME,     "expire_time",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_GIC_RELATIVE_EXPIRE_TIME,     "relative_expire_time",          OB_MYSQL_TYPE_VARCHAR),
};

int extract_entry_time(const ObRouteEntry &entry, char *create_timebuf, char *valid_timebuf,
                       char *access_timebuf, char *update_timebuf, char *expire_timebuf,
                       char *relative_expire_timebuf, const uint32_t buf_len);

ObShowRouteHandler::ObShowRouteHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), list_bucket_(0)
{
  if (!info.get_large_key_string().empty()) {
    int32_t min_len =std::min(info.get_large_key_string().length(), static_cast<int32_t>(OB_MAX_CONFIG_VALUE_LEN));
    MEMCPY(value_str_, info.get_large_key_string().ptr(), min_len);
    value_str_[min_len] = '\0';
  } else {
    value_str_[0] = '\0';
  }
}

int ObShowRouteHandler::handle_show_table(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump header", K(ret));
  } else if (OB_FAIL(fill_table_entry_name())) {
    WDIAG_ICMD("fail to fill entry name", K(ret));
  } else {
    ObTableCache &table_cache = get_global_table_cache();
    bool terminate = false;
    ObTableEntry *entry = NULL;
    TableIter it;
    for (; (list_bucket_ < MT_HASHTABLE_PARTITIONS) && (OB_SUCC(ret)) && !terminate; ++list_bucket_) {
      ObProxyMutex *bucket_mutex = table_cache.lock_for_key(list_bucket_);
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (!lock_bucket.is_locked()) {
        DEBUG_ICMD("fail to try lock table_cache, schedule in again", K_(list_bucket), K(event_ret));
        terminate = true;
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("start traversing ObTableEntry", K_(list_bucket));
        if (OB_FAIL(table_cache.run_todo_list(list_bucket_))) {
          LOG_WDIAG("fail to run todo list", K(list_bucket_), K(ret));
        } else {
          entry = table_cache.first_entry(list_bucket_, it);
          while (NULL != entry && OB_SUCC(ret)) {
            if (common::match_like(entry->get_cluster_name(), entry_name_.cluster_name_)
                && common::match_like(entry->get_tenant_name(), entry_name_.tenant_name_)
                && common::match_like(entry->get_database_name(), entry_name_.database_name_)
                && common::match_like(entry->get_table_name(), entry_name_.table_name_)
                && OB_FAIL(dump_table_item(*entry))) {
              LOG_WDIAG("fail to dump_item", KPC(entry), K(ret));
            } else {
              entry = table_cache.next_entry(list_bucket_, it);
            }
          }//end of while
        }//end of else
      }//end of locked
    }//end of for list_bucket_

    if (!terminate && OB_SUCC(ret)) {
      DEBUG_ICMD("finish traversing all entry");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump table entry");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowRouteHandler::handle_show_partition(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump header", K(ret));
  } else {
    ObPartitionCache &partition_cache = get_global_partition_cache();
    bool terminate = false;
    ObPartitionEntry *entry = NULL;
    PartitionIter it;
    for (; (list_bucket_ < MT_HASHTABLE_PARTITIONS) && (OB_SUCC(ret)) && !terminate; ++list_bucket_) {
      ObProxyMutex *bucket_mutex = partition_cache.lock_for_key(list_bucket_);
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (!lock_bucket.is_locked()) {
        DEBUG_ICMD("fail to try lock cache, schedule in again", K_(list_bucket), K(event_ret));
        terminate = true;
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("start traversing ObPartitionEntry", K_(list_bucket));
        if (OB_FAIL(partition_cache.run_todo_list(list_bucket_))) {
          LOG_WDIAG("fail to run todo list", K(list_bucket_), K(ret));
        } else {
          entry = partition_cache.first_entry(list_bucket_, it);
          while (NULL != entry && OB_SUCC(ret)) {
            if (OB_FAIL(dump_partition_item(*entry))) {
              LOG_WDIAG("fail to dump_item", KPC(entry), K(ret));
            } else {
              entry = partition_cache.next_entry(list_bucket_, it);
            }
          }//end of while
        }//end of else
      }//end of locked
    }//end of for list_bucket_

    if (!terminate && OB_SUCC(ret)) {
      DEBUG_ICMD("finish traversing all entry");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump partition entry");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}


int ObShowRouteHandler::handle_show_routine(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump header", K(ret));
  } else if (OB_FAIL(fill_routine_entry_name())) {
    WDIAG_ICMD("fail to fill entry name", K(ret));
  } else {
    ObRoutineCache &routine_cache = get_global_routine_cache();
    bool terminate = false;
    ObRoutineEntry *entry = NULL;
    RoutineIter it;
    for (; (list_bucket_ < MT_HASHTABLE_PARTITIONS) && (OB_SUCC(ret)) && !terminate; ++list_bucket_) {
      ObProxyMutex *bucket_mutex = routine_cache.lock_for_key(list_bucket_);
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (!lock_bucket.is_locked()) {
        DEBUG_ICMD("fail to try lock cache, schedule in again", K_(list_bucket), K(event_ret));
        terminate = true;
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("start traversing ObRoutineEntry", K_(list_bucket));
        if (OB_FAIL(routine_cache.run_todo_list(list_bucket_))) {
          LOG_WDIAG("fail to run todo list", K(list_bucket_), K(ret));
        } else {
          entry = routine_cache.first_entry(list_bucket_, it);
          while (NULL != entry && OB_SUCC(ret)) {
            if (common::match_like(entry->get_cluster_name(), entry_name_.cluster_name_)
                && common::match_like(entry->get_tenant_name(), entry_name_.tenant_name_)
                && common::match_like(entry->get_database_name(), entry_name_.database_name_)
                && common::match_like(entry->get_package_name(), entry_name_.package_name_)
                && common::match_like(entry->get_routine_name(), entry_name_.table_name_)
                && OB_FAIL(dump_routine_item(*entry))) {
              LOG_WDIAG("fail to dump_item", KPC(entry), K(ret));
            } else {
              entry = routine_cache.next_entry(list_bucket_, it);
            }
          }//end of while
        }//end of else
      }//end of locked
    }//end of for list_bucket_

    if (!terminate && OB_SUCC(ret)) {
      DEBUG_ICMD("finish traversing all entry");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump routine entry");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowRouteHandler::handle_show_global_index(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump header", K(ret));
  } else {
    ObIndexCache &index_cache = get_global_index_cache();
    bool terminate = false;
    ObIndexEntry *entry = NULL;
    IndexIter it;
    for (; (list_bucket_ < MT_HASHTABLE_PARTITIONS) && (OB_SUCC(ret)) && !terminate; ++list_bucket_) {
      ObProxyMutex *bucket_mutex = index_cache.lock_for_key(list_bucket_);
      MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
      if (!lock_bucket.is_locked()) {
        DEBUG_ICMD("fail to try lock cache, schedule in again", K_(list_bucket), K(event_ret));
        terminate = true;
        if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          EDIAG_ICMD("fail to schedule self", K(ret));
        } else {
          event_ret = EVENT_CONT;
        }
      } else {
        DEBUG_ICMD("start traversing ObPartitionEntry", K_(list_bucket));
        if (OB_FAIL(index_cache.run_todo_list(list_bucket_))) {
          LOG_WDIAG("fail to run todo list", K(list_bucket_), K(ret));
        } else {
          entry = index_cache.first_entry(list_bucket_, it);
          while (NULL != entry && OB_SUCC(ret)) {
            if (OB_FAIL(dump_global_index_item(*entry))) {
              LOG_WDIAG("fail to dump_item", KPC(entry), K(ret));
            } else {
              entry = index_cache.next_entry(list_bucket_, it);
            }
          }//end of while
        }//end of else
      }//end of locked
    }//end of for list_bucket_

    if (!terminate && OB_SUCC(ret)) {
      DEBUG_ICMD("finish traversing all entry");
      if (OB_FAIL(encode_eof_packet())) {
        WDIAG_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump partition entry");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowRouteHandler::fill_table_entry_name()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_routine_entry_name())) {
  } else if (!entry_name_.package_name_.empty() && entry_name_.table_name_.empty()) {
    entry_name_.table_name_ = entry_name_.package_name_;
  }
  return ret;
}

int ObShowRouteHandler::fill_routine_entry_name()
{
  int ret = OB_SUCCESS;
  if (!entry_name_.is_valid()) {
    ObString name(value_str_);
    if (name.empty()) {//show ..
      entry_name_.shallow_copy(name, name, name, name, name);
    } else {
      ObString cluster_name = name.split_on(' ');
      if (cluster_name.empty()) {//show .. cname
        entry_name_.cluster_name_ = name;
        name.set_length(0);
        entry_name_.tenant_name_ = name;
        entry_name_.database_name_ = name;
        entry_name_.package_name_ = name;
        entry_name_.table_name_ = name;
      } else {
        entry_name_.cluster_name_ = cluster_name;
        ObString tenant_name = name.split_on(' ');
        if (tenant_name.empty()) {//show .. cname tname
          entry_name_.tenant_name_ = name;
          name.set_length(0);
          entry_name_.database_name_ = name;
          entry_name_.package_name_ = name;
          entry_name_.table_name_ = name;
        } else {
          entry_name_.tenant_name_ = tenant_name;
          ObString database_name = name.split_on(' ');
          if (database_name.empty()) {//show .. cname tname dname
            entry_name_.database_name_ = name;
            name.set_length(0);
            entry_name_.package_name_ = name;
            entry_name_.table_name_ = name;
          } else {//show .. cname tname dname tname
            entry_name_.database_name_ = database_name;
            ObString package_name = name.split_on(' ');
            if (package_name.empty()) {//show .. cname tname dname pname
              entry_name_.package_name_ = name;
              name.set_length(0);
              entry_name_.table_name_ = name;
            } else {//show .. cname tname dname pname tname
              entry_name_.package_name_ = package_name;
              entry_name_.table_name_ = name;
            }
          }
        }
      }
    }
    INFO_ICMD("succ to get entry name", K_(value_str), K_(entry_name));
  }
  return ret;
}

int ObShowRouteHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (header_encoded_) {
    DEBUG_ICMD("header is already encoded, skip this");
  } else {
    switch (sub_type_) {
      case OBPROXY_T_SUB_ROUTE_PARTITION: {
        if (OB_FAIL(encode_header(ROUTE_PARTITION_COLUMN_ARRAY, OB_RPC_MAX_ROUTE_COLUMN_ID))) {
          WDIAG_ICMD("fail to encode header", K(ret));
        }
        break;
      }
      case OBPROXY_T_SUB_ROUTE_ROUTINE: {
        if (OB_FAIL(encode_header(ROUTE_ROUTINE_COLUMN_ARRAY, OB_RRC_MAX_ROUTE_COLUMN_ID))) {
          WDIAG_ICMD("fail to encode header", K(ret));
        }
        break;
      }
      case OBPROXY_T_SUB_ROUTE_GLOBALINDEX: {
        if (OB_FAIL(encode_header(GLOBAL_INDEX_COLUMN_ARRAY, OB_GIC_MAX_GLOBAL_INDEX_COLUMN_ID))) {
          WDIAG_ICMD("fail to encode header", K(ret));
        }
        break;
      }
      default: {
        if (OB_FAIL(encode_header(ROUTE_COLUMN_ARRAY, OB_RC_MAX_ROUTE_COLUMN_ID))) {
          WDIAG_ICMD("fail to encode header", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      header_encoded_ = true;
    }
  }
  return ret;
}

int ObShowRouteHandler::dump_table_item(const ObTableEntry &entry)
{
  int ret = OB_SUCCESS;
  const int64_t count = entry.get_server_count();
  ObSqlString server_addr;
  ObRole role = INVALID_ROLE;
  ObReplicaType replica_type = REPLICA_TYPE_MAX;
  const ObTenantServer *ts = NULL;
  const ObProxyPartitionLocation *pl = NULL;
  ObProxyPartInfo *part_info = NULL;
  const bool is_dummy_entry = entry.is_dummy_entry();
  if (is_dummy_entry) {
    ts = entry.get_tenant_servers();
  } else if (entry.is_location_entry()) {
    pl = entry.get_first_pl();
  } else if (entry.is_part_info_entry()) {
    part_info = entry.get_part_info();
  }
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    char ip_port_buf[MAX_IP_ADDR_LENGTH];
    memset(ip_port_buf, 0, sizeof(ip_port_buf));
    if (NULL != ts) {
      role = ts->get_replica_location(i)->role_;
      replica_type = ts->get_replica_location(i)->replica_type_;
      ts->get_replica_location(i)->server_.ip_port_to_string(ip_port_buf, sizeof(ip_port_buf));
    } else if (NULL != pl) {
      role = pl->get_replica(i)->role_;
      replica_type = pl->get_replica(i)->replica_type_;
      pl->get_replica(i)->server_.ip_port_to_string(ip_port_buf, sizeof(ip_port_buf));
    } else {
      role = INVALID_ROLE;
      replica_type = REPLICA_TYPE_MAX;
    }
    const ObString &string = ObProxyReplicaLocation::get_replica_type_string(replica_type);
    if (OB_FAIL(server_addr.append_fmt("server[%ld]=%s,%s,%.*s; ",
                                        i,
                                        ip_port_buf,
                                        role2str(role),
                                        string.length(), string.ptr()))) {
      WDIAG_ICMD("fail to append server_addr", K(i), K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != part_info && part_info->is_valid()) {
    if (OB_FAIL(server_addr.append_fmt("part_level=%d, first_func=%s, "
                                       "first_space=%d, first_num=%ld, "
                                       "sub_func=%s, sub_space=%d, sub_num=%ld",
                                       part_info->get_part_level(),
                                       get_partition_func_type_str(part_info->get_first_part_option().part_func_type_),
                                       part_info->get_first_part_option().part_space_,
                                       part_info->get_first_part_option().part_num_,
                                       get_partition_func_type_str(part_info->get_sub_part_option().part_func_type_),
                                       part_info->get_sub_part_option().part_space_,
                                       part_info->get_sub_part_option().part_num_
                                       )
        )) {
      WDIAG_ICMD("fail to append server_addr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRow row;
    ObObj cells[OB_RC_MAX_ROUTE_COLUMN_ID];
    cells[OB_RC_CNAME].set_varchar(entry.get_cluster_name());
    cells[OB_RC_TENAME].set_varchar(entry.get_tenant_name());
    cells[OB_RC_DNAME].set_varchar(entry.get_database_name());
    cells[OB_RC_TABLE_NAME].set_varchar(entry.get_table_name());
    cells[OB_RC_STATE].set_varchar(entry.get_route_entry_state());
    cells[OB_RC_PART_NUM].set_int(entry.get_part_num());
    cells[OB_RC_REPLICA_NUM].set_int(entry.get_replica_num());
    cells[OB_RC_TABLE_ID].set_uint64(entry.get_table_id());
    cells[OB_RC_CR_VERSION].set_int(entry.get_cr_version());
    cells[OB_RC_SCHEMA_VERSION].set_int(entry.get_schema_version());
    cells[OB_RC_FROM_RSLIST].set_varchar(entry.is_entry_from_rslist() ? "Y" : "N");
    cells[OB_RC_SERVER_ADDR].set_varchar(server_addr.string());

    const uint32_t buf_len = 64;
    char create_timebuf[buf_len];
    char valid_timebuf[buf_len];
    char access_timebuf[buf_len];
    char update_timebuf[buf_len];
    char expire_timebuf[buf_len];
    char relative_expire_timebuf[buf_len];

    if (OB_FAIL(extract_entry_time(entry, create_timebuf, valid_timebuf, access_timebuf,
        update_timebuf, expire_timebuf, relative_expire_timebuf, buf_len))) {
      WDIAG_ICMD("fail to extract_entry_time", K(entry), K(ret));
    } else {
      cells[OB_RC_CREATE].set_varchar(create_timebuf);
      cells[OB_RC_LAST_VALID].set_varchar(valid_timebuf);
      cells[OB_RC_LAST_ACCESS].set_varchar(access_timebuf);
      cells[OB_RC_LAST_UPDATE].set_varchar(update_timebuf);
      cells[OB_RC_EXPIRE_TIME].set_varchar(expire_timebuf);
      cells[OB_RC_RELATIVE_EXPIRE_TIME].set_varchar(relative_expire_timebuf);

      row.cells_ = cells;
      row.count_ = OB_RC_MAX_ROUTE_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      } else {
        DEBUG_ICMD("succ to encode row packet", K(entry));
      }
    }
  }

  return ret;
}

int extract_entry_time(const ObRouteEntry &entry, char *create_timebuf, char *valid_timebuf,
                       char *access_timebuf, char *update_timebuf, char *expire_timebuf,
                       char *relative_expire_time, const uint32_t buf_len)
{
  int ret = OB_SUCCESS;
  struct tm struct_tm;
  MEMSET(&struct_tm, 0, sizeof(struct tm));
  size_t strftime_len = 0;
  time_t time_us = usec_to_sec(entry.get_create_time_us());
  if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
  } else {
    strftime_len = strftime(create_timebuf, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
    if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                K(create_timebuf), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    time_us = usec_to_sec(entry.get_last_valid_time_us());
    if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
    } else {
      strftime_len = strftime(valid_timebuf, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                  K(valid_timebuf), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    time_us = usec_to_sec(entry.get_last_access_time_us());
    if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
    } else {
      strftime_len = strftime(access_timebuf, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                  K(access_timebuf), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    time_us = usec_to_sec(entry.get_last_update_time_us());
    if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
    } else {
      strftime_len = strftime(update_timebuf, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                  K(update_timebuf), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    time_us = usec_to_sec(entry.get_time_for_expired());
    if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
    } else {
      strftime_len = strftime(expire_timebuf, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                  K(expire_timebuf), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    time_us = usec_to_sec(get_global_table_cache().get_cache_expire_time_us());
    if (OB_ISNULL(localtime_r(&time_us, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(time_us), K(ret));
    } else {
      strftime_len = strftime(relative_expire_time, buf_len, "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= buf_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", buf_len,
                  K(expire_timebuf), K(ret));
      }
    }
  }
  return ret;
}

int ObShowRouteHandler::dump_partition_item(const ObPartitionEntry &entry)
{
  int ret = OB_SUCCESS;
  const int64_t count = entry.get_server_count();
  ObSqlString server_addr;
  ObRole role = INVALID_ROLE;
  ObReplicaType replica_type = REPLICA_TYPE_MAX;
  const ObProxyPartitionLocation &pl = entry.get_pl();
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    role = pl.get_replica(i)->role_;
    replica_type = pl.get_replica(i)->replica_type_;
    const ObString &string = ObProxyReplicaLocation::get_replica_type_string(replica_type);
    char ip_port_buf[MAX_IP_ADDR_LENGTH];
    pl.get_replica(i)->server_.ip_port_to_string(ip_port_buf, sizeof(ip_port_buf));
    if (OB_FAIL(server_addr.append_fmt("server[%ld]=%s,%s,%.*s; ",
                                        i,
                                        ip_port_buf,
                                        role2str(role),
                                        string.length(), string.ptr()))) {
      WDIAG_ICMD("fail to append server_addr", K(i), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObNewRow row;
    ObObj cells[OB_RPC_MAX_ROUTE_COLUMN_ID];
    cells[OB_RPC_TABLE_ID].set_uint64(entry.get_table_id());
    cells[OB_RPC_PARTITION_ID].set_uint64(entry.get_partition_id());
    cells[OB_RPC_STATE].set_varchar(entry.get_route_entry_state());
    cells[OB_RPC_CR_VERSION].set_int(entry.get_cr_version());
    cells[OB_RPC_SCHEMA_VERSION].set_int(entry.get_schema_version());
    cells[OB_RPC_SERVER_ADDR].set_varchar(server_addr.string());

    const uint32_t buf_len = 64;
    char create_timebuf[buf_len];
    char valid_timebuf[buf_len];
    char access_timebuf[buf_len];
    char update_timebuf[buf_len];
    char expire_timebuf[buf_len];
    char relative_expire_timebuf[buf_len];

    if (OB_FAIL(extract_entry_time(entry, create_timebuf, valid_timebuf, access_timebuf,
        update_timebuf, expire_timebuf, relative_expire_timebuf, buf_len))) {
      WDIAG_ICMD("fail to extract_entry_time", K(entry), K(ret));
    } else {
      cells[OB_RPC_CREATE].set_varchar(create_timebuf);
      cells[OB_RPC_LAST_VALID].set_varchar(valid_timebuf);
      cells[OB_RPC_LAST_ACCESS].set_varchar(access_timebuf);
      cells[OB_RPC_LAST_UPDATE].set_varchar(update_timebuf);
      cells[OB_RPC_EXPIRE_TIME].set_varchar(expire_timebuf);
      cells[OB_RPC_RELATIVE_EXPIRE_TIME].set_varchar(relative_expire_timebuf);

      row.cells_ = cells;
      row.count_ = OB_RPC_MAX_ROUTE_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      } else {
        DEBUG_ICMD("succ to encode row packet", K(entry));
      }
    }
  }

  return ret;
}

int ObShowRouteHandler::dump_global_index_item(const ObIndexEntry &entry)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    ObNewRow row;
    ObObj cells[OB_GIC_MAX_GLOBAL_INDEX_COLUMN_ID];
    cells[OB_GIC_TABLE_ID].set_uint64(entry.get_table_id());
    cells[OB_GIC_DATA_TABLE_ID].set_uint64(entry.get_data_table_id());
    cells[OB_GIC_INDEX_TABLE_NAME].set_varchar(entry.get_index_table_name());
    cells[OB_GIC_STATE].set_varchar(entry.get_route_entry_state());
    cells[OB_GIC_CR_VERSION].set_int(entry.get_cr_version());
    cells[OB_GIC_SCHEMA_VERSION].set_int(entry.get_schema_version());

    const uint32_t buf_len = 64;
    char create_timebuf[buf_len];
    char valid_timebuf[buf_len];
    char access_timebuf[buf_len];
    char update_timebuf[buf_len];
    char expire_timebuf[buf_len];
    char relative_expire_timebuf[buf_len];

    if (OB_FAIL(extract_entry_time(entry, create_timebuf, valid_timebuf, access_timebuf,
        update_timebuf, expire_timebuf, relative_expire_timebuf, buf_len))) {
      WDIAG_ICMD("fail to extract_entry_time", K(entry), K(ret));
    } else {
      cells[OB_GIC_CREATE].set_varchar(create_timebuf);
      cells[OB_GIC_LAST_VALID].set_varchar(valid_timebuf);
      cells[OB_GIC_LAST_ACCESS].set_varchar(access_timebuf);
      cells[OB_GIC_LAST_UPDATE].set_varchar(update_timebuf);
      cells[OB_GIC_EXPIRE_TIME].set_varchar(expire_timebuf);
      cells[OB_GIC_RELATIVE_EXPIRE_TIME].set_varchar(relative_expire_timebuf);

      row.cells_ = cells;
      row.count_ = OB_GIC_MAX_GLOBAL_INDEX_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      } else {
        DEBUG_ICMD("succ to encode row packet", K(entry));
      }
    }
  }

  return ret;
}

int ObShowRouteHandler::dump_routine_item(const ObRoutineEntry &entry)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_RRC_MAX_ROUTE_COLUMN_ID];
  cells[OB_RRC_CNAME].set_varchar(entry.get_cluster_name());
  cells[OB_RRC_TENAME].set_varchar(entry.get_tenant_name());
  cells[OB_RRC_DNAME].set_varchar(entry.get_database_name());
  cells[OB_RRC_PNAME].set_varchar(entry.get_package_name());
  cells[OB_RRC_RNAME].set_varchar(entry.get_routine_name());
  cells[OB_RRC_STATE].set_varchar(entry.get_route_entry_state());
  cells[OB_RRC_ROUTINE_TYPE].set_int(entry.get_routine_type());
  cells[OB_RRC_ROUTINE_ID].set_uint64(entry.get_routine_id());
  cells[OB_RRC_CR_VERSION].set_int(entry.get_cr_version());
  cells[OB_RRC_SCHEMA_VERSION].set_int(entry.get_schema_version());
  cells[OB_RRC_ROUTE_SQL].set_varchar(entry.get_route_sql());

  const uint32_t buf_len = 64;
  char create_timebuf[buf_len];
  char valid_timebuf[buf_len];
  char access_timebuf[buf_len];
  char update_timebuf[buf_len];
  char expire_timebuf[buf_len];
  char relative_expire_timebuf[buf_len];

  if (OB_FAIL(extract_entry_time(entry, create_timebuf, valid_timebuf, access_timebuf,
      update_timebuf, expire_timebuf, relative_expire_timebuf, buf_len))) {
    WDIAG_ICMD("fail to extract_entry_time", K(entry), K(ret));
  } else {
    cells[OB_RRC_CREATE].set_varchar(create_timebuf);
    cells[OB_RRC_LAST_VALID].set_varchar(valid_timebuf);
    cells[OB_RRC_LAST_ACCESS].set_varchar(access_timebuf);
    cells[OB_RRC_LAST_UPDATE].set_varchar(update_timebuf);
    cells[OB_RRC_EXPIRE_TIME].set_varchar(expire_timebuf);
    cells[OB_RRC_RELATIVE_EXPIRE_TIME].set_varchar(relative_expire_timebuf);

    row.cells_ = cells;
    row.count_ = OB_RRC_MAX_ROUTE_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    } else {
      DEBUG_ICMD("succ to encode row packet", K(entry));
    }
  }
  return ret;
}

static int show_route_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowRouteHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowRouteHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowJsonConfigHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowRouteHandler", K(ret));
  } else {
    if (OBPROXY_T_SUB_ROUTE_PARTITION == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowRouteHandler::handle_show_partition);
    } else if (OBPROXY_T_SUB_ROUTE_ROUTINE == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowRouteHandler::handle_show_routine);
    } else if (OBPROXY_T_SUB_ROUTE_GLOBALINDEX == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowRouteHandler::handle_show_global_index);
    } else {
      SET_CONTINUATION_HANDLER(handler, &ObShowRouteHandler::handle_show_table);
    }
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowRouteHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowRouteHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_route_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_ROUTE,
                                                               &show_route_cmd_callback))) {
    WDIAG_ICMD("fail to register_cmd CMD_TYPE_ROUTE", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase



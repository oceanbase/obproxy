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

#include "cmd/ob_show_memory_handler.h"
#include <malloc.h>
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "cmd/ob_show_sqlaudit_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
//ModMemoryColumn
enum
{
  OB_MMC_MODE_NAME = 0,
  OB_MMC_MOD_TYPE,
  OB_MMC_HOLD,
  OB_MMC_USED,
  OB_MMC_COUNT,
  OB_MMC_AVG_USED,
  OB_MMC_MAX_MOD_MEMORY_COLUMN_ID,
};

//ObjPoolModMemoryColumn
enum
{
  OB_OPC_FREE_LIST_NAME = 0,
  OB_OPC_ALLOCATED,
  OB_OPC_IN_USE,
  OB_OPC_COUNT,
  OB_OPC_TYPE_SIZE,
  OB_OPC_CHUNK_COUNT,
  OB_OPC_CHUNK_BYTE_SIZE,
  OB_OPC_MAX_OBJPOOL_COLUMN_ID,
};

const ObProxyColumnSchema MOD_MEMORY_COLUMN_ARRAY[OB_MMC_MAX_MOD_MEMORY_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_MMC_MODE_NAME,  "mod_name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_MMC_MOD_TYPE,   "mod_type",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_MMC_HOLD,       "hold",       obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_MMC_USED,       "used",       obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_MMC_COUNT,      "count",      obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_MMC_AVG_USED,   "avg_used",   obmysql::OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema OBJPOOL_COLUMN_ARRAY[OB_OPC_MAX_OBJPOOL_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_OPC_FREE_LIST_NAME,   "free_list_name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_ALLOCATED,        "allocated",        obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_IN_USE,           "in_use",           obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_COUNT,            "count",            obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_TYPE_SIZE,        "type_size",        obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_CHUNK_COUNT,      "chunk_count",      obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_OPC_CHUNK_BYTE_SIZE,  "chunk_byte_size",  obmysql::OB_MYSQL_TYPE_VARCHAR),
};

ObShowMemoryHandler::ObShowMemoryHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                                         const ObInternalCmdInfo &info)
  :  ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type())
{
}

int ObShowMemoryHandler::handle_show_memory(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_mod_memory_header())) {
    WARN_ICMD("fail to dump_header", K(ret));
  } else {
    const ObModSet &mod_set = get_global_mod_set();
    ObModItem mod_item;
    ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ob_malloc_allocator is null", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ObModSet::MOD_COUNT_LIMIT; ++i) {
      allocator->get_tenant_mod_usage(OB_SERVER_TENANT_ID, static_cast<int32_t>(i), mod_item);
      if (mod_item.hold_ > 0) {
        if (OB_FAIL(dump_mod_memory(mod_set.get_mod_name(i), is_allocator_mod(i) ? "allocator" : "user",
                                    mod_item.hold_, mod_item.used_, mod_item.count_))) {
          WARN_ICMD("fail to dump mod memory", K(mod_set.get_mod_name(i)), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      // dump memory allocated by glibc
      struct mallinfo2 mi = mallinfo2();
      int64_t allocated = mi.arena + mi.hblkhd;
      int64_t used = allocated - mi.fordblks;
      if (OB_FAIL(dump_mod_memory("GLIBC", "user", allocated, used, mi.hblks))) {
        LOG_WARN("fail to dump memory info", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // dump sqlaudit memory
      ObSqlauditRecordQueue *sqlaudit_record_queue_ = NULL;
      int64_t hold = 0;
      int64_t count = 0;

      if (NULL != (sqlaudit_record_queue_ = get_global_sqlaudit_processor().acquire())) {
        hold = sqlaudit_record_queue_->get_current_memory_size();
        sqlaudit_record_queue_->refcount_dec();
        sqlaudit_record_queue_ = NULL;
        count = 1;
      }

      if (OB_FAIL(dump_mod_memory("OB_SQL_AUDIT", "user", hold, hold, count))) {
        LOG_WARN("fail to dump memory info", K(ret));
      }

      if (OB_SUCC(ret)) {
        if (0 != (hold = get_global_sqlaudit_processor().get_last_record_queue_memory_size())) {
          count = 1;
        } else {
          count = 0;
        }

        if (OB_FAIL(dump_mod_memory("OB_SQL_AUDIT_LAST", "user", hold, hold, count))) {
          LOG_WARN("fail to dump memory info", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump memory info");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowMemoryHandler::dump_mod_memory_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(MOD_MEMORY_COLUMN_ARRAY, OB_MMC_MAX_MOD_MEMORY_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowMemoryHandler::format_int_to_str(const int64_t value, ObSqlString &string)
{
  return string.append_fmt("% 'ld", value);
}

int ObShowMemoryHandler::dump_mod_memory(const char *name, const char *type, const int64_t hold,
                                         const int64_t used, const int64_t count)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSqlString value;
  ObObj cells[OB_MMC_MAX_MOD_MEMORY_COLUMN_ID];
  cells[OB_MMC_MODE_NAME].set_varchar(name);
  cells[OB_MMC_MOD_TYPE].set_varchar(type);
  if (OB_FAIL(format_int_to_str(hold, value))) {
    LOG_WARN("fail to format_int_to_str", K(ret));
  } else {
    cells[OB_MMC_HOLD].set_varchar(value.ptr() + pos);
    pos = value.length();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(used, value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_MMC_USED].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(count, value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_MMC_COUNT].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str((0 == count) ? 0 : used / count, value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_MMC_AVG_USED].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }

  if (OB_SUCC(ret)) {
    ObNewRow row;
    row.cells_ = cells;
    row.count_ = OB_MMC_MAX_MOD_MEMORY_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowMemoryHandler::handle_show_objpool(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_objpool_header())) {
    WARN_ICMD("fail to dump_header", K(ret));
  } else {
    ObVector<common::ObObjFreeList *> fll;
    if (OB_FAIL(ObObjFreeListList::get_freelists().get_info(fll))) {
      LOG_WARN("fail to get free list info", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < fll.size(); ++i) {
        if (match_like(fll[i]->get_name(), like_name_)
            && OB_FAIL(dump_objpool_memory(fll[i]))) {
         WARN_ICMD("fail to dump objpool memory", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump objpool info");
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  UNUSED(event_ret);
  return ret;
}

int ObShowMemoryHandler::dump_objpool_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(OBJPOOL_COLUMN_ARRAY, OB_OPC_MAX_OBJPOOL_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowMemoryHandler::dump_objpool_memory(const ObObjFreeList *fl)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSqlString value;
  ObObj cells[OB_OPC_MAX_OBJPOOL_COLUMN_ID];
  cells[OB_OPC_FREE_LIST_NAME].set_varchar(fl->get_name());
  if (OB_FAIL(format_int_to_str((fl->get_allocated() - fl->get_allocated_base()) * fl->get_type_size(), value))) {
    LOG_WARN("fail to format_int_to_str", K(ret));
  } else {
    cells[OB_OPC_ALLOCATED].set_varchar(value.ptr() + pos);
    pos = value.length();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str((fl->get_used() - fl->get_used_base()) * fl->get_type_size(), value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_OPC_IN_USE].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(fl->get_used() - fl->get_used_base(), value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_OPC_COUNT].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(fl->get_type_size(), value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_OPC_TYPE_SIZE].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(fl->get_chunk_count(), value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_OPC_CHUNK_COUNT].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(format_int_to_str(fl->get_chunk_byte_size(), value))) {
      LOG_WARN("fail to format_int_to_str", K(ret));
    } else {
      cells[OB_OPC_CHUNK_BYTE_SIZE].set_varchar(value.ptr() + pos);
      pos = value.length();
    }
  }

  if (OB_SUCC(ret)) {
    ObNewRow row;
    row.cells_ = cells;
    row.count_ = OB_OPC_MAX_OBJPOOL_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

static int show_memory_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowMemoryHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowMemoryHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowMemoryHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowMemoryHandler");
  } else {
    if (OBPROXY_T_SUB_MEMORY_OBJPOOL == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowMemoryHandler::handle_show_objpool);
    } else {
      SET_CONTINUATION_HANDLER(handler, &ObShowMemoryHandler::handle_show_memory);
    }
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowMemoryHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowMemoryHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_memory_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_MEMORY,
                                                               &show_memory_cmd_callback))) {
    WARN_ICMD("fail to proxy_memory_stat_callback", K(ret));
  }
  return ret;
}


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

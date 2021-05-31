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

#include "cmd/ob_show_stat_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
//StatColumnID
enum
{
  OB_SC_STAT_NAME = 0,
  OB_SC_VALUE,
  OB_SC_PERSIST_TYPE,
  OB_SC_MAX_STAT_COLUMN_ID,
};

const ObProxyColumnSchema STAT_COLUMN_ARRAY[OB_SC_MAX_STAT_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_SC_STAT_NAME,     "stat_name",    OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_SC_VALUE,         "value",        OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_SC_PERSIST_TYPE,  "persist_type", OB_MYSQL_TYPE_VARCHAR),
};

ObShowStatHandler::ObShowStatHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                                     const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info),
    sub_type_(info.get_sub_cmd_type())
{
  SET_HANDLER(&ObShowStatHandler::handle_show_stat);
}

int ObShowStatHandler::handle_show_stat(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_stat_header())) {
    WARN_ICMD("fail to dump_header", K(ret));
  } else if (OBPROXY_T_SUB_STAT_REFRESH == sub_type_ && OB_FAIL(g_stat_processor.exec_raw_stat_sync_cbs())) {
    WARN_ICMD("fail to update all stat records", K(ret), K(sub_type_));
  } else {
    const ObRecRecord *record = g_stat_processor.get_all_records_head();
    while (OB_SUCC(ret) && NULL != record) {
      if (match_like(record->name_, like_name_)) {
        DEBUG_ICMD("stat name matched", K_(like_name), K(record->name_), K(sub_type_));
        if (OB_FAIL(dump_stat_item(record))) {
          WARN_ICMD("fail to dump stat item", K(ret), K(record));
        }
      }
      record = g_stat_processor.get_all_records_next(const_cast<ObRecRecord *>(record));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump stat", K_(like_name), K_(sub_type));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowStatHandler::dump_stat_item(const obproxy::ObRecRecord *record)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_SC_MAX_STAT_COLUMN_ID];
  int64_t value = (RECD_FLOAT != record->data_type_) ? record->data_.rec_int_
                                                     : static_cast<int64_t>(record->data_.rec_float_);
  cells[OB_SC_STAT_NAME].set_varchar(record->name_);
  cells[OB_SC_VALUE].set_int(value);
  cells[OB_SC_PERSIST_TYPE].set_varchar(get_persist_type_str(record->stat_meta_.persist_type_));

  row.cells_ = cells;
  row.count_ = OB_SC_MAX_STAT_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowStatHandler::dump_stat_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(STAT_COLUMN_ARRAY, OB_SC_MAX_STAT_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

const ObString ObShowStatHandler::get_persist_type_str(const ObRecPersistType type) const
{
  ObString ret;
  switch (type) {
    case RECP_NULL: {
      ret = ObString::make_string("NULL");
      break;
    }
    case RECP_PERSISTENT: {
      ret = ObString::make_string("PERSISTENT");
      break;
    }
    default: {
      WARN_ICMD("it should not happened", K(type));
      break;
    }
  }
  return ret;
}

static int show_stat_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowStatHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowStatHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowStatHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowStatHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowStatHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowStatHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_stat_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_STAT,
                                                               &show_stat_cmd_callback))) {
    WARN_ICMD("fail to register CMD_TYPE_PROXYSTAT", K(ret));
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

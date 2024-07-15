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

#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "cmd/ob_show_resource_handler.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

// ClusterColumn
enum
{
  OB_CR_NAME = 0,
  OB_CR_ID,
  OB_CR_STATE,
  OB_CR_VERSION,
  OB_CR_REF_COUNT,
  OB_CR_LAST_ACCESS_TIME_NS,
  OB_CR_LAST_ACCESS_TIME_STR,
  OB_CR_DELETING_COMPLETE_THREAD_NUM,
  OB_CR_FETCH_RS_LIST_TASK_COUNT,
  OB_CR_LAST_FETCH_RS_LIST_TIME_NS,
  OB_CR_LAST_FETCH_RS_LIST_TIME_STR,
  OB_CR_FETCH_IDC_LIST_TASK_COUNT,
  OB_CR_LAST_FETCH_IDC_LIST_TIME_NS,
  OB_CR_LAST_FETCH_IDC_LIST_TIME_STR,
  OB_CR_MAX_RESOURCE_COLUMN_ID,
};

const ObProxyColumnSchema RESOURCE_COLUMN_ARRAY[OB_CR_MAX_RESOURCE_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_CR_NAME,                "cluster_name",        obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_CR_ID,                  "cluster_id",          obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_STATE,               "state",               obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_CR_VERSION,             "version",             obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_REF_COUNT,           "ref_count",           obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_ACCESS_TIME_NS, "last_access_time_ns", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_ACCESS_TIME_STR, "last_access_time",   obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_CR_DELETING_COMPLETE_THREAD_NUM, "deleting_completed_thread_num", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_FETCH_RS_LIST_TASK_COUNT, "fetch_rs_list_task_count", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_FETCH_RS_LIST_TIME_NS, "last_fetch_rs_list_time_ns", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_FETCH_RS_LIST_TIME_STR, "last_fetch_rs_list_time", obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_CR_FETCH_IDC_LIST_TASK_COUNT, "fetch_idc_list_task_count", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_FETCH_IDC_LIST_TIME_NS, "last_fetch_idc_list_time_ns", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_CR_LAST_FETCH_IDC_LIST_TIME_STR, "last_fetch_idc_list_time", obmysql::OB_MYSQL_TYPE_VARCHAR),
};

ObShowResourceHandler::ObShowResourceHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info)
{
  SET_HANDLER(&ObShowResourceHandler::handle_show_resource);
}

int ObShowResourceHandler::dump_resource_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(RESOURCE_COLUMN_ARRAY, OB_CR_MAX_RESOURCE_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowResourceHandler::handle_show_resource(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObSEArray<ObClusterResource *, 32> cr_array;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_resource_header())) {
    WDIAG_ICMD("fail to dump resource info header", K(ret));
  } else if (OB_FAIL(get_global_resource_pool_processor().acquire_all_cluster_resource(cr_array))) {
    WDIAG_ICMD("fail to acquire all cluster resource from resource_pool", K(ret));
  } else {
    ObClusterResource *cr = NULL;
    for (int64_t i = 0; (i < cr_array.count()) && OB_SUCC(ret); ++i) {
      cr = cr_array.at(i);
      if (common::match_like(cr->get_cluster_name().ptr(), like_name_)) {
        if (OB_FAIL(dump_resource_item(cr))) {
          LOG_WDIAG("fail to dump cluster resource", KPC(cr), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump cluster resource", K_(like_name));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  for (int64_t i = 0; i < cr_array.count(); ++i) {
    cr_array.at(i)->dec_ref();
  }
  cr_array.reset();

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowResourceHandler::dump_resource_item(const ObClusterResource *cr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != cr)) {
    char access_timebuf[64] = {'\0'};
    char rs_timebuf[64] = {'\0'};
    char idc_timebuf[64] = {'\0'};
    time_t t = 0;
    struct tm struct_tm;
    MEMSET(&struct_tm, 0, sizeof(struct tm));
    ObNewRow row;
    ObObj cells[OB_CR_MAX_RESOURCE_COLUMN_ID];
    cells[OB_CR_NAME].set_varchar(cr->get_cluster_name());
    cells[OB_CR_ID].set_int(cr->get_cluster_id());
    cells[OB_CR_STATE].set_varchar(cr->get_cr_state_str());
    cells[OB_CR_VERSION].set_int(cr->version_);
    cells[OB_CR_REF_COUNT].set_int(cr->ref_count_);
    cells[OB_CR_LAST_ACCESS_TIME_NS].set_int(cr->last_access_time_ns_);
    cells[OB_CR_LAST_FETCH_IDC_LIST_TIME_NS].set_int(cr->last_idc_list_refresh_time_ns_);
    cells[OB_CR_LAST_FETCH_RS_LIST_TIME_NS].set_int(cr->last_rslist_refresh_time_ns_);
    cells[OB_CR_DELETING_COMPLETE_THREAD_NUM].set_int(cr->deleting_completed_thread_num_);
    cells[OB_CR_FETCH_RS_LIST_TASK_COUNT].set_int(cr->fetch_rslist_task_count_);
    cells[OB_CR_FETCH_IDC_LIST_TASK_COUNT].set_int(cr->fetch_idc_list_task_count_);

    t = hrtime_to_sec(cr->last_access_time_ns_); // to second
    if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
    } else {
      int64_t strftime_len = strftime(access_timebuf, sizeof(access_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(access_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("timebuf is not enough", K(strftime_len), "timebuf length",
                  sizeof(access_timebuf), K(access_timebuf), K(ret));
      } else {
        cells[OB_CR_LAST_ACCESS_TIME_STR].set_varchar(access_timebuf);
      }
    }

    t = hrtime_to_sec(cr->last_rslist_refresh_time_ns_); // to second
    if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
    } else {
      int64_t strftime_len = strftime(rs_timebuf, sizeof(rs_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(rs_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("rs_timebuf is not enough", K(strftime_len), "rs_timebuf length",
                  sizeof(rs_timebuf), K(rs_timebuf), K(ret));
      } else {
        cells[OB_CR_LAST_FETCH_RS_LIST_TIME_STR].set_varchar(rs_timebuf);
      }
    }

    t = hrtime_to_sec(cr->last_idc_list_refresh_time_ns_); // to second
    if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
      ret = OB_ERR_UNEXPECTED;
      WDIAG_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
    } else {
      int64_t strftime_len = strftime(idc_timebuf, sizeof(idc_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(idc_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WDIAG_ICMD("idc_timebuf is not enough", K(strftime_len), "idc_timebuf length",
                  sizeof(idc_timebuf), K(idc_timebuf), K(ret));
      } else {
        cells[OB_CR_LAST_FETCH_IDC_LIST_TIME_STR].set_varchar(idc_timebuf);
      }
    }

    if (OB_SUCC(ret)) {
      row.cells_ = cells;
      row.count_ = OB_CR_MAX_RESOURCE_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }

  return ret;
}

static int show_resource_cmd_callback(
    ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowResourceHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowResourceHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowResourceHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowResourceHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowResourceHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowResourceHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_resource_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_RESOURCE,
                                                               &show_resource_cmd_callback))) {
    WDIAG_ICMD("fail to proxy_resource_callback", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

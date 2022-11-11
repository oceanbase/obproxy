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
#include "cmd/ob_show_congestion_handler.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

//CongestionColumnID
enum
{
  OB_CC_CLUSTER_NAME = 0,
  OB_CC_ZONE_NAME,
  OB_CC_REGION_NAME,
  OB_CC_ZONE_STATE,
  OB_CC_SERVER_IP,
  OB_CC_CR_VERSION,
  OB_CC_SERVER_STATE,
  OB_CC_ALIVE_CONGESTED,
  OB_CC_LAST_ALIVE_CONGESTED,
  OB_CC_DEAD_CONGESTED,
  OB_CC_LAST_DEAD_CONGESTED,
  OB_CC_STAT_ALIVE_FAILURES,
  OB_CC_STAT_CONN_FAILURES,
  OB_CC_CONN_LAST_FAIL_TIME,
  OB_CC_CONN_FAILURE_EVENTS,
  OB_CC_ALIVE_LAST_FAIL_TIME,
  OB_CC_ALIVE_FAILURE_EVENTS,
  OB_CC_REF_COUNT,
  OB_CC_DETECT_CONGESTED,
  OB_CC_LAST_DETECT_CONGESTED,
  OB_CC_MAX_CONGESTION_COLUMN_ID,
};

const ObProxyColumnSchema CONGESTION_COLUMN_ARRAY[OB_CC_MAX_CONGESTION_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CC_CLUSTER_NAME,          "cluster_name",         obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ZONE_NAME,             "zone_name",            obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_REGION_NAME,           "region_name",          obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ZONE_STATE,            "zone_state",           obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_SERVER_IP,             "server_ip",            obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_CR_VERSION,            "cr_version",           obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_SERVER_STATE,          "server_state",         obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ALIVE_CONGESTED,       "alive_congested",      obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_LAST_ALIVE_CONGESTED,  "last_alive_congested", obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_DEAD_CONGESTED,        "dead_congested",       obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_LAST_DEAD_CONGESTED,   "last_dead_congested",  obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_STAT_ALIVE_FAILURES,   "stat_alive_failures",  obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_STAT_CONN_FAILURES,    "stat_conn_failures",   obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_CONN_LAST_FAIL_TIME,   "conn_last_fail_time",  obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_CONN_FAILURE_EVENTS,   "conn_failure_events",  obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_ALIVE_LAST_FAIL_TIME,  "alive_last_fail_time", obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ALIVE_FAILURE_EVENTS,  "alive_failure_events", obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_REF_COUNT,             "ref_count",            obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_DETECT_CONGESTED,      "detect_congested",     obmysql::OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_LAST_DETECT_CONGESTED, "last_detect_congested",obmysql::OB_MYSQL_TYPE_VARCHAR),
};


ObShowCongestionHandler::ObShowCongestionHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info),
    sub_type_(info.get_sub_cmd_type()), idx_(0), list_bucket_(0)
{
  SET_HANDLER(&ObShowCongestionHandler::handle_congestion);
  if (!info.get_cluster_string().empty()) {
    int32_t min_len =std::min(info.get_cluster_string().length(), static_cast<int32_t>(OB_PROXY_MAX_CLUSTER_NAME_LENGTH));
    MEMCPY(cluster_str_, info.get_cluster_string().ptr(), min_len);
    cluster_str_[min_len] = '\0';
  } else {
    cluster_str_[0] = '\0';
  }
  cr_array_.reset();
}

void ObShowCongestionHandler::destroy()
{
  ObClusterResource *cr = NULL;
  for (int64_t i = 0; i < cr_array_.count(); ++i) {
    cr = cr_array_.at(i);
    if (NULL != cr) {
      get_global_resource_pool_processor().release_cluster_resource(cr);
      cr = NULL;
    }
  }
  cr_array_.reset();
}

int ObShowCongestionHandler::handle_congestion(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_NONE;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WARN_ICMD("fail to dump congestion header", K(ret));
  } else if (OB_FAIL(fill_cluster_resource_array())) {
    WARN_ICMD("fail to fill cluster resource array", K(ret));
  } else {
    bool terminate = false;
    ObClusterResource *cluster_resource = NULL;
    ObCongestionManager *cm = NULL;
    ObCongestionEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = NULL;
    Iter it;
    for (; !terminate && OB_SUCC(ret) && idx_ < cr_array_.count(); ++idx_, list_bucket_ = 0) {
      cluster_resource = cr_array_.at(idx_);
      if (OB_ISNULL(cluster_resource)) {
        WARN_ICMD("cluster resource is null", K_(idx), "count", cr_array_.count());
        terminate = true;
      } else {
        DEBUG_ICMD("start traversing cluster", K_(idx), "cluster_name", cluster_resource->get_cluster_name());
        cm = &(cluster_resource->congestion_manager_);
        for (; !terminate && OB_SUCC(ret) && (list_bucket_ < cm->get_sub_part_count()); ++list_bucket_) {
          bucket_mutex = cm->lock_for_key(list_bucket_);
          MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, ethread);
          if (!lock_bucket.is_locked()) {
            DEBUG_ICMD("fail to try lock congestion_manager, schedule in again", K_(list_bucket), K(event_ret));
            terminate = true;
            if (OB_ISNULL(g_event_processor.schedule_in(this, MYSQL_LIST_RETRY, ET_TASK))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              ERROR_ICMD("fail to schedule self", K(ret));
            } else {
              event_ret = EVENT_CONT;
            }
          } else {
            DEBUG_ICMD("start traversing ObCongestionEntry", K_(list_bucket));
            if (OB_FAIL(cm->run_todo_list(list_bucket_))) {
              WARN_ICMD("fail to run_todo_list", K(ret));
            } else {
              entry = cm->first_entry(list_bucket_, it);
              while (OB_SUCC(ret) && NULL != entry) {
                if (OBPROXY_T_SUB_CONGEST_ALL == sub_type_ || entry->is_congested()) {
                  if (OB_FAIL(dump_item(entry, cluster_resource->get_cluster_name()))) {
                    WARN_ICMD("fail to dump ObCongestionEntry", K(list_bucket_));
                  } else {
                    DEBUG_ICMD("succ to dump ObCongestionEntry", "cluster name",
                               cluster_resource->get_cluster_name(), K(idx_), K(list_bucket_));
                  }
                }
                entry = cm->next_entry(list_bucket_, it);
              }//end of while
            }//end of else
          }//end of locked
        }//end of for list_bucket_
      }//end of NULL != cluster_resource
    }//end of for idx_

    if (!terminate && idx_ >= cr_array_.count()) {
      DEBUG_ICMD("finish traversing all cluster");
      if (OB_FAIL(encode_eof_packet())) {
        WARN_ICMD("fail to encode eof packet", K(ret));
      } else {
        INFO_ICMD("succ to dump congestion");
        event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
      }
    } else if (terminate && OB_ISNULL(cluster_resource)) {
      INFO_ICMD("can not find dictated cluster");
      event_ret = internal_error_callback(OB_ENTRY_NOT_EXIST);
    }
  }//end of handle body

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowCongestionHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (header_encoded_) {
    DEBUG_ICMD("header is already encoded, skip this");
  } else {
    if (OB_FAIL(encode_header(CONGESTION_COLUMN_ARRAY, OB_CC_MAX_CONGESTION_COLUMN_ID))) {
      WARN_ICMD("fail to encode header", K(ret));
    } else {
      header_encoded_ = true;
    }
  }
  return ret;
}

int ObShowCongestionHandler::fill_cluster_resource_array()
{
  int ret = OB_SUCCESS;
  if (!cr_array_.empty()) {
    DEBUG_ICMD("cluster_resource_array_ is already filled, skip this");
  } else {
    ObString cluster_name(cluster_str_);
    if (!cluster_name.empty()) {
      ObClusterResource *cr = NULL;
      cr = get_global_resource_pool_processor().acquire_avail_cluster_resource(cluster_name);
      if (NULL != cr) {
        if (OB_FAIL(cr_array_.push_back(cr))) {
          WARN_ICMD("fail to push back cr_array_", K(cluster_name), K(cr), K(ret));
          get_global_resource_pool_processor().release_cluster_resource(cr);
          cr = NULL;
        }
      }
    } else {
      if (OB_FAIL(get_global_resource_pool_processor().acquire_all_avail_cluster_resource(cr_array_))) {
        WARN_ICMD("fail to acquire all cluster resource from resource_pool", K(ret));
      }
    }
  }
  return ret;
}

int ObShowCongestionHandler::dump_item(const ObCongestionEntry *entry, const ObString cluster_name)
{
  int ret = OB_SUCCESS;
  char alive_timebuf[64];
  char dead_timebuf[64];
  char detect_timebuf[64];
  char conn_last_fail[64];
  char alive_last_fail[64];
  ip_port_text_buffer server_ip;
  time_t t = 0;
  struct tm struct_tm;
  MEMSET(&struct_tm, 0, sizeof(struct tm));
  size_t strftime_len = 0;

  if (OB_FAIL(ops_ip_nptop(entry->server_ip_, server_ip, sizeof(server_ip)))) {
    PROXY_NET_LOG(WARN, "fail to ops_ip_nptop", K(entry->server_ip_), K(ret));
  }

  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONGESTION_COLUMN_ID];
  if (OB_SUCC(ret)) {
    cells[OB_CC_CLUSTER_NAME].set_varchar(cluster_name);
    cells[OB_CC_ZONE_NAME].set_varchar(entry->zone_state_->zone_name_);
    cells[OB_CC_REGION_NAME].set_varchar(entry->zone_state_->region_name_);
    cells[OB_CC_ZONE_STATE].set_varchar(ObCongestionZoneState::get_zone_state_name(entry->zone_state_->state_));
    cells[OB_CC_SERVER_IP].set_varchar(server_ip);
    cells[OB_CC_CR_VERSION].set_int(entry->cr_version_);
    cells[OB_CC_SERVER_STATE].set_varchar(entry->get_server_state_name((entry->server_state_)));
    cells[OB_CC_ALIVE_CONGESTED].set_int(entry->alive_congested_);
    if (0 != entry->last_alive_congested_) {
      t = entry->last_alive_congested_;
      if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
      } else {
        strftime_len = strftime(alive_timebuf, sizeof(alive_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      }
    } else {
      strftime_len = snprintf(alive_timebuf, sizeof(alive_timebuf), "%ld", entry->last_alive_congested_);
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(alive_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WARN_ICMD("timebuf is not enough", K(strftime_len), "timebuf length", sizeof(alive_timebuf),
                  K(alive_timebuf), K(ret));
      } else {
        cells[OB_CC_LAST_ALIVE_CONGESTED].set_varchar(alive_timebuf);
      }
    }
  }

  if (OB_SUCC(ret)) {
    cells[OB_CC_DEAD_CONGESTED].set_int(entry->dead_congested_);
    if (0 != entry->last_dead_congested_) {
      t = entry->last_dead_congested_;
      if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
      } else {
        strftime_len = strftime(dead_timebuf, sizeof(dead_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      }
    } else {
      strftime_len = snprintf(dead_timebuf, sizeof(dead_timebuf), "%ld", entry->last_dead_congested_);
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(dead_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WARN_ICMD("timebuf is not enough", K(strftime_len), K(dead_timebuf), K(ret));
      } else {
        cells[OB_CC_LAST_DEAD_CONGESTED].set_varchar(dead_timebuf);
      }
    }
  }

  if (OB_SUCC(ret)) {
    cells[OB_CC_STAT_ALIVE_FAILURES].set_int(entry->stat_alive_failures_);
    cells[OB_CC_STAT_CONN_FAILURES].set_int(entry->stat_conn_failures_);
    if (0 != entry->conn_fail_history_.last_event_) {
      t = entry->conn_fail_history_.last_event_;
      if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
      } else {
        strftime_len = strftime(conn_last_fail, sizeof(conn_last_fail), "%Y-%m-%d %H:%M:%S", &struct_tm);
      }
    } else {
      strftime_len = snprintf(conn_last_fail, sizeof(conn_last_fail), "%ld", entry->conn_fail_history_.last_event_);
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(conn_last_fail))) {
        ret = OB_BUF_NOT_ENOUGH;
        WARN_ICMD("timebuf is not enough", K(strftime_len), K(conn_last_fail), K(ret));
      } else {
        cells[OB_CC_CONN_LAST_FAIL_TIME].set_varchar(conn_last_fail);
      }
    }
  }

  if (OB_SUCC(ret)) {
    cells[OB_CC_CONN_FAILURE_EVENTS].set_int(entry->conn_fail_history_.events_);
    if (0 != entry->alive_fail_history_.last_event_) {
      t = entry->alive_fail_history_.last_event_;
      if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
      } else {
        strftime_len = strftime(alive_last_fail, sizeof(alive_last_fail), "%Y-%m-%d %H:%M:%S", &struct_tm);
      }
    } else {
      strftime_len = snprintf(alive_last_fail, sizeof(alive_last_fail), "%ld", entry->alive_fail_history_.last_event_);
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(alive_last_fail))) {
        ret = OB_BUF_NOT_ENOUGH;
        WARN_ICMD("timebuf is not enough", K(strftime_len), K(ret));
      } else {
        cells[OB_CC_ALIVE_LAST_FAIL_TIME].set_varchar(alive_last_fail);
      }
    }
  }

  if (OB_SUCC(ret)) {
    cells[OB_CC_DETECT_CONGESTED].set_int(entry->detect_congested_);
    if (0 != entry->last_detect_congested_) {
      t = entry->last_detect_congested_;
      if (OB_ISNULL(localtime_r(&t, &struct_tm))) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("fail to converts the calendar time timep to broken-time representation", K(t), K(ret));
      } else {
        strftime_len = strftime(detect_timebuf, sizeof(detect_timebuf), "%Y-%m-%d %H:%M:%S", &struct_tm);
      }
    } else {
      strftime_len = snprintf(detect_timebuf, sizeof(detect_timebuf), "%ld", entry->last_detect_congested_);
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(strftime_len <= 0) || OB_UNLIKELY(strftime_len >= sizeof(detect_timebuf))) {
        ret = OB_BUF_NOT_ENOUGH;
        WARN_ICMD("timebuf is not enough", K(strftime_len), K(detect_timebuf), K(ret));
      } else {
        cells[OB_CC_LAST_DETECT_CONGESTED].set_varchar(detect_timebuf);
      }
    }
  }

  if (OB_SUCC(ret)) {
    cells[OB_CC_ALIVE_FAILURE_EVENTS].set_int(entry->alive_fail_history_.events_);
    cells[OB_CC_REF_COUNT].set_int(entry->ref_count_);
  }

  if (OB_SUCC(ret)) {
    row.cells_ = cells;
    row.count_ = OB_CC_MAX_CONGESTION_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}


static int show_congestion_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowCongestionHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowCongestionHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowCongestionHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowCongestionHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowCongestionHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowCongestionHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_congestion_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_CONGESTION,
                                                               &show_congestion_cmd_callback))) {
    WARN_ICMD("fail to proxy_congestion_callback", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

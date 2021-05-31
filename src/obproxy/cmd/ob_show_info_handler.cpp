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

#define USING_LOG_PREFIX PROXY

#include "cmd/ob_show_info_handler.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/ob_ldc_location.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "obutils/ob_hot_upgrade_processor.h"
#include "obutils/ob_vip_tenant_processor.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//INFO IDC Column
enum
{
  OB_IC_IDC_GLOBAL_NAME = 0,
  OB_IC_IDC_CLUSTER_NAME,
  OB_IC_IDC_MATCH_TYPE,
  OB_IC_IDC_REGION_NAMES,
  OB_IC_IDC_SAME_IDC,
  OB_IC_IDC_SAME_REGION,
  OB_IC_IDC_OTHER_REGION,
  OB_IC_IDC_MAX_COLUMN_ID,
};

//INFO Column DETAIL
enum
{
  OB_IC_DETAIL_NAME = 0,
  OB_IC_DETAIL_INFO,
  OB_IC_DETAIL_MAX_COLUMN_ID,
};

const ObProxyColumnSchema LIST_COLUMN_ARRAY[OB_IC_IDC_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_IC_IDC_GLOBAL_NAME,     "global_idc_name",obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_CLUSTER_NAME,    "cluster_name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_MATCH_TYPE,      "match_type",     obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_REGION_NAMES,    "regions_name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_SAME_IDC,        "same_idc",       obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_SAME_REGION,     "same_region",    obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_IDC_OTHER_REGION,    "other_region",   obmysql::OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema DETAIL_COLUMN_ARRAY[OB_IC_DETAIL_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_IC_DETAIL_NAME,   "name",   obmysql::OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_IC_DETAIL_INFO,   "info",   obmysql::OB_MYSQL_TYPE_VARCHAR),
};


ObShowInfoHandler::ObShowInfoHandler(ObContinuation *cont, ObMIOBuffer *buf,
                                     const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type())
{
  SET_HANDLER(&ObShowInfoHandler::main_handle);
}

int ObShowInfoHandler::main_handle(int event, void *data)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("it should not happened, this_ethread is null", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WARN_ICMD("fail to dump_list_header", K(ret));
  } else if (OB_FAIL(dump_body())) {
    WARN_ICMD("fail to dump_list_body sm", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_ICMD("fail to encode eof packet", K(ret));
  } else {
    INFO_ICMD("succ to dump info");
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowInfoHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (OBPROXY_T_SUB_INFO_IDC == sub_type_) {
    if (OB_FAIL(encode_header(LIST_COLUMN_ARRAY, OB_IC_IDC_MAX_COLUMN_ID))) {
      WARN_ICMD("fail to encode header", K(ret));
    }
  } else {
    if (OB_FAIL(encode_header(DETAIL_COLUMN_ARRAY, OB_IC_DETAIL_MAX_COLUMN_ID))) {
      WARN_ICMD("fail to encode header", K(ret));
    }
  }
  return ret;
}

int ObShowInfoHandler::dump_body()
{
  int ret = OB_SUCCESS;
  if (OBPROXY_T_SUB_INFO_IDC == sub_type_) {
    if (OB_FAIL(dump_idc_body())) {
      WARN_ICMD("fail to dump_idc_body", K(ret));
    }
  } else {
    ObSqlString name;
    ObNewRow row;
    ObObj cells[OB_IC_DETAIL_MAX_COLUMN_ID];
    row.count_ = OB_IC_DETAIL_MAX_COLUMN_ID;
    row.cells_ = cells;

    int64_t pos = 0;
    const int64_t buf_len = OB_MALLOC_NORMAL_BLOCK_SIZE;
    char *buf = reinterpret_cast<char *>(ob_malloc(buf_len, ObModIds::OB_PROXY_PRINTF));
    if (OB_ISNULL(buf)) {
      LOG_ERROR("fail to malloc memory", K(buf_len));
    } else {
      switch (sub_type_) {
        case OBPROXY_T_SUB_INFO_BINARY: {
          const ObString &kernel_release = get_kernel_release_string(get_global_config_server_processor().get_kernel_release());
          const char *md5 = get_global_hot_upgrade_processor().get_proxy_self_md5();
          if (OB_FAIL(name.append_fmt("binary info"))) {
            WARN_ICMD("fail to append info", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s-%s-%s\nversion:%.*s\nMD5:%s\n"
              "REVISION:%s\nBUILD_TIME:%s %s\nBUILD_FLAGS:%s",
              APP_NAME, PACKAGE_STRING, RELEASEID, kernel_release.length(), kernel_release.ptr(), md5,
              build_version(), build_date(), build_time(), build_flags()))) {
            WARN_ICMD("fail to databuff_printf info", K(ret));
          }
          break;
        }
        case OBPROXY_T_SUB_INFO_UPGRADE: {
          if (OB_FAIL(name.append_fmt("hot upgrade info"))) {
            WARN_ICMD("fail to append info", K(ret));
          } else if (buf_len <= (pos = get_global_hot_upgrade_processor().to_string(buf, buf_len))) {
            ret = OB_SIZE_OVERFLOW;
            WARN_ICMD("fail to databuff_printf info", K(pos), K(buf_len), K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          WARN_ICMD("unknown type", K(sub_type_), K(ret));
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      cells[OB_IC_DETAIL_NAME].set_varchar(name.string());
      cells[OB_IC_DETAIL_INFO].set_varchar(buf);

      if (OB_FAIL(encode_row_packet(row))) {
        WARN_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }

    if (OB_LIKELY(NULL != buf)) {
      ob_free(buf);
      buf = NULL;
    }

  }
  return ret;
}

int ObShowInfoHandler::dump_idc_body()
{
  int ret = OB_SUCCESS;
  ObString idc_name;
  char idc_name_buf[common::MAX_PROXY_IDC_LENGTH];
  {
    obsys::CRLockGuard guard(get_global_proxy_config().rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(get_global_proxy_config().proxy_idc_name.str()));
    if (len < 0 || len > common::MAX_PROXY_IDC_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      WARN_ICMD("proxy_idc_name's length is over size", K(len),
               "proxy_idc_name", get_global_proxy_config().proxy_idc_name.str(), K(ret));
    } else {
      memcpy(idc_name_buf, get_global_proxy_config().proxy_idc_name.str(), len);
      idc_name.assign(idc_name_buf, static_cast<int32_t>(len));
    }
  }
  ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info;
  ObSEArray<ObClusterResource *, 32> cr_array;
  const int64_t buf_len = OB_MALLOC_NORMAL_BLOCK_SIZE;
  char *common_buf = NULL;
  if (OB_ISNULL(common_buf = reinterpret_cast<char *>(ob_malloc(buf_len, ObModIds::OB_PROXY_PRINTF)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to malloc memory", K(buf_len), K(ret));
  } else if (OB_FAIL(get_global_resource_pool_processor().acquire_all_cluster_resource(cr_array))) {
    WARN_ICMD("fail to acquire all cluster resource from resource_pool", K(ret));
  } else {
    ObClusterResource *cr = NULL;
    for (int64_t i = 0; (i < cr_array.count()) && OB_SUCC(ret); ++i) {
      cr = cr_array.at(i);
      if (match_like(cr->get_cluster_name().ptr(), like_name_)) {
        if (OB_FAIL(dump_resource_idc_info(*cr, idc_name, simple_servers_info, common_buf, buf_len))) {
          WARN_ICMD("fail to dump cluster resource idc_info", KPC(cr), K(ret));
        }
      }
    }
  }

  if (OB_LIKELY(NULL != common_buf)) {
    ob_free(common_buf);
    common_buf = NULL;
  }

  for (int64_t i = 0; i < cr_array.count(); ++i) {
    cr_array.at(i)->dec_ref();
  }
  cr_array.reset();
  return ret;
}

int ObShowInfoHandler::dump_resource_idc_info(const obutils::ObClusterResource &cr,
    const common::ObString &idc_name, common::ObIArray<obutils::ObServerStateSimpleInfo> &ss_info,
    char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const bool is_base_servers_added = cr.is_base_servers_added();
  ss_info.reuse();
  ObSEArray<ObString, 5> region_names;
  ObSEArray<ObString, 5> same_idc;
  ObSEArray<ObString, 5> same_region;
  ObSEArray<ObString, 5> other_region;
  ObProxyNameString region_name_from_idc_list;

  ObNewRow row;
  ObObj cells[OB_IC_IDC_MAX_COLUMN_ID];
  row.count_ = OB_IC_IDC_MAX_COLUMN_ID;
  row.cells_ = cells;

  if (!is_base_servers_added) {
    INFO_ICMD("base servers has not added, treat all server as ok",K(ret));
  } else {
    const uint64_t new_ss_version = cr.server_state_version_;
    common::ObIArray<ObServerStateSimpleInfo> &server_state_info = const_cast<obutils::ObClusterResource &>(cr).get_server_state_info(new_ss_version);
    common::DRWLock &server_state_lock = const_cast<obutils::ObClusterResource &>(cr).get_server_state_lock(new_ss_version);
    server_state_lock.rdlock();
    if (OB_FAIL(ss_info.assign(server_state_info))) {
      WARN_ICMD("fail to assign servers_info_", K(ret));
    }
    server_state_lock.rdunlock();
  }

  row.cells_[OB_IC_IDC_GLOBAL_NAME].set_varchar(idc_name);
  row.cells_[OB_IC_IDC_CLUSTER_NAME].set_varchar(cr.get_cluster_name());

  ObLDCLocation::ObRegionMatchedType match_type = ObLDCLocation::MATCHED_BY_NONE;
  if (FAILEDx(ObLDCLocation::get_region_name(ss_info, idc_name, cr.get_cluster_name(),
                                             cr.get_cluster_id(),
                                             region_name_from_idc_list,
                                             match_type, region_names))) {
    WARN_ICMD("fail to get region name", K(idc_name), K(ret));
  } else {
    const bool is_ldc_used = (!idc_name.empty() && !region_names.empty());
    for (int64_t j = 0; OB_SUCC(ret) && j < ss_info.count(); ++j) {
      const ObServerStateSimpleInfo &ss = ss_info.at(j);
      if (is_ldc_used) {
        if (ObLDCLocation::is_in_logic_region(region_names, ss.region_name_)) {
          if (((ObLDCLocation::MATCHED_BY_IDC == match_type) && (0 == ss.idc_name_.case_compare(idc_name))) // ignore case
              || ((ObLDCLocation::MATCHED_BY_ZONE_PREFIX == match_type) && ss.zone_name_.prefix_case_match(idc_name))) {
            if (OB_FAIL(same_idc.push_back(ss.zone_name_))) {
              WARN_ICMD("failed to push back same_idc", K(j), K(ss), K(same_idc), K(ret));
            }
          } else {
            if (OB_FAIL(same_region.push_back(ss.zone_name_))) {
              WARN_ICMD("failed to push back same_region", K(j), K(ss), K(same_region), K(ret));
            }
          }
        } else {
          if (OB_FAIL(other_region.push_back(ss.zone_name_))) {
            WARN_ICMD("failed to push back other_region", K(j), K(ss), K(other_region), K(ret));
          }
        }
      } else {
        if (OB_FAIL(same_idc.push_back(ss.zone_name_))) {
          WARN_ICMD("failed to push back same_idc", K(j), K(ss), K(same_idc), K(ret));
        }
      }
    }
  }

  row.cells_[OB_IC_IDC_MATCH_TYPE].set_varchar(ObLDCLocation::get_region_match_type_string(match_type));
  if (OB_SUCC(ret) && OB_NOT_NULL(buf)) {
    int64_t pos = 0;
    int64_t curr_pos = 0;
    if (buf_len <= (pos = region_names.to_string(buf + curr_pos, buf_len))) {
      ret = OB_SIZE_OVERFLOW;
      WARN_ICMD("fail to databuff_printf region_names", K(pos), K(buf_len), K(ret));
    } else {
      row.cells_[OB_IC_IDC_REGION_NAMES].set_varchar(buf + curr_pos, static_cast<int32_t>(pos));
      curr_pos += pos;
    }

    if (OB_SUCC(ret)) {
      if (buf_len <= (pos = same_idc.to_string(buf + curr_pos, buf_len))) {
        ret = OB_SIZE_OVERFLOW;
        WARN_ICMD("fail to databuff_printf same_idc", K(pos), K(buf_len), K(ret));
      } else {
        row.cells_[OB_IC_IDC_SAME_IDC].set_varchar(buf + curr_pos, static_cast<int32_t>(pos));
        curr_pos += pos;
      }
    }

    if (OB_SUCC(ret)) {
      if (buf_len <= (pos = same_region.to_string(buf + curr_pos, buf_len))) {
        ret = OB_SIZE_OVERFLOW;
        WARN_ICMD("fail to databuff_printf same_region", K(pos), K(buf_len), K(ret));
      } else {
        row.cells_[OB_IC_IDC_SAME_REGION].set_varchar(buf + curr_pos, static_cast<int32_t>(pos));
        curr_pos += pos;
      }
    }

    if (OB_SUCC(ret)) {
      if (buf_len <= (pos = other_region.to_string(buf + curr_pos, buf_len))) {
        ret = OB_SIZE_OVERFLOW;
        WARN_ICMD("fail to databuff_printf region_names", K(pos), K(buf_len), K(ret));
      } else {
        row.cells_[OB_IC_IDC_OTHER_REGION].set_varchar(buf + curr_pos, static_cast<int32_t>(pos));
        curr_pos += pos;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), "cluaster_name", cr.get_cluster_name(), K(ret));
    }
  }
  return ret;
}

static int show_info_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowInfoHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowInfoHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowInfoHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowInfoHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowInfoHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowInfoHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_info_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_INFO,
                                                               &show_info_cmd_callback))) {
    WARN_ICMD("fail to register CMD_TYPE_SM", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

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

#include "cmd/ob_show_vip_handler.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_task.h"
#include "iocore/eventsystem/ob_event_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

//VipColumn
enum
{
  OB_VC_VID = 0,
  OB_VC_VIP,
  OB_VC_VPORT,
  OB_VC_TENANT_NAME,
  OB_VC_CLUSTER_NAME,
  OB_VC_INFO,
  OB_VC_MAX_VIP_COLUMN_ID,
};

const ObProxyColumnSchema VIP_COLUMN_ARRAY[OB_VC_MAX_VIP_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_VC_VID,           "vid",          OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_VC_VIP,           "vip",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_VC_VPORT,         "vport",        OB_MYSQL_TYPE_LONG),
    ObProxyColumnSchema::make_schema(OB_VC_TENANT_NAME,   "tenant_name",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_VC_CLUSTER_NAME,  "cluster_name", OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_VC_INFO,          "info",         OB_MYSQL_TYPE_VARCHAR),
};

ObShowVipHandler::ObShowVipHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info)
{
  SET_HANDLER(&ObShowVipHandler::main_handler);
}

int ObShowVipHandler::main_handler(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObVipAddr vip_addr;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WARN_ICMD("fail to dump header", K(ret));
  } else {
    ObVipTenantCache &vt_cache = get_global_vip_tenant_processor().get_vt_cache();
    ObVipTenant vip_tenant;
    if (!like_name_.empty()) {
      //dump one vip tenant
      ObString match_name(like_name_);
      ObString vid_string = match_name.split_on(' ');
      if (vid_string.empty() || match_name.empty()) {
        ret = OB_INVALID_ARGUMENT;
        WARN_ICMD("unexpected argument", K(vid_string), K(match_name), K(ret));
      } else if (OB_FAIL(get_int_value(vid_string, vip_addr.vid_))) {
        WARN_ICMD("fail to get_int_value", K(vid_string), K(ret));
      } else {
        ObString vip_string = match_name.split_on(':');
        int64_t vport = 0;
        if (vip_string.empty() || match_name.empty()) {
          ret = OB_INVALID_ARGUMENT;
          WARN_ICMD("unexpected argument", K(vip_string), K(match_name), K(ret));
        } else if (OB_FAIL(get_int_value(match_name, vport))) {
          WARN_ICMD("fail to get_int_value", K(match_name), K(vport), K(ret));
        } else if (!vip_addr.addr_.set_ipv4_addr(vip_string, static_cast<int32_t>(vport))) {
          ret = OB_INVALID_ARGUMENT;
          WARN_ICMD("fail to set_ipv4_addr", K(vip_string), K(vport), K(ret));
        } else if (!vip_addr.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          WARN_ICMD("invalid vip_addr", K(vip_addr), K(match_name), K(ret));
        } else {
          DEBUG_ICMD("succ get vip addr", K(vip_addr), K(match_name));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(vt_cache.get(vip_addr, vip_tenant))) {
          WARN_ICMD("fail to get vip_tenant", K(ret));
          if (OB_ENTRY_NOT_EXIST == ret) {
            //empty result
            ret = OB_SUCCESS;
          }
        } else if (OB_FAIL(dump_item(vip_tenant))) {
          WARN_ICMD("fail to dump item", K(vip_tenant), K(ret));
        }
      } else {
        ret = OB_ERR_OPERATOR_UNKNOWN;//return this errno
      }
    } else {
      //dump all vip tenant
      ObVipTenantCache::VTHashMap *cache_map = NULL;
      obsys::CRLockGuard guard(vt_cache.rwlock_);
      if (OB_ISNULL(cache_map = vt_cache.get_cache_map())) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("cache_map is null", K(ret));
      } else {
        for (ObVipTenantCache::VTHashMap::iterator it = cache_map->begin();
             OB_SUCC(ret) && it != cache_map->end(); ++it) {
          if (OB_FAIL(dump_item(*it))) {
            WARN_ICMD("fail to dump item", K(*it), K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WARN_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump cluster", K_(like_name));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowVipHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(VIP_COLUMN_ARRAY, OB_VC_MAX_VIP_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowVipHandler::dump_item(const ObVipTenant &vip_tenant)
{
  int ret = OB_SUCCESS;
  const char *empty_str = "";
  char addr_str[MAX_IP_PORT_LENGTH];
  addr_str[0] = '\0';
  if (!vip_tenant.vip_addr_.addr_.ip_to_string(addr_str, MAX_IP_ADDR_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    WARN_ICMD("fail to covert to addr to string", K(addr_str), K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_VC_MAX_VIP_COLUMN_ID];
    cells[OB_VC_VID].set_int(vip_tenant.vip_addr_.vid_);
    cells[OB_VC_VIP].set_varchar(addr_str);
    cells[OB_VC_VPORT].set_int32(vip_tenant.vip_addr_.addr_.get_port());
    cells[OB_VC_TENANT_NAME].set_varchar(vip_tenant.tenant_name_);
    cells[OB_VC_CLUSTER_NAME].set_varchar(vip_tenant.cluster_name_);
    cells[OB_VC_INFO].set_varchar(empty_str);
    row.cells_ = cells;
    row.count_ = OB_VC_MAX_VIP_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WARN_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

static int show_vip_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowVipHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowVipHandler(cont, buf,info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObShowJsonConfigHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowVipHandler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObShowVipHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowVipHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_vip_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_VIP, &show_vip_cmd_callback))) {
    WARN_ICMD("fail to register_cmd CMD_TYPE_VIP", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase



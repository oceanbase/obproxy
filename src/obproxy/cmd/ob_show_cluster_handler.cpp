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

#include "cmd/ob_show_cluster_handler.h"
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

//ClusterColumn
enum
{
  OB_CC_CLUSTER_NAME = 0,
  OB_CC_CLUSTER_ID,
  OB_CC_RS_URL,
  OB_CC_ROOT_ADDR,
  OB_CC_ROLE,
  OB_CC_REPLICA_TYPE,
  OB_CC_MAX_CLUSTER_COLUMN_ID,
};

//ClusterIDCColumn
enum
{
  OB_CCI_CLUSTER_NAME = 0,
  OB_CCI_CLUSTER_ID,
  OB_CCI_IDC_URL,
  OB_CCI_IDC,
  OB_CCI_REGION,
  OB_CCI_MAX_CLUSTER_COLUMN_ID,
};


const ObProxyColumnSchema CLUSTER_COLUMN_ARRAY[OB_CC_MAX_CLUSTER_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CC_CLUSTER_NAME,   "cluster_name",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_CLUSTER_ID,     "cluster_id",     OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CC_RS_URL,         "rs_url",         OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ROOT_ADDR,      "root_addr",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_ROLE,           "role",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_REPLICA_TYPE,   "replica_type",   OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema CLUSTER_IDC_COLUMN_ARRAY[OB_CCI_MAX_CLUSTER_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CCI_CLUSTER_NAME,   "cluster_name",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CCI_CLUSTER_ID,     "cluster_id",     OB_MYSQL_TYPE_LONGLONG),
    ObProxyColumnSchema::make_schema(OB_CCI_IDC_URL,        "idc_url",        OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CCI_IDC,            "idc",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CCI_REGION,         "region",         OB_MYSQL_TYPE_VARCHAR),
};


ObShowClusterHandler::ObShowClusterHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type())
{
  SET_HANDLER(&ObShowClusterHandler::handle_show_cluster);
}

int ObShowClusterHandler::handle_show_cluster(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  ObProxyJsonConfigInfo *json_info= NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_cluster_header())) {
    WDIAG_ICMD("fail to dump cluster info header", K(ret));
  } else if (OB_ISNULL(json_info = get_global_config_server_processor().acquire())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("fail to get proxy json config info", K(ret));
  } else {
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    ObProxyClusterArrayInfo &cluster_array = const_cast<ObProxyClusterArrayInfo &>(json_info->get_cluster_array());
    const ObProxyMetaTableInfo &table_info = json_info->get_meta_table_info();
    for (ObProxyClusterArrayInfo::CIHashMap::iterator it = cluster_array.ci_map_.begin();
         OB_SUCC(ret) && it != cluster_array.ci_map_.end(); ++it) {
      if (common::match_like(it->cluster_name_, like_name_)) {
        if (OB_FAIL(dump_cluster_info(*it, allocator))) {
          WDIAG_ICMD("fail to dump cluster info", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && common::match_like(table_info.cluster_info_.cluster_name_, like_name_)) {
      if (OB_FAIL(dump_cluster_info(table_info.cluster_info_, allocator))) {
        WDIAG_ICMD("fail to dump meta db cluster info", K(ret));
      }
    }
  }

  get_global_config_server_processor().release(json_info);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
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

int ObShowClusterHandler::dump_cluster_header()
{
  int ret = OB_SUCCESS;
  if (OBPROXY_T_SUB_INFO_IDC == sub_type_) {
    if (OB_FAIL(encode_header(CLUSTER_IDC_COLUMN_ARRAY, OB_CCI_MAX_CLUSTER_COLUMN_ID))) {
      WDIAG_ICMD("fail to encode header", K(ret));
    }
  } else {
    if (OB_FAIL(encode_header(CLUSTER_COLUMN_ARRAY, OB_CC_MAX_CLUSTER_COLUMN_ID))) {
      WDIAG_ICMD("fail to encode header", K(ret));
    }
  }
  return ret;
}

int ObShowClusterHandler::dump_cluster_info(const ObProxyClusterInfo &cluster_info,
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator)
{
  int ret = OB_SUCCESS;
  ObProxySubClusterInfo *sub_cluster_info = NULL;
  if (OB_FAIL(cluster_info.get_sub_cluster_info(OB_DEFAULT_CLUSTER_ID, sub_cluster_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WDIAG("fail to get master cluster info", K(cluster_info), K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(sub_cluster_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("master cluster info is null", K(cluster_info), K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OBPROXY_T_SUB_INFO_IDC == sub_type_) {
      if (NULL == sub_cluster_info || sub_cluster_info->idc_list_.empty()) {
        ObProxyIDCInfo idc_item;
        if (OB_FAIL(dump_cluster_idc_list_item(cluster_info, idc_item, allocator))) {
          WDIAG_ICMD("fail to dump cluster info item", K(cluster_info), K(ret));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sub_cluster_info->idc_list_.count(); ++i) {
          if (OB_FAIL(dump_cluster_idc_list_item(cluster_info, sub_cluster_info->idc_list_[i], allocator))) {
            WDIAG_ICMD("fail to dump cluster info item", K(cluster_info), K(ret));
          }
        }
      }
    } else {
      if (NULL == sub_cluster_info || sub_cluster_info->web_rs_list_.empty()) {
        ObProxyReplicaLocation addr;
        if (OB_FAIL(dump_cluster_rslist_item(cluster_info, addr, allocator))) {
          WDIAG_ICMD("fail to dump cluster info item", K(cluster_info), K(ret));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sub_cluster_info->web_rs_list_.count(); ++i) {
          if (OB_FAIL(dump_cluster_rslist_item(cluster_info, sub_cluster_info->web_rs_list_[i], allocator))) {
            WDIAG_ICMD("fail to dump cluster info item", K(cluster_info), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObShowClusterHandler::dump_cluster_rslist_item(const ObProxyClusterInfo &cluster_info,
    const ObProxyReplicaLocation &addr, ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator)
{
  int ret = OB_SUCCESS;
  char addr_str[MAX_IP_PORT_LENGTH];
  addr_str[0] = '\0';
  char *url = NULL;
  allocator.reuse();
  if (OB_FAIL(get_global_config_server_processor().get_cluster_url(cluster_info.cluster_name_, allocator, url))) {
    LOG_WDIAG("fail to get rs url", K(cluster_info.cluster_name_), K(ret));
  } else if (OB_ISNULL(url)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("rs url is null", K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_CC_MAX_CLUSTER_COLUMN_ID];
    cells[OB_CC_CLUSTER_NAME].set_varchar(cluster_info.cluster_name_);
    cells[OB_CC_CLUSTER_ID].set_int(cluster_info.master_cluster_id_);
    cells[OB_CC_RS_URL].set_varchar(url);
    if (OB_FAIL(addr.server_.ip_port_to_string(addr_str, MAX_IP_PORT_LENGTH))) {
      WDIAG_ICMD("fail to covert to addr to string");
    } else {
      cells[OB_CC_ROOT_ADDR].set_varchar(addr_str);
      cells[OB_CC_ROLE].set_varchar(role2str(addr.role_));
      cells[OB_CC_REPLICA_TYPE].set_varchar(ObProxyReplicaLocation::get_replica_type_string(addr.replica_type_));
    }
    if (OB_SUCC(ret)) {
      row.cells_ = cells;
      row.count_ = OB_CC_MAX_CLUSTER_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObShowClusterHandler::dump_cluster_idc_list_item(const ObProxyClusterInfo &cluster_info,
    const ObProxyIDCInfo &idc_info, ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator)
{
  int ret = OB_SUCCESS;
  char *url = NULL;
  allocator.reuse();
  if (get_global_proxy_config().with_config_server_) {
    if (OB_FAIL(get_global_config_server_processor().get_idc_url(cluster_info.cluster_name_, allocator, url))) {
      LOG_WDIAG("fail to get idc url", K(cluster_info.cluster_name_), K(ret));
    } else if (OB_ISNULL(url)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("rs url is null", K(ret));
    }
  } else {
    url = const_cast<char *>("");
  }

  if (OB_SUCC(ret)) {
    ObNewRow row;
    ObObj cells[OB_CCI_MAX_CLUSTER_COLUMN_ID];
    cells[OB_CCI_CLUSTER_NAME].set_varchar(cluster_info.cluster_name_);
    cells[OB_CCI_CLUSTER_ID].set_int(cluster_info.master_cluster_id_);
    cells[OB_CCI_IDC_URL].set_varchar(url);
    cells[OB_CCI_IDC].set_varchar(idc_info.idc_name_.name_string_);
    cells[OB_CCI_REGION].set_varchar(idc_info.region_name_.name_string_);

    row.cells_ = cells;
    row.count_ = OB_CCI_MAX_CLUSTER_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}


static int show_cluster_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowClusterHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowClusterHandler(cont, buf,info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowJsonConfigHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowClusterHandler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowClusterHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowClusterHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_cluster_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_CLUSTER,
                                                               &show_cluster_cmd_callback))) {
    WDIAG_ICMD("fail to proxy_cluster_stat_callback", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase



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

#include "share/config/ob_server_config.h"
#include "common/ob_common_utility.h"
#include "common/mysql_proxy/ob_isql_client.h"
#include "common/ob_record_header.h"
#include "common/ob_zone.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"

namespace oceanbase
{
namespace common
{
using namespace share;

ObServerConfig::ObServerConfig()
    : disk_actual_space_(0), self_addr_(), system_config_(NULL)
{
}

ObServerConfig::~ObServerConfig()
{
}

ObServerConfig &ObServerConfig::get_instance()
{
  static ObServerConfig config;
  return config;
}

int ObServerConfig::init(const ObSystemConfig &config)
{
  int ret = OB_SUCCESS;
  system_config_ = &config;
  if (OB_ISNULL(system_config_)) {
    ret = OB_INIT_FAIL;
  }
  return ret;
}

int64_t ObServerConfig::get_server_memory_limit()
{
  int64_t memory = get_phy_mem_size() * memory_limit_percentage / 100;
  if (0 != memory_limit.get()) {
    memory = memory_limit;
  }
  return memory;
}

int64_t ObServerConfig::get_server_memory_avail()
{
  int64_t reserved = 0;
  if (0 != system_memory_percentage.get()) {
    reserved = get_server_memory_limit() * system_memory_percentage / 100;
  }
  return get_server_memory_limit() - reserved;
}

int64_t ObServerConfig::get_reserved_server_memory()
{
  return get_server_memory_limit() * system_memory_percentage / 100;
}

int ObServerConfig::read_config()
{
  int ret = OB_SUCCESS;
  ObSystemConfigKey key;
  ObAddr server;
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (!server.set_ipv4_addr(ntohl(obsys::CNetUtil::getLocalAddr(devname)), 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(true != server.ip_to_string(local_ip, sizeof(local_ip)))) {
    ret = OB_CONVERT_ERROR;
  } else {
    key.set_varchar(ObString::make_string("svr_type"), print_server_role(get_server_type()));
    key.set_int(ObString::make_string("svr_port"), rpc_port);
    key.set_varchar(ObString::make_string("svr_ip"), local_ip);
    key.set_varchar(ObString::make_string("zone"), zone);
    ObConfigContainer::const_iterator it = container_.begin();
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      key.set_name(it->first.str());
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
      } else {
        key.set_version(it->second->version());
        int temp_ret = system_config_->read_config(key, *(it->second));
        if (OB_SUCCESS != temp_ret) {
          OB_LOG(DEBUG, "Read config error", "name", it->first.str(), K(temp_ret));
        }
      }
    }
  }
  return ret;
}

int ObServerConfig::check_all() const
{
  int ret = OB_SUCCESS;
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
    } else if (!it->second->check()) {
      int temp_ret = OB_INVALID_CONFIG;
      OB_LOG(WARN, "Configure setting invalid",
             "name", it->first.str(), "value", it->second->str(), K(temp_ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObServerConfig::strict_check_special() const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (!cluster_id.check()) {
      ret = OB_INVALID_CONFIG;
      SHARE_LOG(WARN, "invalid cluster id", K(ret), K(cluster_id.str()));
    }
  }
  return ret;
}

void ObServerConfig::print() const
{
  OB_LOG(INFO, "===================== *begin server config report * =====================");
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      OB_LOG(WARN, "config item is null", "name", it->first.str());
    } else {
      _OB_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  OB_LOG(INFO, "===================== *stop server config report* =======================");
}

} // end of namespace common
} // end of namespace oceanbase

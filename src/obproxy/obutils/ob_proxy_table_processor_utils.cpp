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
#include "obutils/ob_proxy_table_processor_utils.h"
#include <sys/utsname.h>
#include "utils/ob_proxy_table_define.h"
#include "iocore/net/ob_net_def.h"
#include "iocore/net/ob_socket_manager.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_utils.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_config_server_processor.h"


using namespace oceanbase::share;
using namespace oceanbase::common;
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
static const char *INADDR_ANY_IP = "0.0.0.0";
static const char *INADDR_LOOPBACK_IP = "127.0.0.1";

static const char *GET_PROXY_INFO_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
    "current_pid, hot_upgrade_cmd, new_binary_version, %s "
    "FROM %s WHERE proxy_ip = '%s' AND proxy_port = %d LIMIT %ld";

static const char *INSERT_DUPLICATE_PROXY_INFO_SQL =
    "INSERT INTO %s ( "
    "proxy_ip, proxy_port, regist_time, proxy_id, app_name, "
    "binary_version, config_version, system_info, current_pid, "
    "hot_upgrade_cmd, parent_hot_upgrade_status, sub_hot_upgrade_status, "
    "new_binary_version, new_binary_5u_md5, new_binary_6u_md5, "
    "new_binary_7u_md5, reload_config, info "
    " ) VALUES ( "
    "'%s', %d, now(), %ld, '%s', "
    "'%s', %ld, '%s', %ld, "
    "'%.*s', '%.*s', '%.*s',"
    "'%s', '%s', '%s', "
    "'%s', '%.*s', '%s'"
    " ) ON DUPLICATE KEY UPDATE "
    "proxy_id=%ld, app_name='%s', "
    "binary_version='%s', config_version=%ld, system_info='%s', current_pid=%ld, "
    "hot_upgrade_cmd='%.*s', parent_hot_upgrade_status='%.*s', sub_hot_upgrade_status='%.*s', "
    "new_binary_version='%s', new_binary_5u_md5='%s', new_binary_6u_md5='%s', "
    "new_binary_7u_md5='%s', reload_config='%.*s', info='%s' ";

static const char *INSERT_PROXY_KV_TABLE_INFO_SQL =
    "INSERT INTO %s (name, datatype, value, info) "
    "VALUES ('%s', %d, '%.*s', '%s')";

static const char *GET_CONFIG_VERSION_VALUE_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ value FROM  %s WHERE name='%s%.*s' LIMIT %ld";

static const char *UPDATE_PROXY_TABLE_CONFIG_INFO_SQL =
    "UPDATE %s SET config_version=%ld, reload_config='%.*s'"
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_INFO_WITH_HUS_SQL =
    "UPDATE %s SET "
    "proxy_id =%ld, app_name='%s', binary_version='%s', config_version=%ld, "
    "current_pid=%ld, info='%s', "
    "parent_hot_upgrade_status='%.*s', sub_hot_upgrade_status='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_INFO_WITH_HUS_AND_CMD_SQL =
    "UPDATE %s SET "
    "proxy_id =%ld, app_name='%s', binary_version='%s', config_version=%ld, "
    "current_pid=%ld, info='%s', "
    "parent_hot_upgrade_status='%.*s', sub_hot_upgrade_status='%.*s', "
    "hot_upgrade_cmd='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_HOT_UPGRADE_CMD_EITHER_SQL =
    "UPDATE %s SET "
    "hot_upgrade_cmd='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d "
    "AND (hot_upgrade_cmd = '%.*s' or hot_upgrade_cmd = '%.*s')";

static const char *UPDATE_PROXY_HOT_UPGRADE_CMD_ANOTHER_SQL =
    "UPDATE %s SET "
    "hot_upgrade_cmd='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d "
    "AND (hot_upgrade_cmd = '%.*s' or hot_upgrade_cmd = '%.*s' or hot_upgrade_cmd = '%.*s')";

static const char *UPDATE_PROXY_PARENT_HUS_AND_CMD_SQL =
    "UPDATE %s SET "
    "parent_hot_upgrade_status='%.*s', hot_upgrade_cmd='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_BOTH_HUS_SQL =
    "UPDATE %s SET "
    "parent_hot_upgrade_status='%.*s', sub_hot_upgrade_status='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_BOTH_HUS_AND_CMD_SQL =
    "UPDATE %s SET "
    "parent_hot_upgrade_status='%.*s', sub_hot_upgrade_status='%.*s', "
    "hot_upgrade_cmd='%.*s' "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *UPDATE_PROXY_UPDATE_TIME_SQL =
    "UPDATE %s SET update_time=CURRENT_TIMESTAMP(6) "
    "WHERE proxy_ip = '%s' AND proxy_port = %d";

static const char *GET_PROXY_KV_TABLE_INFO_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ name, value "
    "FROM  %s "
    "WHERE name IN ("
    "'%.*s','%.*s','%.*s','%.*s','%.*s','%.*s',"
    "'%.*s','%.*s','%.*s','%.*s','%.*s','%.*s') "
    "LIMIT %ld";

static const char *GET_PROXY_ALL_VIP_TENANT_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ vid, vip, vport, tenant_name, cluster_name, info FROM %s LIMIT %ld";

static const char *JSON_REQUEST_TARGET = "REQUEST_TARGET";
static const char *JSON_RW_TYPE = "RW_TYPE";


int ObProxyTableProcessorUtils::get_proxy_info(ObMysqlProxy &mysql_proxy,
    const char *proxy_ip, const int32_t proxy_port, ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_ip) || OB_UNLIKELY(proxy_port <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(ret));
  } else {
    ObMysqlResultHandler result_handler;
    char sql[OB_SHORT_SQL_LENGTH];
    const ObProxyKernelRelease kernel_release = get_global_config_server_processor().get_kernel_release();
    int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, GET_PROXY_INFO_SQL,
                           ObProxyServerInfo::get_new_binary_md5_str(kernel_release),
                           ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                           proxy_ip, proxy_port, INT64_MAX);
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
    } else if (OB_FAIL(mysql_proxy.read(sql, result_handler))) {
      LOG_WARN("fail to read proxy info", K(sql), K(ret));
    } else {
      if (OB_FAIL(result_handler.next())) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) { //no record
          ret = OB_ENTRY_NOT_EXIST;
          LOG_INFO("no record found", K(sql), K(ret));
        } else {
          LOG_WARN("fail to get proxy info", K(sql), K(ret));
        }
      } else {
        if (OB_FAIL(fill_proxy_info(result_handler, proxy_info))) {
          LOG_WARN("fail to fill proxy info", K(ret));
        } else {
          // check if this is only one
          if (OB_UNLIKELY(OB_ITER_END != (ret = result_handler.next()))) {
            LOG_WARN("fail to get proxy info, there is more than one record", K(ret));
            ret = OB_ERR_UNEXPECTED;
          } else {
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::fill_proxy_info(ObMysqlResultHandler &result_handler,
                                                ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  char cmd_str_buf[OB_MAX_PROXY_HOT_UPGRADE_CMD_LEN + 1];
  int64_t cmd_str_len = 0;
  const ObProxyKernelRelease kernel_release = get_global_config_server_processor().get_kernel_release();
  PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "current_pid", proxy_info.current_pid_, int64_t);
  PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "hot_upgrade_cmd", cmd_str_buf,
                                   static_cast<int32_t>(sizeof(cmd_str_buf)), cmd_str_len);
  PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "new_binary_version",
                                   proxy_info.new_binary_version_,
                                    static_cast<int32_t>(sizeof(proxy_info.new_binary_version_)),
                                    tmp_real_str_len);

  PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler,
                                   ObProxyServerInfo::get_new_binary_md5_str(kernel_release),
                                   proxy_info.new_binary_md5_,
                                   static_cast<int32_t>(sizeof(proxy_info.new_binary_md5_)),
                                   tmp_real_str_len);

  if (OB_SUCC(ret)) {
    ObString cmd_str(cmd_str_len, cmd_str_buf);
    if (OB_FAIL(proxy_info.parse_hu_cmd(cmd_str))) {
      LOG_WARN("fail to parse hot upgrade cmd", K(cmd_str), K(ret));
    } else {
      LOG_DEBUG("fill proxy info succ", K(proxy_info), K(ret));
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::get_proxy_kv_table_info(ObMysqlProxy &mysql_proxy,
    const ObProxyKVRowsName &rows, ObProxyKVTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  ObMysqlResultHandler result_handler;
  char sql[OB_SHORT_SQL_LENGTH];
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, GET_PROXY_KV_TABLE_INFO_SQL,
                         ObProxyTableInfo::PROXY_KV_TABLE_NAME,
                         rows.name_[INDEX_CONFIG_VERSION].length(), rows.name_[INDEX_CONFIG_VERSION].ptr(),
                         rows.name_[INDEX_VIP_TENANT_VERSION].length(), rows.name_[INDEX_VIP_TENANT_VERSION].ptr(),
                         rows.name_[INDEX_APP_UPGRADE_SWITCH].length(), rows.name_[INDEX_APP_UPGRADE_SWITCH].ptr(),
                         rows.name_[INDEX_APP_UPGRADE_START_TIME].length(), rows.name_[INDEX_APP_UPGRADE_START_TIME].ptr(),
                         rows.name_[INDEX_APP_UPGRADE_DURATION_MINUTES].length(), rows.name_[INDEX_APP_UPGRADE_DURATION_MINUTES].ptr(),
                         rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_NAME].length(), rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_NAME].ptr(),
                         rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_MD5].length(), rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_MD5].ptr(),
                         rows.name_[INDEX_ALL_UPGRADE_SWITCH].length(), rows.name_[INDEX_ALL_UPGRADE_SWITCH].ptr(),
                         rows.name_[INDEX_ALL_UPGRADE_START_TIME].length(), rows.name_[INDEX_ALL_UPGRADE_START_TIME].ptr(),
                         rows.name_[INDEX_ALL_UPGRADE_DURATION_MINUTES].length(), rows.name_[INDEX_ALL_UPGRADE_DURATION_MINUTES].ptr(),
                         rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME].length(), rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME].ptr(),
                         rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5].length(), rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5].ptr(),
                         INT64_MAX);

  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_FAIL(mysql_proxy.read(sql, result_handler))) {
    LOG_WARN("fail to read proxy kv table info", K(sql), K(ret));
  } else if (OB_FAIL(fill_kv_table_info(result_handler, rows, kv_info))) {
    LOG_WARN("fail to fill proxy kv table info", K(ret));
  } else {
    LOG_DEBUG("succ to get proxy kv table info", K(rows), K(kv_info));
  }
  return ret;
}

int ObProxyTableProcessorUtils::fill_kv_table_info(ObMysqlResultHandler &result_handler,
    const ObProxyKVRowsName &rows, ObProxyKVTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  ObString kv_name;
  ObString kv_value;
  while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "name", kv_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "value", kv_value);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(kv_info.fill_rows(rows, kv_name, kv_value))) {
        LOG_WARN("fail to fill rows, ignore it, continue", K(kv_name), K(kv_value), K(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_DEBUG("succ to fill rows", K(kv_name), K(kv_value));
      }
      kv_name.reset();
      kv_value.reset();
    }
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail to get result from result_handler set", K(ret));
  }
  return ret;
}

int ObProxyTableProcessorUtils::add_or_modify_proxy_info(ObMysqlProxy &mysql_proxy,
                                                         ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  char *sql = NULL;
  const int64_t sql_len = OB_SHORT_SQL_LENGTH * 4;
  if (OB_ISNULL(sql = static_cast<char *>(op_fixed_mem_alloc(sql_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    const ObProxyKernelRelease kernel_release = get_global_config_server_processor().get_kernel_release();
    const char *null_str = "";
    const ObString &hu_cmd = ObHotUpgraderInfo::get_cmd_string(proxy_info.hu_cmd_);
    const ObString &parent_hu_status = ObHotUpgraderInfo::get_status_string(proxy_info.parent_hu_status_);
    const ObString &sub_hu_status = ObHotUpgraderInfo::get_status_string(proxy_info.sub_hu_status_);
    const ObString &rc_status = ObHotUpgraderInfo::get_rc_status_string(proxy_info.rc_status_);
    const int64_t len = snprintf(sql, sql_len, INSERT_DUPLICATE_PROXY_INFO_SQL,
                                 ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                                 proxy_info.proxy_ip_,
                                 proxy_info.proxy_port_,
                                 proxy_info.proxy_id_,
                                 proxy_info.app_name_,
                                 proxy_info.binary_version_, proxy_info.config_version_,
                                 proxy_info.uname_info_, proxy_info.current_pid_,
                                 hu_cmd.length(), hu_cmd.ptr(),
                                 parent_hu_status.length(), parent_hu_status.ptr(),
                                 sub_hu_status.length(), sub_hu_status.ptr(),
                                 proxy_info.new_binary_version_,
                                 (RELEASE_5U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 (RELEASE_6U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 (RELEASE_7U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 rc_status.length(), rc_status.ptr(),
                                 proxy_info.info_,

                                 proxy_info.proxy_id_,
                                 proxy_info.app_name_,
                                 proxy_info.binary_version_, proxy_info.config_version_,
                                 proxy_info.uname_info_, proxy_info.current_pid_,
                                 hu_cmd.length(), hu_cmd.ptr(),
                                 parent_hu_status.length(), parent_hu_status.ptr(),
                                 sub_hu_status.length(), sub_hu_status.ptr(),
                                 proxy_info.new_binary_version_,
                                 (RELEASE_5U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 (RELEASE_6U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 (RELEASE_7U == kernel_release ? proxy_info.new_binary_md5_ : null_str),
                                 rc_status.length(), rc_status.ptr(),
                                 proxy_info.info_);
    int64_t affected_rows = -1;
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= sql_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(sql_len), K(ret));
    } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(sql), K(affected_rows), K(ret));
    }
  }
  if (OB_LIKELY(NULL != sql)) {
    op_fixed_mem_free(sql, sql_len);
  }
  return ret;
};

int ObProxyTableProcessorUtils::add_proxy_kv_table_info(ObMysqlProxy &mysql_proxy,
    const char *appname)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(appname)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(appname), K(ret));
  } else {
    char sql[OB_SHORT_SQL_LENGTH];
    const ObString &bu_status = ObHotUpgraderInfo::get_bu_status_string(BUS_UPGRADE_OFF);
    const char *batch_upgrade_status_info = "set it [upgrade_off | upgrade_part | upgrade_on] to"
                                            "upgrade some proxy of this app";
    const int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, INSERT_PROXY_KV_TABLE_INFO_SQL,
                                 ObProxyTableInfo::PROXY_KV_TABLE_NAME,
                                 appname,
                                 ObVarcharType,
                                 bu_status.length(), bu_status.ptr(),
                                 batch_upgrade_status_info);
    int64_t affected_rows = -1;
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(OB_SHORT_SQL_LENGTH), K(ret));
    } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
      if (-ER_DUP_ENTRY == ret) {
        LOG_INFO("appname has already be added", K(sql), K(appname), K(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      }
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(sql), K(affected_rows), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
};

int ObProxyTableProcessorUtils::get_config_version(ObMysqlProxy &mysql_proxy, int64_t &version)
{
  int ret = OB_SUCCESS;
  ObMysqlResultHandler result_handler;
  char sql[OB_SHORT_SQL_LENGTH];
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, GET_CONFIG_VERSION_VALUE_SQL,
                      ObProxyTableInfo::PROXY_KV_TABLE_NAME,
                      get_global_proxy_config().app_name_str_,
                      ObProxyTableInfo::PROXY_CONFIG_VERSION_NAME.length(),
                      ObProxyTableInfo::PROXY_CONFIG_VERSION_NAME.ptr(),
                      INT64_MAX);
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_FAIL(mysql_proxy.read(sql, result_handler))) {
    LOG_WARN("fail to read proxy info", K(sql), K(ret));
  } else {
    if (OB_FAIL(result_handler.next())) {
      if (OB_ITER_END == ret) { //no record
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no result", K(sql), K(ret));
      } else {
        LOG_WARN("fail to get result", K(sql), K(ret));
      }
    } else {
      int64_t value = 0;
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "value", value, int64_t);
      //check if this is only one
      if (OB_SUCC(ret)) {
        if (OB_ITER_END != (ret = result_handler.next())) {
          LOG_WARN("fail to get result, there is more than one record", K(sql), K(value), K(ret));
          ret = OB_ERR_UNEXPECTED;
        } else {
          version = value;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

// get proxy info from ObProxyTableInfo::PROXY_KV_TABLE_NAME
int ObProxyTableProcessorUtils::get_vip_tenant_info(ObMysqlProxy &mysql_proxy,
    ObVipTenantCache::VTHashMap &cache_map)
{
  int ret = OB_SUCCESS;
  ObMysqlResultHandler result_handler;
  char sql[OB_SHORT_SQL_LENGTH];
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, GET_PROXY_ALL_VIP_TENANT_SQL,
                         ObProxyTableInfo::PROXY_VIP_TENANT_TABLE_NAME, INT64_MAX);
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_FAIL(mysql_proxy.read(sql, result_handler))) {
    LOG_WARN("fail to read all vip tenant", K(sql), K(ret));
  } else if (OB_FAIL(fill_local_vt_cache(result_handler, cache_map))) {
    LOG_WARN("fail to fill vip tenant cache", K(ret));
  } else {
    LOG_DEBUG("succ to vip tenant info", "count", cache_map.count());
  }
  return ret;
}

int ObProxyTableProcessorUtils::fill_local_vt_cache(ObMysqlResultHandler &result_handler,
    ObVipTenantCache::VTHashMap &cache_map)
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_str_len = 0;
  int64_t vid = 0;
  int64_t vport = 0;
  char vip[OB_IP_STR_BUFF];
  vip[0] = '\0';
  ObString tenant_name;
  ObString cluster_name;
  ObString info;
  ObVipTenant *vip_tenant = NULL;
  int64_t request_target_type = -1;
  int64_t rw_type = -1;
  json::Value *info_config = NULL;

  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
  json::Parser parser;

  if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  }

  while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "vid", vid, int64_t);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "vip", vip, OB_IP_STR_BUFF, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "vport", vport, int64_t);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "tenant_name", tenant_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "cluster_name", cluster_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "info", info);

    if (OB_SUCC(ret) && !info.empty()) {
      if (OB_FAIL(parser.parse(info.ptr(), info.length(), info_config))) {
        LOG_WARN("parse json failed", K(ret), K(info));
      } if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(info_config, json::JT_OBJECT))) {
        LOG_WARN("check config info type failed", K(ret));
      } else {
        DLIST_FOREACH(it, info_config->get_object()) {
          if (it->name_ == JSON_REQUEST_TARGET) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, json::JT_NUMBER))) {
              LOG_WARN("check config info type failed", K(ret));
            } else {
              request_target_type = it->value_->get_number();
            }
          } else if (it->name_ == JSON_RW_TYPE) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it->value_, json::JT_NUMBER))) {
              LOG_WARN("check config info type failed", K(ret));
            } else {
              rw_type = it->value_->get_number();
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(vip_tenant = op_alloc(ObVipTenant))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObVipTenant", K(ret));
      } else if (FALSE_IT(vip_tenant->vip_addr_.set(ObAddr::convert_ipv4_addr(vip),
          static_cast<int32_t>(vport), vid))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set vip_addr", K(vip), K(vport), K(vid), K(ret));
      } else if (OB_FAIL(vip_tenant->set_tenant_cluster(tenant_name, cluster_name))) {
        LOG_WARN("fail to set tenant cluster for ObVipTenant", K(tenant_name), K(cluster_name), K(ret));
      } else if (OB_FAIL(vip_tenant->set_request_target_type(request_target_type))) {
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_FAIL(vip_tenant->set_rw_type(rw_type))) {
        LOG_WARN("unexpected error", K(ret));
      } else if (OB_UNLIKELY(!vip_tenant->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid input value", KPC(vip_tenant), K(ret));
      } else if (OB_FAIL(cache_map.unique_set(vip_tenant))) {
        LOG_WARN("fail to insert one vip_tenant into cache_map", K(*vip_tenant), K(ret));
      } else {
        LOG_DEBUG("succ to insert one vip_tenant into cache_map", K(*vip_tenant));
        vip_tenant = NULL;
        vid = 0;
        vip[0] = '\0';
        vport = 0;
        tenant_name.reset();
        cluster_name.reset();
        info_config = NULL;
        request_target_type = -1;
        rw_type = -1;
      }
    }//end if OB_SUCCESS
  }//end of while

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
    LOG_DEBUG("succ to fill local vt cache", K(ret));
  } else {
    if (OB_LIKELY(NULL != vip_tenant)) {
      op_free(vip_tenant);
      vip_tenant = NULL;
    }
    if (OB_LIKELY(NULL != info_config)) {
      info_config = NULL;
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::get_proxy_local_addr(ObAddr &addr)
{
  int ret = OB_SUCCESS;
  char ip_str[OB_IP_STR_BUFF] = {'\0'};
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  in_port_t port = 0;
  ObIpAddr inbound_ip;

  // get inbound ip and port
  if (OB_LIKELY(info.is_inherited_)) {
    if (OB_UNLIKELY(NO_FD == info.fd_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this process is inherited born, but listen fd is invalid",
               "is_inherited", info.is_inherited_, "fd", info.fd_, K(ret));
    } else {
      struct sockaddr sock_addr;
      int64_t namelen = sizeof(sock_addr);
      memset(&sock_addr, 0, namelen);
      if (OB_FAIL(ObSocketManager::getsockname(info.fd_, &sock_addr , &namelen))) {
        LOG_WARN("fail to get sock name", K(info.fd_), K(ret));
      } else {
        struct sockaddr_in *ain = (sockaddr_in *)&sock_addr;
        inbound_ip.assign(sock_addr);
        port =static_cast<in_port_t>((ntohs)(ain->sin_port));
      }
    }
  } else { // init from config
    port = static_cast<in_port_t>(get_global_proxy_config().listen_port);
    int ip_ret = 0;
    obsys::CRLockGuard guard(get_global_proxy_config().rwlock_);
    if (OB_UNLIKELY(0 != (ip_ret = inbound_ip.load(get_global_proxy_config().local_bound_ip)))) {
      ret = OB_ERR_UNEXPECTED;
      const char *local_bound_ip = get_global_proxy_config().local_bound_ip;
      LOG_WARN("fail to load local bound ip", K(ip_ret), K(local_bound_ip), K(ret));
    }
  }

  // check valid
  if (OB_SUCC(ret)) {
    inbound_ip.get_ip_str(ip_str, OB_IP_STR_BUFF);
    ObAddr any_addr(ObAddr::IPV4, INADDR_ANY_IP, port);
    ObAddr local_addr(ObAddr::IPV4, INADDR_LOOPBACK_IP, port);
    ObAddr inbound_addr(ObAddr::IPV4, ip_str, port);

    LOG_DEBUG("inboud_addr", K(ip_str));
    // preferably use user specified inbound addr;
    // if inbound_addr == 0.0.0.0 or 127.0.0.1, we treat it as not user specified ip,
    // and in this case, we get one local net addr;
    if (inbound_addr == any_addr || inbound_addr == local_addr) {
      memset(ip_str, 0, OB_IP_STR_BUFF);
      if (OB_FAIL(get_one_local_addr(ip_str, OB_IP_STR_BUFF))) {
        LOG_WARN("fail to get one local addr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    addr.set_ipv4_addr(ip_str, port);
    if (OB_UNLIKELY(!addr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid addr", K(addr), K(ret));
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_info(ObMysqlProxy &mysql_proxy,
                                                  ObProxyServerInfo &proxy_info,
                                                  const bool set_rollback/*false*/)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  const ObString &parent_hu_status = ObHotUpgraderInfo::get_status_string(proxy_info.parent_hu_status_);
  const ObString &sub_hu_status = ObHotUpgraderInfo::get_status_string(proxy_info.sub_hu_status_);
  char sql[OB_SHORT_SQL_LENGTH];
  if (OB_UNLIKELY(set_rollback)) {
    const ObString &rollback_cmd = ObHotUpgraderInfo::get_cmd_string(HUC_ROLLBACK);
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_INFO_WITH_HUS_AND_CMD_SQL,
                   ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                   proxy_info.proxy_id_,
                   proxy_info.app_name_, proxy_info.binary_version_,
                   proxy_info.config_version_,
                   proxy_info.current_pid_, proxy_info.info_,
                   parent_hu_status.length(), parent_hu_status.ptr(),
                   sub_hu_status.length(), sub_hu_status.ptr(),
                   rollback_cmd.length(), rollback_cmd.ptr(),
                   proxy_info.proxy_ip_,
                   proxy_info.proxy_port_);
  } else {
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_INFO_WITH_HUS_SQL,
                   ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                   proxy_info.proxy_id_,
                   proxy_info.app_name_, proxy_info.binary_version_,
                   proxy_info.config_version_,
                   proxy_info.current_pid_, proxy_info.info_,
                   parent_hu_status.length(), parent_hu_status.ptr(),
                   sub_hu_status.length(), sub_hu_status.ptr(),
                   proxy_info.proxy_ip_,
                   proxy_info.proxy_port_);
  }

  int64_t affected_rows = -1;
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(ret));
  } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(sql), K(affected_rows), K(ret));
  } else {}

  if (OB_SUCC(ret) || OB_UNLIKELY(-1 == affected_rows)) {
    info.last_parent_status_ = proxy_info.parent_hu_status_;
    info.last_sub_status_ = proxy_info.sub_hu_status_;
    LOG_DEBUG("update last hu status", "last_parent_status", parent_hu_status,
              "last_sub_status", sub_hu_status);
  }
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_table_config(ObMysqlProxy &mysql_proxy,
    const char *proxy_ip, const int32_t proxy_port, const int64_t config_version,
    const ObReloadConfigStatus rc_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_ip)
      || OB_UNLIKELY(proxy_port <= 0)
      || OB_UNLIKELY(rc_status < RCS_NONE && rc_status >= RCS_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(rc_status), K(ret));
  } else {
    char sql[OB_SHORT_SQL_LENGTH];
    int64_t affected_rows = -1;
    const ObString &rc_status_string = ObHotUpgraderInfo::get_rc_status_string(rc_status);
    const int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_TABLE_CONFIG_INFO_SQL,
                                 ObProxyTableInfo::PROXY_INFO_TABLE_NAME, config_version,
                                 rc_status_string.length(), rc_status_string.ptr(),
                                 proxy_ip, proxy_port);

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
    } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(sql), K(affected_rows), K(ret));
    } else { }
  }
  return ret;
};

int ObProxyTableProcessorUtils::get_proxy_update_hu_cmd_sql(char *sql_buf,
                                                            const int64_t buf_len,
                                                            const char *proxy_ip,
                                                            const int32_t proxy_port,
                                                            const ObHotUpgradeCmd target_cmd,
                                                            const ObHotUpgradeCmd orig_cmd,
                                                            const ObHotUpgradeCmd orig_cmd_either,
                                                            const ObHotUpgradeCmd orig_cmd_another/*HUC_MAX*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_ISNULL(proxy_ip)
      || OB_UNLIKELY(proxy_port <= 0)
      || OB_UNLIKELY(target_cmd < HUC_NONE && target_cmd >= HUC_MAX)
      || OB_UNLIKELY(orig_cmd < HUC_NONE && orig_cmd >= HUC_MAX)
      || OB_UNLIKELY(orig_cmd_either < HUC_NONE && orig_cmd_either > HUC_MAX)
      || OB_UNLIKELY(orig_cmd_another < HUC_NONE && orig_cmd_another > HUC_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_buf), K(buf_len), K(proxy_ip), K(proxy_port), K(target_cmd),
                                 K(orig_cmd), K(orig_cmd_either), K(ret));
  } else {
    const ObString &target_cmd_string = ObHotUpgraderInfo::get_cmd_string(target_cmd);
    const ObString &orig_cmd_string = ObHotUpgraderInfo::get_cmd_string(orig_cmd);
    const ObString &orig_cmd_either_string = ObHotUpgraderInfo::get_cmd_string(orig_cmd_either);
    int64_t len = 0;
    if (HUC_MAX == orig_cmd_another) {
      len = snprintf(sql_buf, buf_len, UPDATE_PROXY_HOT_UPGRADE_CMD_EITHER_SQL,
                     ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                     target_cmd_string.length(), target_cmd_string.ptr(),
                     proxy_ip,
                     proxy_port,
                     orig_cmd_string.length(), orig_cmd_string.ptr(),
                     orig_cmd_either_string.length(), orig_cmd_either_string.ptr());
    } else {
      const ObString &orig_cmd_another_string = ObHotUpgraderInfo::get_cmd_string(orig_cmd_another);
      len = snprintf(sql_buf, buf_len, UPDATE_PROXY_HOT_UPGRADE_CMD_ANOTHER_SQL,
                     ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                     target_cmd_string.length(), target_cmd_string.ptr(),
                     proxy_ip,
                     proxy_port,
                     orig_cmd_string.length(), orig_cmd_string.ptr(),
                     orig_cmd_either_string.length(), orig_cmd_either_string.ptr(),
                     orig_cmd_another_string.length(), orig_cmd_another_string.ptr());
    }

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql_buf), K(ret));
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_hu_cmd(ObMysqlProxy &mysql_proxy,
                                                    const char *proxy_ip,
                                                    const int32_t proxy_port,
                                                    const ObHotUpgradeCmd target_cmd,
                                                    const ObHotUpgradeCmd orig_cmd,
                                                    const ObHotUpgradeCmd orig_cmd_either,
                                                    const ObHotUpgradeCmd orig_cmd_another/*HUC_MAX*/)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = -1;
  char sql[OB_SHORT_SQL_LENGTH];
  if (OB_ISNULL(proxy_ip)
      || OB_UNLIKELY(proxy_port <= 0)
      || OB_UNLIKELY(target_cmd < HUC_NONE && target_cmd >= HUC_MAX)
      || OB_UNLIKELY(orig_cmd < HUC_NONE && orig_cmd >= HUC_MAX)
      || OB_UNLIKELY(orig_cmd_either < HUC_NONE && orig_cmd_either > HUC_MAX)
      || OB_UNLIKELY(orig_cmd_another < HUC_NONE && orig_cmd_another > HUC_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(target_cmd),
                                 K(orig_cmd), K(orig_cmd_either), K(orig_cmd_another), K(ret));
  } else if (OB_FAIL(get_proxy_update_hu_cmd_sql(sql, OB_SHORT_SQL_LENGTH, proxy_ip,
      proxy_port, target_cmd, orig_cmd, orig_cmd_either, orig_cmd_another))) {
    LOG_WARN("fail to get proxy update hu cmd sql", K(sql), K(OB_SHORT_SQL_LENGTH), K(proxy_ip),
             K(proxy_port), K(target_cmd), K(orig_cmd), K(orig_cmd_either), K(orig_cmd_another), K(ret));
  } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
    LOG_WARN("fail to execute sql", K(sql), K(orig_cmd), K(target_cmd), K(ret));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(sql), K(orig_cmd), K(target_cmd), K(affected_rows), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_status_and_cmd(ObMysqlProxy &mysql_proxy,
                                                            const char *proxy_ip,
                                                            const int32_t proxy_port,
                                                            const ObHotUpgradeStatus parent_status,
                                                            const ObHotUpgradeStatus sub_status,
                                                            const ObHotUpgradeCmd cmd/*HUC_MAX*/,
                                                            const bool retries_too_many/*false*/)
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_ISNULL(proxy_ip) || OB_UNLIKELY(proxy_port <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(ret));
  } else {
    const ObString &parent_status_string = ObHotUpgraderInfo::get_status_string(parent_status);
    const ObString &sub_status_string = ObHotUpgraderInfo::get_status_string(sub_status);
    char sql[OB_SHORT_SQL_LENGTH];
    int64_t len = 0;

    if (OB_UNLIKELY(retries_too_many)) {
      //merge new failed str, like "fetch binary failed, failure retries too many"
      char merge_str[OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN];
      const ObString &last_parent_status = ObHotUpgraderInfo::get_status_string(info.last_parent_status_);
      len = snprintf(merge_str, OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN, "%.*s, %.*s",
                     last_parent_status.length(), last_parent_status.ptr(),
                     parent_status_string.length(), parent_status_string.ptr());
      if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to merge_str for retries too many", K(len), K(merge_str), K(ret));
      } else {
        const ObString &none_cmd = ObHotUpgraderInfo::get_cmd_string(HUC_NONE);
        len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_BOTH_HUS_AND_CMD_SQL,
                       ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                       strlen(merge_str), merge_str,
                       sub_status_string.length(), sub_status_string.ptr(),
                       none_cmd.length(), none_cmd.ptr(),
                       proxy_ip,
                       proxy_port);
      }

    } else if (OB_LIKELY(!ObHotUpgraderInfo::is_vailed_cmd(cmd))) {
      len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_BOTH_HUS_SQL,
                     ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                     parent_status_string.length(), parent_status_string.ptr(),
                     sub_status_string.length(), sub_status_string.ptr(),
                     proxy_ip,
                     proxy_port);

    } else {
      const ObString &cmd_string = ObHotUpgraderInfo::get_cmd_string(cmd);
      len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_BOTH_HUS_AND_CMD_SQL,
                     ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                     parent_status_string.length(), parent_status_string.ptr(),
                     sub_status_string.length(), sub_status_string.ptr(),
                     cmd_string.length(), cmd_string.ptr(),
                     proxy_ip,
                     proxy_port);
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = -1;
      if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
      } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(parent_status), K(sub_status), K(ret));
      } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(sql), K(parent_status), K(sub_status), K(affected_rows), K(ret));
      } else {}

      if (OB_SUCC(ret) || OB_UNLIKELY(-1 == affected_rows)) {
        LOG_DEBUG("update last hu status",
                  "last_parent_status", ObHotUpgraderInfo::get_status_string(info.last_parent_status_),
                  "last_sub_status", ObHotUpgraderInfo::get_status_string(info.last_sub_status_),
                  "parent_status", parent_status_string,
                  "sub_status", sub_status_string);
        info.last_parent_status_ = parent_status;
        info.last_sub_status_ = sub_status;
      }
     }
  }
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_exited_status(ObMysqlProxy &mysql_proxy,
                                                           const char *proxy_ip,
                                                           const int32_t proxy_port,
                                                           const ObHotUpgradeStatus parent_status,
                                                           const ObHotUpgradeCmd target_cmd)
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_ISNULL(proxy_ip) || OB_UNLIKELY(proxy_port <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(ret));
  } else {
    char sql[OB_SHORT_SQL_LENGTH];
    const ObString &target_cmd_string = ObHotUpgraderInfo::get_cmd_string(target_cmd);
    const ObString &parent_status_string = ObHotUpgraderInfo::get_status_string(parent_status);
    const int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_PARENT_HUS_AND_CMD_SQL,
                                 ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                                 parent_status_string.length(), parent_status_string.ptr(),
                                 target_cmd_string.length(), target_cmd_string.ptr(),
                                 proxy_ip,
                                 proxy_port);
    int64_t affected_rows = -1;
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
    } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(parent_status), K(target_cmd), K(ret));
    } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(sql), K(parent_status), K(target_cmd), K(affected_rows), K(ret));
    } else {
      LOG_DEBUG("update last parent hu status",
                "last_parent_status", ObHotUpgraderInfo::get_status_string(info.last_parent_status_),
                "curr parent_status", ObHotUpgraderInfo::get_status_string(parent_status));
      info.last_parent_status_ = parent_status;
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::update_proxy_update_time(ObMysqlProxy &mysql_proxy,
    const char *proxy_ip, const int32_t proxy_port)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy_ip) || OB_UNLIKELY(proxy_port <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy_ip), K(proxy_port), K(ret));
  } else {
    char sql[OB_SHORT_SQL_LENGTH];
    int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, UPDATE_PROXY_UPDATE_TIME_SQL,
                           ObProxyTableInfo::PROXY_INFO_TABLE_NAME,
                           proxy_ip,
                           proxy_port);
    int64_t affected_rows = -1;
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
    } else if (OB_FAIL(mysql_proxy.write(sql, affected_rows))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (OB_UNLIKELY(affected_rows < 0) || OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(sql), K(affected_rows), K(ret));
    } else { }
  }
  return ret;
}

int ObProxyTableProcessorUtils::get_one_local_addr(char *addr_str, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(addr_str) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", "ip addr", addr_str, K(len), K(ret));
  } else {
    ObIpAddr addr_arr[MAX_LOCAL_ADDR_LIST_COUNT];
    bool is_need_all = false;
    int64_t valid_cnt = 0;
    // 1. get local ip by hostname
    if (OB_FAIL(net::get_local_addr_list(addr_arr, net::MAX_LOCAL_ADDR_LIST_COUNT,
                                         is_need_all, valid_cnt))) {
      LOG_WARN("fail to get_local_addr_list", K(valid_cnt), K(ret));
    }

    if (OB_FAIL(ret) || OB_UNLIKELY(valid_cnt <= 0)) {
      LOG_INFO("fail to get local addr list, maybe hostname is not correctly set",
               K(valid_cnt), K(ret));
      ret = OB_SUCCESS;
      valid_cnt = 0;
      // 2. get local ip by nic
      if (OB_FAIL(net::get_local_addr_list_by_nic(addr_arr, net::MAX_LOCAL_ADDR_LIST_COUNT,
                                                  is_need_all, valid_cnt))) {
        LOG_WARN("fail to get local addr list by nic", K(valid_cnt), K(ret));
      } else if (OB_UNLIKELY(valid_cnt <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get local addr list through hostname and nic, please check"
                 "whether hostname or netdevice is correctly set, and has valid ip addrs"
                 "(exclude 0.0.0.0 and 127.0.0.1)", K(valid_cnt), K(ret));
      }
    }

    if (OB_SUCC(ret) && valid_cnt > 0) {
      // sort, descending order
      std::sort(addr_arr, addr_arr + valid_cnt);
      addr_arr[0].get_ip_str(addr_str, len); // choose the first one

      // just used to check valid
      int32_t fake_port = 1;
      ObAddr addr(ObAddr::IPV4, addr_str, fake_port);
      // just defense, we can not use ip like 127.0.0.1 and 0.0.0.0
      ObAddr localhost_addr(ObAddr::IPV4, INADDR_LOOPBACK_IP, fake_port);
      ObAddr any_addr(ObAddr::IPV4, INADDR_ANY_IP, fake_port);
      if (OB_UNLIKELY(!addr.is_valid()) || addr == localhost_addr || addr == any_addr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ip addr", "ip addr", addr_str, K(ret));
      } else {
        LOG_DEBUG("get one local addr succ", "ip(ingore port)", addr);
      }
    }
  }
  return ret;
}

int ObProxyTableProcessorUtils::get_uname_info(char *info, const int64_t info_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info) || OB_UNLIKELY(info_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(info), K(info_len), K(ret));
  } else {
    struct utsname u;
    int fb = 0;
    if (OB_UNLIKELY(0 != (fb = uname(&u)))) {
      ret = OB_ERR_SYS;
      LOG_WARN("fail to get uname info", KERRMSGS, K(ret));
    } else {
      ObConfigServerProcessor &config_server_processor = get_global_config_server_processor();
      int64_t length = -1;
      ObProxyKernelRelease kernel_release = RELEASE_MAX;

      if (config_server_processor.is_init()
          && (RELEASE_MAX != (kernel_release = config_server_processor.get_kernel_release()))) {
        const ObString &kernel_release_string = get_kernel_release_string(kernel_release);
        length = snprintf(info, info_len, "(%s)-(%s)-([%s] %s)-(%s)-(%s)",
                          u.sysname, u.nodename,
                          kernel_release_string.ptr(),
                          u.release, u.version, u.machine);
      } else {
        length = snprintf(info, info_len, "(%s)-(%s)-(%s)-(%s)-(%s)",
                          u.sysname, u.nodename, u.release, u.version, u.machine);
      }

      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= info_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enought", K(length), K(info_len), K(info), K(ret));
      }
    }
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

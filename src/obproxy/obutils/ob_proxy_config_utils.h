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

#ifndef OBPROXY_CONFIG_UTILS_H
#define OBPROXY_CONFIG_UTILS_H
#include "lib/ob_define.h"
#include "lib/lock/Mutex.h"

namespace oceanbase
{
namespace common
{
class ObConfigItem;
}
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
}

namespace obutils
{
static const char *const CFG_DUMP_NAME                   = "./obproxy_config.bin";
static const char *const CFG_SERVER_INFO_DUMP_NAME       = "./obproxy_config_server_info.json";
static const char *const CFG_SERVER_SHARD_INFO_DUMP_NAME = "./obproxy_shard_config_server_info.json";
static const char *const CFG_RSLIST_INFO_DUMP_NAME       = "./obproxy_rslist_info.json";
static const char *const CFG_IDC_LIST_INFO_DUMP_NAME     = "./obproxy_idc_list_info.json";

class ObProxyConfig;
class ObProxyConfigUtils
{
public:
  // covert config item to sql, and write sql to file
  static int dump_config_update_sql(const char *file_dir = "./");

  // insert config item to ObProxyTableInfo::PROXY_CONFIG_TABLE_NAME directly
  static int execute_config_update_sql(proxy::ObMysqlProxy &sql_proxy);

  // dump obproxy config to file
  static int dump2file(const ObProxyConfig &proxy_config);
  static int load_config_from_file(ObProxyConfig &proxy_config);
  static bool is_user_visible(const common::ObConfigItem &item);
  static bool is_memory_visible(const common::ObConfigItem &item);

private:
  static int get_config_item_sql(const common::ObConfigItem &item, char *str, const int64_t len);
  static int get_insert_all_proxy_upgrade_sql(char *str, const int64_t len, int64_t &affected_rows);
  static int get_insert_vip_tenant_version_sql(char *str, const int64_t len);

  static int prepare_sql_file(const char *file_dir, int &file_fd);
  static int write_sql_to_file(const int fd, const char *sql);

  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigUtils);
};

/* ObProxyFileUtils is used to dump proxy config and proxy config server json info
 * into local file or load them from file. In order to make sure that the files
 * wont't be deleted manually by mistake, we should write them into both etc dir
 * and conf dir
*/
class ObProxyFileUtils
{
public:
  static int write(const char *file_name, const char *buf, const int64_t len);
  static int read(const char *file_name, char *buf,
                  const int64_t len, int64_t &read_len);
  static int get_file_size(const char *file_name, int64_t &size);

  static int write_to_file(const char *dir, const char *file_name,
                           const char *buf, const int64_t len,
                           const bool need_backup = true);
  static int read_from_file(const char *path, const char *file_name,
                            char *buf, const int64_t len, int64_t &read_len);

  static int calc_file_size(const char *path, const char *file_name, int64_t &size);

  static int move_file_dir(const char *src_dir, const char *dst_dir);

  static void clear_dir(const char *dir);
private:
  static int write_and_backup_file(const char *path, const char *tmp_path, const char *old_path,
                                   const char *buf, const int64_t len, const bool need_backup);

  DISALLOW_COPY_AND_ASSIGN(ObProxyFileUtils);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_CONFIG_UTILS_H

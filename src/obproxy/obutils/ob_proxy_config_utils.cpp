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
#include "obutils/ob_proxy_config_utils.h"
#include <sys/stat.h>
#include "lib/file/ob_file.h"
#include "utils/ob_proxy_table_define.h"
#include "utils/ob_layout.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/client/ob_mysql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
static const ObString LOCAL_DIR = ObString::make_string(".");
static const ObString PARENT_DIR = ObString::make_string("..");

const char *INSERT_KV_TABLE_VERSION_SQL =
    "INSERT INTO %s "
    "(name, datatype, value, info) VALUES ('%s', %d, '%ld', '%s') "
    "ON DUPLICATE KEY UPDATE name = VALUES(name);\n";

const char *INSERT_KV_TABLE_ALL_PROXY_UPGRADE_SQL =
    "INSERT INTO %s "
    "(name, datatype, value, info) VALUES "
    "('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%ld', '%s'), "
    "('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%.*s', '%s'), "
    "('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%.*s', '%s'), ('%.*s%.*s', %d, '%.*s', '%s') "
    "ON DUPLICATE KEY UPDATE name = VALUES(name);\n";

const char *INSERT_CONFIG_TABLE_ALL_PROXY_SQL =
    "INSERT INTO %s "
    "(app_name, name, value, need_reboot, info) VALUES "
    "('all_proxy', '%s', '%s', '%s', '%s') "
    "ON DUPLICATE KEY UPDATE name = VALUES(name);\n";

int ObProxyConfigUtils::dump_config_update_sql(const char *file_dir)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char *sql = NULL;
  const int64_t len = OB_SHORT_SQL_LENGTH * 4;

  if (OB_ISNULL(file_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file dir", K(ret));
  } else if (OB_FAIL(prepare_sql_file(file_dir, fd))) {
    LOG_WARN("fail to prepare sql file", K(file_dir), K(ret));
  } else if (OB_ISNULL(sql = static_cast<char *>(op_fixed_mem_alloc(len)))) {// 4k
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    // create proxy info table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy table sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // create proxy config table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_config_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy config table sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // create proxy stat table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_stat_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy stat table sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // create proxy kv table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_kv_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy kv table sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // create proxy vip tenant table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_vip_tenant_table_sql(sql, len))) {
        LOG_WARN("fail to get create vip tenant table sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // insert vip tenant version into proxy kv table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_insert_vip_tenant_version_sql(sql, len))) {
        LOG_WARN("fail to get insert vip tenant version sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      }
    }

    // insert batch upgrade rows into proxy kv table
    if (OB_SUCC(ret)) {
      int64_t expect_affected_rows = 0;
      if (OB_FAIL(get_insert_all_proxy_upgrade_sql(sql, len, expect_affected_rows))) {
        LOG_WARN("fail to get insert all proxy upgrade sql", K(len), K(ret));
      } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
        LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
      } else {/*do noting*/}
    }

    // insert config items into proxy config table
    if (OB_SUCC(ret)) {
      ObProxyConfig *all_proxy_config = NULL;
      if (OB_ISNULL(all_proxy_config = new (std::nothrow) ObProxyConfig())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new memory for ObProxyConfig", K(ret));
      } else {
        const ObConfigContainer &container = all_proxy_config->get_container();
        ObConfigContainer::const_iterator it = container.begin();
        ObConfigItem *item = NULL;
        const ObString USER_LEVEL(OB_CONFIG_VISIBLE_LEVEL_USER);
        for (; OB_SUCC(ret) && it != container.end(); ++it) {
          if (OB_ISNULL(item = it->second)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error happened, config item must not be null", K(ret));
          } else if (USER_LEVEL == item->visible_level()) {
            if (OB_FAIL(get_config_item_sql(*item, sql, len))) {
              LOG_WARN("fail to get config item sql", K(len), K(ret));
            } else if (OB_FAIL(write_sql_to_file(fd, sql))) {
              LOG_ERROR("fail to write sql to file", K(sql), K(fd), K(ret));
            }
          }
        }
      }

      if (OB_LIKELY(NULL != all_proxy_config)) {
        delete all_proxy_config;
        all_proxy_config = NULL;
      }
    }
  }

  if (OB_LIKELY(NULL != sql)) {
    op_fixed_mem_free(sql, len);
    sql = NULL;
  }

  if (OB_LIKELY(fd >= 0) && OB_UNLIKELY(0 != close(fd))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to close file fd", K(fd), KERRMSGS, K(ret));
  }
  return ret;
}

int ObProxyConfigUtils::prepare_sql_file(const char *file_dir, int &file_fd)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int64_t pos = 0;
  char sql_file[OB_MAX_FILE_NAME_LENGTH];
  sql_file[0] = '\0';
  file_fd = -1;

  if (OB_ISNULL(file_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file dir", K(file_dir), K(ret));
  } else if (OB_FAIL(databuff_printf(sql_file, OB_MAX_FILE_NAME_LENGTH, pos, "%s", file_dir))) {
    LOG_WARN("fail to append sql file dir", K(ret));
  } else if (OB_FAIL(common::FileDirectoryUtils::create_full_path(sql_file))) {
    LOG_ERROR("fail to create sql file dir", K(file_dir), K(ret));
  } else if (OB_FAIL(databuff_printf(sql_file, OB_MAX_FILE_NAME_LENGTH, pos, "%s", "obproxy_config_update.sql"))) {
    LOG_WARN("fail to append sql file name", K(ret));
  } else if (OB_UNLIKELY((fd = open(sql_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)) < 0)) {
    ret = OB_IO_ERROR;
    LOG_WARN("can't open sql file", KERRMSGS, K(sql_file), K(ret));
  } else if (OB_UNLIKELY(lockf(fd, F_TLOCK, 0) < 0)) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to lock sql file", KERRMSGS, K(ret));
  } else if (OB_UNLIKELY(ftruncate(fd, 0) < 0)) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to truncate sql file", KERRMSGS, K(ret));
  } else {
    file_fd = fd;
  }

  return ret;
}

int ObProxyConfigUtils::write_sql_to_file(const int fd, const char *sql)
{
  int ret = OB_SUCCESS;
  int64_t sql_len = 0;

  if (OB_ISNULL(sql) || OB_UNLIKELY((sql_len = STRLEN(sql)) <= 0) || OB_UNLIKELY(fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql), K(fd), K(ret));
  } else if (OB_UNLIKELY(sql_len != ::write(fd, sql, sql_len))) {
    ret =  OB_IO_ERROR;
    LOG_WARN("write sql file error", KERRMSGS, K(sql), K(ret));
  }

  return ret;
}

int ObProxyConfigUtils::execute_config_update_sql(ObMysqlProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const int64_t len = OB_SHORT_SQL_LENGTH * 4;// 4k
  char *sql = NULL;

  if (OB_ISNULL(sql = static_cast<char *>(op_fixed_mem_alloc(len)))) {// 4k
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    int64_t affected_rows = -1;

    // create proxy info table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy table sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 0", K(sql), K(affected_rows), K(ret));
      }
    }

    // create proxy config table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_config_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy config table sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 0", K(sql), K(affected_rows), K(ret));
      }
    }

    // create proxy stat table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_stat_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy stat table sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 0", K(sql), K(affected_rows), K(ret));
      }
    }

    // create proxy kv table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_kv_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy kv table sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 0", K(sql), K(affected_rows), K(ret));
      }
    }

    // create proxy vip tenant table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      if (OB_FAIL(ObProxyTableInfo::get_create_proxy_vip_tenant_table_sql(sql, len))) {
        LOG_WARN("fail to get create proxy vip tenant table sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 0", K(sql), K(affected_rows), K(ret));
      }
    }

    // insert vip tenant version into proxy kv table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      if (OB_FAIL(get_insert_vip_tenant_version_sql(sql, len))) {
        LOG_WARN("fail to get insert vip tenant version sql", K(len), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(1 != affected_rows) && OB_UNLIKELY(0 != affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be 1 or 0", K(sql),
                 K(affected_rows), K(ret));
      }
    }

    // insert batch upgrade rows into proxy kv table
    if (OB_SUCC(ret)) {
      affected_rows = -1;
      int64_t expect_affected_rows = -1;
      if (OB_FAIL(get_insert_all_proxy_upgrade_sql(sql, len, expect_affected_rows))) {
        LOG_WARN("fail to get insert all proxy upgrade sql", K(len), K(sql), K(ret));
      } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_UNLIKELY(affected_rows > expect_affected_rows) || OB_UNLIKELY(affected_rows < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in this scenario, affeted_rows must be lower than expect_affected_rows", K(sql),
                 K(affected_rows), K(expect_affected_rows), K(ret));
      }
    }

    // insert config items into proxy config table
    if (OB_SUCC(ret)) {
      ObProxyConfig *all_proxy_config = NULL;
      if (OB_ISNULL(all_proxy_config = new (std::nothrow) ObProxyConfig())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new memory for ObProxyConfig", K(ret));
      } else {
        const ObConfigContainer &container = all_proxy_config->get_container();
        ObConfigContainer::const_iterator it = container.begin();
        ObConfigItem *item = NULL;
        const ObString USER_LEVEL(OB_CONFIG_VISIBLE_LEVEL_USER);
        for (; OB_SUCC(ret) && it != container.end(); ++it) {
          affected_rows = -1;
          if (OB_ISNULL(item = it->second)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error happened, config item must not be null", K(ret));
          } else if (USER_LEVEL == item->visible_level()) {
            if (OB_FAIL(get_config_item_sql(*item, sql, len))) {
              LOG_WARN("fail to get config item sql", K(len), K(ret));
            } else if (OB_FAIL(sql_proxy.write(sql, affected_rows))) {
              LOG_WARN("fail to execute sql", K(sql), K(ret));
            } else if (OB_UNLIKELY(1 != affected_rows) && OB_UNLIKELY(0 != affected_rows)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("in this scenario, affeted_rows must either 1 or 0",
                       K(sql), K(affected_rows), K(ret));
            }
          }
        }
      }

      if (OB_LIKELY(NULL != all_proxy_config)) {
        delete all_proxy_config;
        all_proxy_config = NULL;
      }
    }
  }

  if (OB_LIKELY(NULL != sql)) {
    op_fixed_mem_free(sql, len);
    sql = NULL;
  }

  return ret;
}

int ObProxyConfigUtils::get_insert_vip_tenant_version_sql(char *str, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_ISNULL(str) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str), K(len), K(ret));
  } else {
    ObObj default_value;
    default_value.set_int(ObProxyTableInfo::PROXY_VIP_TENANT_VERSION_DEFAULT_VALUE);

    if (OB_FAIL(databuff_printf(str, len, pos, INSERT_KV_TABLE_VERSION_SQL,
                                ObProxyTableInfo::PROXY_KV_TABLE_NAME,
                                ObProxyTableInfo::PROXY_VIP_TENANT_VERSION_NAME,
                                default_value.get_type(),
                                default_value.get_int(),
                                "obproxy newest vip tenant version"))) {
      LOG_WARN("fail to append config version sql", K(ret));
    }
  }
  return ret;
}

int ObProxyConfigUtils::get_insert_all_proxy_upgrade_sql(char *str, const int64_t len,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str), K(len), K(ret));
  } else {
    const int64_t ALL_PROXY_BINARY_MAX_SIZE = 9;
    const char *all_proxy_upgrade_info[ALL_PROXY_BINARY_MAX_SIZE] = {
        "upgrade switch for all proxy, value must be upgrade_off | upgrade_on",
        "upgrade start time for all proxy, value must like xx:xx e.g. 01:10, or null",
        "upgrade duration minutes for all proxy, value set to 60 means 60 minutes, or null",
        "5u binary name for all proxy",
        "6u binary name for all proxy",
        "7u binary name for all proxy",
        "5u binary md5 for all proxy",
        "6u binary md5 for all proxy",
        "7u binary md5 for all proxy",
    };
    int64_t pos = 0;
    ObObj string_value;
    string_value.set_varchar(NULL, 0);
    ObObj int_value;
    int_value.set_int(ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_DEFAULT_VALUE);
    const ObString varchar = string_value.get_varchar();
    const ObString *bianry_name = ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_NAME;
    const ObString *bianry_md5 = ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_MD5;
    const ObString &all_proxy  = ObProxyTableInfo::PROXY_ALL_PROXY_HEADER;
    if (OB_FAIL(databuff_printf(str, len, pos, INSERT_KV_TABLE_ALL_PROXY_UPGRADE_SQL,
                                ObProxyTableInfo::PROXY_KV_TABLE_NAME,
                                all_proxy.length(), all_proxy.ptr(),
                                ObProxyTableInfo::PROXY_UPGRADE_SWITCH_NAME.length(),
                                ObProxyTableInfo::PROXY_UPGRADE_SWITCH_NAME.ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[0],

                                all_proxy.length(), all_proxy.ptr(),
                                ObProxyTableInfo::PROXY_UPGRADE_START_TIME_NAME.length(),
                                ObProxyTableInfo::PROXY_UPGRADE_START_TIME_NAME.ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[1],

                                all_proxy.length(), all_proxy.ptr(),
                                ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_NAME.length(),
                                ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_NAME.ptr(),
                                int_value.get_type(), int_value.get_int(),
                                all_proxy_upgrade_info[2],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_name[RELEASE_5U].length(), bianry_name[RELEASE_5U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[3],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_name[RELEASE_6U].length(), bianry_name[RELEASE_6U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[4],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_name[RELEASE_7U].length(), bianry_name[RELEASE_7U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[5],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_md5[RELEASE_5U].length(), bianry_md5[RELEASE_5U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[6],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_md5[RELEASE_6U].length(), bianry_md5[RELEASE_6U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[7],

                                all_proxy.length(), all_proxy.ptr(),
                                bianry_md5[RELEASE_7U].length(), bianry_md5[RELEASE_7U].ptr(),
                                string_value.get_type(), varchar.length(), varchar.ptr(),
                                all_proxy_upgrade_info[8]))) {
      LOG_WARN("fail to append all proxy rows sql", K(len), K(pos), K(ret));
    } else {
      affected_rows = ALL_PROXY_BINARY_MAX_SIZE;
    }
  }
  return ret;
}

int ObProxyConfigUtils::get_config_item_sql(const ObConfigItem &item,
                                            char *str, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *need_reboot_str = (item.need_reboot() ? OB_CONFIG_NEED_REBOOT : OB_CONFIG_NOT_NEED_REBOOT);

  if (OB_ISNULL(str) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str), K(len), K(ret));
  } else if (OB_FAIL(databuff_printf(str, len, pos, INSERT_CONFIG_TABLE_ALL_PROXY_SQL,
                                     ObProxyTableInfo::PROXY_CONFIG_TABLE_NAME, item.name(), item.str(),
                                     need_reboot_str, item.info()))) {
    LOG_WARN("fail to append config item sql", K(ret));
  }

  return ret;
}

bool ObProxyConfigUtils::is_user_visible(const ObConfigItem &item)
{
   return ObString::make_string(OB_CONFIG_VISIBLE_LEVEL_USER) == item.visible_level();
}

bool ObProxyConfigUtils::is_memory_visible(const ObConfigItem &item)
{
   return ObString::make_string(OB_CONFIG_VISIBLE_LEVEL_MEMORY) == item.visible_level();
}

int ObProxyConfigUtils::dump2file(const ObProxyConfig &proxy_config)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  static ObMutex file_mutex = PTHREAD_MUTEX_INITIALIZER;
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;

  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(OB_PROXY_CONFIG_BUFFER_SIZE, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("ob tc malloc memory for buf fail", K(ret));
  } else {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    if (OB_FAIL(proxy_config.serialize(buf, OB_PROXY_CONFIG_BUFFER_SIZE, pos))) {
      LOG_WARN("fail to serialize proxy config", K(ret));
    } else if (OB_UNLIKELY(pos > OB_PROXY_CONFIG_BUFFER_SIZE)) {
      ret = OB_SERIALIZE_ERROR;
      LOG_WARN("fail to serialize", K(pos), K(OB_PROXY_CONFIG_BUFFER_SIZE), K(ret));
    } else {/*do nothing*/}
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(mutex_acquire(&file_mutex))) {
      LOG_ERROR("fail to acquire mutex", K(ret));
    } else {
      if (OB_FAIL(ObProxyFileUtils::write(CFG_DUMP_NAME, buf, pos))) {
        LOG_WARN("fail to write config bin to file", K(CFG_DUMP_NAME), K(ret));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mutex_release(&file_mutex)))) {
        LOG_ERROR("fail to release mutex", K(tmp_ret));
      }
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObProxyConfigUtils::load_config_from_file(ObProxyConfig &proxy_config)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t read_len = 0;
  int64_t pos = 0;
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;

  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(OB_PROXY_CONFIG_BUFFER_SIZE, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("ob tc malloc memory for buf fail", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read(CFG_DUMP_NAME, buf, OB_PROXY_CONFIG_BUFFER_SIZE, read_len))) {
    // no need to print warn log, the caller will do it
  } else {
    obsys::CWLockGuard guard(proxy_config.rwlock_);
    if (OB_FAIL(proxy_config.deserialize(buf, read_len, pos))) {
      LOG_WARN("fail to deserialize proxy config", K(ret));
    } else if (OB_UNLIKELY(pos != read_len)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialize proxy config failed", K(ret));
    } else {
      proxy_config.set_app_name(proxy_config.app_name);
    }
  }

  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

int ObProxyFileUtils::write(const char *file_name, const char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_to_file(get_global_layout().get_etc_dir(), file_name, buf, len))) {
    LOG_WARN("fail to write file to etc dir", K(file_name), K(ret));
  } else if (OB_FAIL(write_to_file(get_global_layout().get_conf_dir(), file_name, buf, len))) {
    LOG_WARN("fail to write file to conf dir", K(file_name), K(ret));
  }
  return ret;
}

int ObProxyFileUtils::write_to_file(const char *dir, const char *file_name,
                                    const char *buf, const int64_t len,
                                    const bool need_backup /*true*/)
{
  int ret = OB_SUCCESS;
  char *path = NULL;
  char *tmp_path = NULL;
  char *old_path = NULL;

  if (OB_ISNULL(dir) || OB_ISNULL(file_name)
      || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(dir), K(file_name), K(buf), K(len), K(ret));
  } else {
    int64_t path_len = 0;
    int64_t pos_tmp = 0;
    int64_t pos_old = 0;
    ObMemAttr mem_attr;
    mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(FileDirectoryUtils::create_full_path(dir))) {
      LOG_WARN("fail to makedir", K(dir), K(ret));
    } else if (OB_FAIL(ObLayout::merge_file_path(dir, file_name, allocator, path))) {
      LOG_WARN("fail to merge file path", K(file_name), K(ret));
    } else if (OB_ISNULL(path) || OB_UNLIKELY((path_len = STRLEN(path)) <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid path", K(path), K(ret));
    } else if (OB_ISNULL(tmp_path =  static_cast<char *>(ob_malloc(path_len + 8, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob malloc memory for tmp_path fail", K(ret));
    } else if (OB_FAIL(databuff_printf(tmp_path, path_len + 8, pos_tmp, "%s.tmp", path))) {
      LOG_WARN("fail to fill tmp_path", K(path), K(tmp_path), K(ret));
    } else if (OB_ISNULL(old_path =  static_cast<char *>(ob_malloc(path_len + 8, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob malloc memory for old_path fail", K(ret));
    } else if (OB_FAIL(databuff_printf(old_path, path_len + 8, pos_old, "%s.old", path))) {
      LOG_WARN("fail to fill old_path", K(path), K(old_path), K(ret));
    } else if (OB_FAIL(write_and_backup_file(path, tmp_path, old_path, buf, len, need_backup))) {
      LOG_WARN("fail to write and backup file", K(path), K(tmp_path), K(old_path), K(ret));
    } else {
      LOG_INFO("write file successfully!", K(path), K(old_path), K(len), K(ret));
    }
  }

  if (OB_LIKELY(NULL != tmp_path)) {
    ob_free(tmp_path);
    tmp_path = NULL;
  }
  if (OB_LIKELY(NULL != old_path)) {
    ob_free(old_path);
    old_path = NULL;
  }
  return ret;
}

int ObProxyFileUtils::write_and_backup_file(const char *path, const char *tmp_path,
    const char *old_path, const char *buf, const int64_t len, const bool need_backup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path) || OB_ISNULL(tmp_path) || OB_ISNULL(old_path)
      || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid path or buffer", K(path), K(tmp_path), K(old_path), K(buf), K(len), K(ret));
  } else {
    // Attention: when we use FileUtils to write file, we should set sync flag to false
    // otherwise it will sync all system io buffer to the write queue, which will cost much time
    int fd = -1;
    if (OB_UNLIKELY((fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)) < 0)) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to create file", KERRMSGS, K(tmp_path), K(ret));
    } else if (OB_UNLIKELY(len != unintr_write(fd, buf, len))) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to write buf to file", K(fd), KP(buf), K(len), KERRMSGS, K(tmp_path), K(ret));
    } else {
      bool is_old_file_existed = true;
      if (OB_UNLIKELY(0 != ::rename(path, old_path))) {
        if (ENOENT == errno) {
          //last file do not exist, ignore
          is_old_file_existed = false;
          LOG_INFO("last file do not exist, ignore", KERRMSGS,
                   "origial name", path);
        } else {
          ret = OB_ERR_SYS;
          LOG_WARN("fail to move file", KERRMSGS,
                   "origial name", path, "new name", old_path, K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(0 != ::rename(tmp_path, path))) {
          ret = OB_ERR_SYS;
          LOG_WARN("fail to move file", KERRMSGS,
                   "origial name", tmp_path, "new name", path, K(ret));

          //we need rollback the old file
          if (is_old_file_existed) {
            if (OB_UNLIKELY(0 != ::rename(old_path, path))) {
              LOG_WARN("fail to move file", KERRMSGS,
                       "origial name", old_path, "new name", path);
            }
          }
        } else if (!need_backup) {
          ::unlink(old_path);
        }
      }
    }
    if (fd > 0) {
      ::close(fd);
    }
  }
  return ret;
}

int ObProxyFileUtils::read(const char *file_name, char *buf, const int64_t len, int64_t &read_len)
{
  int ret = OB_SUCCESS;
  const char *dir = NULL;
  dir = get_global_layout().get_etc_dir();
  if (OB_FAIL(read_from_file(dir, file_name, buf, len, read_len))) {
    // if fail to read from etc dir, we can also try to read from .conf
    dir = get_global_layout().get_conf_dir();
    if (OB_FAIL(read_from_file(dir, file_name, buf, len, read_len))) {
     // no need to print warn log, the caller will do it
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succ to read file", K(dir), K(file_name), K(read_len), K(len));
  } else {
    // try use last backup file
    int64_t file_name_len = STRLEN(file_name);
    int64_t pos = 0;
    char *old_file_name = NULL;
    ObMemAttr mem_attr;
    mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;
    if (OB_ISNULL(old_file_name =  static_cast<char *>(ob_malloc(file_name_len + 8, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob malloc memory for old_file_name fail", K(ret));
    } else if (OB_FAIL(databuff_printf(old_file_name, file_name_len + 8, pos, "%s.old", file_name))) {
      LOG_WARN("fail to fill old_file_name", K(file_name), K(file_name_len), K(ret));
    } else {
      dir = get_global_layout().get_etc_dir();
      if (OB_FAIL(read_from_file(dir, old_file_name, buf, len, read_len))) {
        // if fail to read from etc dir, we can also try to read from .conf
        dir = get_global_layout().get_conf_dir();
        if (OB_FAIL(read_from_file(dir, old_file_name, buf, len, read_len))) {
         // no need to print warn log, the caller will do it
        }
      }
      if (OB_SUCC(ret)) {
        LOG_WARN("Attention: old file was read", K(dir), K(old_file_name), K(file_name), K(read_len), K(len));
      }
    }

    if (OB_LIKELY(NULL != old_file_name)) {
      ob_free(old_file_name);
      old_file_name = NULL;
    }
  }
  return ret;
}

int ObProxyFileUtils::read_from_file(const char *dir, const char *file_name,
                                     char *buf, const int64_t len, int64_t &read_len)
{
  int ret = OB_SUCCESS;
  read_len = 0;

  if (OB_ISNULL(dir) || OB_ISNULL(file_name)
      || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(dir), K(file_name), K(buf), K(len), K(ret));
  } else {
    char *path = NULL;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(ObLayout::merge_file_path(dir, file_name, allocator, path))) {
      LOG_WARN("fail to merge file path", K(file_name), K(ret));
    } else if (OB_ISNULL(path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid path", K(path), K(ret));
    } else {
      int fd = -1;
      if (OB_UNLIKELY((fd = ::open(path, O_RDONLY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0)) {
        ret = OB_IO_ERROR;
      } else if (OB_UNLIKELY((read_len = unintr_pread(fd, buf, len, 0)) < 0)) {
        ret = OB_IO_ERROR;
        LOG_WARN("read file error!", K(path), K(read_len), K(ret));
      } else if (OB_LIKELY(read_len < len)) {
        buf[read_len] = '\0';
      }
      if (fd > 0) ::close(fd);
    }
  }

  return ret;
}

int ObProxyFileUtils::get_file_size(const char *file_name, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_FAIL(calc_file_size(get_global_layout().get_etc_dir(), file_name, size))) {
    // if fail to calc file size from etc dir, we can also try to get from .conf
    if (OB_FAIL(calc_file_size(get_global_layout().get_conf_dir(), file_name, size))) {
     // no need to print warn log, the caller will do it
    }
  }

  if (OB_FAIL(ret)) {
    // try use last backup file
    int64_t file_name_len = STRLEN(file_name);
    int64_t pos = 0;
    char *old_file_name = NULL;
    const char *dir = NULL;
    ObMemAttr mem_attr;
    mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;
    if (OB_ISNULL(old_file_name =  static_cast<char *>(ob_malloc(file_name_len + 8, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob malloc memory for old_file_name fail", K(ret));
    } else if (OB_FAIL(databuff_printf(old_file_name, file_name_len + 8, pos, "%s.old", file_name))) {
      LOG_WARN("fail to fill old_file_name", K(file_name), K(file_name_len), K(ret));
    } else {
      dir = get_global_layout().get_etc_dir();
      if (OB_FAIL(calc_file_size(dir, old_file_name, size))) {
        // if fail to calc_file_size from etc dir, we can also try to get from .conf
        dir = get_global_layout().get_conf_dir();
        if (OB_FAIL(calc_file_size(dir, old_file_name, size))) {
         // no need to print warn log, the caller will do it
        }
      }
      if (OB_SUCC(ret)) {
        LOG_WARN("Attention: old file was calc size", K(dir), K(old_file_name), K(size), K(file_name));
      }
    }

    if (OB_LIKELY(NULL != old_file_name)) {
      ob_free(old_file_name);
      old_file_name = NULL;
    }
  }
  return ret;
}

int ObProxyFileUtils::calc_file_size(const char *dir, const char *file_name, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_ISNULL(dir) || OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(dir), K(file_name), K(ret));
  } else {
    char *path = NULL;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(ObLayout::merge_file_path(dir, file_name, allocator, path))) {
      LOG_WARN("fail to merge file path", K(file_name), K(ret));
    } else if (OB_ISNULL(path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid path", K(path), K(ret));
    } else {
      struct stat st;
      if (OB_UNLIKELY(0 != (stat(path, &st)))) {
        ret = OB_IO_ERROR;
      } else {
        size = st.st_size;
      }
    }
  }
  return ret;
}

int ObProxyFileUtils::move_file_dir(const char *src_dir, const char *dst_dir)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_dir) || OB_ISNULL(dst_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(src_dir), K(dst_dir), K(ret));
  } if (OB_UNLIKELY(0 != ::rename(src_dir, dst_dir))) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to move dir", KERRMSGS,
             "src dir", src_dir, "dst dir", dst_dir, K(ret));
  }
  return ret;
}

void ObProxyFileUtils::clear_dir(const char *dir)
{
  if (OB_NOT_NULL(dir)) {
    struct dirent *ent = NULL;
    DIR *file_dir = NULL;
    if (NULL != (file_dir = opendir(dir))) {
      char tmp_path[FileDirectoryUtils::MAX_PATH];
      while (NULL != (ent = readdir(file_dir))) {
        // we only load current config dir
        if (LOCAL_DIR == ent->d_name || PARENT_DIR == ent->d_name) {
          continue;
        }
        snprintf(tmp_path, FileDirectoryUtils::MAX_PATH, "%s/%s", dir, ent->d_name);
        if (ent->d_type == DT_DIR) {
          clear_dir(tmp_path);
        } else {
          ::unlink(tmp_path);
        }
      }
      if (OB_UNLIKELY(0 != closedir(file_dir))) {
        LOG_WARN("fail to close dir", "dir", dir, KERRMSGS);
      } else {
        ::rmdir(dir);
      }
    }
  }
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

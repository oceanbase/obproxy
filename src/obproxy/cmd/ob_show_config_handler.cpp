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

#include "cmd/ob_show_config_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_config_utils.h"
#include "obutils/ob_config_server_processor.h"
#include "omt/ob_proxy_config_table_processor.h"
#include "lib/encrypt/ob_encrypted_helper.h"

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
const ObString JSON_CONFIG_VERSION      = ObString::make_string("json_config_version");
const ObString JSON_CONFIG_BIN_URL      = ObString::make_string("json_config_bin_url");
const ObString JSON_CONFIG_DB           = ObString::make_string("json_config_meta_table_db");
const ObString JSON_CONFIG_USER         = ObString::make_string("json_config_meta_table_user");
const ObString JSON_CONFIG_PASSWORD     = ObString::make_string("json_config_meta_table_password");
const ObString JSON_CONFIG_REAL_CLUSTER = ObString::make_string("json_config_real_meta_cluster");
const ObString JSON_CONFIG_CLUSTER      = ObString::make_string("json_config_cluster_count");
const ObString JSON_CONFIG_MODIFIED     = ObString::make_string("json_config_modified_time");

//ConfigColumnID
enum
{
  OB_CC_NAME = 0,
  OB_CC_VALUE,
  OB_CC_INFO,
  OB_CC_NEED_REBOOT,
  OB_CC_VISIBLE_LEVEL,
  OB_CC_RANGE,
  OB_CC_CONFIG_LEVEL,
  OB_CC_MAX_CONFIG_COLUMN_ID,
};

//SyslogLevelColumnID
enum
{
  OB_SLC_PARENT_MOD = 0,
  OB_SLC_SUB_MOD,
  OB_SLC_LEVEL,
  OB_SLC_USAGE,
  OB_SLC_MAX_SYSLOG_LEVEL_COLUMN_ID,
};

// proxy_config表，用于诊断内存中的多级配置的值
enum
{
  OB_PC_VID = 0,
  OB_PC_VIP,
  OB_PC_VPORT,
  OB_PC_TENANT_NAME,
  OB_PC_CLUSTER_NAME,
  OB_PC_NAME,
  OB_PC_VALUE,
  OB_PC_CONFIG_LEVEL,
  OB_PC_MAX_COLUMN_ID,
};

const ObProxyColumnSchema CONFIG_COLUMN_ARRAY[OB_CC_MAX_CONFIG_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_CC_NAME,          "name",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_VALUE,         "value",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_INFO,          "info",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_NEED_REBOOT,   "need_reboot",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_VISIBLE_LEVEL, "visible_level",  OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_RANGE,         "range",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_CC_CONFIG_LEVEL,  "config_level",   OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema SYSLOG_LEVEL_COLUMN_ARRAY[OB_SLC_MAX_SYSLOG_LEVEL_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_SLC_PARENT_MOD, "parent_mod", OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_SUB_MOD,    "sub_mod",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_LEVEL,      "level",      OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_SLC_USAGE,      "usage",      OB_MYSQL_TYPE_VARCHAR),
};

const ObProxyColumnSchema ALL_CONFIG_COLUMN_ARAAY[OB_PC_MAX_COLUMN_ID] = {
    ObProxyColumnSchema::make_schema(OB_PC_VID,           "vid",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_VIP,           "vip",            OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_VPORT,         "vport",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_TENANT_NAME,   "tenant_name",    OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_CLUSTER_NAME,  "cluster_name",   OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_NAME,          "name",           OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_VALUE,         "value",          OB_MYSQL_TYPE_VARCHAR),
    ObProxyColumnSchema::make_schema(OB_PC_CONFIG_LEVEL,  "config_level",   OB_MYSQL_TYPE_VARCHAR),
};

ObShowConfigHandler::ObShowConfigHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type())
{
  SET_HANDLER(&ObShowConfigHandler::handle_show_config);
}

int ObShowConfigHandler::handle_show_config(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_config_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else if (OBPROXY_T_SUB_CONFIG_ALL == sub_type_) {  // show proxyconfig all
    if (OB_FAIL(omt::get_global_proxy_config_table_processor().dump_config_item(*this, like_name_))) {
        LOG_WDIAG("fail to dump proxy config item", K_(like_name), K(ret));
    } else {
      // do nothing
    }
  } else {
    const ObConfigContainer &container = get_global_proxy_config().get_container();
    ObString proxy_section("obproxy");
    if (OBPROXY_T_SUB_INVALID == sub_type_) {
      ObConfigContainer::const_iterator it = container.begin();
      obsys::CRLockGuard guard(get_global_proxy_config().rwlock_);
      for (; OB_SUCC(ret) && it != container.end(); ++it) {
        if (common::match_like(it->second->name(), like_name_) && OB_FAIL(dump_config_item(*(it->second)))) {
          WDIAG_ICMD("fail to dump config item", K_(like_name), K(ret));
        }
      }
    } else {
      ObProxyConfig *orig_config = NULL;
      if (OB_ISNULL(orig_config = new (std::nothrow) ObProxyConfig())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to new memory for ObProxyConfig", K(ret));
      } else {
        const ObConfigContainer &orig_container = orig_config->get_container();
        ObConfigContainer::const_iterator orig_it = orig_container.begin();
        ObConfigContainer::const_iterator it = container.begin();
        //we need lock it whenever handle item->value
        obsys::CRLockGuard guard(get_global_proxy_config().rwlock_);
        for (; OB_SUCC(ret) && it != container.end() && orig_it != orig_container.end(); ++it, ++orig_it) {
          if (OBPROXY_T_SUB_CONFIG_DIFF == sub_type_) {
            if ((ObString::make_string(orig_it->second->str()) != it->second->str())
                 && common::match_like(it->second->name(), like_name_)
                 && OB_FAIL(dump_config_item(*(it->second)))) {
              WDIAG_ICMD("fail to dump diff config item", K_(like_name), K(ret));
            }
          } else if (OBPROXY_T_SUB_CONFIG_DIFF_USER == sub_type_) {
            if (ObProxyConfigUtils::is_user_visible(*(it->second))
                && (ObString::make_string(orig_it->second->str()) != it->second->str())
                && common::match_like(it->second->name(), like_name_)
                && OB_FAIL(dump_config_item(*(it->second)))) {
              WDIAG_ICMD("fail to dump diff user config item", K_(like_name), K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            WDIAG_ICMD("it should not arrive here",
                      "sub_type", get_obproxy_sub_stmt_name(sub_type_), K_(like_name), K(ret));
          }
        }
      }

      if (OB_LIKELY(NULL != orig_config)) {
        delete orig_config;
        orig_config = NULL;
      }
    }


    if (OB_SUCC(ret) && OBPROXY_T_SUB_INVALID == sub_type_) {
      ObProxyJsonConfigInfo *json_info= NULL;
      if (OB_ISNULL(json_info = get_global_config_server_processor().acquire())) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("fail to get proxy json config info", K(ret));
      } else if (common::match_like(JSON_CONFIG_VERSION, like_name_)
                 && OB_FAIL(dump_json_config_version(json_info->get_data_info().version_))) {
        WDIAG_ICMD("fail to dump json config version", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_BIN_URL, like_name_)
                 && OB_FAIL(dump_json_config_bin_url(json_info->get_bin_url().url_))) {
        WDIAG_ICMD("fail to dump json config bin url", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_DB, like_name_)
                 && OB_FAIL(dump_json_config_db(json_info->get_meta_table_info().db_))) {
        WDIAG_ICMD("fail to dump meta table db", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_USER, like_name_)
                 && OB_FAIL(dump_json_config_username(json_info->get_meta_table_info().username_))) {
        WDIAG_ICMD("fail to dump meta table username", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_PASSWORD, like_name_)
                 && OB_FAIL(dump_json_config_password(json_info->get_meta_table_info().password_))) {
        WDIAG_ICMD("fail to dump meta table password", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_REAL_CLUSTER, like_name_)
                 && OB_FAIL(dump_json_config_real_cluster_name(json_info->get_meta_table_info().real_cluster_name_))) {
        WDIAG_ICMD("fail to dump meta table real cluster", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_CLUSTER, like_name_)
                 && OB_FAIL(dump_json_config_cluster_count(json_info->get_cluster_count()))) {
        WDIAG_ICMD("fail to dump json config cluster count", K_(like_name), K(ret));
      } else if (common::match_like(JSON_CONFIG_MODIFIED, like_name_)
                 && OB_FAIL(dump_json_config_gmt_modified(json_info->gmt_modified_))) {
        WDIAG_ICMD("fail to dump json config modified time", K_(like_name), K(ret));
      } else if (seq_ == (original_seq_ + OB_CC_MAX_CONFIG_COLUMN_ID + 3)
          && 0 == ObString::make_string("syslog_level").case_compare(like_name_)) {
        // if there is only one row result, and it is "syslog_level", we re-encode it
        // header need OB_CC_MAX_CONFIG_COLUMN_ID+1 sequences, one row need one sequence,
        // so the next sequence should be OB_CC_MAX_CONFIG_COLUMN_ID+3
        if (OB_FAIL(dump_syslog_level())) {
          WDIAG_ICMD("fail to dump syslog_level", K_(like_name), K(ret));
        }
      }
      get_global_config_server_processor().release(json_info);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
    } else {
      INFO_ICMD("succ to dump config", K_(like_name));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

int ObShowConfigHandler::dump_config_item(const ObConfigItem &item)
{
  int ret = OB_SUCCESS;
  const char *need_reboot_str = (item.need_reboot() ? OB_CONFIG_NEED_REBOOT : OB_CONFIG_NOT_NEED_REBOOT);
  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(item.name());
  cells[OB_CC_VALUE].set_varchar(item.str());
  cells[OB_CC_INFO].set_varchar(item.info());
  cells[OB_CC_NEED_REBOOT].set_varchar(need_reboot_str);
  cells[OB_CC_VISIBLE_LEVEL].set_varchar(item.visible_level());
  cells[OB_CC_RANGE].set_varchar(item.range_str());
  cells[OB_CC_CONFIG_LEVEL].set_varchar(item.config_level_to_str());

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_version(const ObString &version)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_VERSION);
  cells[OB_CC_VALUE].set_varchar(version);
  cells[OB_CC_INFO].set_varchar("json config info version");
  cells[OB_CC_NEED_REBOOT].set_varchar("false");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WARN_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_bin_url(const ObString &url)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_BIN_URL);
  cells[OB_CC_VALUE].set_varchar(url);
  cells[OB_CC_INFO].set_varchar("bin url, used for hot upgrade");
  cells[OB_CC_NEED_REBOOT].set_varchar("false");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_db(const ObString &db)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_DB);
  cells[OB_CC_VALUE].set_varchar(db);
  cells[OB_CC_INFO].set_varchar("meta table db name");
  cells[OB_CC_NEED_REBOOT].set_varchar("true");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_username(const ObString &username)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_USER);
  cells[OB_CC_VALUE].set_varchar(username);
  cells[OB_CC_INFO].set_varchar("meta table username, format'user@tenant#cluster'");
  cells[OB_CC_NEED_REBOOT].set_varchar("true");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_password(const ObString &password)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_PASSWORD);
  // sha1加密
  char password_staged1_buf[ENC_STRING_BUF_LEN] {};
  ObString password_string(ENC_STRING_BUF_LEN, password_staged1_buf);
  if (!password.empty() && OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(password, password_string))) {
    LOG_WDIAG("encrypt_passwd_to_stage1 failed", K(ret));
  } else {
    // skip '*'
    cells[OB_CC_VALUE].set_varchar(password_staged1_buf + 1);
    cells[OB_CC_INFO].set_varchar("meta table password");
    cells[OB_CC_NEED_REBOOT].set_varchar("true");
    cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_real_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_REAL_CLUSTER);
  cells[OB_CC_VALUE].set_varchar(cluster_name);
  cells[OB_CC_INFO].set_varchar("meta table real cluster");
  cells[OB_CC_NEED_REBOOT].set_varchar("true");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_cluster_count(const int64_t count)
{
  int ret = OB_SUCCESS;

  ObNewRow row;
  ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
  cells[OB_CC_NAME].set_varchar(JSON_CONFIG_CLUSTER);
  cells[OB_CC_VALUE].set_int(count);
  cells[OB_CC_INFO].set_varchar("ob cluster count, meta db cluster not included");
  cells[OB_CC_NEED_REBOOT].set_varchar("false");
  cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

  row.cells_ = cells;
  row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_json_config_gmt_modified(const int64_t time)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_TIMESTAMP_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(ObTimeUtility::usec_to_str(time, buf, OB_MAX_TIMESTAMP_LENGTH, pos))) {
    WDIAG_ICMD("fail to convert usec to datetime string", K(ret));
  } else {
    ObNewRow row;
    ObObj cells[OB_CC_MAX_CONFIG_COLUMN_ID];
    cells[OB_CC_NAME].set_varchar(JSON_CONFIG_MODIFIED);
    cells[OB_CC_VALUE].set_varchar(buf);
    cells[OB_CC_INFO].set_varchar("json config modified time");
    cells[OB_CC_NEED_REBOOT].set_varchar("false");
    cells[OB_CC_VISIBLE_LEVEL].set_varchar("virtual");

    row.cells_ = cells;
    row.count_ = OB_CC_MAX_CONFIG_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
    }
  }
  return ret;
}

int ObShowConfigHandler::dump_syslog_level()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(reset())) {//before dump_syslog_level, we need clean buf
    WDIAG_ICMD("fail to do reset", K(ret));
  } else if (OB_FAIL(dump_syslog_level_header())) {//encode header
    WDIAG_ICMD("fail to dump syslog_level header", K(ret));
  } else {
    //encode body
    const ObLogger &logger = ObLogger::get_logger();
    const ObLogNameIdMap &name_id_map = logger.get_name_id_map();
    const ObLogIdLevelMap &id_level_map = logger.get_id_level_map();
    const char *par_name = NULL;
    const char *sub_name = NULL;
    const char *level_str = NULL;
    int8_t level_id = OB_LOG_LEVEL_NP;

    for (uint64_t par_index = 0; OB_SUCC(ret) && par_index < OB_LOG_MAX_PAR_MOD_SIZE; ++par_index) {
      if (OB_SUCC(name_id_map.get_par_mod_name(par_index, par_name)) && OB_LIKELY(NULL != par_name)) {
        // dump parent mod
        level_id = id_level_map.get_level(par_index);
        if (OB_FAIL(logger.get_level_str(level_id, level_str))) {
          WDIAG_ICMD("fail to get level str", "level_id", level_id, "level", level_str, K(ret));
        } else if (OB_FAIL(dump_syslog_level_item(level_str, par_name))) {
          WDIAG_ICMD("fail to dump syslog_level item", "level", level_str, "par_name", par_name, K(ret));
        }
        //dump sub
        for (uint64_t sub_index = 1; OB_SUCC(ret) && sub_index < OB_LOG_MAX_SUB_MOD_SIZE; ++sub_index) {
          if (OB_SUCC(name_id_map.get_sub_mod_name(par_index, sub_index, sub_name)) && OB_LIKELY(NULL != sub_name)) {
            level_id = id_level_map.get_level(par_index, sub_index);
            if (OB_FAIL(logger.get_level_str(level_id, level_str))) {
              WDIAG_ICMD("fail to get level str", "level_id", level_id, "level", level_str, K(ret));
            } else if (OB_FAIL(dump_syslog_level_item(level_str, par_name, sub_name))) {
              WDIAG_ICMD("fail to dump syslog_level item", "level", level_str, "par_name", par_name,
                       "sub_name", sub_name, K(ret));
            }
          }//end of NULL != sub_name
        }//end of for(sub_index)
      }//end of NULL != par_name
    }//end of for(par_index)
  }//end of encode body
  return ret;
}

int ObShowConfigHandler::dump_syslog_level_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(SYSLOG_LEVEL_COLUMN_ARRAY, OB_SLC_MAX_SYSLOG_LEVEL_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_syslog_level_item(const char *level_str, const char *par_mod_name,
    const char *sub_mod_name)
{
  const int64_t SYSLOG_LEVEL_MAX_USAGE_LENGTH = 32;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(level_str) || OB_ISNULL(par_mod_name)) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("argument is null", K(ret));
  } else {
    const char *sub_mod_name_tmp = (NULL == sub_mod_name ? "*" : sub_mod_name);
    char usage_str[SYSLOG_LEVEL_MAX_USAGE_LENGTH];
    int32_t length = snprintf(usage_str, sizeof(usage_str), "%s.%s:%s", par_mod_name, sub_mod_name_tmp, level_str);
    if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int32_t>(sizeof(usage_str)))) {
      ret = OB_BUF_NOT_ENOUGH;
      WDIAG_ICMD("usage_str is not enough", K(length), "usage_str length", sizeof(usage_str), K(usage_str), K(ret));
    } else {
      ObNewRow row;
      ObObj cells[OB_SLC_MAX_SYSLOG_LEVEL_COLUMN_ID];
      cells[OB_SLC_PARENT_MOD].set_varchar(par_mod_name);
      cells[OB_SLC_SUB_MOD].set_varchar(sub_mod_name_tmp);
      cells[OB_SLC_LEVEL].set_varchar(level_str);
      cells[OB_SLC_USAGE].set_varchar(usage_str);

      row.cells_ = cells;
      row.count_ = OB_SLC_MAX_SYSLOG_LEVEL_COLUMN_ID;
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObShowConfigHandler::dump_config_header()
{
  int ret = OB_SUCCESS;
  if (OBPROXY_T_SUB_CONFIG_ALL == sub_type_) {  // show proxyconfig all
    if (OB_FAIL(encode_header(ALL_CONFIG_COLUMN_ARAAY, OB_PC_MAX_COLUMN_ID))) {
      WDIAG_ICMD("fail to encode all config header", K(ret));
    }
  } else if (OB_FAIL(encode_header(CONFIG_COLUMN_ARRAY, OB_CC_MAX_CONFIG_COLUMN_ID))) {
    WDIAG_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowConfigHandler::dump_all_config_item(const omt::ObProxyConfigItem &item)
{
  int ret = OB_SUCCESS;
  char vip_buf[MAX_IP_ADDR_LENGTH]{};
  item.vip_info_.vip_addr_.addr_.ip_to_string(vip_buf, MAX_IP_ADDR_LENGTH);

  ObNewRow row;
  ObObj cells[OB_PC_MAX_COLUMN_ID];
  // ObProxyConfigItem中，目前没有info、range、need_reboot、visiable_level，所以无需展示
  // vip vid vport
  cells[OB_PC_VID].set_int(item.vip_info_.vip_addr_.vid_);
  cells[OB_PC_VIP].set_varchar(vip_buf);
  cells[OB_PC_VPORT].set_int(item.vip_info_.vip_addr_.addr_.port_);
  // tenant name、cluster name
  cells[OB_PC_TENANT_NAME].set_varchar(item.vip_info_.tenant_name_.ptr());
  cells[OB_PC_CLUSTER_NAME].set_varchar(item.vip_info_.cluster_name_.ptr());
  // name、value、config_level
  cells[OB_PC_NAME].set_varchar(item.config_item_.name());
  cells[OB_PC_VALUE].set_varchar(item.config_item_.str());
  cells[OB_PC_CONFIG_LEVEL].set_varchar(item.config_level_.ptr());
  row.cells_ = cells;
  row.count_ = OB_PC_MAX_COLUMN_ID;
  if (OB_FAIL(encode_row_packet(row))) {
    WDIAG_ICMD("fail to encode proxy_config row packet", K(row), K(ret));
  }
  return ret;
}

static int show_config_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowConfigHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowConfigHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowConfigHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowConfigHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObShowConfigHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowConfigHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_config_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_CONFIG,
                                                               &show_config_cmd_callback))) {
    WDIAG_ICMD("fail to proxy_config_stat_callback", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

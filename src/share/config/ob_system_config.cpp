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

#include "share/config/ob_system_config.h"
#include "share/config/ob_config.h"

namespace oceanbase
{
namespace common
{

#define GET_CONFIG_COLUMN_VALUE(type, name, cmd)\
  if (OB_SUCC(ret)) {\
    if (OB_SUCC(rs->get_##type(#name, val_##type))) {\
      cmd;\
    } else if (OB_ERR_NULL_VALUE == ret) {\
      SHARE_LOG(DEBUG, "row " #name " :value is null");\
      ret = OB_SUCCESS;\
    } else {\
      SHARE_LOG(WDIAG, "failed to get " #name " from __all_sys_parameter", K(ret));\
    }\
  }

int ObSystemConfig::update(ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WDIAG, "system config result is null", K(ret));
  }

  while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
    ObSystemConfigKey key;
    ObSystemConfigValue value;
    ObString val_varchar;
    int64_t val_int = 0;
    common::sqlclient::ObMySQLResult *rs = result.get_result();
    if (OB_ISNULL(rs)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WDIAG, "system config result is null", K(ret));
    }

    GET_CONFIG_COLUMN_VALUE(varchar, name, key.set_name(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, svr_type, key.set_server_type(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, svr_ip, key.set_server_ip(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, value, value.set_value(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, info, value.set_info(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, section, value.set_section(val_varchar));
    GET_CONFIG_COLUMN_VALUE(varchar, visible_level, value.set_visible_level(val_varchar));
    GET_CONFIG_COLUMN_VALUE(int, need_reboot, value.set_need_reboot(static_cast<bool>(val_int)));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rs->get_varchar("zone", val_varchar))) {
        if (OB_ERR_NULL_VALUE == ret) {
          ret = OB_SUCCESS;
        } else {
          SHARE_LOG(WDIAG, "failed to get zone from __all_sys_parameter", K(ret));
        }
      } else if (OB_FAIL(key.set_zone(val_varchar))) {
        SHARE_LOG(WDIAG, "set zone failed", K(ret), K(val_varchar));
      }
    }
    GET_CONFIG_COLUMN_VALUE(int, svr_port, key.set_int(ObString::make_string("svr_port"), val_int));
    if (OB_SUCC(ret)) {
      if (OB_SUCC(rs->get_int("config_version", val_int))) {
        key.set_version(val_int);
        version_ = max(version_, val_int);
      } else {
        SHARE_LOG(WDIAG, "failed to get config_version from __all_sys_parameter", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int hash_ret = OB_SUCCESS;
      hash_ret = map_.set_refactored(key, value);
      if (OB_SUCCESS != hash_ret) {
        if (OB_HASH_EXIST == hash_ret) {
          SHARE_LOG(WDIAG, "sys config insert repeatly", "name", key.name(), K(hash_ret));
        } else {
          ret = hash_ret;
          SHARE_LOG(WDIAG, "sys config map set failed", "name", key.name(), K(ret));
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    SHARE_LOG(WDIAG, "failed to get result from result set", K(ret));
  }

  return ret;
}

int ObSystemConfig::find_all_matched(const ObSystemConfigKey &key,
                                     ObArray<hashmap::const_iterator> &all_config) const
{
  int ret = OB_SUCCESS;
  all_config.reset();
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();

  for (; it != last; ++it) {
    if (it->first.match(key)) {
      all_config.push_back(it);
    }
  }

  return ret;
}

int ObSystemConfig::find_newest(const ObSystemConfigKey &key,
                                const ObSystemConfigValue *&pvalue,
                                int64_t &max_version) const
{
  int ret = OB_SEARCH_NOT_FOUND;
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();
  max_version = key.get_version();
  pvalue = NULL;

  for (; it != last; ++it) {
    if (it->first.match(key) && it->first.get_version() > max_version) {
      max_version = it->first.get_version();
      pvalue = &it->second;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSystemConfig::find(const ObSystemConfigKey &key,
                         const ObSystemConfigValue *&pvalue) const
{
  int ret = OB_SUCCESS;

  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();
  pvalue = NULL;

  if (OB_ISNULL(pvalue)) {
    // check if ip and port both matched
    for (it = map_.begin(); it != last; ++it) {
      if (it->first.match_ip_port(key)) {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (OB_ISNULL(pvalue)) {
    /* check if server type matched */
    for (it = map_.begin(); it != last; ++it) {
      if (it->first.match_server_type(key)) {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (OB_ISNULL(pvalue)) {
    /* check if zone matched */
    for (it = map_.begin(); it != last; ++it) {
      if (it->first.match_zone(key)) {
        pvalue = &it->second;
        break;
      }
    }
  }
  if (OB_ISNULL(pvalue)) {
    /* check if matched */
    for (it = map_.begin(); it != last; ++it) {
      if (it->first.match(key)) {
        pvalue = &it->second;
        break;
      }
    }
  }

  if (OB_ISNULL(pvalue)) {
    ret = OB_NO_RESULT;
  }
  return ret;
}

int ObSystemConfig::reload(FILE *fp)
{
  int ret = OB_SUCCESS;
  size_t cnt = 0;
  if (OB_ISNULL(fp)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(EDIAG, "Got NULL file pointer", K(ret));
  } else {
    ObSystemConfigKey key;
    ObSystemConfigValue value;
    while (1) { // one config reload failure not effect others, so no OB_SUCC(ret) here.
      if (1 != (cnt = fread(&key, sizeof(key), 1, fp))) {
        if (0 == cnt) {
          break;
        } else {
          ret = OB_ERR_UNEXPECTED;
          SHARE_LOG(WDIAG, "fail to read config from file", KERRMSGS, K(ret));
        }
      } else if (1 != (cnt = fread(&value, sizeof(value), 1, fp))) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WDIAG, "fail to read config from file", KERRMSGS, K(ret));
      } else {
        int hash_ret = OB_SUCCESS;
        hash_ret = map_.set_refactored(key, value);
        if (OB_SUCCESS != hash_ret) {
          if (OB_HASH_EXIST == hash_ret) {
            SHARE_LOG(WDIAG, "system config insert repeatly", "name", key.name());
          } else {
            ret = hash_ret;
            SHARE_LOG(WDIAG, "system config map set fail", "name", key.name(), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSystemConfig::read_int32(const ObSystemConfigKey &key,
                               int32_t &value,
                               const int32_t &def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  int64_t version = 0;
  char *p = NULL;
  if (OB_SUCC(find_newest(key, pvalue, version)) && OB_LIKELY(NULL != pvalue)) {
    value = static_cast<int32_t>(strtol(pvalue->value(), &p, 0));
    if (p == pvalue->value()) {
      SHARE_LOG(EDIAG, "config is not integer", "name", key.name(), "value", p);
    } else if (OB_ISNULL(p) || OB_UNLIKELY('\0' != *p)) {
      SHARE_LOG(WDIAG, "config was truncated", "name", key.name(), "value", p);
    } else {
      SHARE_LOG(INFO, "use internal config", "name", key.name(), K(value));
    }
  } else {
    value = def;
    SHARE_LOG(INFO, "use default config", "name", key.name(), K(value), K(pvalue), K(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSystemConfig::read_int64(const ObSystemConfigKey &key,
                               int64_t &value,
                               const int64_t &def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  int64_t version = 0;
  char *p = NULL;
  if (OB_SUCC(find_newest(key, pvalue, version)) && OB_LIKELY(NULL != pvalue)) {
    value = strtoll(pvalue->value(), &p, 0);
    if (p == pvalue->value()) {
      SHARE_LOG(EDIAG, "config is not integer", "name", key.name(), "value", p);
    } else if (OB_ISNULL(p) || OB_UNLIKELY('\0' != *p)) {
      SHARE_LOG(WDIAG, "config was truncated", "name", key.name(), "value", p);
    } else {
      SHARE_LOG(INFO, "use internal config", "name", key.name(), K(value));
    }
  } else {
    value = def;
    SHARE_LOG(INFO, "use default config", "name", key.name(), K(value), K(pvalue), K(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSystemConfig::read_str(const ObSystemConfigKey &key,
                             char buf[], int64_t len,
                             const char *def) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  int64_t version = 0;
  if (OB_SUCC(find_newest(key, pvalue, version)) && OB_LIKELY(NULL != pvalue)) {
    int wlen = 0;
    if ((wlen = snprintf(buf, len, "%s", pvalue->value())) < 0) {
      SHARE_LOG(EDIAG, "reload config error", "name", key.name());
    } else if (wlen >= len) {
      SHARE_LOG(WDIAG, "config was truncated", "name", key.name());
    } else {
      SHARE_LOG(INFO, "use internal config", "name", key.name(), K(buf));
    }
    /* for safe */
    buf[len - 1] = '\0';
  } else {
    if (buf != def) {
      int64_t pos = 0;
      if(OB_FAIL(databuff_printf(buf, len, pos, "%s", def))) {
        SHARE_LOG(WDIAG, "buf is not long enough", K(key.name()), K(def), K(pvalue), K(ret));
      }
    }
    SHARE_LOG(INFO, "use default config", "name", key.name(), K(def), K(pvalue), K(ret));
    ret = OB_SUCCESS; // by design
  }
  return ret;
}

int ObSystemConfig::read_config(const ObSystemConfigKey &key,
                                ObConfigItem &item) const
{
  int ret = OB_SUCCESS;
  const ObSystemConfigValue *pvalue = NULL;
  int64_t version = 0;
  // if find nothing, ignore directly
  if (OB_SUCC(find_newest(key, pvalue, version))) {
    if (OB_ISNULL(pvalue)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (!item.set_value(pvalue->value())) {
        // without set ret
        SHARE_LOG(WDIAG, "set config item value failed",
                  K(key.name()), K(pvalue->value()), K(version));
      }
      item.set_version(version);
    }
  }
  return ret;
}

int64_t ObSystemConfig::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  hashmap::const_iterator it = map_.begin();
  hashmap::const_iterator last = map_.end();

  pos += snprintf(buf + pos, len - pos, "total: [%ld]\n", map_.size());

  for (; it != last; ++it) {
    pos += snprintf(buf + pos, len - pos, "name: [%s], value: [%s]\n",
                    it->first.name(), it->second.value());
  }

  return pos;
}

} // end of namespace common
} // end of namespace oceanbase

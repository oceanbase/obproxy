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

#define USING_LOG_PREFIX SHARE

#include "share/config/ob_server_config.h"
#include "common/ob_record_header.h"

static const char *ignore_config_opt_str[] = {"target_db_server"};

namespace oceanbase
{
namespace common
{

ObConfigContainer *&ObInitConfigContainer::local_container()
{
  static __thread ObConfigContainer *l_container = NULL;
  return l_container;
}

const ObConfigContainer &ObInitConfigContainer::get_container()
{
  return container_;
}

ObInitConfigContainer::ObInitConfigContainer()
{
  local_container() = &container_;
}

ObCommonConfig::ObCommonConfig()
{
}

ObCommonConfig::~ObCommonConfig()
{
}

// tmp use for opt str
int ObCommonConfig::add_extra_config_from_opt(const char *config_str,
                                              int64_t version /* = 0 */ ,
                                              bool check_name /* = false */)
{
  return add_config(config_str, version, check_name, ",\n", ignore_config_opt_str);
}

int ObCommonConfig::add_extra_config(const char *config_str,
                                     int64_t version /* = 0 */ ,
                                     bool check_name /* = false */)
{
  return add_config(config_str, version, check_name, "\n", NULL);
}

int ObCommonConfig::add_config(const char *config_str,
                               int64_t version /* = 0 */ ,
                               bool check_name /* = false */,
                               const char* delim,
                               const char* ignore_conf[])
{
  int ret = OB_SUCCESS;
  const int64_t MAX_OPTS_LENGTH = sysconf(_SC_ARG_MAX);
  int64_t config_str_length = 0;
  char *buf = NULL;
  char *saveptr = NULL;
  char *token = NULL;

  if (OB_ISNULL(config_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("config str is null", K(ret));
  } else if ((config_str_length = static_cast<int64_t>(STRLEN(config_str))) >= MAX_OPTS_LENGTH) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_EDIAG("Extra config is too long", K(ret));
  } else if (OB_ISNULL(buf = new (std::nothrow) char[config_str_length + 1])) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("ob tc malloc memory for buf fail", K(ret));
  } else {
    MEMCPY(buf, config_str, config_str_length);
    buf[config_str_length] = '\0';
    token = STRTOK_R(buf, delim, &saveptr);
  }

  while (OB_SUCC(ret) && OB_LIKELY(NULL != token)) {
    char *saveptr_one = NULL;
    const char *name = NULL;
    const char *value_tmp = "";
    const char *value = NULL;
    ObConfigItem *const *pp_item = NULL;
    if (OB_ISNULL(name = STRTOK_R(token, "=", &saveptr_one))) {
      ret = OB_INVALID_CONFIG;
      LOG_EDIAG("Invalid config string", K(token), K(ret));
    } else if (OB_ISNULL(saveptr_one) || OB_UNLIKELY('\0' == *(saveptr_one))) {
      LOG_INFO("Empty config string", K(token), K(name));
      value = value_tmp;
    } else {
      value = saveptr_one;
    }

    bool ignore = false;
    if (OB_SUCC(ret) && OB_NOT_NULL(ignore_conf)) {
      int64_t ignore_conf_num = sizeof(ignore_config_opt_str)/sizeof(char*);
      for (int64_t i = 0; i < ignore_conf_num; i++) {
        ObString conf(ignore_conf[i]);
        if (OB_NOT_NULL(ignore_conf[i]) && conf.case_compare(name) == 0) {
          ignore = true;
          break;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (ignore) {
      LOG_INFO("Ignore config", K(name), K(value));
    } else if (OB_ISNULL(pp_item = container_.get(ObConfigStringKey(name)))) {
      /* make compatible with previous configuration */
      ret = check_name ? OB_INVALID_CONFIG : OB_SUCCESS;
      LOG_WDIAG("Invalid config string, no such config item", K(name), K(value), K(ret));
    } else if (!(*pp_item)->set_value(value)) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("Invalid config value", K(name), K(value), K(ret));
    } else if (!(*pp_item)->check()) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("Invalid config, value out of range", K(name), K(value), K(ret));
    } else {
      (*pp_item)->set_version(version);
      LOG_INFO("Load config succ", K(name), K(value));
    }
    token = STRTOK_R(NULL, delim, &saveptr);
  }

  if (NULL != buf) {
    delete [] buf;
    buf = NULL;
  }
  return ret;
}

DEFINE_SERIALIZE(ObCommonConfig)
{
  int ret = OB_SUCCESS;

  if (pos + MIN_LENGTH >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObRecordHeader header;
    char *const p_header = buf + pos;
    char *const p_data = p_header + header.get_serialize_size();
    const int64_t pos_data = pos + header.get_serialize_size();
    int64_t pos_beg = pos;

    pos += header.get_serialize_size();
    const ObString memory_level(OB_CONFIG_VISIBLE_LEVEL_MEMORY);
    ObConfigContainer::const_iterator it = container_.begin();
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (memory_level == it->second->visible_level()) {
        //no need serialize
      } else {
        ret = databuff_printf(buf, buf_len, pos, "%s=%s\n", it->first.str(), it->second->str());
      }
    }

    if (OB_SUCC(ret)) {
      header.magic_ = OB_COMMON_CONFIG_MAGIC;
      header.header_length_ = static_cast<int16_t>(header.get_serialize_size());
      header.version_ = 1;
      header.data_length_ = static_cast<int32_t>(pos - pos_data);
      header.data_zlength_ = header.data_length_;
      header.data_checksum_ = ob_crc64(p_data, pos - pos_data);
      header.set_header_checksum();

      ret = header.serialize(buf, buf_len, pos_beg);
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObCommonConfig)
{
  int ret = OB_SUCCESS;
  if (data_len - pos < MIN_LENGTH) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObRecordHeader header;
    const char *const p_header = buf + pos;
    const char *const p_data = p_header + header.get_serialize_size();
    const int64_t pos_data = pos + header.get_serialize_size();
    if (OB_FAIL(header.deserialize(buf, data_len, pos))) {
      LOG_EDIAG("deserialize header failed", K(ret));
    } else if (OB_FAIL(header.check_header_checksum())) {
      LOG_EDIAG("check header checksum failed", K(ret));
    } else if (OB_COMMON_CONFIG_MAGIC != header.magic_) {
      ret = OB_INVALID_DATA;
      LOG_EDIAG("check magic number failed", K_(header.magic), K(ret));
    } else if (data_len - pos_data != header.data_zlength_) {
      ret = OB_INVALID_DATA;
      LOG_EDIAG("check data len failed",
                K(data_len), K(pos_data), K_(header.data_zlength), K(ret));
    } else if (OB_FAIL(header.check_payload_checksum(p_data, data_len - pos_data))) {
      LOG_EDIAG("check data checksum failed", K(ret));
    } else if (OB_FAIL(add_extra_config(buf + pos))) {
      LOG_EDIAG("Read server config failed", K(ret));
    } else {
      pos += header.data_length_;
    }
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase

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

#include <openssl/sha.h>

#include "cmd/ob_alter_config_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_reload_config.h"
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
ObAlterConfigSetHandler::ObAlterConfigSetHandler(ObContinuation *cont, ObMIOBuffer *buf,
                                                 const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), capability_(info.get_capability())
{
  SET_HANDLER(&ObAlterConfigSetHandler::handle_set_config);
  int32_t min_len = 0;
  if (!info.get_key_string().empty()) {
    min_len =std::min(info.get_key_string().length(), static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN));
    MEMCPY(key_str_, info.get_key_string().ptr(), min_len);
  }
  key_str_[min_len] = '\0';

  if (OBPROXY_T_SUB_CONFIG_INT_VAULE == info.get_sub_cmd_type()) {
    //no need check
    snprintf(value_str_, sizeof(value_str_), "%ld", info.get_first_int());
  } else {
    min_len = 0;
    if (!info.get_value_string().empty()) {
      min_len =std::min(info.get_value_string().length(), static_cast<int32_t>(OB_MAX_CONFIG_VALUE_LEN));
      MEMCPY(value_str_, info.get_value_string().ptr(), min_len);
    }
    value_str_[min_len] = '\0';
  }
}

int ObAlterConfigSetHandler::handle_set_config(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  char *old_value = NULL;
  ObProxyReloadConfig *reload_config = NULL;
  bool has_update_config = false;
  bool has_dump_config = false;
  ObString key_string(key_str_);
  ObString value_string(value_str_);

  if ((0 == key_string.case_compare("observer_sys_password")
      || 0 == key_string.case_compare("obproxy_sys_password")
      || 0 == key_string.case_compare("observer_sys_password1"))
      && !value_string.empty()) {
    char passwd_staged1_buf[ENC_STRING_BUF_LEN];
    ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
    if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(value_string, passwd_string))) {
      LOG_WARN("encrypt_passwd_to_stage1 failed", K(ret));
    } else {
      MEMCPY(value_str_, passwd_staged1_buf + 1, 40);
      value_str_[40] = '\0';
      value_string.assign(value_str_, 40);
      LOG_DEBUG("alter password", K(key_string), K(value_string));
    }
  }

  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(ret));
  } else if (OB_ISNULL(reload_config = get_global_internal_cmd_processor().get_reload_config())) {
    ret = OB_ERR_NULL_VALUE;
    WARN_ICMD("fail to get reload config", K(ret));

  //1. get old config value
  } else if (OB_ISNULL(old_value = static_cast<char *>(op_fixed_mem_alloc(OB_MAX_CONFIG_VALUE_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for old_value", "size", OB_MAX_CONFIG_VALUE_LEN, K(ret));
  } else if (OB_FAIL(get_global_proxy_config().get_old_config_value(key_string, old_value, OB_MAX_CONFIG_VALUE_LEN))) {
    LOG_WARN("fail to get old config value", K(key_string), K(ret));

  //2. update config value
  } else if (key_string == get_global_proxy_config().app_name.name()) {
    ret = OB_NOT_SUPPORTED;
    WARN_ICMD("app_name can only modified when restart", K(old_value), K(ret));
  } else if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, value_string))) {
    WARN_ICMD("fail to update config", K(key_string), K(value_string), K(ret));
  } else {
    has_update_config = true;
    DEBUG_ICMD("succ to update config", K(key_string), K(value_string), K(old_value));
  }

  //3. check config and dump it
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_global_proxy_config().check_proxy_serviceable())) {
      LOG_WARN("fail to check proxy string_item config", K(ret));
    } else if (OB_FAIL(get_global_proxy_config().dump_config_to_local())) {
      WARN_ICMD("fail to dump_config_to_local", K(ret));
    } else {
      has_dump_config = true;
    }
  }

  //4. reload config to memory
  if (OB_SUCC(ret)) {
    if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
      WARN_ICMD("fail to reload config, but config has already dumped!!", K(ret));
    } else {
      DEBUG_ICMD("succ to update config", K(key_string), K(value_string));
    }
  }

  //5. rollback
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (has_update_config) {
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().update_config_item(
          key_string, ObString::make_string(old_value))))) {
        WARN_ICMD("fail to back to old config", K(key_string), K(old_value), K(tmp_ret));
      } else {
        DEBUG_ICMD("succ to back to old config", K(key_string), K(old_value));
      }
    }
    if (has_dump_config && OB_LIKELY(OB_SUCCESS == tmp_ret)) {
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = (*reload_config)(get_global_proxy_config())))) {
        WARN_ICMD("fail to reload old config", K(tmp_ret));
      } else {
        DEBUG_ICMD("succ to reload old config");
      }
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().dump_config_to_local()))) {
        WARN_ICMD("fail to dump old config", K(tmp_ret));
      } else {
        DEBUG_ICMD("succ to dump old config");
      }
    }
  }

  if (OB_LIKELY(NULL != old_value)) {
    op_fixed_mem_free(old_value, OB_MAX_CONFIG_VALUE_LEN);
    old_value = NULL;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_ok_packet(0, capability_))) {
      WARN_ICMD("fail to encode ok packet", K(ret));
    } else {
      INFO_ICMD("succ to update config", K(key_string), K(value_string));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  } else {
    int errcode = ret;
    if (OB_FAIL(encode_err_packet(errcode))) {
      WARN_ICMD("fail to encode err resp packet", K(errcode), K(ret));
    } else {
      INFO_ICMD("succ to encode err resp packet", K(errcode));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

static int alter_config_set_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObAlterConfigSetHandler *handler = NULL;
  const bool is_query_cmd = false;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObAlterConfigSetHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ERROR_ICMD("fail to new ObAlterConfigSetHandler", K(ret));
  } else if (OB_FAIL(handler->init(is_query_cmd))) {
    WARN_ICMD("fail to init for ObAlterConfigSetHandler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      ERROR_ICMD("fail to schedule ObAlterConfigSetHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObAlterConfigSetHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int alter_config_set_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_ALTER_CONFIG,
                                                               &alter_config_set_cmd_callback))) {
    WARN_ICMD("fail to register CMD_TYPE_ALTER", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

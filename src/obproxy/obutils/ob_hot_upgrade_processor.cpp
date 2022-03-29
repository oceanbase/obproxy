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

#include "obutils/ob_hot_upgrade_processor.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "utils/ob_layout.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_task.h"
#include "iocore/eventsystem/ob_blocking_task.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/net/ob_net.h"
#include "stat/ob_processor_stats.h"
#include "stat/ob_mysql_stats.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_async_common_task.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "prometheus/ob_prometheus_processor.h"


using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::prometheus;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObHotUpgradeProcessor &get_global_hot_upgrade_processor()
{
  static ObHotUpgradeProcessor hu_processor;
  return hu_processor;
}

/******************************ObProxyHotUpgrader*******************************************/
int ObProxyHotUpgrader::spawn_process()
{
  int ret = OB_SUCCESS;
  ObExecCtx ctx;
  if (OB_FAIL(fill_exec_ctx(ctx))) {
    LOG_ERROR("fail to fill exec ctx", K(ret));
  } else {
    ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    info.sub_pid_ = fork();
    switch (info.sub_pid_) {
      case -1: {
        ret  = OB_ERR_SYS;
        LOG_ERROR("fail to fork", "ProcessID", getpid(), "fork_ret", info.sub_pid_,
                  KERRMSGS, K(ret));
        break;
      }
      case 0: {
        int ret = execve(ctx.path_, ctx.argv_, ctx.envp_);
        if (OB_UNLIKELY(-1 == ret)) {
          LOG_ERROR("fail to execve", K(ctx.path_), K(ctx.argv_[0]), K(ctx.envp_[0]),
                    K(ret), KERRMSGS);
          // Attention: as 'exit()' will destruct the global variables and local static variables,
          // flushes standard I/O buffers and removes temporary files created,
          // call any functions registered with atexit(), and than call '_exit()'.
          // if we use 'exit()', the sub process will be core immediately.
          _exit(1);
        }
        break;
      }
      default: {
        // if sub process do execve failed (in spawn_process), parent will not know.
        // so when this case happened, human need intervention(send rollback cmd,
        // send "hot upgrade" will be invalid, send "commit" cmd will exit)
        // or parent rollback automatically
        LOG_INFO("succ create subprocess", "subprocess PID", info.sub_pid_);
        break;
      }
    }
  }
  return ret;
}

int ObProxyHotUpgrader::fill_exec_ctx(ObExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  memset(&ctx, 0, sizeof(ctx));
  if (OB_FAIL(get_envp(ctx.envp_))) {
    LOG_ERROR("fail to get envp", K(ret));
  } else {
    ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    ctx.path_ = info.argv_[0];
    ctx.name_ = "new obproxy binary process";
    ctx.argv_ = info.inherited_argv_;
  }
  return ret;
}

int ObProxyHotUpgrader::get_envp(char **&envp)
{
  static const int64_t OBPROXY_ENVP_MAX_SIZE = 128;
  int ret = OB_SUCCESS;
  int64_t count = 1;  // envp's valid items count
  bool is_obproxy_root_set = false;
  char *obproxy_root = NULL;
  if (NULL != (obproxy_root = getenv("OBPROXY_ROOT"))) {
    ++count;
    is_obproxy_root_set = true;
    LOG_DEBUG("get env", K(obproxy_root));
  }
  int64_t size = (count + 1) * sizeof(char *);
  char *var = NULL;
  var = reinterpret_cast<char *>(malloc(size));
  if (OB_ISNULL(var)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to malloc", K(size), K(ret));
  } else {
    memset(var, 0, size);
    envp = reinterpret_cast<char **>(var);
    envp[0] = reinterpret_cast<char *>(malloc(OBPROXY_ENVP_MAX_SIZE));
    if (OB_ISNULL(envp[0])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      free(var);
      var = NULL;
      envp = NULL;
      LOG_ERROR("fail to malloc", K(OBPROXY_ENVP_MAX_SIZE), K(ret));
    } else {
      const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
      int64_t length = snprintf(envp[0], static_cast<size_t>(OBPROXY_ENVP_MAX_SIZE),
                                OBPROXY_INHERITED_FD"=%d", info.fd_);
      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= OBPROXY_ENVP_MAX_SIZE)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enought", K(length), "envp[0]_length", OBPROXY_ENVP_MAX_SIZE, K(envp[0]), K(ret));
      }
    }
    //set OBPROXY_ROOT
    if (OB_SUCC(ret) && is_obproxy_root_set) {
      envp[1] = reinterpret_cast<char *>(malloc(OBPROXY_ENVP_MAX_SIZE));
      if (OB_ISNULL(envp[1])) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        free(envp[0]);
        free(var);
        var = NULL;
        envp = NULL;
        LOG_ERROR("fail to malloc", K(ret));
      } else {
        int64_t length = snprintf(envp[1], static_cast<size_t>(OBPROXY_ENVP_MAX_SIZE),
                                  "OBPROXY_ROOT=%s", obproxy_root);
        if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= OBPROXY_ENVP_MAX_SIZE)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("buf not enought", K(length), "envp[1]_length", OBPROXY_ENVP_MAX_SIZE, K(envp[1]), K(ret));
        }
      }
    }
    //attention:do not forget to set this
    if (OB_SUCC(ret)) {
      envp[count] = NULL;
    }
  }

  // for debug
  if (OB_SUCC(ret)) {
    if (is_obproxy_root_set) {
      LOG_INFO("current envp", K(count), K(envp[0]), K(envp[1]), K(envp[2]));
    } else {
      LOG_INFO("current envp", K(count), K(envp[0]), K(envp[1]), K(is_obproxy_root_set));
    }
  }
  return ret;
}

int get_binary_md5(const char *binary, char *md5_buf, const int64_t md5_buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (OB_ISNULL(binary) || OB_ISNULL(md5_buf) || md5_buf_len <= OB_DEFAULT_PROXY_MD5_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", K(binary), K(ret));
  } else if (OB_UNLIKELY(0 != access(binary, R_OK))) {//check whether exist
    ret = ob_get_sys_errno();
    LOG_WARN("failed to access file", K(binary), KERRMSGS);
  } else {
    const int64_t MAX_SHELL_STR_LENGTH = ObLayout::MAX_PATH_LENGTH + 10;
    char *shell_str = static_cast<char *>(op_fixed_mem_alloc(MAX_SHELL_STR_LENGTH));
    if (OB_ISNULL(shell_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(MAX_SHELL_STR_LENGTH), K(ret));
    } else {
      struct sigaction action;
      struct sigaction old_action;
      sigemptyset(&action.sa_mask);
      action.sa_handler = SIG_DFL;// set it SIG_DFL
      action.sa_flags = 0;
      FILE *fp = NULL;
      int64_t length = snprintf(shell_str, MAX_SHELL_STR_LENGTH, "md5sum %s", binary);

      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= MAX_SHELL_STR_LENGTH)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough", K(length), "shell_str_length", sizeof(shell_str), K(shell_str), K(ret));
      } else if (OB_UNLIKELY(0 != sigaction(SIGCHLD, &action, &old_action))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to sigaction SIGCHLD", KERRMSGS, K(ret));
      } else {
        if (OB_ISNULL((fp = popen(shell_str, "r")))) {
          ret = ob_get_sys_errno();
          LOG_WARN("failed to popen fp", K(shell_str), K(binary), KERRMSGS, K(ret));
        } else if (OB_ISNULL(fgets(md5_buf, static_cast<int32_t>(md5_buf_len), fp))) {
          ret = ob_get_sys_errno();
          LOG_WARN("fail to fgets md5_str", K(shell_str), K(md5_buf), KERRMSGS, K(ret));
        } else {
          LOG_INFO("succeed to get binary md5", K(shell_str), K(md5_buf), K(md5_buf_len));
        }

        if (OB_LIKELY(NULL != fp)) {
          if (-1 == pclose(fp)) {
            ret = ob_get_sys_errno();
            LOG_WARN("failed to pclose fp", K(fp), KERRMSGS, K(ret));
          } else {
            fp = NULL;
          }
        }

        if (OB_UNLIKELY(0 != sigaction(SIGCHLD, &old_action, NULL))) {//reset it
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to sigaction SIGCHLD", KERRMSGS, K(ret));
        }
      }
    }

    if (NULL != shell_str) {
      op_fixed_mem_free(shell_str, MAX_SHELL_STR_LENGTH);
      shell_str = NULL;
    }
  }

  LOG_DEBUG("finish get binary md5", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

/******************************ObHotUpgradeProcessor************************************/
ObHotUpgradeProcessor::ObHotUpgradeProcessor()
    : is_inited_(false), is_timeout_rollback_(false), is_self_md5_available_(false),
      is_self_binary_(false), cmd_(HUC_NONE), mysql_proxy_(NULL), hu_cont_(NULL), hot_upgrade_cont_(NULL),
      info_(get_global_hot_upgrade_info()), proxy_port_(0), upgrade_failures_(0),
      check_available_failures_(0), timeout_rollback_timeout_at_(0), wait_cr_finish_timeout_at_(0)
{
  proxy_ip_[0] = '\0';
  proxy_self_md5_[0] = '\0';
  hot_binary_name_[0] = '\0';
  hot_binary_md5_[0] = '\0';
  cold_binary_name_[0] = '\0';
  cold_binary_md5_[0] = '\0';
}

void ObHotUpgradeProcessor::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(hu_cont_))) {
    LOG_WARN("fail to destroy hot upgrader cont", K(ret));
  }
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(hot_upgrade_cont_))) {
    LOG_WARN("fail to destroy hot upgrader cont", K(ret));
  }
  if (OB_LIKELY(is_inited_)) {
    is_inited_ = false;
    cmd_ = HUC_NONE;
    is_timeout_rollback_ = false;
    is_self_md5_available_ = false;
    is_self_binary_ = false;
    mysql_proxy_ = NULL;
    proxy_ip_[0] = '\0';
    proxy_port_ = 0;
    upgrade_failures_ = 0;
    check_available_failures_ = 0;
    timeout_rollback_timeout_at_ = 0;
    wait_cr_finish_timeout_at_ = 0;
  }
}

DEF_TO_STRING(ObHotUpgradeProcessor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(proxy_port), K_(proxy_ip), K_(is_self_md5_available),
       K_(proxy_self_md5), K_(upgrade_failures), K_(check_available_failures),
       K_(is_timeout_rollback), K_(timeout_rollback_timeout_at),
       K_(wait_cr_finish_timeout_at), K_(is_self_binary),
       K_(hot_binary_name), K_(hot_binary_md5),
       K_(cold_binary_name), K_(cold_binary_md5),
       "cmd", ObHotUpgraderInfo::get_cmd_string(cmd_),
       KPC_(hu_cont), KPC_(mysql_proxy), K_(info));
  J_OBJ_END();
  return pos;
}

int ObHotUpgradeProcessor::do_repeat_task()
{
  ObAsyncCommonTask *cont = get_global_hot_upgrade_processor().get_hot_upgrade_cont();
  if (OB_LIKELY(NULL != cont)) {
    // set interval to 0, so that task will not be scheduled anymore
    int64_t interval_us = 0;
    cont->set_interval(interval_us);
  }
  return get_global_hot_upgrade_processor().handle_hot_upgrade();
}

int ObHotUpgradeProcessor::init(ObMysqlProxy &mysql_proxy)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("it has already inited", K(ret));
  } else if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_local_addr(info_.local_addr_))) {
    LOG_WARN("fail to get proxy local addr", K(info_.local_addr_), K(ret));
  } else if (OB_UNLIKELY(!info_.local_addr_.ip_to_string(proxy_ip_, static_cast<int32_t>(sizeof(proxy_ip_))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to covert ip to string", K(info_.local_addr_), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory for ObProxyMutex", K(ret));
  } else {
    proxy_port_ = info_.local_addr_.get_port();
    mysql_proxy_ = &mysql_proxy;
    is_inited_ = true;

    if (!info_.is_in_single_service()) {
      LOG_DEBUG("subprocess schedule hot upgrade rollback task", K_(info));
      schedule_timeout_rollback();
    }
  }
  return ret;
}

// schedule hot upgrade task, this is create new proxy event,
// we will fetch bin and fork new proxy in it
int ObHotUpgradeProcessor::schedule_upgrade_task()
{
  int ret = OB_SUCCESS;
  const int64_t interval_us = (info_.is_restart()
      ? 0
      : ObRandomNumUtils::get_random_half_to_full(get_global_proxy_config().fetch_proxy_bin_random_time));

  cmd_ = info_.cmd_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hot upgrade processor is not inited", K(ret));
  } else if (OB_UNLIKELY(interval_us < 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("fetch_proxy_bin_random_time must not be < 0", K(interval_us), K(ret));
  } else if (NULL == hu_cont_) {
    if (OB_ISNULL(hu_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                                "hot_upgrade_task",
                                                ObHotUpgradeProcessor::do_repeat_task, NULL))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start hot_upgrade cont", K(ret));
    }
  } else if (OB_ISNULL(g_event_processor.schedule_in(hu_cont_,
                       HRTIME_USECONDS(interval_us), ET_BLOCKING,
                       ASYNC_PROCESS_DO_REPEAT_TASK_EVENT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule hot upgrade cont", K(ret));
  }
  return ret;
}

int ObHotUpgradeProcessor::prepare_binary()
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (HUC_RESTART == cmd_ || HUC_LOCAL_RESTART == cmd_) {
    LOG_INFO("begin to do restart", "cmd", ObHotUpgraderInfo::get_cmd_string(cmd_), K(info));
    if (OB_FAIL(check_binary_availability())) {
      info_.update_parent_status(HU_STATUS_UNAVAILABLE_BINARY);
      LOG_WARN("fail to check binary availability", K_(info), K(ret));

    } else if (OB_FAIL(dump_config())) {
      info_.update_parent_status(HU_STATUS_BACKUP_BIN_FAIL);
      LOG_WARN("fail to dump config", K_(info), K(ret));

    } else {
      set_is_self_binary(true);
    }
  } else if (HUC_HOT_UPGRADE == cmd_ || HUC_AUTO_UPGRADE == cmd_ || HUC_UPGRADE_BIN == cmd_) {
    LOG_INFO("begin to do upgrade", "cmd", ObHotUpgraderInfo::get_cmd_string(cmd_), K(info));
    char *obproxy_tmp = NULL;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;

    if (OB_FAIL(FileDirectoryUtils::create_full_path(get_global_layout().get_bin_dir()))) {
      LOG_WARN("fail to makedir", "dir", get_global_layout().get_bin_dir(), K(ret));
    } else if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_bin_dir(), "obproxy_tmp", allocator, obproxy_tmp))) {
      info_.update_parent_status(HU_STATUS_FETCH_BIN_FAIL);// we treate this fetch bin failed
      LOG_WARN("fail to merge file path of obproxy_tmp",  K_(info), K(ret));
    } else if (OB_FAIL(fetch_new_proxy_bin(obproxy_tmp))) {
      info_.update_parent_status(HU_STATUS_FETCH_BIN_FAIL);
      LOG_WARN("fail to fetch new proxy bin", K_(info), K(ret));
    } else if (OB_FAIL(check_proxy_bin_release(obproxy_tmp))) {
      info_.update_parent_status(HU_STATUS_CHECK_BIN_RELEASE_FAIL);
      LOG_WARN("fail to check binary release of obproxy_tmp", K(obproxy_tmp), K_(info), K(ret));
    } else if (OB_FAIL(check_proxy_bin_md5(obproxy_tmp))) {
      info_.update_parent_status(HU_STATUS_CHECK_BIN_MD5_FAIL);
      LOG_WARN("fail to check binary md5 of obproxy_tmp", K(obproxy_tmp), K_(info), K(ret));
    } else if (OB_FAIL(dump_config())) {
      info_.update_parent_status(HU_STATUS_BACKUP_BIN_FAIL);
      LOG_WARN("fail to dump config", K_(info), K(ret));
    } else if (OB_FAIL(rename_binary(obproxy_tmp))) {
      info_.update_parent_status(HU_STATUS_BACKUP_BIN_FAIL);
      LOG_WARN("fail to rename binary", K_(info), K(ret));
    } else {
      set_is_self_binary(false);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cmd", "cmd", ObHotUpgraderInfo::get_cmd_string(cmd_), K(info), K(ret));
  }
  return ret;
}

int ObHotUpgradeProcessor::handle_hot_upgrade()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hu processor is not inited", K(ret));
  } else {
    if (HU_STATE_FORK_NEW_PROXY == info_.get_state()) {
      if (OB_FAIL(prepare_binary())) {
        LOG_WARN("fail to prepare binary", K(ret));
      } else {
        if (HUC_UPGRADE_BIN == cmd_) {
          info_.update_parent_status(HU_STATUS_UPGRADE_BINARY_SUCC);
          LOG_INFO("succ to upgrade binary", K_(info));
        } else {
          // in order parent can be do timeout rollback early than sub processs
          // and the timeout rollback task can be initiate definitely
          // we move schedule_timeout_rollback() here
          schedule_timeout_rollback();

          if (OB_FAIL(ObProxyHotUpgrader::spawn_process())) {
            cancel_timeout_rollback();
            if (!is_self_binary() && OB_FAIL(restore_binary())) {
              LOG_WARN("fail to restore old binary, but it has nothing affect", K_(info), K(ret));
            }
            info_.update_parent_status(HU_STATUS_CREATE_NEW_PROXY_FAIL);
            LOG_WARN("fail to spawn sub process", K_(info), K(ret));
          } else {
            // as it is a asynchronous task, we need set state immediately.
            info_.update_state(HU_STATE_WAIT_CR_CMD);
            info_.update_both_status(HU_STATUS_CREATE_NEW_PROXY_SUCC, HU_STATUS_NEW_PROXY_CREATED_SUCC);
            info_.update_last_hu_status(HU_STATUS_CREATE_NEW_PROXY_SUCC, HU_STATUS_NEW_PROXY_CREATED_SUCC);
            LOG_INFO("succ to fork new proxy, start turn to HU_STATE_WAIT_CR_CMD", K_(info));
          }
        }
      }
    } else {
      LOG_ERROR("there is no need to do hot upgrade now, it should not enter here", K_(info));
      //do nothing
    }
  }
  return ret;
}

int ObHotUpgradeProcessor::check_binary_availability()
{
  int ret = OB_SUCCESS;
  char *binary = NULL;
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_bin_dir(), "obproxy", allocator, binary))) {
    LOG_WARN("fail to merge file path of obproxy",  K(ret));
  } else if (OB_UNLIKELY(0 != access(binary, F_OK))) {//check whether exist
    ret = ob_get_sys_errno();
    LOG_WARN("failed to access file", K(binary), KERRMSGS, K(ret));
  } else if (OB_UNLIKELY(0 != chmod(binary, S_IRWXU | S_IRGRP | S_IXGRP | S_IXOTH))) {
    ret = ob_get_sys_errno();
    LOG_WARN("fail to chmod of binary", KERRMSGS, K(binary), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObHotUpgradeProcessor::fetch_new_proxy_bin(const char *bin_save_path)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 1;
  if (OB_ISNULL(bin_save_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("binary is null", K(ret));
  } else {
    bool need_retry = true;
    //only one is available between them
    const char *target_binary_name_ = (hot_binary_name_[0] != '\0' ? hot_binary_name_ : cold_binary_name_);
    while (need_retry && retry_times <= 3) {
      if (OB_FAIL(get_global_config_server_processor().do_fetch_proxy_bin(bin_save_path,
          target_binary_name_))) {
        LOG_WARN("fail to fetch proxy bin", K(retry_times), K(ret));
      } else {
        need_retry = false;
        LOG_INFO("succ to fetch proxy bin from net", K(ret));
      }
      ++retry_times;
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 != (tmp_ret = chmod(bin_save_path, S_IRWXU | S_IRGRP | S_IXGRP | S_IXOTH)))) {
      ret = ob_get_sys_errno();
      LOG_WARN("fail to chmod of binary", K(tmp_ret), KERRMSGS, K(bin_save_path), K(ret));
    } else {
      LOG_INFO("succ to fetch new proxy bin and chmod it", K(ret));
    }
  }
  return ret;
}

int ObHotUpgradeProcessor::check_proxy_bin_md5(const char *binary)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(binary)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", K(binary), K(ret));
  } else {
    const char *target_binary_md5_ = (hot_binary_md5_[0] != '\0' ? hot_binary_md5_ : cold_binary_md5_);
    const ObString new_binary_md5(target_binary_md5_);
    char fetch_binary_md5_str[OB_DEFAULT_PROXY_MD5_LEN + 1] = {'\0'};

    if (OB_UNLIKELY(!is_available_md5(new_binary_md5))) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("new_binary_md5 is error", K(new_binary_md5), K(ret));
    } else if (OB_FAIL(get_binary_md5(binary, fetch_binary_md5_str,
                                      static_cast<int64_t>(sizeof(fetch_binary_md5_str))))) {
      LOG_WARN("fail to get binary md5", K(binary), K(fetch_binary_md5_str), K(ret));
    } else if (0 != new_binary_md5.compare(fetch_binary_md5_str)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("fetch_bin md5 is different from new_binary_md5",
               K(fetch_binary_md5_str), K(new_binary_md5), K(ret));
    } else {
      LOG_INFO("succeed to check proxy bin md5", K(binary));
    }
  }
  return ret;
}

int ObHotUpgradeProcessor::check_proxy_bin_release(const char *binary)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(binary)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", K(binary), K(ret));
  } else {
    const int64_t MAX_SHELL_STR_LENGTH = ObLayout::MAX_PATH_LENGTH + 10;
    char *shell_str = NULL;
    const int64_t now = ObTimeUtility::current_time();
    if (OB_UNLIKELY(0 != access(binary, X_OK))) {//check whether execute permissions
      ret = ob_get_sys_errno();
      LOG_WARN("failed to access file", K(binary), KERRMSGS, K(ret));
    } else if (OB_ISNULL(shell_str = static_cast<char *>(op_fixed_mem_alloc(MAX_SHELL_STR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(MAX_SHELL_STR_LENGTH), K(ret));
    } else {
      struct sigaction action;
      struct sigaction old_action;
      sigemptyset(&action.sa_mask);
      action.sa_handler = SIG_DFL;// set it SIG_DFL
      action.sa_flags = 0;
      FILE *fp = NULL;
      int64_t length = snprintf(shell_str, MAX_SHELL_STR_LENGTH, "%s -R", binary);
      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= MAX_SHELL_STR_LENGTH)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enough", K(length), "shell_str_length", sizeof(shell_str), K(shell_str), K(ret));
      } else if (OB_UNLIKELY(0 != sigaction(SIGCHLD, &action, &old_action))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to sigaction SIGCHLD", KERRMSGS, K(ret));
      } else {
        if (OB_ISNULL((fp = popen(shell_str, "r")))) {
          ret = ob_get_sys_errno();
          LOG_WARN("failed to popen fp", K(shell_str), K(binary), KERRMSGS, K(ret));
        } else {
          char *result_buf = shell_str;
          MEMSET(result_buf, 0, MAX_SHELL_STR_LENGTH);
          fgets(result_buf, static_cast<int32_t>(MAX_SHELL_STR_LENGTH), fp);
          if (0 != STRLEN(result_buf)
              && (NULL != strstr(result_buf, ".el") || NULL != strstr(result_buf, ".alios"))) {
            if (OB_FAIL(get_global_config_server_processor().check_kernel_release(result_buf))) {
              LOG_WARN("failed to parser linux kernel release in result_buf", K(result_buf), K(ret));
            } else {
              LOG_INFO("succeed to check proxy binary release", K(result_buf));
            }
          } else {
            LOG_INFO("maybe upgrade to old proxy, treat it succ", K(result_buf));
          }
        }

        if (OB_LIKELY(NULL != fp)) {
          if (-1 == pclose(fp)) {
            ret = ob_get_sys_errno();
            LOG_WARN("failed to pclose fp", K(fp), KERRMSGS, K(ret));
          } else {
            fp = NULL;
          }
        }

        if (OB_UNLIKELY(0 != sigaction(SIGCHLD, &old_action, NULL))) {//reset it
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to sigaction SIGCHLD", KERRMSGS, K(ret));
        }
      }

      if (NULL != shell_str) {
        op_fixed_mem_free(shell_str, MAX_SHELL_STR_LENGTH);
        shell_str = NULL;
      }
    }
    LOG_DEBUG("finish check proxy binary release", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  }
  return ret;
}

void ObHotUpgradeProcessor::schedule_timeout_rollback()
{
  timeout_rollback_timeout_at_ = HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_rollback_timeout)
                                 + get_hrtime_internal(); // nanosecond
  is_timeout_rollback_ = false;
  LOG_INFO("succ to schedule hot upgrade timeout rollback job", K_(timeout_rollback_timeout_at));
}

void ObHotUpgradeProcessor::cancel_timeout_rollback()
{
  LOG_INFO("cancel hot upgrade timeout rollback job", K_(timeout_rollback_timeout_at));
  timeout_rollback_timeout_at_ = 0;
  is_timeout_rollback_ = false;
}

int ObHotUpgradeProcessor::check_timeout_rollback()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(info_.need_timeout_rollback_check())) {
    ObHRTime diff_time = 0;
    if (OB_LIKELY(timeout_rollback_timeout_at_ <= 0)) {
      // maybe has not started
    } else if (0 < (diff_time = timeout_rollback_timeout_at_ - get_hrtime_internal())) {
      LOG_DEBUG("it is not time to do hot upgrade timeout rollback job",
                K_(timeout_rollback_timeout_at), K(diff_time));
    } else {
      LOG_WARN("it is time, but we still not receive rollback cmd, we need do timeout rollback job",
                K_(timeout_rollback_timeout_at), K(diff_time));
      is_timeout_rollback_ = true;

      // proxy set rollback only if last cmd is HOT_UPGRADE or AUTO_UPGRADE or RESTART
      const ObHotUpgradeCmd target_cmd = HUC_ROLLBACK;
      const ObHotUpgradeCmd orig_cmd = HUC_HOT_UPGRADE;
      const ObHotUpgradeCmd orig_cmd_either = HUC_AUTO_UPGRADE;
      const ObHotUpgradeCmd orig_cmd_another = HUC_RESTART;
      // we will try 3 times to update_proxy_hu_cmd.
      int64_t retry_times = 3;
      while (OB_LIKELY(retry_times > 0) && OB_SUCC(ret)) {
        if (OB_NOT_NULL(mysql_proxy_) && OB_FAIL(ObProxyTableProcessorUtils::update_proxy_hu_cmd(*mysql_proxy_, proxy_ip_,
            proxy_port_, target_cmd, orig_cmd, orig_cmd_either, orig_cmd_another))
            && OB_ERR_UNEXPECTED != ret) {
          LOG_WARN("fail to update hot upgrade cmd, unknown error code, we will retry update",
                   K(ret), K(target_cmd), K(orig_cmd), K(orig_cmd_either),
                   K(proxy_ip_), K(proxy_port_), K(retry_times));
          --retry_times;
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("succ to update hot upgrade cmd to rollback", K(retry_times));
      } else if (OB_ERR_UNEXPECTED == ret) {
        // maybe parent(sub) has update table, we fail to update again
        LOG_WARN("fail to update hot upgrade cmd, maybe other one has updated table, "
                 "we will do rollback later", K(ret));
      } else {
        LOG_WARN("fail to update hot upgrade cmd, we will do rollback later", K(ret));
      }
    }
  }
  return ret;
}


//state of waiting hot_upgrade cmd
int ObHotUpgradeProcessor::state_wait_hu_cmd(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("enter HU_STATE_WAIT_HU_CMD state", "pid", getpid(),
            "current status", ObHotUpgraderInfo::get_status_string(info_.status_),
            "receive cmd", ObHotUpgraderInfo::get_cmd_string(info_.cmd_));

  switch (info_.cmd_) {
    case HUC_RESTART:
      //restart also arrive here, but no need check_arguments
    case HUC_UPGRADE_BIN:
    case HUC_AUTO_UPGRADE:
      //auto upgrade also arrive here, nothing different here
    case HUC_HOT_UPGRADE: {
      //if this is restart cmd, only retry once
      const int64_t max_upgrade_failures = (HUC_RESTART == info_.cmd_ ? 1 : get_global_proxy_config().hot_upgrade_failure_retries.get());
      if (OB_UNLIKELY(upgrade_failures_ >= max_upgrade_failures)) {
        info_.update_parent_status(HU_STATUS_FAILURE_RETRIES_TOO_MANY);
        LOG_WARN("continual upgrade failure retries is too much, we will re-accept it until "
                 "'hot_upgrade_cmd' has been reset to 'hot_upgrade/auto_upgrade'.", K_(info),
                 K(upgrade_failures_), K(max_upgrade_failures), K(ret));
        info_.cmd_ = HUC_NONE;

      } else {
        //when recv "hot_upgrade" cmd, proxy need do follow things:
        //1. promote to parent, reset all hu_status
        //2. check argument(new binary version and md5)
        //3. create event to fetch & execv new binary
        //4. update status, turn to HU_STATE_FORK_NEW_PROXY state

        if (is_first_upgrade()) {
          info_.is_parent_ = true;// move to parent site, no mater on which site before
          info_.update_both_status(HU_STATUS_NONE, HU_STATUS_NONE);
          info_.update_last_hu_status(HU_STATUS_NONE, HU_STATUS_NONE);
        }

        if (OB_FAIL(check_arguments(proxy_info))) {
          ++upgrade_failures_;
          info_.update_parent_status(HU_STATUS_INVALID_ARGUMENT);
          LOG_WARN("fail to check arguments for hot_upgrade", K_(info), K_(upgrade_failures), K(ret));

        } else if (OB_FAIL(schedule_upgrade_task())) {
          ++upgrade_failures_;
          info_.update_parent_status(HU_STATUS_SCHEDULE_CREATE_NEW_PROXY_EVENT_FAIL);
          LOG_WARN("fail to schedule upgrade task", K_(info), K_(upgrade_failures), K(ret));

        } else {
          ObHotUpgradeStatus status = HU_STATUS_NONE;
          if (info_.is_restart()) {
            status = HU_STATUS_START_DOING_RESTART;
          } else if (HUC_UPGRADE_BIN == info_.cmd_) {
            status = HU_STATUS_START_UPGRADE_BINARY;
          } else {
            status = HU_STATUS_START_HOT_UPGRADE;
          }
          info_.update_both_status(status, HU_STATUS_NONE);
          info_.update_state(HU_STATE_FORK_NEW_PROXY);
          LOG_INFO("start to do fork new proxy job", K_(info));
        }
      }
      break;
    }
    case HUC_EXIT: {
      //when recv "exit" cmd, we need do follow things:
      //1. update status in table
      //2. do quick exit in 5 seconds
      if (OB_NOT_NULL(mysql_proxy_) && OB_FAIL(ObProxyTableProcessorUtils::update_proxy_exited_status(*mysql_proxy_,
              proxy_ip_, proxy_port_, HU_STATUS_DO_QUICK_EXIT, HUC_NONE))) {
        LOG_WARN("fail to update hot upgrade status and cmd to DO_GRACEFUL_EXIT, NONE", K_(info), K(ret));
      }
      if (OB_SUCC(ret) || info_.is_local_exit()) {
        info_.disable_net_accept();
        info_.is_parent_ = true;// move to parent site, no mater on which site before
        info_.update_parent_status(HU_STATUS_DO_QUICK_EXIT);
        info_.graceful_exit_start_time_ = get_hrtime_internal();
        info_.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().delay_exit_time)
                                        + info_.graceful_exit_start_time_; // nanosecond
        g_proxy_fatal_errcode = OB_SERVER_IS_STOPPING;
        LOG_WARN("parent process stop accepting new connection, stop check timer", K_(info), K(g_proxy_fatal_errcode), K(ret));
      }
      break;
    }
    case HUC_NONE:
    case HUC_ROLLBACK:
    case HUC_COMMIT: {
      upgrade_failures_ = 0;
      LOG_DEBUG("it is invalid cmd, no need to perform, upgrade_failures clear");
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unknown ObHotUpgradeCmd, it should not enter here", K_(info), K(ret));
    }
  }
  return ret;
}

// state of waiting fork_new_proxy
int ObHotUpgradeProcessor::state_fork_new_proxy()
{
  int ret = OB_SUCCESS;
  LOG_INFO("enter HU_STATE_FORK_NEW_PROXY state", "pid", getpid(),
           "current status", ObHotUpgraderInfo::get_status_string(info_.status_),
           "receive cmd", ObHotUpgraderInfo::get_cmd_string(info_.cmd_));

  switch (info_.status_) {
    case HU_STATUS_UNAVAILABLE_BINARY:
    case HU_STATUS_FETCH_BIN_FAIL:
    case HU_STATUS_CHECK_BIN_MD5_FAIL:
    case HU_STATUS_BACKUP_BIN_FAIL:
    case HU_STATUS_CREATE_NEW_PROXY_FAIL: {
      //if fail to fetch && excv new proxy, we will turn to HU_STATE_WAIT_HU_CMD state
      ++upgrade_failures_;
      info_.update_state(HU_STATE_WAIT_HU_CMD);
      LOG_WARN("fail to fork sub for hot_upgrade, back to HU_STATE_WAIT_HU_CMD state",
               K_(info_.status), K_(upgrade_failures));
      break;
    }
    case HU_STATUS_START_DOING_RESTART:
    case HU_STATUS_START_HOT_UPGRADE: {
      LOG_DEBUG("parent is forking new proxy", K_(info));
      break;
    }
    case HU_STATUS_START_UPGRADE_BINARY: {
      LOG_DEBUG("parent is upgrading binary", K_(info));
      break;
    }
    case HU_STATUS_UPGRADE_BINARY_SUCC: {
      info_.update_state(HU_STATE_WAIT_HU_CMD);
      info_.cmd_ = HUC_NONE;//reset
      LOG_INFO("succ to upgrade binary, start turn to HU_STATE_WAIT_HU_CMD", K_(info));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unknown ObHotUpgradeStatus, it should not enter here", K_(info_.status));
    }
  }
  return ret;
}

// state of waiting commit or rollback cmd
int ObHotUpgradeProcessor::state_wait_cr_cmd(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("enter HU_STATE_WAIT_CR_CMD state", "pid", getpid(),
           "current status", ObHotUpgraderInfo::get_status_string(info_.status_),
           "receive cmd", ObHotUpgraderInfo::get_cmd_string(info_.cmd_));

  if (info_.is_parent()) { // parent will enter here
    ret = state_parent_wait_cr_cmd(proxy_info);
  } else { // sub process will enter here
    ret = state_sub_wait_cr_cmd();
  }
  return ret;
}

// state of parent waiting commit or rollback cmd
int ObHotUpgradeProcessor::state_parent_wait_cr_cmd(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(info_.is_parent())) {
    bool is_sub_abnormal = false;
    if (info_.is_sub_exited() && HUC_ROLLBACK != info_.cmd_ && !is_timeout_rollback_) {
      // if sub exited earlier without rollback cmd or timeout rollback,
      // parent treats it as fork failed
      LOG_WARN("sub process has exited abnormally without rollback event, parent need back to state: "
               "HU_STATE_FORK_NEW_PROXY", "sub_pid", info_.sub_pid_);
      is_sub_abnormal = true;
      info_.cmd_ = HUC_ROLLBACK;//go to rollback case

    } else if (OB_UNLIKELY(is_timeout_rollback_)) {
      LOG_INFO("it is time, we need do timeout rollback", K_(info), K_(timeout_rollback_timeout_at));
      info_.cmd_ = HUC_ROLLBACK;//go to rollback case

    } else {
      // do nothing
    }

    switch (info_.cmd_) {
      case HUC_COMMIT: {
        //when recv commit, parent must do follow things:
        //1. disable net_accept
        //2. set graceful_exit_end_time_
        //3. update status and state
        //4. cancel timeout_rollback

        info_.disable_net_accept();
        info_.graceful_exit_start_time_ = get_hrtime_internal();
        info_.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_exit_timeout)
                                        + info_.graceful_exit_start_time_;// nanosecond
        info_.update_both_status(HU_STATUS_RECV_COMMIT_AND_EXIT, HU_STATUS_COMMIT_SUCC);
        info_.update_state(HU_STATE_WAIT_CR_FINISH);
        cancel_timeout_rollback();
        LOG_WARN("parent process stop accepting new connection, "
                  "stop check timer, and wait to die gradually", K_(info));
        break;
      }

      case HUC_ROLLBACK: {
        //when recv rollback, parent must do follow things:
        //1. update status, state and wait_cr_finish_timeout_at_
        //2. if sub was exited, reset sub_pid_
        //3. reset upgrade_failures_
        //4. cancel timeout_rollback job
        //5. restore binary (ignore failure)
        //6. dump config to local (ignore failure)

        if (OB_UNLIKELY(is_sub_abnormal)) {
          //if sub was exited abnormally, turn to HU_STATE_WAIT_HU_CMD;
          ++upgrade_failures_;
          info_.update_state(HU_STATE_WAIT_HU_CMD);
          info_.update_both_status(HU_STATUS_CREATE_NEW_PROXY_FAIL, HU_STATUS_EXITED);
          info_.reset_sub_pid(); //must set it in case of recv signal later

        } else {
          info_.update_parent_status(is_timeout_rollback_ ? HU_STATUS_TIMEOUT_ROLLBACK_SUCC : HU_STATUS_ROLLBACK_SUCC);
          if (info_.is_sub_exited()) {
            // if sub has already exit, we turn to HU_STATE_WAIT_HU_CMD immediately
            info_.update_state(HU_STATE_WAIT_HU_CMD);
            info_.reset_sub_pid(); //must set it in case of recv signal later
          } else {
            info_.update_state(HU_STATE_WAIT_CR_FINISH);
            info_.update_sub_status(is_timeout_rollback_ ? HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT : HU_STATUS_RECV_ROLLBACK_AND_EXIT);
            wait_cr_finish_timeout_at_ = get_hrtime_internal()
                                         + HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_exit_timeout)
                                         + HRTIME_USECONDS(get_global_proxy_config().proxy_info_check_interval);
          }
          upgrade_failures_ = 0;// reset to zero if ever succ to fork new proxy
        }
        info_.cmd_ = HUC_ROLLBACK;//reset it to rollback
        cancel_timeout_rollback();//cancel_timeout_rollback must be at end

        if (!is_self_binary() && OB_FAIL(restore_binary())) {//!!it maybe cost long time
          LOG_WARN("fail to restore binary, but it has nothing affect", K(ret));
        }
        if (OB_FAIL(dump_config())) {//!!it maybe cost long time
          LOG_WARN("fail to dump obproxy_config.bin in rollback, but it has nothing affect", K(ret));
        }
        LOG_INFO("parent process come to normal service status.", K_(info));
        break;
      }
      case HUC_NONE:
      case HUC_EXIT:
      case HUC_HOT_UPGRADE: {
        LOG_DEBUG("it is invalid cmd, no need to perform", K_(info));
        break;
      }
      case HUC_RESTART:
      case HUC_AUTO_UPGRADE: {
        //auto upgrade also arrive here, parent proxy need send commit via subprocess after sub was created succ
        if (proxy_info.current_pid_ != getpid()) {
          LOG_INFO("parent proxy need send commit via subprocesst", K_(info));
          if (OB_FAIL(send_commit_via_subprocess())) {
            LOG_WARN("fail to send commit via subprocesst", K_(info), K(ret));
          }
        } else {
          LOG_DEBUG("parent wait subprocess normally working", K_(info));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unknown ObHotUpgradeCmd, it should not enter here", K_(info), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sub should not enter here", K_(info), K(ret));
  }
  return ret;
}

// state of sub waiting commit or rollback cmd
int ObHotUpgradeProcessor::state_sub_wait_cr_cmd()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!info_.is_parent())) {
    bool is_parent_abnormal = false;
    if (info_.is_parent_exited() && OB_LIKELY(HUC_COMMIT != info_.cmd_)) {
      // if parent is exited without accepting HUC_COMMIT cmd, sub need update his status "
      // to let DBA know it
      LOG_WARN("parent process has exited abnormally without commit event, sub need back to state: "
               "HU_STATE_WAIT_HU_CMD");
      info_.cmd_ = HUC_COMMIT;//go to commit case
      is_parent_abnormal = true;

    } else if (OB_UNLIKELY(is_timeout_rollback_)) {
      LOG_INFO("it is time, we need do timeout rollback", K_(info), K_(timeout_rollback_timeout_at));
      info_.cmd_ = HUC_ROLLBACK;//go to commit case

    } else {
      //do nothing
    }

    switch (info_.cmd_) {
      case HUC_COMMIT: {
        //when recv commit, sub will do follow things:
        //1. update status, state and wait_cr_finish_timeout_at_
        //2. reset upgrade_failures_
        //3. cancel timeout_rollback job

        if (info_.is_parent_exited()) {
          // if sub has already exit, we turn to HU_STATE_WAIT_HU_CMD immediately
          info_.update_state(HU_STATE_WAIT_HU_CMD);
          if (OB_UNLIKELY(is_parent_abnormal)) {
            //parent has exited abnormally, sub keep last status to let DBA knows it
          } else {
            info_.update_sub_status(HU_STATUS_COMMIT_SUCC);
          }

        } else {
          info_.update_state(HU_STATE_WAIT_CR_FINISH);
          info_.update_both_status(HU_STATUS_RECV_COMMIT_AND_EXIT, HU_STATUS_COMMIT_SUCC);
          wait_cr_finish_timeout_at_ = get_hrtime_internal()
                                       + HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_exit_timeout)
                                       + HRTIME_USECONDS(get_global_proxy_config().proxy_info_check_interval);
        }
        upgrade_failures_ = 0;// reset to zero if ever succ to fork new proxy
        cancel_timeout_rollback();
        if (!is_self_binary() && OB_FAIL(backup_old_binary())) {//!!it maybe cost long time
          LOG_WARN("fail to backup old binary, but it has nothing affect", K(ret));
        }
        info_.cmd_ = HUC_COMMIT;//reset it to commit
        LOG_INFO("sub process come to normal service status", K_(info));
        break;
      }

      case HUC_ROLLBACK: {
        //when recv rollback, sub must do follow things:
        //1. disable net_accept
        //2. set graceful_exit_end_time_
        //3. update status and state
        //4. cancel timeout_rollback job

        info_.disable_net_accept();
        info_.graceful_exit_start_time_ = get_hrtime_internal();
        info_.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_exit_timeout)
                                        + info_.graceful_exit_start_time_; // nanosecond
        info_.update_state(HU_STATE_WAIT_CR_FINISH);
        if (is_timeout_rollback_) {
          info_.update_both_status(HU_STATUS_TIMEOUT_ROLLBACK_SUCC, HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT);
        } else {
          info_.update_both_status(HU_STATUS_ROLLBACK_SUCC, HU_STATUS_RECV_ROLLBACK_AND_EXIT);
        }
        cancel_timeout_rollback();
        LOG_WARN("sub process stop accept new connection, "
                 "stop check timer, and wait to die gradually.", K_(info));
        break;
      }
      case HUC_NONE:
      case HUC_EXIT:
      case HUC_RESTART:
      case HUC_AUTO_UPGRADE:
      case HUC_HOT_UPGRADE: {
        LOG_DEBUG("it is invalid cmd, no need to perform", K_(info));
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unknown ObHotUpgradeCmd, it should not enter here", K_(info), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("parent should not enter here", K_(info), K(ret));
  }
  return ret;
}

// state of waiting commit or rollback finish
int ObHotUpgradeProcessor::state_wait_cr_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("enter HU_STATE_WAIT_CR_FINISH state", "pid", getpid(),
           "current status", ObHotUpgraderInfo::get_status_string(info_.status_),
           "receive cmd", ObHotUpgraderInfo::get_cmd_string(info_.cmd_));

  // parent will enter here
  ObHRTime wait_time = wait_cr_finish_timeout_at_ - get_hrtime_internal();
  if (info_.is_parent()) {
    if (info_.is_sub_exited() || OB_LIKELY(wait_time <= 0)) {
      //when sub was exited or wait timeout, parent must do follow things:
      //1. update status, state
      //2. reset sub_pid_

      LOG_INFO("sub process has exited, parent process will turn to HU_STATE_WAIT_HU_CMD",
                K_(info), K(wait_time));
      info_.update_state(HU_STATE_WAIT_HU_CMD);
      info_.update_sub_status(HU_STATUS_EXITED);
      info_.reset_sub_pid(); //must set it in case of recv signal later
    } else {
      //wait for sub exited
    }
  } else {
    if (info_.is_parent_exited() || OB_LIKELY(wait_time <= 0)) {
      //when parent was exited or wait timeout, sub must do follow things:
      //1. update status and state

      LOG_INFO("parent process has exited, sub process will back to HU_STATE_WAIT_HU_CMD",
                K_(info), K(wait_time));
      info_.update_state(HU_STATE_WAIT_HU_CMD);
      info_.update_parent_status(HU_STATUS_EXITED);
    } else {
      //wait for parent exited
    }
  }
  return ret;
}

//here we will check new_binary version, md5, and fetch_bin random_time for doing hot_upgrade
int ObHotUpgradeProcessor::check_arguments(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("it is not inited", K(ret));
  } else if (info_.is_restart()) {
    // restart cmd no need check arguments
  } else {
    const ObString new_binary_version = ObString::make_string(proxy_info.new_binary_version_).trim();
    const ObString new_binary_md5 = ObString::make_string(proxy_info.new_binary_md5_).trim();

    if (OB_UNLIKELY(new_binary_version.empty())
        || OB_UNLIKELY(new_binary_version.length() > OB_MAX_PROXY_BINARY_VERSION_LEN)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("new binary version is not available", K(new_binary_version), K(ret));
    } else if (OB_UNLIKELY(!is_available_md5(new_binary_md5))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("new binary md5's is not available", K(new_binary_md5), K(ret));
    } else if (OB_FAIL(get_global_config_server_processor().check_kernel_release(new_binary_version))) {
      LOG_WARN("new_binary_version is not available", K(new_binary_version), K(ret));
    } else {
      if (HUC_UPGRADE_BIN == info_.cmd_) {
        MEMCPY(cold_binary_name_, new_binary_version.ptr(), new_binary_version.length());
        cold_binary_name_[new_binary_version.length()] = '\0';
        MEMCPY(cold_binary_md5_, new_binary_md5.ptr(), new_binary_md5.length());
        cold_binary_md5_[new_binary_md5.length()] = '\0';
        hot_binary_name_[0] = '\0';
        hot_binary_md5_[0] = '\0';
      } else {
        MEMCPY(hot_binary_name_, new_binary_version.ptr(), new_binary_version.length());
        hot_binary_name_[new_binary_version.length()] = '\0';
        MEMCPY(hot_binary_md5_, new_binary_md5.ptr(), new_binary_md5.length());
        hot_binary_md5_[new_binary_md5.length()] = '\0';
        cold_binary_name_[0] = '\0';
        cold_binary_md5_[0] = '\0';
      }

      LOG_DEBUG("succ to check new_binary version and new_binary md5 for_hot_upgrade",
                "hot_binary_name", hot_binary_name_,
                "hot_binary_md5", hot_binary_md5_,
                "cold_binary_name", cold_binary_name_,
                "cold_binary_md5", cold_binary_md5_);
    }
  }
  return ret;
}

// after check new binary md5 well, we need rename old binary before fork subprocess
// we can tolerate 'obproxy' binary absenting during hot upgrading,
// but can not tolerate 'obproxy_tmp' binary erring during hot upgrading
int ObHotUpgradeProcessor::rename_binary(const char *obproxy_tmp) const
{
  int ret = OB_SUCCESS;
  char *obproxy_old = NULL;
  char *obproxy = NULL;
  const int64_t now = ObTimeUtility::current_time();
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_bin_dir(), "obproxy_old",
                                               allocator, obproxy_old))) {
    LOG_WARN("fail to merge file path of obproxy_old", K(obproxy_old), K(ret));
  } else if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_bin_dir(), "obproxy",
                                               allocator, obproxy))) {
    LOG_WARN("fail to merge file path of obproxy", K(obproxy), K(ret));
  } else {
    bool obproxy_exist = true;
    int tmp_ret = OB_SUCCESS;

    // obproxy --> obproxy_old
    if (OB_UNLIKELY(0 != (tmp_ret = rename(obproxy, obproxy_old)))) {
      // we can tolerate 'obproxy' binary absenting during hot upgrading
      if (ENOENT == errno) {
        obproxy_exist = false;
        LOG_WARN("!Attention: 'obproxy' do not exist, however hot upgrade can still work ",
                  K(tmp_ret), KERRMSGS, K(obproxy), K(obproxy_old));
      } else {
        ret = ob_get_sys_errno();
        LOG_WARN("fail to rename 'obproxy' to 'obproxy_old'", K(tmp_ret),
                 KERRMSGS, K(ret), K(obproxy), K(obproxy_old));
      }
    }
    // obproxy_tmp --> obproxy
    if (OB_SUCC(ret) && OB_UNLIKELY(0 != (tmp_ret = rename(obproxy_tmp, obproxy)))) {
      // we can not tolerate 'obproxy_tmp' binary erring during hot upgrading
      ret = ob_get_sys_errno();
      LOG_WARN("!Attention: fail to rename 'obproxy_tmp' to 'obproxy', "
               "we need rename 'obproxy_old' back to 'obproxy'",
               K(tmp_ret), KERRMSGS, K(ret), K(obproxy_tmp), K(obproxy));
      // obproxy_old --> obproxy
      if (obproxy_exist && OB_UNLIKELY(0 != (tmp_ret = rename(obproxy_old, obproxy)))) {
        LOG_WARN("!Attention: fail to rename 'obproxy_old' back to 'obproxy', "
                 "the 'obproxy' is not belong to current proxy, however proxy can still work",
                 K(tmp_ret), KERRMSGS, K(ret), K(obproxy), K(obproxy_old));
      }
    }
  }
  LOG_INFO("finish rename binary", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

int ObHotUpgradeProcessor::dump_config() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_proxy_config().dump_config_to_local())) {
    LOG_WARN("fail to dump obproxy_config.bin", K(ret));
  }
  return ret;
}

// if we fail to fork new proxy, or we start rollback, we need restore old binary
int ObHotUpgradeProcessor::restore_binary() const
{
  int ret = OB_SUCCESS;
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  char *obproxy_old = NULL;
  char *obproxy = NULL;
  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  // obproxy_old --> obproxy
  if (OB_FAIL(ObLayout::merge_file_path(
              get_global_layout().get_bin_dir(), "obproxy_old", allocator, obproxy_old))) {
    LOG_WARN("fail to merge file path of proxy_old", K(obproxy_old), K(ret));
  } else if (OB_FAIL(ObLayout::merge_file_path(
              get_global_layout().get_bin_dir(), "obproxy", allocator, obproxy))) {
    LOG_WARN("fail to merge file path of proxy", K(obproxy), K(ret));
  } else if (0 != (tmp_ret = rename(obproxy_old, obproxy))) {
    if (ENOENT == errno) {
      LOG_WARN("!Attention: 'obproxy_old' do not exist, the current proxy process can still work",
                K(tmp_ret), K(obproxy_old), KERRMSGS);
      if (OB_UNLIKELY(0 != (tmp_ret = remove(obproxy)))) {
        LOG_WARN("!Attention: fail to remove 'obproxy', the current binary 'obproxy' is not "
                 "belong to the current proxy process", K(tmp_ret), KERRMSGS, K(ret));
      }
    } else {
      ret = ob_get_sys_errno();
      LOG_WARN("fail to rename 'obproxy_old' to 'obproxy'", K(tmp_ret), KERRMSGS,
               K(ret), K(obproxy_old), K(obproxy));
    }
  }
  LOG_INFO("finish restore binary", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

// if we succ to commit, we backup old binary maybe better
int ObHotUpgradeProcessor::backup_old_binary() const
{
  int ret = OB_SUCCESS;
  ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
  char *obproxy_old = NULL;
  char *obproxy_backup = NULL;
  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  // obproxy_old --> obproxy.old
  if (OB_FAIL(ObLayout::merge_file_path(
              get_global_layout().get_bin_dir(), "obproxy_old", allocator, obproxy_old))) {
    LOG_WARN("fail to merge file path of proxy_old", K(obproxy_old), K(ret));
  } else if (OB_FAIL(ObLayout::merge_file_path(
             get_global_layout().get_bin_dir(), "obproxy.old", allocator, obproxy_backup))) {
    LOG_WARN("fail to merge file path of proxy", K(obproxy_backup), K(ret));
  } else if (0 != (tmp_ret = rename(obproxy_old, obproxy_backup))) {
    if (ENOENT == errno) {
      LOG_WARN("!Attention: 'obproxy_old' do not exist, the current proxy process can still work",
                K(tmp_ret), K(obproxy_old), KERRMSGS);
    } else {
      ret = ob_get_sys_errno();
      LOG_WARN("!Attention: fail to rename 'obproxy_old' back to 'obproxy.old', "
               "the current binary 'obproxy.old' is not belong to the last proxy",
               KERRMSGS, K(ret), K(obproxy_old), K(obproxy_backup));
    }
  }
  LOG_INFO("finish backup old binary", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

int ObHotUpgradeProcessor::init_raw_client(ObRawMysqlClient &raw_client, const ObProxyLoginUserType type)
{
  int ret = OB_SUCCESS;
  //1. get config server username and passwd
  ObString string_username;
  ObString string_passwd;
  ObString string_passwd1;
  ObString string_db;
  char full_user_name[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1] = {0};
  char passwd_staged1_buf[ENC_STRING_BUF_LEN] = {0}; // 1B '*' + 40B octal num
  ObProxyLoginInfo login_info;
  int64_t pos = 0;

  switch (type) {
    case USER_TYPE_METADB: {
      string_passwd.assign_ptr(passwd_staged1_buf, ENC_STRING_BUF_LEN);
      if (OB_FAIL(get_global_config_server_processor().get_proxy_meta_table_login_info(login_info))) {
        LOG_WARN("fail to get meta table login info", K(ret));
      } else if (OB_UNLIKELY(!login_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid proxy meta table login info", K(login_info), K(ret));
      } else if (OB_FAIL(databuff_printf(full_user_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, pos, "%.*s#%s",
          login_info.username_.length(), login_info.username_.ptr(), OB_META_DB_CLUSTER_NAME))) {
        LOG_WARN("fail to databuff_printf", K(login_info), "cluster name", OB_META_DB_CLUSTER_NAME, K(ret));
      } else if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(login_info.password_, string_passwd))) {
        LOG_WARN("fail to encrypt_passwd_to_stage1", K(login_info), "cluster name", OB_META_DB_CLUSTER_NAME, K(ret));
      } else {
        string_passwd += 1;//cut the head'*'
        string_username.assign_ptr(full_user_name, static_cast<ObString::obstr_size_t>(pos));
        string_db = login_info.db_;
      }
      break;
    }
    case USER_TYPE_PROXYRO: {
      char cluster_name[OB_PROXY_FULL_USER_NAME_MAX_LEN + 1];
      cluster_name[0] = '\0';
      if (OB_FAIL(get_global_config_server_processor().get_default_cluster_name(cluster_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1))) {
        LOG_WARN("fail to get default cluster name", K(ret));
      } else {
        ObString default_cname = '\0' == cluster_name[0]
                                 ? ObString::make_string(OB_PROXY_DEFAULT_CLUSTER_NAME)
                                 : ObString::make_string(cluster_name);
        if (OB_FAIL(databuff_printf(full_user_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, pos, "%s#%.*s",
            ObProxyTableInfo::READ_ONLY_USERNAME, default_cname.length(), default_cname.ptr()))) {
          LOG_WARN("fail to databuff_printf", "username", ObProxyTableInfo::READ_ONLY_USERNAME, K(default_cname), K(ret));
        } else {
          string_username.assign_ptr(full_user_name, static_cast<ObString::obstr_size_t>(pos));
          string_passwd.assign_ptr(get_global_proxy_config().observer_sys_password.str(),
                                   static_cast<ObString::obstr_size_t>(strlen(get_global_proxy_config().observer_sys_password.str())));
          string_passwd1.assign_ptr(get_global_proxy_config().observer_sys_password1.str(),
                                   static_cast<ObString::obstr_size_t>(strlen(get_global_proxy_config().observer_sys_password1.str())));
          string_db.assign_ptr(ObProxyTableInfo::READ_ONLY_DATABASE,
                               static_cast<ObString::obstr_size_t>(STRLEN(ObProxyTableInfo::READ_ONLY_DATABASE)));
        }
      }
      break;
    }
    case USER_TYPE_PROXYSYS: {
      if (OB_FAIL(databuff_printf(full_user_name, OB_PROXY_FULL_USER_NAME_MAX_LEN + 1, pos, "%s@%s",
          OB_PROXYSYS_USER_NAME, OB_PROXYSYS_TENANT_NAME))) {
        LOG_WARN("fail to databuff_printf", "user name", OB_PROXYSYS_USER_NAME,
                 "tenant name", OB_PROXYSYS_TENANT_NAME, K(ret));
      } else {
        string_username.assign_ptr(full_user_name, static_cast<ObString::obstr_size_t>(pos));
        string_passwd.assign_ptr(get_global_proxy_config().obproxy_sys_password.str(),
                                 static_cast<ObString::obstr_size_t>(STRLEN(get_global_proxy_config().obproxy_sys_password.str())));
        string_db.reset();
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unknown ObProxyLoginUserType, it should not enter here", K(type), K(ret));
    }
  }

  //2. fill ObRawMysqlClient
  if (OB_SUCC(ret)) {
    ObSEArray<ObAddr, 1> local_addrs;
    ObAddr addr;
    if (OB_UNLIKELY(!addr.set_ipv4_addr(proxy_ip_, proxy_port_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to set addr", K(proxy_ip_), K(proxy_port_), K(ret));
    } else if (OB_FAIL(local_addrs.push_back(addr))) {
      LOG_WARN("fail to push addr to local_addrs", K(addr), K(ret));
    } else if (OB_FAIL(raw_client.init(string_username, string_passwd, string_db, string_passwd1))) {
      LOG_WARN("fail to init raw mysql client", K(string_username), K(string_db), K(ret));
    } else if (OB_FAIL(raw_client.set_server_addr(local_addrs))) {
      LOG_WARN("fail to set server addr", K(ret));
    } else {
      LOG_DEBUG("succ to fill raw client", K(string_username), K(string_db));
    }
  }
  return ret;
}

int ObHotUpgradeProcessor::send_cmd_and_check_response(const char *sql,
    const ObProxyLoginUserType type, const int64_t max_retry_times/*1*/, const int64_t expected_affected_rows/*0*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type <= USER_TYPE_NONE)
      || OB_UNLIKELY(type >= USER_TYPE_MAX)
      || OB_ISNULL(sql)
      || OB_UNLIKELY(max_retry_times <= 0)
      || OB_UNLIKELY(expected_affected_rows < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(sql), K(type), K(expected_affected_rows), K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t timeout_ms = usec_to_msec(get_global_proxy_config().short_async_task_timeout);
    ObRawMysqlClient raw_client;
    ObClientMysqlResp *resp = NULL;
    int64_t affected_rows = 0;
    bool is_done = false;
    if (OB_FAIL(init_raw_client(raw_client, type))) {
      LOG_WARN("fail to init raw client", K(type), K(ret));
    } else {
      info_.set_rejected_user(type);

      for (int64_t i = 0; i < max_retry_times && OB_SUCC(ret) && !is_done; ++i) {
        if (OB_FAIL(raw_client.sync_raw_execute(sql, timeout_ms, resp))) {
          LOG_WARN("fail to sync raw execute", K(sql), K(timeout_ms), K(i), K(max_retry_times), K(ret));
          if (i < (max_retry_times - 1)) {
            ret = OB_SUCCESS;//ignore it
          }
        } else if (OB_ISNULL(resp)) {
          ret = OB_NO_RESULT;
          LOG_WARN("resp is NULL", K(i), K(timeout_ms), K(sql), K(ret));
        } else {
          if (resp->is_error_resp()) {
            ret = -resp->get_err_code();
            LOG_WARN("fail to execute sql", K(sql), K(ret));
          } else if (resp->is_ok_resp()) {
            if (OB_FAIL(resp->get_affected_rows(affected_rows))) {
              LOG_WARN("fail to get affected rows", K(sql), K(ret));
            } else if (OB_UNLIKELY(expected_affected_rows != affected_rows)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected affected_rows", K(expected_affected_rows), K(affected_rows), K(sql), K(ret));
            } else {
              is_done = true;
              LOG_DEBUG("succ to send cmd", K(i), K(sql));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected resp", K(ret));
          }
        }
        if (NULL != resp) {
          op_free(resp);
          resp = NULL;
        }
      }
      info_.reset_rejected_user();

      if (OB_SUCC(ret) && !is_done) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    LOG_INFO("finish send cmd and check response", "cost time(us)",
             ObTimeUtility::current_time() - now, K(sql), K(ret));
  }
  return ret;
}

//parent proxy need send commit via subprocess for checking sub available
int ObHotUpgradeProcessor::send_commit_via_subprocess()
{
  int ret = OB_SUCCESS;
  const ObHotUpgradeCmd orig_cmd = HUC_AUTO_UPGRADE;
  const ObHotUpgradeCmd orig_cmd_either = HUC_RESTART;
  const ObHotUpgradeCmd target_cmd = HUC_COMMIT;
  const int64_t max_retry_times = 1;
  const int64_t expected_affected_rows = 1;
  char sql[OB_SHORT_SQL_LENGTH];
  if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_update_hu_cmd_sql(sql, OB_SHORT_SQL_LENGTH,
      proxy_ip_, proxy_port_, target_cmd, orig_cmd, orig_cmd_either))) {
    LOG_WARN("fail to get proxy update hu cmd sql", K(sql), K(OB_SHORT_SQL_LENGTH), K_(proxy_ip),
             K_(proxy_port), K(target_cmd), K(orig_cmd), K(orig_cmd_either), K(ret));
  } else if (OB_FAIL(send_cmd_and_check_response(sql, USER_TYPE_METADB, max_retry_times, expected_affected_rows))) {
    LOG_WARN("fail to send commit via subprocess", K(sql), K(USER_TYPE_METADB), K(ret));
  } else {
    LOG_INFO("succ to send commit via subprocess");
  }
  return ret;
}

int ObHotUpgradeProcessor::check_subprocess_available()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  //if failures is in [0, max/2], use proxyro to check observer
  //otherwise failures is in [0, max/2], use proxyro to check observer
  const bool use_proxysys = (check_available_failures_ >= (OB_MAX_CHECK_SUBPROCESS_FAILURES / 2));
  char sql[OB_SHORT_SQL_LENGTH];
  ObRawMysqlClient raw_client;
  int64_t len = 0;
  ObProxyLoginUserType login_type = USER_TYPE_NONE;
  if (use_proxysys) {
    login_type = USER_TYPE_PROXYSYS;
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, "show proxystat refresh like 'current_cluster_resource_count'");
  } else {
    login_type = USER_TYPE_PROXYRO;
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, "SELECT /*+READ_CONSISTENCY(WEAK)*/ 1");
  }
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_FAIL(init_raw_client(raw_client, login_type))) {
    LOG_WARN("fail to init raw client", K(ret));
  } else {
    const int64_t timeout_ms = usec_to_msec(get_global_proxy_config().short_async_task_timeout);
    ObClientMysqlResp *resp = NULL;
    ObMysqlResultHandler result_handler;
    info_.set_rejected_user(login_type);
    if (OB_FAIL(raw_client.sync_raw_execute(sql, timeout_ms, resp))) {
      LOG_WARN("fail to sync raw execute", K(sql), K(resp), K(timeout_ms), K(ret));
    } else if (OB_ISNULL(resp)) {
      ret = OB_NO_RESULT;
      LOG_WARN("resp is NULL", K(timeout_ms), K(sql), K(ret));
    } else {
      if (resp->is_error_resp()) {
        ret = -resp->get_err_code();
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (!resp->is_resultset_resp()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected resp, this is not resultset", K(ret));
      } else {
        result_handler.set_resp(resp);
        resp = NULL;
        if (OB_FAIL(result_handler.next())) {
          LOG_WARN("fail to get result", K(sql), K(ret));
        } else {
          int64_t value = 0;
          if (use_proxysys) {
            PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "value", value, int64_t);
          } else {
            PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "1", value, int64_t);
          }
          //check if this is only one
          if (OB_SUCC(ret)) {
            if (OB_ITER_END != (ret = result_handler.next())) {
              LOG_WARN("fail to get result, there is more than one record", K(sql), K(value), K(ret));
              ret = OB_ERR_UNEXPECTED;
            } else if (use_proxysys && value <= 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error unexpected result", K(sql), K(value), K(ret));
            } else if (!use_proxysys  && 1 != value) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error unexpected result", K(sql), K(value), K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      if (NULL != resp) {
        op_free(resp);
        resp = NULL;
      }
    }
    info_.reset_rejected_user();
  }
  LOG_INFO("finish check subprocess available", "cost time(us)", ObTimeUtility::current_time() - now,
           K(sql), K(ret));
  return ret;
}

int ObHotUpgradeProcessor::do_hot_upgrade_repeat_task()
{
  return get_global_hot_upgrade_processor().do_hot_upgrade_work();
}

void ObHotUpgradeProcessor::update_hot_upgrade_interval()
{
  ObAsyncCommonTask *cont = get_global_hot_upgrade_processor().get_new_hot_upgrade_cont();
  if (OB_LIKELY(NULL != cont)) {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().proxy_hot_upgrade_check_interval);
    cont->set_interval(interval_us);
  }
}

int ObHotUpgradeProcessor::start_hot_upgrade_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("obproxy table processor is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL != hot_upgrade_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hot upgrade cont has been scheduled", K(ret));
  } else {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().proxy_hot_upgrade_check_interval);
    if (OB_ISNULL(hot_upgrade_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                      "proxy_info_check_task",
                                      ObHotUpgradeProcessor::do_hot_upgrade_repeat_task,
                                      ObHotUpgradeProcessor::update_hot_upgrade_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start proxy table check task", K(ret));
    } else {
      LOG_INFO("succ to start proxy table check task", K(interval_us));
    }
  }
  return ret;
}

int ObHotUpgradeProcessor::do_hot_upgrade_work()
{
  int ret = OB_SUCCESS;

  ObProxyLocalCMDType cmd_type = get_global_proxy_config().get_local_cmd_type();
  switch (cmd_type) {
    case OB_LOCAL_CMD_NONE: {
      //do nothing
      break;
    }
    case OB_LOCAL_CMD_EXIT: {
      get_global_proxy_config().reset_local_cmd();
      if (info_.is_in_idle_state() && info_.is_parent()) {
        info_.cmd_ = HUC_LOCAL_EXIT;
        LOG_INFO("proxy will do quick exit from local cmd, no need check proxy info", K_(info));
      } else {
        LOG_WARN("it is doing hot upgrading now, CAN NOT do quick exit from local cmd", K_(info));
     }

     break;
   }
    case OB_LOCAL_CMD_RESTART: {
      get_global_proxy_config().reset_local_cmd();
      if (info_.is_in_idle_state() && info_.is_parent()) {
        info_.cmd_ = HUC_LOCAL_RESTART;
        LOG_INFO("proxy will do restart from local cmd", K_(info));
      } else {
        LOG_WARN("it is doing hot upgrading now, CAN NOT do quick restart from local cmd", K_(info));
      }
      break;
    }
    default: {
      LOG_WARN("unknown ObProxyLocalCMDType, reset it default value", K(cmd_type));
      get_global_proxy_config().reset_local_cmd();
    }
  }

  switch (info_.get_state()) {
    case HU_STATE_WAIT_HU_CMD: {
      LOG_DEBUG("enter HU_STATE_WAIT_HU_CMD state", "pid", getpid(),
                "current status", ObHotUpgraderInfo::get_status_string(info_.status_),
                "receive cmd", ObHotUpgraderInfo::get_cmd_string(info_.cmd_));

      switch (info_.cmd_) {
        case HUC_LOCAL_RESTART: {
          g_ob_prometheus_processor.destroy_exposer();
          if (OB_FAIL(ObProxyHotUpgrader::spawn_process())) {
            LOG_WARN("fail to spawn sub process", K_(info), K(ret));
            g_ob_prometheus_processor.create_exposer();
          } else {
            schedule_timeout_rollback();
            info_.disable_net_accept();  // disable accecpt new connection
            info_.update_sub_status(HU_STATUS_NONE);
            info_.update_state(HU_STATE_WAIT_LOCAL_CR_FINISH);
            LOG_INFO("succ to fork new proxy, start turn to HU_STATE_WAIT_CR_FINISH", K_(info));
          }
          break;
        }
        case HUC_LOCAL_EXIT: {
          info_.disable_net_accept();
          info_.graceful_exit_start_time_ = get_hrtime_internal();
          info_.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().delay_exit_time)
            + info_.graceful_exit_start_time_; // nanosecond
          g_proxy_fatal_errcode = OB_SERVER_IS_STOPPING;
          LOG_WARN("parent process stop accepting new connection, stop check timer", K_(info), K(g_proxy_fatal_errcode), K(ret));
          break;
        }
        case HUC_NONE:
          break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unknown ObHotUpgradeCmd, it should not enter here", K_(info), K(ret));
        }
      }
      info_.cmd_ = HUC_NONE;
      break;
    }
    case HU_STATE_WAIT_LOCAL_CR_FINISH: {
      if (OB_LIKELY(common::OB_SUCCESS == lib::mutex_acquire(&info_.hot_upgrade_mutex_))) {
        if (info_.is_sub_exited()) {
          info_.enable_net_accept();
          g_ob_prometheus_processor.create_exposer();
          info_.update_sub_status(HU_STATUS_NONE);
          info_.update_state(HU_STATE_WAIT_HU_CMD);
        } else {
          if (OB_LIKELY(!info_.parent_hot_upgrade_flag_)) {
            ObHRTime diff_time = 0;
            if (OB_LIKELY(timeout_rollback_timeout_at_ <= 0)) {
              // maybe has not started
            } else if (0 < (diff_time = timeout_rollback_timeout_at_ - get_hrtime_internal())) {
              LOG_DEBUG("it is not time to do hot upgrade timeout rollback job",
                        K_(timeout_rollback_timeout_at), K(diff_time));
            } else {
              kill(info_.sub_pid_, SIGKILL);
              info_.enable_net_accept();
              g_ob_prometheus_processor.create_exposer();
              info_.update_state(HU_STATE_WAIT_HU_CMD);
              info_.parent_hot_upgrade_flag_ = true;
              LOG_INFO("upgrade timeout, will send SIGKILL to subprocess", K_(info));
            }
          }
        }
        lib::mutex_release(&info_.hot_upgrade_mutex_);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("it should not enter here", K_(info));
    }
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

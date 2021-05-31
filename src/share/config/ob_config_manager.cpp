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

#include "share/config/ob_config_manager.h"
#include "lib/file/file_directory_utils.h"
#include "lib/profile/ob_trace_id.h"

namespace oceanbase
{
namespace common
{

int ObConfigManager::base_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(system_config_.init())) {
    LOG_ERROR("init system config failed", K(ret));
  } else if (OB_FAIL(server_config_.init(system_config_))) {
    LOG_ERROR("init server config failed", K(ret));
  }
  update_task_.config_mgr_ = this;
  return ret;
}

int ObConfigManager::init(const ObAddr &server, ObTimer &timer)
{
  self_ = server;
  timer_ = &timer;
  return OB_SUCCESS;
}

int ObConfigManager::init(ObMySQLProxy &sql_proxy, const ObAddr &server, ObTimer &timer)
{
  sql_proxy_ = &sql_proxy;
  self_ = server;
  timer_ = &timer;
  return OB_SUCCESS;
}

int ObConfigManager::reload_config()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_config_.check_all())) {
    LOG_WARN("Check configuration failed, can't reload", K(ret));
  } else if (OB_FAIL(reload_config_func_())) {
    LOG_WARN("Reload configuration failed.", K(ret));
  }
  return ret;
}

int ObConfigManager::load_config(const char *path)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;

  if (OB_ISNULL(path) || OB_UNLIKELY(STRLEN(path) <= 0)) {
    path = dump_path_;
  }

  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(
      OB_MAX_PACKET_LENGTH, ObModIds::OB_BUFFER)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed", LITERAL_K(OB_MAX_PACKET_LENGTH), K(ret));
  } else if (OB_ISNULL(fp = fopen(path, "rb"))) {
    if (ENOENT == errno) {
      ret = OB_FILE_NOT_EXIST;
      LOG_INFO("Config file doesn't exist, read from command line", K(path), K(ret));
    } else {
      ret = OB_IO_ERROR;
      LOG_ERROR("Can't open file", K(path), KERRMSGS, K(ret));
    }
  } else {
    LOG_INFO("Using config file", K(path));
    MEMSET(buf, 0, OB_MAX_PACKET_LENGTH);
    int64_t len = fread(buf, 1, OB_MAX_PACKET_LENGTH, fp);
    int64_t pos = 0;

    if (OB_UNLIKELY(0 != ferror(fp))) { // read with error
      ret = OB_IO_ERROR;
      LOG_ERROR("Read config file error", K(path), K(ret));
    } else if (OB_UNLIKELY(0 == feof(fp))) { // not end of file
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("Config file is too long", K(path), K(ret));
    } else if (OB_FAIL(server_config_.deserialize(buf, len, pos))) {
      LOG_ERROR("Deserialize server config failed", K(path), K(ret));
    } else if (OB_UNLIKELY(pos != len)) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_ERROR("Deserialize server config failed", K(path), K(ret));
    }
    if (OB_UNLIKELY(0 != fclose(fp))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Close config file failed", K(ret));
    }
  }
  if (OB_LIKELY(NULL != buf)) {
    ob_free(buf);
    buf = NULL;
  }

  return ret;
}

int ObConfigManager::dump2file(const char *path) const
{
  int ret = OB_SUCCESS;
  int fd = 0;
  ssize_t size = 0;

  if (OB_ISNULL(path)) {
    path = dump_path_;
  }

  PageArena<> pa;

  if (OB_ISNULL(path) || STRLEN(path) <= 0) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("NO dump path specified!", K(ret));
  } else {
    // write server config
    char *buf = NULL;
    char *tmp_path = NULL;
    char *hist_path = NULL;
    int64_t pos = 0;
    if (OB_ISNULL(buf = pa.alloc(OB_MAX_PACKET_LENGTH))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob tc malloc memory for buf failed", K(ret));
    }
    if (OB_ISNULL(tmp_path = pa.alloc(MAX_PATH_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob tc malloc memory for tmp configure path failed", K(ret));
    } else {
      snprintf(tmp_path, MAX_PATH_SIZE, "%s.tmp", path);
    }
    if (OB_ISNULL(hist_path = pa.alloc(MAX_PATH_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("ob tc malloc memory for history configure path fail", K(ret));
    } else {
      snprintf(hist_path, MAX_PATH_SIZE, "%s.history", path);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(server_config_.serialize(buf, OB_MAX_PACKET_LENGTH, pos))) {
        LOG_WARN("Serialize server config fail!", K(ret));
      } else if ((fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC,
                              S_IRUSR  | S_IWUSR | S_IRGRP)) < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to create config file", K(tmp_path), KERRMSGS, K(ret));
      } else if (pos != (size = write(fd, buf, pos))) {
        ret = OB_IO_ERROR;
        LOG_WARN("Write server config fail!", K(pos), K(size), K(ret));
        if (0 != close(fd)) {
          LOG_WARN("fail to close file fd", K(fd), KERRMSGS, K(ret));
        }
      } else if (0 != close(fd)) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to close file fd", K(fd), KERRMSGS, K(ret));
      } else {
        LOG_INFO("Write server config successfully!");
      }
    }

    if (OB_SUCC(ret)) {
      if (0 != ::rename(path, hist_path) && errno != ENOENT) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to backup history config file", KERRMSGS, K(ret));
      }

      if (0 != ::rename(tmp_path, path)) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to move tmp config file", KERRMSGS, K(ret));
      }
    }
  }
  return ret;
}

int ObConfigManager::update_local()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObMySQLProxy::MySQLResult result;
    int64_t start = ObTimeUtility::current_time();
    const char *sqlstr = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
        "data_type, value, value_strict, info, need_reboot, section, visible_level "
        "from __all_sys_parameter";
    if (OB_FAIL(sql_proxy_->read(result, sqlstr))) {
      LOG_WARN("read config from __all_sys_parameter failed", K(sqlstr), K(ret));
    } else {
      LOG_INFO("read config from __all_sys_parameter succed", K(start));
      ret = system_config_.update(result);
    }
  }

  if (OB_SUCC(ret)) {
    if ('\0' == dump_path_[0]) {
      ret = OB_NOT_INIT;
      LOG_ERROR("Dump path doesn't set, stop read config", K(ret));
    } else if (OB_FAIL(server_config_.read_config())) {
      LOG_ERROR("Read server config failed", K(ret));
    } else if (OB_FAIL(reload_config())) {
      LOG_WARN("Reload configuration failed", K(ret));
    } else if (OB_FAIL(dump2file())) {
      LOG_ERROR("Dump to file failed", K_(dump_path), K(ret));
    } else {
      LOG_INFO("Reload server config successfully!");
      char path[MAX_PATH_SIZE] = {};
      static const char *CONF_COPY_NAME = "/observer.conf.bin";

      for (int64_t idx = 0; idx < server_config_.config_additional_dir.size(); ++idx) {
        if (OB_SUCC(server_config_.config_additional_dir.get(idx, path, MAX_PATH_SIZE))) {
          if (STRLEN(path) > 0) {
            if (OB_FAIL(common::FileDirectoryUtils::create_full_path(path))) {
              LOG_ERROR("create additional configure directory fail", K(path), K(ret));
            } else if (STRLEN(path) + STRLEN(CONF_COPY_NAME) < static_cast<uint64_t>(MAX_PATH_SIZE)) {
              strcat(path, CONF_COPY_NAME);
              if (OB_FAIL(dump2file(path))) {
                LOG_ERROR("make additional configure file copy fail", K(path), K(ret));
                ret = OB_SUCCESS;  // ignore ret code.
              }
            } else {
              LOG_ERROR("additional configure directory path is too long",
                        K(path), "len", STRLEN(path));
            }
          }
        }
      }
    }
    server_config_.print();
  } else {
    LOG_WARN("Read system config from inner table error", K(ret));
  }
  return ret;
}

int ObConfigManager::got_version(int64_t version, const bool remove_repeat)
{
  int ret = OB_SUCCESS;
  bool schedule_task = false;
  if (version < 0) {
    // from rs_admin, do whatever
    update_task_.update_local_ = false;
    schedule_task = true;
  } else if (0 == version) {
    // do nothing
    LOG_DEBUG("root server restarting");
  } else if (current_version_ == version) {
    // no new version
  } else if (version < current_version_) {
    LOG_WARN("Local config is newer than rs, weird", K_(current_version), K(version));
  } else if (version > current_version_) {
    if (!mutex_.tryLock()) {
      LOG_DEBUG("Processed by other thread!");
    } else {
      if (version > newest_version_) {
        // local:current_version_, newest:newest_version, got:version
        LOG_INFO("Got new config version", K_(current_version), K_(newest_version), K(version));
        newest_version_ = version;  // for rootserver hb to others
        update_task_.update_local_ = true;
        schedule_task = true;
      } else if (version < newest_version_) {
        // impossible, local:current_version_, newest:newest_version, got:version
        LOG_WARN("Receive weird config version",
                 K_(current_version), K_(newest_version), K(version));
      }
      mutex_.unlock();
    }
  }

  if (schedule_task) {
    bool schedule = true;
    if (OB_ISNULL(timer_)) {
      // if got a config version before manager init, ignore this version
      newest_version_ = current_version_;
      schedule = false;
      ret = OB_NOT_INIT;
      LOG_WARN("Couldn't update config because timer is NULL", K(ret));
    } else if (true == remove_repeat) {
      schedule = !timer_->task_exist(update_task_);
      LOG_INFO("no need repeat schedule the same task");
    } else {
      // do nothing
    }

    if (schedule) {
      update_task_.version_ = version;
      update_task_.scheduled_time_ = obsys::CTimeUtil::getMonotonicTime();
      if (OB_FAIL(timer_->schedule(update_task_, 0, false))) {
        LOG_ERROR("Update local config failed", K(ret));
      } else {
        LOG_INFO("Schedule update config task successfully!");
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

void ObConfigManager::UpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(config_mgr), K(ret));
  } else if (OB_ISNULL(config_mgr_->timer_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(config_mgr_->timer), K(ret));
  } else {
    int64_t current_version = config_mgr_->current_version_;
    int64_t version = version_;
    ObCurTraceId::init(config_mgr_->self_);
    if (config_mgr_->current_version_ == version) {
      ret = OB_ALREADY_DONE;
    } else if (config_mgr_->newest_version_ > version) {
      ret = OB_CANCELED;
    } else if (update_local_) {
      config_mgr_->current_version_ = version;
      if (OB_FAIL(config_mgr_->system_config_.clear())) {
        // just print log, ignore ret
        LOG_WARN("Clear system config map failed", K(ret));
      } else {
        // do nothing
      }
      if (OB_FAIL(config_mgr_->update_local())) {
        LOG_WARN("Update local config failed", K(ret));
        // recovery current_version_
        config_mgr_->current_version_ = current_version;
        // retry update local config
        if (OB_FAIL(config_mgr_->timer_->schedule(*this, 1000 * 1000L, false))) {
          LOG_WARN("Reschedule update local config failed", K(ret));
        }
      } else {
        // do nothing
      }
    } else if (OB_FAIL(config_mgr_->reload_config())) {
      LOG_WARN("Reload configuration failed", K(ret));
    } else {
      config_mgr_->server_config_.print();
    }
    ObCurTraceId::reset();
  }
}

} // namespace common
} // namespace oceanbase

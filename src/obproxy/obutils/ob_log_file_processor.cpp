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
#include "obutils/ob_log_file_processor.h"
#include <sys/stat.h>
#include <sys/statfs.h>
#include <unistd.h>
#include <dirent.h>
#include "lib/container/ob_array_iterator.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_async_common_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
//------ ObProxyLogFileStruct------
DEF_TO_STRING(ObProxyLogFileStruct)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(mtime), K_(size), K_(full_path));
  J_OBJ_END();
  return pos;
}

ObLogFileProcessor &get_global_log_file_processor()
{
  static ObLogFileProcessor log_file_processor;
  return log_file_processor;
}

ObLogFileProcessor::ObLogFileProcessor()
  : is_inited_(false), cleanup_cont_(NULL)
{
}

void ObLogFileProcessor::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(cleanup_cont_))) {
      LOG_WARN("fail to destroy cleanup task", K(ret));
    }
  }
  is_inited_ = false;
}

int ObLogFileProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the log file processor has already been inited", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLogFileProcessor::start_cleanup_log_file()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("log file processor is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL != cleanup_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log file cleanup task has already been schuduled", K_(cleanup_cont), K(ret));
  } else {
    int64_t interval_us = get_global_proxy_config().log_cleanup_interval;
    if (OB_ISNULL(cleanup_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                  "log_cleanup_task",
                                  ObLogFileProcessor::do_repeat_task,
                                  ObLogFileProcessor::update_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start cleanup task", K(interval_us), K(ret));
    } else {
      LOG_INFO("succ to create and start log cleanup task", K(interval_us));
    }
  }

  return ret;
}

int ObLogFileProcessor::set_log_cleanup_interval()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("log file processor is not inited", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(cleanup_cont_))) {
    LOG_WARN("fail to set log cleanup intreval");
  }
  return ret;
}

int ObLogFileProcessor::match_file_name(const char *layout_dir, const char *file_name,
                                        ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator,
                                        ObProxyLogFileStruct &file_st,
                                        bool &need_further_handle)
{
  int ret = OB_SUCCESS;
  need_further_handle = false;
  allocator.reuse();
  char *full_path = NULL;
  int64_t pos = 0;
  struct stat st;
  if (OB_ISNULL(layout_dir) || OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(layout_dir), K(file_name), K(ret));
  } else if (OB_FAIL(ObLayout::merge_file_path(layout_dir, file_name, allocator, full_path))) {
    LOG_WARN("fail to merge file", K(layout_dir), K(file_name), K(ret));
  } else if (0 != (stat(full_path, &st))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to stat dir", K(full_path), KERRMSGS, K(ret));
  } else if (S_ISDIR(st.st_mode)) {
    LOG_DEBUG("skip directory", K(full_path));
  } else if (OB_FAIL(databuff_printf(file_st.full_path_, OB_MAX_FILE_NAME_LENGTH, pos, "%s", full_path))) {
    LOG_WARN("fail to copy full path to file_st", K(ret));
  } else {
    file_st.size_ = st.st_size;
    file_st.mtime_ = st.st_mtime;
    need_further_handle = true;
  }
  return ret;
}

int ObLogFileProcessor::get_disk_size(const char *dir, int64_t &avail_size)
{
  int ret = OB_SUCCESS;
  avail_size = 0;
  if (OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dir is null", K(ret));
  } else {
    struct statfs st;
    if (OB_UNLIKELY(0 != ::statfs(dir, &st))) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to read dir info", KERRMSGS, K(ret));
    } else {
      avail_size = st.f_bsize * st.f_bfree;
    }
  }
  return ret;
}

int ObLogFileProcessor::do_repeat_task()
{
  return get_global_log_file_processor().cleanup_log_file();
}

void ObLogFileProcessor::update_interval()
{
  ObAsyncCommonTask *cont = NULL;
  if (OB_LIKELY(NULL != (cont =
      get_global_log_file_processor().get_cleanup_cont()))) {
    int64_t interval_us = get_global_proxy_config().log_cleanup_interval;
    cont->set_interval(interval_us);
  }
}

int ObLogFileProcessor::cleanup_log_file()
{
  int ret = OB_SUCCESS;
  int64_t begin = ObTimeUtility::current_time();
  int64_t total_size = 0;
  int64_t cur_process_log_size = 0;
  ObArray<ObProxyLogFileStruct> log_array;
  ObArray<ObProxyLogFileStruct> invalid_log_array;
  struct dirent *ent = NULL;
  const char *layout_log_dir = NULL;
  DIR *log_dir = NULL;
  const bool is_in_single_service = get_global_hot_upgrade_info().is_in_single_service();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("logfile processor is not inited", K(ret));
  } else if (OB_ISNULL(layout_log_dir = get_global_layout().get_log_dir())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get log dir", K(ret));
  } else if (OB_ISNULL(log_dir = opendir(layout_log_dir))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to open dir", K(layout_log_dir), KERRMSGS, K(ret));
  } else {
    bool need_further_handle = false;
    ObProxyLogFileStruct log_st;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    ObString file_name;
    while (OB_SUCC(ret) && NULL != (ent = readdir(log_dir))) {
      file_name.assign_ptr(ent->d_name, static_cast<int32_t>(STRLEN(ent->d_name)));
      if (!file_name.prefix_match("obproxy")) {
        // we only handle logs writtente by obproxy, and all xflush log should not be deleted
      } else if (OB_FAIL(match_file_name(layout_log_dir, ent->d_name, allocator, log_st, need_further_handle))) {
        LOG_WARN("fail to filter log file", K(ret));
      } else if (need_further_handle) {
        need_further_handle = false;
        total_size += log_st.size_;

        bool skip_file = false;
        bool self_process_file = false;
        ObString full_file_name;
        full_file_name.assign_ptr(log_st.full_path_, static_cast<int32_t>(STRLEN(log_st.full_path_)));

        ObString current_file_name;
        ObString current_wf_file_name;
        char wf_filename[OB_MAX_FILE_NAME_LENGTH + 1] = "\0";

        for (ObLogFDType type = FD_DEFAULT_FILE; !skip_file && type < MAX_FD_FILE; type = (ObLogFDType)(type + 1)) {
          const ObLogFileStruct& log_file = OB_LOGGER.get_log_file(type);
          current_file_name.assign_ptr(log_file.filename_, static_cast<int32_t>(STRLEN(log_file.filename_)));
          if (log_file.is_enable_wf_file() && log_file.wf_fd_ > 0) {
            snprintf(wf_filename, OB_MAX_FILE_NAME_LENGTH, "%s.wf", log_file.filename_);
            current_wf_file_name.assign_ptr(wf_filename, static_cast<int32_t>(STRLEN(wf_filename)));
          } else {
            current_wf_file_name.reset();
          }

          if (type == FD_DEFAULT_FILE
           && ((!current_file_name.empty() && full_file_name.prefix_match(current_file_name))
            || (!current_wf_file_name.empty() && full_file_name.prefix_match(current_wf_file_name)))) {
            cur_process_log_size += log_st.size_;
            self_process_file = true;
          }

          if (full_file_name == current_file_name || full_file_name == current_wf_file_name) {
            skip_file = true;
          }
        }

        if (skip_file) {
          //skip current using log
        } else if (!is_in_single_service) {
          //when in hot upgrade progress, just clean up log of self process
          if (self_process_file) {
            need_further_handle = true;
          }
        } else {
          need_further_handle = true;
          if ((file_name.after('.')).after('.') == "log") {
            // handle other invalid obproxy.xxx.log
            if (OB_FAIL(invalid_log_array.push_back(log_st))) {
              LOG_WARN("fail to add file into invalid log array", K(ret));
            } else {
              need_further_handle = false;
            }
          }
        }
        if (need_further_handle && OB_FAIL(log_array.push_back(log_st))) {
          LOG_WARN("fail to add file into log array", K(log_array), K(log_st), K(ret));
        }
      }
    }//end of while
  }
  if (OB_SUCC(ret)) {
    std::sort(log_array.begin(), log_array.end());
    if (OB_FAIL(do_cleanup_invalid_log_file(invalid_log_array))) {
      LOG_WARN("fail to do cleanup invalid log file", K(ret));
    } else if (OB_FAIL(do_cleanup_log_file(log_array, is_in_single_service, cur_process_log_size, total_size))) {
      LOG_WARN("fail to do cleanup log file", K(ret));
    }
  }
  if (OB_LIKELY(NULL != log_dir) && OB_UNLIKELY(0 != closedir(log_dir))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to close dir", K(log_dir), KERRMSGS, K(ret));
  }

  LOG_DEBUG("finish cleanup log file", "log_array count", log_array.count(),
            "cost time(us)", ObTimeUtility::current_time() - begin);
  return ret;
}

int ObLogFileProcessor::do_cleanup_invalid_log_file(ObArray<ObProxyLogFileStruct> &log_array)
{
  int ret = OB_SUCCESS;
  std::sort(log_array.begin(), log_array.end());
  int64_t delete_num = log_array.count() - MAX_INVALID_PROXY_LOG_NUM;
  for (int64_t i = 0; OB_SUCC(ret) && i < delete_num; ++i) {
    if (OB_UNLIKELY(0 != ::unlink(log_array[i].full_path_))) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to unlink file", K(log_array[i].full_path_), KERRMSGS, K(ret));
    }
  }
  return ret;
}

int ObLogFileProcessor::do_cleanup_log_file(ObArray<ObProxyLogFileStruct> &log_array, const bool is_in_single_service,
                                            const int64_t cur_process_log_size, int64_t &total_size)
{
  int ret = OB_SUCCESS;
  int64_t avail_size = 0;
  static const int64_t min_avail_size = 1024 * 1024 * 1024; // 1GB
  if (OB_FAIL(get_disk_size(get_global_layout().get_log_dir(), avail_size))) {
    // if fail, just ignore ret, we only use log_dir_size_threshold to do cleanup
    LOG_WARN("fail to get disk size", K(ret));
    ret = OB_SUCCESS;
  }
  ObProxyConfig &config = get_global_proxy_config();
  int64_t thresh_hold = config.log_dir_size_threshold;
  int64_t log_file_percentage = config.log_file_percentage;
  if (!is_in_single_service) {
    if (total_size > 0) {
      double proportion = static_cast<double>(cur_process_log_size) / static_cast<double>(total_size);
      thresh_hold = static_cast<int64_t>(static_cast<double>(thresh_hold) * proportion);
      total_size = cur_process_log_size;
    }
  }
  if (OB_LIKELY(avail_size >= 0) && OB_LIKELY(log_file_percentage > 0)) {
    if (avail_size <= min_avail_size) {
      LOG_WARN("disk avail size is too small!!!", K(avail_size));
    }
    thresh_hold = std::min(thresh_hold, ((total_size + avail_size) / 100) * log_file_percentage);
  }
  if (thresh_hold < total_size) {
    LOG_INFO("begin to delete log file", K(thresh_hold), K(total_size));
    int64_t low_water_mark = thresh_hold / 2;
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < log_array.count() && low_water_mark < total_size; ++i) {
      if (OB_UNLIKELY(0 != ::unlink(log_array[i].full_path_))) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to unlink file", K(log_array[i].full_path_), KERRMSGS, K(ret));
      } else {
        total_size -= log_array[i].size_;
        LOG_INFO("succ to cleanup file", K(log_array[i].full_path_), K(ret));
      }
    }
    if (OB_UNLIKELY(i == log_array.count()) && OB_UNLIKELY(thresh_hold < total_size)) {
      LOG_WARN("no file could be cleanup any more, but log total size is still larger than thresh_hold",
               K(thresh_hold), K(low_water_mark), K(total_size));
    }
  }
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase

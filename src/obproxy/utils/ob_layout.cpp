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
#include "utils/ob_layout.h"
#include <unistd.h>

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{

ObLayout &get_global_layout()
{
  static ObLayout g_layout;
  return g_layout;
}

ObLayout::ObLayout()
    : is_inited_(false), prefix_(NULL), bin_dir_(NULL), etc_dir_(NULL),
      log_dir_(NULL), conf_dir_(NULL), control_config_dir_(NULL), dbconfig_dir_(NULL)
{
}

ObLayout::~ObLayout()
{
}

int ObLayout::init(const char *start_cmd)
{
  int ret = OB_SUCCESS;
  char *cwd = NULL;
  PageArena<char> arena;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    MPRINT("layout has already been inited, ret=%d", ret);
  } else if (OB_ISNULL(start_cmd)) {
    ret = OB_ERR_UNEXPECTED;
    MPRINT("start cmd is NULL, ret=%d", ret);
  } else if (OB_ISNULL(cwd = arena.alloc(MAX_PATH_LENGTH))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    MPRINT("fail to alloc memeory,alloc_size=%ld, ret=%d", MAX_PATH_LENGTH, ret);
  } else if (OB_ISNULL(getcwd(cwd, MAX_PATH_LENGTH))) {
    ret = OB_ERR_UNEXPECTED;
    MPRINT("fail to get current working dir, errno=%d, errmsg=%s, ret=%d", errno, strerror(errno), ret);
  } else if (OB_FAIL(init_bin_dir(cwd, start_cmd))) {
    MPRINT("fail to init proxy bin dir, ret=%d", ret);
  } else if (OB_FAIL(init_dir_prefix(cwd))) {
    MPRINT("fail to init proxy dir prefix, ret=%d", ret);
  } else if (OB_FAIL(construct_dirs())) {
    MPRINT("fail to construct dirs, ret=%d", ret);
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObLayout::merge_file_path(const char *root, const char *file, common::ObIAllocator &allocator, char *&buf)
{
  int ret = OB_SUCCESS;
  buf = NULL;

  if (OB_ISNULL(root) || OB_ISNULL(file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "root", P(root), "file", P(file), K(ret));
  } else {
    int64_t root_len = STRLEN(root);
    int64_t file_len = STRLEN(file);
    //one '/' append after root and a '\0' at the end
    int64_t need_len = root_len + file_len + 2;
    int64_t path_len = 0;
    char *path = NULL;
    if (OB_ISNULL(path = static_cast<char *>(allocator.alloc(need_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memeory", K(need_len), K(ret));
    } else if ('/' == file[0]) {
      //do nothing if file is rooted ,just return file
      path[0] = '/';
      path_len = 1;
    } else {
      MEMCPY(path, root, root_len);
      path_len = root_len;
      if ((root_len > 0) && ('/' != path[root_len - 1])) {
        path[path_len++] = '/';
      }
    }

    while (OB_SUCC(ret) && '\0' != *file) {
      const char *next = file;
      while ('\0' != *next && '/' != (*next)) {
        ++next;
      }
      int64_t seg_len = next - file;
      if (0 == seg_len || (1 == seg_len && '.' == file[0])) {
        //nop when (/ or ./)
      } else if (2 == seg_len && '.' == file[0] && '.' == file[1]) {
        //  handle ../
        if (1 == path_len && '/' == path[0]) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(root), K(file), K(ret));
        } else if (path_len >= 3 && 0 == MEMCMP(path + path_len - 3, "../", 3)) {
          if ('\0' != *next) {
            ++seg_len;
          }
          MEMCPY(path + path_len, "../", seg_len);
          path_len += seg_len;
        } else {
          do {
            --path_len;
          } while (path_len && ('/' != path[path_len - 1]));
        }
      } else {
        //an actually segment .append it to path
        if ('\0' != *next) {
          ++seg_len;
        }
        MEMCPY(path + path_len, file, seg_len);
        path_len += seg_len;
      }

      if ('\0' != *next) {
        //skip over trailing slash to the next segment
        ++next;
      }
      file = next;
    }

    if (OB_SUCC(ret)) {
      path[path_len] = '\0';
      buf = path;
    }
  }
  return ret;
}

int ObLayout::init_bin_dir(const char *cwd, const char *start_cmd)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cwd) || OB_ISNULL(start_cmd)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid cwd or start cmd, cwd=%s, start_cmd=%s, ret=%d", cwd, start_cmd, ret);
  } else if (OB_FAIL(merge_file_path(cwd, start_cmd, allocator_, bin_dir_))) {
    MPRINT("fail to merge bin dir, ret=%d", ret);
  } else {
    int64_t bin_len = STRLEN(bin_dir_);
    while (bin_len > 1 && '/' != bin_dir_[bin_len - 1]) {
      bin_dir_[bin_len - 1] = '\0';
      --bin_len;
    }
  }

  return ret;
}

int ObLayout::init_dir_prefix(const char *cwd)
{
  int ret = OB_SUCCESS;
  char *env_path = NULL;
  int64_t prefix_len = 0;

  if (OB_ISNULL(cwd)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid cwd, cwd=%s, ret=%d", cwd, ret);
  } else if (OB_ISNULL(env_path = getenv("OBPROXY_ROOT"))) {
    MPRINT("OBPROXY_ROOT is not set, set current working dir");
    prefix_len = STRLEN(cwd);
  } else {
    int64_t env_path_len = STRLEN(env_path);
    if (OB_UNLIKELY(env_path_len >= MAX_PATH_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      MPRINT("root path buf is not enough, env_path=%s, env_path_len=%ld, ret=%d", env_path, env_path_len, ret);
    } else if (OB_UNLIKELY('/' != env_path[0])) {
      ret = OB_INVALID_ARGUMENT;
      MPRINT("invalid env path for proxy, env_path=%s, ret=%d", env_path, ret);
    } else {
      //trim the ending slashes
      while (env_path_len > 1 && '/' == env_path[env_path_len - 1]) {
        --env_path_len;
      }
      prefix_len = env_path_len;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(prefix_ = static_cast<char *>(allocator_.alloc(prefix_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MPRINT("fail to alloc memeory, prefix_len=%ld, ret=%d", prefix_len, ret);
    } else {
      MEMCPY(prefix_, (NULL != env_path) ? env_path : cwd, prefix_len);
      prefix_[prefix_len] = '\0';
    }
  }

  return ret;
}

int ObLayout::construct_dirs()
{
  int ret = OB_SUCCESS;
  static const char *ETC_PATH= "etc";
  static const char *LOG_PATH = "log";
  static const char *CONF_PATH = ".conf";
  static const char *CONTROL_CONFIG_PATH = "control-config";
  static const char *DBCONFIG_PATH = "sharding-config";
  if (OB_FAIL(construct_single_dir(ETC_PATH, etc_dir_))) {
    MPRINT("fail to construct etc dir, ret=%d", ret);
  } else if (OB_FAIL(construct_single_dir(LOG_PATH, log_dir_))) {
    MPRINT("fail to construct log dir, ret=%d", ret);
  } else if (OB_FAIL(construct_single_dir(CONF_PATH, conf_dir_))) {
    MPRINT("fail to construct .conf dir, ret=%d", ret);
  } else if (OB_FAIL(construct_single_dir(CONTROL_CONFIG_PATH, control_config_dir_))) {
    MPRINT("fail to construct .conf dir, ret=%d", ret);
  } else if (OB_FAIL(construct_single_dir(DBCONFIG_PATH, dbconfig_dir_))) {
  } else { }

  return ret;
}

int ObLayout::extract_actual_path(const char * const full_path, char *&actual_path)
{
  int ret = OB_SUCCESS;
  struct stat64 file_info;
  int64_t len = 0;
  if (OB_UNLIKELY(len = strlen(full_path)) == 0) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid arguments, full_path=%s, ret=%d", full_path, ret);
  } else {
    //if this directory was exist, and it is a symbolic link, need extract its actual path
    if (0 == ::lstat64(full_path, &file_info)
        && S_ISLNK(file_info.st_mode)) {
      MPRINT("this is symbolic link, need extract actual path, full_path=%s", full_path);
      ssize_t buf_size = static_cast<ssize_t>(file_info.st_size + 1);
      if (OB_ISNULL(actual_path = static_cast<char *>(allocator_.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        MPRINT("fail to alloc memeory, buf_size=%ld, ret=%d", buf_size, ret);
      } else {
        ssize_t path_size = ::readlink(full_path, actual_path, buf_size);
        if (path_size != (buf_size - 1)) {
          ret = ob_get_sys_errno();
          MPRINT("fail to readlink file, full_path=%s, path_size=%ld, buf_size=%ld, "
                 "errno=%d, errmsg=%s, ret=%d",
                 full_path, path_size, buf_size, errno, strerror(errno), ret);
        } else {
          actual_path[path_size] = '\0';
          MPRINT("succ to extract actual path, full_path=%s, actual_path=%s", full_path, actual_path);
        }
      }
    } else {
      //if do not use symbolic link, actual_path equal to full_path
      actual_path = const_cast<char *>(full_path);
    }
  }
  return ret;
}

int ObLayout::construct_single_dir(const char *sub_dir, char *&full_path)
{
  int ret = OB_SUCCESS;
  full_path = NULL;
  char *actual_path = NULL;
  if (OB_FAIL(merge_file_path(prefix_, sub_dir, allocator_, full_path))) {
    MPRINT("fail to merge_file_path, sub_dir=%s, ret=%d", sub_dir, ret);
  } else if (OB_FAIL(extract_actual_path(full_path, actual_path))) {
    MPRINT("fail to extract_actual_path, full_path=%s, ret=%d", full_path, ret);
  } else if (OB_FAIL(FileDirectoryUtils::create_full_path(actual_path))) {
    MPRINT("fail to create_full_path, actual_path=%s, ret=%d", actual_path, ret);
  } else {
    //do nothing
  }

  if (NULL != actual_path && full_path != actual_path) {
    allocator_.free(actual_path);
  }
  actual_path = NULL;
  return ret;
}

}//end of namespace obproxy
}//end of namespace oceanbase

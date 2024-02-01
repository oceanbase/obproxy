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

#include "lib/file/file_directory_utils.h"

#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"

#include "lib/tbsys.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>


namespace oceanbase
{
namespace common
{

//return true if filename is exists
int FileDirectoryUtils::is_exists(const char *file_path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  struct stat64 file_info;
  if (NULL == file_path || strlen(file_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(file_path), K(ret));
  } else {
    result = (0 == stat64(file_path, &file_info));
  }

  return ret;
}

//return ture if dirname is a directory
int FileDirectoryUtils::is_directory(const char *directory_path, bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  struct stat64 file_info;
  if (NULL == directory_path ||  strlen(directory_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(directory_path), K(ret));
  } else {
    result = (0 == stat64(directory_path, &file_info) && S_ISDIR(file_info.st_mode));
  }

  return ret;
}

int FileDirectoryUtils::is_link(const char *link_path, bool &result)
{
  int ret = OB_SUCCESS;
  if (NULL == link_path || strlen(link_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(link_path), K(ret));
  }  else {
    struct stat64 file_info;
    result = (0 == lstat64(link_path, &file_info) && S_ISLNK(file_info.st_mode));
  }
  return ret;
}

//create the give dirname, return true on success or dirname exists
int FileDirectoryUtils::create_directory(const char *directory_path)
{
  int ret = OB_SUCCESS;
  mode_t umake_value = umask(0);
  umask(umake_value);
  mode_t mode = (S_IRWXUGO & (~umake_value)) | S_IWUSR | S_IXUSR;

  if (NULL == directory_path || strlen(directory_path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(directory_path), K(ret));
  } else if (::mkdir(directory_path, mode)) {
    if (EEXIST == errno) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_IO_ERROR;
      LIB_LOG(WDIAG, "create directory failed.", K(directory_path), KERRMSGS, K(ret));
    }
  }

  return ret;
}

//creates the full path of fullpath, return true on success
int FileDirectoryUtils::create_full_path(const char *fullpath)
{

  int ret = OB_SUCCESS;
  struct stat64 file_info;
  int64_t len = 0;
  if (NULL == fullpath || (len = strlen(fullpath)) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(fullpath), K(ret));
  } else {
    ret = ::stat64(fullpath, &file_info);
    if (0 == ret) {
      if (!S_ISDIR(file_info.st_mode)) {
        ret = OB_ENTRY_EXIST;
        LIB_LOG(WDIAG, "file is exists but not a directory.", K(fullpath), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      ret = OB_SUCCESS;
      // path not exists.
      char dirpath[MAX_PATH + 1];
      strncpy(dirpath, fullpath, len);
      dirpath[len] = '\0';
      char *path = dirpath;

      // skip leading char '/'
      while (*path++ == '/');

      while (OB_SUCC(ret)) {
        path = strchr(path, '/');
        if (NULL == path) {
          break;
        }

        *path = '\0';
        if (OB_FAIL(create_directory(dirpath))) {
          LIB_LOG(WDIAG, "create directory failed.", K(dirpath), K(ret));
        } else {
          *path++ = '/';
          // skip '/'
          while (*path++ == '/')
            ;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(create_directory(dirpath))) {
          LIB_LOG(WDIAG, "create directory failed.", K(dirpath), K(ret));
        }
      }
    }
  }

  return ret;
}

//delete the given file, return true if filename exists
// return OB_SUCCESS on success;
int FileDirectoryUtils::delete_file(const char *filename)
{
  int ret = OB_SUCCESS;
  struct stat64 file_info;
  if (NULL == filename || strlen(filename) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(filename), K(ret));
  } else {
    ret = ::stat64(filename, &file_info);
    if (0 != ret) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WDIAG, "file is not exists.", K(filename), K(ret));
    } else if (S_ISDIR(file_info.st_mode)) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WDIAG, "file is directory, use delete_directory.",
              K(filename), K(ret));
    } else if (0 != unlink(filename)){
      ret = OB_IO_ERROR;
      LIB_LOG(WDIAG, "unlink file failed.", K(filename), KERRMSGS, K(ret));
    }
  }
  return ret;
}

//delete the given directory and anything under it. Returns true on success
int FileDirectoryUtils::delete_directory(const char *dirname)
{
  int ret = OB_SUCCESS;
  bool is_dir = false;
  if (NULL == dirname || strlen(dirname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(dirname), K(ret));
  } else if (OB_FAIL(is_directory(dirname, is_dir))) {
    LIB_LOG(WDIAG, "check if directory failed.", K(dirname), K(ret));
  } else if (!is_dir) {
    ret = OB_FILE_NOT_EXIST;
    LIB_LOG(WDIAG, "file path is not a directory.", K(dirname), K(ret));
  } else if (0 != rmdir(dirname)) {
    ret = OB_IO_ERROR;
    LIB_LOG(WDIAG, "rmdir failed.", K(dirname), KERRMSGS, K(ret));
  }
  return ret;
}

//return the size of filename
int FileDirectoryUtils::get_file_size(const char *filename, int64_t &size)
{
  int ret = OB_SUCCESS;
  struct stat64 file_info;
  if (NULL == filename || strlen(filename) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments.", K(filename), K(ret));
  } else {
    ret = ::stat64(filename, &file_info);
    if (0 != ret) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WDIAG, "file is not exists.", K(filename), K(ret));
    } else if (S_ISDIR(file_info.st_mode)) {
      ret = OB_FILE_NOT_EXIST;
      LIB_LOG(WDIAG, "file is not a file.", K(filename), K(ret));
    } else {
      size = file_info.st_size;
    }
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase

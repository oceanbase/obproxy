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

#include "proxy/api/ob_plugin.h"
#include <stdio.h>
#include "proxy/api/ob_api.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;

static const char *plugin_dir = ".";

typedef void (*init_func_t)(int argc, char *argv[]);

// g_plugin_reg_list has an entry for each plugin
// we've successfully been able to load.
//
// g_plugin_reg_current is used to associate the
// plugin we're in the process of loading with
// it struct. We need this global pointer since
// the API doesn't have any plugin context.  Init
// is single threaded so we can get away with the
// global pointer
DLL<ObPluginRegInfo> g_plugin_reg_list;
ObPluginRegInfo *g_plugin_reg_current = NULL;

ObPluginRegInfo::ObPluginRegInfo()
    : plugin_registered_(false), plugin_path_(NULL),
      plugin_name_(NULL), vendor_name_(NULL), support_email_(NULL)
{ }

/**
 * int file_fd_readline(int fd, int bufsz, char *buf)
 *
 * This routine reads bytes from <fd> into the buffer pointed to by <buf>.
 * The reading stops when (a) a LF is read into the buffer, (b) the end of
 * file is reached, or (c) <bufsz> - 1 bytes are read. The <bufsz> parameter
 * must be >= 2.
 *
 * The data pointed to by <buf> is always NULL terminated, and the LF is
 * left in the data. This routine can be used as a replacement for
 * fgets-like functions. If the <bufsz> is too small to hold a complete line,
 * the partial line will be stored, and subsequent reads will read more data.
 *
 * This routine returns the number of bytes read, 0 on end of file, or
 * a negative errno on error.
 */
//static int file_fd_readline(int fd, int bufsz, char *buf)
//{
//  int ret = 0;
//  char c = 0;
//  int i = 0;
//
//  if (bufsz < 2) {
//    ret = (-EINVAL);           /* bufsz must by >= 2 */
//  } else {
//    while (i < bufsz - 1) {       /* leave 1 byte for NUL */
//      int n = (int)read(fd, &c, 1);    /* read 1 byte */
//      if (0 == n) {
//        break;                    /* EOF */
//      } else if (n < 0) {
//        ret = n;               /* error */
//        break;
//      } else {
//        buf[i++] = c;               /* store in buffer */
//        if ('\n' == c) {
//          break;                    /* stop if stored a LF */
//        }
//      }
//    }
//
//    if (ret >= 0) {
//      buf[i] = '\0';                /* NULL terminate buffer */
//      ret = i;
//    }
//  }
//  return ret;                   /* number of bytes read */
//}                               /* End ink_file_fd_readline */

//static void plugin_load(int argc, char *argv[])
//{
//  char path[MAX_PATH_SIZE];
//  void *handle = NULL;
//  init_func_t init;
//  ObPluginRegInfo *plugin_reg_temp = NULL;
//
//  if (argc < 1) {
//    WARN_API("argument isn't enough, argc=%d", argc);
//  } else {
//    snprintf(path, sizeof(path), "%s/%s", plugin_dir, argv[0]);
//    INFO_API("loading plugin '%s'", path);
//
//    plugin_reg_temp = g_plugin_reg_list.head_;
//    while (NULL != plugin_reg_temp) {
//      if (0 == STRCMP(plugin_reg_temp->plugin_path_, path)) {
//        WARN_API("multiple loading of plugin %s", path);
//        break;
//      }
//      plugin_reg_temp = (plugin_reg_temp->link_).next_;
//    }
//
//    handle = dlopen(path, RTLD_NOW);
//    if (NULL == handle) {
//      ERROR_API("unable to load '%s': %s", path, dlerror());
//      exit(1);
//    }
//
//    // Allocate a new registration structure for the
//    // plugin we're starting up
//    ob_assert(NULL == g_plugin_reg_current);
//    g_plugin_reg_current = new(std::nothrow) ObPluginRegInfo();
//    if (NULL == g_plugin_reg_current) {
//      ERROR_API("failed new ObPluginRegInfo");
//    } else {
//      g_plugin_reg_current->plugin_path_ = strndup(path, MAX_PATH_SIZE);
//
//      init = (init_func_t)dlsym(handle, "plugin_init");
//      if (NULL == init) {
//        ERROR_API("unable to find TSPluginInit function in '%s': %s", path, dlerror());
//      } else {
//        init(argc, argv);
//
//        g_plugin_reg_list.push(g_plugin_reg_current);
//        g_plugin_reg_current = NULL;
//      }
//    }
//  }
//}

int plugin_init()
{
  int ret = OB_SUCCESS;
//  char path[MAX_PATH_SIZE];
//  char line[1024];
//  char *p = NULL;
//  char *argv[64] = {NULL};
//  int argc = 0;
//  int fd = -1;
  static bool INIT_ONCE = true;

  if (INIT_ONCE) {
    ret = api_init();
    plugin_dir = "."; // FIXME: for test
    INIT_ONCE = false;
  }

// we will use it later
//
//  if (OB_SUCC(ret)) {
//    snprintf(path, sizeof(path), "%s/%s", plugin_dir, "plugin.config");
//    fd = open(path, O_RDONLY);
//    if (fd < 0) {
//      WARN_API("unable to open plugin config file '%s': %d, %s",
//               path, errno, ERRMSG);
//      ret = OB_ERROR;
//    }
//  }
//
//  if (OB_SUCCESS == ret && fd >= 0) {
//    while (file_fd_readline(fd, sizeof(line) - 1, line) > 0) {
//      argc = 0;
//      p = line;
//
//      // strip leading white space and test for comment or blank line
//      while (*p && (' ' == *p || '\r' == *p || '\n' == *p)) {
//        ++p;
//      }
//
//      if (('\0' == *p) || ('#' == *p)) {
//        continue;
//      }
//
//      // not comment or blank, so rip line into tokens
//      while (true) {
//        while (*p && (' ' == *p || '\r' == *p || '\n' == *p)) {
//          ++p;
//        }
//
//        if (('\0' == *p) || ('#' == *p)) {
//          break;                  // EOL
//        }
//
//        if ('\"' == *p) {
//          p += 1;
//
//          argv[argc++] = p;
//
//          while (*p && ('\"' != *p)) {
//            p += 1;
//          }
//
//          if ('\0' == *p) {
//            break;
//          }
//
//          *p++ = '\0';
//        } else {
//          argv[argc++] = p;
//
//          while (*p && !(' ' == *p || '\r' == *p || '\n' == *p) && ('#' != *p)) {
//            p += 1;
//          }
//
//          if (('\0' == *p) || ('#' == *p)) {
//            break;
//          }
//
//          *p++ = '\0';
//        }
//      }
//
//      plugin_load(argc, argv);
//    }
//
//    if (fd >= 0 && 0 != close(fd)) {
//      ret = OB_IO_ERROR;
//      WARN_API("fail to close file fd:%d, errmsg:%s, ret:%d", fd, ERRMSG, ret);
//    }
//  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

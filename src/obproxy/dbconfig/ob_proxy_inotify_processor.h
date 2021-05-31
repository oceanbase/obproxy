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

#ifndef OBPROXY_INOTIFY_PROCESSOR_H
#define OBPROXY_INOTIFY_PROCESSOR_H

#include <sys/epoll.h>
#include <sys/inotify.h>
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/file/file_directory_utils.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

#define MAX_INOTIFY_FILE_COUNT 1024
#define INOTIFY_BUFLEN (MAX_INOTIFY_FILE_COUNT * (sizeof(struct inotify_event) + common::OB_MAX_TENANT_NAME_LENGTH))

#define OBPROXY_INVALID_FD -1
#define OBPROXY_DEFAULT_EPOLL_SIZE 1

enum ObDbConfigDirType
{
  DIR_INVALID = 0,
  DIR_ROOT,
  DIR_TENANT
};

class ObWatchFile
{
public:
  ObWatchFile() : fd_(OBPROXY_INVALID_FD), wd_(OBPROXY_INVALID_FD), type_(DIR_INVALID), file_name_(), full_path_()
  {
    full_path_buf_[0] = '\0';
  }
  ~ObWatchFile() {}

  void destroy()
  {
    _PROXY_LOG(INFO, "ObWatchFile will be destroyed, full_path=%s, this=%p", full_path_buf_, this);
    if (OBPROXY_INVALID_FD != fd_) {
      ::close(fd_);
    }
    fd_ = OBPROXY_INVALID_FD;
    wd_ = OBPROXY_INVALID_FD;
    type_ = DIR_INVALID;
    full_path_buf_[0] = '\0';
    file_name_.reset();
    full_path_.reset();
    op_free(this);
  }
  TO_STRING_KV(K_(fd), K_(wd), K_(type), K_(full_path));

public:
  int fd_; // inotify file descriptor
  int wd_; // inotify watch descriptor
  ObDbConfigDirType type_;
  common::ObString file_name_;
  common::ObString full_path_;
  char full_path_buf_[common::FileDirectoryUtils::MAX_PATH + 1];
  LINK(ObWatchFile, watch_file_link_);
};

class ObInotifyProcessor
{
public:
  ObInotifyProcessor() : is_inited_(false), epoll_fd_(OBPROXY_INVALID_FD)
  {
    memset(epoll_events_, 0, sizeof(epoll_events_));
    memset(inotify_buf_, 0, sizeof(inotify_buf_));
  }
  ~ObInotifyProcessor() {}

  int init();
  int start_watch_sharding_config();
  int do_inotify_watch();

  int add_watch(ObDbConfigDirType type, const common::ObString &file_name);
  int remove_watch(const common::ObString &full_path);

public:
  static const int64_t FD_HASH_BUCKET_SIZE = 128;
  // wd ---> dir
  struct ObFDHashing
  {
    typedef const common::ObString &Key;
    typedef ObWatchFile Value;
    typedef ObDLList(ObWatchFile, watch_file_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->full_path_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<ObFDHashing, FD_HASH_BUCKET_SIZE> FDHashMap;

private:
  int handle_epoll_event(int64_t count);
  int handle_inotify_event(ObWatchFile &wf, struct inotify_event &event);


private:
  static const int64_t EPOLL_MAX_EVENTS = 1024;
  
  bool is_inited_;
  int epoll_fd_;
  struct epoll_event epoll_events_[EPOLL_MAX_EVENTS];
  char inotify_buf_[INOTIFY_BUFLEN];
  FDHashMap fd_map_;
};


ObInotifyProcessor &get_global_inotify_processor();

} // end dbconfig
} // end obproxy
} // end oceanbase
#endif /* OBPROXY_INOTIFY_PROCESSOR_H */

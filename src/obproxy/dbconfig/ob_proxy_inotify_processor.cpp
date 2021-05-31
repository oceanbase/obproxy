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
#include "dbconfig/ob_proxy_inotify_processor.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "utils/ob_proxy_lib.h"
#include "utils/ob_layout.h"
#include "obutils/ob_async_common_task.h"
#include "iocore/eventsystem/ob_shard_watch_task.h"
#include "iocore/net/ob_socket_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

static const ObString TENANT_FILE_NAME = ObString::make_string("Tenant.json");
static const uint32_t ROOT_MASK = IN_DELETE | IN_CREATE ;
static const uint32_t TENANT_MASK = IN_CREATE | IN_MODIFY;

ObInotifyProcessor &get_global_inotify_processor()
{
  static ObInotifyProcessor g_inotify_processor;
  return g_inotify_processor;
}

class ObInotifyWatchTask : public ObAsyncCommonTask
{
public:
  explicit ObInotifyWatchTask(event::ObProxyMutex *m) : ObAsyncCommonTask(m, "ObInotifyWatchTask") {}
  virtual ~ObInotifyWatchTask() {}

  static int alloc_inotify_watch_task(ObInotifyWatchTask *&cont);

  virtual int init_task();
  virtual int finish_task(void *data)
  {
    UNUSED(data);
    return OB_SUCCESS;
  }
};

int ObInotifyWatchTask::alloc_inotify_watch_task(ObInotifyWatchTask *&cont)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  cont = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for mutex", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObInotifyWatchTask(mutex))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for ObInotifyWatchTask", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  }
  return ret;
}

int ObInotifyWatchTask::init_task()
{
  int ret = OB_SUCCESS;
  ObInotifyProcessor &inotify_processor = get_global_inotify_processor();
  while (true) {
    if (OB_FAIL(inotify_processor.do_inotify_watch())) {
      LOG_WARN("fail to do inotify watch", K(ret));
    }
  }
  terminate_ = true;
  return ret;
}

//------------ObInotifyProcessor---------
int ObInotifyProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObSocketManager::epoll_create(OBPROXY_DEFAULT_EPOLL_SIZE, epoll_fd_))) {
    LOG_WARN("fail to do epoll_create", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init inotify processor", K(epoll_fd_));
  }
  return ret;
}

int ObInotifyProcessor::start_watch_sharding_config()
{
  int ret = OB_SUCCESS;
  ObInotifyWatchTask *cont = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  ObSEArray<ObString, 1> tenant_array;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInotifyProcessor has not been inited", K(ret));
  } else if (OB_FAIL(add_watch(DIR_ROOT, ObString::make_empty_string()))) {
    LOG_WARN("fail to add watch sharding config root dir", K(ret));
  } else if (OB_FAIL(dbconfig_cache.get_all_logic_tenant(tenant_array))) {
    LOG_WARN("fail to get all logic tenant", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_array.count(); ++i) {
    const ObString &tenant_name = tenant_array.at(i);
    if (OB_FAIL(add_watch(DIR_TENANT, tenant_name))) {
      LOG_WARN("fail to add watch tenant config", K(tenant_name), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObInotifyWatchTask::alloc_inotify_watch_task(cont))) {
      LOG_WARN("fail to alloc fetch task for inotify watch", K(ret));
    } else if (OB_ISNULL(cont)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cont is null", K(ret));
    } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_SHARD_WATCH))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule inotify watch task", K(ret));
    } else {
      LOG_INFO("succ to schedule inotify watch task");
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
    }
  }
  return ret;
}

int ObInotifyProcessor::add_watch(ObDbConfigDirType type, const ObString &file_name)
{
  int ret = OB_SUCCESS;
  ObWatchFile *wf = NULL;
  if (OB_UNLIKELY(!is_inited_) || OB_UNLIKELY(DIR_INVALID == type)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInotifyProcessor has not been inited", K(type), K(ret));
  } else if (OB_ISNULL(wf = op_alloc(ObWatchFile))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObWatchFile", K(type), K(file_name), K(ret));
  } else {
    wf->fd_ = inotify_init();
    wf->type_ = type;
    int64_t dbconfig_dir_length = strlen(get_global_layout().get_dbconfig_dir());
    if (!file_name.empty()) {
      snprintf(wf->full_path_buf_, FileDirectoryUtils::MAX_PATH + 1, "%s/%.*s",
               get_global_layout().get_dbconfig_dir(),
               static_cast<int32_t>(file_name.length()), file_name.ptr());
      wf->file_name_.assign_ptr(wf->full_path_buf_ + dbconfig_dir_length + 1, file_name.length());
    } else {
      snprintf(wf->full_path_buf_, FileDirectoryUtils::MAX_PATH + 1, "%s",
               get_global_layout().get_dbconfig_dir());
    }
    wf->full_path_.assign_ptr(wf->full_path_buf_, static_cast<int32_t>(strlen(wf->full_path_buf_)));
    uint32_t mask = DIR_ROOT == type ? ROOT_MASK : TENANT_MASK;
    if (OB_UNLIKELY((wf->wd_ = inotify_add_watch(wf->fd_, wf->full_path_buf_, mask) < 0))) {
      ret = ob_get_sys_errno();
      LOG_WARN("fail to add file to inotify watch", KPC(wf), K(file_name), K(ret));
    } else {
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.events = EPOLLIN;
      ev.data.ptr = reinterpret_cast<void *>(wf);
      if (OB_FAIL((ObSocketManager::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wf->fd_, &ev)))) {
        LOG_WARN("fail to add epoll event", KPC(wf), K(epoll_fd_), K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(fd_map_.unique_set(wf))) {
      LOG_WARN("fail to add watch file into fd map", KPC(wf), K(ret));
      // remember to rm epoll and watch
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY((ObSocketManager::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, wf->fd_, NULL)) < 0)) {
        tmp_ret = ob_get_sys_errno();
        LOG_WARN("fail to delete epoll event", KPC(wf), K(tmp_ret));
      } else if (OB_UNLIKELY(inotify_rm_watch(wf->fd_, wf->wd_) < 0)) {
        tmp_ret = ob_get_sys_errno();
        LOG_WARN("fail to rm watched file", KPC(wf), K(tmp_ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ to add watch file", KPC(wf));
    }
  }
  if (OB_FAIL(ret) && NULL != wf) {
    wf->destroy();
    wf = NULL;
  }
  return ret;
}

int ObInotifyProcessor::remove_watch(const ObString &full_path)
{
  int ret = OB_SUCCESS;
  ObWatchFile *wf = NULL;
  if (OB_FAIL(fd_map_.get_refactored(full_path, wf))) {
    LOG_WARN("fail to get watch file", K(full_path), K(ret));
  } else if (OB_ISNULL(wf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("watch file is null", K(full_path), K(ret));
  } else if (OB_FAIL((ObSocketManager::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, wf->fd_, NULL)))) {
    LOG_WARN("fail to add epoll event", KPC(wf), K(epoll_fd_), K(ret));
  } else {
    inotify_rm_watch(wf->fd_, wf->wd_);
    fd_map_.remove(full_path);
    wf->destroy();
    wf = NULL;
  }
  return ret;
}

int ObInotifyProcessor::do_inotify_watch()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObInotifyProcessor has not beed inited", K(ret));
  } else {
    int64_t count = 0;
    if (OB_FAIL(ObSocketManager::epoll_wait(epoll_fd_, epoll_events_, EPOLL_MAX_EVENTS, -1, count))) {
      LOG_WARN("fail to do epoll wait", K(epoll_fd_), K(ret));
    } else if (OB_FAIL(handle_epoll_event(count))) {
      LOG_WARN("fail to handle epoll events", K(ret));
    }
  }
  return ret;
}

int ObInotifyProcessor::handle_epoll_event(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count > EPOLL_MAX_EVENTS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(count), K(ret));
  } else {
    for (int64_t i = 0; i < count; ++i) {
      ObWatchFile *wf = reinterpret_cast<ObWatchFile *>(epoll_events_[i].data.ptr);
      if (OB_ISNULL(wf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("watch file is null", K(ret));
      } else if (OB_UNLIKELY(wf->fd_ < 0) || OB_UNLIKELY(wf->wd_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("watch file is invalid", KPC(wf), K(ret));
      } else {
        int64_t read_size = 0;
        memset(inotify_buf_, 0, INOTIFY_BUFLEN);
        do {
          ret = ObSocketManager::read(wf->fd_, reinterpret_cast<void *>(inotify_buf_), INOTIFY_BUFLEN, read_size);
        } while (OB_SYS_EINTR == read_size);
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_UNLIKELY(read_size < 0) || OB_UNLIKELY(read_size > INOTIFY_BUFLEN)) {
          ret = OB_IO_ERROR;
          LOG_WARN("invalid file count", K(read_size), K(ret));
        } else {
          for (int64_t i = 0; i < read_size; ) {
            struct inotify_event *tmp_ev = reinterpret_cast<struct inotify_event *>(inotify_buf_ + i);
            if (OB_FAIL(handle_inotify_event(*wf, *tmp_ev))) {
              LOG_WARN("fail to handle inotify event", KPC(wf), K(ret));
            }
            i += (sizeof(struct inotify_event) + tmp_ev->len); // continue to handle other inotify event
          } // end for inotify events
        }
      }
    } // end for epoll events
  }
  return ret;
}

int ObInotifyProcessor::handle_inotify_event(ObWatchFile &wf, struct inotify_event &event)
{
  int ret = OB_SUCCESS;
  switch(wf.type_) {
    case DIR_ROOT: {
      if (event.mask & IN_CREATE && event.mask & IN_ISDIR) {
        LOG_INFO("will add tenant dir", "dir", event.name);
        if (OB_FAIL(add_watch(DIR_TENANT, event.name))) {
          LOG_WARN("fail to add watch logic tenant dir", "tenant", event.name, K(ret));
        } else {
          // if monitor new dir create, need try to load the logic tenant
          ObString tenant_name(static_cast<int64_t>(event.len), event.name);
          LOG_INFO("will load tenant config", K(tenant_name), K(ret));
          if (OB_FAIL(get_global_dbconfig_cache().load_logic_tenant_config(tenant_name))) {
            LOG_WARN("fail to load tenant config", K(tenant_name), K(ret));
            ret = OB_SUCCESS; // ignore ret
          }
        }
      } else if (event.mask & IN_DELETE && event.mask & IN_ISDIR) {
        LOG_INFO("will remove watch tenant dir", "dir", event.name);
        // TODO delete logic tenant
        char tmp_buf[FileDirectoryUtils::MAX_PATH + 1];
        snprintf(tmp_buf, FileDirectoryUtils::MAX_PATH + 1, "%s/%s",
                 wf.full_path_buf_, event.name);
        if (OB_FAIL(remove_watch(ObString::make_string(tmp_buf)))) {
          LOG_WARN("fail to remove watch logic tenant dir", "tenant", event.name, K(ret));
        }
      }
      break;
    }
    case DIR_TENANT: {
      if ((event.mask & IN_CREATE || event.mask & IN_MODIFY || event.mask & IN_MOVE)
          && 0 == (event.mask & IN_ISDIR)
          && event.len > 0
          && TENANT_FILE_NAME.case_compare(event.name) == 0) {
        // parse new logic tenant config
        ObString tenant_name = wf.file_name_;
        LOG_INFO("will load tenant config", K(tenant_name), K(ret));
        if (OB_FAIL(get_global_dbconfig_cache().load_logic_tenant_config(tenant_name))) {
          LOG_WARN("fail to load tenant config", K(tenant_name), K(ret));
        }
      }
      break;
    }
    default: {
      LOG_WARN("unknown watch dir type", K(wf));
      break;
    }
  }
  return ret;
}


} // end dbconfig
} // end obproxy
} // end oceanbase

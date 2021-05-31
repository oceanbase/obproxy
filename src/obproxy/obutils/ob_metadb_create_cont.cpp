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
#include "lib/profile/ob_trace_id.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "obutils/ob_metadb_create_cont.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "stat/ob_stat_processor.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
int ObMetadbCreateCont::main_handler(int event, void *data)
{
  int ret_event = EVENT_CONT;
  int ret = OB_SUCCESS;
  ++reentrancy_count_;
  LOG_DEBUG("ObMetadbCreateCont::main_handler receive event",
            K(event), K(data), K_(reentrancy_count));
  switch (event) {
    case METADB_CREATE_START_EVENT: {
      pending_action_ = NULL;
      if (OB_FAIL(handle_create_meta_db())) {
        LOG_WARN("fail to handle create meta db", K(ret));
      }
      break;
    }
    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT: {
      pending_action_ = NULL;
      if (OB_FAIL(handle_create_complete(data))) {
        LOG_WARN("fail to handle creat complete", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown event", K(event), K(data), K(ret));
      break;
    }
  };

  if (terminate_ && (1 == reentrancy_count_)) {
    ret_event = EVENT_DONE;
    destroy();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count));
    }
  }

  return ret_event;
}

int ObMetadbCreateCont::handle_create_meta_db()
{
  int ret = OB_SUCCESS;
  ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();
  ObString tenant_name = ObString::make_string(OB_META_DB_CLUSTER_NAME);

  // if metadb has created, just return succ;
  //    such as, use metadb login(mysql -hxxxx -Pxxx -uroot@MetaDataBase), newest metadb has created
  //
  // if metadb has not created, just do create;
  if (OB_FAIL(rp_processor.get_cluster_resource(*this,
          (process_async_task_pfn)&ObMetadbCreateCont::handle_create_complete,
          false, tenant_name, OB_DEFAULT_CLUSTER_ID, pending_action_))) {
    LOG_WARN("fail to get cluster resource", "cluster name", OB_META_DB_CLUSTER_NAME, K(ret));
  } else if (NULL == pending_action_) { // created succ
    LOG_INFO("metadb resource was created by others, no need create again");
  }
  return ret;
}

int ObMetadbCreateCont::handle_create_complete(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    // create fail
    if (retry_count_ > 0) {
      --retry_count_;
      LOG_INFO("fail to create metadb, will retry", "remain retry count", retry_count_, K(data));
      if (OB_ISNULL(pending_action_ = g_event_processor.schedule_in(
              this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), ET_CALL, METADB_CREATE_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule fetch rslist task", K(ret));
      }
    } else {
      LOG_WARN("fail to crate metadb, no chance retry", K(data));
      terminate_ = true;
    }
  } else {
    ObClusterResource *cr = reinterpret_cast<ObClusterResource *>(data);
    LOG_INFO("metadb cluster create succ", K(cr), KPC(cr), K_(meta_proxy));
    if (NULL != meta_proxy_) {
      ObProxyLoginInfo login_info;
      ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
      char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
      ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
      if (OB_FAIL(cs_processor.get_proxy_meta_table_login_info(login_info))) {
        LOG_WARN("fail to get meta table login info", K(ret));
      } else if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(login_info.password_, passwd_string))) {
        LOG_WARN("fail to encrypt_passwd_to_stage1", K(login_info), K(ret));
      } else {
        passwd_string += 1;//trim the head'*'
        const bool is_meta_mysql_client = true;
        if (OB_FAIL(meta_proxy_->rebuild_client_pool(cr, is_meta_mysql_client,
                   OB_META_DB_CLUSTER_NAME, OB_DEFAULT_CLUSTER_ID, login_info.username_, passwd_string, login_info.db_))) {
          LOG_WARN("fail to create meta mysql client pool", K(login_info), K(ret));
        }
      }
    }

    cr->dec_ref(); // free
    cr = NULL;
    terminate_ = true;
  }

  return ret;
}


int ObMetadbCreateCont::create_metadb(ObMysqlProxy *meta_proxy)
{
  int ret = OB_SUCCESS;
  ObMetadbCreateCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for proxy mutex error", K(ret));
  } else if (OB_ISNULL(cont = new (std::nothrow) ObMetadbCreateCont(mutex, meta_proxy))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObMetadbCreateCont", K(ret));
  } else {
    // create cluster resource must be done in work thread
    if (!self_ethread().is_event_thread_type(ET_CALL)) {
      if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_CALL, METADB_CREATE_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule create metadb cluster resource event", K(ret));
      }
    } else {
      MUTEX_TRY_LOCK(lock, cont->mutex_, this_ethread());
      if (lock.is_locked()) { // must be locked
        ObCurTraceId::set((uint64_t)(cont->mutex_.ptr_));
        cont->handle_event(METADB_CREATE_START_EVENT, NULL);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the lock must be locked", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
      mutex = NULL;
    } else {
      if (NULL != mutex) {
        mutex->free();
        mutex = NULL;
      }
    }
  }

  return ret;
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

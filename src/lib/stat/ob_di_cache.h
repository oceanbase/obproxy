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

#ifndef OB_DI_CACHE_H_
#define OB_DI_CACHE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_di_list.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
class DIRWLock
{
public:
  DIRWLock();
  ~DIRWLock();
  int try_rdlock();
  int try_wrlock();
  void rdlock();
  void wrlock();
  void wr2rdlock();
  void unlock();
private:
  static const uint32_t WRITE_MASK = 1<<30;
  volatile uint32_t lock_;
};

class ObDISessionCollect : public ObDINode<ObDISessionCollect>
{
public:
  ObDISessionCollect();
  virtual ~ObDISessionCollect();
  void clean();
  TO_STRING_EMPTY();
  uint64_t session_id_;
  ObDiagnoseSessionInfo base_value_;
  DIRWLock lock_;
};

class ObDITenantCollect : public ObDINode<ObDITenantCollect>
{
public:
  ObDITenantCollect();
  virtual ~ObDITenantCollect();
  void clean();
  uint64_t tenant_id_;
  uint64_t last_access_time_;
  ObDiagnoseTenantInfo base_value_;
};

class ObDISessionCache
{
public:
  static ObDISessionCache &get_instance();
  void destroy();

  int get_all_diag_info(ObIArray<std::pair<uint64_t, ObDISessionCollect*> > &diag_infos);

  int get_the_diag_info(
      uint64_t session_id,
      ObDISessionCollect *&diag_infos);
  int get_node(uint64_t session_id, ObDISessionCollect *&session_collect);
private:
  struct ObSessionBucket
  {
    ObSessionBucket() : list_(), lock_()
    {
    }
    int get_the_node(const uint64_t hash_value, ObDISessionCollect *&value)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      ObDISessionCollect *head = list_.get_header();
      ObDISessionCollect *node = list_.get_first();
      while (head != node && NULL != node) {
        if (hash_value == node->session_id_) {
          value = node;
          ret = OB_SUCCESS;
          break;
        } else {
          node = node->get_next();
        }
      }
      return ret;
    }
    ObDIList<ObDISessionCollect> list_;
    DIRWLock lock_;
  };
  ObDISessionCache();
  virtual ~ObDISessionCache();
  ObSessionBucket di_map_[OB_MAX_SERVER_SESSION_CNT];
  ObDISessionCollect collects_[OB_MAX_SERVER_SESSION_CNT];
};

struct ObTenantBucket
{
  ObTenantBucket() : list_()
  {
  }
  int get_the_node(const uint64_t hash_value, ObDITenantCollect *&value)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    ObDITenantCollect *head = list_.get_header();
    ObDITenantCollect *node = list_.get_first();
    while (head != node && NULL != node) {
      if (hash_value == node->tenant_id_) {
        value = node;
        ret = OB_SUCCESS;
        break;
      } else {
        node = node->get_next();
      }
    }
    return ret;
  }
  ObDIList<ObDITenantCollect> list_;
};

struct AddWaitEvent
{
  AddWaitEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, ObDiagnoseTenantInfo &info)
  {
    base_info.add_wait_event(info);
  }
};

struct AddStatEvent
{
  AddStatEvent() {}
  void operator()(ObDiagnoseTenantInfo &base_info, ObDiagnoseTenantInfo &info)
  {
    base_info.add_stat_event(info);
  }
};

struct AddLatchStat
{
  AddLatchStat() {}
  void operator()(ObDiagnoseTenantInfo &base_info, ObDiagnoseTenantInfo &info)
  {
    base_info.add_latch_stat(info);
  }
};



class ObDITenantCache : public ObDINode<ObDITenantCache>
{
public:
  ObDITenantCache();
  virtual ~ObDITenantCache();
  int get_the_diag_info(
      uint64_t tenant_id,
      ObDiagnoseTenantInfo &diag_infos);
  template<class _callback>
  int get_all_node(ObIAllocator &allocator, ObTenantBucket *di_map, _callback &callback);
  int get_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect);
  static const int64_t MAX_TENANT_NODE_NUM = 32;
private:
  int add_node(uint64_t tenant_id, ObDITenantCollect *&tenant_collect);
  int del_node(uint64_t tenant_id);
  ObTenantBucket di_map_[MAX_TENANT_NODE_NUM];
  ObDITenantCollect collects_[MAX_TENANT_NODE_NUM];
  uint64_t last_access_time_;
};

class ObDIGlobalTenantCache
{
public:
  static ObDIGlobalTenantCache &get_instance();
  void link(ObDITenantCache *node);
  void unlink(ObDITenantCache *node);
  int get_all_wait_event(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_all_stat_event(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_all_latch_stat(
      ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos);
  int get_the_diag_info(
      uint64_t tenant_id,
      ObDiagnoseTenantInfo &diag_infos);
private:
  ObDIGlobalTenantCache();
  virtual ~ObDIGlobalTenantCache();
  template<class _callback>
  int get_all_diag_info(ObIAllocator &allocator,
      ObIArray<std::pair<uint64_t, ObDiagnoseTenantInfo*> > &diag_infos, _callback &callback);
  ObDIList<ObDITenantCache> list_;
  int64_t cnt_;
  DIRWLock lock_;
};

}// end namespace common
}
#endif /* OB_DI_CACHE_H_ */

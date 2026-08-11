/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include "proxy/mysql/ob_service_name_session_info.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

DEF_TO_STRING(ObTenantInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(tenant_name), K_(cluster_name));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObServiceNameCursorInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(cursor_id), K_(cursor_tenant_info));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObServiceNamePsInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(ps_id), K_(ps_tenant_info));
  J_OBJ_END();
  return pos;
}


int ObServiceNameCursorInfo::set_cursor_tenant(const common::ObString &tenant_name,
                                               const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cursor_tenant_info_.tenant_name_.rewrite(tenant_name))) {
    LOG_WDIAG("fail to rewrite tenant_name", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(cursor_tenant_info_.cluster_name_.rewrite(cluster_name))) {
    LOG_WDIAG("fail to rewrite cluster_name_", K(tenant_name), K(cluster_name), K(ret));
  }
  return ret;
}


void ObServiceNameCursorInfo::destroy()
{
  LOG_INFO("ObServiceNameCursorInfo will be destroyed", KPC(this));
  op_free(this);
}

int ObServiceNamePsInfo::add_ps_tenant(const common::ObString &tenant_name,
                                       const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObTenantInfo tenant_info;
  if (OB_FAIL(tenant_info.tenant_name_.rewrite(tenant_name))) {
    LOG_WDIAG("fail to rewrite tenant_name", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(tenant_info.cluster_name_.rewrite(cluster_name))) {
    LOG_WDIAG("fail to rewrite cluster_name_", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(ps_tenant_info_.push_back(tenant_info))) {
    LOG_WDIAG("fail to push back tenant info", K(tenant_name), K(cluster_name), K(ret));
  } else {
    LOG_DEBUG("succ to add ps tenant", K(tenant_info));
  }
  return ret;
}

void ObServiceNamePsInfo::destroy()
{
  LOG_INFO("ObServiceNameCursorInfo will be destroyed", KPC(this));
  op_free(this);
}

int ObServiceaNameSessionInfo::add_cursor_id_tenant_info(uint32_t client_cursor_id,
                                                         const common::ObString &tenant_name,
                                                         const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObServiceNameCursorInfo *cursor_info = NULL;
  if (OB_ISNULL(cursor_info = op_alloc_args(ObServiceNameCursorInfo, client_cursor_id, tenant_name, cluster_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObServiceNameCursorInfo",
              K(client_cursor_id), K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(cursor_info_map_.unique_set(cursor_info))) {
    LOG_WDIAG("fail to unique insert cursor tenant to HashMap",
              K(client_cursor_id), K(tenant_name), K(cluster_name), K(ret));
  } else {
    LOG_DEBUG("succ to add cursor id tenant info", KPC(cursor_info));
    cursor_info = NULL;
  }

  // 成功后，cursor_info应该置为NULL，否则需要释放
  if (NULL != cursor_info) {
    cursor_info->destroy();
  }
  return ret;
}

int ObServiceaNameSessionInfo::add_ps_id_tenant_info(uint32_t client_ps_id,
                                                     const common::ObString &tenant_name,
                                                     const common::ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  ObServiceNamePsInfo *ps_info = NULL;
  if (OB_ISNULL(ps_info = op_alloc_args(ObServiceNamePsInfo, client_ps_id, tenant_name, cluster_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObServiceNamePsInfoMap",
              K(client_ps_id), K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(ps_info_map_.unique_set(ps_info))) {
    LOG_WDIAG("fail to unique insert cursor tenant to HashMap",
              K(client_ps_id), K(tenant_name), K(cluster_name), K(ret));
  } else {
    LOG_DEBUG("succ to add ps id tenant info", KPC(ps_info));
    ps_info = NULL;
  }

  // 成功后，ps_info应该置为NULL，否则需要释放
  if (NULL != ps_info) {
    ps_info->destroy();
  }
  return ret;
}

ObServiceNameCursorInfo *ObServiceaNameSessionInfo::get_cursor_id_tenant_info(uint32_t client_cursor_id) const
{
  ObServiceNameCursorInfo *cursor_info_ret = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(cursor_info_map_.get_refactored(client_cursor_id, cursor_info_ret))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WDIAG("fail to get cursor_info with client cursor id for service name", K(client_cursor_id), K(ret));
    }
  } else if (OB_ISNULL(cursor_info_ret)) {
    LOG_WDIAG("get cursor_info is null", K(client_cursor_id), K(ret));
  }
  return cursor_info_ret;
}

ObServiceNamePsInfo *ObServiceaNameSessionInfo::get_ps_id_tenant_info(uint32_t client_ps_id) const
{
  ObServiceNamePsInfo *ps_info_ret = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ps_info_map_.get_refactored(client_ps_id, ps_info_ret))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WDIAG("fail to get ps_info with client ps id for service name", K(client_ps_id), K(ret));
    }
  } else if (OB_ISNULL(ps_info_ret)) {
    LOG_WDIAG("get cursor_info is null", K(client_ps_id), K(ret));
  }
  return ps_info_ret;
}

void ObServiceaNameSessionInfo::remove_cursor_info(uint32_t client_ps_id)
{
  ObServiceNameCursorInfo *cursor_info = cursor_info_map_.remove(client_ps_id);
  if (OB_NOT_NULL(cursor_info)) {
    cursor_info->destroy();
    cursor_info = NULL;
  }
}

void ObServiceaNameSessionInfo::remove_ps_info(uint32_t client_ps_id)
{
  ObServiceNamePsInfo *ps_info = ps_info_map_.remove(client_ps_id);
  if (OB_NOT_NULL(ps_info)) {
    ps_info->destroy();
    ps_info = NULL;
  }
}

void ObServiceaNameSessionInfo::destroy()
{
  // 释放ps info
  ObServiceNamePsInfoMap::iterator ps_last = ps_info_map_.end();
  ObServiceNamePsInfoMap::iterator ps_tmp_iter;
  for (ObServiceNamePsInfoMap::iterator ps_iter = ps_info_map_.begin(); ps_iter != ps_last;) {
    ps_tmp_iter = ps_iter;
    ++ps_iter;
    ps_tmp_iter->destroy();
  }
  ps_info_map_.reset();

  // 释放cursor info
  ObServiceNameCursorInfoMap::iterator cursor_last = cursor_info_map_.end();
  ObServiceNameCursorInfoMap::iterator cursor_tmp_iter;
  for (ObServiceNameCursorInfoMap::iterator cursor_iter = cursor_info_map_.begin(); cursor_iter != cursor_last;) {
    cursor_tmp_iter = cursor_iter;
    ++cursor_iter;
    cursor_tmp_iter->destroy();
  }
  cursor_info_map_.reset();
  op_free(this);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

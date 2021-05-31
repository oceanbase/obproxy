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

#include "lib/stat/ob_session_stat.h"

namespace oceanbase
{
namespace common
{

ObSessionDIBuffer::ObSessionDIBuffer()
  : tenant_cache_(),
    session_collect_(NULL),
    tenant_collect_(NULL)
{
}

ObSessionDIBuffer::~ObSessionDIBuffer()
{
}

int ObSessionDIBuffer::switch_both(const uint64_t tenant_id, const uint64_t session_id)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = switch_session(session_id))) {
  } else {
    ret = switch_tenant(tenant_id);
  }
  return ret;
}

int ObSessionDIBuffer::switch_session(const uint64_t session_id)
{
  int ret = OB_SUCCESS;
  if (NULL != session_collect_ && session_id == session_collect_->session_id_) {
  } else {
    if (NULL != session_collect_) {
      session_collect_->lock_.unlock();
    }
    ret = ObDISessionCache::get_instance().get_node(session_id, session_collect_);
  }
  return ret;
}

int ObSessionDIBuffer::switch_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (NULL != tenant_collect_ && tenant_id == tenant_collect_->tenant_id_) {
  } else {
    ret = tenant_cache_.get_node(tenant_id, tenant_collect_);
  }
  return ret;
}

uint64_t ObSessionDIBuffer::get_tenant_id()
{
  uint64_t tenant_id = 0;
  if (NULL == tenant_collect_) {
    tenant_id = OB_SYS_TENANT_ID;
  } else {
    tenant_id = tenant_collect_->tenant_id_;
  }
  return tenant_id;
}

void ObSessionDIBuffer::reset_session()
{
  if (NULL != session_collect_) {
    session_collect_->lock_.unlock();
    session_collect_ = NULL;
  }
}

/**
 *--------------------------------------------------------ObSessionStatEstGuard---------------------------------------------
 */
ObSessionStatEstGuard::ObSessionStatEstGuard(uint64_t tenant_id, uint64_t session_id)
  : prev_tenant_id_(OB_SYS_TENANT_ID),
    prev_session_id_(0)
{
  ObSessionDIBuffer *buffer = ObDITls<ObSessionDIBuffer>::get_instance();
  if (NULL != buffer) {
    if (NULL != (buffer->get_curr_tenant())) {
      prev_tenant_id_ = buffer->get_curr_tenant()->tenant_id_;
    }
    if (NULL != (buffer->get_curr_session())) {
      prev_session_id_ = buffer->get_curr_session()->session_id_;
    }
    if (0 < tenant_id && 0 < session_id) {
      buffer->switch_both(tenant_id, session_id);
    }
  }
}

ObSessionStatEstGuard::~ObSessionStatEstGuard()
{
  ObSessionDIBuffer *buffer = ObDITls<ObSessionDIBuffer>::get_instance();
  int ret = OB_SUCCESS;
  if (NULL != buffer) {
    if (OB_FAIL(buffer->switch_tenant(prev_tenant_id_))) {
      // do nothing
    }
    if (0 != prev_session_id_) {
      if (OB_FAIL(buffer->switch_session(prev_session_id_))) {
        // do nothing
      }
    } else {
      buffer->reset_session();
    }
  }
}

ObTenantStatEstGuard::ObTenantStatEstGuard(uint64_t tenant_id)
  : prev_tenant_id_(OB_SYS_TENANT_ID)
{
  int ret = OB_SUCCESS;
  ObSessionDIBuffer *buffer = ObDITls<ObSessionDIBuffer>::get_instance();
  if (NULL != buffer) {
    if (NULL != (buffer->get_curr_tenant())) {
      prev_tenant_id_ = buffer->get_curr_tenant()->tenant_id_;
    }
    if (0 < tenant_id) {
      if (OB_FAIL(buffer->switch_tenant(tenant_id))) {
        // do nothing
      }
    }
  }
}

ObTenantStatEstGuard::~ObTenantStatEstGuard()
{
  int ret = OB_SUCCESS;
  ObSessionDIBuffer *buffer = ObDITls<ObSessionDIBuffer>::get_instance();
  if (NULL != buffer) {
    if (OB_FAIL(buffer->switch_tenant(prev_tenant_id_))) {
      // do nothing
    }
  }
}

void  __attribute__((constructor(101))) init_SessionDIBuffer()
{
  oceanbase::common::ObDITls<ObSessionDIBuffer>::get_instance();
}
} /* namespace common */
} /* namespace oceanbase */

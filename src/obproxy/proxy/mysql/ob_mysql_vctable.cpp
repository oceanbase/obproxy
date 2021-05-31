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
#include "proxy/mysql/ob_mysql_vctable.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObMysqlVCTable::ObMysqlVCTable()
{
  memset(&vc_table_, 0, sizeof(vc_table_));
}

ObMysqlVCTableEntry *ObMysqlVCTable::new_entry()
{
  ObMysqlVCTableEntry *ret = NULL;

  for (int64_t i = 0; NULL == ret && i < VC_TABLE_MAX_ENTRIES; ++i) {
    if (NULL == vc_table_[i].vc_) {
      ret = vc_table_ + i;
    }
  }
  return ret;
}

ObMysqlVCTableEntry *ObMysqlVCTable::find_entry(ObVConnection *vc)
{
  ObMysqlVCTableEntry *ret = NULL;
  if (OB_LIKELY(NULL != vc)) {
    for (int64_t i = 0; NULL == ret && i < VC_TABLE_MAX_ENTRIES; ++i) {
      if (vc_table_[i].vc_ == vc) {
        ret = vc_table_ + i;
      }
    }
  }
  return ret;
}

ObMysqlVCTableEntry *ObMysqlVCTable::find_entry(ObVIO *vio)
{
  ObMysqlVCTableEntry *ret = NULL;
  if (OB_LIKELY(NULL != vio)) {
    for (int64_t i = 0; NULL == ret && i < VC_TABLE_MAX_ENTRIES; ++i) {
      if (vc_table_[i].read_vio_ == vio || vc_table_[i].write_vio_ == vio) {
        if (OB_LIKELY(NULL != vc_table_[i].vc_)) {
          ret = vc_table_ + i;
        }
      }
    }
  }
  return ret;
}

// Deallocates all buffers from the associated
// entry and re-initializes it's other fields for reuse
int ObMysqlVCTable::remove_entry(ObMysqlVCTableEntry *e)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(e) || OB_UNLIKELY(NULL != e->vc_ && !e->in_tunnel_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid VCTableEntry", K(e), K(ret));
  } else {
    e->vc_ = NULL;
    e->eos_ = false;
    if (NULL != e->read_buffer_) {
      free_miobuffer(e->read_buffer_);
      e->read_buffer_ = NULL;
    }
    if (NULL != e->write_buffer_) {
      free_miobuffer(e->write_buffer_);
      e->write_buffer_ = NULL;
    }
    e->read_vio_ = NULL;
    e->write_vio_ = NULL;
    e->vc_handler_ = NULL;
    e->vc_type_ = MYSQL_UNKNOWN;
    e->in_tunnel_ = false;
  }
  return ret;
}

// Close the associate vc for the entry,
// and the call remove_entry
int ObMysqlVCTable::cleanup_entry(ObMysqlVCTableEntry *e, bool need_close)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(e) || OB_ISNULL(e->vc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vc table entry", K(e), K(ret));
  } else {
    LOG_DEBUG("cleanup_entry", "in_tunnel", e->in_tunnel_,
              "eos", e->eos_, "vc_type", e->vc_type_, "vc", e->vc_, K(need_close));
    if (!e->in_tunnel_) {
      if (need_close) {
        e->vc_->do_io_close();
        e->vc_ = NULL;
      }
    }
    if (OB_FAIL(remove_entry(e))) {
      LOG_WARN("fail to remove entry", K(ret));
    }
  }
  return ret;
}

int ObMysqlVCTable::cleanup_all()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < VC_TABLE_MAX_ENTRIES; ++i) {
    if (NULL != vc_table_[i].vc_) {
      if (OB_FAIL(cleanup_entry(vc_table_ + i))) {
        LOG_WARN("fail to cleanup vc table entry", K(ret));
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

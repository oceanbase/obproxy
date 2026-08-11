/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX PROXY

#include "proxy/rpc/rpclib/ob_tablet_ls_entry.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
void  ObTabletLsEntry::free()
{
  LOG_DEBUG(" ObTabletLsEntry will be free", K(*this));
  buf_ = NULL;
  this->~ObTabletLsEntry();
  op_fixed_mem_free(this, buf_len_);
}

int  ObTabletLsEntry::alloc_and_init_tablet_ls_entry(const int64_t tenant_id, const int64_t table_id, OB_TABLET_TO_LS_MAP &tablet_ls_map,
                                                     const int64_t cr_version, const int64_t cr_id,  ObTabletLsEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || cr_version < 0 || NULL != entry
    || table_id == 0 )) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(tenant_id), K(table_id), K(cr_version), K(entry),
               K(ret));
  } else {
    int64_t alloc_size = sizeof( ObTabletLsEntry);

    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc tablet_ls entry succ", K(alloc_size));
      entry = new (buf)  ObTabletLsEntry();
      if (OB_FAIL(entry->init(tenant_id, table_id, tablet_ls_map, buf, alloc_size))) {
        LOG_WDIAG("fail to init  ObTabletLsEntry", K(ret));
      } else {
        entry->inc_ref();
        entry->renew_last_access_time();
        entry->renew_last_valid_time();
        entry->set_avail_state();
        entry->set_cr_version(cr_version);
        entry->set_cr_id(cr_id);
      }
    }
    if ((OB_FAIL(ret)) && (NULL != buf)) {
      if (NULL != entry) {
        entry->~ObTabletLsEntry();
        entry = NULL;
      }
      op_fixed_mem_free(buf, alloc_size);
      alloc_size = 0;
    }
  }
  return ret;
}

int  ObTabletLsEntry::init(const int64_t tenant_id, const int64_t table_id, OB_TABLET_TO_LS_MAP &tablet_ls_map,
                           char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf_len), K(buf), K(ret));
  } else {
    int64_t pos = sizeof( ObTabletLsEntry);

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pos != buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to copy string into buf", K(pos), K(buf_len), K(*this), K(ret));
      } else {
        tablet_to_ls_map_.reuse();
        if (tablet_ls_map.created()) {
          //init map base tablet_ls_map;
          OB_TABLET_TO_LS_MAP::iterator iter = tablet_ls_map.begin();
          OB_TABLET_TO_LS_MAP::iterator end = tablet_ls_map.end();
          while (iter != end) {
            tablet_to_ls_map_.set_refactored(iter->first, iter->second);
            iter++;
          }
        }
        tenant_id_ = tenant_id;
        table_id_ = table_id;
        create_time_us_ = ObTimeUtility::current_time();
        buf_len_ = buf_len;
        buf_ = buf;
        is_inited_ = true;
        LOG_DEBUG("succ to init ObTabletLsEntry", K(*this));
      }
    }
  }

  return ret;
}

int64_t  ObTabletLsEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(ref_count),
       K_(cr_version),
       K_(cr_id),
       K_(create_time_us),
       K_(last_valid_time_us),
       K_(last_access_time_us),
       K_(last_update_time_us),
       K_(schema_version),
       K_(tenant_version),
       K_(time_for_expired),
       "state", get_route_entry_state(state_),
       K_(is_inited),
       K_(tenant_id),
       K_(table_id));
  J_OBJ_END();
  return pos;
}

bool ObTabletLsEntry::is_same_tablet_ls_map(const OB_TABLET_TO_LS_MAP &new_map) const
{
  bool bret = true;
  const OB_TABLET_TO_LS_MAP &cur_map = get_tablet_ls_map_const();
  if (cur_map.size() != new_map.size()) {
    // size not match
    bret = false;
  } else {
    OB_TABLET_TO_LS_MAP::const_iterator iter = cur_map.begin();
    OB_TABLET_TO_LS_MAP::const_iterator end = cur_map.end();
    bret = true;
    while(iter != end && bret) {
      int64_t *value = (int64_t *)new_map.get(iter->first);
      if (!value || *value != iter->second)  {
        bret = false;
      }
      iter++;
    }
  }

  return bret;
}

int64_t  ObTabletLsEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(table_id), K_(cr_version), K_(cr_id));
  J_OBJ_END();
  return pos;
}


}
}
}
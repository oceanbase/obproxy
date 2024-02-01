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
#include "proxy/route/ob_sql_table_entry.h"
#include "lib/string/ob_string.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

void ObSqlTableEntry::destroy()
{
  LOG_INFO("ObSqlTableEntry will be destroyed", KPC(this));
  key_.reset();
  int64_t total_len = sizeof(ObSqlTableEntry) + buf_len_;
  buf_start_ = NULL;
  buf_len_ = 0;
  op_fixed_mem_free(this, total_len);
}

int ObSqlTableEntry::alloc_and_init_sql_table_entry(const ObSqlTableEntryKey &key,
                                                    const ObString &table_name,
                                                    ObSqlTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid()) || OB_UNLIKELY(NULL != entry)
      || OB_UNLIKELY(table_name.length() > OB_MAX_TABLE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(key), K(entry), K(table_name), K(ret));
  } else {
    int64_t name_size = key.cluster_name_.length()
                        + key.tenant_name_.length() + key.database_name_.length()
                        + key.sql_id_.length() + table_name.length();
    int64_t obj_size = sizeof(ObSqlTableEntry);
    int64_t alloc_size =  name_size + obj_size;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc entry succ", K(alloc_size), K(obj_size), K(name_size), K(key), K(table_name));
      entry = new (buf) ObSqlTableEntry();
      if (OB_FAIL(entry->init(buf + obj_size, name_size))) {
        LOG_WDIAG("fail to init entry", K(alloc_size), K(ret));
      } else {
        entry->copy_key_and_name(key, table_name);
        entry->set_create_time();
        entry->set_avail_state();
        entry->inc_ref();
      }
    }
  }
  if (OB_FAIL(ret) && NULL != entry) {
    entry->dec_ref();
    entry = NULL;
  }
  return ret;
}

int ObSqlTableEntry::init(char *buf_start, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf_len), K(buf_start), K(ret));
  } else {
    create_time_us_ = hrtime_to_usec(event::get_hrtime());
    buf_len_ = buf_len;
    buf_start_ = buf_start;
  }

  return ret;
}

void ObSqlTableEntry::copy_key_and_name(const ObSqlTableEntryKey &key, const ObString &table_name)
{
  key_.cr_id_ = key.cr_id_;
  key_.cr_version_ = key.cr_version_;
  int64_t pos = 0;
  MEMCPY(buf_start_ + pos, key.cluster_name_.ptr(), key.cluster_name_.length());
  key_.cluster_name_.assign_ptr(buf_start_ + pos, static_cast<int32_t>(key.cluster_name_.length()));
  pos += key.cluster_name_.length();
  MEMCPY(buf_start_ + pos, key.tenant_name_.ptr(), key.tenant_name_.length());
  key_.tenant_name_.assign_ptr(buf_start_ + pos, static_cast<int32_t>(key.tenant_name_.length()));
  pos += key.tenant_name_.length();
  MEMCPY(buf_start_ + pos, key.database_name_.ptr(), key.database_name_.length());
  key_.database_name_.assign_ptr(buf_start_ + pos, static_cast<int32_t>(key.database_name_.length()));
  pos += key.database_name_.length();
  MEMCPY(buf_start_ + pos, key.sql_id_.ptr(), key.sql_id_.length());
  key_.sql_id_.assign_ptr(buf_start_ + pos, static_cast<int32_t>(key.sql_id_.length()));
  pos += key.sql_id_.length();
  MEMCPY(buf_start_ + pos, table_name.ptr(), table_name.length());
  table_name_.assign_ptr(buf_start_ + pos, static_cast<int32_t>(table_name.length()));
  pos += table_name.length();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

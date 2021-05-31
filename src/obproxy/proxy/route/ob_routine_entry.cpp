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

#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/route/ob_routine_entry.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/route/obproxy_part_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

common::ObString get_routine_type_string(const ObRoutineType type)
{
  static const ObString type_string_array[ROUTINE_MAX_TYPE] =
  {
      ObString::make_string("INVALID_TYPE"),
      ObString::make_string("PROCEDURE_TYPE"),
      ObString::make_string("FUNCTION_TYPE"),
      ObString::make_string("PACKAGE_TYPE"),
  };

  ObString string;
  if (OB_LIKELY(type >= INVALID_ROUTINE_TYPE) && OB_LIKELY(type < ROUTINE_MAX_TYPE)) {
    string = type_string_array[type];
  }
  return string;
}
int ObRoutineEntry::init(char *buf_start, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(buf_len), K(buf_start), K(ret));
  } else {
    create_time_us_ = ObTimeUtility::current_time();
    buf_len_ = buf_len;
    buf_start_ = buf_start;
    is_inited_ = true;
  }

  return ret;
}

int ObRoutineEntry::set_names_sql(const ObRoutineEntryName &names,
    const common::ObString &route_sql,
    const uint32_t parse_extra_char_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!names.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(names), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(names_.deep_copy(names, buf_start_, buf_len_ - route_sql.length() - parse_extra_char_num))) {
    LOG_WARN("fail to deep copy table entry names", K(ret));
  } else {
    if (route_sql.empty()) {
      route_sql_.reset();
    } else {
      int64_t pos = names_.get_total_str_len();
      MEMCPY(buf_start_ + pos, route_sql.ptr(), route_sql.length());
      // add '\0' at the tail for parser
      memset(buf_start_ + pos + route_sql.length(), '\0', parse_extra_char_num);
      route_sql_.assign_ptr(buf_start_ + pos, route_sql.length());//not include the end '\0', but must has '\0' in the end
    }
  }
  return ret;
}

void ObRoutineEntry::free()
{
  LOG_DEBUG("ObRoutineEntry was freed", KPC(this));
  names_.reset();
  route_sql_.reset();
  is_package_database_ = false;
  routine_id_ = common::OB_INVALID_ID;
  cr_id_ = OB_INVALID_CLUSTER_ID;
  routine_type_ = INVALID_ROUTINE_TYPE;

  int64_t total_len = sizeof(ObRoutineEntry) + buf_len_;
  buf_start_ = NULL;
  buf_len_ = 0;
  op_fixed_mem_free(this, total_len);
}

int ObRoutineEntry::alloc_and_init_routine_entry(
    const ObRoutineEntryName &names,
    const int64_t cr_version,
    const int64_t cr_id,
    const common::ObString &route_sql,
    ObRoutineEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!names.is_valid() || cr_version < 0 || NULL != entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(names), K(cr_version), K(entry), K(ret));
  } else {
    const uint32_t PARSE_EXTRA_CHAR_NUM = 2;
    const int64_t name_size = names.get_total_str_len();
    const int64_t obj_size = sizeof(ObRoutineEntry);
    const int64_t sql_size = route_sql.length() + PARSE_EXTRA_CHAR_NUM;
    const int64_t alloc_size =  name_size + obj_size + sql_size;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc entry succ", K(alloc_size), K(obj_size), K(name_size), K(sql_size), K(route_sql), K(names));
      entry = new (buf) ObRoutineEntry();
      if (OB_FAIL(entry->init(buf + obj_size, name_size + sql_size))) {
        LOG_WARN("fail to init entry", K(alloc_size), K(ret));
      } else if (OB_FAIL(entry->set_names_sql(names, route_sql, PARSE_EXTRA_CHAR_NUM))) {
        LOG_WARN("fail to set name", K(names), K(route_sql), K(ret));
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
      op_fixed_mem_free(buf, alloc_size);
      entry = NULL;
    }
  }
  return ret;
}

int64_t ObRoutineEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  pos += ObRouteEntry::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(KP(this),
       K_(is_inited),
       "routine_type", get_routine_type_string(routine_type_),
       K_(routine_id),
       K_(cr_id),
       K_(names),
       K_(route_sql),
       K_(is_package_database),
       K_(buf_len),
       KP_(buf_start));
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

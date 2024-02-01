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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_schema_struct.h"

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

ObTenantResource::ObTenantResource()
{
  reset();
}

ObTenantResource::~ObTenantResource()
{
}

void ObTenantResource::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  cpu_reserved_ = 0;
  cpu_max_ = 0;
  mem_reserved_ = 0;
  mem_max_ = 0;
  iops_reserved_ = 0;
  iops_max_ = 0;
  tps_reserved_ = 0;
  tps_max_ = 0;
  qps_reserved_ = 0;
  qps_max_ = 0;
}

bool ObTenantResource::is_valid() const
{
  return common::OB_INVALID_ID != tenant_id_
      && cpu_reserved_ >= 0
      && cpu_max_ >= 0
      && cpu_reserved_ <= cpu_max_
      && mem_reserved_ >= 0
      && mem_max_ >= 0
      && mem_reserved_ <= mem_max_
      && iops_reserved_ >= 0
      && iops_max_ >= 0
      && iops_reserved_ <= iops_max_
      && tps_reserved_ >= 0
      && tps_max_ >= 0
      && tps_reserved_ <= tps_max_
      && qps_reserved_ >= 0
      && qps_max_ >= 0
      && qps_reserved_ <= qps_max_;
}

int64_t ObTenantResource::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(cpu_reserved),
       K_(cpu_max),
       K_(mem_reserved),
       K_(mem_max),
       K_(iops_reserved),
       K_(iops_max),
       K_(tps_reserved),
       K_(tps_max),
       K_(qps_reserved),
       K_(qps_max));
  return pos;
}

OB_SERIALIZE_MEMBER(ObTenantResource,
                    tenant_id_,
                    cpu_reserved_,
                    cpu_max_,
                    mem_reserved_,
                    mem_max_,
                    iops_reserved_,
                    iops_max_,
                    tps_reserved_,
                    tps_max_,
                    qps_reserved_,
                    qps_max_);

ObUser::ObUser()
{
  reset();
}

ObUser::~ObUser()
{
}

void ObUser::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  user_id_ = common::OB_INVALID_ID;
  MEMSET(user_name_, 0, sizeof(user_name_));
  MEMSET(host_, 0, sizeof(host_));
  MEMSET(passwd_, 0, sizeof(passwd_));
  MEMSET(info_, 0, sizeof(info_));
  priv_all_ = true;
  priv_alter_ = true;
  priv_create_ = true;
  priv_create_user_ = true;
  priv_delete_ = true;
  priv_drop_ = true;
  priv_grant_option_ = true;
  priv_insert_ = true;
  priv_update_ = true;
  priv_select_ = true;
  priv_replace_ = true;
  is_locked_ = true;
}

int64_t ObUser::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(user_id),
       K_(host),
       K_(passwd),
       K_(info),
       K_(priv_all),
       K_(priv_alter),
       K_(priv_create),
       K_(priv_create_user),
       K_(priv_delete),
       K_(priv_drop),
       K_(priv_grant_option),
       K_(priv_insert),
       K_(priv_update),
       K_(priv_select),
       K_(priv_replace),
       K_(is_locked));
  return pos;
}

ObDatabasePrivilege::ObDatabasePrivilege()
{
  reset();
}

ObDatabasePrivilege::~ObDatabasePrivilege()
{
}

void ObDatabasePrivilege::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  user_id_ = common::OB_INVALID_ID;
  MEMSET(host_, 0, sizeof(host_));
  database_id_ = common::OB_INVALID_ID;
  priv_all_ = true;
  priv_alter_ = true;
  priv_create_ = true;
  priv_delete_ = true;
  priv_drop_ = true;
  priv_grant_option_ = true;
  priv_insert_ = true;
  priv_update_ = true;
  priv_select_ = true;
  priv_replace_ = true;
}

int64_t ObDatabasePrivilege::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(user_id),
       K_(host),
       K_(database_id),
       K_(priv_all),
       K_(priv_alter),
       K_(priv_create),
       K_(priv_delete),
       K_(priv_drop),
       K_(priv_grant_option),
       K_(priv_insert),
       K_(priv_update),
       K_(priv_select),
       K_(priv_replace));
  return pos;
}

ObSysParam::ObSysParam()
{
  reset();
}

ObSysParam::~ObSysParam()
{
}

int ObSysParam::init(const uint64_t tenant_id,
                     const common::ObZone &zone,
                     const ObString &name,
                     int64_t data_type,
                     const ObString &value,
                     const ObString &min_val,
                     const ObString &max_val,
                     const ObString &info,
                     int64_t flags)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  zone_ = zone;
  data_type_ = data_type;
  flags_ = flags;
  int64_t pos = 0;
  if (OB_INVALID == tenant_id || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("tenant id is invalid or some variable is null", K(name), K(ret));
  } else if (OB_FAIL(databuff_printf(name_, OB_MAX_SYS_PARAM_NAME_LENGTH, pos, "%.*s", name.length(),
                                    name.ptr()))) {
    LOG_WDIAG("failed to print name", K(name), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(value_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", value.length(),
                                    value.ptr()))) {
    LOG_WDIAG("failed to print value", K(value), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(min_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", min_val.length(),
                                    min_val.ptr()))) {
    LOG_WDIAG("failed to print min_val", K(min_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(max_val_, OB_MAX_SYS_PARAM_VALUE_LENGTH, pos, "%.*s", max_val.length(),
                                    max_val.ptr()))) {
    LOG_WDIAG("failed to print max_val", K(max_val), K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(info_, OB_MAX_SYS_PARAM_INFO_LENGTH, pos, "%.*s", info.length(),
                                    info.ptr()))) {
    LOG_WDIAG("failed to print info", K(info), K(ret));
  } else {/*do nothing*/}
  return ret;
}

void ObSysParam::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  zone_.reset();
  MEMSET(name_, 0, sizeof(name_));
  data_type_ = 0;
  MEMSET(value_, 0, sizeof(value_));
  MEMSET(min_val_, 0, sizeof(min_val_));
  MEMSET(max_val_, 0, sizeof(max_val_));
  MEMSET(info_, 0, sizeof(info_));
  flags_ = 0;
}

int64_t ObSysParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(tenant_id),
       K_(zone),
       K_(name),
       K_(data_type),
       K_(value),
       K_(info),
       K_(flags));
  return pos;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTenantSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObSchema::ObSchema()
    : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(NULL)
{
}

ObSchema::ObSchema(common::ObIAllocator *allocator)
    : buffer_(this), error_ret_(OB_SUCCESS), is_inner_allocator_(false), allocator_(allocator)
{
}

ObSchema::~ObSchema()
{
  if (is_inner_allocator_) {
    common::ObArenaAllocator *arena = static_cast<common::ObArenaAllocator *>(allocator_);
    OB_DELETE(ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, arena);
    allocator_ = NULL;
  }
  buffer_ = NULL;
}

void *ObSchema::alloc(int64_t size)
{
  void *ret = NULL;
  if (NULL == allocator_) {
    if (NULL == (allocator_ = OB_NEW(ObArenaAllocator, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA, ObModIds::OB_SCHEMA_OB_SCHEMA_ARENA))) {
      LOG_WDIAG("Fail to new allocator.");
    } else {
      is_inner_allocator_ = true;
      ret = allocator_->alloc(size);
    }
  } else {
    ret = allocator_->alloc(size);
  }

  return ret;
}

void ObSchema::free(void *ptr)
{
  if (NULL != ptr) {
    if (NULL != allocator_) {
      allocator_->free(ptr);
    }
  }
}

int ObSchema::string_array2str(const common::ObIArray<common::ObString> &string_array,
                               char *str, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(str), K(buf_size));
  } else {
    MEMSET(str, 0, static_cast<uint32_t>(buf_size));
    int64_t nwrite = 0;
    int64_t n = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < string_array.count(); ++i) {
      n = snprintf(str + nwrite, static_cast<uint32_t>(buf_size - nwrite),
          "%s%s", to_cstring(string_array.at(i)), (i != string_array.count() - 1) ? ";" : "");
      if (n <= 0 || n >= buf_size - nwrite) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WDIAG("snprintf failed", K(ret));
      } else {
        nwrite += n;
      }
    }
  }
  return ret;

}

int ObSchema::str2string_array(const char *str,
                               common::ObIArray<common::ObString> &string_array) const
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  while (OB_SUCC(ret)) {
    item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ";", &save_ptr);
    if (NULL != item_str) {
      if (OB_FAIL(string_array.push_back(ObString::make_string(item_str)))) {
        LOG_WDIAG("push_back failed", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObSchema::deep_copy_str(const char *src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WDIAG("There has error in this schema, ", K(ret));
  } else if (OB_ISNULL(src)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("The src is NULL, ", K(ret));
  } else {
    int64_t len = strlen(src) + 1;
    if (NULL == (buf = static_cast<char*>(alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("Fail to allocate memory, ", K(len), K(ret));
    } else {
      MEMCPY(buf, src, len-1);
      buf[len-1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
    }
  }

  return ret;
}

int ObSchema::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WDIAG("There has error in this schema, ", K(ret));
  } else {
    if (src.length() > 0) {
      int64_t len = src.length() + 1;
      if (NULL == (buf = static_cast<char*>(alloc(len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("Fail to allocate memory, ", K(len), K(ret));
      } else {
        MEMCPY(buf, src.ptr(), len-1);
        buf[len - 1] = '\0';
        dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
      }
    } else {
      dest.reset();
    }
  }

  return ret;
}

int ObSchema::deep_copy_obj(const ObObj &src, ObObj &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t size = src.get_deep_copy_size();

  if (OB_SUCCESS != error_ret_) {
    ret = error_ret_;
    LOG_WDIAG("There has error in this schema, ", K(ret));
  } else {
    if (size > 0) {
      if (NULL == (buf = static_cast<char*>(alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("Fail to allocate memory, ", K(size), K(ret));
      } else if (OB_FAIL(dest.deep_copy(src, buf, size, pos))){
        LOG_WDIAG("Fail to deep copy obj, ", K(ret));
      }
    } else {
      dest = src;
    }
  }

  return ret;
}

int ObSchema::deep_copy_string_array(const ObIArray<ObString> &src_array,
                                     ObArrayHelper<ObString> &dst_array)
{
  int ret = OB_SUCCESS;
  if (NULL != dst_array.get_base_address()) {
    free(dst_array.get_base_address());
    dst_array.reset();
  }
  const int64_t alloc_size = src_array.count() * static_cast<int64_t>(sizeof(ObString));
  void *buf = NULL;
  if (src_array.count() <= 0) {
    // do nothing
  } else if (NULL == (buf = alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("alloc failed", K(alloc_size), K(ret));
  } else {
    dst_array.init(src_array.count(), static_cast<ObString *>(buf));
    for (int64_t i = 0; OB_SUCC(ret) && i < src_array.count(); ++i) {
      ObString str;
      if (OB_FAIL(deep_copy_str(src_array.at(i), str))) {
        LOG_WDIAG("deep_copy_str failed", K(ret));
      } else if (OB_FAIL(dst_array.push_back(str))) {
        LOG_WDIAG("push_back failed", K(ret));
        // free memory avoid memory leak
        for (int64_t j = 0; j < dst_array.count(); ++j) {
          free(dst_array.at(j).ptr());
        }
        free(str.ptr());
      }
    }
  }

  if (OB_SUCCESS != ret && NULL != buf) {
    free(buf);
  }
  return ret;
}

int ObSchema::add_string_to_array(const ObString &str,
                                  ObArrayHelper<ObString> &str_array)
{
  int ret = OB_SUCCESS;
  const int64_t extend_cnt = STRING_ARRAY_EXTEND_CNT;
  int64_t alloc_size = 0;
  void *buf = NULL;
  // if not init, alloc memory and init it
  if (!str_array.check_inner_stat()) {
    alloc_size = extend_cnt * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("alloc failed", K(alloc_size), K(ret));
    } else {
      str_array.init(extend_cnt, static_cast<ObString *>(buf));
    }
  }

  if (OB_SUCC(ret)) {
    ObString temp_str;
    if (OB_FAIL(deep_copy_str(str, temp_str))) {
      LOG_WDIAG("deep_copy_str failed", K(ret));
    } else {
      // if full, extend it
      if (str_array.get_array_size() == str_array.count()) {
        alloc_size = (str_array.count() + extend_cnt) * static_cast<int64_t>(sizeof(ObString));
        if (NULL == (buf = alloc(alloc_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("alloc failed", K(alloc_size), K(ret));
        } else {
          ObArrayHelper<ObString> new_array(
              str_array.count() + extend_cnt, static_cast<ObString *>(buf));
          if (OB_FAIL(new_array.assign(str_array))) {
            LOG_WDIAG("assign failed", K(ret));
          } else {
            free(str_array.get_base_address());
            str_array = new_array;
          }
        }
        if (OB_SUCCESS != ret && NULL != buf) {
          free(buf);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(str_array.push_back(temp_str))) {
          LOG_WDIAG("push_back failed", K(ret));
          free(temp_str.ptr());
        }
      }
    }
  }
  return ret;
}

int ObSchema::serialize_string_array(char *buf, const int64_t buf_len, int64_t &pos,
                                     const ObArrayHelper<ObString> &str_array) const
{
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  const int64_t count = str_array.count();
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("buf should not be null", K(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, count))) {
    LOG_WDIAG("serialize count failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str_array.count(); ++i) {
      if (OB_FAIL(str_array.at(i).serialize(buf, buf_len, pos))) {
        LOG_WDIAG("serialize string failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    pos = temp_pos;
  }
  return ret;
}

int ObSchema::deserialize_string_array(const char *buf, const int64_t data_len, int64_t &pos,
                                       common::ObArrayHelper<common::ObString> &str_array)
{
  int ret = OB_SUCCESS;
  int64_t temp_pos = pos;
  int64_t count = 0;
  str_array.reset();
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("buf should not be null", K(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WDIAG("deserialize count failed", K(ret));
  } else if (0 == count){
    //do nothing
  } else {
    void *array_buf = NULL;
    const int64_t alloc_size = count * static_cast<int64_t>(sizeof(ObString));
    if (NULL == (array_buf = alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("alloc memory failed", K(alloc_size), K(ret));
    } else {
      str_array.init(count, static_cast<ObString *>(array_buf));
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObString str;
        ObString copy_str;
        if (OB_FAIL(str.deserialize(buf, data_len, pos))) {
          LOG_WDIAG("string deserialize failed", K(ret));
        } else if (OB_FAIL(deep_copy_str(str, copy_str))) {
          LOG_WDIAG("deep_copy_str failed", K(ret));
        } else if (OB_FAIL(str_array.push_back(copy_str))) {
          LOG_WDIAG("push_back failed", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    pos = temp_pos;
  }
  return ret;
}

int64_t ObSchema::get_string_array_serialize_size(
      const ObArrayHelper<ObString> &str_array) const
{
  int64_t serialize_size = 0;
  const int64_t count = str_array.count();
  serialize_size += serialization::encoded_length_vi64(count);
  for (int64_t i = 0; i < count; ++i) {
    serialize_size += str_array.at(i).get_serialize_size();
  }
  return serialize_size;
}

void ObSchema::reset_string(ObString &str)
{
  if (NULL != str.ptr()) {
    free(str.ptr());
  }
  str.reset();
}

void ObSchema::reset_string_array(ObArrayHelper<ObString> &str_array)
{
  if (NULL != str_array.get_base_address()) {
    free(str_array.get_base_address());
  }
  str_array.reset();
}

const char *ObSchema::extract_str(const ObString &str) const
{
  return str.empty() ? "" : str.ptr();
}

void ObSchema::reset()
{
  error_ret_ = OB_SUCCESS;
  if (is_inner_allocator_ && NULL != allocator_) {
    //It's better to invoke the reset methods of allocator if the ObIAllocator has reset function
    ObArenaAllocator *arena = static_cast<ObArenaAllocator*>(allocator_);
    arena->reuse();
  }
}
common::ObCollationType ObSchema::get_cs_type_with_cmp_mode(const ObNameCaseMode mode)
{
  common::ObCollationType cs_type = common::CS_TYPE_INVALID;

  if (OB_ORIGIN_AND_INSENSITIVE == mode || OB_LOWERCASE_AND_INSENSITIVE == mode) {
    cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  } else if (OB_ORIGIN_AND_SENSITIVE == mode){
    cs_type = common::CS_TYPE_UTF8MB4_BIN;
  } else {
    SHARE_SCHEMA_LOG(EDIAG, "invalid ObNameCaseMode value", K(mode));
  }
  return cs_type;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTenantSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObTenantSchema::ObTenantSchema()
  : ObSchema()
{
  reset();
}

ObTenantSchema::ObTenantSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObTenantSchema::ObTenantSchema(const ObTenantSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObTenantSchema::~ObTenantSchema()
{
}

ObTenantSchema& ObTenantSchema::operator =(const ObTenantSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_schema_version(src_schema.schema_version_);
    set_replica_num(src_schema.replica_num_);
    set_locked(src_schema.locked_);
    set_read_only(src_schema.read_only_);
    set_collation_type(src_schema.get_collation_type());
    set_charset_type(src_schema.get_charset_type());
    set_name_case_mode(src_schema.get_name_case_mode());
    set_rewrite_merge_version(src_schema.get_rewrite_merge_version());
    if (OB_FAIL(set_tenant_name(src_schema.tenant_name_))) {
      LOG_WDIAG("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_zone_list(src_schema.zone_list_))) {
      LOG_WDIAG("set_zone_list failed", K(ret));
    } else if (OB_FAIL(set_primary_zone(src_schema.primary_zone_))) {
      LOG_WDIAG("set_primary_zone failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WDIAG("set_comment failed", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObTenantSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_ && schema_version_ > 0;
}

void ObTenantSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  reset_string(tenant_name_);
  replica_num_ = 0;
  reset_string_array(zone_list_);
  reset_string(primary_zone_);
  locked_ = false;
  read_only_ = false;
  rewrite_merge_version_ = 0;
  charset_type_ = ObCharset::get_default_charset();
  collation_type_ = ObCharset::get_default_collation(ObCharset::get_default_charset());
  name_case_mode_ = OB_NAME_CASE_INVALID;
  reset_string(comment_);
}

int64_t ObTenantSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += tenant_name_.length() + 1;
  convert_size += zone_list_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < zone_list_.count(); ++i) {
    convert_size += zone_list_.at(i).length() + 1;
  }
  convert_size += primary_zone_.length() + 1;
  convert_size += comment_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_, schema_version_, tenant_name_,
              replica_num_, primary_zone_, locked_,
              comment_, charset_type_, collation_type_,
              name_case_mode_, read_only_, rewrite_merge_version_);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("func_SERIALIZE failed", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WDIAG("serialize_string_array failed", K(ret));
  }

  LOG_INFO("serialize schema",
           K_(tenant_id), K_(schema_version), K_(tenant_name),
           K_(replica_num), K_(primary_zone), K_(locked),
           K_(comment), K_(charset_type), K_(collation_type),
           K_(name_case_mode), K_(rewrite_merge_version), K(ret));
  return ret;
}

OB_DEF_DESERIALIZE(ObTenantSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, schema_version_, tenant_name_, replica_num_,
              primary_zone_, locked_, comment_, charset_type_, collation_type_,
              name_case_mode_, read_only_, rewrite_merge_version_);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(set_tenant_name(tenant_name_))) {
    LOG_WDIAG("set_tenant_name failed", K(ret));
  } else if (OB_FAIL(set_primary_zone(primary_zone_))) {
    LOG_WDIAG("set_primary_zone failed", K(ret));
  } else if (OB_FAIL(set_comment(comment_))) {
    LOG_WDIAG("set_comment failed", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, zone_list_))) {
    LOG_WDIAG("deserialize_string_array failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTenantSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, schema_version_, tenant_name_, replica_num_,
              primary_zone_, locked_, comment_, charset_type_,
              collation_type_, name_case_mode_, read_only_, rewrite_merge_version_);
  len +=get_string_array_serialize_size(zone_list_);
  return len;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObDatabaseSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObDatabaseSchema::ObDatabaseSchema()
  : ObSchema()
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObDatabaseSchema::ObDatabaseSchema(const ObDatabaseSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObDatabaseSchema::~ObDatabaseSchema()
{
}

ObDatabaseSchema &ObDatabaseSchema::operator =(const ObDatabaseSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_database_id(src_schema.database_id_);
    set_schema_version(src_schema.schema_version_);
    set_replica_num(src_schema.replica_num_);
    set_charset_type(src_schema.charset_type_);
    set_collation_type(src_schema.collation_type_);
    set_name_case_mode(src_schema.name_case_mode_);
    set_read_only(src_schema.read_only_);
    set_default_tablegroup_id(src_schema.default_tablegroup_id_);

    if (OB_FAIL(set_database_name(src_schema.database_name_))) {
      LOG_WDIAG("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_zone_list(src_schema.zone_list_))) {
      LOG_WDIAG("set_zone_list failed", K(ret));
    } else if (OB_FAIL(set_primary_zone(src_schema.primary_zone_))) {
      LOG_WDIAG("set_primary_zone failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WDIAG("set_comment failed", K(ret));
    } else if (OB_FAIL(set_default_tablegroup_name(src_schema.default_tablegroup_name_))) {
      LOG_WDIAG("set_comment failed", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

ObTenantDatabaseId ObDatabaseSchema::get_tenant_database_id() const
{
  return ObTenantDatabaseId(tenant_id_, database_id_);
}

int64_t ObDatabaseSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += database_name_.length() + 1;
  convert_size += zone_list_.count() * static_cast<int64_t>(sizeof(ObString));
  for (int64_t i = 0; i < zone_list_.count(); ++i) {
    convert_size += zone_list_.at(i).length() + 1;
  }
  convert_size += primary_zone_.length() + 1;
  convert_size += comment_.length() + 1;
  return convert_size;
}

bool ObDatabaseSchema::is_valid() const
{
  return ObSchema::is_valid() && common::OB_INVALID_ID != tenant_id_
      && common::OB_INVALID_ID != database_id_ && schema_version_ > 0;
}

void ObDatabaseSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  reset_string(database_name_);
  replica_num_ = 0;
  reset_string_array(zone_list_);
  reset_string(primary_zone_);
  reset_string(comment_);
  charset_type_ = common::CHARSET_INVALID;
  collation_type_ = common::CS_TYPE_INVALID;
  name_case_mode_ = OB_NAME_CASE_INVALID;
  read_only_ = false;
  default_tablegroup_id_ = OB_INVALID_ID;
  reset_string(default_tablegroup_name_);
}

void ObDatabaseSchema::print_info() const
{
  LOG_INFO("database schema", "database_schema", *this);
}

OB_DEF_SERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_,
              database_id_, schema_version_, database_name_, replica_num_,
              primary_zone_, comment_, charset_type_, collation_type_, name_case_mode_, read_only_,
              default_tablegroup_id_, default_tablegroup_name_);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("func_SERIALIZE failed", K(ret));
  } else if (OB_FAIL(serialize_string_array(buf, buf_len, pos, zone_list_))) {
    LOG_WDIAG("serialize_string_array failed", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatabaseSchema)
{
  int ret = OB_SUCCESS;
  ObString database_name;
  ObString comment;
  ObString default_tablegroup_name;
  ObString primary_zone;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_,
              database_id_, schema_version_, database_name, replica_num_,
              primary_zone, comment, charset_type_, collation_type_, name_case_mode_, read_only_,
              default_tablegroup_id_, default_tablegroup_name);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(set_database_name(database_name))) {
    LOG_WDIAG("set_tenant_name failed", K(ret));
  } else if (OB_FAIL(set_primary_zone(primary_zone))) {
    LOG_WDIAG("set_primary_zone failed", K(ret));
  } else if (OB_FAIL(set_comment(comment))) {
    LOG_WDIAG("set_comment failed", K(ret));
  } else if (OB_FAIL(set_default_tablegroup_name(default_tablegroup_name))) {
    LOG_WDIAG("set_comment failed", K(ret));
  } else if (OB_FAIL(deserialize_string_array(buf, data_len, pos, zone_list_))) {
    LOG_WDIAG("deserialize_string_array failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatabaseSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_id_, database_id_, schema_version_,
              database_name_, replica_num_, primary_zone_,
              comment_, charset_type_, collation_type_,
              name_case_mode_, read_only_, default_tablegroup_id_,
              default_tablegroup_name_);

  len += get_string_array_serialize_size(zone_list_);
  return len;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObTablegroupSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/

ObTablegroupSchema::ObTablegroupSchema()
    : ObSchema(),
      tenant_id_(OB_INVALID_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(0),
      tablegroup_name_(),
      comment_()
{
}

ObTablegroupSchema::ObTablegroupSchema(common::ObIAllocator *allocator)
    : ObSchema(allocator),
      tenant_id_(OB_INVALID_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(0),
      tablegroup_name_(),
      comment_()
{
}

ObTablegroupSchema::ObTablegroupSchema(const ObTablegroupSchema &src_schema)
    : ObSchema(),
      tenant_id_(OB_INVALID_ID),
      tablegroup_id_(OB_INVALID_ID),
      schema_version_(0),
      tablegroup_name_(),
      comment_()
{
  *this = src_schema;
}

ObTablegroupSchema::~ObTablegroupSchema()
{
}

ObTablegroupSchema &ObTablegroupSchema::operator =(const ObTablegroupSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    tablegroup_id_ = src_schema.tablegroup_id_;
    schema_version_ = src_schema.schema_version_;

    if (OB_FAIL(deep_copy_str(src_schema.tablegroup_name_, tablegroup_name_))) {
      LOG_WDIAG("Fail to deep copy tablegroup name, ", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_schema.comment_, comment_))) {
      LOG_WDIAG("Fail to deep copy comment, ", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

ObTenantTablegroupId ObTablegroupSchema::get_tenant_tablegroup_id() const
{
  return ObTenantTablegroupId(tenant_id_, tablegroup_id_);
}

int64_t ObTablegroupSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += tablegroup_name_.length() + 1;
  convert_size += comment_.length() + 1;
  return convert_size;
}

bool ObTablegroupSchema::is_valid() const
{
  return ObSchema::is_valid() && OB_INVALID_ID != tenant_id_
      && OB_INVALID_ID != tablegroup_id_ && schema_version_ > 0;
}

void ObTablegroupSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  tablegroup_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  tablegroup_name_.reset();
  comment_.reset();
}

void ObTablegroupSchema::print_info() const
{
  LOG_INFO(
      "ObTablegroupSchema:",
      K_(tenant_id),
      K_(tablegroup_id),
      K_(schema_version),
      K_(tablegroup_name),
      K_(comment));
}

OB_DEF_SERIALIZE(ObTablegroupSchema)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, tablegroup_id_, schema_version_,
      tablegroup_name_, comment_);
  return ret;
}


OB_DEF_DESERIALIZE(ObTablegroupSchema)
{
  int ret = OB_SUCCESS;
  ObString tablegroup_name;
  ObString comment;

  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, tablegroup_id_, schema_version_,
      tablegroup_name, comment);

  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(tablegroup_name, tablegroup_name_))) {
    LOG_WDIAG("Fail to deep copy tablegroup name, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(comment, comment_))) {
    LOG_WDIAG("Fail to deep copy comment, ", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTablegroupSchema)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, tablegroup_id_, schema_version_,
      tablegroup_name_, comment_);
  return len;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObPartitionOption-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObPartitionOption::ObPartitionOption()
    : ObSchema(),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_func_expr_(),
      part_num_(1)
{
}

ObPartitionOption::ObPartitionOption(ObIAllocator *allocator)
    : ObSchema(allocator),
      part_func_type_(PARTITION_FUNC_TYPE_HASH),
      part_func_expr_(),
      part_num_(1)
{
}

ObPartitionOption::~ObPartitionOption()
{
}

ObPartitionOption::ObPartitionOption(const ObPartitionOption &expr)
    : ObSchema(), part_func_type_(PARTITION_FUNC_TYPE_HASH), part_num_(1)
{
  *this = expr;
}

ObPartitionOption &ObPartitionOption::operator =(const ObPartitionOption &expr)
{
  if (this != &expr) {
    reset();
    int ret = OB_SUCCESS;

    part_num_ = expr.part_num_;
    part_func_type_ = expr.part_func_type_;
    if (OB_FAIL(deep_copy_str(expr.part_func_expr_, part_func_expr_))) {
      LOG_WDIAG("Fail to deep copy part func expr, ", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }

  }
  return *this;
}

bool ObPartitionOption::operator ==(const ObPartitionOption &expr) const
{
  return (part_func_type_ == expr.part_func_type_)
      && (part_num_ == expr.part_num_)
      && (part_func_expr_ == expr.part_func_expr_);
}

bool ObPartitionOption::operator !=(const ObPartitionOption &expr) const
{
  return !(*this == expr);
}

void ObPartitionOption::reset()
{
  part_func_type_ = PARTITION_FUNC_TYPE_HASH;
  part_num_ = 1;
  reset_string(part_func_expr_);
  ObSchema::reset();
}

int64_t ObPartitionOption::get_convert_size() const
{
  return sizeof(*this) + part_func_expr_.length() + 1;
}

bool ObPartitionOption::is_valid() const
{
  return ObSchema::is_valid() && part_num_ > 0;
}

OB_DEF_SERIALIZE(ObPartitionOption)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, part_func_type_,
              part_func_expr_, part_num_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPartitionOption)
{
  int ret = OB_SUCCESS;
  ObString part_func_expr;

  LST_DO_CODE(OB_UNIS_DECODE, part_func_type_, part_func_expr, part_num_);

  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(part_func_expr, part_func_expr_))) {
    LOG_WDIAG("Fail to deep copy part_func_expr, ", K(ret));
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPartitionOption)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, part_func_type_, part_func_expr_, part_num_);
  return len;
}

/*-------------------------------------------------------------------------------------------------
 * ------------------------------ObViewSchema-------------------------------------------
 ----------------------------------------------------------------------------------------------------*/
ObViewSchema::ObViewSchema()
    : ObSchema(),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false)
{
}

ObViewSchema::ObViewSchema(ObIAllocator *allocator)
    : ObSchema(allocator),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false)
{
}

ObViewSchema::~ObViewSchema()
{
}

ObViewSchema::ObViewSchema(const ObViewSchema &src_schema)
    : ObSchema(),
      view_definition_(),
      view_check_option_(VIEW_CHECK_OPTION_NONE),
      view_is_updatable_(false)
{
  *this = src_schema;
}

ObViewSchema &ObViewSchema::operator =(const ObViewSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;

    view_check_option_ = src_schema.view_check_option_;
    view_is_updatable_ = src_schema.view_is_updatable_;

    if (OB_FAIL(deep_copy_str(src_schema.view_definition_, view_definition_))) {
      LOG_WDIAG("Fail to deep copy view definition, ", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObViewSchema::operator==(const ObViewSchema &other) const
{
  return view_definition_ == other.view_definition_
      && view_check_option_ == other.view_check_option_
      && view_is_updatable_ == other.view_is_updatable_;
}

bool ObViewSchema::operator!=(const ObViewSchema &other) const
{
  return !(*this == other);
}

int64_t ObViewSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(*this);
  convert_size += view_definition_.length() + 1;

  return convert_size;
}

bool ObViewSchema::is_valid() const
{
  return ObSchema::is_valid() && !view_definition_.empty();
}

void ObViewSchema::reset()
{
  ObSchema::reset();
  reset_string(view_definition_);
  view_check_option_ = VIEW_CHECK_OPTION_NONE;
  view_is_updatable_ = false;
}

OB_DEF_SERIALIZE(ObViewSchema)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              view_definition_,
              view_check_option_,
              view_is_updatable_);
  return ret;
}

OB_DEF_DESERIALIZE(ObViewSchema)
{
  int ret = OB_SUCCESS;
  ObString definition;

  LST_DO_CODE(OB_UNIS_DECODE,
              definition,
              view_check_option_,
              view_is_updatable_);

  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data, ", K(ret));
  } else if (OB_FAIL(deep_copy_str(definition, view_definition_))) {
    LOG_WDIAG("Fail to deep copy view definition, ", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObViewSchema)
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              view_definition_,
              view_check_option_,
              view_is_updatable_);
  return len;
}

const char *ob_view_check_option_str(const ViewCheckOption option)
{
  const char *ret = "invalid";
  const char *option_ptr[] =
  { "none", "local", "cascaded" };
  if (option >= 0 && option < VIEW_CHECK_OPTION_MAX) {
    ret = option_ptr[option];
  }
  return ret;
}

const char *ob_index_status_str(ObIndexStatus status)
{
  const char *ret = "invalid";
  const char *status_ptr[] =
  { "not_found", "unavailable", "available", "unique_checking", "unique_inelegible", "index_error" };
  if (status >= 0 && status < INDEX_STATUS_MAX) {
    ret = status_ptr[status];
  }
  return ret;
}

ObTenantTableId &ObTenantTableId::operator =(const ObTenantTableId &tenant_table_id)
{
  tenant_id_ = tenant_table_id.tenant_id_;
  table_id_ = tenant_table_id.table_id_;
  return *this;
}

/*************************For managing Privileges****************************/
//ObTenantUserId
OB_SERIALIZE_MEMBER(ObTenantUserId,
                    tenant_id_,
                    user_id_);

//ObPrintPrivSet
DEF_TO_STRING(ObPrintPrivSet)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF("\"");
  if ((priv_set_ & OB_PRIV_ALTER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_ALTER,");
  }
  if ((priv_set_ & OB_PRIV_CREATE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_USER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_USER,");
  }
  if ((priv_set_ & OB_PRIV_DELETE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_DELETE,");
  }
  if ((priv_set_ & OB_PRIV_DROP) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_DROP,");
  }
  if ((priv_set_ & OB_PRIV_GRANT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_GRANT_OPTION,");
  }
  if ((priv_set_ & OB_PRIV_INSERT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_INSERT,");
  }
  if ((priv_set_ & OB_PRIV_UPDATE) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_UPDATE,");
  }
  if ((priv_set_ & OB_PRIV_SELECT) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SELECT,");
  }
  if ((priv_set_ & OB_PRIV_INDEX) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_INDEX,");
  }
  if ((priv_set_ & OB_PRIV_CREATE_VIEW) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_CREATE_VIEW,");
  }
  if ((priv_set_ & OB_PRIV_SHOW_VIEW) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SHOW_VIEW,");
  }
  if ((priv_set_ & OB_PRIV_SHOW_DB) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SHOW_DB,");
  }
  if ((priv_set_ & OB_PRIV_SUPER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_SUPER,");
  }
  if ((priv_set_ & OB_PRIV_SUPER) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_PROCESS,");
  }
  if ((priv_set_ & OB_PRIV_BOOTSTRAP) && OB_SUCCESS == ret) {
    ret = BUF_PRINTF("PRIV_BOOTSTRAP,");
  }
  if (OB_SUCCESS == ret && pos > 1) {
    pos--; //Delete last ','
  }
  ret = BUF_PRINTF("\"");
  return pos;
}

//ObPriv

ObPriv& ObPriv::operator=(const ObPriv &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    schema_version_ = other.schema_version_;
    priv_set_ = other.priv_set_;
  }
  return *this;
}

void ObPriv::reset()
{
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  schema_version_ = 1;
  priv_set_ = 0;
}

OB_SERIALIZE_MEMBER(ObPriv,
                    tenant_id_,
                    user_id_,
                    schema_version_,
                    priv_set_);

//ObUserInfo
ObUserInfo::ObUserInfo(ObIAllocator *allocator)
  : ObSchema(allocator),
    ObPriv(),
    user_name_(),
    host_(),
    passwd_(),
    info_(),
    locked_(false)
{
}

ObUserInfo::ObUserInfo(const ObUserInfo &other)
  : ObSchema(), ObPriv()
{
  *this = other;
}

ObUserInfo::~ObUserInfo()
{
}

ObUserInfo& ObUserInfo::operator=(const ObUserInfo &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);
    locked_ = other.locked_;

    if (OB_FAIL(deep_copy_str(other.user_name_, user_name_))) {
      LOG_WDIAG("Fail to deep copy user_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.host_, host_))) {
      LOG_WDIAG("Fail to deep copy host_name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.passwd_, passwd_))) {
      LOG_WDIAG("Fail to deep copy passwd", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.info_, info_))) {
      LOG_WDIAG("Fail to deep copy info", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObUserInfo::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObUserInfo::reset()
{
  user_name_.reset();
  host_.reset();
  passwd_.reset();
  info_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObUserInfo::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += user_name_.length() + 1;
  convert_size += host_.length() + 1;
  convert_size += passwd_.length() + 1;
  convert_size += info_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObUserInfo)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE,
              user_name_,
              host_,
              passwd_,
              info_,
              locked_);
  return ret;
}

OB_DEF_DESERIALIZE(ObUserInfo)
{
  int ret = OB_SUCCESS;
  ObString user_name;
  ObString host;
  ObString passwd;
  ObString info;

  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE,
              user_name,
              host,
              passwd,
              info,
              locked_);

  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(user_name, user_name_))) {
    LOG_WDIAG("Fail to deep copy user_name", K(user_name), K(ret));
  } else if (OB_FAIL(deep_copy_str(host, host_))) {
    LOG_WDIAG("Fail to deep copy host", K(host), K(ret));
  } else if (OB_FAIL(deep_copy_str(passwd, passwd_))) {
    LOG_WDIAG("Fail to deep copy host", K(passwd), K(ret));
  } else if (OB_FAIL(deep_copy_str(info, info_))) {
    LOG_WDIAG("Fail to deep copy host", K(passwd), K(ret));
  } else { }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObUserInfo)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              user_name_,
              host_,
              passwd_,
              info_,
              locked_);
  return len;
}

//ObDBPriv
ObDBPriv& ObDBPriv::operator=(const ObDBPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);
    sort_ = other.sort_;

    if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WDIAG("Fail to deep copy db", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObDBPriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObDBPriv::reset()
{
  ObSchema::reset();
  ObPriv::reset();
  db_.reset();
  sort_ = 0;
}

int64_t ObDBPriv::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += db_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObDBPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, sort_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDBPriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, sort_);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WDIAG("Fail to deep copy user_name", K(db), K(ret));
  } else {}
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDBPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, sort_);
  return len;
}

//ObTablePriv
ObTablePriv& ObTablePriv::operator=(const ObTablePriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    ObPriv::operator=(other);

    if (OB_FAIL(deep_copy_str(other.db_, db_))) {
      LOG_WDIAG("Fail to deep copy db", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.table_, table_))) {
      LOG_WDIAG("Fail to deep copy table", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObTablePriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObTablePriv::reset()
{
  db_.reset();
  table_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObTablePriv::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += db_.length() + 1;
  convert_size += table_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObTablePriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, db_, table_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTablePriv)
{
  int ret = OB_SUCCESS;
  ObString db;
  ObString table;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, db, table);
  if (!OB_SUCC(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(db, db_))) {
    LOG_WDIAG("Fail to deep copy user_name", K(db), K(ret));
  } else if (OB_FAIL(deep_copy_str(table, table_))) {
    LOG_WDIAG("Fail to deep copy user_name", K(table), K(ret));
  } else {}
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTablePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, db_, table_);
  return len;
}

int ObNeedPriv::deep_copy(const ObNeedPriv &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  priv_level_ = other.priv_level_;
  priv_set_ = other.priv_set_;
  is_sys_table_ = other.is_sys_table_;
  if (OB_FAIL(ob_write_string(allocator, other.db_, db_))) {
    LOG_WDIAG("Fail to deep copy db", K_(db), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.table_, table_))) {
    LOG_WDIAG("Fail to deep copy table", K_(table), K(ret));
  }
  return ret;
}

int ObStmtNeedPrivs::deep_copy(const ObStmtNeedPrivs &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  need_privs_.reset();
  if (OB_FAIL(need_privs_.reserve(other.need_privs_.count()))) {
    LOG_WDIAG("fail to reserve need prives size", K(ret), K(other.need_privs_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.need_privs_.count(); ++i) {
    const ObNeedPriv &priv_other = other.need_privs_.at(i);
    ObNeedPriv priv_new;
    if (OB_FAIL(priv_new.deep_copy(priv_other, allocator))) {
      LOG_WDIAG("Fail to deep copy ObNeedPriv", K(priv_new), K(ret));
    } else {
      need_privs_.push_back(priv_new);
    }
  }
  return ret;
}

const char *OB_PRIV_LEVEL_STR[OB_PRIV_MAX_LEVEL] =
{
  "INVALID_LEVEL",
  "USER_LEVEL",
  "DB_LEVEL",
  "TABLE_LEVEL",
  "DB_ACCESS_LEVEL"
};

const char *ob_priv_level_str(const ObPrivLevel grant_level)
{
  const char *ret = "Unknown";
  if (grant_level < OB_PRIV_MAX_LEVEL && grant_level > OB_PRIV_INVALID_LEVEL) {
    ret = OB_PRIV_LEVEL_STR[grant_level];
  }
  return ret;
}

//ObTableType=>const char* ;
const char *ob_table_type_str(ObTableType type)
{
  const char *type_ptr = "UNKNOWN";
  switch (type) {
  case SYSTEM_TABLE: {
      type_ptr = "SYSTEM TABLE";
      break;
    }
  case SYSTEM_VIEW: {
      type_ptr = "SYSTEM VIEW";
      break;
    }
  case VIRTUAL_TABLE: {
      type_ptr = "VIRTUAL TABLE";
      break;
    }
  case USER_TABLE: {
      type_ptr = "USER TABLE";
      break;
    }
  case USER_VIEW: {
      type_ptr = "USER VIEW";
      break;
    }
  case USER_INDEX: {
      type_ptr = "USER INDEX";
      break;
    }
  case TMP_TABLE: {
      type_ptr = "TMP TABLE";
      break;
    }
  default: {
      break;
    }
  }
  return type_ptr;
}

//ObTableType => mysql table type str : SYSTEM VIEW, BASE TABLE, VIEW
const char *ob_mysql_table_type_str(ObTableType type)
{
  const char *type_ptr = "UNKNOWN";
  switch (type) {
    case SYSTEM_TABLE:
    case USER_TABLE:
      type_ptr = "BASE TABLE";
      break;
    case USER_VIEW:
      type_ptr = "VIEW";
      break;
    case SYSTEM_VIEW:
      type_ptr = "SYSTEM VIEW";
      break;
    case VIRTUAL_TABLE:
      type_ptr = "VIRTUAL TABLE";
      break;
    case USER_INDEX:
      type_ptr = "USER INDEX";
      break;
    case TMP_TABLE:
      type_ptr = "TMP TABLE";
      break;
    default:
      LOG_WDIAG("unkonw table type", K(type));
      break;
  }
  return type_ptr;
}

ObTableType get_inner_table_type_by_id(const uint64_t tid) {
  if (!is_inner_table(tid)) {
    LOG_WDIAG("tid is not inner table", K(tid));
  }
  ObTableType type = MAX_TABLE_TYPE;
  if (is_sys_table(tid)) {
    type = SYSTEM_TABLE;
  } else if (is_virtual_table(tid)) {
    type = VIRTUAL_TABLE;
  } else if (is_sys_view(tid)) {
    type = SYSTEM_VIEW;
  } else {
    // 30001 ~ 50000
    // MAX_TABLE_TYPE;
  }
  return type;
}

const char *schema_type_str(const ObSchemaType schema_type)
{
  const char *str = "";
  if (TENANT_SCHEMA == schema_type) {
    str = "tenant_schema";
  } else if (USER_SCHEMA == schema_type) {
    str = "user_schema";
  } else if (DATABASE_SCHEMA == schema_type) {
    str = "database_schema";
  } else if (TABLEGROUP_SCHEMA == schema_type) {
    str = "tablegroup_schema";
  } else if (TABLE_SCHEMA == schema_type) {
    str = "table_schema";
  } else if (DATABASE_PRIV == schema_type) {
    str = "database_priv";
  } else if (TABLE_PRIV == schema_type) {
    str = "table_priv";
  } else if (OUTLINE_SCHEMA == schema_type) {
    str = "outline_schema";
  }
  return str;
}

bool is_normal_schema(const ObSchemaType schema_type)
{
  return schema_type == TENANT_SCHEMA ||
      schema_type == USER_SCHEMA ||
      schema_type == DATABASE_SCHEMA ||
      schema_type == TABLEGROUP_SCHEMA ||
      schema_type == TABLE_SCHEMA ||
      schema_type == OUTLINE_SCHEMA;
}

#if 0
//------Funcs of outlineinfo-----//
ObTenantOutlineId &ObTenantOutlineId::operator =(const ObTenantOutlineId &tenant_outline_id)
{
  tenant_id_ = tenant_outline_id.tenant_id_;
  outline_id_ = tenant_outline_id.outline_id_;
  return *this;
}
#endif

ObOutlineInfo::ObOutlineInfo() : ObSchema()
{
  reset();
}

ObOutlineInfo::ObOutlineInfo(common::ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObOutlineInfo::ObOutlineInfo(const ObOutlineInfo &src_info) : ObSchema()
{
  reset();
  *this = src_info;
}

ObOutlineInfo::~ObOutlineInfo() {}

ObOutlineInfo &ObOutlineInfo::operator=(const ObOutlineInfo &src_info)
{
  if (this != &src_info) {
    reset();
    int ret = OB_SUCCESS;
    tenant_id_ = src_info.tenant_id_;
    database_id_ = src_info.database_id_;
    outline_id_ = src_info.outline_id_;
    schema_version_ = src_info.schema_version_;
    used_ = src_info.used_;
    compatible_ = src_info.compatible_;
    enabled_ = src_info.enabled_;
    format_ = src_info.format_;
    if (OB_FAIL(deep_copy_str(src_info.name_, name_))) {
      LOG_WDIAG("Fail to deep copy name", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.signature_, signature_))) {
      LOG_WDIAG("Fail to deep copy signature", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.outline_content_, outline_content_))) {
      LOG_WDIAG("Fail to deep copy outline_content", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.sql_text_, sql_text_))) {
      LOG_WDIAG("Fail to deep copy sql_text", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.owner_, owner_))) {
      LOG_WDIAG("Fail to deep copy sql_text", K(ret));
    } else if (OB_FAIL(deep_copy_str(src_info.version_, version_))) {
      LOG_WDIAG("Fail to deep copy sql_text", K(ret));
    } else {/*do noghing*/}
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

void ObOutlineInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  outline_id_ = OB_INVALID_ID;
  schema_version_ = 0;
  reset_string(name_);
  reset_string(signature_);
  reset_string(outline_content_);
  reset_string(sql_text_);
  reset_string(owner_);
  used_ = false;
  reset_string(version_);
  compatible_ = true;
  enabled_ = true;
  format_ = HINT_NORMAL;
  ObSchema::reset();
}

bool ObOutlineInfo::is_valid() const
{
  bool valid_ret = true;
  if (!ObSchema::is_valid()) {
    valid_ret = false;
  } else if (name_.empty() || signature_.empty() || outline_content_.empty() || sql_text_.empty()
             || owner_.empty() || version_.empty()) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_ || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else if (schema_version_ <= 0) {
    valid_ret = false;
  } else {/*do nothing*/}
  return valid_ret;
}

bool ObOutlineInfo::is_valid_for_replace() const
{
  bool valid_ret = true;
  if (!ObSchema::is_valid()) {
    valid_ret = false;
  } else if (name_.empty() || signature_.empty() || outline_content_.empty() || sql_text_.empty()
             || owner_.empty() || version_.empty()) {
    valid_ret = false;
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_ || OB_INVALID_ID == outline_id_) {
    valid_ret = false;
  } else {/*do nothing*/}
  return valid_ret;
}

int64_t ObOutlineInfo::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObOutlineInfo);
  convert_size += name_.length() + 1;
  convert_size += signature_.length() + 1;
  convert_size += outline_content_.length() + 1;
  convert_size += sql_text_.length() + 1;
  convert_size += owner_.length() + 1;
  convert_size += version_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObOutlineInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, database_id_, outline_id_, schema_version_,
      name_, signature_, outline_content_, sql_text_, owner_, used_, version_, compatible_,
      enabled_, format_);
  return ret;
}


OB_DEF_DESERIALIZE(ObOutlineInfo)
{
  int ret = OB_SUCCESS;
  ObString name;
  ObString signature;
  ObString outline_content;
  ObString sql_text;
  ObString owner;
  ObString version;

  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, database_id_, outline_id_, schema_version_,
      name, signature, outline_content, sql_text, owner, used_, version, compatible_,
      enabled_, format_);

  if (OB_FAIL(ret)) {
    LOG_WDIAG("Fail to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(name, name_))) {
    LOG_WDIAG("Fail to deep copy outline name", K(ret));
  } else if (OB_FAIL(deep_copy_str(signature, signature_))) {
    LOG_WDIAG("Fail to deep copy signature", K(ret));
  } else if (OB_FAIL(deep_copy_str(outline_content, outline_content_))) {
    LOG_WDIAG("Fail to deep copy outline_content", K(ret));
  } else if (OB_FAIL(deep_copy_str(sql_text, sql_text_))) {
    LOG_WDIAG("Fail to deep copy sql_text", K(ret));
  } else if (OB_FAIL(deep_copy_str(owner, owner_))) {
    LOG_WDIAG("Fail to deep copy sql_text", K(ret));
  } else if (OB_FAIL(deep_copy_str(version, version_))) {
    LOG_WDIAG("Fail to deep copy sql_text", K(ret));
  } else {/*do nothing*/}

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOutlineInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tenant_id_, database_id_, outline_id_, schema_version_,
      name_, signature_, outline_content_, sql_text_, owner_, used_, version_, compatible_,
      enabled_, format_);
  return len;
}

//------end of funcs of outlineinfo-----//
} //namespace schema
} //namespace share
} //namespace oceanbase

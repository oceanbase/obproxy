#define USING_LOG_PREFIX PROXY

#include "proxy/route/ob_index_entry.h"
#include "proxy/route/ob_index_cache.h"
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
int ObIndexEntry::init(char *buf_start, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf_start)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf_len), K(buf_start), K(ret));
  } else {
    create_time_us_ = ObTimeUtility::current_time();
    buf_len_ = buf_len;
    buf_start_ = buf_start;
    is_inited_ = true;
  }

  return ret;
}

void ObIndexEntry::free()
{
  LOG_DEBUG("ObIndexEntry will be free", K(*this));
  int64_t total_len = sizeof(ObIndexEntry) + buf_len_;
  buf_start_ = NULL;
  buf_len_ = 0;
  op_fixed_mem_free(this, total_len);
}

int ObIndexEntry::set_names(const ObIndexEntryName &name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(name), K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(name_.deep_copy(name, buf_start_, buf_len_))) {
    LOG_WDIAG("fail to deep copy table entry names", K(ret));
  } else {
    // success
  }
  return ret;
}

int ObIndexEntry::generate_index_table_name()
{
  int ret = OB_SUCCESS;

  if (!name_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("index name is invalid", K(ret));
  } else {
    int len = 0;
    int64_t buf_len = OB_MAX_INDEX_TABLE_NAME_LENGTH;

    // @return "__idx" + "_" + "table_id" + "_" + "index_name"
    len = snprintf(index_table_name_buf_, buf_len, "__idx_%lu_%.*s", data_table_id_, name_.index_name_.length(), name_.index_name_.ptr());

    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill index table name", K_(data_table_id), K_(name_.index_name), K(ret));
    } else {
      index_table_name_.assign(index_table_name_buf_, len);
    }
  }

  return ret;
}

int ObIndexEntry::alloc_and_init_index_entry(
    const ObIndexEntryName &name,
    const int64_t cr_version,
    const int64_t cr_id,
    ObIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!name.is_valid() || cr_version < 0 || NULL != entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(name), K(cr_version), K(entry), K(ret));
  } else {
    int64_t name_size = name.get_total_str_len();
    int64_t obj_size = sizeof(ObIndexEntry);
    int64_t alloc_size =  name_size + obj_size;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc entry succ", K(alloc_size), K(obj_size), K(name_size), K(name));
      entry = new (buf) ObIndexEntry();
      if (OB_FAIL(entry->init(buf + obj_size, name_size))) {
        LOG_WDIAG("fail to init entry", K(alloc_size), K(ret));
      } else if (OB_FAIL(entry->set_names(name))) {
        LOG_WDIAG("fail to set name", K(name), K(ret));
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
      alloc_size = 0;
    }
  }
  return ret;
}

int64_t ObIndexEntry::to_string(char *buf, const int64_t buf_len) const
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
       K_(index_type),
       K_(table_id),
       K_(data_table_id),
       K_(name));
  J_OBJ_END();
  return pos;
}

int64_t ObIndexEntryName::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name),
       K_(tenant_name),
       K_(database_name),
       K_(index_name),
       K_(table_name));
  J_OBJ_END();
  return pos;
}

int ObIndexEntryName::deep_copy(const ObIndexEntryName &name,
                                char *buf_start,
                                const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_start) || (buf_len < name.get_total_str_len())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(buf_start), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!name.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("this table entry name is not valid", K(name), K(ret));
  } else {
    int64_t total_len = name.get_total_str_len();
    int64_t pos = 0;
    int64_t len = name.cluster_name_.length();
    MEMCPY(buf_start, name.cluster_name_.ptr(), len);
    cluster_name_.assign(buf_start, static_cast<int32_t>(len));
    pos += len;

    len = name.tenant_name_.length();
    MEMCPY(buf_start + pos, name.tenant_name_.ptr(), len);
    tenant_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.database_name_.length();
    MEMCPY(buf_start + pos, name.database_name_.ptr(), len);
    database_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.index_name_.length();
    MEMCPY(buf_start + pos, name.index_name_.ptr(), len);
    index_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    len = name.table_name_.length();
    MEMCPY(buf_start + pos, name.table_name_.ptr(), len);
    table_name_.assign(buf_start + pos, static_cast<int32_t>(len));
    pos += len;

    if (OB_UNLIKELY(pos != total_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to deep copy", K(pos), K(total_len), K(*this), K(ret));
    } else {
      LOG_DEBUG("succ deep copy ObIndexEntryName", K(name), K(*this), K(total_len));
    }
  }

  return ret;
}

int64_t ObIndexEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(name), K_(cr_version), K_(cr_id));
  J_OBJ_END();
  return pos;
}


}
}
}
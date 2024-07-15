#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_tablegroup_entry.h"
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
void ObTableGroupEntry::free()
{
  LOG_DEBUG("ObTableGroupEntry will be free", K(*this));
  buf_ = NULL;
  op_fixed_mem_free(this, buf_len_);
}

int ObTableGroupEntry::alloc_and_init_tablegroup_entry(const int64_t tenant_id, const ObString &tablegroup_name,
                                                       const ObString &database_name, const ObString &sharding,
                                                       const ObIArray<ObTableGroupTableNameInfo> &table_names,
                                                       const int64_t cr_version, const int64_t cr_id, ObTableGroupEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID || cr_version < 0 || NULL != entry
    || tablegroup_name.empty() || database_name.empty() || sharding.empty() || table_names.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(tenant_id), K(cr_version), K(entry),
              K(tablegroup_name), K(database_name), K(sharding), K(table_names), K(ret));
  } else {
    int64_t alloc_size = sizeof(ObTableGroupEntry);
    alloc_size += tablegroup_name.length();
    alloc_size += database_name.length();
    alloc_size += sharding.length();
    for (int i = 0; i < table_names.count(); ++i) {
      alloc_size += table_names.at(i).table_name_length_;
    }
    char *buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
    } else {
      LOG_DEBUG("alloc tablegroup entry succ", K(alloc_size));
      entry = new (buf) ObTableGroupEntry();
      if (OB_FAIL(entry->init(tenant_id, tablegroup_name, database_name, sharding, table_names, buf, alloc_size))) {
        LOG_WDIAG("fail to init ObTableGroupEntry", K(ret));
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

int ObTableGroupEntry::init(const int64_t tenant_id, const ObString &tablegroup_name,
                            const ObString &database_name, const ObString &sharding,
                            const ObIArray<ObTableGroupTableNameInfo> &table_names,
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
    int64_t pos = sizeof(ObTableGroupEntry);
    int64_t len = tablegroup_name.length();
    MEMCPY(buf + pos, tablegroup_name.ptr(), len);
    tablegroup_name_.assign(buf + pos, static_cast<int32_t>(len));
    pos += len;

    len = database_name.length();
    MEMCPY(buf + pos, database_name.ptr(), len);
    database_name_.assign(buf + pos, static_cast<int32_t>(len));
    pos += len;

    len = sharding.length();
    MEMCPY(buf + pos, sharding.ptr(), len);
    sharding_.assign(buf + pos, static_cast<int32_t>(len));
    pos += len;

    for (int i = 0; i < table_names.count() && OB_SUCC(ret); ++i) {
      ObString table_name;
      len = table_names.at(i).table_name_length_;
      MEMCPY(buf + pos, table_names.at(i).table_name_, len);
      table_name.assign(buf + pos, static_cast<int32_t>(len));
      pos += len;

      if (OB_FAIL(table_names_.push_back(table_name))) {
        LOG_WDIAG("fail to push_back table_name into table_names", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(pos != buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to copy string into buf", K(pos), K(buf_len), K(*this), K(ret));
      } else {
        tenant_id_ = tenant_id;
        create_time_us_ = ObTimeUtility::current_time();
        buf_len_ = buf_len;
        buf_ = buf;
        is_inited_ = true;
        LOG_DEBUG("succ to init ObTableGroupEntry", K(*this));
      }
    }
  }

  return ret;
}

int64_t ObTableGroupEntry::to_string(char *buf, const int64_t buf_len) const
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
       K_(tablegroup_name),
       K_(database_name),
       K_(table_names),
       K_(sharding));
  J_OBJ_END();
  return pos;
}

int64_t ObTableGroupEntryKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tenant_id), K_(tablegroup_name), K_(database_name), K_(cr_version), K_(cr_id));
  J_OBJ_END();
  return pos;
}


}
}
}
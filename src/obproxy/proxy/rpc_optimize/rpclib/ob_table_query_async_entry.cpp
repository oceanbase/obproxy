#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
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

int ObTableQueryAsyncEntry::init(int64_t query_session_id)
{
  int ret = OB_SUCCESS;

  client_query_session_id_ = query_session_id;
  first_query_ = true;

  return ret;
}

int ObTableQueryAsyncEntry::allocate(ObTableQueryAsyncEntry *&query_async_entry)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObTableQueryAsyncEntry);
  void *buf = op_fixed_mem_alloc(alloc_size);

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to alloc mem", K(alloc_size), K(ret));
  } else {
    query_async_entry = new (buf) ObTableQueryAsyncEntry();
    query_async_entry->total_len_ = alloc_size;
    query_async_entry->inc_ref();
    query_async_entry->set_avail_state();

    LOG_DEBUG("ObTableQueryAsyncEntry will be alloc", KPC(query_async_entry));
  }
  if (OB_FAIL(ret) && (NULL != buf)) {
    op_fixed_mem_free(buf, alloc_size);
    query_async_entry = NULL;
    alloc_size = 0;
  }

  return ret;
}

inline void ObTableQueryAsyncEntry::free()
{
  LOG_DEBUG("ObTableQueryAsyncEntry will be free", K(*this));
  reset();
  op_fixed_mem_free(this, total_len_);
}

int ObTableQueryAsyncEntry::get_current_tablet_id(int64_t &tablet_id) const
{
  int ret = OB_SUCCESS;

  if (!partition_table_ || current_position_ >= tablet_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to get_current_tablet_id", K_(partition_table), K_(current_position), K(tablet_ids_.count()));
  } else {
    tablet_id = tablet_ids_.at(current_position_);
  }

  return ret;
}

}
}
}
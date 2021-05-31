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
#include "proxy/mysql/ob_cursor_struct.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

DEF_TO_STRING(ObCursorIdAddr)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(cursor_id), K_(addr));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObCursorIdPair)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(client_cursor_id), K_(server_cursor_id));
  J_OBJ_END();
  return pos;
}

int ObCursorIdAddr::alloc_cursor_id_addr(uint32_t cursor_id, const struct sockaddr &addr, ObCursorIdAddr *&cursor_id_addr)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t alloc_size = sizeof(ObCursorIdAddr);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for cursor id entry", K(alloc_size), K(ret));
  } else {
    cursor_id_addr = new (buf) ObCursorIdAddr(cursor_id, addr);
  }
  return ret;
}

void ObCursorIdAddr::destroy()
{
  LOG_INFO("cursor id addr will be destroyed", KPC(this));
  int64_t total_len = sizeof(ObCursorIdAddr);
  op_fixed_mem_free(this, total_len);
}

int ObCursorIdPair::alloc_cursor_id_pair(uint32_t client_cursor_id, uint32_t server_cursor_id, ObCursorIdPair *&cursor_id_pair)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t alloc_size = sizeof(ObCursorIdPair);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for cursor id pair", K(alloc_size), K(ret));
  } else {
    cursor_id_pair = new (buf) ObCursorIdPair(client_cursor_id, server_cursor_id);
  }
  return ret;
}

void ObCursorIdPair::destroy()
{
  LOG_INFO("cursor id pair will be destroyed", KPC(this));
  int64_t total_len = sizeof(ObCursorIdPair);
  op_fixed_mem_free(this, total_len);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

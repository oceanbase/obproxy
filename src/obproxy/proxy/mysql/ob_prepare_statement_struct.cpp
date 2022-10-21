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
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/mysql/ob_mysql_client_session.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

DEF_TO_STRING(ObPsIdAddrs)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(ps_id), K_(addrs));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObPsSqlMeta)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(column_count), K_(param_count), K_(param_types),
       KP_(param_type), K_(param_type_len));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObPsEntry)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), "ps_sql_len", base_ps_sql_.length(), "ps_sql", ObProxyMysqlRequest::get_print_sql(base_ps_sql_),
       K_(base_ps_parse_result));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObPsIdEntry)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(ps_id), KPC_(ps_entry), K_(ps_meta));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObPsIdPair)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(client_ps_id), K_(server_ps_id));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObTextPsEntry)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), "text_ps_sql_len", base_ps_sql_.length(),
      "text_ps_sql", ObProxyMysqlRequest::get_print_sql(base_ps_sql_),
      K_(base_ps_parse_result));
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObTextPsNameEntry)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(text_ps_name), KPC_(text_ps_entry), K_(version));
  J_OBJ_END();
  return pos;
}

void ObBasePsEntry::destroy() {
  if (NULL != ps_entry_cache_) {
    ps_entry_cache_->delete_base_ps_entry(this);
  }
  base_ps_parse_result_.reset();
}

int ObPsIdAddrs::alloc_ps_id_addrs(uint32_t ps_id, const struct sockaddr &addr, ObPsIdAddrs *&ps_id_addrs)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t alloc_size = sizeof(ObPsIdAddrs);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for cursor id entry", K(alloc_size), K(ret));
  } else {
    ps_id_addrs = new (buf) ObPsIdAddrs(ps_id);
    if (OB_FAIL(ps_id_addrs->add_addr(addr))) {
      LOG_WARN("set addr in ps_id_addrs failed", "addr", net::ObIpEndpoint(addr), K(ret));
      ps_id_addrs->destroy();
      ps_id_addrs = NULL;
    }
  }
  return ret;
}

int ObPsIdAddrs::add_addr(const struct sockaddr &socket_addr) {
  int ret = OB_SUCCESS;
  net::ObIpEndpoint addr(socket_addr);
  if (OB_FAIL(addrs_.set_refactored(addr))) {
    LOG_WARN("set refactored failed", K(addr), K(ret));
  }
  return ret;
}
int ObPsIdAddrs::remove_addr(const struct sockaddr &socket_addr) {
  int ret = OB_SUCCESS;
  net::ObIpEndpoint addr(socket_addr);
  if (OB_FAIL(addrs_.erase_refactored(addr))) {
    LOG_WARN("set refactored failed", K(addr), K(ret));
  }
  return ret;
}

void ObPsIdAddrs::destroy()
{
  LOG_INFO("ps id addrs will be destroyed", KPC(this));

  // release HashSet
  this->~ObPsIdAddrs();

  int64_t total_len = sizeof(ObPsIdAddrs);
  op_fixed_mem_free(this, total_len);
}

int ObPsIdEntry::alloc_ps_id_entry(uint32_t ps_id, ObPsEntry *ps_entry, ObPsIdEntry *&ps_id_entry)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t alloc_size = sizeof(ObPsIdEntry);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ps id entry", K(alloc_size), K(ret));
  } else {
    ps_id_entry = new (buf) ObPsIdEntry(ps_id, ps_entry);
    LOG_DEBUG("alloc ps id", K(ps_id));
  }
  return ret;
}

void ObPsIdEntry::destroy()
{
  LOG_INFO("ps id entry will be destroyed", KPC(this));
  ps_meta_.reset();
  int64_t total_len = sizeof(ObPsIdEntry);
  ps_entry_->dec_ref();
  ps_entry_ = NULL;
  op_fixed_mem_free(this, total_len);
}

int ObPsIdPair::alloc_ps_id_pair(uint32_t client_ps_id, uint32_t server_ps_id, ObPsIdPair *&ps_id_pair)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t alloc_size = sizeof(ObPsIdPair);
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ps id pair", K(alloc_size), K(ret));
  } else {
    ps_id_pair = new (buf) ObPsIdPair(client_ps_id, server_ps_id);
  }
  return ret;
}

void ObPsIdPair::destroy()
{
  LOG_INFO("ps id pair will be destroyed", KPC(this));
  int64_t total_len = sizeof(ObPsIdPair);
  op_fixed_mem_free(this, total_len);
}

int ObPsSqlMeta::set_param_type(const char *param_type, int64_t param_type_len)
{
  int ret = OB_SUCCESS;

  if (NULL != param_type_ && param_type_len_ > 0) {
    op_fixed_mem_free(param_type_, param_type_len_);
    param_type_ = NULL;
    param_type_len_ = 0;
  }

  if (OB_ISNULL(param_type_ = static_cast<char *>(op_fixed_mem_alloc(param_type_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc param type", K(param_type_len), K(ret));
  } else {
    memcpy(param_type_, param_type, param_type_len);
    param_type_len_ = param_type_len;
  }

  return ret;
}

int ObPsEntry::alloc_and_init_ps_entry(const ObString &ps_sql,
                                       const ObSqlParseResult &parse_result,
                                       ObPsEntry *&entry)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = 0;
  char *buf = NULL;

  int64_t obj_size = sizeof(ObPsEntry);
  int64_t sql_len = ps_sql.length() + PARSE_EXTRA_CHAR_NUM;

  alloc_size += sizeof(ObPsEntry) + sql_len;
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ps entry", K(alloc_size), K(ret));
  } else {
    entry = new (buf) ObPsEntry();
    if (OB_FAIL(entry->init(buf + obj_size, alloc_size - obj_size))) {
      LOG_WARN("fail to init ps entry", K(ret));
    } else if (OB_FAIL(entry->set_sql(ps_sql))) {
      LOG_WARN("fail to set ps sql", K(ret));
    } else {
      entry->set_base_ps_parse_result(parse_result);
    }
  }

  if (OB_FAIL(ret) && NULL != buf) {
    op_fixed_mem_free(buf, alloc_size);
    entry = NULL;
    alloc_size = 0;
  }
  return ret;
}

int ObPsEntry::init(char *buf_start, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(buf_start) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(buf_start), K(buf_len), K(ret));
  } else {
    buf_start_ = buf_start;
    buf_len_ = buf_len;
    is_inited_ = true;
  }
  return ret;
}

int ObPsEntry::set_sql(const ObString &ps_sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(ps_sql.length() + PARSE_EXTRA_CHAR_NUM > buf_len_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not enough", K_(buf_len), K(ps_sql), K(ret));
  } else {
    int64_t pos = 0;
    MEMCPY(buf_start_ + pos, ps_sql.ptr(), ps_sql.length());
    base_ps_sql_.assign(buf_start_ + pos, ps_sql.length());
    pos += ps_sql.length();
    MEMSET(buf_start_ + pos, 0, PARSE_EXTRA_CHAR_NUM);
  }
  return ret;
}

void ObPsEntry::destroy()
{
  LOG_INFO("ps entry will be destroyed", KPC(this));
  if (OB_LIKELY(is_inited_)) {
    ObBasePsEntry::destroy();
    is_inited_ = false;
    int64_t total_len = sizeof(ObPsEntry) + buf_len_;
    buf_start_ = NULL;
    buf_len_ = 0;
    op_fixed_mem_free(this, total_len);
  }
}

void ObBasePsEntryCache::destroy()
{
  ObBasePsEntryMap::iterator last = base_ps_map_.end();
  ObBasePsEntryMap::iterator tmp_iter;
  for (ObBasePsEntryMap::iterator base_ps_iter = base_ps_map_.begin(); base_ps_iter != last;) {
    tmp_iter = base_ps_iter;
    ++base_ps_iter;
    tmp_iter->destroy();
  }
  base_ps_map_.reset();
}

int init_ps_entry_cache_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_ps_entry_cache_for_one_thread(i))) {
      PROXY_NET_LOG(WARN, "fail to new ObBasePsEntryCache", K(i), K(ret));
    }
  }
  return ret;
}

int init_text_ps_entry_cache_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_text_ps_entry_cache_for_one_thread(i))) {
      PROXY_NET_LOG(WARN, "fail to new ObBasePsEntryCache", K(i), K(ret));
    }
  }
  return ret;
}

int init_ps_entry_cache_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_CALL][index]->ps_entry_cache_
    = new (std::nothrow) ObBasePsEntryCache())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObBasePsEntryCache", K(index), K(ret));
  }
  return ret;
}

int init_text_ps_entry_cache_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(g_event_processor.event_thread_[ET_CALL][index]->text_ps_entry_cache_
    = new (std::nothrow) ObBasePsEntryCache())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObBasePsEntryCache", K(index), K(ret));
  }
  return ret;
}

int ObTextPsEntry::alloc_and_init_text_ps_entry(const ObString &text_ps_sql,
                                                const ObSqlParseResult &parse_result,
                                                ObTextPsEntry *&entry)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = 0;
  char *buf = NULL;

  int64_t obj_size = sizeof(ObTextPsEntry);
  int64_t sql_len = text_ps_sql.length() + PARSE_EXTRA_CHAR_NUM;

  alloc_size = obj_size + sql_len;
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for text ps entry", K(alloc_size), K(ret));
  } else {
    entry = new (buf) ObTextPsEntry();
    if (OB_FAIL(entry->init(buf + obj_size, alloc_size - obj_size, text_ps_sql))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for text ps entry", K(alloc_size), K(ret));
    } else {
      entry->set_base_ps_parse_result(parse_result);
    }
  }

  if (OB_FAIL(ret) && NULL != buf) {
    op_fixed_mem_free(buf, alloc_size);
    entry = NULL;
    alloc_size = 0;
  }

  return ret;
}

int ObTextPsEntry::init(char *buf_start, int64_t buf_len, const ObString &text_ps_sql)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(buf_start) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(buf_start), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(text_ps_sql.length() + PARSE_EXTRA_CHAR_NUM > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not enough", K(buf_len), K(text_ps_sql), K(ret));
  } else {
    int64_t pos = 0;
    MEMCPY(buf_start, text_ps_sql.ptr(), text_ps_sql.length());
    pos = text_ps_sql.length();
    base_ps_sql_.assign(buf_start, text_ps_sql.length());
    MEMSET(buf_start + pos, 0, PARSE_EXTRA_CHAR_NUM);
    is_inited_ = true;
    buf_start_ = buf_start;
    buf_len_ = buf_len;
  }

  return ret;
}

void ObTextPsEntry::destroy()
{
  LOG_INFO("text ps entry will be destroyed", KPC(this));
  if (OB_LIKELY(is_inited_)) {
    ObBasePsEntry::destroy();
    is_inited_ = false;
    int64_t total_len = sizeof(ObTextPsEntry) + buf_len_;
    buf_start_ = NULL;
    buf_len_ = 0;
    op_fixed_mem_free(this, total_len);
  }
}

int ObTextPsNameEntry::alloc_text_ps_name_entry(const ObString &text_ps_name,
                                                ObTextPsEntry *text_ps_entry,
                                                ObTextPsNameEntry *&text_ps_name_entry)
{
  int ret = OB_SUCCESS;

  if (text_ps_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt name is empty", K(ret));
  } else {
    char *buf = NULL;
    int64_t alloc_size = sizeof(ObTextPsNameEntry) + text_ps_name.length();
    if (OB_ISNULL(buf = static_cast<char*>(op_fixed_mem_alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for text ps name entry", K(alloc_size), K(ret));
    } else {
      char *buf_start = buf + sizeof(ObTextPsNameEntry);
      MEMCPY(buf_start, text_ps_name.ptr(), text_ps_name.length());
      text_ps_name_entry = new (buf) ObTextPsNameEntry(buf_start, text_ps_name.length(), text_ps_entry);
    }
  }

  return ret;
}

void ObTextPsNameEntry::destroy()
{
  LOG_INFO("text ps name entry will be destroyed", KPC(this));
  int64_t total_len = sizeof(ObTextPsNameEntry) + text_ps_name_.length();
  text_ps_entry_->dec_ref();
  text_ps_entry_ = NULL;
  op_fixed_mem_free(this, total_len);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

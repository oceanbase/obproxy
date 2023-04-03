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

#ifndef OBPROXY_PREPARE_STATEMENT_STRUCT_H
#define OBPROXY_PREPARE_STATEMENT_STRUCT_H

#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "iocore/net/ob_inet.h"
#include "lib/allocator/ob_mod_define.h"
#include "obproxy/iocore/eventsystem/ob_buf_allocator.h"
#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_proxy_sql_parser.h"

#define PARAM_TYPE_BLOCK_SIZE  1 << 9 // 512

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace obproxy
{
namespace proxy
{
class ObMysqlClientSession;
// stored in client session info
class ObPsIdAddrs
{
public:
  static const int BUCKET_SIZE = 8;
  static const int NODE_NUM = 8;

  ObPsIdAddrs() : ps_id_(0), addrs_() {
  }
  ObPsIdAddrs(uint32_t ps_id) : ps_id_(ps_id), addrs_() {
  }
  ~ObPsIdAddrs() {};

  static int alloc_ps_id_addrs(uint32_t ps_id, const struct sockaddr &addr, ObPsIdAddrs *&ps_id_addrs);
  void destroy();
  int add_addr(const struct sockaddr &socket_addr);
  int remove_addr(const struct sockaddr &socket_addr);
  ObIArray<net::ObIpEndpoint> &get_addrs() { return addrs_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t ps_id_; // client ps id
  ObSEArray<net::ObIpEndpoint, 4> addrs_;
  LINK(ObPsIdAddrs, ps_id_addrs_link_);
};

// ps_id ----> ObPsIdAddrs
struct ObPsIdAddrsHashing
{
  typedef const uint32_t &Key;
  typedef ObPsIdAddrs Value;
  typedef ObDLList(ObPsIdAddrs, ps_id_addrs_link_) ListHead;

  static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(key), 0); }
  static Key key(Value const *value) { return value->ps_id_; }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

typedef common::hash::ObBuildInHashMap<ObPsIdAddrsHashing> ObPsIdAddrsMap;

class ObBasePsEntryCache;

class ObBasePsEntry : public common::ObSharedRefCount
{
public:
  ObBasePsEntry() : base_ps_sql_(), is_inited_(false), ps_entry_cache_(NULL),
                    buf_start_(NULL), buf_len_(0), base_ps_parse_result_() {}
  virtual ~ObBasePsEntry() {}
  obutils::ObSqlParseResult &get_base_ps_parse_result() { return base_ps_parse_result_; }
  void set_base_ps_parse_result(const obutils::ObSqlParseResult &parse_result)
  {
    base_ps_parse_result_ = parse_result;
  }

  void set_ps_entry_cache(ObBasePsEntryCache *ps_entry_cache)
  {
    ps_entry_cache_ = ps_entry_cache;
  }

  bool is_valid() const
  {
    return !base_ps_sql_.empty();
  }

  const common::ObString &get_base_ps_sql() { return base_ps_sql_; }

  virtual void free() {}
  virtual void destroy();

public:
  common::ObString base_ps_sql_;
  LINK(ObBasePsEntry, base_ps_entry_link_);

  // parser need extra two byte '\0'
  const static int64_t PARSE_EXTRA_CHAR_NUM = 2;

protected:
  bool is_inited_;
  ObBasePsEntryCache *ps_entry_cache_;
  char *buf_start_;
  int64_t buf_len_;
  obutils::ObSqlParseResult base_ps_parse_result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBasePsEntry);
};

class ObPsSqlMeta
{

public:
  ObPsSqlMeta()
  : param_count_(0), column_count_(0),
    param_types_(), param_type_(NULL), param_type_len_(0)
  {
    param_types_.set_mod_id(common::ObModIds::OB_PROXY_PARAM_TYPE_ARRAY);
    param_types_.set_block_size(PARAM_TYPE_BLOCK_SIZE);
  }
  ~ObPsSqlMeta() {}

  void reset();

  int64_t get_param_count() const { return param_count_; }
  int64_t get_column_count() const { return column_count_; }
  ObIArray<obmysql::EMySQLFieldType> &get_param_types() { return param_types_; }
  ObString get_param_type() { return ObString(param_type_len_, param_type_); }
  int set_param_type(const char *param_type, int64_t param_type_len);
  void set_param_count(int64_t param_count) { param_count_ = param_count; }
  void set_column_count(int64_t column_count) { column_count_ = column_count; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  int64_t param_count_;
  int64_t column_count_;
  common::ObArray<obmysql::EMySQLFieldType> param_types_;
  char *param_type_;
  int64_t param_type_len_;
  DISALLOW_COPY_AND_ASSIGN(ObPsSqlMeta);
};

inline void ObPsSqlMeta::reset()
{
  param_count_ = 0;
  column_count_ = 0;
  param_types_.reset();

  if (NULL != param_type_ && param_type_len_ > 0) {
    op_fixed_mem_free(param_type_, param_type_len_);
  }

  param_type_ = NULL;
  param_type_len_ = 0;
}

class ObPsEntry : public ObBasePsEntry
{
public:
  ObPsEntry() : ObBasePsEntry() {}
  ~ObPsEntry() {}

  template <typename T>
  static int alloc_and_init_ps_entry(const common::ObString &ps_sql,
                                     const obutils::ObSqlParseResult &parse_result,
                                     T *&entry);
  int init(char *buf_start, int64_t buf_len);
  virtual void free();
  virtual void destroy();
  int set_sql(const common::ObString &ps_sql);
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  const static int64_t PARSE_EXTRA_CHAR_NUM = 2;

public:
  DISALLOW_COPY_AND_ASSIGN(ObPsEntry);
};

class ObGlobalPsEntry : public ObPsEntry
{
public:
  ObGlobalPsEntry() : ObPsEntry() {}
  ~ObGlobalPsEntry() {}
  virtual void free();
  template <typename T>
  static int alloc_and_init_ps_entry(const common::ObString &ps_sql,
                                     const obutils::ObSqlParseResult &parse_result,
                                     T *&entry);
};

template <typename T>
int ObGlobalPsEntry::alloc_and_init_ps_entry(const ObString &ps_sql,
                                             const obutils::ObSqlParseResult &parse_result,
                                             T *&entry)
{
  return ObPsEntry::alloc_and_init_ps_entry(ps_sql, parse_result, entry);
}

template <typename T>
int ObPsEntry::alloc_and_init_ps_entry(const ObString &ps_sql,
                                       const obutils::ObSqlParseResult &parse_result,
                                       T *&entry)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = 0;
  char *buf = NULL;

  int64_t obj_size = sizeof(T);
  int64_t sql_len = ps_sql.length() + PARSE_EXTRA_CHAR_NUM;

  alloc_size += sizeof(T) + sql_len;
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(WARN, "fail to alloc mem for ps entry", K(alloc_size), K(ret));
  } else {
    entry = new (buf) T();
    if (OB_FAIL(entry->init(buf + obj_size, alloc_size - obj_size))) {
      PROXY_SM_LOG(WARN, "fail to init ps entry", K(ret));
    } else if (OB_FAIL(entry->set_sql(ps_sql))) {
      PROXY_SM_LOG(WARN, "fail to set ps sql", K(ret));
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

// stored in client session info
class ObPsIdEntry
{
public:
  ObPsIdEntry() : ps_id_(0), ps_entry_(NULL), ps_meta_() {}
  ObPsIdEntry(uint32_t client_id, ObPsEntry *entry)
      : ps_id_(client_id), ps_entry_(entry) {
  }
  ~ObPsIdEntry() {}

  static int alloc_ps_id_entry(uint32_t ps_id, ObPsEntry *ps_entry, ObPsIdEntry *&ps_id_entry);

  void destroy();
  bool is_valid() const
  {
    return 0 != ps_id_
           && NULL != ps_entry_
           && ps_entry_->is_valid();
  }

  uint32_t get_ps_id() { return ps_id_; }
  ObPsEntry *get_ps_entry() { return ps_entry_; }
  ObPsSqlMeta &get_ps_sql_meta() { return ps_meta_; }
  int64_t get_param_count() const { return ps_meta_.get_param_count(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t ps_id_; // client ps id
  ObPsEntry *ps_entry_;
  ObPsSqlMeta ps_meta_;
  LINK(ObPsIdEntry, ps_id_link_);
};

// stored in server session info
class ObPsIdPair
{
public:
  ObPsIdPair() : client_ps_id_(0),
                 server_ps_id_(0) {}
  ObPsIdPair(uint32_t client_id, uint32_t server_id)
      : client_ps_id_(client_id), server_ps_id_(server_id) {}
  ~ObPsIdPair() {}

  static int alloc_ps_id_pair(uint32_t client_ps_id, uint32_t server_ps_id, ObPsIdPair *&ps_id_pair);

  void destroy();
  bool is_valid() const { return 0 != client_ps_id_ && 0 != server_ps_id_; }
  uint32_t get_client_ps_id() const { return client_ps_id_; }
  void set_client_ps_id(uint32_t id) { client_ps_id_ = id; }
  uint32_t get_server_ps_id() const { return server_ps_id_; }
  void set_server_ps_id(uint32_t id) { server_ps_id_ = id; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t client_ps_id_;
  uint32_t server_ps_id_;
  LINK(ObPsIdPair, ps_id_pair_link_);
};
  
class ObTextPsEntry : public ObBasePsEntry
{
public:
  ObTextPsEntry() : ObBasePsEntry() {}
  ~ObTextPsEntry() { }

  template <typename T>
  static int alloc_and_init_ps_entry(const common::ObString &sql,
                                     const obutils::ObSqlParseResult &parse_result,
                                     T *&entry);
  int init(char *buf_start, int64_t buf_len, const common::ObString &text_ps_sql);
  virtual void free();
  virtual void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTextPsEntry);
};

template <typename T>
int ObTextPsEntry::alloc_and_init_ps_entry(const ObString &text_ps_sql,
                                           const obutils::ObSqlParseResult &parse_result,
                                           T *&entry)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = 0;
  char *buf = NULL;

  int64_t obj_size = sizeof(T);
  int64_t sql_len = text_ps_sql.length() + PARSE_EXTRA_CHAR_NUM;

  alloc_size = obj_size + sql_len;
  if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(WARN, "fail to alloc mem for text ps entry", K(alloc_size), K(ret));
  } else {
    entry = new (buf) T();
    if (OB_FAIL(entry->init(buf + obj_size, alloc_size - obj_size, text_ps_sql))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_SM_LOG(WARN, "fail to alloc mem for text ps entry", K(alloc_size), K(ret));
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

class ObGlobalTextPsEntry : public ObTextPsEntry
{
public:
  ObGlobalTextPsEntry() : ObTextPsEntry() {}
  ~ObGlobalTextPsEntry() {}
  virtual void free();
  template <typename T>
  static int alloc_and_init_ps_entry(const common::ObString &sql,
                                     const obutils::ObSqlParseResult &parse_result,
                                     T *&entry);
};

template <typename T>
int ObGlobalTextPsEntry::alloc_and_init_ps_entry(const ObString &sql,
                                                 const obutils::ObSqlParseResult &parse_result,
                                                 T *&entry)
{
  return ObTextPsEntry::alloc_and_init_ps_entry(sql, parse_result, entry);
}

class ObTextPsNameEntry
{
public:
  ObTextPsNameEntry() : text_ps_name_(), text_ps_entry_(NULL), version_(0) {}
  ~ObTextPsNameEntry() { destroy(); }
  ObTextPsNameEntry(char *buf, const int64_t buf_len, ObTextPsEntry *entry)
  {
    text_ps_name_.assign_ptr(buf, static_cast<int32_t>(buf_len));
    text_ps_entry_ = entry;
  }

  static int alloc_text_ps_name_entry(const ObString &text_ps_name,
                                      ObTextPsEntry *text_ps_entry,
                                      ObTextPsNameEntry *&text_ps_name_entry);
  void destroy();
  bool is_valid() const
  {
    return !text_ps_name_.empty() && NULL != text_ps_entry_ && text_ps_entry_->is_valid();
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  // text_ps_name and text_ps_entry alloced in alloc_text_ps_name_entry
  ObString text_ps_name_;
  ObTextPsEntry *text_ps_entry_;
  uint32_t version_;
  LINK(ObTextPsNameEntry, text_ps_name_link_);
};

class ObBasePsEntryCache
{
public:
  ObBasePsEntryCache() {}
  virtual ~ObBasePsEntryCache() { destroy(); }
  virtual void destroy() {}

public:
  virtual void delete_base_ps_entry(ObBasePsEntry *base_ps_entry) {
    UNUSED(base_ps_entry);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObBasePsEntryCache);
};

class ObBasePsEntryThreadCache : public ObBasePsEntryCache
{
public:
  ObBasePsEntryThreadCache() : ObBasePsEntryCache(), ps_entry_thread_map_() {}
  ~ObBasePsEntryThreadCache() { destroy(); }
  void destroy();

public:
  static const int64_t HASH_BUCKET_SIZE = 64;
  struct ObBasePsEntryHashing
  {
    typedef const common::ObString &Key;
    typedef ObBasePsEntry Value;
    typedef ObDLList(ObBasePsEntry, base_ps_entry_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash();  }
    static Key key(Value const *value) { return value->base_ps_sql_;  }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs;  }
  };
  typedef common::hash::ObBuildInHashMap<ObBasePsEntryHashing, HASH_BUCKET_SIZE> ObBasePsEntryMap;

public:
  template <typename T>
  int acquire_ps_entry(const common::ObString &sql, T *&ps_entry);

  template <typename T>
  int create_ps_entry(const common::ObString &sql,
                      const obutils::ObSqlParseResult &parse_result,
                      T *&ps_entry);

  template <typename T>
  int acquire_or_create_ps_entry(const common::ObString &sql,
                                 const obutils::ObSqlParseResult &parse_result,
                                 T *&ps_entry);

  void delete_base_ps_entry(ObBasePsEntry *base_ps_entry);

private:
  ObBasePsEntryMap ps_entry_thread_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBasePsEntryThreadCache);
};

template <typename T>
int ObBasePsEntryThreadCache::acquire_ps_entry(const ObString &sql, T *&ps_entry)
{
  int ret = OB_SUCCESS;
  ObBasePsEntry *tmp_entry = NULL;
  if (OB_FAIL(ps_entry_thread_map_.get_refactored(sql, tmp_entry))) {
    //do nothing
  } else {
    ps_entry = static_cast<T*>(tmp_entry);
    ps_entry->inc_ref();
  }
  return ret;
}

template <typename T>
int ObBasePsEntryThreadCache::create_ps_entry(const common::ObString &sql,
                                        const obutils::ObSqlParseResult &parse_result,
                                        T *&ps_entry)
{
  int ret = OB_SUCCESS;
  ObBasePsEntry* tmp_ps_entry = NULL;
  if (OB_FAIL(ps_entry_thread_map_.get_refactored(sql, tmp_ps_entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(T::alloc_and_init_ps_entry(sql, parse_result, ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to alloc and init ps entry", K(ret));
      } else if (OB_FAIL(ps_entry_thread_map_.unique_set(ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to add ps entry to cache", K(ret));
        if (OB_LIKELY(NULL != ps_entry)) {
          ps_entry->destroy();
          ps_entry = NULL;
        }
      } else {
        ObBasePsEntry *tmp_entry = static_cast<ObBasePsEntry*>(ps_entry);
        tmp_entry->set_ps_entry_cache(this);
      }
    }
  } else {
    ps_entry = static_cast<T*>(tmp_ps_entry);
  }
  if (OB_SUCC(ret)) {
    ps_entry->inc_ref();
  }
  return ret;
}

template <typename T>
int ObBasePsEntryThreadCache::acquire_or_create_ps_entry(const ObString &sql,
                                                         const obutils::ObSqlParseResult &parse_result,
                                                         T *&ps_entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(acquire_ps_entry(sql, ps_entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_ps_entry(sql, parse_result, ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to create ps entry", K(sql), K(ret));
      } else {
        PROXY_SM_LOG(DEBUG, "succ to create ps entry", K(sql), KPC(ps_entry));
      }
    } else {
      PROXY_SM_LOG(WARN, "fail to get ps entry", K(sql), K(ret));
    }
  }
  return ret;
}

class ObBasePsEntryGlobalCache : public ObBasePsEntryCache
{
public:
  ObBasePsEntryGlobalCache() : ObBasePsEntryCache(), lock_(), ps_entry_global_map_() {}
  ~ObBasePsEntryGlobalCache() { destroy(); }
  void destroy();

public:
  static const int64_t HASH_BUCKET_SIZE = 64;  
  struct ObBasePsEntryHashing
  {
    typedef const common::ObString &Key;
    typedef ObBasePsEntry Value;
    typedef ObDLList(ObBasePsEntry, base_ps_entry_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash();  }
    static Key key(Value const *value) { return value->base_ps_sql_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs;  }
    static int64_t inc_ref(Value* value) { return ATOMIC_AAF(&value->ref_count_, 1); }
    static bool bcas_ref(Value* value, int64_t cmpv, int64_t newv) { return ATOMIC_BCAS(&value->ref_count_, cmpv, newv); }
    static int64_t get_ref(Value* value) { return ATOMIC_LOAD(&value->ref_count_); }
    static void destroy(Value* value) { value->destroy(); }
  };
  typedef common::hash::ObBuildInHashMapForRefCount<ObBasePsEntryHashing, HASH_BUCKET_SIZE> ObBasePsEntryGlobalMap;

public:
  template <typename T>
  int acquire_ps_entry(const common::ObString &sql, T *&ps_entry);

  template <typename T>
  int create_ps_entry(const common::ObString &sql,
                      const obutils::ObSqlParseResult &parse_result,
                      T *&ps_entry);

  template <typename T>
  int acquire_or_create_ps_entry(const common::ObString &sql,
                                 const obutils::ObSqlParseResult &parse_result,
                                 T *&ps_entry);

  void delete_base_ps_entry(ObBasePsEntry *base_ps_entry);

private:
  common::DRWLock lock_;
  ObBasePsEntryGlobalMap ps_entry_global_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBasePsEntryGlobalCache);
};

template <typename T>
int ObBasePsEntryGlobalCache::acquire_ps_entry(const ObString &sql, T *&ps_entry)
{
  int ret = OB_SUCCESS;
  common::DRWLock::RDLockGuard guard(lock_);
  ObBasePsEntry *tmp_entry = NULL;
  if (OB_FAIL(ps_entry_global_map_.get_refactored(sql, tmp_entry))) {
    //do nothing
  } else {
    ps_entry = static_cast<T*>(tmp_entry);
  }
  return ret;
}

template <typename T>
int ObBasePsEntryGlobalCache::create_ps_entry(const common::ObString &sql,
                                        const obutils::ObSqlParseResult &parse_result,
                                        T *&ps_entry)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(lock_);
  ObBasePsEntry* tmp_ps_entry = NULL;
  if (OB_FAIL(ps_entry_global_map_.get_refactored(sql, tmp_ps_entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(T::alloc_and_init_ps_entry(sql, parse_result, ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to alloc and init ps entry", K(ret));
      } else if (OB_FAIL(ps_entry_global_map_.unique_set(ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to add ps entry to cache", K(ret));
        if (OB_LIKELY(NULL != ps_entry)) {
          ps_entry->destroy();
          ps_entry = NULL;
        }
      } else {
        ObBasePsEntry *tmp_entry = static_cast<ObBasePsEntry*>(ps_entry);
        tmp_entry->set_ps_entry_cache(this);
      }
    }
  } else {
    ps_entry = static_cast<T*>(tmp_ps_entry);
  }
  return ret;
}

template <typename T>
int ObBasePsEntryGlobalCache::acquire_or_create_ps_entry(const ObString &sql,
                                                         const obutils::ObSqlParseResult &parse_result,
                                                         T *&ps_entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(acquire_ps_entry(sql, ps_entry))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_ps_entry(sql, parse_result, ps_entry))) {
        PROXY_SM_LOG(WARN, "fail to create ps entry", K(sql), K(ret));
      } else {
        PROXY_SM_LOG(DEBUG, "succ to create ps entry", K(sql), KPC(ps_entry));
      }
    } else {
      PROXY_SM_LOG(WARN, "fail to get ps entry", K(sql), K(ret));
    }
  }
  return ret;
}

int init_ps_entry_cache_for_thread();
int init_ps_entry_cache_for_one_thread(int64_t index);
int init_text_ps_entry_cache_for_thread();
int init_text_ps_entry_cache_for_one_thread(int64_t index);
ObBasePsEntryGlobalCache &get_global_ps_entry_cache();
ObBasePsEntryGlobalCache &get_global_text_ps_entry_cache();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PREPARE_STATEMENT_STRUCT_H

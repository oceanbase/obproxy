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

  // use SimpleAllocer to save mem
  typedef common::hash::ObHashSet<net::ObIpEndpoint,
                                  common::hash::ReadWriteDefendMode,
                                  common::hash::hash_func<net::ObIpEndpoint>,
                                  common::hash::equal_to<net::ObIpEndpoint>,
                                  common::hash::SimpleAllocer<common::hash::HashSetTypes<net::ObIpEndpoint>::AllocType, NODE_NUM>
                                 > ADDR_HASH_SET;

  ObPsIdAddrs() : ps_id_(0), addrs_() {
    addrs_.create(BUCKET_SIZE);
  }
  ObPsIdAddrs(uint32_t ps_id) : ps_id_(ps_id), addrs_() {
    addrs_.create(BUCKET_SIZE);
  }
  ~ObPsIdAddrs() {};

  static int alloc_ps_id_addrs(uint32_t ps_id, const struct sockaddr &addr, ObPsIdAddrs *&ps_id_addrs);
  void destroy();
  int add_addr(const struct sockaddr &socket_addr);
  int remove_addr(const struct sockaddr &socket_addr);
  ADDR_HASH_SET &get_addrs() { return addrs_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint32_t ps_id_; // client ps id
  ADDR_HASH_SET addrs_;

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

  virtual void free() { destroy(); }

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

  static int alloc_and_init_ps_entry(const common::ObString &ps_sql,
                                     const obutils::ObSqlParseResult &parse_result,
                                     ObPsEntry *&entry);
  int init(char *buf_start, int64_t buf_len);
  void destroy();
  int set_sql(const common::ObString &ps_sql);

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  const static int64_t PARSE_EXTRA_CHAR_NUM = 2;

public:
  DISALLOW_COPY_AND_ASSIGN(ObPsEntry);
};

// stored in client session info
class ObPsIdEntry
{
public:
  ObPsIdEntry() : ps_id_(0), ps_entry_(NULL), ps_meta_() {}
  ObPsIdEntry(uint32_t client_id, ObPsEntry *entry)
      : ps_id_(client_id), ps_entry_(entry) {
    ps_entry_->inc_ref();
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
  ObTextPsEntry() : ObBasePsEntry(), version_(0) {} 
  ~ObTextPsEntry() { destroy(); }

  static int alloc_and_init_text_ps_entry(const common::ObString &sql,
                                          const obutils::ObSqlParseResult &parse_result,
                                          ObTextPsEntry *&entry);
  int init(char *buf_start, int64_t buf_len, const common::ObString &text_ps_sql);
  void destroy();

  void set_version(uint32_t version) { version_ = version; }
  uint32_t get_version() const { return version_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  uint32_t version_;

  DISALLOW_COPY_AND_ASSIGN(ObTextPsEntry);
};

class ObTextPsNameEntry
{
public:
  ObTextPsNameEntry() : text_ps_name_(), text_ps_entry_(NULL) {}
  ~ObTextPsNameEntry() { destroy(); }
  ObTextPsNameEntry(char *buf, const int64_t buf_len, ObTextPsEntry *entry)
  {
    text_ps_name_.assign(buf, static_cast<int32_t>(buf_len));
    text_ps_entry_ = entry;
  }

  static int alloc_text_ps_name_entry(const ObString &text_ps_name,
                                      ObTextPsEntry *text_ps_entry,
                                      ObTextPsNameEntry *&text_ps_name_entry);
  void destroy();
  bool is_valid() const
  {
    return !text_ps_name_.empty()
           && NULL != text_ps_entry_
           && text_ps_entry_->is_valid();
  }
  const ObString& get_text_ps_name() { return text_ps_name_; }
  ObTextPsEntry *get_text_ps_entry() { return text_ps_entry_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  // text_ps_name and text_ps_entry alloced in alloc_text_ps_name_entry
  ObString text_ps_name_;
  ObTextPsEntry *text_ps_entry_;
  LINK(ObTextPsNameEntry, text_ps_name_link_);
};

class ObBasePsEntryCache
{
public:
  ObBasePsEntryCache() : base_ps_map_() {}
  ~ObBasePsEntryCache() { destroy(); }
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
  int get_ps_entry(const common::ObString &sql, ObPsEntry *&ps_entry)
  {
    int ret = common::OB_SUCCESS;
    ObBasePsEntry *tmp_entry = NULL;
    if (OB_FAIL(base_ps_map_.get_refactored(sql, tmp_entry))) {
      //do nothing
    } else {
      ps_entry = static_cast<ObPsEntry*>(tmp_entry);
    }
    return ret;
  }

  int set_ps_entry(ObPsEntry *ps_entry)
  {
    ObBasePsEntry *tmp_entry = static_cast<ObBasePsEntry*>(ps_entry);
    tmp_entry->set_ps_entry_cache(this);
    return base_ps_map_.unique_set(tmp_entry);
  }

  int get_text_ps_entry(const common::ObString &sql, ObTextPsEntry *&text_ps_entry)
  {
    int ret = common::OB_SUCCESS;
    ObBasePsEntry *tmp_entry = NULL;
    if (OB_FAIL(base_ps_map_.get_refactored(sql, tmp_entry))) {
      //do nothing
    } else {
      text_ps_entry = static_cast<ObTextPsEntry*>(tmp_entry);
    }
    return ret;
  }

  int set_text_ps_entry(ObTextPsEntry *text_ps_entry)
  {
    ObBasePsEntry *tmp_entry = text_ps_entry;
    return base_ps_map_.unique_set(tmp_entry);
  }

  int delete_text_ps_entry(ObTextPsEntry *text_ps_entry)
  {
    if (NULL != text_ps_entry) {
      base_ps_map_.remove(text_ps_entry->base_ps_sql_);
      text_ps_entry->destroy();
    }
    return common::OB_SUCCESS;
  }

  void delete_base_ps_entry(ObBasePsEntry *base_ps_entry)
  {
    base_ps_map_.remove(base_ps_entry);
  }

private:
  ObBasePsEntryMap base_ps_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBasePsEntryCache);
};

int init_ps_entry_cache_for_thread();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PREPARE_STATEMENT_STRUCT_H

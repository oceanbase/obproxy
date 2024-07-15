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

#ifndef _OB_TABLE_TABLE_H
#define _OB_TABLE_TABLE_H 1
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "common/ob_object.h"
#include "common/ob_rowkey.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "common/ob_common_types.h"
#include "common/ob_range.h"
#include "common/ob_range2.h"
#include "common/ob_role.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/utility/ob_tablet_id.h"
#include "lib/utility/ob_ls_id.h"
#include "ob_rpc_struct.h"
#include "ob_proxy_rpc_serialize_utils.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace obproxy
{
namespace obkv
{
using common::ObString;
using common::ObRowkey;
using common::ObObj;
using common::ObIArray;
using common::ObSEArray;

class ObTableSingleOpEntity;
class ObTableResult;
typedef  ODP_IGNORE_UNIS_FIELD_TYPE(ObTableResult) OB_UNIS_IGNORE_TABLE_RESULT;
typedef  ODP_IGNORE_UNIS_FIELD_TYPE(ObTableSingleOpEntity) OB_UNIS_IGNORE_SINGLE_OP_ENTITY;

enum class EntityMode {
  NORMAL = 0,
  LAZY_MODE = 1,
};

static const EntityMode OBKV_ENTITY_MODE = EntityMode::LAZY_MODE;

////////////////////////////////////////////////////////////////
// structs of a table storage interface
////////////////////////////////////////////////////////////////

class ObTableBitMap;
class ObTableTabletOp;
class ObTableBatchOperation;
class ObTableSingleOp;
class ObTableSingleOpQuery;
class ObTableSingleOpEntity;
class ObTableQuery;
class ObTableQueryAndMutate;
class ObTableOperation;
class ObTableOperationResult;
class ObTableEntity;
class ObHTableFilter;
class ObTableSingleOpResult;


// Will be completely deserialize by the type, and collect info we need
// Only copy buf in serialization
typedef ODP_IGNORE_FIELD_TYPE(ObTableBitMap) OB_IGNORE_TABLE_BIT_MAP;
typedef ODP_IGNORE_FIELD_TYPE(ObTableTabletOp) OB_IGNORE_TABLE_TABLET_OP;
typedef ODP_IGNORE_FIELD_TYPE(ObTableSingleOp) OB_IGNORE_TABLE_SINGLE_OP;
typedef ODP_IGNORE_FIELD_TYPE(ObTableSingleOpEntity) OB_IGNORE_TABLE_SINGLE_OP_ENTITY;
typedef ODP_IGNORE_FIELD_TYPE(ObTableQuery) OB_IGNORE_TABLE_QUERY;
typedef ODP_IGNORE_FIELD_TYPE(ObTableQueryAndMutate) OB_IGNORE_TABLE_QUERY_AND_MUTATE;
typedef ODP_IGNORE_FIELD_TYPE(ObTableOperation) OB_IGNORE_TABLE_OPERATION;


// Won't be completely deserialized, we skip the deserialization by the unis header
// Only copy buf in serialization
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObTableBatchOperation) OB_UNIS_IGNORE_TABLE_BATCH_OP;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObTableSingleOpQuery) OB_UNIS_IGNORE_TABLE_SINGLE_OP_QUERY;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObTableOperationResult) OB_UNIS_IGNORE_TABLE_OP_RESULT;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObTableSingleOpResult) OB_UNIS_IGNORE_TABLE_SINGLE_OP_RESULT;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObTableEntity) OB_UNIS_IGNORE_TABLE_ENTITY;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObHTableFilter) OB_UNIS_IGNORE_HTABLE_FILTER;
typedef ODP_IGNORE_UNIS_FIELD_TYPE(ObDataBuffer) OB_UNIS_IGNORE_DATA_BUFFER;

class ObTableBitMap {
public:
  typedef uint8_t size_type;
  static const size_type BYTES_PER_BLOCK = sizeof(size_type);   // 1
  static const size_type BITS_PER_BLOCK = BYTES_PER_BLOCK * 8;  // 1 * 8 = 8
  static const size_type BLOCK_MOD_BITS = 3;                    // 2^3 = 8

public:
  ObTableBitMap()
    : block_count_(-1),
      valid_bits_num_(0)
  {};
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size() const;

  int init_bitmap_size(int64_t valid_bits_num);
  void clear()
  {
    datas_.reset();
    block_count_ = -1;
    valid_bits_num_ = 0;
  }
  int reset();

  int push_block_data(size_type data);

  int get_true_bit_positions(ObIArray<int64_t> &true_pos) const;

  int get_block_value(int64_t idx, size_type &value) const
  {
    return datas_.at(idx, value);
  }

  int set(int64_t bit_pos);

  OB_INLINE int64_t get_block_count() const
  {
    return block_count_;
  }

  OB_INLINE bool has_init() const
  {
    return block_count_ != -1;
  }

  OB_INLINE static int64_t get_need_blocks_num(int64_t num_bits)
  {
    return ((num_bits + BITS_PER_BLOCK - 1) & ~(BITS_PER_BLOCK - 1)) >> BLOCK_MOD_BITS;
  }
  OB_INLINE int64_t get_valid_bits_num() const { return valid_bits_num_; }
  DECLARE_TO_STRING;
private:
  ObSEArray<size_type, 8> datas_;
  // no need SERIALIZE
  int64_t block_count_;
  int64_t valid_bits_num_;
};

/// A Table Entity
class ObITableEntity: public common::ObDLinkBase<ObITableEntity>
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO_V(1)
public:
  ObITableEntity() {};
  virtual ~ObITableEntity() = default;
  virtual void reset() = 0;
  virtual bool is_empty() const { return 0 == get_rowkey_size() && 0 == get_properties_count(); }
  //@{ primary key contains partition key. Note that all values are shallow copy.
  virtual int set_rowkey(const ObRowkey &rowkey) = 0;
  virtual int set_rowkey(const ObITableEntity &other) = 0;
  virtual int set_rowkey_value(int64_t idx, const ObObj &value) = 0;
  virtual int add_rowkey_value(const ObObj &value) = 0;
  virtual int64_t get_rowkey_size() const = 0;
  virtual int get_rowkey_value(int64_t idx, ObObj &value) const = 0;
  virtual ObRowkey get_rowkey() = 0;
  virtual void get_rowkey(ObRowkey &rowkey) = 0;
  virtual int64_t hash_rowkey() const = 0;
  //@}
  //@{ property is a key-value pair.
  virtual int set_property(const ObString &prop_name, const ObObj &prop_value) = 0;
  virtual int get_property(const ObString &prop_name, ObObj &prop_value) const = 0;
  virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const = 0; // @todo property iterator
  virtual int get_properties_names(ObIArray<ObString> &properties) const = 0;
  virtual int get_properties_values(ObIArray<ObObj> &properties_values) const = 0;
  virtual const ObObj &get_properties_value(int64_t idx) const = 0;
  virtual int64_t get_properties_count() const = 0;
  virtual const ObRpcFieldBuf &get_properties_buf() const = 0;
  virtual void set_properties_buf(const ObRpcFieldBuf &buf) = 0;
  virtual void reset_properties_buf() = 0;

  // properties optimize for ObTableLSOps
  // only implement for ObTableSingleOpEntity 
  virtual void set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names) = 0;
  virtual void set_is_same_properties_names(bool is_same_properties_names) = 0;
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) = 0;

  virtual const ObTableBitMap *get_rowkey_names_bp() const = 0;
  virtual ObTableBitMap *get_rowkey_names_bp() = 0;
  virtual const ObTableBitMap *get_properties_names_bp() const = 0;
  virtual ObTableBitMap *get_properties_names_bp() = 0;

  virtual const ObIArray<ObString>* get_all_rowkey_names() const = 0;
  virtual const ObIArray<ObString>* get_all_properties_names() const = 0;
  //@}
  //virtual int deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other);
  //virtual int deep_copy_rowkey(common::ObIAllocator &allocator, const ObITableEntity &other);
  //virtual int deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other);
  //virtual int deep_copy_properties_normal_mode(common::ObIAllocator &allocator, const ObITableEntity &other);
  virtual int add_retrieve_property(const ObString &prop_name);
  //void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  //common::ObIAllocator *get_allocator() { return alloc_; }
  VIRTUAL_TO_STRING_KV("ITableEntity", "");
  //protected:
  //common::ObIAllocator *alloc_;  // for deep copy in deserialize
};

class ObITableEntityFactory
{
public:
  virtual ~ObITableEntityFactory() = default;
  virtual ObITableEntity *alloc() = 0;
  virtual void free(ObITableEntity *obj) = 0;
  virtual void free_and_reuse() = 0;
  virtual int64_t get_used_count() const = 0;
  virtual int64_t get_free_count() const = 0;
  virtual int64_t get_used_mem() const = 0;
  virtual int64_t get_total_mem() const = 0;
};

/// An implementation for ObITableEntity
class ObTableEntity: public ObITableEntity
{
public:
  ObTableEntity();
  virtual ~ObTableEntity();
  virtual int set_rowkey(const ObRowkey &rowkey) override;
  virtual int set_rowkey(const ObITableEntity &other) override;
  virtual int set_rowkey_value(int64_t idx, const ObObj &value) override;
  virtual int add_rowkey_value(const ObObj &value) override;
  virtual int64_t get_rowkey_size() const override { return rowkey_.count(); };
  virtual int get_rowkey_value(int64_t idx, ObObj &value) const override;
  virtual int64_t hash_rowkey() const override;
  virtual int get_property(const ObString &prop_name, ObObj &prop_value) const override;
  virtual int set_property(const ObString &prop_name, const ObObj &prop_value) override;
  virtual int push_value(const ObObj &prop_value);
  virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const override;
  virtual int get_properties_names(ObIArray<ObString> &properties_names) const override;
  virtual int get_properties_values(ObIArray<ObObj> &properties_values) const override;
  virtual int64_t get_properties_count() const override;
  virtual void reset() override;
  virtual ObRowkey get_rowkey() override;
  virtual void get_rowkey(ObRowkey &rowkey) override;
  virtual const ObObj &get_properties_value(int64_t idx) const override;

  const ObIArray<ObString> &get_properties_names() const { return properties_names_; }
  ObIArray<ObString> &get_properties_names() { return properties_names_; }

  const ObIArray<ObString> &get_rowkey_names() const { return rowkey_names_; }
  ObIArray<ObString> &get_rowkey_names() { return rowkey_names_; }

  const ObIArray<ObObj> &get_properties_values() const { return properties_values_; }
  ObIArray<ObObj> &get_properties_values() { return properties_values_; }


  const ObIArray<ObObj> &get_rowkey_objs() const { return rowkey_; }; 
  ObIArray<ObObj> &get_rowkey_objs() { return rowkey_; };
 
  virtual const ObRpcFieldBuf &get_properties_buf() const override { return properties_buf_; }
  virtual void reset_properties_buf() override;
  virtual void set_properties_buf(const ObRpcFieldBuf &buf) override;

  // properties optimie for ObTableLSOps
  virtual void set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names) override;
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) override;
  virtual const ObTableBitMap *get_rowkey_names_bp() const override;
  virtual ObTableBitMap *get_rowkey_names_bp() override;
  virtual const ObTableBitMap * get_properties_names_bp() const override;
  virtual ObTableBitMap * get_properties_names_bp() override;
  virtual const ObIArray<ObString>* get_all_rowkey_names() const override;
  virtual const ObIArray<ObString>* get_all_properties_names() const override;
  virtual void set_is_same_properties_names(bool is_same_properties_names) override;


  DECLARE_TO_STRING;
private:
  bool has_exist_in_properties(const ObString &name, int64_t *idx = nullptr) const;
protected:
  ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> rowkey_;
  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> properties_names_;
  ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> properties_values_;
  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> rowkey_names_;
  ObRpcFieldBuf properties_buf_;
};

enum class ObTableEntityType
{
  ET_DYNAMIC = 0,
  ET_KV = 1,
  ET_HKV = 2
};

// @note not thread-safe
template <typename T>
class ObTableEntityFactory: public ObITableEntityFactory
{
public:
  ObTableEntityFactory(int64_t label = common::ObModIds::TABLE_ENTITY)
      :alloc_(label)
  {}
  virtual ~ObTableEntityFactory();
  virtual ObITableEntity *alloc() override;
  virtual void free(ObITableEntity *obj) override;
  virtual void free_and_reuse() override;
  virtual int64_t get_free_count() const { return free_list_.get_size(); }
  virtual int64_t get_used_count() const { return used_list_.get_size(); }
  virtual int64_t get_used_mem() const { return alloc_.used(); }
  virtual int64_t get_total_mem() const { return alloc_.total(); }
private:
  void free_all();
private:
  common::ObArenaAllocator alloc_;
  common::ObDList<ObITableEntity> used_list_;
  common::ObDList<ObITableEntity> free_list_;
};

template <typename T>
ObTableEntityFactory<T>::~ObTableEntityFactory()
{
  free_all();
}

template <typename T>
ObITableEntity *ObTableEntityFactory<T>::alloc()
{
  ObITableEntity *entity = free_list_.remove_first();
  if (NULL == entity) {
    void * ptr = alloc_.alloc(sizeof(T));
    if (NULL == ptr) {
      CLIENT_LOG(WDIAG, "no memory for table entity");
    } else {
      entity = new(ptr) T();
      used_list_.add_last(entity);
    }
  } else {
    used_list_.add_last(entity);
  }
  return entity;
}

template <typename T>
void ObTableEntityFactory<T>::free(ObITableEntity *entity)
{
  if (NULL != entity) {
    entity->reset();
    //entity->set_allocator(NULL);
    used_list_.remove(entity);
    free_list_.add_last(entity);
  }
}

template <typename T>
void ObTableEntityFactory<T>::free_and_reuse()
{
  while (!used_list_.is_empty()) {
    this->free(used_list_.get_first());
  }
}

template <typename T>
void ObTableEntityFactory<T>::free_all()
{
  ObITableEntity *entity = NULL;
  while (NULL != (entity = used_list_.remove_first())) {
    entity->~ObITableEntity();
  }
  while (NULL != (entity = free_list_.remove_first())) {
    entity->~ObITableEntity();
  }
}

enum class ObQueryOperationType : int {
  QUERY_START = 0,
  QUERY_NEXT = 1,
  QUERY_MAX
};

/// Table Operation Type
/// used for both table operation and single op
struct ObTableOperationType
{
  enum Type
  {
    GET = 0,
    INSERT = 1,
    DEL = 2,
    UPDATE = 3,
    INSERT_OR_UPDATE = 4,
    REPLACE = 5,
    INCREMENT = 6,
    APPEND = 7,
    SCAN = 8,
    TTL = 9, // internal type for ttl executor cache key
    CHECK_AND_INSERT_UP = 10,
    INVALID = 11
  };
};

/// A table operation
class ObTableOperation
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  //const ObITableEntity &entity() const { return *entity_; }
  ObTableOperationType::Type type() const { return operation_type_; }
  void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  //void set_entity(const ObITableEntity &entity) { entity_ = &entity; }
  //void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  //int get_entity(const ObITableEntity *&entity) const;
  //int get_entity(ObITableEntity *&entity);
  //uint64_t get_checksum();
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableOperation &other);
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntity &entity, const ObTableOperation &other);
  TO_STRING_KV(K_(operation_type), "entity", to_cstring(entity_));
private:
  ObTableEntity entity_;
  ObTableOperationType::Type operation_type_;
};

/// common result for ObTable
class ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableResult()
      :errno_(common::OB_ERR_UNEXPECTED)
  {
    sqlstate_[0] = '\0';
    msg_[0] = '\0';
  }
  ~ObTableResult() = default;
  void set_errno(int err) { errno_ = err; }
  int get_errno() const { return errno_; }
  int assign(const ObTableResult &other);
  TO_STRING_KV(K_(errno));
private:
  static const int64_t MAX_MSG_SIZE = common::OB_MAX_ERROR_MSG_LEN;
protected:
  int32_t errno_;
  char sqlstate_[6];  // terminate with '\0'
  char msg_[MAX_MSG_SIZE]; // terminate with '\0'
};

/// result for ObTableOperation
class ObTableOperationResult final: public ObTableResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableOperationResult();
  ~ObTableOperationResult() = default;
  // ObTableOperationResult(const ObTableOperationResult &result)
  //   : operation_type_(result.type()), entity_(NULL), affected_rows_(result.get_affected_rows())
  // {}

  //ObTableOperationType::Type type() const { return operation_type_; }
  //int get_entity(const ObITableEntity *&entity) const;
  //int get_entity(ObITableEntity *&entity);

  //void set_entity(ObITableEntity &entity) { entity_ = &entity; }
  //void set_type(ObTableOperationType::Type op_type) { operation_type_ = op_type; }
  int64_t get_affected_rows() const { return affected_rows_; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }

  //int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableOperationResult &other);
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntity &result_entity, const ObTableOperationResult &other);
  DECLARE_TO_STRING;
private:
  ObTableOperationType::Type operation_type_;
  OB_UNIS_IGNORE_TABLE_ENTITY entity_;
  int64_t affected_rows_;
};

class ObIRetryPolicy
{
public:
  virtual bool need_retry(int32_t curr_retry_count, int last_errno, int64_t &retry_interval)
  {
    UNUSEDx(curr_retry_count, last_errno, retry_interval);
    return false;
  }
};

class ObLinearRetry : public ObIRetryPolicy
{};

class ObExponentialRetry : public ObIRetryPolicy
{};

class ObNoRetry : public ObIRetryPolicy
{};

/// consistency levels
/// @see https://www.atatech.org/articles/102030
enum class ObTableConsistencyLevel
{
  STRONG = 0,
  EVENTUAL = 1,
};
/// clog row image type
/// @see share::ObBinlogRowImage
enum class ObBinlogRowImageType
{
  MINIMAL = 0,
  NOBLOB = 1,
  FULL = 2,
};
/// request options for all the table operations
class ObTableRequestOptions final
{
public:
  ObTableRequestOptions();
  ~ObTableRequestOptions() = default;

  void set_consistency_level(ObTableConsistencyLevel consistency_level) { consistency_level_ = consistency_level; }
  ObTableConsistencyLevel consistency_level() const { return consistency_level_; }
  void set_server_timeout(int64_t server_timeout_us) { server_timeout_us_ = server_timeout_us; }
  int64_t server_timeout() const { return server_timeout_us_; }
  void set_execution_time(int64_t max_execution_time_us) { max_execution_time_us_ = max_execution_time_us; }
  int64_t max_execution_time() const { return max_execution_time_us_; }
  void set_retry_policy(ObIRetryPolicy *retry_policy) { retry_policy_ = retry_policy; }
  ObIRetryPolicy* retry_policy() { return retry_policy_; }
  void set_returning_affected_rows(bool returning) { returning_affected_rows_ = returning; }
  bool returning_affected_rows() const { return returning_affected_rows_; }
  void set_returning_rowkey(bool returning) { returning_rowkey_ = returning; }
  bool returning_rowkey() const { return returning_rowkey_; }
  void set_returning_affected_entity(bool returning) { returning_affected_entity_ = returning; }
  bool returning_affected_entity() const { return returning_affected_entity_; }
  void set_binlog_row_image_type(ObBinlogRowImageType type) { binlog_row_image_type_ = type; }
  ObBinlogRowImageType binlog_row_image_type() const { return binlog_row_image_type_; }
private:
  ObTableConsistencyLevel consistency_level_;
  int64_t server_timeout_us_;
  int64_t max_execution_time_us_;
  ObIRetryPolicy *retry_policy_;
  bool returning_affected_rows_;  // default: false
  bool returning_rowkey_;         // default: false
  bool returning_affected_entity_;  // default: false
  // bool batch_operation_as_atomic_;  // default: false
  // int route_policy
  ObBinlogRowImageType binlog_row_image_type_;  // default: FULL
};

/// A batch operation
class ObTableBatchOperation
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
// public:
//   static const int64_t MAX_BATCH_SIZE = 1000;
//   static const int64_t COMMON_BATCH_SIZE = 8;
public:
  ObTableBatchOperation()
      :table_operations_(common::ObModIds::TABLE_BATCH_OPERATION, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
       is_readonly_(true),
       is_same_type_(true),
       is_same_properties_names_(true)
  {}
  ~ObTableBatchOperation() = default;
  void reset();
  // void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }

  int64_t count() const { return table_operations_.count(); }
  //const ObTableOperation &at(int64_t idx) const { return table_operations_.at(idx); }
  //int push_back(const ObTableOperation &op) { return table_operations_.push_back(op); }
  bool is_readonly() const { return is_readonly_; }
  bool is_same_type() const { return is_same_type_; }
  bool is_same_properties_names() const { return is_same_properties_names_; }
  void set_readonly(bool is_readonly) { is_readonly_ = is_readonly; }
  void set_same_type(bool is_same_type) { is_same_type_ = is_same_type; }
  void set_same_properties_names(bool is_same_properties_names) { is_same_properties_names_ = is_same_properties_names; }
  //uint64_t get_checksum();
  //int get_sub_table_operation(ObTableBatchOperation &sub_batch_operation, const common::ObIArray<int64_t> &sub_index);
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableBatchOperation &other);
  int set_table_ops(const common::ObIArray<ObRpcFieldBuf> &table_op_buf);

  TO_STRING_KV(K_(is_readonly),
               K_(is_same_type),
               K_(is_same_properties_names),
               "operatiton_count",
               table_operations_.count(),
               K_(table_operations));

private:
  ObSEArray<OB_IGNORE_TABLE_OPERATION, SUB_REQ_COUNT> table_operations_;
  bool is_readonly_;
  bool is_same_type_;
  bool is_same_properties_names_;
  // do not serialize
  // ObITableEntityFactory *entity_factory_;
};

/// result for ObTableBatchOperation
class ObTableBatchOperationResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableBatchOperationResult()
      :table_operations_result_(common::ObModIds::TABLE_BATCH_OPERATION_RESULT, common::OB_MALLOC_NORMAL_BLOCK_SIZE)
       //entity_factory_(NULL),
       //alloc_(NULL)
  {}
  virtual ~ObTableBatchOperationResult() = default;
  void reset();
  //void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  //ObITableEntityFactory *get_entity_factory() { return entity_factory_; }
  const ObTableOperationResult &at(int64_t idx) const { return table_operations_result_.at(idx); }
  void remove(int64_t idx) { table_operations_result_.remove(idx); }
  // void push_back(const ObTableOperationResult &res) {table_operations_result_.push_back(res);}
  int push_back(const ObTableOperationResult &res);// {table_operations_result_.push_back(res);}
  //void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableBatchOperationResult &other);
  int64_t get_table_operation_result_count() { return table_operations_result_.count(); }
  //common::ObIAllocator *get_allocator() { return alloc_; }
  TO_STRING_KV("operatiton_result_count", table_operations_result_.count(),
               K_(table_operations_result));
private:
  ObSEArray<ObTableOperationResult, SUB_REQ_COUNT> table_operations_result_;
  //ObITableEntityFactory *entity_factory_;
  //common::ObIAllocator *alloc_;
};

class ObHTableConstants
{
public:
  static constexpr int64_t LATEST_TIMESTAMP = -INT64_MAX;
  static constexpr int64_t OLDEST_TIMESTAMP = INT64_MAX;
  static constexpr int64_t INITIAL_MIN_STAMP = 0;
  static constexpr int64_t INITIAL_MAX_STAMP = INT64_MAX;

  static const char* const ROWKEY_CNAME;
  static const char* const CQ_CNAME;
  static const char* const VERSION_CNAME;
  static const char* const VALUE_CNAME;
  static const ObString ROWKEY_CNAME_STR;
  static const ObString CQ_CNAME_STR;
  static const ObString VERSION_CNAME_STR;
  static const ObString VALUE_CNAME_STR;

  // create table t1$cf1 (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T));
  static const int64_t COL_IDX_K = 0;
  static const int64_t COL_IDX_Q = 1;
  static const int64_t COL_IDX_T = 2;
  static const int64_t COL_IDX_V = 3;
private:
  ObHTableConstants() = delete;
};

/// special filter for HTable
class ObHTableFilter final
{
  OB_UNIS_VERSION(1);
public:
  ObHTableFilter();
  ~ObHTableFilter() = default;
  void reset();
  void set_valid(bool valid) { is_valid_ = valid; }
  bool is_valid() const { return is_valid_; }

  /// Get the column with the specified qualifier.
  int add_column(const ObString &qualifier);
  /// Get versions of columns with the specified timestamp.
  int set_timestamp(int64_t timestamp) { min_stamp_ = timestamp; max_stamp_ = timestamp + 1; return common::OB_SUCCESS; }
  /// Get versions of columns only within the specified timestamp range, [minStamp, maxStamp).
  int set_time_range(int64_t min_stamp, int64_t max_stamp);
  /// Get up to the specified number of versions of each column.
  int set_max_versions(int32_t versions);
  /// Set the maximum number of values to return per row per Column Family
  /// @param limit - the maximum number of values returned / row / CF
  int set_max_results_per_column_family(int32_t limit);
  /// Set offset for the row per Column Family.
  /// @param offset - is the number of kvs that will be skipped.
  int set_row_offset_per_column_family(int32_t offset);
  /// Apply the specified server-side filter when performing the Query.
  /// @param filter - a file string using the hbase filter language
  /// @see the filter language at https://issues.apache.org/jira/browse/HBASE-4176
  int set_filter(const ObString &filter);

  const ObIArray<ObString> &get_columns() const { return select_column_qualifier_; }
  bool with_latest_timestamp() const { return with_all_time() && 1 == max_versions_; }
  bool with_timestamp() const { return min_stamp_ == max_stamp_ && min_stamp_ >= 0; }
  bool with_all_time() const { return ObHTableConstants::INITIAL_MIN_STAMP == min_stamp_ && ObHTableConstants::INITIAL_MAX_STAMP == max_stamp_; }
  int64_t get_min_stamp() const { return min_stamp_; }
  int64_t get_max_stamp() const { return max_stamp_; }
  int32_t get_max_versions() const { return max_versions_; }
  int32_t get_max_results_per_column_family() const { return limit_per_row_per_cf_; }
  int32_t get_row_offset_per_column_family() const { return offset_per_row_per_cf_; }
  const ObString &get_filter() const { return filter_string_; }
  void clear_columns() { select_column_qualifier_.reset(); }
  uint64_t get_checksum() const;
  int deep_copy(common::ObIAllocator &allocator, const ObHTableFilter &other);

  TO_STRING_KV(K_(is_valid),
               "column_qualifier", select_column_qualifier_,
               K_(min_stamp),
               K_(max_stamp),
               K_(max_versions),
               K_(limit_per_row_per_cf),
               K_(offset_per_row_per_cf),
               K_(filter_string));
private:
  bool is_valid_;
  ObSEArray<ObString, 16> select_column_qualifier_;
  int64_t min_stamp_;  // default -1
  int64_t max_stamp_;  // default -1
  int32_t max_versions_;  // default 1
  int32_t limit_per_row_per_cf_;  // default -1 means unlimited
  int32_t offset_per_row_per_cf_; // default 0
  ObString filter_string_;
};

/// A table query
/// 1. support multi range scan
/// 2. support reverse scan
/// 3. support secondary index scan
class ObTableQuery
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQuery()
      //:deserialize_allocator_(NULL),
     :key_ranges_(),
      select_columns_(),
      filter_string_(),
      limit_(-1),
      offset_(0),
      scan_order_(common::ObQueryFlag::Forward),
      index_name_(),
      batch_size_(-1),
      max_result_size_(-1),
      htable_filter_(),
      rowkey_columns_(),
      cluster_version_(0)
  {}
  ~ObTableQuery() = default;
  void reset();
  bool is_valid() const;

  /// add a scan range, the number of scan ranges should be >=1.
  int add_scan_range(common::ObNewRange &scan_range);
  /// Scan order: Forward (By default) and Reverse.
  int set_scan_order(common::ObQueryFlag::ScanOrder scan_order);
  /// Set the index to scan, could be 'PRIMARY' (by default) or any other secondary index.
  int set_scan_index(const ObString &index_name);
  /// Add select columns.
  int add_select_column(const ObString &columns);
  /// Add select columns.
  int add_rowkey_column(const ObString &rowkey_column);
  /// Set the max rows to return. The value of -1 represents there is NO limit. The default value is -1.
  /// For htable, set the limit of htable rows for this scan.
  int set_limit(int32_t limit);
  /// Set the offset to return. The default value is 0.
  int set_offset(int32_t offset);
  /// Add filter, currently NOT supported.
  int set_filter(const ObString &filter);
  /// Add filter only for htable.
  /// Set max row count of each batch.
  /// For htable, set the maximum number of cells to return for each call to next().
  int set_batch(int32_t batch_size);
  /// Set the maximum result size.
  /// The default is -1; this means that no specific maximum result size will be set for this query.
  /// @param max_result_size - The maximum result size in bytes.
  int set_max_result_size(int64_t max_result_size);

  void set_cluster_version(int64_t cluster_version) { cluster_version_ = cluster_version; }
  int64_t get_cluster_version() { return cluster_version_ ; }

  const ObIArray<ObString> &get_select_columns() const { return select_columns_; }

  int64_t get_rowkey_size() const { return rowkey_columns_.count(); } 
  const ObIArray<ObString> &get_rowkey_columns() const { return rowkey_columns_; }
  ObIArray<ObString> &get_rowkey_columns() { return rowkey_columns_; }
  const ObIArray<common::ObNewRange> &get_scan_ranges() const { return key_ranges_; }
   ObIArray<common::ObNewRange> &get_scan_ranges()  { return key_ranges_; }
  int32_t get_limit() const { return limit_; }
  int32_t get_offset() const { return offset_; }
  common::ObQueryFlag::ScanOrder get_scan_order() const { return scan_order_; }
  const ObString &get_index_name() const { return index_name_; }
  int32_t get_batch() const { return batch_size_; }
  int64_t get_max_result_size() const { return max_result_size_; }
  int64_t get_range_count() const { return key_ranges_.count(); }
  //uint64_t get_checksum() const;
  //int deep_copy(common::ObIAllocator &allocator, const ObTableQuery &other);

  void clear_scan_range() { key_ranges_.reset(); }
  //void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_allocator_ = allocator; }
  TO_STRING_KV(K_(key_ranges),
               K_(select_columns),
               K_(filter_string),
               K_(limit),
               K_(offset),
               K_(scan_order),
               K_(index_name),
               K_(htable_filter),
               K_(batch_size),
               K_(max_result_size),
               K_(rowkey_columns));
public:
  static ObString generate_filter_condition(const ObString &column, const ObString &op, const ObObj &value);
  static ObString combile_filters(const ObString &filter1, const ObString &op, const ObString &filter2);
  static common::ObNewRange generate_prefix_scan_range(const ObRowkey &rowkey_prefix);
protected:
  //common::ObIAllocator *deserialize_allocator_;
  ObSEArray<common::ObNewRange, ROWKEY_COLUMNS_COUNT> key_ranges_;
  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> select_columns_;
  ObString filter_string_;
  int32_t limit_;  // default -1 means unlimited
  int32_t offset_;
  common::ObQueryFlag::ScanOrder scan_order_;
  ObString index_name_;
  int32_t batch_size_;
  int64_t max_result_size_;
  OB_UNIS_IGNORE_HTABLE_FILTER htable_filter_;
  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> rowkey_columns_;
  int64_t cluster_version_;
};

/// result for ObTableQuery
class ObTableEntityIterator
{
public:
  ObTableEntityIterator() = default;
  virtual ~ObTableEntityIterator();
  /**
   * fetch the next entity
   * @return OB_ITER_END when finished
   */
  virtual int get_next_entity(const ObITableEntity *&entity) = 0;
};

/// query and mutate the selected rows.
class ObTableQueryAndMutate final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQueryAndMutate()
      :return_affected_entity_(true)
  {}
  const ObTableQuery &get_query() const { return query_; }
  ObTableQuery &get_query() { return query_; }
  bool return_affected_entity() const { return return_affected_entity_; }

  //void set_deserialize_allocator(common::ObIAllocator *allocator);
  //void set_entity_factory(ObITableEntityFactory *entity_factory);
  //uint64_t get_checksum();
  //int deep_copy(common::ObIAllocator &allocator, ObITableEntityFactory &entity_factory, const ObTableQueryAndMutate &other);

  void set_cluster_version(int64_t cluster_version) { query_.set_cluster_version(cluster_version); }
  int64_t get_cluster_version() { return query_.get_cluster_version(); }

  TO_STRING_KV(K_(query),
               K_(mutations));
private:
  ObTableQuery query_;
  OB_UNIS_IGNORE_TABLE_BATCH_OP mutations_;
  bool return_affected_entity_;
};

// inline void ObTableQueryAndMutate::set_deserialize_allocator(common::ObIAllocator *allocator)
// {
//   query_.set_deserialize_allocator(allocator);
// }

// inline void ObTableQueryAndMutate::set_entity_factory(ObITableEntityFactory *entity_factory)
// {
//   mutations_.set_entity_factory(entity_factory);
// }

class ObTableQueryResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableQueryResult();
  virtual ~ObTableQueryResult() {}
  void reset();
  void reset_except_property();
  void rewind();
  //virtual int get_next_entity(const ObITableEntity *&entity) override;
  // unused fun
  //int add_property_name(const ObString &name);
  //int add_row(const common::ObNewRow &row);
  //int get_first_row(common::ObNewRow &row) const;
  //int get_next_row(common::ObNewRow &row);
  //int64_t get_fetched_row_count() const { return next_row_count_; }
  //int get_empty_row(common::ObNewRow *&row);
  //virtual bool reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size);

  //int add_all_property_deep_copy(const ObTableQueryResult &other);
  //int add_all_row_deep_copy(const ObTableQueryResult &other);
  // used func
  int add_all_property_shallow_copy(const ObTableQueryResult &other);
  int add_all_row_shallow_copy(const ObTableQueryResult &other);

  int64_t get_row_count() const { return row_count_; }
  int64_t get_property_count() const { return properties_names_.count(); }
  int64_t get_result_size();
  int get_empty_obobj(ObObj *&obobj, int64_t count);
  common::ObDataBuffer &get_buf() { return buf_; }
  const common::ObDataBuffer &get_buf() const { return buf_; }
  //virtual int deep_copy(common::ObIAllocator &allocator, const ObTableQueryResult &other);
private:
// const int64_t OB_MAX_PACKET_BUFFER_LENGTH = (1 << 26) - (1 << 20); // buffer length for max packet, 63MB
  static const int64_t MAX_BUF_BLOCK_SIZE =  ((1 << 26) - (1 << 20)) - (1024*1024LL);
  static const int64_t DEFAULT_BUF_BLOCK_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE - (1024*1024LL);
  int alloc_buf_if_need(const int64_t size);
private:
  common::ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> properties_names_;            // serialize
  int64_t row_count_;                                                             // serialize
  common::ObDataBuffer buf_;                                                      // serialize
  common::ObSEArray<common::ObDataBuffer, SUB_REQ_COUNT> proxy_agg_data_buf_;     // serialize but not deserialize, handle by proxy

  common::ObArenaAllocator allocator_;
  //// unused field
  int64_t fixed_result_size_;
  // for deserialize and read
  int64_t curr_idx_;
  // ObTableEntity curr_entity_;
  int64_t next_row_offset_; //offset of buffer in data
  int64_t next_row_count_;  //count of row has fetched
};

class ObTableQueryAndMutateResult final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(affected_rows));

  //int deep_copy(common::ObIAllocator &allocator, const ObTableQueryAndMutateResult &other);
public:
  int64_t affected_rows_;
  // If return_affected_entity_ in ObTableQueryAndMutate is set, then return the respond entity.
  // In the case of delete and insert_or_update, return the old rows before modified.
  // In the case of increment and append, return the new rows after modified.
  ObTableQueryResult affected_entity_;
};

/* to support load data */
/// Table Direct Load Operation Type
enum class ObTableDirectLoadOperationType {
  BEGIN = 0,
  COMMIT = 1,
  ABORT = 2,
  GET_STATUS = 3,
  INSERT = 4,
  HEART_BEAT = 5,
  MAX_TYPE
};

/* to support ObTableMoveResult for rerote in RPC handle
 */

struct ObTableMoveReplicaInfo final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(table_id),
               K_(schema_version),
               K_(part_id),
               K_(server),
               K_(role),
               K_(replica_type),
               K_(part_renew_time),
               K_(reserved));
  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;
public:
  uint64_t table_id_;
  uint64_t schema_version_;
  uint64_t part_id_;
  common::ObAddr server_;
  common::ObRole role_;
  common::ObReplicaType replica_type_;
  int64_t part_renew_time_;
  uint64_t reserved_;
};

/* any OBKV table operation maybe return this to reroute next  */
class ObTableMoveResult final
{
  OB_UNIS_VERSION(1);
public:
  const ObTableMoveReplicaInfo &get_replica_info() const { return replica_info_; }
  const uint64_t get_resrved() { return reserved_; }
  TO_STRING_KV(K_(replica_info),
               K_(reserved));
  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;
private:
  ObTableMoveReplicaInfo replica_info_;
  uint64_t reserved_;
};

class ObTableQuerySyncResult: public ObTableQueryResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableQuerySyncResult()
    : is_end_(false),
      query_session_id_(0)
  {}
  virtual ~ObTableQuerySyncResult() {}
public:
  bool     is_end_;
  uint64_t  query_session_id_; // from server gen
};

struct ObTableDirectLoadResultHeader
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadResultHeader() : operation_type_(ObTableDirectLoadOperationType::MAX_TYPE) {}
  TO_STRING_KV(K_(addr), K_(operation_type));
public:
  common::ObAddr addr_;
  ObTableDirectLoadOperationType operation_type_;
};

class ObTableDirectLoadResult final
{
  OB_UNIS_VERSION(2);
public:
  ObTableDirectLoadResult() : allocator_(nullptr) {}
  template <class Res>
  int set_res(const Res &res, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = res.get_serialize_size();
    if (size > 0) {
      char *buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WDIAG, "fail to alloc memory", K(ret), K(size));
      } else if (OB_FAIL(res.serialize(buf, size, pos))) {
        SERVER_LOG(WDIAG, "fail to serialize res", K(ret), K(res));
      } else {
        res_content_.assign_ptr(buf, size);
      }
    }
    return ret;
  }
  template <class Res>
  int get_res(Res &res) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(res_content_.empty())) {
      ret = common::OB_INVALID_ARGUMENT;
      SERVER_LOG(WDIAG, "invalid args", K(ret), KPC(this));
    } else if (OB_FAIL(res.deserialize(res_content_.ptr(), res_content_.length(), pos))) {
      SERVER_LOG(WDIAG, "fail to deserialize res content", K(ret), KPC(this));
    }
    return ret;
  }
  // TO_STRING_KV(K_(header), "res_content", common::ObHexStringWrap(res_content_));
  TO_STRING_KV(K_(header), "res_content_len", res_content_.length());
public:
  common::ObIAllocator *allocator_; // for deserialize
  ObTableDirectLoadResultHeader header_;
  ObString res_content_;
};


// Compared to ObTableQuery, ObTableSingleOpQuery only changed the serialization/deserialization method
class ObTableSingleOpQuery final : public ObTableQuery
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableSingleOpQuery() : all_rowkey_names_(nullptr) {}
  ~ObTableSingleOpQuery() = default;

  //void reset();

  // OB_INLINE const common::ObIArray<common::ObNewRange> &get_scan_range() const
  // {
  //   return key_ranges_;
  // }
  // OB_INLINE common::ObIArray<common::ObNewRange> &get_scan_range()
  // {
  //   return key_ranges_;
  // }
  // OB_INLINE const common::ObString &get_index_name() const
  // {
  //   return index_name_;
  // }
  // OB_INLINE const ObString &get_filter_string() const
  // {
  //   return filter_string_;
  // }
  OB_INLINE int64_t get_scan_range_columns_count() const
  {
    return rowkey_columns_.count();
  }
  OB_INLINE void set_dictionary(const ObIArray<ObString> *all_rowkey_names)
  {
    all_rowkey_names_ = all_rowkey_names;
  }
  OB_INLINE bool has_dictionary() const
  {
    return OB_NOT_NULL(all_rowkey_names_);
  }
  // OB_INLINE const ObTableBitMap &get_scan_range_cols_bp() const
  // {
  //   return scan_range_cols_bp_;
  // }
  // OB_INLINE ObTableBitMap &get_scan_range_cols_bp()
  // {
  //   return scan_range_cols_bp_;
  // }
  TO_STRING_KV(K_(index_name),
               K_(key_ranges),
               K_(filter_string));
private:
  ObTableBitMap scan_range_cols_bp_; 
  const ObIArray<ObString> *all_rowkey_names_; // do not serialize 
};

class ObTableSingleOpEntity : public ObTableEntity {
  OB_UNIS_VERSION_WITH_REWRITE_INFO_V(1);

public:
  ObTableSingleOpEntity() : is_same_properties_names_(false), all_rowkey_names_(nullptr), all_properties_names_(nullptr)
  {}

  ~ObTableSingleOpEntity() = default;

  OB_INLINE virtual void set_is_same_properties_names(bool is_same_properties_names)
  {
    is_same_properties_names_ = is_same_properties_names;
  }

  // virtual void set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names) override
  // {
  //   all_rowkey_names_ = all_rowkey_names;
  //   all_properties_names_ = all_properties_names;
  // }

  // OB_INLINE virtual const ObIArray<ObString> *get_all_rowkey_names() const
  // {
  //   return all_rowkey_names_;
  // }
  // OB_INLINE virtual const ObIArray<ObString> *get_all_properties_names() const
  // {
  //   return all_properties_names_;
  // }

  OB_INLINE virtual const ObTableBitMap *get_rowkey_names_bp() const
  {
    return &rowkey_names_bp_;
  }

  OB_INLINE virtual ObTableBitMap *get_rowkey_names_bp()
  {
    return &rowkey_names_bp_;
  }


  OB_INLINE virtual const ObTableBitMap *get_properties_names_bp() const
  {
    return &properties_names_bp_;
  }
  OB_INLINE virtual ObTableBitMap *get_properties_names_bp()
  {
    return &properties_names_bp_;
  }

  virtual void reset() override;

  // must construct bitmap before serialization !!
  virtual int construct_names_bitmap(const ObITableEntity& req_entity) override;

  //virtual int deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other) override;

  static int construct_column_names(const ObTableBitMap &names_bit_map,
                                              const ObIArray<ObString> &all_column_names,
                                              ObIArray<ObString> &column_names);
  //virtual int deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other) override;

// private:

//   OB_INLINE bool has_dictionary() const
//   {
//     return OB_NOT_NULL(all_rowkey_names_) && OB_NOT_NULL(all_properties_names_);
//   }

protected:
  ObTableBitMap rowkey_names_bp_;
  ObTableBitMap properties_names_bp_;

  // no need serialization
  bool is_same_properties_names_;
  ObIArray<ObString> *all_rowkey_names_;
  ObIArray<ObString> *all_properties_names_;
};

// basic execute unit
class ObTableSingleOp
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableSingleOp()
      : op_type_(ObTableOperationType::INVALID),
        flag_(0),
        entities_(),
        op_query_(nullptr),
        all_rowkey_names_(nullptr),
        all_properties_names_(nullptr)
  {}

  ~ObTableSingleOp() = default;

  //OB_INLINE ObTableSingleOpQuery* get_query() { return op_query_; }
  //OB_INLINE const ObTableSingleOpQuery* get_query() const { return op_query_; }

  //OB_INLINE ObIArray<ObTableSingleOpEntity> &get_entities() { return entities_; }

  //OB_INLINE const ObIArray<ObTableSingleOpEntity> &get_entities() const { return entities_; }

  OB_INLINE ObTableOperationType::Type get_op_type() const { return op_type_; }

  // OB_INLINE void set_op_query(ObTableSingleOpQuery *op_query)
  // {
  //   op_query_ = op_query;
  // }

  // OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator)
  // {
  //   deserialize_alloc_ = allocator;
  // }

  OB_INLINE bool is_check_no_exists() const { return is_check_no_exists_; }

  // OB_INLINE void set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names)
  // {
  //   all_rowkey_names_ = all_rowkey_names;
  //   all_properties_names_ = all_properties_names;
  // }

  // OB_INLINE void set_is_same_properties_names(bool is_same)
  // {
  //   is_same_properties_names_ = is_same;
  // }
  //OB_INLINE void set_all_rowkey_names(ObIArray<ObString> *all_rowkey_names) { all_rowkey_names_ = all_rowkey_names; }
  //OB_INLINE void set_all_properties_names(ObIArray<ObString> *all_properties_names) { all_properties_names_ = all_properties_names; }
  OB_INLINE bool need_query() const { return op_type_ == ObTableOperationType::CHECK_AND_INSERT_UP; }

  //uint64_t get_checksum(); 

  void reset();

  TO_STRING_KV(K_(op_type),
               K_(flag),
               K_(is_check_no_exists),
               K_(op_query),
               K_(entities));
private:
  ObTableOperationType::Type op_type_;
  union
  {
    uint64_t flag_;
    struct
    {
      bool is_check_no_exists_ : 1;
      int64_t reserved : 63;
    };
  };
  ObSEArray<OB_IGNORE_TABLE_SINGLE_OP_ENTITY, 4> entities_;
  //common::ObIAllocator *deserialize_alloc_; // do not serialize
  OB_UNIS_IGNORE_TABLE_SINGLE_OP_QUERY *op_query_;
  ObIArray<ObString>* all_rowkey_names_; // do not serialize
  ObIArray<ObString>* all_properties_names_; // do not serialize
  //bool is_same_properties_names_ = false;
};

// A collection of single operations for a specified tablet
class ObTableTabletOp
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  static const int64_t COMMON_OPS_SIZE = 8;
public:
  ObTableTabletOp()
      : tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
        option_flag_(0),
        single_ops_()
        //all_rowkey_names_(nullptr),
        //all_properties_names_(nullptr),
        //is_ls_same_properties_names_(false)
  {}
  ~ObTableTabletOp() = default;
  OB_INLINE int64_t count() const { return single_ops_.count(); }
  //OB_INLINE void set_entity_factory(ObTableEntityFactory<ObTableSingleOpEntity> *entity_factory) { entity_factory_ = entity_factory; }
  //OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_alloc_ = allocator; }
  //OB_INLINE ObTableSingleOp &at(int64_t idx) { return single_ops_.at(idx); }
  OB_INLINE uint64_t get_tablet_id() const { return tablet_id_.id(); }
  OB_INLINE uint64_t get_option_flag() const { return option_flag_; }
  //OB_INLINE uint64_t get_is_ls_same_prop_name() const { return is_ls_same_properties_names_; }
  //OB_INLINE ObIArray<ObString>  *get_all_rowkey_names() { return all_rowkey_names_; } 
  //OB_INLINE ObIArray<ObString>  *get_all_properties_names() const { return all_properties_names_; } 
  OB_INLINE void set_tablet_id(const uint64_t tablet_id) { tablet_id_ =  tablet_id; }
  OB_INLINE void set_option_flag(const uint64_t option_flag) { option_flag_ =  option_flag; }
  //OB_INLINE void set_dictionary(ObIArray<ObString> *all_rowkey_names, ObIArray<ObString> *all_properties_names) {
  //  all_rowkey_names_ = all_rowkey_names;
  //  all_properties_names_ = all_properties_names;
  //}
  //OB_INLINE void set_is_ls_same_prop_name(bool is_same) {
  // is_ls_same_properties_names_ = is_same;
  //}
  int set_single_ops(const common::ObIArray<ObRpcFieldBuf> &single_op_buf);
  
  OB_INLINE ObSEArray<OB_IGNORE_TABLE_SINGLE_OP, 1> &get_single_ops() { return single_ops_;  } 
  OB_INLINE const ObSEArray<OB_IGNORE_TABLE_SINGLE_OP, 1> &get_single_ops() const { return single_ops_;  } 
  //int get_single_ops(ObIArray<ObTableSingleOp> &single_ops, const ObIArray<int64_t> &indexes) const;

  //OB_INLINE ObIArray<ObString>* get_all_rowkey_names() const { return all_rowkey_names_; }
  //OB_INLINE ObIArray<ObString>* get_all_properties_names() const { return all_properties_names_; }
  //OB_INLINE void set_all_properties_names(ObIArray<ObString> *all_properties_names) { all_properties_names_ = all_properties_names; }
  //OB_INLINE void set_all_rowkey_names(ObIArray<ObString> *all_rowkey_names) { all_rowkey_names_ = all_rowkey_names; }
  TO_STRING_KV(K_(tablet_id),
               K_(option_flag),
               K_(is_same_type),
               K_(is_same_properties_names),
               "single_ops_count", single_ops_.count(),
               K_(single_ops));
private:
  common::ObTabletID tablet_id_;
  union
  {
    uint64_t option_flag_;
    struct
    {
      bool is_same_type_ : 1;
      bool is_same_properties_names_ : 1;
      uint64_t reserved : 62;
    };
  };
  common::ObSEArray<OB_IGNORE_TABLE_SINGLE_OP, 1> single_ops_;
  //ObTableEntityFactory<ObTableSingleOpEntity> *entity_factory_; // do not serialize
  //common::ObIAllocator *deserialize_alloc_; // do not serialize
  //ObIArray<ObString>* all_rowkey_names_; // do not serialize
  //ObIArray<ObString>* all_properties_names_; // do not serialize
  //bool is_ls_same_properties_names_;
};


template <typename T>
struct ColumnsNameIndexPair
{
public:
  ColumnsNameIndexPair<T>() {}
  ColumnsNameIndexPair<T>(T value, uint32_t index) : value_(value), index_(index) {}
  ~ColumnsNameIndexPair<T>() {}
  bool operator < (const ColumnsNameIndexPair<T> &other) { return index_ < other.index_; }
public:
  T value_;
  uint32_t index_;
};

/*
  ObTableLSOp contains multi ObTableTabletOp belonging to the same LS,
  ObTableTabletOp contains multi ObTableSingleOp belonging to the same Tablet,
  ObTableSingleOp is a basic unit of operation
*/
class ObTableLSOp
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
// public:
//   static const int64_t MAX_BATCH_SIZE = 1000;
//   static const int64_t COMMON_BATCH_SIZE = 8;
public:
  ObTableLSOp()
    : ls_id_(common::ObLSID::INVALID_LS_ID),
      table_id_(common::OB_INVALID_ID),
      rowkey_names_(),
      properties_names_(),
      option_flag_(0),
      //entity_factory_(nullptr),
      //deserialize_alloc_(nullptr),
      tablet_ops_()
  {}
  void reset();
  //OB_INLINE void set_entity_factory(ObTableEntityFactory<ObTableSingleOpEntity> *entity_factory) { entity_factory_ = entity_factory; }
  //OB_INLINE void set_deserialize_allocator(common::ObIAllocator *allocator) { deserialize_alloc_ = allocator; }
  OB_INLINE int64_t count() const { return tablet_ops_.count(); }
  OB_INLINE int64_t get_ls_id() const { return ls_id_.id(); }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE const ObString &get_table_name() const {return table_name_;}
  OB_INLINE uint64_t get_option_flag() const { return option_flag_; }

  OB_INLINE void set_ls_id(const int64_t ls_id) {ls_id_ = ls_id;} 
  OB_INLINE void set_table_id(const int64_t table_id) {table_id_ = table_id;} 
  OB_INLINE void set_table_name(const common::ObString &table_name) {table_name_ = table_name;}
  OB_INLINE void set_option_flag(const uint64_t option_flag) { option_flag_ = option_flag; }
  //int64_t get_single_op_count() const; 

  OB_INLINE ObTableTabletOp &at(int64_t idx) { return tablet_ops_.at(idx); }
  OB_INLINE const ObTableTabletOp &at(int64_t idx) const { return tablet_ops_.at(idx); }
  const ObSEArray<ObString, 4> &get_all_rowkey_names() const { return rowkey_names_; }
  ObSEArray<ObString, 4> &get_all_rowkey_names() { return rowkey_names_; }
  const ObSEArray<ObString, 4> &get_all_properties_names() const { return properties_names_; }
  ObSEArray<ObString, 4> &get_all_properties_names() { return properties_names_; }
  OB_INLINE void set_dictionary(const ObSEArray<ObString, 4> &rowkey_names,
                                const ObSEArray<ObString, 4> &properties_names)
  {
    rowkey_names_ = rowkey_names;
    properties_names_ = properties_names;
  }
  OB_INLINE ObSEArray<ObTableTabletOp, SUB_REQ_COUNT>& get_tablet_ops() { return tablet_ops_; } 
  OB_INLINE const ObSEArray<ObTableTabletOp, SUB_REQ_COUNT>& get_tablet_ops() const { return tablet_ops_; } 
  OB_INLINE bool return_one_result() const { return return_one_result_; }
  OB_INLINE bool is_same_type() const { return is_same_type_; }
  TO_STRING_KV(K_(ls_id),
               K_(option_flag),
               "tablet_ops_count_", tablet_ops_.count(),
               K_(table_name),
               K_(table_id),
               K_(rowkey_names),
               K_(properties_names),
               K_(tablet_ops));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLSOp);
  common::ObLSID ls_id_;
  common::ObString table_name_; 
  uint64_t table_id_;
  ObSEArray<ObString, 4> rowkey_names_;
  ObSEArray<ObString, 4> properties_names_;
  union
  {
    uint64_t option_flag_;
    struct
    {
      bool is_same_type_ : 1;
      bool is_same_properties_names_ : 1;
      bool return_one_result_ : 1;
      bool need_all_prop_bitmap_ : 1;
      uint64_t reserved : 62;
    };
  };

  ObSEArray<ObTableTabletOp, SUB_REQ_COUNT> tablet_ops_;
};

class ObTableSingleOpResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTableSingleOpResult() : table_result_(), operation_type_(ObTableOperationType::GET), single_entity_(), affected_rows_(0) {}
  int64_t get_affected_rows() const { return affected_rows_; }
  void set_affected_rows(int64_t affected_rows) { affected_rows_ = affected_rows; }
  TO_STRING_KV(K(table_result_), K(operation_type_), K(single_entity_), K(affected_rows_));
private:
  OB_UNIS_IGNORE_TABLE_RESULT table_result_;
  ObTableOperationType::Type operation_type_;
  OB_UNIS_IGNORE_SINGLE_OP_ENTITY single_entity_;
  int64_t affected_rows_;
};

class ObTableTabletOpResult
{
  OB_UNIS_VERSION(1);
public:
 ObTableTabletOpResult()
     : single_op_result_(common::ObModIds::TABLE_LS_OPERATION_RESULT, ObTableTabletOp::COMMON_OPS_SIZE),
       all_properties_names_(NULL),
       all_rowkey_names_(NULL) {}
 virtual ~ObTableTabletOpResult() = default;
  OB_INLINE void set_all_properties_names(ObIArray<ObString> *all_properties_names) {
    all_properties_names_ = all_properties_names;
  }

  OB_INLINE void set_all_rowkey_names(ObIArray<ObString> *all_rowkey_names) {
    all_rowkey_names_ = all_rowkey_names;
  }
  ObSEArray<ObTableSingleOpResult, SUB_REQ_COUNT> &get_single_op_result() { return single_op_result_; }
  TO_STRING_KV(K_(single_op_result), K_(all_properties_names), K_(all_rowkey_names));
private:
  ObSEArray<ObTableSingleOpResult, SUB_REQ_COUNT>  single_op_result_; 
  uint64_t reserved_;
  ObIArray<ObString>* all_properties_names_;
  ObIArray<ObString>* all_rowkey_names_;
};

class ObTableLSOpResult
{
  OB_UNIS_VERSION(1);
public:
  ObTableLSOpResult()
    : tablet_op_result_(common::ObModIds::TABLE_LS_OPERATION_RESULT, SUB_REQ_COUNT) {}
  virtual ~ObTableLSOpResult() = default;
  int set_all_properties_names(const ObIArray<ObString>& all_properties_names) {
    return properties_names_.assign(all_properties_names);
  }
  int set_all_rowkey_names(const ObIArray<ObString>& all_rowkey_names) {
    return rowkey_names_.assign(all_rowkey_names);
  }
  ObSEArray<ObTableTabletOpResult, SUB_REQ_COUNT>  &get_tablet_op_result() { return tablet_op_result_; }
  OB_INLINE ObSEArray<ObString, 1> &get_rowkey_names() { return rowkey_names_; }
  OB_INLINE ObSEArray<ObString, 4> &get_properties_names() { return properties_names_; }
  TO_STRING_KV(K_(tablet_op_result), K_(rowkey_names), K_(properties_names));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLSOpResult);
  ObSEArray<ObTableTabletOpResult, SUB_REQ_COUNT> tablet_op_result_;
  // allways empty
  ObSEArray<ObString, 1> rowkey_names_;
  // Only when this batch of operations is read-only is it not empty.
  ObSEArray<ObString, 4> properties_names_;
  // do not serialize
};

struct ObTableApiCredential final
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t CREDENTIAL_BUF_SIZE = 256;
  ObTableApiCredential();
  ~ObTableApiCredential();
public:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  int64_t expire_ts_;     // not used
  uint64_t hash_val_;

  bool operator==(const ObTableApiCredential &other) const;
  bool operator!=(const ObTableApiCredential &other) const;
public:
  // int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  uint64_t hash() const { return hash_val_; };
  TO_STRING_KV(K_(cluster_id),
               K_(tenant_id),
               K_(user_id),
               K_(database_id),
               K_(expire_ts),
               K_(hash_val));
};

inline bool ObTableApiCredential::operator==(const ObTableApiCredential&other) const
{
  return ((cluster_id_ == other.cluster_id_)
          && (tenant_id_ == other.tenant_id_)
          && (user_id_ == other.user_id_)
          && (expire_ts_ == other.expire_ts_)
          && (hash_val_ == other.hash_val_));
}

inline bool ObTableApiCredential::operator!=(const ObTableApiCredential&other) const
{
  return !(*this == other);
}

} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase


#endif /* _OB_TABLE_TABLE_H */

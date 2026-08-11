/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TABLE_RPC_STRUCT_H
#define _OB_TABLE_RPC_STRUCT_H 1
#include "ob_table.h"
#include "common/data_buffer.h"
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

#define OB_TABLE_OPTION_DEFAULT INT64_C(0)
#define OB_TABLE_OPTION_RETURNING_ROWKEY (INT64_C(1) << 0)
#define OB_TABLE_OPTION_USE_PUT (INT64_C(1) << 1)
#define OB_TABLE_OPTION_RETURN_ONE_RES (INT64_C(1) << 2)
#define OB_TABLE_OPTION_SERVER_CAN_RETRY (INT64_C(1) << 3)
#define OB_TABLE_OPTION_DIS_NEED_TABLET_ID (INT64_C(1) << 4)

/// @see PCODE_DEF(OB_TABLE_API_LOGIN, 0x1101)
class ObTableLoginRequest final
{
  OB_UNIS_VERSION(1);
public:
  uint8_t auth_method_;  // always 1 for now
  uint8_t client_type_;  // 1: libobtable; 2: java client 3: hbase client
  uint8_t client_version_;  // always 1 for now
  uint8_t reserved1_;
  uint32_t client_capabilities_;
  uint32_t max_packet_size_;  // for stream result
  uint32_t reserved2_;  // always 0 for now
  uint64_t reserved3_;  // always 0 for now
  ObString tenant_name_;
  ObString user_name_;
  ObString pass_secret_;
  ObString pass_scramble_;  // 20 bytes random string
  ObString database_name_;
  int64_t ttl_us_;  // 0 means no TTL
  ObString client_info_;
  uint8_t allow_distribute_capability_;

public:
  ObTableLoginRequest() : auth_method_(0), client_type_(1),client_version_(1),
                          reserved1_(0), client_capabilities_(0), max_packet_size_(0),
                          reserved2_(0), reserved3_(0), tenant_name_(), user_name_(),
                          pass_secret_(), pass_scramble_(), database_name_(), ttl_us_(0),client_info_(),allow_distribute_capability_(0)
  {}
  ObTableLoginRequest(const ObTableLoginRequest &request) {
    auth_method_ = request.auth_method_;
    client_type_ = request.client_type_;
    client_version_ = request.client_version_;
    reserved1_ = request.reserved1_;
    client_capabilities_ = request.client_capabilities_;
    max_packet_size_ = request.max_packet_size_;
    reserved2_ = request.reserved2_;
    reserved3_ = request.reserved3_;
    tenant_name_ = request.tenant_name_;
    user_name_ = request.user_name_;
    pass_secret_ = request.pass_secret_;
    pass_scramble_ = request.pass_scramble_;
    database_name_ = request.database_name_;
    ttl_us_ = request.ttl_us_;
    client_info_ = request.client_info_;
    allow_distribute_capability_ = request.allow_distribute_capability_;
  }

  TO_STRING_KV(K_(auth_method),
               K_(client_type),
               K_(client_version),
               K_(reserved1),
               K_(client_capabilities),
               K_(max_packet_size),
               K_(reserved2),
               K_(reserved3),
               K_(tenant_name),
               K_(user_name),
               K_(database_name),
               K_(ttl_us),
               K_(client_info),
               K_(allow_distribute_capability));
};

class ObTableLoginResult final
{
  OB_UNIS_VERSION(1);
public:
  uint32_t server_capabilities_;
  uint32_t reserved1_;  // always 0 for now
  uint64_t reserved2_;  // always 0 for now
  ObString server_version_;
  ObString credential_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
public:
  TO_STRING_KV(K_(server_capabilities),
               K_(reserved1),
               K_(reserved2),
               K_(server_version),
               K_(credential),
               K_(tenant_id),
               K_(user_id),
               K_(database_id));
};

////////////////////////////////////////////////////////////////
/// @see PCODE_DEF(OB_TABLE_API_EXECUTE, 0x1102)
class ObTableOperationRequest final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableOperationRequest() : credential_(), table_name_(), table_id_(common::OB_INVALID_ID),
      partition_id_(common::OB_INVALID_ID), entity_type_(), table_operation_(),
      consistency_level_(), returning_rowkey_(false), returning_affected_entity_(false),
      returning_affected_rows_(false),
      binlog_row_image_type_(ObBinlogRowImageType::FULL)
      {}
  ~ObTableOperationRequest() {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(table_operation),
               K_(consistency_level),
               K_(returning_rowkey),
               K_(returning_affected_entity),
               K_(returning_affected_rows));
  
  // FOR v4
  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  //int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  //int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

  // FOR rewrite optimize
  //int deserialize_get_position(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);

public:
  /// the credential returned when login.
  ObString credential_;
  /// table name.
  ObString table_name_;
  /// table id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t table_id_;  // for optimize purpose
  /// partition id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t partition_id_;  // for optimize purpose
  /// entity type. Set it to gain better performance. If unknown, set it to be ObTableEntityType::DYNAMIC.
  ObTableEntityType entity_type_;  // for optimize purpose
  /// table operation.
  OB_IGNORE_TABLE_OPERATION table_operation_;
  /// read consistency level. currently only support STRONG.
  ObTableConsistencyLevel consistency_level_;
  /// Whether return the rowkey, currently the value MUST be false (In the case of Append/Increment the value could be true).
  bool returning_rowkey_;
  /// Whether return the row which has been modified, currently the value MUST be false (In the case of Append/Increment, the value could be true)
  bool returning_affected_entity_;
  /// Whether return affected_rows
  bool returning_affected_rows_;
  /// Whether record the full row in binlog of modification
  ObBinlogRowImageType binlog_row_image_type_;
};

////////////////////////////////////////////////////////////////
/// batch operation of ONE partition
/// @see PCODE_DEF(OB_TABLE_API_BATCH_EXECUTE, 0x1103)
class ObTableBatchOperationRequest final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableBatchOperationRequest() : credential_(), table_name_(), table_id_(common::OB_INVALID_ID),
      partition_id_(common::OB_INVALID_ID), entity_type_(), batch_operation_(),
      consistency_level_(), option_flag_(OB_TABLE_OPTION_DEFAULT), returning_affected_entity_(false),
      returning_affected_rows_(false),batch_operation_as_atomic_(false),
      binlog_row_image_type_(ObBinlogRowImageType::FULL)
      {}
  ~ObTableBatchOperationRequest() {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(batch_operation),
               K_(consistency_level),
               K_(option_flag),
               K_(returning_affected_entity),
               K_(returning_affected_rows),
               K_(batch_operation_as_atomic));

  // FOR v4
  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  //int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  //int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

  // FOR rewrite optimize
  //int deserialize_get_position(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);

public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// partition id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t partition_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableBatchOperation batch_operation_;
  // Only support STRONG
  ObTableConsistencyLevel consistency_level_;
  // Only support false (Support true for only Append/Increment)
  uint8_t option_flag_;
  // Only support false (Support true for only Append/Increment)
  bool returning_affected_entity_;
  /// whether return affected_rows
  bool returning_affected_rows_;
  // batch oepration suppoert atomic operation
  bool batch_operation_as_atomic_;
  /// Whether record the full row in binlog of modification
  ObBinlogRowImageType binlog_row_image_type_;
};

enum class ObHBaseOperationType : int
{
  INVALID = 0,
  PUT = 1,
  PUT_LIST = 2,
  DELETE = 3,
  DELETE_LIST = 4,
  GET = 5,
  GET_LIST = 6,
  EXISTS = 7,
  EXISTS_LIST = 8,
  BATCH = 9,
  BATCH_CALLBACK = 10,
  SCAN = 11,
  CHECK_AND_PUT = 12,
  CHECK_AND_DELETE = 13,
  CHECK_AND_MUTATE = 14,
  APPEND = 15,
  INCREMENT = 16,
  INCREMENT_COLUMN_VALUE = 17,
  MUTATE_ROW = 18
};
////////////////////////////////////////////////////////////////
// @see PCODE_DEF(OB_TABLE_API_EXECUTE_QUERY, 0x1104)
class ObTableQueryRequest
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQueryRequest()
      :table_id_(common::OB_INVALID_ID),
       partition_id_(common::OB_INVALID_ID),
       entity_type_(ObTableEntityType::ET_DYNAMIC),
       consistency_level_(ObTableConsistencyLevel::STRONG),
       option_flag_(OB_TABLE_OPTION_DEFAULT),
       is_need_option_flag_(false)
  {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(consistency_level),
               K_(query),
               K_(option_flag),
               K_(is_need_option_flag));

  OB_INLINE bool is_distribute_need_tablet_id() const { return option_flag_ & OB_TABLE_OPTION_DIS_NEED_TABLET_ID; }

  // FOR v4
  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  //int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  //int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

  // FOR rewrite optimize
  //int deserialize_get_position(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);

public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// partition id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t partition_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  // only support STRONG
  ObTableConsistencyLevel consistency_level_;
  OB_IGNORE_TABLE_QUERY query_;
  uint8_t option_flag_;
  bool is_need_option_flag_;
};

class ObTableQueryResultIterator
{
public:
  ObTableQueryResultIterator() {}
  virtual ~ObTableQueryResultIterator() {}
  virtual int get_next_result(ObTableQueryResult *&one_result) = 0;
  virtual bool has_more_result() const = 0;
};

class ObTableQueryAndMutateRequest final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQueryAndMutateRequest()
      :table_id_(common::OB_INVALID_ID),
      partition_id_(common::OB_INVALID_ID),
      binlog_row_image_type_(ObBinlogRowImageType::FULL),
      hbase_op_type_(ObHBaseOperationType::INVALID),
      option_flag_(OB_TABLE_OPTION_DEFAULT),
      is_need_option_flag_(false),
      is_need_hbase_op_type_(false)
  {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(query_and_mutate),
               K_(hbase_op_type),
               K_(option_flag),
               K_(is_need_option_flag),
               K_(is_need_hbase_op_type));

  OB_INLINE bool is_distribute_need_tablet_id() const { return option_flag_ & OB_TABLE_OPTION_DIS_NEED_TABLET_ID; }

  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  //int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  //int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

  // FOR rewrite optimize
  //int deserialize_get_position(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);

public:
  ObString credential_;
  ObString table_name_;
  uint64_t table_id_;  // for optimize purpose
  /// partition id. Set it to gain better performance. If unknown, set it to be OB_INVALID_ID
  uint64_t partition_id_;  // for optimize purpose
  ObTableEntityType entity_type_;  // for optimize purpose
  OB_IGNORE_TABLE_QUERY_AND_MUTATE query_and_mutate_;
  ObBinlogRowImageType binlog_row_image_type_;
  ObHBaseOperationType hbase_op_type_;
  uint8_t option_flag_;
  bool is_need_option_flag_;
  /// set when payload contains trailing \ref hbase_op_type_ after optional \ref option_flag_
  bool is_need_hbase_op_type_;
};

class ObTableQuerySyncRequest : public ObTableQueryRequest
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQuerySyncRequest()
      :query_session_id_(0),
       query_type_(ObQueryOperationType::QUERY_MAX),
       hbase_op_type_(ObHBaseOperationType::INVALID),
       is_need_hbase_op_type_(false)
  {}
  virtual ~ObTableQuerySyncRequest(){}

  int serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const;
  int serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const;
  //int deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos);
  //int deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

  // FOR rewrite optimize
  //int deserialize_get_position(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  //int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);

  // bool is_valid() const { return ObQueryOperationType::QUERY_START == query_type_ || ObQueryOperationType::QUERY_NEXT == query_type_; }
  bool is_valid() const { return (ObQueryOperationType::QUERY_START <= query_type_ && ObQueryOperationType::QUERY_MAX > query_type_); }

  INHERIT_TO_STRING_KV("ObTableQueryRequest", ObTableQueryRequest, K_(query_session_id), K_(query_type), K_(hbase_op_type), K_(is_need_hbase_op_type));

public:
  uint64_t query_session_id_;
  ObQueryOperationType query_type_;
  ObHBaseOperationType hbase_op_type_;
  bool is_need_hbase_op_type_;
};

struct ObTableDirectLoadRequestHeader
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectLoadRequestHeader() : operation_type_(ObTableDirectLoadOperationType::MAX_TYPE) {}
  TO_STRING_KV(K_(addr), K_(operation_type));
public:
  common::ObAddr addr_;
  ObTableDirectLoadOperationType operation_type_;
};

class ObTableDirectLoadRequest
{
  OB_UNIS_VERSION(2);
public:
  ObTableDirectLoadRequest() {}

  //not be used in OBProxy now
  template <class Arg>
  int set_arg(const Arg &arg, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = arg.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WDIAG, "fail to alloc memory", K(ret), K(size));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      SERVER_LOG(WDIAG, "fail to serialize arg", K(ret), K(arg));
    } else {
      arg_content_.assign_ptr(buf, size);
    }
    return ret;
  }

  //not be used in OBProxy now
  template <class Arg>
  int get_arg(Arg &arg) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(arg_content_.empty())) {
      ret = common::OB_INVALID_ARGUMENT;
      SERVER_LOG(WDIAG, "invalid args", K(ret), KPC(this));
    } else if (OB_FAIL(arg.deserialize(arg_content_.ptr(), arg_content_.length(), pos))) {
      SERVER_LOG(WDIAG, "fail to deserialize arg content", K(ret), KPC(this));
    }
    return ret;
  }
  TO_STRING_KV(K_(header),
               "credential", (credential_),
               "arg_content", (arg_content_));
  int deserialize_get_position_v4(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
  int deserialize_get_position_v4_(const char *buf, int64_t buf_len, int64_t &pos, REWRITE_INFO_ARG);
public:
  ObTableDirectLoadRequestHeader header_;
  ObString credential_;
  ObString arg_content_;
};

class ObTableLSOpRequest final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableLSOpRequest()
    : credential_(),
      entity_type_(),
      consistency_level_(),
      ls_op_(),
      hbase_op_type_(ObHBaseOperationType::INVALID),
      is_need_hbase_op_type_(false)
  {
  }
  ~ObTableLSOpRequest() {}

  TO_STRING_KV(K_(credential),
               K_(entity_type),
               K_(consistency_level),
               K_(ls_op),
               K_(hbase_op_type),
               K_(is_need_hbase_op_type));
public:
  ObString credential_;
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableConsistencyLevel consistency_level_;
  ObTableLSOp ls_op_;
  ObHBaseOperationType hbase_op_type_;
  bool is_need_hbase_op_type_;
};

struct ObObkvGetRouteOperationType
{
  enum Type
  {
    GETROUTE = 0,
    INVALID = 1
  };
};

class ObObkvGetRouteRequest final
{
  OB_UNIS_VERSION(1);
public:
  /// the credential returned when login.
  ObObkvGetRouteOperationType::Type type_;
  ObString credential_;
  /// table name.
  ObString table_name_;
  ObString cluster_name_;
  ObString tenant_name_;
  ObString database_name_;
  bool force_renew_;

  TO_STRING_KV(K_(type),
               K_(credential),
               K_(table_name),
               K_(cluster_name),
               K_(tenant_name),
               K_(database_name));
};

enum class ObTableRpcMetaType : uint8_t
{
  INVALID = 0,
  TABLE_PARTITION_INFO = 1,     // route refresh
  HTABLE_REGION_LOCATOR = 2,    // table region locator
  HTABLE_REGION_METRICS = 3,    // table region metrics
  HTABLE_CREATE_TABLE = 4,	    // create table
  HTABLE_DELETE_TABLE = 5,	    // delete table
  HTABLE_TRUNCATE_TABLE = 6,	  // truncate table
  HTABLE_EXISTS = 7,		        // check table existence
  HTABLE_GET_DESC = 8,	        // table descriptor
  HTABLE_ENABLE_TABLE = 9,      // enable table
  HTABLE_DISABLE_TABLE = 10,    // disable table
  HTABLE_META_MAX = 255
};

class ObTableMetaRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObTableMetaRequest()
      : credential_(),
        meta_type_(ObTableRpcMetaType::INVALID),
        data_() {}
  ~ObTableMetaRequest() = default;
  TO_STRING_KV(K_(credential),
               K_(meta_type),
               K_(data));

public:
  ObString credential_;
  ObTableRpcMetaType meta_type_;
  ObString data_;
};

class ObHbaseOperationRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObHbaseOperationRequest() : credential_(), table_name_(), op_type_(ObTableOperationType::INVALID),
                              keys_(common::ObModIds::OB_RPC_HBASE_OPERATION, 4 * sizeof(ObObj)),
                              same_cf_rows_(common::ObModIds::OB_RPC_HBASE_OPERATION, 4 * sizeof(ObHbaseCfRow)) {}
  ~ObHbaseOperationRequest() = default;
  OB_INLINE bool is_valid() const { return op_type_ == ObTableOperationType::INSERT_OR_UPDATE; } // only support hbase put now
  OB_INLINE int64_t get_key_index() const { return same_cf_rows_.at(0).key_indexs_.at(0);}
  OB_INLINE ObString get_table_name() const { return same_cf_rows_.at(0).column_family_;}
  OB_INLINE int get_column_Q_value(ObObj &obj) const { return same_cf_rows_.at(0).cells_.at(0).get_column_Q_value(obj);}
  OB_INLINE int get_column_T_value(ObObj &obj) const { return same_cf_rows_.at(0).cells_.at(0).get_column_T_value(obj);}
  int get_column_K_value(ObObj &obj) const;
  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(op_type),
               K_(keys),
               K_(same_cf_rows));
public:
  ObString credential_;
  ObString table_name_;
  union
  {
    uint64_t option_flag_;
    struct {
      uint64_t reserved:64;
    };
  };
  ObTableOperationType::Type op_type_;
  ObSEArray<ObObj, 4> keys_;
  ObSEArray<ObHbaseCfRow, 4> same_cf_rows_;
};

} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_STRUCT_H */

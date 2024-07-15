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

/// @see PCODE_DEF(OB_TABLE_API_LOGIN, 0x1101)
class ObTableLoginRequest final
{
  OB_UNIS_VERSION(1);
public:
  uint8_t auth_method_;  // always 1 for now
  uint8_t client_type_;  // 1: libobtable; 2: java client
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
public:
  ObTableLoginRequest() : auth_method_(0), client_type_(1),client_version_(1),
                          reserved1_(0), client_capabilities_(0), max_packet_size_(0),
                          reserved2_(0), reserved3_(0), tenant_name_(), user_name_(),
                          pass_secret_(), pass_scramble_(), database_name_(), ttl_us_(0)
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
               K_(ttl_us));
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
       consistency_level_(ObTableConsistencyLevel::STRONG)
  {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(consistency_level),
               K_(query));

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
      binlog_row_image_type_(ObBinlogRowImageType::FULL)
  {}

  TO_STRING_KV(K_(credential),
               K_(table_name),
               K_(table_id),
               K_(partition_id),
               K_(entity_type),
               K_(query_and_mutate));

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
};

class ObTableQuerySyncRequest : public ObTableQueryRequest
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObTableQuerySyncRequest()
      :query_session_id_(0),
       query_type_(ObQueryOperationType::QUERY_MAX)
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

  bool is_valid() const { return ObQueryOperationType::QUERY_START == query_type_ || ObQueryOperationType::QUERY_NEXT == query_type_; }

  INHERIT_TO_STRING_KV("ObTableQueryRequest", ObTableQueryRequest, K_(query_session_id), K_(query_type));

public:
  uint64_t query_session_id_;
  ObQueryOperationType query_type_;
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
      ls_op_()
  {
  }
  ~ObTableLSOpRequest() {}

  TO_STRING_KV(K_(credential),
               K_(entity_type),
               K_(consistency_level),
               K_(ls_op));
public:
  ObString credential_;
  ObTableEntityType entity_type_;  // for optimize purpose
  ObTableConsistencyLevel consistency_level_;
  ObTableLSOp ls_op_;
};


} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_STRUCT_H */

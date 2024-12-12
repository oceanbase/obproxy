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

#ifndef _OB_REDIS_RPC_REQUEST_H
#define _OB_REDIS_RPC_REQUEST_H 1

#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h" //ObHashMap
#include "lib/hash/ob_hashutils.h"
#include "obproxy/obkv/table/ob_table_rpc_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

const ObString REDIS_INDEX_NAME = "index";
const ObString REDIS_VALUE_NAME = "value";
const ObString REDIS_EXPIRE_NAME = "EXPIRE_TS";
const ObString REDIS_PROPERTY_NAME = "REDIS_CODE_STR";
const ObString DB_PROPERTY_NAME = "db";
const ObString RKEY_PROPERTY_NAME = "rkey";

//class ObRedisOperationRequest
//{
//  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
//
//public:
//  ObRedisOperationRequest()
//      : credential_(), table_name_(), table_id_(common::OB_INVALID_ID), tablet_id_(),
//        entity_type_(), rowkey_(), properties_names_(), properties_values_(), rowkey_names_(),
//        operation_type_(ObTableOperationType::REDIS), consistency_level_(), returning_rowkey_(false),
//        returning_affected_entity_(false), returning_affected_rows_(false),
//        binlog_row_image_type_(ObBinlogRowImageType::FULL)
//  {
//  }
//
//  ~ObRedisOperationRequest() {}
//
//  TO_STRING_KV(K_(credential),
//               K_(table_name),
//               K_(table_id),
//               K_(tablet_id),
//               K_(entity_type),
//               K_(rowkey),
//               K_(properties_names),
//               K_(properties_values),
//               K_(rowkey_names),
//               K_(consistency_level),
//               K_(returning_rowkey),
//               K_(returning_affected_entity),
//               K_(returning_affected_rows));
//
//public:
//  ObString credential_;
//  ObString table_name_;
//  uint64_t table_id_;
//  common::ObTabletID tablet_id_;
//  ObTableEntityType entity_type_;
//  ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> rowkey_;
//  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> properties_names_;
//  ObSEArray<ObObj, ROWKEY_COLUMNS_COUNT> properties_values_;
//  ObSEArray<ObString, ROWKEY_COLUMNS_COUNT> rowkey_names_;
//  ObTableOperationType::Type operation_type_;
//  ObTableConsistencyLevel consistency_level_;
//  bool returning_rowkey_;
//  bool returning_affected_entity_;
//  bool returning_affected_rows_;
//  ObBinlogRowImageType binlog_row_image_type_;
//};

class ObRedisOperationRequest final
{
  OB_UNIS_VERSION_WITH_REWRITE_INFO(1);
public:
  ObRedisOperationRequest() : credential_(), table_name_(), table_id_(common::OB_INVALID_ID),
      partition_id_(common::OB_INVALID_ID), entity_type_(), table_operation_(),
      consistency_level_(), returning_rowkey_(false), returning_affected_entity_(false),
      returning_affected_rows_(false),
      binlog_row_image_type_(ObBinlogRowImageType::FULL)
      {}
  ~ObRedisOperationRequest() {}

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

  int64_t get_serialize_size_v4(void) const;
  int64_t get_serialize_size_v4_(void) const;

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
  ObTableOperation table_operation_;
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

// impl ObRpcTableOperationRequest
class ObRpcRedisOperationRequest : public ObRpcRequest
{
public:
  ObRpcRedisOperationRequest() : redis_table_request_() {}
  ~ObRpcRedisOperationRequest() {}
  uint64_t get_table_id() const override { return redis_table_request_.table_id_; }
  // uint64_t get_partition_id() const override { return redis_table_request_.tablet_id_.id(); }
  uint64_t get_partition_id() const override { return redis_table_request_.partition_id_; }
  common::ObString get_credential() const override { return redis_table_request_.credential_; }
  common::ObString get_table_name() const override { return redis_table_request_.table_name_; }
  ObTableEntityType get_entity_type() const override { return ObTableEntityType::ET_DYNAMIC; }
  void set_entity_type(ObTableEntityType type) override { UNUSED(type); }
  bool is_hbase_request() const override { return false; }
  bool is_read_weak() const override { return obkv::ObTableConsistencyLevel::STRONG != redis_table_request_.consistency_level_; }
  int calc_partition_id(common::ObArenaAllocator &allocator,
                        proxy::ObRpcReq &ob_rpc_req,
                        proxy::ObProxyPartInfo &part_info,
                        int64_t &partition_id) override;

  void set_table_id(uint64_t table_id) override {redis_table_request_.table_id_ = table_id; }
  // void set_partition_id(uint64_t part_id) override { redis_table_request_.tablet_id_ = ObTabletID(part_id); }
  void set_partition_id(uint64_t part_id) override { redis_table_request_.partition_id_ = part_id; }

  int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  int64_t get_encode_size() const override;
  int analyze_request(const char *buf, const int64_t len, int64_t &pos) override;

  ObRedisOperationRequest &get_redis_operation_request() { return redis_table_request_; }
  // ObTableOperationRequest &get_redis_operation_request() { return redis_table_request_; }

  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(redis_table_request));
private:
  ObRedisOperationRequest redis_table_request_;
  // ObTableOperationRequest redis_table_request_;
};


class ObRpcRedisAuthRequest : public ObRpcRedisRequest
{
public:
  ObRpcRedisAuthRequest() : full_user_name_(), password_() {}
  ~ObRpcRedisAuthRequest() {}
  int decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) const override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buf, buf_len, pos);
    return ret;
  }
  int64_t get_encode_size() const override { return 0; }
public:
  // SFINAE for op_reclaim_alloc
  static const int64_t OP_LOCAL_NUM = 64;

  ObString full_user_name_;
  ObString password_;
};

class ObRpcRedisCommonRequest : public ObRpcRedisRequest
{
public:
  int decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) const override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buf, buf_len, pos);
    return ret;
  }
  int64_t get_encode_size() const override { return 0; }
  int calc_partition_id(common::ObArenaAllocator &allocator,
                        proxy::ObRpcReq &ob_rpc_req,
                        proxy::ObProxyPartInfo &part_info,
                        int64_t &partition_id) override;
public:
  // SFINAE for op_reclaim_alloc
  static const int64_t OP_LOCAL_NUM = 256;
};

class ObRpcRedisInternalRequest : public ObRpcRedisRequest
{
public:
  int decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) const override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buf, buf_len, pos);
    return ret;
  }
  int64_t get_encode_size() const override { return 0; }
};

class ObRpcRedisGlobalRequest : public ObRpcRedisRequest
{
public:
  int decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db) override;
  int encode(char *buf, int64_t &buf_len, int64_t &pos) const override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buf, buf_len, pos);
    return ret;
  }
  int64_t get_encode_size() const override { return 0; }
  int calc_partition_id(common::ObArenaAllocator &allocator,
                        proxy::ObRpcReq &ob_rpc_req,
                        proxy::ObProxyPartInfo &part_info,
                        int64_t &partition_id) override;

};

} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase
#endif

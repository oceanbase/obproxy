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

#include "obkv/redis/ob_redis_rpc_request.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "proxy/rpc/redis/ob_rpc_redis_command_factory.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{
// TODO use memcpy to reduce encode cost
//---------------------------------ObRedisOperationSimplifiedRequest--------------------//
OB_UNIS_DEF_SERIALIZE(ObRedisOperationSimplifiedRequest,
                    credential_,
                    redis_db_,
                    tablet_id_,
                    table_id_,
                    reserved_,
                    resp_str_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObRedisOperationSimplifiedRequest,
                    credential_,
                    redis_db_,
                    tablet_id_,
                    table_id_,
                    reserved_,
                    resp_str_);

int ObRedisOperationSimplifiedRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_HEADER(UNIS_VERSION, len);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_v4_(buf, buf_len, pos))) {
      LOG_WDIAG("serialize fail", K(ret));
    }
  }
  return ret;
}

int ObRedisOperationSimplifiedRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_ENCODE,
              credential_,
              redis_db_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(ls_id_)))) {
      LOG_WDIAG("serialize tablet ID failed", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(tablet_id_)))) {
      LOG_WDIAG("serialize tablet ID failed", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              table_id_,
              reserved_,
              resp_str_);
  return ret;
}

int64_t ObRedisOperationSimplifiedRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObRedisOperationSimplifiedRequest::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(CLS);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              credential_,
              redis_db_);

  len += 8;   // ls_id
  len += 8;   // tablet_id

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              table_id_,
              reserved_,
              resp_str_);
  return len;
}

//-------------------------------ObRpcRedisOperationSimplifiedRequest--------------------//
int ObRpcRedisOperationSimplifiedRequest::calc_partition_id(ObArenaAllocator &allocator,
                                                  ObRpcReq &ob_rpc_req,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSEDx(allocator, ob_rpc_req, part_info, partition_id);
  LOG_WDIAG("unexpected partition calculation called", KPC(this), K(ret), K(lbt()));
  return ret;
}

int ObRpcRedisOperationSimplifiedRequest::analyze_request(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSEDx(buf, len , pos);
  LOG_WDIAG("unexpected analyze request called", KPC(this), K(ret), K(lbt()));
  return ret;
}


int ObRpcRedisOperationSimplifiedRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{

  LOG_DEBUG("ObRpcRedisOperationSimplifiedRequest::encode", K(buf), K(buf_len), K(pos));
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;    // 将pos设置为meta之后
  check_sum_pos = pos; // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableOperationRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化table_request
    LOG_DEBUG("ObRpcRedisOperationSimplifiedRequest::encode",  K_(&redis_table_simplified_request), K(this), K_(redis_table_simplified_request), K(buf), K(buf_len), K(pos));
    // if (OB_FAIL(redis_table_request_.serialize(buf, buf_len, pos))) {
    //   LOG_WDIAG("fail to serialize for table request");
    // }
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableOperationRequest::encode less v4, can not come here", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(redis_table_simplified_request_);
    } else {
      LOG_DEBUG("ObRpcTableOperationRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(redis_table_simplified_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for table request");
      }
    }
    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      // TODO 考虑header部分进行memcpy减少序列化流程
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        LOG_DEBUG("ObRpcRedisOperationSimplifiedRequest::encode succ", K(buf), K(buf_len), K(pos));
        // success
      }
    }
  }
  return ret;
}

int64_t ObRpcRedisOperationSimplifiedRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  len += redis_table_simplified_request_.get_serialize_size();
  return len;
}
//---------------------------------ObRedisOperationRequest------------------------------//
// v3 cluster serialize
OB_UNIS_DEF_SERIALIZE(ObRedisOperationRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    partition_id_,
                    entity_type_,
                    table_operation_,
                    consistency_level_,
                    returning_rowkey_,
                    returning_affected_entity_,
                    returning_affected_rows_,
                    binlog_row_image_type_);

// v3 cluster serailize
OB_UNIS_DEF_SERIALIZE_SIZE(ObRedisOperationRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    partition_id_,
                    entity_type_,
                    table_operation_,
                    consistency_level_,
                    returning_rowkey_,
                    returning_affected_entity_,
                    returning_affected_rows_,
                    binlog_row_image_type_);

void ObRedisOperationRequest::reset()
{
  credential_.reset();
  table_name_.reset();
  table_id_ = common::OB_INVALID_ID;
  partition_id_ = common::OB_INVALID_ID;
  table_operation_.reset();
}
int ObRedisOperationRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_HEADER(UNIS_VERSION, len);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialize_v4_(buf, buf_len, pos))) {
      LOG_WDIAG("serialize fail", K(ret));
    }
  }
  return ret;
}

int ObRedisOperationRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_ENCODE,
              credential_,
              table_name_,
              table_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(partition_id_)))) {
      LOG_WDIAG("serialize tablet ID failed", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              entity_type_,
              table_operation_,
              consistency_level_,
              returning_rowkey_,
              returning_affected_entity_,
              returning_affected_rows_,
              binlog_row_image_type_);
  return ret;
}

int64_t ObRedisOperationRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObRedisOperationRequest::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(CLS);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              credential_,
              table_name_,
              table_id_);

  len += 8;   // tablet_id

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              entity_type_,
              table_operation_,
              consistency_level_,
              returning_rowkey_,
              returning_affected_entity_,
              returning_affected_rows_,
              binlog_row_image_type_);
  return len;
}

//ODP_DEF_DESERIALIZE_HEADER(ObRedisOperationRequest)
//{
//  int ret = OK_;
//  int64_t version = 0;
//  int64_t len = 0;
//  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
//  if (OB_SUCC(ret)) {
//    int64_t pos_orig = pos;
//    pos = 0;
//    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
//      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
//    }
//    rpc_request->table_id_position_ += pos_orig;
//    rpc_request->partition_id_position_ += pos_orig;
//    pos = pos_orig + len;
//  }
//  return ret;
//}
//
//ODP_DEF_DESERIALIZE_PAYLOAD(ObRedisOperationRequest)
//{
//  int ret = OK_;
//  BASE_SER(CLS);
//  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
//  // record table id pos and len
//  rpc_request->table_id_position_ = pos;
//  OB_UNIS_DECODE(table_id_);
//  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;
//
//  // record table id
//  if (OB_SUCC(ret)) {
//    rpc_request->partition_id_position_ = pos;
//    if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
//      OB_UNIS_DECODE(partition_id_);
//    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
//      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
//    }
//    rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;
//  }
//
//  LST_DO_CODE(OB_UNIS_DECODE, entity_type_);
//  if (OB_SUCC(ret) && OB_FAIL(rpc_request->init_rowkey_info(1))) {
//    LOG_WDIAG("fail to init sub req", K(ret));
//  }
//  if (OB_SUCC(ret) && OB_FAIL(table_operation_.deserialize(buf, data_len, pos, rpc_request))) {
//    LOG_WDIAG("fail to deserialize table operation", K(ret));
//  }
//  LST_DO_CODE(OB_UNIS_DECODE, consistency_level_, returning_rowkey_, returning_affected_entity_,
//              returning_affected_rows_, binlog_row_image_type_);
//  return ret;
//
//}

int ObRpcRedisOperationRequest::calc_partition_id(ObArenaAllocator &allocator,
                                                  ObRpcReq &ob_rpc_req,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSEDx(allocator, ob_rpc_req, part_info, partition_id);
  LOG_WDIAG("unexpected partition calculation called", KPC(this), K(ret), K(lbt()));
  return ret;
}

int ObRpcRedisOperationRequest::analyze_request(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSEDx(buf, len , pos);
  LOG_WDIAG("unexpected analyze request called", KPC(this), K(ret), K(lbt()));
  return ret;
}


int ObRpcRedisOperationRequest::encode(char *buf, int64_t &buf_len, int64_t &pos)
{

  LOG_DEBUG("ObRpcRedisOperationRequest::encode", K(buf), K(buf_len), K(pos));
  int ret = OB_SUCCESS;
  int64_t meta_size = rpc_packet_meta_.get_serialize_size();
  int64_t origin_pos = pos;
  int64_t check_sum_pos;

  pos += meta_size;    // 将pos设置为meta之后
  check_sum_pos = pos; // 后续做checksum需要从这个pos开始

  if (pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("fail to encode ObRpcTableOperationRequest", K(ret), KP(buf), K(buf_len), K(pos), K(meta_size));
  } else {
    // 序列化table_request
    LOG_DEBUG("ObRpcRedisOperationRequest::encode",  K_(&redis_table_request), K(this), K_(redis_table_request), K(buf), K(buf_len), K(pos));
    // if (OB_FAIL(redis_table_request_.serialize(buf, buf_len, pos))) {
    //   LOG_WDIAG("fail to serialize for table request");
    // }
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version_)) {
      LOG_DEBUG("ObRpcTableOperationRequest::encode less v4", K(buf), K(buf_len), K(pos));
      OB_UNIS_ENCODE(redis_table_request_);
    } else {
      LOG_DEBUG("ObRpcTableOperationRequest::encode v4", K(buf), K(buf_len), K(pos));
      if (OB_FAIL(redis_table_request_.serialize_v4(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize for table request");
      }
    }
    if (OB_SUCC(ret)) {
      // 首先计算checksum
      int64_t request_size = pos - check_sum_pos;
      uint64_t check_sum = ob_crc64(static_cast<void *>(buf + check_sum_pos), request_size);
      int64_t ez_payload_size = rpc_packet_meta_.rpc_header_.get_encoded_size() + request_size;

      rpc_packet_meta_.ez_header_.ez_payload_size_ = static_cast<uint32_t>(ez_payload_size);
      rpc_packet_meta_.rpc_header_.checksum_ = check_sum;

      // 这里传入原始的pos, 序列化meta信息
      if (OB_FAIL(rpc_packet_meta_.serialize(buf, buf_len, origin_pos))) {
        LOG_WDIAG("fail to encode meta", K_(rpc_packet_meta), K(ret));
      } else if (origin_pos != check_sum_pos) {
        // double check
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("origin pos is not equal to check sum pos, unexpected", K(ret), K(origin_pos), K(check_sum_pos));
      } else {
        LOG_DEBUG("ObRpcRedisOperationRequest::encode succ", K(buf), K(buf_len), K(pos));
        // success
      }
    }
  }
  return ret;
}

int64_t ObRpcRedisOperationRequest::get_encode_size() const
{
  int64_t len = 0;
  len += this->ObRpcRequest::get_encode_size();
  len += redis_table_request_.get_serialize_size();
  return len;
}

int ObRpcRedisAuthRequest::decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db)
{
  int ret = OB_SUCCESS;
  set_redis_args(redis_args);
  set_redis_db(redis_db);
  if (redis_args->count() == 2) {
    full_user_name_ = redis_args->at(1);
    password_ = redis_args->at(2);
  } else if (redis_args->count() == 3) {
    full_user_name_ = "default user name";
    password_ = redis_args->at(1);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected auth request", K(ret));
  }
  return ret;
}

int ObRpcRedisCommonRequest::decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db)
{
  int ret = OB_SUCCESS;
  set_redis_args(redis_args);
  set_redis_db(redis_db);
  if (OB_FAIL(decode_rowkey_value())) {
    LOG_WDIAG("fail to decode rowkey value", K(ret));
  }
  return ret;
}

int ObRpcRedisInternalRequest::decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db)
{
  int ret = OB_SUCCESS;
  set_redis_args(redis_args);
  set_redis_db(redis_db);
  return ret;
}


int ObRpcRedisGlobalRequest::decode(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *redis_args, uint64_t redis_db)
{
  int ret = OB_SUCCESS;
  set_redis_args(redis_args);
  set_redis_db(redis_db);
  return ret;
}

int ObRpcRedisCommonRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                               proxy::ObRpcReq &ob_rpc_req,
                                               proxy::ObProxyPartInfo &part_info,
                                               int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();

  if (0 == rowkey_.count() || OB_ISNULL(meta_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rowkey value to handle", K(ret), "count", rowkey_.count(), K_(rowkey));
  } else {
    // ROWKEY_VALUE real_rowkey_value(common::ObModIds::OB_RPC_TABLE_ROWKEY, ROWKEY_COLUMNS_COUNT * sizeof(common::ObObj)); // with db
    ROWKEY_VALUE_OBJ(real_rowkey_value);
    ROWKEY_COLUMN_OBJ(rowkey_columns);  // mock , redis do not need columns

    ObObj db_value(ObObjType::ObUInt64Type);
    db_value.set_uint64(redis_db_);

    ObString db_name("db");
    rowkey_columns.push_back(db_name);
    //for observer 4.2.5
    if (meta_info_->is_need_vk()) {
      ObString vk_name("vk"); //for hash
      rowkey_columns.push_back(vk_name);
    } else {
      ObString vk_name("rkey"); //for string
      rowkey_columns.push_back(vk_name);
    }
    partition_ids_.reset();

    for (int i = 0; OB_SUCC(ret) && i < rowkey_.count(); ++i) {
      LOG_DEBUG("redis to calc_partition_id ", K_(rowkey), K(i), K(db_value));
      if (OB_FAIL(real_rowkey_value.push_back(db_value))
        || OB_FAIL(real_rowkey_value.push_back(rowkey_.at(i)))) {
        LOG_WDIAG("fail to call push_back for real_rowkey_value", K(ret), K(i));
      // } else if (FALSE_IT(LOG_DEBUG("redis to calc_partition_id2", K(real_rowkey_value), K(i)))) {

      } else if (OB_FAIL(ObRpcExprCalcTool::calculate_partition_id_with_rowkey(allocator,
                                                                               real_rowkey_value,
                                                                               rowkey_columns,
                                                                               part_info,
                                                                               partition_id))) {
        LOG_WDIAG("fail to call calculate_partition_id_with_rowkey", K(ret), K(i));
      } else {
        real_rowkey_value.reset();
        partition_ids_.push_back(partition_id);
        // TODO : support ls id
      }
    }

    if (OB_SUCC(ret)) {
      if (1 == partition_ids_.count()) {
        obkv_info.set_definitely_single(true);
        // obkv_info.set_ls_id(ls_id);
        obkv_info.set_partition_id(partition_id);
      } else if (partition_ids_.count() > 1) {
        obkv_info.set_shard(true);
        partition_id = common::OB_INVALID_INDEX;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected redis partition id calculation result", K(ret), "part_id_count",
                  partition_ids_.count(), KP(this));
      }
    }
  }

  return ret;
}

int ObRpcRedisGlobalRequest::calc_partition_id(common::ObArenaAllocator &allocator,
                                               proxy::ObRpcReq &ob_rpc_req,
                                               proxy::ObProxyPartInfo &part_info,
                                               int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();

  if (0 == rowkey_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rowkey value to handle", K(ret), "count", rowkey_.count(), K_(rowkey));
  } else {
    // ROWKEY_VALUE real_rowkey_value(common::ObModIds::OB_RPC_TABLE_ROWKEY, ROWKEY_COLUMNS_COUNT * sizeof(common::ObObj)); // with db
    ROWKEY_VALUE_OBJ(real_rowkey_value);
    ROWKEY_COLUMN_OBJ(rowkey_columns);  // mock , redis do not need columns
    ObObj db_value(ObObjType::ObUInt64Type);
    db_value.set_uint64(redis_db_);
    // int64_t ls_id;

    ObString db_name("db");
    rowkey_columns.push_back(db_name);
    //for observer 4.2.5
    if (meta_info_->is_need_vk()) {
      ObString vk_name("vk"); //for hash
      rowkey_columns.push_back(vk_name);
    } else {
      ObString vk_name("rkey"); //for string
      rowkey_columns.push_back(vk_name);
    }
    partition_ids_.reset();
    // ASSERT THAT: it only one rowkey info for OBServer-4.2.5
    for (int i = 0; OB_SUCC(ret) && i < rowkey_.count(); ++i) {
      LOG_DEBUG("redis to calc_partition_id ", K_(rowkey), K(i));
      if (OB_FAIL(real_rowkey_value.push_back(db_value))
        || OB_FAIL(real_rowkey_value.push_back(rowkey_.at(i)))) {
        LOG_WDIAG("fail to call push_back for real_rowkey_value", K(ret), K(i));
      } else if (OB_FAIL(ObRpcExprCalcTool::calculate_partition_id_with_rowkey(allocator,
                                                                               real_rowkey_value,
                                                                               rowkey_columns,
                                                                               part_info,
                                                                               partition_id))) {
        LOG_WDIAG("fail to call calculate_partition_id_with_rowkey", K(ret), K(i));
      } else {
        real_rowkey_value.reset();
        partition_ids_.push_back(partition_id);
        // TODO : support ls id
      }
    }

    if (OB_SUCC(ret)) {
      if (1 == partition_ids_.count()) {
        obkv_info.set_definitely_single(true);
        // obkv_info.set_ls_id(ls_id);
        obkv_info.set_partition_id(partition_id);
      } else if (partition_ids_.count() > 1) {
        obkv_info.set_shard(true);
        partition_id = common::OB_INVALID_INDEX;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected redis partition id calculation result", K(ret), "part_id_count",
                  partition_ids_.count(), KP(this));
      }
    }
  }

  return ret;
}

} // end of namespace obkv
} // end of namespace obproxy
} // end of namespace oceanbase

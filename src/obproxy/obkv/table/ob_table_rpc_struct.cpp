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
#include "ob_table_rpc_struct.h"
#include "common/ob_table_object.h"
#include "lib/utility/ob_unify_serialize.h"
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obkv;


template <typename RequestT>
int encode_optional_hbase_op_type(char *buf, const int64_t buf_len, int64_t &pos, const RequestT &req)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && req.is_need_hbase_op_type_) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, req.hbase_op_type_))) {
      LOG_WDIAG("encode hbase_op_type fail", K(ret));
    }
  }
  return ret;
}

template <typename RequestT>
void add_serialize_len_optional_hbase_op_type(int64_t &len, const RequestT &req)
{
  if (req.is_need_hbase_op_type_) {
    len += serialization::encoded_length(req.hbase_op_type_);
  }
}

int decode_optional_hbase_op_type_tail(const char *buf, const int64_t data_len, int64_t &pos,
    bool &is_need_hbase_op_type, ObHBaseOperationType &hbase_op_type)
{
  int ret = OB_SUCCESS;
  is_need_hbase_op_type = false;
  if (OB_SUCC(ret) && pos < data_len) {
    is_need_hbase_op_type = true;
    if (OB_FAIL(serialization::decode(buf, data_len, pos, hbase_op_type))) {
      LOG_WDIAG("decode hbase_op_type fail", K(ret), K(data_len), K(pos));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTableLoginRequest,
                    auth_method_,
                    client_type_,
                    client_version_,
                    reserved1_,
                    client_capabilities_,
                    max_packet_size_,
                    reserved2_,
                    reserved3_,
                    tenant_name_,
                    user_name_,
                    pass_secret_,
                    pass_scramble_,
                    database_name_,
                    ttl_us_,
                    client_info_,
                    allow_distribute_capability_);

OB_SERIALIZE_MEMBER(ObTableLoginResult,
                    server_capabilities_,
                    reserved1_,
                    reserved2_,
                    server_version_,
                    credential_,
                    tenant_id_,
                    user_id_,
                    database_id_);

/*OB_SERIALIZE_MEMBER(ObTableOperationRequest,
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
*/
/*OB_SERIALIZE_MEMBER(ObTableBatchOperationRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    entity_type_,
                    batch_operation_,
                    consistency_level_,
                    option_flag_,
                    returning_affected_entity_,
                    returning_affected_rows_,
                    partition_id_,
                    batch_operation_as_atomic_,
                    binlog_row_image_type_);
*/
/*OB_SERIALIZE_MEMBER(ObTableQueryRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    partition_id_,
                    entity_type_,
                    consistency_level_,
                    query_
                    );*/
/*OB_SERIALIZE_MEMBER(ObTableQueryAndMutateRequest,
                    credential_,
                    table_name_,
                    table_id_,
                    partition_id_,
                    entity_type_,
                    query_and_mutate_,
                    binlog_row_image_type_);*/

////////////////////////////////////////////////////////////////
/*OB_SERIALIZE_MEMBER((ObTableQuerySyncRequest, ObTableQueryRequest),
                    query_session_id_,
                    query_type_
                    );*/

//////////////////////////// ObTableOperationRequest ////////////////////////////////////


// v3 cluster serialize
OB_UNIS_DEF_SERIALIZE(ObTableOperationRequest,
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
OB_UNIS_DEF_SERIALIZE_SIZE(ObTableOperationRequest,
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

int ObTableOperationRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
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

int ObTableOperationRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
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

/*int ObTableOperationRequest::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableOperationRequest::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              entity_type_,
              table_operation_,
              consistency_level_,
              returning_rowkey_,
              returning_affected_entity_,
              returning_affected_rows_,
              binlog_row_image_type_);
  return ret;
}
*/
int64_t ObTableOperationRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableOperationRequest::get_serialize_size_v4_(void) const
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
/*
// overload serialize function to get additional position related information
int ObTableOperationRequest::deserialize_get_position(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableOperationRequest::deserialize_get_position_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  OB_UNIS_DECODE(partition_id_);
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              table_operation_,
                              consistency_level_,
                              returning_rowkey_,
                              returning_affected_entity_,
                              returning_affected_rows_,
                              binlog_row_image_type_);
  return ret;
}

*/
ODP_DEF_DESERIALIZE_HEADER(ObTableOperationRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableOperationRequest)
{
  int ret = OK_;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  // record table id pos and len
  rpc_request->table_id_position_ = pos;
  OB_UNIS_DECODE(table_id_);
  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;

  // record table id
  if (OB_SUCC(ret)) {
    rpc_request->partition_id_position_ = pos;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
      OB_UNIS_DECODE(partition_id_);  
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
    rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;
  }

  LST_DO_CODE(OB_UNIS_DECODE, entity_type_);
  if (OB_SUCC(ret) && OB_FAIL(rpc_request->init_rowkey_info(1))) {
    LOG_WDIAG("fail to init sub req", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(table_operation_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize table operation", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE, consistency_level_, returning_rowkey_, returning_affected_entity_,
              returning_affected_rows_, binlog_row_image_type_);
  return ret;

}

/*int ObTableOperationRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}
int ObTableOperationRequest::deserialize_get_position_v4_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              table_operation_,
                              consistency_level_,
                              returning_rowkey_,
                              returning_affected_entity_,
                              returning_affected_rows_,
                              binlog_row_image_type_);
  return ret;
}*/
//////////////////////////// ObTableBatchOperationRequest ////////////////////////////////////

OB_UNIS_DEF_SERIALIZE(ObTableBatchOperationRequest,
                      credential_,
                      table_name_,
                      table_id_,
                      entity_type_,
                      batch_operation_,
                      consistency_level_,
                      option_flag_,
                      returning_affected_entity_,
                      returning_affected_rows_,
                      partition_id_,
                      batch_operation_as_atomic_,
                      binlog_row_image_type_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableBatchOperationRequest,
                           credential_,
                           table_name_,
                           table_id_,
                           entity_type_,
                           batch_operation_,
                           consistency_level_,
                           option_flag_,
                           returning_affected_entity_,
                           returning_affected_rows_,
                           partition_id_,
                           batch_operation_as_atomic_,
                           binlog_row_image_type_);

int ObTableBatchOperationRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
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

int ObTableBatchOperationRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_ENCODE,
              credential_,
              table_name_,
              table_id_,
              entity_type_,
              batch_operation_,
              consistency_level_,
              option_flag_,
              returning_affected_entity_,
              returning_affected_rows_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(partition_id_)))) {
      LOG_WDIAG("serialize tablet ID failed", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              batch_operation_as_atomic_,
              binlog_row_image_type_);
  return ret;
}
/*
int ObTableBatchOperationRequest::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableBatchOperationRequest::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_,
              entity_type_,
              batch_operation_,
              consistency_level_,
              option_flag_,
              returning_affected_entity_,
              returning_affected_rows_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              batch_operation_as_atomic_,
              binlog_row_image_type_);
  return ret;
}
*/
int64_t ObTableBatchOperationRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableBatchOperationRequest::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(CLS);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              credential_,
              table_name_,
              table_id_,
              entity_type_,
              batch_operation_,
              consistency_level_,
              option_flag_,
              returning_affected_entity_,
              returning_affected_rows_);

  len += 8;   // tablet_id

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              batch_operation_as_atomic_,
              binlog_row_image_type_);
  return len;
}

/*
int ObTableBatchOperationRequest::deserialize_get_position(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}
*/


ODP_DEF_DESERIALIZE_HEADER(ObTableBatchOperationRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}


ODP_DEF_DESERIALIZE_PAYLOAD(ObTableBatchOperationRequest)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  
  // record table id
  rpc_request->table_id_position_ = pos;
  OB_UNIS_DECODE(table_id_);
  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_);

  if (OB_FAIL(batch_operation_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize batch operation", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE, consistency_level_, option_flag_, returning_affected_entity_,
              returning_affected_rows_);
  // record table id
  if (OB_SUCC(ret)) {
    rpc_request->partition_id_position_ = pos;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
      OB_UNIS_DECODE(partition_id_);  
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
    rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;
  }
  LST_DO_CODE(OB_UNIS_DECODE, batch_operation_as_atomic_, binlog_row_image_type_);
  return ret;
}

/*int ObTableBatchOperationRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableBatchOperationRequest::deserialize_get_position_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  // part_id_position = pos;
  table_id_len = pos - table_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              batch_operation_,
                              consistency_level_,
                              option_flag_,
                              returning_affected_entity_,
                              returning_affected_rows_);
  partition_id_pos = pos;
  OB_UNIS_DECODE(partition_id_);
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, batch_operation_as_atomic_,
                              binlog_row_image_type_);
  return ret;
}*/
//////////////////////////// ObTableQueryRequest ////////////////////////////////////

OB_UNIS_DEF_SERIALIZE(ObTableQueryRequest,
                      credential_,
                      table_name_,
                      table_id_,
                      partition_id_,
                      entity_type_,
                      consistency_level_,
                      query_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableQueryRequest,
                           credential_,
                           table_name_,
                           table_id_,
                           partition_id_,
                           entity_type_,
                           consistency_level_,
                           query_);

int ObTableQueryRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
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

int ObTableQueryRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
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
              consistency_level_,
              query_);
  if (OB_SUCC(ret) && is_need_option_flag_) {
    LST_DO_CODE(OB_UNIS_ENCODE, option_flag_);
  }
  return ret;
}
/*
int ObTableQueryRequest::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableQueryRequest::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              entity_type_,
              consistency_level_,
              query_);
  return ret;
}
*/
int64_t ObTableQueryRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableQueryRequest::get_serialize_size_v4_(void) const
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
              consistency_level_,
              query_);
  if (is_need_option_flag_) {
    len += 1;  // option_flag_
  }
  return len;
}

/*int ObTableQueryRequest::deserialize_get_position(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}*/

ODP_DEF_DESERIALIZE_HEADER(ObTableQueryRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableQueryRequest)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  
  // record table id
  rpc_request->table_id_position_ = pos;
  OB_UNIS_DECODE(table_id_);
  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;

  // record partition id
  if (OB_SUCC(ret)) {
    rpc_request->partition_id_position_ = pos;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
      OB_UNIS_DECODE(partition_id_);  
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
    rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;
  }

  LST_DO_CODE(OB_UNIS_DECODE, entity_type_, consistency_level_);
  if (OB_SUCC(ret)) {
    int64_t origin_pos = pos;
    char* origin_buf = const_cast<char*>(buf + pos);
    if (OB_FAIL(rpc_request->init_rowkey_info(1))) {
      LOG_WDIAG("fail to init rpc request", K(ret));
    } else if (OB_FAIL(query_.deserialize(buf, data_len, pos, rpc_request))) {
      LOG_WDIAG("fail to deserialize table query", K(ret));
    } else if (OB_FAIL(rpc_request->add_sub_req_buf(ObRpcFieldBuf(origin_buf, pos - origin_pos)))) {
      LOG_WDIAG("fail to add sub req buf", K(ret));
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    is_need_option_flag_ = true;
    OB_UNIS_DECODE(option_flag_);
  }
  return ret;
}

/*
int ObTableQueryRequest::deserialize_get_position_v4_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              consistency_level_,
                              query_);
  return ret;
}

int ObTableQueryRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableQueryRequest::deserialize_get_position_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  OB_UNIS_DECODE(partition_id_);
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              consistency_level_,
                              query_);
  return ret;
}*/
/////////////////////////// ObTableQueryAndMutateRequest /////////////////////////////////////

OB_UNIS_DEF_SERIALIZE(ObTableQueryAndMutateRequest,
                      credential_,
                      table_name_,
                      table_id_,
                      partition_id_,
                      entity_type_,
                      query_and_mutate_,
                      binlog_row_image_type_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableQueryAndMutateRequest,
                           credential_,
                           table_name_,
                           table_id_,
                           partition_id_,
                           entity_type_,
                           query_and_mutate_,
                           binlog_row_image_type_);

int ObTableQueryAndMutateRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
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

int ObTableQueryAndMutateRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
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
              query_and_mutate_,
              binlog_row_image_type_);
  if (OB_SUCC(ret) && is_need_option_flag_) {
    LST_DO_CODE(OB_UNIS_ENCODE, option_flag_);
  }
  if (OB_SUCC(ret)) {
    ret = encode_optional_hbase_op_type(buf, buf_len, pos, *this);
  }
  return ret;
}
/*
int ObTableQueryAndMutateRequest::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableQueryAndMutateRequest::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE,
              credential_,
              table_name_,
              table_id_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              entity_type_,
              query_and_mutate_,
              binlog_row_image_type_);
  return ret;
}*/

int64_t ObTableQueryAndMutateRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}

int64_t ObTableQueryAndMutateRequest::get_serialize_size_v4_(void) const
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
              query_and_mutate_,
              binlog_row_image_type_);

  if (is_need_option_flag_) {
    len += 1;  // option_flag_
  }
  add_serialize_len_optional_hbase_op_type(len, *this);
  return len;
}
/*
int ObTableQueryAndMutateRequest::deserialize_get_position(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}*/
/*
int ObTableQueryAndMutateRequest::deserialize_get_position_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  OB_UNIS_DECODE(partition_id_);
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              query_and_mutate_,
                              binlog_row_image_type_);
  return ret;
}
int ObTableQueryAndMutateRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableQueryAndMutateRequest::deserialize_get_position_v4_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  table_id_pos = pos;
  OB_UNIS_DECODE(table_id_);
  table_id_len = pos - table_id_pos;
  partition_id_pos = pos;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
  }
  partition_id_len = pos - partition_id_pos;
  LST_DO_CODE(OB_UNIS_DECODE, entity_type_,
                              query_and_mutate_,
                              binlog_row_image_type_);
  return ret;
}*/

ODP_DEF_DESERIALIZE_HEADER(ObTableQueryAndMutateRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableQueryAndMutateRequest)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_SER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_);
  // record table id pos and len
  rpc_request->table_id_position_ = pos;
  OB_UNIS_DECODE(table_id_);
  rpc_request->table_id_len_ = pos - rpc_request->table_id_position_;

  // record table id
  if (OB_SUCC(ret)) {
    rpc_request->partition_id_position_ = pos;
    if (IS_CLUSTER_VERSION_LESS_THAN_V4(rpc_request->get_cluster_version())) {
      OB_UNIS_DECODE(partition_id_);
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t *>(&partition_id_)))) {
      LOG_WDIAG("deserialize tablet ID failed", K(ret), KP(buf), K(data_len), K(pos));
    }
    rpc_request->partition_id_len_ = pos - rpc_request->partition_id_position_;
  }

  LST_DO_CODE(OB_UNIS_DECODE, entity_type_);
  if (OB_FAIL(query_and_mutate_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize query_and_mutate op", K(ret));
  }
  LST_DO_CODE(OB_UNIS_DECODE, binlog_row_image_type_);
  if (OB_SUCC(ret) && pos < data_len) {
    is_need_option_flag_ = true;
    OB_UNIS_DECODE(option_flag_);
  }
  if (OB_SUCC(ret)) {
    ret = decode_optional_hbase_op_type_tail(buf, data_len, pos, is_need_hbase_op_type_, hbase_op_type_);
  }
  return ret;
}

/////////////////////////// ObTableQuerySyncRequest /////////////////////////////////////
OB_UNIS_SERIALIZE(ObTableQuerySyncRequest);
int ObTableQuerySyncRequest::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  if (OB_FAIL(ObTableQueryRequest::serialize(buf, buf_len, pos))) {
    RPC_WARN("fail to call ObTableQueryRequest::serialize");
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, query_session_id_, query_type_);
  }
  if (OB_SUCC(ret)) {
    ret = encode_optional_hbase_op_type(buf, buf_len, pos, *this);
  }
  return ret;
}

OB_UNIS_SERIALIZE_SIZE(ObTableQuerySyncRequest);
int64_t ObTableQuerySyncRequest::get_serialize_size_(void) const
{
  int64_t len = ObTableQueryRequest::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, query_session_id_, query_type_);
  add_serialize_len_optional_hbase_op_type(len, *this);
  return len;
}

int ObTableQuerySyncRequest::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
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
int ObTableQuerySyncRequest::serialize_v4_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  UNF_UNUSED_SER;
  if (OB_FAIL(ObTableQueryRequest::serialize_v4(buf, buf_len, pos))) {
    RPC_WARN("fail to call ObTableQueryRequest::serialize_v4");
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              query_session_id_,
              query_type_
              );
  if (OB_SUCC(ret)) {
    ret = encode_optional_hbase_op_type(buf, buf_len, pos, *this);
  }
  return ret;
}
/*
int ObTableQuerySyncRequest::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER(CLS, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_v4_(buf + pos_orig, len, pos))) {
      LOG_WDIAG("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}
int ObTableQuerySyncRequest::deserialize_v4_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  if (OB_FAIL(ObTableQueryRequest::deserialize_v4(buf, data_len, pos))) {
    RPC_WARN("fail to call ObTableQueryRequest::deserialize_v4");
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              query_session_id_,
              query_type_
              );
  return ret;
}*/
int64_t ObTableQuerySyncRequest::get_serialize_size_v4(void) const
{
  int64_t len = get_serialize_size_v4_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION, len);
  return len;
}
int64_t ObTableQuerySyncRequest::get_serialize_size_v4_(void) const
{
  int64_t len = 0;
  len += ObTableQueryRequest::get_serialize_size_v4();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              query_session_id_,
              query_type_
              );
  add_serialize_len_optional_hbase_op_type(len, *this);
  return len;
}
/*int ObTableQuerySyncRequest::deserialize_get_position(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}
int ObTableQuerySyncRequest::deserialize_get_position_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
   int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  if (OB_FAIL(ObTableQueryRequest::deserialize_get_position(buf, data_len, pos, REWRITE_INFO_ARG_VALUE))) {
    RPC_WARN("fail to call ObTableQueryRequest::deserialize_get_position");
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              query_session_id_,
              query_type_
              );
  return ret;
}

int ObTableQuerySyncRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableQuerySyncRequest::deserialize_get_position_v4_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNF_UNUSED_DES;
  if (OB_FAIL(ObTableQueryRequest::deserialize_get_position_v4(buf, data_len, pos, REWRITE_INFO_ARG_VALUE))) {
    RPC_WARN("fail to call ObTableQueryRequest::deserialize_get_position_v4");
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              query_session_id_,
              query_type_
              );
  return ret;
}*/

ODP_DEF_DESERIALIZE_HEADER(ObTableQuerySyncRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  // todo: check if it is a matter that table_query_request rewrite payload_pos and payload_len
  
  // TableQrerySync will try to deserialize TableQuery which lead to paylolan_len double record 
  // we record a temp var here
  int64_t origin_payload_len_pos;
  int64_t origin_payload_len_len;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, origin_payload_len_pos, origin_payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    rpc_request->payload_len_position_ = origin_payload_len_pos;
    rpc_request->payload_len_len_ = origin_payload_len_len;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableQuerySyncRequest)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  if (OB_FAIL(ObTableQueryRequest::deserialize(buf, data_len, pos, rpc_request))) {
    RPC_WARN("fail to call ObTableQueryRequest::deserialize_get_position_v4");
  }
  LST_DO_CODE(OB_UNIS_DECODE, query_session_id_, query_type_);
  if (OB_SUCC(ret)) {
    ret = decode_optional_hbase_op_type_tail(buf, data_len, pos, is_need_hbase_op_type_, hbase_op_type_);
  }
  return ret;
}

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadRequestHeader,
                           addr_,
                           operation_type_);

OB_SERIALIZE_MEMBER(ObTableDirectLoadRequest,
                    header_,
                    credential_,
                    arg_content_);



int ObTableDirectLoadRequest::deserialize_get_position_v4(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version, len, payload_len_pos, payload_len_len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_get_position_v4_(buf + pos_orig, len, pos, REWRITE_INFO_ARG_VALUE))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    table_id_pos += pos_orig;
    partition_id_pos += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

int ObTableDirectLoadRequest::deserialize_get_position_v4_(const char *buf, int64_t data_len, int64_t &pos, REWRITE_INFO_ARG)
{
  int ret = OK_;
  UNUSED(payload_len_pos);
  UNUSED(payload_len_len);
  UNUSED(table_id_len);
  UNUSED(table_id_pos);
  UNUSED(partition_id_len);
  UNUSED(partition_id_pos);
  UNF_UNUSED_DES;

  LST_DO_CODE(OB_UNIS_DECODE,
              header_,
              credential_
              );
  return ret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadResultHeader,
                           addr_,
                           operation_type_);

OB_DEF_DESERIALIZE(ObTableDirectLoadResult)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null allocator in deserialize", K(ret));
  } else {
    ObString tmp_res_content;
    LST_DO_CODE(OB_UNIS_DECODE,
                header_,
                tmp_res_content);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(*allocator_, tmp_res_content, res_content_))) {
      LOG_WDIAG("fail to copy string", K(ret));
    }
  }
  return ret;
}

OB_UNIS_SERIALIZE(ObTableLSOpRequest);
int ObTableLSOpRequest::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OK_;
  UNF_UNUSED_SER;
  BASE_SER(ObTableLSOpRequest);
  LST_DO_CODE(OB_UNIS_ENCODE, credential_, entity_type_, consistency_level_, ls_op_);
  if (OB_SUCC(ret)) {
    ret = encode_optional_hbase_op_type(buf, buf_len, pos, *this);
  }
  return ret;
}

OB_UNIS_SERIALIZE_SIZE(ObTableLSOpRequest);
int64_t ObTableLSOpRequest::get_serialize_size_(void) const
{
  int64_t len = 0;
  BASE_ADD_LEN(ObTableLSOpRequest);
  LST_DO_CODE(OB_UNIS_ADD_LEN, credential_, entity_type_, consistency_level_, ls_op_);
  add_serialize_len_optional_hbase_op_type(len, *this);
  return len;
}

ODP_DEF_DESERIALIZE_HEADER(ObTableLSOpRequest)
{
  int ret = OK_;
  int64_t version = 0;
  int64_t len = 0;
  DESERIALIZE_HEADER_WITH_INFO(CLS, version ,len, rpc_request->payload_len_position_, rpc_request->payload_len_len_);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos, rpc_request))) {
      RPC_WARN("deserialize_ fail",
               "slen", len, K(pos), K(ret));
    }
    rpc_request->table_id_position_ += pos_orig;
    rpc_request->partition_id_position_ += pos_orig;
    rpc_request->ls_id_postition_ += pos_orig;
    pos = pos_orig + len;
  }
  return ret;
}

ODP_DEF_DESERIALIZE_PAYLOAD(ObTableLSOpRequest)
{
  int ret = OK_;
  UNF_UNUSED_DES;
  BASE_DESER(CLS);
  LST_DO_CODE(OB_UNIS_DECODE, credential_, entity_type_, consistency_level_);
  if (OB_FAIL(ls_op_.deserialize(buf, data_len, pos, rpc_request))) {
    LOG_WDIAG("fail to deserialize ls operation", K(ret));
  }
  if (OB_SUCC(ret)) {
    ret = decode_optional_hbase_op_type_tail(buf, data_len, pos, is_need_hbase_op_type_, hbase_op_type_);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObObkvGetRouteRequest,
                    type_,
                    credential_,
                    table_name_,
                    cluster_name_,
                    tenant_name_,
                    database_name_,
                    force_renew_);

OB_SERIALIZE_MEMBER(ObTableMetaRequest,
                     credential_,
                     meta_type_,
                     data_);

OB_DEF_DESERIALIZE(ObHbaseOperationRequest, ) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, credential_, table_name_, option_flag_, op_type_);
  if (OB_SUCC(ret)) {
    int64_t keys_count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &keys_count))) {
      LOG_WDIAG("fail to decode keys count", K(ret));
    } else if (OB_FAIL(keys_.prepare_allocate(keys_count))) {
      LOG_WDIAG("fail to prepare allocate keys", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < keys_count; ++i) {
        // attention!!! can only use ObTableSerialUtil::deserialize to deserialize ObObj
        // because ObObj type is not same as ObTableObjType
        if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, keys_.at(i)))) {
          LOG_WDIAG("fail to deserialize key", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t same_cf_rows_count = 0;
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &same_cf_rows_count))) {
      LOG_WDIAG("fail to decode same_cf_rows count", K(ret));
    } else if (OB_FAIL(same_cf_rows_.prepare_allocate(same_cf_rows_count))) {
      LOG_WDIAG("fail to prepare allocate same_cf_rows", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < same_cf_rows_count; ++i) {
        if (OB_FAIL(same_cf_rows_.at(i).deserialize(buf, data_len, pos))) {
          LOG_WDIAG("fail to deserialize cf_row", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObHbaseOperationRequest, ) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, credential_, table_name_, option_flag_, op_type_);
  int64_t keys_count = keys_.count();
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, keys_count))) {
    LOG_WDIAG("fail to encode keys count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < keys_count; ++i) {
      if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, keys_.at(i)))) {
        LOG_WDIAG("fail to serialize key", K(ret));
      }
    }
  }
  int64_t same_cf_rows_count = same_cf_rows_.count();
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, same_cf_rows_count))) {
    LOG_WDIAG("fail to encode same_cf_rows count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < same_cf_rows_count; ++i) {
      if (OB_FAIL(same_cf_rows_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WDIAG("fail to serialize cf_row", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHbaseOperationRequest, ) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, credential_, table_name_, option_flag_, op_type_);
  int64_t keys_count = keys_.count();
  OB_UNIS_ADD_LEN(keys_count);
  for (int64_t i = 0; i < keys_count; ++i) {
    len += ObTableSerialUtil::get_serialize_size(keys_.at(i));
  }
  int64_t same_cf_rows_count = same_cf_rows_.count();
  OB_UNIS_ADD_LEN(same_cf_rows_count);
  for (int64_t i = 0; i < same_cf_rows_count; ++i) {
    len += same_cf_rows_.at(i).get_serialize_size();
  }
  return len;
}

int ObHbaseOperationRequest::get_column_K_value(ObObj &obj) const {
  int64_t key_index = get_key_index();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_index < 0 || key_index >= keys_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid key index", K(ret), K(key_index), K(keys_.count()));
  } else {
    obj = keys_.at(key_index);
  }
  return OB_SUCCESS;
}
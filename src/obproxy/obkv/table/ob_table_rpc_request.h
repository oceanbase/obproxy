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

#ifndef _OB_TABLE_RPC_REQUEST_H
#define _OB_TABLE_RPC_REQUEST_H 1

#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h" //ObHashMap
#include "lib/hash/ob_hashutils.h"

#include "ob_rpc_struct.h"
#include "ob_table_rpc_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{


// use no thread defend mode, make sure these map used in one thread
typedef common::hash::ObHashMap<int64_t, ObSEArray<int64_t, 4>, common::hash::NoPthreadDefendMode> PARTITION_ID_MAP;
typedef common::hash::ObHashMap<int64_t, ObSEArray<int64_t, 4>, common::hash::NoPthreadDefendMode> LS_TABLET_ID_MAP;
typedef common::hash::ObHashMap<int64_t, ObSEArray<int64_t, 4>, common::hash::NoPthreadDefendMode> TABLET_ID_INDEX_MAP;

class ObRpcTableLoginRequest : public ObRpcRequest
{
public:
  ObRpcTableLoginRequest() :login_request_() {}
  virtual ~ObRpcTableLoginRequest() {}

  uint8_t get_auth_method() const {return login_request_.auth_method_;}
  uint8_t get_client_type() const {return login_request_.client_type_;}
  uint8_t get_client_version() const {return login_request_.client_version_;}
  uint32_t get_client_capabilities() const {return login_request_.client_capabilities_;}
  uint32_t get_max_packet_size() const {return login_request_.max_packet_size_;}
  const ObString &get_tenant_name() const {return login_request_.tenant_name_;}
  const ObString &get_user_name() const {return login_request_.user_name_;}
  const ObString &get_pass_secret() const {return login_request_.pass_secret_;}
  const ObString &get_pass_scramble() const {return login_request_.pass_scramble_;}
  const ObString &get_database_name() const {return login_request_.database_name_;}
  ObString get_database_name() {return login_request_.database_name_;}
  uint64_t get_ttl_us() const {return login_request_.ttl_us_;}

  void set_auth_method(uint8_t auth_method) {login_request_.auth_method_ = auth_method;}
  void set_client_type(uint8_t client_type) {login_request_.client_type_ = client_type;}
  void set_client_version(uint8_t client_version) {login_request_.client_version_ = client_version;}
  void set_client_capabilities(uint32_t cap) {login_request_.client_capabilities_ = cap;}
  void set_max_packet_size(uint32_t max_packet) {login_request_.max_packet_size_ = max_packet;}
  void set_tenant_name(const ObString &tenant_name) {login_request_.tenant_name_ = tenant_name;}
  void set_user_name(const ObString &user_name) {login_request_.user_name_ = user_name;}
  void set_pass_secret(const ObString &pass_secret) {login_request_.pass_secret_ = pass_secret;}
  void set_pass_scramble(const ObString &pass_scramble) {login_request_.pass_scramble_ = pass_scramble;}
  void set_database_name(const ObString &database_name) {login_request_.database_name_ = database_name;}
  void set_ttl_us(uint64_t ttl_us) {login_request_.ttl_us_ = ttl_us;}

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  int deep_copy(common::ObIAllocator &allocator, const ObRpcTableLoginRequest &login_request);

  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;

  virtual bool is_read_weak() const { return true; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;
  TO_STRING_KV(K_(rpc_packet_meta), K_(login_request));
private:
  ObTableLoginRequest login_request_;
};

class ObRpcTableOperationRequest : public ObRpcRequest
{
public:
  ObRpcTableOperationRequest() : table_request_() {}
  ~ObRpcTableOperationRequest() {}

  ObString get_table_name() {return table_request_.table_name_;}
  uint64_t get_table_id() const override {return table_request_.table_id_;}
  uint64_t get_partition_id() const override {return table_request_.partition_id_;}
  ObTableEntityType get_entity_type() const {return table_request_.entity_type_;}
  const OB_IGNORE_TABLE_OPERATION &get_table_operation() const {return table_request_.table_operation_;}
  ObTableConsistencyLevel get_consistency_level() const {return table_request_.consistency_level_;}
  bool get_return_rowkey() const {return table_request_.returning_rowkey_;}
  bool get_return_affected_entity() const {return table_request_.returning_affected_entity_;}
  bool get_return_affected_rows() const {return table_request_.returning_affected_rows_;}
  ObBinlogRowImageType get_image_type() const {return table_request_.binlog_row_image_type_;}

  void set_credential(const ObString &credential) {table_request_.credential_ = credential;}
  void set_table_name(const ObString &table_name) {table_request_.table_name_ = table_name;}
  virtual void set_table_id(uint64_t table_id) override {table_request_.table_id_ = table_id;}
  virtual void set_partition_id(uint64_t part_id) override {table_request_.partition_id_ = part_id;}
  void set_entity_type(ObTableEntityType type) {table_request_.entity_type_ = type;}
  void set_table_operation(const OB_IGNORE_TABLE_OPERATION &op)  {table_request_.table_operation_  = op;}
  void set_consistency_level(ObTableConsistencyLevel level) {table_request_.consistency_level_ = level;}
  void set_return_rowkey(bool return_rowkey) {table_request_.returning_rowkey_ = return_rowkey;}
  void set_return_affected_entity(bool affected_entity) {table_request_.returning_affected_entity_ = affected_entity;}
  void set_return_affected_rows(bool affected_rows) {table_request_.returning_affected_rows_ = affected_rows;}
  void set_image_type(ObBinlogRowImageType type) {table_request_.binlog_row_image_type_ = type;}

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableOperationRequest &other);

  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual ObString get_credential() const { return table_request_.credential_; }
  virtual ObString get_table_name() const { return table_request_.table_name_; }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == table_request_.entity_type_; }
  virtual bool is_read_weak() const { return obkv::ObTableConsistencyLevel::STRONG != table_request_.consistency_level_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;
  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest,  K_(table_request));
private:
  ObTableOperationRequest table_request_;
};

class ObRpcTableBatchOperationRequest : public ObRpcRequest
{

public:
  ObRpcTableBatchOperationRequest();
  ~ObRpcTableBatchOperationRequest() {}

  int init_as_sub_batch_operation_request(const ObRpcTableBatchOperationRequest &request, const common::ObIArray<int64_t> &sub_index);
  uint64_t get_table_id() const override {return batch_request_.table_id_;}
  uint64_t get_partition_id() const override {return batch_request_.partition_id_;}
  ObTableEntityType get_entity_type() const {return batch_request_.entity_type_;}
  const ObTableBatchOperation &get_table_operation() const {return batch_request_.batch_operation_;}
  ObTableBatchOperation &get_table_operation() {return batch_request_.batch_operation_;}
  ObTableConsistencyLevel get_consistency_level() const {return batch_request_.consistency_level_;}
  uint8_t get_option_flag() const { return batch_request_.option_flag_; }
  bool get_return_affected_entity() const {return batch_request_.returning_affected_entity_;}
  bool get_return_affected_rows() const {return batch_request_.returning_affected_rows_;}
  bool get_batch_operation_as_atomic() const {return batch_request_.batch_operation_as_atomic_;}
  ObBinlogRowImageType get_image_type() const {return batch_request_.binlog_row_image_type_;}

  void set_credential(const ObString &credential) {batch_request_.credential_ = credential;}
  void set_table_name(const ObString &table_name) {batch_request_.table_name_ = table_name;}
  void set_table_id(uint64_t table_id) override {batch_request_.table_id_ = table_id;}
  void set_partition_id(uint64_t part_id) override {batch_request_.partition_id_ = part_id;}
  void set_entity_type(ObTableEntityType type) {batch_request_.entity_type_ = type;}
  void set_table_operation(const ObTableBatchOperation &op)  {batch_request_.batch_operation_  = op;}
  void set_consistency_level(ObTableConsistencyLevel level) {batch_request_.consistency_level_ = level;}
  void set_option_flag(uint8_t option_flag) { batch_request_.option_flag_ = option_flag; }
  void set_return_affected_entity(bool affected_entity) {batch_request_.returning_affected_entity_ = affected_entity;}
  void set_return_affected_rows(bool affected_rows) {batch_request_.returning_affected_rows_ = affected_rows;}
  void set_image_type(ObBinlogRowImageType type) {batch_request_.binlog_row_image_type_ = type;}
  void set_batch_operation_as_atomic(bool atomic) {batch_request_.batch_operation_as_atomic_ = atomic;}

  bool use_put() const { return batch_request_.option_flag_ & OB_TABLE_OPTION_USE_PUT; }
  bool returning_rowkey() const { return batch_request_.option_flag_ & OB_TABLE_OPTION_RETURNING_ROWKEY; }
  bool return_one_result() const { return batch_request_.option_flag_ & OB_TABLE_OPTION_RETURN_ONE_RES; }

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;

  int set_sub_ops(const common::ObIArray<ObRpcFieldBuf> &single_op_buf);
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual ObString get_credential() const { return batch_request_.credential_; }
  virtual ObString get_table_name() const { return batch_request_.table_name_; }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == batch_request_.entity_type_; }
  virtual bool is_read_weak() const { return obkv::ObTableConsistencyLevel::STRONG != batch_request_.consistency_level_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;
  PARTITION_ID_MAP &get_partition_id_map() { return partid_to_index_map_; }
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableBatchOperationRequest &other);

  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(batch_request));
private:
  ObTableBatchOperationRequest batch_request_;
  PARTITION_ID_MAP partid_to_index_map_;  //TODO need check release SEArray
};

class ObRpcTableQueryRequest : public ObRpcRequest
{
public:
  ObRpcTableQueryRequest() :  query_request_(), partition_ids_(), index_table_name_() {
    index_table_name_buf_[0] = '\0';
  }
  ~ObRpcTableQueryRequest() {}
  ObRpcTableQueryRequest(ObRpcTableQueryRequest &request) : ObRpcRequest() {
    set_credential(request.get_credential());
    set_table_name(request.get_table_name());
    set_table_id(request.get_table_id());
    set_partition_id(request.get_partition_id());
    set_entity_type(request.get_entity_type());
    set_table_operation(request.get_query());
    set_consistency_level(request.get_consistency_level());
    set_partition_ids(request.get_partition_ids_noconst());
    set_packet_meta(request.get_packet_meta());
  }

  uint64_t get_table_id() const override {return query_request_.table_id_;}
  uint64_t get_partition_id() const override {return query_request_.partition_id_;}
  ObTableEntityType get_entity_type() const {return query_request_.entity_type_;}
  const OB_IGNORE_TABLE_QUERY &get_query() const {return query_request_.query_;}
  ObTableConsistencyLevel get_consistency_level() const {return query_request_.consistency_level_;}
  const ObSEArray<int64_t, 1> &get_partition_ids() const { return partition_ids_; }
  ObSEArray<int64_t, 1> &get_partition_ids_noconst() { return partition_ids_; }
  ObString get_index_table_name() { return index_table_name_;}

  void set_credential(const ObString &credential) {query_request_.credential_ = credential;}
  void set_table_name(const ObString &table_name) {query_request_.table_name_ = table_name;}
  void set_table_id(uint64_t table_id) override {query_request_.table_id_ = table_id;}
  void set_partition_id(uint64_t part_id) override {query_request_.partition_id_ = part_id;}
  void set_entity_type(ObTableEntityType type) {query_request_.entity_type_ = type;}
  void set_table_operation(const OB_IGNORE_TABLE_QUERY &query)  {query_request_.query_ = query;}
  void set_consistency_level(ObTableConsistencyLevel level) {query_request_.consistency_level_ = level;}
  void set_partition_ids(ObSEArray<int64_t, 1> &partition_ids) { partition_ids_ = partition_ids; }
  int set_partition_ids(ObIArray<int64_t> &partition_ids) {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      if (OB_FAIL(partition_ids_.push_back(partition_ids.at(i)))) {
        PROXY_RPC_SM_LOG(WDIAG, "fail to push back partition id", K(ret));
      } 
    }
    return ret;
  }
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryRequest &other);

  //virtual void set_cluster_version(int64_t cluster_version) override { 
  //  cluster_version_ = cluster_version;
  //  query_request_.query_.set_cluster_version(cluster_version);
  //}

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual ObString get_credential() const { return query_request_.credential_; }
  virtual ObString get_table_name() const { return query_request_.table_name_; }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == query_request_.entity_type_; }
  virtual bool is_read_weak() const { return obkv::ObTableConsistencyLevel::STRONG != query_request_.consistency_level_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;

  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(query_request));
private:
  ObTableQueryRequest query_request_;
  ObSEArray<int64_t, 1> partition_ids_;
  ObString index_table_name_;
  char index_table_name_buf_[common::OB_MAX_INDEX_TABLE_NAME_LENGTH];
};

class ObRpcTableQueryAndMutateRequest : public ObRpcRequest
{
public:
  ObRpcTableQueryAndMutateRequest() : query_and_mutate_request_(), partition_ids_(), index_table_name_()
  {
    index_table_name_buf_[0] = '\0';
  }
  ~ObRpcTableQueryAndMutateRequest() {}
  ObRpcTableQueryAndMutateRequest(ObRpcTableQueryAndMutateRequest &request) :ObRpcRequest() {
    set_credential(request.get_credential());
    set_table_name(request.get_table_name());
    set_table_id(request.get_table_id());
    set_partition_id(request.get_partition_id());
    set_entity_type(request.get_entity_type());
    set_table_operation(request.get_query_and_mutate());
    // set_consistency_level(request.get_consistency_level());
    set_partition_ids(request.get_partition_ids_noconst());
    set_packet_meta(request.get_packet_meta());
  }
  uint64_t get_table_id() const override {return query_and_mutate_request_.table_id_;}
  uint64_t get_partition_id() const override {return query_and_mutate_request_.partition_id_;}
  ObTableEntityType get_entity_type() const {return query_and_mutate_request_.entity_type_;}
  const OB_IGNORE_TABLE_QUERY_AND_MUTATE &get_query_and_mutate() const {return query_and_mutate_request_.query_and_mutate_;}
  // ObTableConsistencyLevel get_consistency_level() const {return query_request_.consistency_level_;}
  const ObSEArray<int64_t, 1> &get_partition_ids() const { return partition_ids_; }
  ObSEArray<int64_t, 1> &get_partition_ids_noconst() { return partition_ids_; }

  void set_credential(const ObString &credential) {query_and_mutate_request_.credential_ = credential;}
  void set_table_name(const ObString &table_name) {query_and_mutate_request_.table_name_ = table_name;}
  void set_table_id(uint64_t table_id) override {query_and_mutate_request_.table_id_ = table_id;}
  void set_partition_id(uint64_t part_id) override {query_and_mutate_request_.partition_id_ = part_id;}
  void set_entity_type(ObTableEntityType type) {query_and_mutate_request_.entity_type_ = type;}
  void set_table_operation(const OB_IGNORE_TABLE_QUERY_AND_MUTATE &query_and_mutate)  {query_and_mutate_request_.query_and_mutate_ = query_and_mutate;}
  // void set_consistency_level(ObTableConsistencyLevel level) {query_request_.consistency_level_ = level;}
  void set_partition_ids(ObSEArray<int64_t, 1> &partition_ids) { partition_ids_ = partition_ids; }
  int set_partition_ids(ObIArray<int64_t> &partition_ids)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      if (OB_FAIL(partition_ids_.push_back(partition_ids.at(i)))) {
        PROXY_RPC_SM_LOG(WDIAG, "fail to push back partition id", K(ret));
      } 
    }
    return ret;
  }

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;
  //virtual void set_cluster_version(int64_t cluster_version) { 
  //  cluster_version_ = cluster_version;
  //  query_and_mutate_request_.query_and_mutate_.set_cluster_version(cluster_version);
  //}
  
  virtual ObString get_credential() const { return query_and_mutate_request_.credential_; }
  virtual ObString get_table_name() const { return query_and_mutate_request_.table_name_; }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == query_and_mutate_request_.entity_type_; }
  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(query_and_mutate_request));

private:
  ObTableQueryAndMutateRequest query_and_mutate_request_;
  ObSEArray<int64_t, 1> partition_ids_;
  ObString index_table_name_;
  char index_table_name_buf_[common::OB_MAX_INDEX_TABLE_NAME_LENGTH];
};

class ObRpcTableQuerySyncRequest : public ObRpcRequest
{
public:
  ObRpcTableQuerySyncRequest() :  query_request_(), partition_ids_() {}
  ~ObRpcTableQuerySyncRequest() {}
  ObRpcTableQuerySyncRequest(ObRpcTableQuerySyncRequest &request) :ObRpcRequest() {
    set_credential(request.get_credential());
    set_table_name(request.get_table_name());
    set_table_id(request.get_table_id());
    set_partition_id(request.get_partition_id());
    set_entity_type(request.get_entity_type());
    set_table_operation(request.get_query());
    set_consistency_level(request.get_consistency_level());
    set_partition_ids(request.get_partition_ids_noconst());
    set_packet_meta(request.get_packet_meta());
  }

  uint64_t get_table_id() const override {return query_request_.table_id_;}
  uint64_t get_partition_id() const override {return query_request_.partition_id_;}
  ObTableEntityType get_entity_type() const {return query_request_.entity_type_;}
  const OB_IGNORE_TABLE_QUERY &get_query() const {return query_request_.query_;}
  const ObTableQuerySyncRequest &get_query_request() const {return query_request_;}
  ObTableConsistencyLevel get_consistency_level() const {return query_request_.consistency_level_;}
  const ObSEArray<int64_t, 1> &get_partition_ids() const { return partition_ids_; }
  ObSEArray<int64_t, 1> &get_partition_ids_noconst() { return partition_ids_; }
  uint64_t get_query_session_id() { return query_request_.query_session_id_; }
  ObQueryOperationType &get_query_type() { return query_request_.query_type_; }

  //virtual void set_cluster_version (int64_t cluster_version) override { 
  //  cluster_version_ = cluster_version;
  //  query_request_.query_.set_cluster_version(cluster_version);
  //}

  void set_credential(const ObString &credential) {query_request_.credential_ = credential;}
  void set_table_name(const ObString &table_name) {query_request_.table_name_ = table_name;}
  void set_table_id(uint64_t table_id) override {query_request_.table_id_ = table_id;}
  void set_partition_id(uint64_t part_id) override {query_request_.partition_id_ = part_id;}
  void set_entity_type(ObTableEntityType type) {query_request_.entity_type_ = type;}
  void set_table_operation(const OB_IGNORE_TABLE_QUERY &query)  {query_request_.query_ = query;}
  void set_consistency_level(ObTableConsistencyLevel level) {query_request_.consistency_level_ = level;}
  void set_partition_ids(ObSEArray<int64_t, 1> &partition_ids) { partition_ids_ = partition_ids; }
  int set_partition_ids(ObIArray<int64_t> &partition_ids);
 
  void set_query_session_id(uint64_t id) { query_request_.query_session_id_ = id; }
  void set_query_type(ObQueryOperationType type) { query_request_.query_type_ = type; }
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableQuerySyncRequest &other);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual ObString get_credential() const { return query_request_.credential_; }
  virtual ObString get_table_name() const { return query_request_.table_name_; }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == query_request_.entity_type_; }
  virtual bool is_read_weak() const { return obkv::ObTableConsistencyLevel::STRONG != query_request_.consistency_level_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;

  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(query_request));
private:
  ObTableQuerySyncRequest query_request_;
  ObSEArray<int64_t, 1> partition_ids_;
};

class ObRpcTableDirectLoadRequest : public ObRpcRequest
{
public:
  ObRpcTableDirectLoadRequest() : direct_load_request_() {}
  ~ObRpcTableDirectLoadRequest() {}
  const ObTableDirectLoadRequest &get_direct_load_request() const { return direct_load_request_; }
  const ObTableDirectLoadRequestHeader &get_direct_load_request_header() const { return direct_load_request_.header_; }
  bool is_begin_request() { return direct_load_request_.header_.operation_type_ == ObTableDirectLoadOperationType::BEGIN; }
  const common::ObAddr &get_direct_load_request_addr() const { return direct_load_request_.header_.addr_; }

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override {
    UNUSEDx(buf, buf_len, pos);
    return common::OB_SUCCESS;
  }
  virtual int64_t get_encode_size() const override { return 0; }
  // rewrite this func to analyze table operation request 
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  /* The first direct load request will be route by LDC by WEAK. it could be sent to any server instance 
  * to execute. Followed request will be send to the server asigned by reuest, and WEAK flag not useful 
  * any more.
  */
  virtual bool is_read_weak() const { return true; }
  virtual ObString get_credential() const { return direct_load_request_.credential_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;
  TO_STRING_KV(K_(rpc_packet_meta), K_(direct_load_request));
private:
  // common::ObArenaAllocator allocator_;
  ObTableDirectLoadRequest direct_load_request_;
};

class ObRpcTableLSOperationRequest : public ObRpcRequest 
{
public:
  ObRpcTableLSOperationRequest();
  ~ObRpcTableLSOperationRequest() {}
  const ObTableLSOp &get_operation() const {return ls_request_.ls_op_;}
  ObTableLSOp &get_operation() {return ls_request_.ls_op_;}

  ObTableEntityType get_entity_type() const {return ls_request_.entity_type_;}
  ObTableConsistencyLevel get_consistency_level() const {return ls_request_.consistency_level_;}
  uint64_t get_table_id() const {return ls_request_.ls_op_.get_table_id();}
  uint64_t get_ls_id() const {return ls_request_.ls_op_.get_ls_id();}
  uint8_t get_option_flag() const { return ls_request_.ls_op_.get_option_flag(); }
  const ObSEArray<ObString, 4> &get_all_rowkey_names() const { return ls_request_.ls_op_.get_all_rowkey_names(); }
  const ObSEArray<ObString, 4> &get_all_properties_names() const { return ls_request_.ls_op_.get_all_properties_names(); }

  void set_credential(const ObString &credential) {ls_request_.credential_ = credential;}
  void set_entity_type(const ObTableEntityType type) {ls_request_.entity_type_ = type;}
  void set_consistency_level(const ObTableConsistencyLevel level) {ls_request_.consistency_level_ = level;}
  void set_table_id(const uint64_t table_id) {ls_request_.ls_op_.set_table_id(table_id);}
  void set_ls_id(const int64_t ls_id) {ls_request_.ls_op_.set_ls_id(ls_id);}
  void set_table_name(const ObString &table_name) {ls_request_.ls_op_.set_table_name(table_name);}
  void set_option_flag(uint8_t option_flag) { ls_request_.ls_op_.set_option_flag(option_flag); }
  void set_dictionary(const ObSEArray<ObString, 4> &rowkey_names, const ObSEArray<ObString, 4> &properties_names) {
    ls_request_.ls_op_.set_dictionary(rowkey_names, properties_names);
  };
  TABLET_ID_INDEX_MAP &get_tablet_id_index_map() {return tablet_id_index_map_;}
  const TABLET_ID_INDEX_MAP &get_tablet_id_index_map() const {return tablet_id_index_map_;}
  LS_TABLET_ID_MAP &get_ls_id_tablet_id_map() {return ls_id_tablet_id_map_;}
  const LS_TABLET_ID_MAP &get_ls_id_tablet_id_map() const {return ls_id_tablet_id_map_;}
  int init_as_sub_ls_operation_request(const ObRpcTableLSOperationRequest &request,
                                       const int64_t ls_id,
                                       const common::ObIArray<int64_t> &tablet_ids);
  int record_ls_tablet_index(const int ls_id, const int tablet_id, const int index);
  int init_tablet_ops(const ObRpcTableLSOperationRequest &root_request, const common::ObIArray<int64_t> &tablet_ids);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  virtual int analyze_request(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual ObString get_credential() const { return ls_request_.credential_; }
  virtual ObString get_table_name() const { return ls_request_.ls_op_.get_table_name(); }
  virtual bool is_hbase_request() const { return ObTableEntityType::ET_HKV == ls_request_.entity_type_; }
  virtual bool is_read_weak() const { return obkv::ObTableConsistencyLevel::STRONG != ls_request_.consistency_level_; }
  virtual int calc_partition_id(common::ObArenaAllocator &allocator,
                                proxy::ObRpcReq &ob_rpc_req,
                                proxy::ObProxyPartInfo &part_info,
                                int64_t &partition_id) override;

  INHERIT_TO_STRING_KV("ObRpcRequest", ObRpcRequest, K_(ls_request));
private:
  ObTableLSOpRequest ls_request_;
  //ObTableEntityFactory<ObTableSingleOpEntity> request_entity_factory_;
  TABLET_ID_INDEX_MAP tablet_id_index_map_;  // ls_id or tablet_id ->  index
  LS_TABLET_ID_MAP ls_id_tablet_id_map_;  // ls_id or tablet_id ->  index
  //common::ObArenaAllocator allocator_;
};

} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase
#endif

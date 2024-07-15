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

#ifndef _OB_TABLE_RPC_RESPONSE_H
#define _OB_TABLE_RPC_RESPONSE_H 1

#include "ob_rpc_struct.h"
#include "ob_table_rpc_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

class ObRpcTableLoginResponse : public ObRpcResponse
{
public:
  ObRpcTableLoginResponse() : login_res_() {}
  ~ObRpcTableLoginResponse() {}
  ObRpcTableLoginResponse(const ObRpcTableLoginResponse &response) : ObRpcResponse(response)
  {
    login_res_.server_capabilities_ = response.login_res_.server_capabilities_;
    login_res_.reserved1_ = response.login_res_.reserved1_;  // always 0 for now
    login_res_.reserved2_ = response.login_res_.reserved2_;  // always 0 for now
    login_res_.server_version_ = response.login_res_.server_version_;
    login_res_.credential_ = response.login_res_.credential_;
    login_res_.tenant_id_ = response.login_res_.tenant_id_;
    login_res_.user_id_ = response.login_res_.user_id_;
    login_res_.database_id_ = response.login_res_.database_id_;
  }

  uint32_t get_server_capabilities() {return login_res_.server_capabilities_;}
  const ObString &get_server_version() {return login_res_.server_version_;}
  const ObString &get_credential() {return login_res_.credential_;}
  uint64_t get_tenant_id() {return login_res_.tenant_id_;}
  uint64_t get_user_id() {return login_res_.user_id_;}
  uint64_t get_database_id() {return login_res_.database_id_;}

  void set_server_capabilities(uint32_t cap) {login_res_.server_capabilities_ = cap;}
  void set_server_version(const ObString &version) {login_res_.server_version_ = version;}
  void set_credential(const ObString &credential) {login_res_.credential_ = credential;}
  void set_tenant_id(uint64_t tenant_id) {login_res_.tenant_id_ = tenant_id;}
  void set_user_id(uint64_t user_id) {login_res_.user_id_ = user_id;}
  void set_database_id(uint64_t database_id) {login_res_.database_id_ = database_id;}

  int deep_copy(common::ObIAllocator &allocator, const ObRpcTableLoginResponse &other);
  virtual int deep_copy(common::ObIAllocator &allocator,  ObRpcResponse *other);

  TO_STRING_KV(K_(rpc_packet_meta), K_(login_res));

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  // rewrite this func to analyze table login response
  virtual int analyze_response(const char *buf, int64_t buf_len, int64_t &pos) override;

private:
  ObTableLoginResult login_res_;
};

class ObRpcTableOperationResponse : public ObRpcResponse
{
public:
  ObRpcTableOperationResponse() : table_res_() {}
  ~ObRpcTableOperationResponse() {}
  ObRpcTableOperationResponse(const ObRpcTableOperationResponse &response)
    : ObRpcResponse(response)//, table_res_(response.get_table_operation_result())
  {
    //res_entity_.set_rowkey(response.res_entity_);
  }

  //ObTableOperationType::Type get_type() const {return table_res_.type();}
  //void get_entity(const ObITableEntity *&entity) const {table_res_.get_entity(entity);}
  //int64_t get_affected_rows() {return table_res_.get_affected_rows();}
  //const ObTableOperationResult &get_operation_result() {return table_res_;}

  //void set_type(ObTableOperationType::Type type) {table_res_.set_type(type);}
  //void set_entity(ObITableEntity &entity) {table_res_.set_entity(entity);}
  //void set_affected_rows(int64_t affected_rows) {table_res_.set_affected_rows(affected_rows);}
  //const ObTableEntity& get_table_entity() { return res_entity_; }
  //const ObTableOperationResult& get_table_operation_result() { return table_res_; }

  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableOperationResponse &other);
  //virtual int deep_copy(common::ObIAllocator &allocator,  ObRpcResponse *other);

  TO_STRING_KV(K_(rpc_packet_meta), K_(table_res));

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  // rewrite this func to analyze table operation response
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  OB_UNIS_IGNORE_TABLE_OP_RESULT table_res_;
  //ObTableEntity res_entity_;
};

class ObRpcTableBatchOperationResponse : public ObRpcResponse
{
public:
  ObRpcTableBatchOperationResponse() : batch_res_() {}
  ~ObRpcTableBatchOperationResponse() {}

  const ObTableBatchOperationResult &get_batch_result() const {return batch_res_;}
  ObTableBatchOperationResult &get_batch_result() {return batch_res_;}
  void set_batch_result(const ObTableBatchOperationResult &batch_res) {batch_res_ = batch_res;}
  //ObTableEntityFactory<ObTableEntity> &get_table_entity_factory() { return res_entity_factory_;}

  TO_STRING_KV(K_(rpc_packet_meta), K_(batch_res));

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const override;
  // rewrite this func to analyze batch operation response
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableBatchOperationResponse &other);
  //virtual int deep_copy(common::ObIAllocator &allocator,  ObRpcResponse *other);

private:
  ObTableBatchOperationResult batch_res_;
  //ObTableEntityFactory<ObTableEntity> res_entity_factory_;
};

class ObRpcTableQueryResponse : public ObRpcResponse
{
public:
  ObRpcTableQueryResponse() : query_res_() {}
  ~ObRpcTableQueryResponse() {}

  const ObTableQueryResult &get_query_result() const {return query_res_;}
  ObTableQueryResult &get_query_result() {return query_res_;}

  // TO_STRING_KV(K_(rpc_packet_meta), K_(res_buf_len), K_(query_res));
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryResponse &other);
  //virtual int deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  // rewrite this func to analyze query response
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  ObTableQueryResult query_res_;
};

class ObRpcTableQueryAndMutateResponse : public ObRpcResponse
{
public:
  ObRpcTableQueryAndMutateResponse() : query_and_mutate_res_() {}
  ~ObRpcTableQueryAndMutateResponse() {}

  const ObTableQueryAndMutateResult &get_query_result() const {return query_and_mutate_res_;}
  ObTableQueryAndMutateResult &get_query_result() {return query_and_mutate_res_;}

  // TO_STRING_KV(K_(rpc_packet_meta), K_(res_buf_len), K_(query_res));
  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableQueryAndMutateResponse &other);
  //virtual int deep_copy(common::ObIAllocator &allocator,  ObRpcResponse *other);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  // rewrite this func to analyze query response
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  // ObTableQueryResult query_res_;
  ObTableQueryAndMutateResult query_and_mutate_res_;
};

class ObRpcTableMoveResponse : public ObRpcResponse
{
public:
  ObRpcTableMoveResponse() : move_result_() {}
  ~ObRpcTableMoveResponse() {}

  const ObTableMoveResult &get_move_result() const { return move_result_; }
  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  ObTableMoveResult move_result_;
};

class ObRpcTableQuerySyncResponse : public ObRpcResponse
{
public:
  ObRpcTableQuerySyncResponse() : query_res_() {}
  ~ObRpcTableQuerySyncResponse() {}
  const ObTableQuerySyncResult &get_query_result() const {return query_res_;} 
  ObTableQuerySyncResult &get_query_result() {return query_res_;} 

  //int deep_copy(common::ObIAllocator &allocator, const ObRpcTableQuerySyncResponse &other);
  //virtual int deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other);

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  // rewrite this func to analyze query response 
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  ObTableQuerySyncResult query_res_;
};

class ObRpcTableDirectLoadResponse : public ObRpcResponse
{
public:
  ObRpcTableDirectLoadResponse() : direct_load_res_() {}
  ~ObRpcTableDirectLoadResponse() {}
  const ObTableDirectLoadResult &get_query_result() const {return direct_load_res_;} 
  ObTableDirectLoadResult &get_query_result() {return direct_load_res_;} 

  // TO_STRING_KV(K_(rpc_packet_meta), K_(res_buf_len), K_(query_res));
  // int deep_copy(common::ObIAllocator &allocator, const ObRpcTableDirectLoadResponse &other);
  // virtual int deep_copy(common::ObIAllocator &allocator, ObRpcResponse *other);

  // virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  // virtual int64_t get_encode_size() const;
  // rewrite this func to analyze query response 
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;

private:
  ObTableDirectLoadResult direct_load_res_;
};

class ObRpcTableLSOperationResponse : public ObRpcResponse
{
public:
  ObRpcTableLSOperationResponse() : ls_res_() {}
  ~ObRpcTableLSOperationResponse() {}

  const ObTableLSOpResult &get_ls_result() const {return ls_res_;}
  ObTableLSOpResult &get_ls_result() {return ls_res_;}
  //ObTableEntityFactory<ObTableSingleOpEntity> &get_table_entity_factory() { return res_entity_factory_;}

  TO_STRING_KV(K_(ls_res));

  virtual int encode(char *buf, int64_t &buf_len, int64_t &pos) override;
  virtual int64_t get_encode_size() const;
  virtual int analyze_response(const char *buf, const int64_t buf_len, int64_t &pos) override;
  //common::ObArenaAllocator &get_allocator() { return allocator_; }
  
private:
  ObTableLSOpResult ls_res_;
  // ObTableEntityFactory<ObTableSingleOpEntity> res_entity_factory_;
  //common::ObArenaAllocator allocator_;
};


} // end namespace obkv
} // end namespace obproxy
} // end namespace oceanbase
#endif

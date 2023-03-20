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


#ifndef OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_EXECUTE_REQ_H_
#define OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_EXECUTE_REQ_H_

#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "obproxy/proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "obproxy/packet/ob_mysql_packet_util.h"
namespace oceanbase
{

namespace obmysql
{

/**
 * This packet is request to OB_MYSQL_COM_STMT_PREPARE_EXECUTE
 * following with param desc && column desc packets
 *
 */

class OMPKPrepareExecuteReq: public ObMySQLPacket
{
public:
  OMPKPrepareExecuteReq() :
    cmd_(OB_MYSQL_COM_STMT_PREPARE_EXECUTE),
    statement_id_(0),
    flags_(0),
    iteration_count_(0),
    query_(NULL),
    param_num_(0),
    null_bitmap_(NULL),
    new_params_bound_flag_(0)
   {}
  virtual ~OMPKPrepareExecuteReq() {}

  virtual int decode();

  inline uint32_t get_statement_id() const { return statement_id_; }
  inline uint32_t get_param_num() const { return param_num_; }
  inline common::ObIArray<obmysql::EMySQLFieldType>& get_param_types() { return param_types_; }
  inline common::ObIArray<obmysql::TypeInfo>& get_type_infos() { return type_infos_; }

private:
  uint8_t cmd_;
  uint32_t statement_id_;
  uint8_t flags_;
  uint32_t iteration_count_;
  common::ObString query_;
  uint32_t param_num_; 
  char* null_bitmap_;
  uint8_t new_params_bound_flag_;
  common::ObSEArray<obmysql::EMySQLFieldType, 2> param_types_;
  common::ObSEArray<obmysql::TypeInfo, 2> type_infos_;
  // ingore value of eache parameter
  uint32_t execute_mode_;
  uint32_t num_close_stmt_count_;
  uint32_t check_sum_;
  uint32_t extend_flag_;

  DISALLOW_COPY_AND_ASSIGN(OMPKPrepareExecuteReq);
};

} //end of obmysql
} //end of oceanbase


#endif //OCEANBASE_RPC_OBMYSQL_OMPK_PREPARE_EXECUTE_REQ_H_

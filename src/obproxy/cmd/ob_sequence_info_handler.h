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

#ifndef OBPROXY_SEQUENCE_INFO_HANDLER_H_
#define OBPROXY_SEQUENCE_INFO_HANDLER_H_
#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_proxy_sequence_entry.h"
#include "opsql/dual_parser/ob_dual_parser.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlSM;
class ObSequenceInfo;
class ObSequenceInfoHandler : public ObInternalCmdHandler
{
public:
  ObSequenceInfoHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObSequenceInfoHandler();
protected:
  int encode_err_packet(const int errcode);
private:
  int handle_sequece_params();
  int handle_single_sequence_params();
  int handle_shard_sequence_params();
  int do_get_next_sequence(int event, void* data);
  int handle_sequence_done(int event, void *data);
  int process_sequence_info(void* data);
  int dump_header();
  int dump_body();
private:
  ObProxyBasicStmtSubType sub_type_;
  ObSequenceRouteParam param_;
  oceanbase::obproxy::proxy::ObMysqlSM* sm_;
  ObSequenceInfo* seq_info_;
  obproxy::obutils::ObProxyDualParseResult* result_;
  int64_t table_id_;
  int64_t group_id_;
  int64_t eid_;
#define SEQUENCE_ERR_MSG_SIZE 256
#define MAX_SEQ_NAME_SIZE 256
  char seq_name_buf_[MAX_SEQ_NAME_SIZE]; // use for transfrom to uppercase
  char seq_id_buf_[2048];
  char real_database_buf_[OB_MAX_DATABASE_NAME_LENGTH];
  char real_table_name_buf_[OB_MAX_TABLE_NAME_LENGTH];
  char err_msg_[SEQUENCE_ERR_MSG_SIZE];
  bool is_shard_seq_req_;
  oceanbase::obproxy::obutils::ObProxyConfigString sequence_sql_;
  oceanbase::obproxy::obutils::ObProxyConfigString logic_tenant_name_;
  oceanbase::obproxy::obutils::ObProxyConfigString logic_database_name_;
  int retry_time_;
  DISALLOW_COPY_AND_ASSIGN(ObSequenceInfoHandler);
};

int sequence_info_cmd_init();
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_SEQUENCE_INFO_HANDLER_H_

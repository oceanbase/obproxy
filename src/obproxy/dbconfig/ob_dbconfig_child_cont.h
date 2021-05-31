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

#ifndef OBPROXY_DB_CONFIG_CHILD_CONT_H
#define OBPROXY_DB_CONFIG_CHILD_CONT_H

#include "dbconfig/ob_dbconfig_fetch_cont.h"

namespace google
{
namespace protobuf
{
class Message;
class Any;
}
}

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObProxyMutex;
class ObContinuation;
class ObEThread;
}
namespace dbconfig
{

class ObDbConfigChildCont : public ObDbConfigFetchCont
{
public:
  ObDbConfigChildCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *cb_thread, const ObDDSCrdType type);
  virtual ~ObDbConfigChildCont() {}

  virtual void destroy();

  virtual int init_task();
  void add_resource_name(const std::string &res_name);

  void set_db_info(ObDbConfigLogicDb *db_info)
  {
    if (NULL != db_info_) {
      db_info_->dec_ref();
      db_info_ = NULL;
    }
    if (NULL != db_info) {
      db_info->inc_ref();
      db_info_ = db_info;
    }
  }

private:
  int parse_child_resource(const google::protobuf::Any &message);
  int parse_database_auth(const google::protobuf::Any &res);
  int parse_database_var(const google::protobuf::Any &res);
  int parse_database_prop(const google::protobuf::Any &res);
  int parse_shard_tpo(const google::protobuf::Any &res);
  int parse_shard_router(const google::protobuf::Any &res);
  int parse_shard_dist(const google::protobuf::Any &res);
  int parse_shard_connector(const google::protobuf::Any &res);
  int parse_shard_prop(const google::protobuf::Any &res);

private:
  static const int64_t MAX_FETCH_RETRY_TIMES = 3;
  static const int64_t RETRY_INTERVAL_MS = 100;

  int64_t fetch_faliure_count_;
  ObDbConfigLogicDb *db_info_;
  std::vector<std::string> resource_array_;
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigChildCont);
};

inline void ObDbConfigChildCont::add_resource_name(const std::string &res_name)
{
  resource_array_.push_back(res_name);
}

} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_CHILD_CONT_H */

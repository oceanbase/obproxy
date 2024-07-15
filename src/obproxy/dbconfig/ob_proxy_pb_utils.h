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

#ifndef OBPROXY_PB_UTILS_H
#define OBPROXY_PB_UTILS_H

#include "lib/ob_define.h"
#include "lib/json/ob_json.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
//#include "lib/string/ob_sql_string.h" //added by zhiyun

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
namespace common
{
class ObSqlString;
}
namespace obproxy
{
namespace dbconfig
{
class ObDataBaseKey;
class ObProxyPbUtils
{
public:
  template<typename T>
  static int dump_child_array_to_file(ObDbConfigChildArrayInfo<T> &src_array);
  static int dump_child_info_to_file(const ObDbConfigChild &child_info);
  static int dump_database_info_to_file(ObDbConfigLogicDb &db_info);
  static int dump_tenant_info_to_file(ObDbConfigLogicTenant &tenant_info);
  static int dump_config_to_buf(common::ObSqlString &opt,
                                const char *buf, const int buf_len,
                                const char *tenant, const int tenant_len,
                                const char *logical_db, const int logical_db_len,
                                const int type,
                                //const char* Type,
                                const char *file_name, const int file_name_len);
  static int init_and_dump_config_to_buf(common::ObSqlString &opt,
                                    const ObDbConfigChild *child_info_p,
                                    const char *tenant, const int tenant_len,
                                    const char *logical_db, const int logical_db_len,
                                    const ObDDSCrdType type);
  template<typename T>
  static int dump_child_array_to_log(ObDbConfigChildArrayInfo<T> &src_array, const ObDDSCrdType type);
  static int dump_child_info_to_log(const ObDbConfigChild &child_info, const ObDDSCrdType type);
  static int dump_database_info_to_log(ObDbConfigLogicDb &db_info);
  static int dump_tenant_info_to_log(ObDbConfigLogicTenant &tenant_info);
  static int parse_local_child_config(const ObDataBaseKey &db_info_key,
                                      const std::string &resource_name,
                                      const ObDDSCrdType type);
  static int parse_local_child_config(ObDbConfigChild &child_info);
  static void parse_shard_auth_user(ObShardConnector &conn_info);

  static int do_parse_json_rules(json::Value *json_root, ObProxyShardRuleList &rule_list, common::ObIAllocator &allocator);
  static int parse_rule_pattern(const std::string &pattern, obutils::ObProxyConfigString &name_prefix,
                                int64_t &count, int64_t &suffix_len,
                                obutils::ObProxyConfigString &name_tail);
  static int parse_group_cluster(const std::string &gc_name, const std::string &es_str, ObGroupCluster &gc_info);
  static int parse_shard_url(const std::string &shard_url, ObShardConnector &conn_info);

private:
  static void get_str_value_by_name(const std::string &shard_url, const char* name, obutils::ObProxyConfigString& value);
public:
  static int parse_database_prop_rule(const std::string &prop_rule, ObDataBaseProp &child_info);
  static int parse_es_info(const std::string &sub_str, ObGroupCluster &gc_info, int64_t &last_eid);
  static int parse_shard_rule(const std::string &rule_name, const std::string &rule_value, ObShardRule &rule_info);
  static int parse_json_rules(const std::string &json_str, ObProxyShardRuleList &rule_list, common::ObIAllocator &allocator);
  static int force_parse_groovy(const common::ObString &expr, ObProxyShardRuleInfo &info, common::ObIAllocator &allocator);
  static int do_parse_from_local_file(const char *dir, const char *file_name,
                                      ObDbConfigChild &child_info);
  static int get_physic_ip(const common::ObString& addr_str, bool is_physic_ip_, sockaddr &addr);
public:
  static const int ODP_CONFIG_LOG_START = -1;
  static const int ODP_CONFIG_LOG_END   = -2;
};

} // end dbconfig
} // end obproxy
} // end oceanbase

#endif // OBPROXY_PB_UTILS_H

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
#include "dbconfig/ob_proxy_pb_utils.h"
#include "dbconfig/ob_proxy_db_config_task.h"
#include "dbconfig/ob_dbconfig_child_cont.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "iocore/net/ob_inet.h"
#include "iocore/eventsystem/ob_grpc_task.h"
#include "utils/ob_proxy_utils.h"
#include "lib/json/ob_json.h"
#include "lib/time/ob_hrtime.h"
#include "lib/file/file_directory_utils.h"
#include "lib/charset/ob_charset.h"
#include "utils/ob_layout.h"
#include "utils/ob_proxy_blowfish.h"
#include "obutils/ob_proxy_config_utils.h"
#include "dbconfig/protobuf/dds-api/database.pb.h"
#include "dbconfig/protobuf/dds-api/databaseAuthorities.pb.h"
#include "dbconfig/protobuf/dds-api/databaseVariables.pb.h"
#include "dbconfig/protobuf/dds-api/databaseProperties.pb.h"
#include "dbconfig/protobuf/dds-api/shardsTopology.pb.h"
#include "dbconfig/protobuf/dds-api/shardsDistribute.pb.h"
#include "dbconfig/protobuf/dds-api/shardsRouter.pb.h"
#include "dbconfig/protobuf/dds-api/shardsConnector.pb.h"
#include "dbconfig/protobuf/dds-api/shardsProperties.pb.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"
#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include <google/protobuf/any.pb.h>
#include <regex>

using namespace obsys;
using namespace google::protobuf;
using namespace com::alipay::dds::api::v1;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

static const std::regex MULTI_PATTERN("^(\\w+)_\\{(\\d+)-(\\d+)\\}(\\w*)$");
static const std::regex SINGLE_PATTERN("^(\\w+)_(\\d+)$");
static const std::regex INT_PATTERN("^Integer.valueOf\\(\\#(\\w+)\\#\\)(%\\d+)?$");
static const std::regex SUBSTR_PATTERN1("^Integer.valueOf\\(String.valueOf\\(\\#(\\w+)\\#\\)\\.getAt\\((\\-?\\d+)\\.+(\\-?\\d+)\\)\\)(%\\d+)?$");
static const std::regex SUBSTR_PATTERN2("^Integer.valueOf\\(\\s*\\#(\\w+)\\#\\.substring\\((\\-?\\d+),(\\d+)\\)\\s*%\\s*(\\d+)\\s*\\)$");
static const std::regex AD_SUBSTR_PATTERN1("^hash\\(substr\\(\\#(\\w+)\\#,\\s*(\\-?\\d+),\\s*(\\d+)\\),\\s*(\\d+)\\)$");
static const std::regex AD_SUBSTR_PATTERN2("^hash\\(substr\\(\\#(\\w+)\\#,\\s*(\\-?\\d+)\\),\\s*(\\d+)\\)$");
static const std::regex ELASTIC_PATTERN1("^toint\\(substr\\(\\#(\\w+)\\#,\\s*(\\-?\\d+),\\s*(\\d+)\\)\\)$");
static const std::regex ELASTIC_PATTERN2("^toint\\(substr\\(\\#(\\w+)\\#,\\s*(\\-?\\d+)\\)\\)$");

const char *OBPROXY_CONFIG_LOG_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%i:%s.%f";
//static const char *CONFIG_
static const char *CONFIG_TIMESTAMP             = "timestamp";
static const char *CONFIG_INSTANCE_IP           = "instance_ip";
static const char *CONFIG_INSTANCE_ZONE         = "instance_zone";
static const char *CONFIG_TENANT                = "tenant";
static const char *CONFIG_LOGICAL_DB            = "logical_db";
static const char *CONFIG_TYPE                  = "type";
static const char *CONFIG_NAME                  = "name";
static const char *CONFIG_VALUE                 = "value";

int ObProxyPbUtils::parse_database_prop_rule(const std::string &prop_rule, ObDataBaseProp &child_info)
{
  int ret = OB_SUCCESS;
  Value *json_root = NULL;
  ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
  Parser parser;
  if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(prop_rule.c_str(), prop_rule.length(), json_root))) {
    LOG_WARN("parse json failed", K(ret), "json_str", prop_rule.c_str());
  } else if (OB_ISNULL(json_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json root is null", K(ret));
  } else if (OB_FAIL(child_info.parse_db_prop_rule(*json_root))) {
    LOG_WARN("fail to parse db prop rule", K(child_info), K(ret));
  }
  return ret;
}

// ddsconsole000host:r10w10,ddsconsole001host:r10w10
// ddsconsole000host:r10w10,ddsconsole001host:r10w10,3:ddsconsole002host:r10w10
int ObProxyPbUtils::parse_group_cluster(const std::string &gc_name, const std::string &es_str, ObGroupCluster &gc_info)
{
  int ret = OB_SUCCESS;
  gc_info.gc_name_.set_value(gc_name.length(), gc_name.c_str());
  size_t pos = 0;
  size_t start = 0;
  int64_t last_eid = -1; // start from -1
  while (OB_SUCC(ret) && std::string::npos != (pos = es_str.find(",", pos))) {
    if (OB_FAIL(parse_es_info(es_str.substr(start, pos - start), gc_info, last_eid))) {
      LOG_WARN("fail to parse es_info", "group value", es_str.c_str());
    } else {
      ++pos;
      start = pos;
    }
  }
  if (OB_SUCC(ret) && start < es_str.length()) {
    if (OB_FAIL(parse_es_info(es_str.substr(start), gc_info, last_eid))) {
      LOG_WARN("fail to parse es_info", "group value", es_str.c_str());
    }
  }
  return ret;
}

int ObProxyPbUtils::parse_es_info(const std::string &sub_str, ObGroupCluster &gc_info, int64_t &last_eid)
{
  int ret = OB_SUCCESS;
  ObElasticInfo es_info;
  size_t colon_pos1 = std::string::npos;
  size_t colon_pos2 = std::string::npos;
  size_t rw_pos = std::string::npos;
  if (std::string::npos == (colon_pos1 = sub_str.find(":"))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tpo config", "group value", sub_str.c_str());
  } else if (std::string::npos != (colon_pos2 = sub_str.find(":", colon_pos1 + 1))) {
    rw_pos = sub_str.find("w", colon_pos2 + 1);
  } else {
    rw_pos = sub_str.find("w", colon_pos1 + 1);
  }
  if (std::string::npos == rw_pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tpo config", "group value", sub_str.c_str());
  }
  if (OB_SUCC(ret)) {
    ObString tmp_str;
    if (std::string::npos != colon_pos2) {
      tmp_str.assign_ptr(sub_str.c_str(), static_cast<ObString::obstr_size_t>(colon_pos1));
      get_int_value(tmp_str, es_info.eid_);
      tmp_str.assign_ptr(sub_str.c_str() + colon_pos1 + 1, static_cast<ObString::obstr_size_t>(colon_pos2 - colon_pos1 -1));
    } else {
      tmp_str.assign_ptr(sub_str.c_str(), static_cast<ObString::obstr_size_t>(colon_pos1));
      es_info.eid_ = last_eid + 1;
    }
    last_eid = es_info.eid_;
    es_info.shard_name_.set_value(tmp_str);
    const char *r_start_pos = tmp_str.ptr() + tmp_str.length() + 2; // start after 'r'
    tmp_str.assign_ptr(r_start_pos, static_cast<ObString::obstr_size_t>(sub_str.c_str() + rw_pos - r_start_pos));
    get_int_value(tmp_str, es_info.read_weight_);
    tmp_str.assign_ptr(sub_str.c_str() + rw_pos + 1, static_cast<ObString::obstr_size_t>(sub_str.length() - rw_pos - 1));
    get_int_value(tmp_str, es_info.write_weight_);
    gc_info.es_array_.push_back(es_info);
  }
  return ret;
}

int ObProxyPbUtils::parse_shard_rule(const std::string &rule_name,
                                     const std::string &rule_value,
                                     ObShardRule &rule_info)
{
  int ret = OB_SUCCESS;
  if (rule_name.compare("tbNamePattern") == 0) {
    rule_info.tb_name_pattern_.set_value(rule_value.length(), rule_value.c_str());
    if (OB_FAIL(parse_rule_pattern(rule_value, rule_info.tb_prefix_, rule_info.tb_size_, rule_info.tb_suffix_len_, rule_info.tb_tail_))) {
      LOG_WARN("fail to parse tbNamePattern", K(ret), "pattern_str", rule_value.c_str());
    }
  } else if (rule_name.compare("dbNamePattern") == 0) {
    rule_info.db_name_pattern_.set_value(rule_value.length(), rule_value.c_str());
    if (OB_FAIL(parse_rule_pattern(rule_value, rule_info.db_prefix_, rule_info.db_size_, rule_info.db_suffix_len_, rule_info.db_tail_))) {
      LOG_WARN("fail to parse dbNamePattern", K(ret), "pattern_str", rule_value.c_str());
    }
  } else if (rule_name.compare("tbRules") == 0) {
    if (OB_FAIL(parse_json_rules(rule_value, rule_info.tb_rules_, rule_info.allocator_))) {
      LOG_WARN("fail to parse tb rules", K(ret), "rules_str", rule_value.c_str());
    }
  } else if (rule_name.compare("dbRules") == 0) {
    if (OB_FAIL(parse_json_rules(rule_value, rule_info.db_rules_, rule_info.allocator_))) {
      LOG_WARN("fail to parse db rules", K(ret), "rules_str", rule_value.c_str());
    }
  } else if (rule_name.compare("elasticRules") == 0) {
    if (OB_FAIL(parse_json_rules(rule_value, rule_info.es_rules_, rule_info.allocator_))) {
      LOG_WARN("fail to parse elastic rules", K(ret), "rules_str", rule_value.c_str());
    }
  } else if (rule_name.compare("tbSuffixPadding") == 0) {
    rule_info.tb_suffix_.set_value(rule_value.length(), rule_value.c_str());
  } else {
    // TODO parse other rules
  }
  return ret;
}

int ObProxyPbUtils::parse_json_rules(const std::string &json_str, ObProxyShardRuleList &rule_list,
                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (json_str.length() > 0) {
    Value *json_root = NULL;
    ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
    Parser parser;
    if (OB_FAIL(parser.init(&json_allocator))) {
      LOG_WARN("json parser init failed", K(ret));
    } else if (OB_FAIL(parser.parse(json_str.c_str(), json_str.length(), json_root))) {
      LOG_INFO("parse json failed", "json_str", json_str.c_str(), K(ret));
    } else if (OB_FAIL(do_parse_json_rules(json_root, rule_list, allocator))) {
      LOG_WARN("fail to do_parse_json_rules", "json_str", json_str.c_str(), K(ret));
    }
  }
  if (OB_FAIL(ret) && json_str.compare("null") == 0) {
    ret = OB_SUCCESS;
    LOG_DEBUG("no rules, will ignore", "json_str", json_str.c_str());
  }
  return ret;
}

int ObProxyPbUtils::do_parse_json_rules(Value *json_root, ObProxyShardRuleList &rule_list, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json root is null", K(ret));
  } else if (JT_STRING == json_root->get_type()) {
  } else if (JT_ARRAY == json_root->get_type()) {
    ObProxyShardRuleInfo rule;
    DLIST_FOREACH(it, json_root->get_array()) {
      rule.reset();
      if (JT_STRING == it->get_type()) {
        if (OB_FAIL(force_parse_groovy(it->get_string(), rule, allocator))) {
          LOG_WARN("fail to pare groovy", K(ret));
        } else if (rule.is_valid()) {
          rule_list.push_back(rule);
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json config", K(ret));
  }

  return ret;
}

int ObProxyPbUtils::parse_rule_pattern(const std::string &pattern, ObProxyConfigString &name_prefix, int64_t &count, int64_t &suffix_len,
  ObProxyConfigString &name_tail)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("will parse regex", "regex", pattern.c_str());
  std::smatch sm1;
  std::smatch sm2;
  if (std::regex_match(pattern, sm1, MULTI_PATTERN)) {
    // multi tb or db
    ObString start_str;
    ObString end_str;
    ObString tail_str;
    start_str.assign_ptr(sm1.str(2).c_str(), static_cast<int32_t>(sm1.str(2).length()));
    end_str.assign_ptr(sm1.str(3).c_str(), static_cast<int32_t>(sm1.str(3).length()));
    tail_str.assign_ptr(sm1.str(4).c_str(), static_cast<int32_t>(sm1.str(4).length()));
    if (!tail_str.empty()) {
      name_tail.set_value(tail_str);
      LOG_INFO("tail_str ", "pattern", pattern.c_str(),K(tail_str));
    }
    int64_t start = -1;
    int64_t end = -1;
    suffix_len = start_str.length();
    if (OB_FAIL(get_int_value(start_str, start))) {
      LOG_WARN("fail to get int value for start", K(start_str), K(ret));
    } else if (OB_FAIL(get_int_value(end_str, end))) {
      LOG_WARN("fail to get int value for end", K(end_str), K(ret));
    } else {
      count = end - start + 1;
      name_prefix.set_value(sm1.str(1).length(), sm1.str(1).c_str());
    }
  } else {
    // single tb or db group
    count = 1;
    name_prefix.set_value(pattern.length(), pattern.c_str());
  }
  return ret;
}

int ObProxyPbUtils::force_parse_groovy(const ObString &expr,
                    ObProxyShardRuleInfo &info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  info.shard_rule_str_.set_value(expr);
  std::string pattern(expr.ptr(), expr.length());
  LOG_DEBUG("will parse regex", "regex", pattern.c_str());
  std::smatch sm;
  bool sub_str_match1 = false;
  bool sub_str_match2 = false;
  bool ad_sub_str_match1 = false;
  bool ad_sub_str_match2 = false;
  bool elastic_sub_str_match1 = false;
  bool elastic_sub_str_match2 = false;
  ObString parse_sql;
  #define OB_PROXY_MAX_CONFIG_STRING_LENGTH 512
  char sql[OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100];
  if ((sub_str_match1 = std::regex_match(pattern, sm, SUBSTR_PATTERN1))
      || (sub_str_match2 = std::regex_match(pattern, sm, SUBSTR_PATTERN2))
      || (ad_sub_str_match1 = std::regex_match(pattern, sm, AD_SUBSTR_PATTERN1))
      || (ad_sub_str_match2 = std::regex_match(pattern, sm, AD_SUBSTR_PATTERN2))
      || (elastic_sub_str_match1 = std::regex_match(pattern, sm, ELASTIC_PATTERN1))
      || (elastic_sub_str_match2 = std::regex_match(pattern, sm, ELASTIC_PATTERN2))) {
    int len = static_cast<int>(sm.str(1).length());
    int copy_len = len > OB_PROXY_MAX_CONFIG_STRING_LENGTH - 1 ? OB_PROXY_MAX_CONFIG_STRING_LENGTH - 1 : len;

    ObString start_str;
    ObString end_str;
    int64_t start_pos = -1;
    int64_t end_pos = -1;
    start_str.assign_ptr(sm.str(2).c_str(), static_cast<int32_t>(sm.str(2).length()));
    end_str.assign_ptr(sm.str(3).c_str(), static_cast<int32_t>(sm.str(3).length()));
    if (OB_FAIL(get_int_value(start_str, start_pos))) {
      LOG_WARN("fail to get int value for sub_string_start_", K(start_str), K(ret));
    } else if (OB_FAIL(get_int_value(end_str, end_pos))) {
      LOG_WARN("fail to get int value for sub_string_end", K(end_str), K(ret));
    } else {
      if (sub_str_match1) {
        snprintf(sql, OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100, "hash(substr(%.*s, %ld, %ld))",
                 copy_len, sm.str(1).c_str(),
                 start_pos >= 0 ? (start_pos + 1) : start_pos, end_pos - start_pos + 1);
      } else if (sub_str_match2) {
        snprintf(sql, OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100, "hash(substr(%.*s, %ld, %ld))",
                 copy_len, sm.str(1).c_str(),
                 start_pos, end_pos);
      } else if (ad_sub_str_match1 || elastic_sub_str_match1) {
        snprintf(sql, OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100, "hash(substr(%.*s, %ld, %ld))",
                 copy_len, sm.str(1).c_str(),
                 start_pos, end_pos);
      } else if (ad_sub_str_match2 || elastic_sub_str_match2) {
        snprintf(sql, OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100, "hash(substr(%.*s, %ld))",
                  copy_len, sm.str(1).c_str(), start_pos);
      }
      parse_sql.assign(sql, static_cast<ObString::obstr_size_t>(strlen(sql)));
    }
  } else if (std::regex_match(pattern, sm, INT_PATTERN)) {
    int len = static_cast<int>(sm.str(1).length());
    int copy_len = len > OB_PROXY_MAX_CONFIG_STRING_LENGTH - 1 ? OB_PROXY_MAX_CONFIG_STRING_LENGTH - 1 : len;
    snprintf(sql, OB_PROXY_MAX_CONFIG_STRING_LENGTH + 100, "hash(%.*s)", copy_len, sm.str(1).c_str());
    parse_sql.assign(sql, static_cast<ObString::obstr_size_t>(strlen(sql)));
  } else {
    parse_sql = expr;
  }

  if (OB_SUCC(ret)) {
    ObArenaAllocator parser_allocator;
    ObFuncExprParser parser(parser_allocator, SHARDING_EXPR_FUNC_PARSE_MODE);
    ObFuncExprParseResult result;
    ObProxyExprFactory factory(allocator);
    ObFuncExprResolverContext ctx(&allocator, &factory);
    ObFuncExprResolver resolver(ctx);
    if (OB_FAIL(parser.parse(parse_sql, result))) {
      LOG_WARN("parse failed", K(ret), K(parse_sql));
    } else if (OB_FAIL(resolver.resolve(result.param_node_, info.expr_))) {
      LOG_WARN("proxy expr resolve failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("sharding expr not supported", K(expr));
    ret = OB_SUCCESS;
  }

  return ret;
}

void ObProxyPbUtils::get_str_value_by_name(const std::string &shard_url,
    const char* name, obutils::ObProxyConfigString& value)
{
  size_t pos1 = shard_url.find(name);
  if (std::string::npos != pos1) {
    pos1 += strlen(name);
    std::string sub_str = shard_url.substr(pos1, shard_url.length() - pos1);
    size_t pos2 = sub_str.find("&");
    if (std::string::npos == pos2) {
      // value is the last param
      pos2 = sub_str.length();
    }
    value.set_value(pos2, sub_str.c_str());
    LOG_INFO(name, K(value));
  }
}
// shard_url format: jdbc:oceanbase://127.0.0.1:18587/group_00?useUnicode=true&characterEncoding=utf8
// or ocj shard url format: http://x.x.x.x/services?Action=xxx&User_ID=xxx&UID=xxx&ObRegion=xxx&database=xxx&k=v
int ObProxyPbUtils::parse_shard_url(const std::string &shard_url, ObShardConnector &conn_info)
{
  int ret = OB_SUCCESS;

  conn_info.shard_url_.set_value(shard_url.length(), shard_url.c_str());
  ObString tmp_str(shard_url.length(), shard_url.c_str());
  if (tmp_str.prefix_case_match(ObString::make_string("jdbc:"))) {
    // jdbc format
    size_t pos1 = shard_url.find("//");
    size_t pos2 = shard_url.find("?");
    if (std::string::npos == pos2) {
      // maybe no conn properties
      pos2 = shard_url.length();
    }
    if (std::string::npos != pos1 && std::string::npos != pos2 && pos2 > pos1) {
      std::string sub_str = shard_url.substr(pos1 + 2, pos2 - pos1 - 2);
      size_t pos3 = sub_str.find(":");
      size_t pos4 = sub_str.find("/");
      if (std::string::npos != pos3 && std::string::npos != pos4 && pos4 > pos3) {
        conn_info.physic_addr_.set_value(pos3, sub_str.c_str());

        ObString ip_src(pos3, sub_str.c_str());
        ObIpEndpoint ip;
        if (OB_FAIL(ops_ip_pton(ip_src, ip))) {
          // check whethe is a valid host
          char hname[MAX_HOST_NAME_LEN];
          snprintf(hname, MAX_HOST_NAME_LEN, "%.*s", static_cast<int>(pos3), sub_str.c_str());
          ObIpAddr ip_addr;
          int64_t valid_cnt;
          if (OB_FAIL(get_addr_by_host(hname, &ip_addr, 1, false, valid_cnt))) {
            LOG_WARN("invalid shard url, can not parse connector info", "shard_url", shard_url.c_str(), K(ret));
          } else {
            conn_info.is_physic_ip_ = false;
          }
        } else {
          conn_info.is_physic_ip_ = true;
        }

        conn_info.physic_port_.set_value(pos4 - pos3 - 1, sub_str.c_str() +  pos3 + 1);
        LOG_INFO("shard url", "port", conn_info.physic_port_, "shard url", conn_info.physic_addr_);
        conn_info.database_name_.set_value(sub_str.length() - pos4 - 1, sub_str.c_str() + pos4 + 1);
      }
    }
    get_str_value_by_name(shard_url, "obReadConsistency=", conn_info.read_consistency_);
  } else if (tmp_str.prefix_case_match(ObString::make_string("http://"))) {
    // ocj format
    get_str_value_by_name(shard_url, "database=", conn_info.database_name_);
    get_str_value_by_name(shard_url, "read_consistency=", conn_info.read_consistency_);
  } else {
    LOG_WARN("invalid shard url, can not parse connector info", "shard_url", shard_url.c_str());
  }

  return ret;
}

void ObProxyPbUtils::parse_shard_auth_user(ObShardConnector &conn_info)
{
  const static char separator = ':';
  const char *pos = NULL;
  ObString full_user_name = conn_info.full_username_.config_string_;
  ObString user;
  ObString tenant;
  ObString cluster;
  pos = full_user_name.find(separator);
  cluster = full_user_name.split_on(pos);
  tenant = full_user_name.split_on(separator);
  user = full_user_name;
  conn_info.tenant_name_.assign_ptr(tenant.ptr(), tenant.length());
  conn_info.cluster_name_.assign_ptr(cluster.ptr(), cluster.length());
  conn_info.username_.assign_ptr(user.ptr(), user.length());
}

int ObProxyPbUtils::dump_tenant_info_to_file(ObDbConfigLogicTenant &tenant_info)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = tenant_info.tenant_name_.config_string_;
  char cur_timestamp[OB_MAX_TIMESTAMP_LENGTH];
  if (OB_FAIL(convert_timestamp_to_version(hrtime_to_usec(get_hrtime_internal()),
                                           cur_timestamp, OB_MAX_TIMESTAMP_LENGTH))) {
    LOG_WARN("fail to convert current timestamp to version", K(ret));
  } else {
    ObString version;
    version.assign_ptr(cur_timestamp, static_cast<int32_t>(strlen(cur_timestamp)));
    tenant_info.set_version(version);
    char tenant_path[FileDirectoryUtils::MAX_PATH];
    char file_name[FileDirectoryUtils::MAX_PATH];
    ObSqlString buf;
    bool need_backup = false;
    snprintf(tenant_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s",
             get_global_layout().get_dbconfig_dir(),
             static_cast<int32_t>(tenant_name.length()), tenant_name.ptr());
    if (OB_FAIL(tenant_info.to_json_str(buf))) {
      LOG_WARN("fail to get json str for tenant info", K(tenant_name), K(ret));
    } else if (OB_FAIL(tenant_info.get_file_name(file_name, FileDirectoryUtils::MAX_PATH))) {
      LOG_WARN("fail to get file name for tenant info", K(tenant_name), K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write_to_file(tenant_path, file_name, buf.ptr(), buf.length(), need_backup))) {
      LOG_WARN("fail to write child info to file", K(ret), K(tenant_name));
    }
  }
  return ret;
}

int ObProxyPbUtils::dump_database_info_to_file(ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;
  char db_path[FileDirectoryUtils::MAX_PATH];
  char file_name[FileDirectoryUtils::MAX_PATH];
  ObSqlString buf;
  bool need_backup = false;
  const ObDataBaseKey &db_info_key = db_info.db_info_key_;
  const ObString &tenant_name = db_info_key.tenant_name_.config_string_;
  const ObString &database_name = db_info_key.database_name_.config_string_;
  snprintf(db_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s/%.*s",
           get_global_layout().get_dbconfig_dir(),
           static_cast<int32_t>(tenant_name.length()), tenant_name.ptr(),
           static_cast<int32_t>(database_name.length()), database_name.ptr());

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dump_child_array_to_file(db_info.da_array_))) {
      LOG_WARN("fail to write database authorities into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.dv_array_))) {
      LOG_WARN("fail to write database variables into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.dp_array_))) {
      LOG_WARN("fail to write database properties into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.st_array_))) {
      LOG_WARN("fail to write shards topologies into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.sr_array_))) {
      LOG_WARN("fail to write shards routers into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.sd_array_))) {
      LOG_WARN("fail to write shards distributes  into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.sc_array_))) {
      LOG_WARN("fail to write shards connectors into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(dump_child_array_to_file(db_info.sp_array_))) {
      LOG_WARN("fail to write shard properties into file", K(tenant_name), K(database_name), K(ret));
    } else if (OB_FAIL(db_info.to_json_str(buf))) {
      LOG_WARN("fail to get json str for database", K(ret));
    } else if (OB_FAIL(db_info.get_file_name(file_name, FileDirectoryUtils::MAX_PATH))) {
      LOG_WARN("fail to get file name for database", K(database_name), K(ret));
    } else if (OB_FAIL(ObProxyFileUtils::write_to_file(db_path, file_name, buf.ptr(), buf.length(), need_backup))) {
      LOG_WARN("fail to write database to file", K(ret));
    }
  }

  return ret;
}

template<typename T>
int ObProxyPbUtils::dump_child_array_to_file(ObDbConfigChildArrayInfo<T> &cr_array)
{
  int ret = OB_SUCCESS;
  typename ObDbConfigChildArrayInfo<T>::CCRHashMap &map = const_cast<typename ObDbConfigChildArrayInfo<T>::CCRHashMap &>(cr_array.ccr_map_);
  typename ObDbConfigChildArrayInfo<T>::CCRHashMap::iterator it = map.begin();
  for (; OB_SUCC(ret) && it != map.end(); ++it) {
    if (!it->need_dump_config()) {
      // do nothing
    } else if (OB_FAIL(dump_child_info_to_file(*it))) {
      LOG_WARN("fail to dump child info into file",  K(ret));
    } else {
      it->set_need_dump_config(false);
    }
  }
  return ret;
}

int ObProxyPbUtils::dump_child_info_to_file(const ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  char db_path[FileDirectoryUtils::MAX_PATH];
  char file_name[FileDirectoryUtils::MAX_PATH];
  ObSqlString buf;
  bool need_backup = false;
  const ObString &tenant_name = child_info.db_info_key_.tenant_name_.config_string_;
  const ObString &database_name = child_info.db_info_key_.database_name_.config_string_;
  snprintf(db_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s/%.*s",
           get_global_layout().get_dbconfig_dir(),
           static_cast<int32_t>(tenant_name.length()), tenant_name.ptr(),
           static_cast<int32_t>(database_name.length()), database_name.ptr());
  if (OB_FAIL(child_info.to_json_str(buf))) {
    LOG_WARN("fail to get json str for child info", K(child_info), K(ret));
  } else if (OB_FAIL(child_info.get_file_name(file_name, FileDirectoryUtils::MAX_PATH))) {
    LOG_WARN("fail to get file name for child info", K(child_info), K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::write_to_file(db_path, file_name, buf.ptr(), buf.length(), need_backup))) {
    LOG_WARN("fail to write child info to file", K(ret), K(child_info));
  }
  return ret;
}

int ObProxyPbUtils::init_and_dump_config_to_buf(common::ObSqlString &opt,
                                    //ObDbConfigLogicTenant &tenant_info,
                                    const ObDbConfigChild *child_info_p,
                                    const char *tenant_name, const int tenant_len,
                                    const char *logical_db, const int logical_db_len,
                                    const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  char file_name[FileDirectoryUtils::MAX_PATH];
  //const ObString &tenant_name = tenant_info.tenant_name_.config_string_;
  ObSqlString buf;
  if (OB_FAIL(child_info_p->to_json_str(buf))) {
    LOG_WARN("fail to get json str for tenant info", K(tenant_name), K(ret));
  } else if (OB_FAIL(child_info_p->get_file_name(file_name, FileDirectoryUtils::MAX_PATH))){
     LOG_WARN("fail to get file name for tenant info", K(tenant_name), K(ret));
  } else if (OB_FAIL(dump_config_to_buf(opt, buf.ptr(), static_cast<int>(buf.length()), tenant_name,
                                        tenant_len, logical_db, logical_db_len,
                                        type, file_name, static_cast<int>(STRLEN(file_name))))){
     LOG_WARN("fail to init child info to buf", K(ret), K(tenant_name));
  }
  return ret;
}

int ObProxyPbUtils::dump_config_to_buf(ObSqlString &opt_str,
                                const char *buf, const int buf_len,
                                const char *tenant, const int tenant_len,
                                const char *logical_db, const int logical_db_len,
                                const int type,
                                const char *file_name, const int file_name_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error to dump CRD info to buf", K(ret));
  } else {
    const char *zone = get_global_proxy_config().server_zone;
    const char *ip = get_global_proxy_config().instance_ip;
    const char *type_name  = "";
    char cur_timestamp[OB_MAX_TIMESTAMP_LENGTH];
    int64_t cur_timestamp_len = OB_MAX_TIMESTAMP_LENGTH;

    if (type >= TYPE_TENANT && type < TYPE_MAX) {
      type_name = get_type_task_name((ObDDSCrdType)type);
    } else if (type == ODP_CONFIG_LOG_START) { //just stand
      type_name = "ODPConfigStart";
    } else if (type == ODP_CONFIG_LOG_END) {
      type_name = "ODPConfigEnd";
    }

    int64_t pos = 0;
    int64_t time_us = hrtime_to_usec(get_hrtime_internal());
    if (OB_FAIL(ObTimeUtility::usec_format_to_str(time_us,
                                                ObString(OBPROXY_CONFIG_LOG_TIMESTAMP_FORMAT),
                                                cur_timestamp, cur_timestamp_len, pos))) {
      LOG_WARN("fail to format timestamp  to str", K(time_us), K(ret));
    } else if (OB_UNLIKELY(pos < 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid timestamp", K(time_us), K(pos), K(ret));
    } else {
      cur_timestamp[pos - 3] = '\0'; // ms
    }

    if (OB_SUCC(ret) && OB_FAIL(opt_str.append_fmt("{\"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":\"%.*s\", "
                                   "\"%s\":\"%.*s\",\"%s\":\"%.*s\", \"%s\":\"%.*s\", \"%s\":%.*s}",
                        CONFIG_TIMESTAMP, static_cast<int>(STRLEN(cur_timestamp)), cur_timestamp,
                        CONFIG_INSTANCE_IP, static_cast<int>((ip == NULL) ?  0 : STRLEN(ip)), (ip == NULL) ? "" : ip,
                        CONFIG_INSTANCE_ZONE, static_cast<int>((zone == NULL) ? 0 : STRLEN(zone)), (zone == NULL) ? "" : zone,
                        CONFIG_TENANT, tenant_len, tenant,
                        CONFIG_LOGICAL_DB, logical_db_len, logical_db,
                        CONFIG_TYPE, static_cast<int>(STRLEN(type_name)), type_name,
                        CONFIG_NAME, file_name_len, file_name,
                        CONFIG_VALUE, buf_len, buf)
       )) {
      LOG_WARN("dump config file into buffer error", K(ret), K(buf), K(logical_db), K(type), K(file_name));
    }
  }
  return ret;
}

int ObProxyPbUtils::dump_tenant_info_to_log(ObDbConfigLogicTenant &tenant_info)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = tenant_info.tenant_name_.config_string_;
  ObSqlString log_buf;
  if (OB_FAIL(init_and_dump_config_to_buf(log_buf, &tenant_info, tenant_name.ptr(), tenant_name.length(),
                                          "", 0, TYPE_TENANT))) {
    LOG_WARN("fail to write tenant info to log", K(ret));
  } else {
    LOG_DEBUG("CRD log info", K(ret), K(log_buf));
    _OBPROXY_CONFIG_LOG(INFO, "%.*s", static_cast<int>(log_buf.length()), log_buf.ptr());
  }

  return ret;
}

int ObProxyPbUtils::dump_database_info_to_log(ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  ObSqlString log_buf;
  const ObDataBaseKey &db_info_key = db_info.db_info_key_;
  const ObString &tenant_name = db_info_key.tenant_name_.config_string_;
  const ObString &database_name = db_info_key.database_name_.config_string_;

  if (OB_FAIL(dump_child_array_to_log(db_info.da_array_, TYPE_DATABASE_AUTH))) {
    LOG_WARN("fail to write database authorities into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.dv_array_, TYPE_DATABASE_VAR))) {
    LOG_WARN("fail to write database variables into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.dp_array_, TYPE_DATABASE_PROP))) {
    LOG_WARN("fail to write database properties into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.st_array_, TYPE_SHARDS_TPO))) {
    LOG_WARN("fail to write shards topologies into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.sr_array_, TYPE_SHARDS_ROUTER))) {
    LOG_WARN("fail to write shards routers into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.sd_array_, TYPE_SHARDS_DIST))) {
    LOG_WARN("fail to write shards distributes into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.sc_array_, TYPE_SHARDS_CONNECTOR))) {
    LOG_WARN("fail to write shards connectors into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(dump_child_array_to_log(db_info.sp_array_, TYPE_SHARDS_PROP))) {
    LOG_WARN("fail to write shard properties into log", K(tenant_name), K(database_name), K(ret));
  } else if (OB_FAIL(init_and_dump_config_to_buf(log_buf, &db_info, tenant_name.ptr(), tenant_name.length(),
                                                 database_name.ptr(), database_name.length(), TYPE_DATABASE))) {
    LOG_WARN("fail to write database to file", K(ret));
  } else {
    LOG_DEBUG("CRD log info ", K(ret), K(log_buf)); // keep it in DEBUG log
    _OBPROXY_CONFIG_LOG(INFO, "%.*s", static_cast<int>(log_buf.length()), log_buf.ptr());
  }

  return ret;
}

template<typename T>
int ObProxyPbUtils::dump_child_array_to_log(ObDbConfigChildArrayInfo<T> &cr_array,
                                              const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  typename ObDbConfigChildArrayInfo<T>::CCRHashMap &map
                    = const_cast<typename ObDbConfigChildArrayInfo<T>::CCRHashMap &>(cr_array.ccr_map_);
  typename ObDbConfigChildArrayInfo<T>::CCRHashMap::iterator it = map.begin();
  for (; OB_SUCC(ret) && it != map.end(); ++it) {
    if (OB_FAIL(dump_child_info_to_log(*it, type))) {
      LOG_WARN("fail to dump child info into log",  K(ret));
    }
  }
  return ret;
}

int ObProxyPbUtils::dump_child_info_to_log(const ObDbConfigChild &child_info, const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  ObSqlString log_buf;
  const ObString &tenant_name = child_info.db_info_key_.tenant_name_.config_string_;
  const ObString &database_name = child_info.db_info_key_.database_name_.config_string_;
  if (OB_FAIL(init_and_dump_config_to_buf(log_buf, &child_info, tenant_name.ptr(), tenant_name.length(),
                                          database_name.ptr(), database_name.length(), type))) {
    LOG_WARN("fail to write child info to log", K(ret), K(child_info));
  } else {
    LOG_DEBUG("CRD log info ", K(ret), K(log_buf), K(log_buf.length())); //
    _OBPROXY_CONFIG_LOG(INFO, "%.*s", static_cast<int>(log_buf.length()), log_buf.ptr());
  }
  return ret;
}

// end---

int ObProxyPbUtils::parse_local_child_config(ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  char db_path[FileDirectoryUtils::MAX_PATH];
  char file_name[FileDirectoryUtils::MAX_PATH];
  const ObString &tenant_name = child_info.db_info_key_.tenant_name_.config_string_;
  const ObString &database_name = child_info.db_info_key_.database_name_.config_string_;
  if (TYPE_TENANT == child_info.type_) {
    snprintf(db_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s",
             get_global_layout().get_dbconfig_dir(),
             static_cast<int32_t>(tenant_name.length()), tenant_name.ptr());
  } else {
    snprintf(db_path, FileDirectoryUtils::MAX_PATH, "%s/%.*s/%.*s",
             get_global_layout().get_dbconfig_dir(),
             static_cast<int32_t>(tenant_name.length()), tenant_name.ptr(),
             static_cast<int32_t>(database_name.length()), database_name.ptr());
  }
  if (OB_FAIL(child_info.get_file_name(file_name, FileDirectoryUtils::MAX_PATH))) {
    LOG_WARN("fail to get local file name", K(ret), K(database_name), K(tenant_name));
  } else if (OB_FAIL(do_parse_from_local_file(db_path, file_name, child_info))) {
    LOG_WARN("fail to do parse from local file", K(db_path), K(file_name), K(ret));
  }
  return ret;
}

int ObProxyPbUtils::do_parse_from_local_file(const char *dir, const char *file_name, ObDbConfigChild &child_info)
{
  int ret = OB_SUCCESS;
  int64_t read_len = 0;
  char *buf = NULL;
  Value *json_root = NULL;
  int64_t buf_size = 0;
  ObString json_str;
  if (OB_ISNULL(dir) || OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config file name or dir", K(dir), K(file_name), K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::calc_file_size(dir, file_name, buf_size))) {
    LOG_WARN("fail to get file size", K(dir), K(file_name), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(buf_size, ObModIds::OB_PROXY_FILE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(ObProxyFileUtils::read_from_file(dir, file_name, buf, buf_size, read_len))) {
    LOG_WARN("fail to read child config from file", K(ret), K(dir), K(file_name), K(buf_size));
  } else {
    ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
    Parser parser;
    if (OB_FAIL(parser.init(&allocator))) {
      LOG_WARN("json parser init failed", K(ret));
    } else if (OB_FAIL(parser.parse(buf, read_len, json_root))) {
      LOG_INFO("parse json failed", K(ret));
    } else if (OB_ISNULL(json_root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json root is null", K(ret));
    } else if (OB_FAIL(child_info.parse_from_json(*json_root))) {
      LOG_WARN("fail to parse from json", K(ret), K(child_info));
    } else {
      LOG_INFO("succ to parse child info from json", K(child_info));
    }
  }
  if (NULL != buf) {
    ob_free(buf);
    buf = NULL;
  }
  return ret;
}

bool ObProxyPbUtils::match_like(const common::ObString &str_text, const common::ObString &str_pattern)
{
  bool ret = false;
  if (str_text.empty()) {
    // return false if text config namme is NULL
  } else if (str_pattern.empty()) {
    ret = true;
  } else {
    ret = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, str_text, str_pattern, 0, '_', '%');
  }
  return ret;
}

int ObProxyPbUtils::get_physic_ip(const common::ObString& addr_str, bool is_physic_ip, sockaddr &addr)
{
  int ret = OB_SUCCESS;
  ObIpEndpoint ip;
  if (is_physic_ip) {
    if (OB_FAIL(ops_ip_pton(addr_str, ip))) {
      LOG_WARN("fail to ops ip pton", "physic_addr", addr_str);
    } else {
      addr = ip.sa_;
    }
  } else {
    // maybe switch ip, so need acquire everytime
    char hname[MAX_HOST_NAME_LEN];
    snprintf(hname, MAX_HOST_NAME_LEN, "%.*s", addr_str.length(), addr_str.ptr());
    ObIpAddr ip_addr;
    int64_t valid_cnt;
    if (OB_FAIL(get_addr_by_host(hname, &ip_addr, 1, false, valid_cnt))) {
      LOG_WARN("invalid physic addr can not parse connector info", "physic addr", addr_str);
    } else {
      ip.assign(ip_addr);
      addr = ip.sa_;
    }
  }

  return ret;
}


} // end dbconfig
} // end obproxy
} // end oceanbase

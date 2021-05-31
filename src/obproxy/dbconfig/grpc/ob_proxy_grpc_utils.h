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

#ifndef OBPROXY_GRPC_UTILS_H
#define OBPROXY_GRPC_UTILS_H
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

enum ObDDSCrdType{
  TYPE_TENANT = 0,
  TYPE_DATABASE,
  TYPE_DATABASE_AUTH,
  TYPE_DATABASE_VAR,
  TYPE_DATABASE_PROP,
  TYPE_SHARDS_TPO,
  TYPE_SHARDS_ROUTER,
  TYPE_SHARDS_DIST,
  TYPE_SHARDS_CONNECTOR,
  TYPE_SHARDS_PROP,
  TYPE_MAX
};

const char *get_type_url(const ObDDSCrdType type)
{
  static const char *type_url_array[TYPE_MAX] =
  {
    "", // tenant does not have any url
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.Database",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.DatabaseAuthorities",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.DatabaseVariables",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.DatabaseProperties",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.ShardsTopology",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.ShardsRouter",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.ShardsDistribute",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.ShardsConnector",
    "type.dds-api.com/dds.alipay-data.alipay.com.v1.ShardsProperties"
  };
  const char *str_ret = "";
  if (type >= TYPE_DATABASE && type < TYPE_MAX) {
    str_ret = type_url_array[type];
  }
  return str_ret;
}

ObDDSCrdType get_task_type(const common::ObString &name)
{
  ObDDSCrdType ret_type = TYPE_MAX;
  if (name == "Tenant") {
    ret_type = TYPE_TENANT;
  } else if (name == "Database") {
    ret_type = TYPE_DATABASE;
  } else if (name == "DatabaseAuthorities") {
    ret_type = TYPE_DATABASE_AUTH;
  } else if (name == "DatabaseVariables") {
    ret_type = TYPE_DATABASE_VAR;
  } else if (name == "DatabaseProperties") {
    ret_type = TYPE_DATABASE_PROP;
  } else if (name == "ShardsTopology") {
    ret_type = TYPE_SHARDS_TPO;
  } else if (name == "ShardsRouter") {
    ret_type = TYPE_SHARDS_ROUTER;
  } else if (name == "ShardsDistribute") {
    ret_type = TYPE_SHARDS_DIST;
  } else if (name == "ShardsConnector") {
    ret_type = TYPE_SHARDS_CONNECTOR;
  } else if (name == "ShardsProperties") {
    ret_type = TYPE_SHARDS_PROP;
  }
  return ret_type;
}

const char *get_type_task_name(const ObDDSCrdType type)
{
  static const char *task_name_array[TYPE_MAX] =
  {
    "Tenant",
    "Database",
    "DatabaseAuthorities",
    "DatabaseVariables",
    "DatabaseProperties",
    "ShardsTopology",
    "ShardsRouter",
    "ShardsDistribute",
    "ShardsConnector",
    "ShardsProperties"
  };
  const char *str_ret = "";
  if (type >= TYPE_TENANT && type < TYPE_MAX) {
    str_ret = task_name_array[type];
  }
  return str_ret;
}

const char *get_child_cr_name(const ObDDSCrdType type)
{
  static const char *cr_name_array[TYPE_MAX] =
  {
    "tenant",
    "database",
    "database_authorities",
    "database_variables",
    "database_properties",
    "shards_topology",
    "shards_router",
    "shards_distribute",
    "shards_connector",
    "shards_properties",
  };
  const char *str_ret = "";
  if (type >= TYPE_TENANT && type < TYPE_MAX) {
    str_ret = cr_name_array[type];
  }
  return str_ret;
}


} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_GRPC_UTILS_H */

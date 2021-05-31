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

#ifndef OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_
#define OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_

#include "ob_root_addr_agent.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace share
{

// store and fetch root server address list via REST style web service.
class ObWebServiceRootAddr : public ObRootAddrAgent
{
public:
  static const int64_t MAX_RECV_CONTENT_LEN = 512 * 1024; // 512KB

  ObWebServiceRootAddr() {}
  virtual ~ObWebServiceRootAddr() {}

  virtual int store(const ObRootAddrList &addr_list, const bool force);
  virtual int fetch(ObRootAddrList &addr_list);

public:
  /// store rs_list to URL
  ///
  /// @param rs_list           RS address list
  /// @param readonly_rs_list  readonly RS address list 
  /// @param appanme           cluster appanme
  /// @param url               target URL
  /// @param timeout_ms        timeout: ms
  ///
  /// @retval OB_SUCCESS success
  /// @retval other error code fail
  static int store_rs_list_on_url(const ObRootAddrList &rs_list, const char *appname,
      const char *url, const int64_t timeout_ms);

  /// get RS_LIST from URL
  ///
  /// @param [in] appanme              cluster appanme
  /// @param [in] url                  target URL
  /// @param [in] timeout_ms           timeout : ms
  /// @param [out] rs_list             address list of RS
  /// @param [out] readonly_rs_list    address list of readonly RS
  ///
  /// @retval OB_SUCCESS       success
  /// @retval other error code fail
  static int fetch_rs_list_from_url(const char *appname,
      const char *url, const int64_t timeout_ms, ObRootAddrList &rs_list);

  static int from_json(const char *json_str, const char *appname, ObRootAddrList &addr_list);
  static int to_json(const ObRootAddrList &addr_list, const char *appname, common::ObSqlString &json);
  static int add_to_list(common::ObString &addr_str, const int64_t sql_port,
                         common::ObString &role, ObRootAddrList &addr_list);

  // call web service, if post_data not NULL use POST method, else use GET method.
  static int call_service(const char *post_data,
      common::ObSqlString &content,
      const char *config_url,
      const int64_t timeout_ms);

  // curl write date interface
  static int64_t curl_write_data(void *ptr, int64_t size, int64_t nmemb, void *stream);

private:
  DISALLOW_COPY_AND_ASSIGN(ObWebServiceRootAddr);
};

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_WEB_SERVICE_ROOT_ADDR_H_

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

#ifndef OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_
#define OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_

#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
using common::ObString;
namespace obmysql
{

class OMPKChangeUser
    : public ObMySQLPacket
{
public:
  OMPKChangeUser();

  ~OMPKChangeUser() {}

  /**
   * Serialize all data not include packet header to buffer
   * @param buffer  buffer
   * @param len     buffer length
   * @param pos     buffer pos
   */
  int serialize(char *buffer, int64_t len, int64_t &pos) const;
  int decode();
  inline const ObString& get_username() { return username_; }
  inline const ObString& get_auth_plugin_name() { return auth_plugin_name_; }
  inline const ObString& get_auth_response() { return auth_plugin_data_; }
  inline void set_auth_plugin_name(const ObString &plugin_name) { auth_plugin_name_ = plugin_name; }
  inline void set_auth_response(const ObString &auth_response) { auth_plugin_data_ = auth_response; }
  virtual int64_t get_serialize_size() const;

private:
  //DISALLOW_COPY_AND_ASSIGN(OMPKChangeUser);
  uint8_t status_;
  ObString username_;
  ObString auth_plugin_data_;
  ObString auth_plugin_name_;
  ObString database_;
  uint8_t character_set_;
}; // end of class

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_ */

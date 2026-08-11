/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OMPK_STRING_H_
#define _OMPK_STRING_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKString
    : public ObMySQLPacket
{
public:
  explicit OMPKString(const common::ObString &str)
      : str_(str)
  {}

  virtual int64_t get_serialize_size() const { return str_.length(); }
  virtual ~OMPKString() {};
  int serialize(char *buffer, const int64_t length, int64_t &pos) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buffer) || length < pos) {
      ret = common::OB_INVALID_ARGUMENT;
    } else if (!str_.empty() && str_.length() <= length - pos) {
      MEMCPY(buffer + pos, str_.ptr(), str_.length());
      pos += str_.length();
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKString);
  const common::ObString &str_;
}; // end of class OMPKString

} // end of namespace obmysql
} // end of namespace oceanbase


#endif /* _OMPK_STRING_H_ */

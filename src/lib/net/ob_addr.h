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

#ifndef _OCEABASE_LIB_NET_OB_ADDR_H_
#define _OCEABASE_LIB_NET_OB_ADDR_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_se_array.h"
#include "lib/json/ob_yson.h"
#include "lib/ob_name_id_def.h"
namespace oceanbase
{
namespace common
{

class ObAddr
{
  OB_UNIS_VERSION(1);

public:
  enum VER {
    IPV4 = 4, IPV6 = 6
  };

  ObAddr()
      : version_(IPV4), ip_(), port_(0)
  {
    memset(&ip_, 0, sizeof(ip_));
  }

  ObAddr(VER version, const char *ip, const int32_t port)
      : version_(IPV4), ip_(), port_(0)
  {
    memset(&ip_, 0, sizeof(ip_));
    if (version == IPV4) {
      IGNORE_RETURN set_ipv4_addr(ip, port);
    } else if (version == IPV6) {
      IGNORE_RETURN set_ipv6_addr(ip, port);
    }
  }

  explicit ObAddr(const int64_t ipv4_server_id)
      : version_(IPV4), ip_(), port_(0)
  {
    ip_.v4_  = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
    port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
  }

  void reset()
  {
    port_ = 0;
    memset(&ip_, 0, sizeof (ip_));
  }

  inline bool is_ip_loopback() const
  {
    return (IPV4 == version_ && INADDR_LOOPBACK == ip_.v4_)
           || (IPV6 == version_ && IN6_IS_ADDR_LOOPBACK(ip_.v6_));
  }
  static uint32_t convert_ipv4_addr(const char *ip);

  int64_t to_string(char *buffer, const int64_t size) const;
  bool ip_to_string(char *buffer, const int32_t size) const;
  int ip_port_to_string(char *buffer, const int32_t size) const;
  TO_YSON_KV(ID(ip), ip_.v4_,
             ID(port), port_);

  bool set_ipv6_addr(const char *ip, const int32_t port);
  bool set_ipv4_addr(const char *ip, const int32_t port);
  bool set_ipv4_addr(const uint32_t ip, const int32_t port);
  bool set_ipv4_addr(const ObString &ip, const int32_t port);

  int parse_from_cstring(const char *ip_str);
  int64_t get_ipv4_server_id() const;
  void set_ipv4_server_id(const int64_t ipv4_server_id);

  int64_t hash() const;
  bool operator !=(const ObAddr &rv) const;
  bool operator ==(const ObAddr &rv) const;
  bool operator < (const ObAddr &rv) const;
  int compare(const ObAddr &rv) const;
  bool is_equal_except_port(const ObAddr &rv) const;
  inline int32_t get_version() const { return version_; }
  inline int32_t get_port() const { return port_; }
  inline uint32_t get_ipv4() const { return ip_.v4_; }
  uint64_t get_ipv6_high() const;
  uint64_t get_ipv6_low() const;
  void set_port(int32_t port);
  void set_max();
  bool is_valid() const;

  void reset_ipv4_10(int ip = 10);

private:
  VER version_;
  union
  {
    uint32_t v4_; //host byte order
    uint32_t v6_[4];
  } ip_;
  int32_t port_;
}; // end of class ObAddr

typedef ObSEArray<ObAddr, 16> ObAddrArray;

int64_t ObAddr::hash() const
{
  int64_t code = 0;

  if (IPV4 == version_) {
    code += (port_ + ip_.v4_);
  } else {
    code += (port_ + ip_.v6_[0] + ip_.v6_[1] + ip_.v6_[2] + ip_.v6_[3]);
  }

  return code;
}

bool ObAddr::operator !=(const ObAddr &rv) const
{
  return !(*this == rv);
}

bool ObAddr::operator ==(const ObAddr &rv) const
{
  return version_ == rv.version_ && port_ == rv.port_
         && ip_.v6_[0] == rv.ip_.v6_[0] && ip_.v6_[1] == rv.ip_.v6_[1]
         && ip_.v6_[2] == rv.ip_.v6_[2] && ip_.v6_[3] == rv.ip_.v6_[3];
}

int ObAddr::compare(const ObAddr &rv) const
{
  return memcmp(this, &rv, sizeof(ObAddr));
}

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_NET_OB_ADDR_H_ */

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
#include "iocore/net/ob_inet.h"
namespace oceanbase
{
namespace common
{

#define IPV6_LEN 16

class ObAddr
{
  OB_UNIS_VERSION(1);

public:
  enum VER {
    IPV4 = 4,
    IPV6 = 6,
    IPINVALID,
  };

  ObAddr()
      : version_(IPINVALID), ip_(), port_(0)
  {
    memset(&ip_, 0, sizeof(ip_));
  }

  ObAddr(VER version, const char *ip, const int32_t port)
      : version_(IPINVALID), ip_(), port_(0)
  {
    memset(&ip_, 0, sizeof(ip_));
    if (version == IPV4) {
      IGNORE_RETURN set_ipv4_addr(ip, port);
    } else if (version == IPV6) {
      IGNORE_RETURN set_ipv6_addr(ip, port);
    }
  }

  // The server_id format cannot be used in the case of IPV6
  // TODO: Consider removing it
  explicit ObAddr(const int64_t ipv4_server_id)
      : version_(IPINVALID), ip_(), port_(0)
  {
    // server_id only supports IPV4 format
    version_ = IPV4;
    ip_.v4_  = static_cast<int32_t>(0x00000000ffffffff & (ipv4_server_id >> 32));
    port_ = static_cast<int32_t>(0x00000000ffffffff & ipv4_server_id);
  }

  void reset()
  {
    version_ = IPINVALID;
    port_ = 0;
    memset(&ip_, 0, sizeof (ip_));
  }

  inline bool is_ip_loopback() const
  {
    return (IPV4 == version_ && INADDR_LOOPBACK == ip_.v4_)
           || (IPV6 == version_ && IN6_IS_ADDR_LOOPBACK(ip_.v6_));
  }
  int convert_ipv4_addr(const char *ip);
  int convert_ipv6_addr(const char *ip);
  struct sockaddr_storage get_sockaddr() const;
  void set_sockaddr(const struct sockaddr &sock_addr);

  int64_t to_string(char *buffer, const int64_t size) const;
  bool ip_to_string(char *buffer, const int32_t size) const;
  int ip_port_to_string(char *buffer, const int32_t size) const;
  TO_YSON_KV(ID(ip), ip_.v4_,
             ID(port), port_);

  bool set_ip_addr(const char *ip, const int32_t port);
  bool set_ip_addr(const ObString &ip, const int32_t port);
  bool set_ipv6_addr(const char *ip, const int32_t port);
  bool set_ipv6_addr(const uint64_t ipv6_high, const uint64_t ipv6_low, const int32_t port);
  bool set_ipv4_addr(const char *ip, const int32_t port);
  bool set_ipv4_addr(const uint32_t ip, const int32_t port);

  int parse_from_cstring(const char *ip_str);
  int64_t get_ipv4_server_id() const;
  void set_ipv4_server_id(const int64_t ipv4_server_id);

  int64_t hash() const;
  bool operator !=(const ObAddr &rv) const;
  bool operator ==(const ObAddr &rv) const;
  bool operator < (const ObAddr &rv) const;
  ObAddr& operator = (const ObAddr &rv);
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

public:
  VER version_;
  union
  {
    // v4 addresses are stored in native byte order,
    // v6 addresses are stored in network byte order
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

ObAddr& ObAddr::operator =(const ObAddr &rv)
{
  if (this != &rv) {
    version_ = rv.version_;
    port_ = rv.port_;
    MEMCPY(ip_.v6_, rv.ip_.v6_, sizeof(ip_.v6_));
  }
  return *this;
}

int ObAddr::compare(const ObAddr &rv) const
{
  return memcmp(this, &rv, sizeof(ObAddr));
}

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_NET_OB_ADDR_H_ */

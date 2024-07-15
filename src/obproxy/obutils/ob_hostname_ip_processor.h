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
 
#ifndef OBPROXY_HOSTNAME_IP_PROCESSOR_H
#define OBPROXY_HOSTNAME_IP_PROCESSOR_H

#include "obproxy/iocore/eventsystem/ob_continuation.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "obproxy/iocore/net/ob_inet.h"
#include "obproxy/obutils/ob_proxy_json_config_info.h"
#include "obproxy/obutils/ob_async_common_task.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObHostnameIpRefreshCont;
class ObHostnameIpRefreshProcessor;

class ObHostnameIpRefreshCont : public obutils::ObAsyncCommonTask
{
public:
  ObHostnameIpRefreshCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *submit_thread,
                          ObHostnameIpRefreshProcessor& refresh_processor)
      : ObAsyncCommonTask(m, "hostname refresh cont", cb_cont, submit_thread),
        refresh_processor_(refresh_processor), async_task_ret_(OB_SUCCESS) {}

  ~ObHostnameIpRefreshCont() {}

  void destroy();
  int init(const ObString& target_hostname);
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void* get_callback_data() {
    return &async_task_ret_;
  };

private:
  const static int MAX_HOSTNAME_LEN = 256; // according to DNS standard
  ObHostnameIpRefreshProcessor& refresh_processor_;
  ObFixedLengthString<MAX_HOSTNAME_LEN> hostname_;
  int64_t async_task_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObHostnameIpRefreshCont);
};

class ObHostnameIpRefreshProcessor
{
public:
  ObHostnameIpRefreshProcessor();
  ~ObHostnameIpRefreshProcessor();
  int sync_get_ip_by_hostname(const ObString& hostname, ObIArray<net::ObIpAddr>& ip_list);
  int async_refresh_ip_by_hostname(const ObString& target_hostname,
                                   event::ObContinuation * cb_cont,
                                   event::ObAction *&action);
  int refresh_hostname_ip_map(const ObString& target_hostname);
  int refresh_hostname_ip_map_all();
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:

  const static int HASH_MAP_SIZE = 128;
  const static int MAX_HOSTNAME_LEN = 256; // according to DNS standard
  struct ObHostnameIpItem
  {
    ObHostnameIpItem(): last_access_time_ns_(0), hostname_(0), ip_list_() {}
    ~ObHostnameIpItem() {}
    bool is_expired(const int64_t expired_time_ns) const { return event::get_hrtime() - last_access_time_ns_ > expired_time_ns; }
    void renew_last_access_time() { last_access_time_ns_ = event::get_hrtime(); }
    int64_t last_access_time_ns_;
    ObFixedLengthString<MAX_HOSTNAME_LEN> hostname_;
    ObSEArray<net::ObIpAddr, 2> ip_list_;
    LINK(ObHostnameIpItem, hostname_ip_item_link_);

    DECLARE_TO_STRING;
  };

  struct ObHostnameIpItemHashing
  {
    typedef const common::ObString Key;
    typedef ObHostnameIpItem Value;
    typedef ObDLList(ObHostnameIpItem, hostname_ip_item_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return ObString(value->hostname_.size(), value->hostname_.ptr()); }
    static bool equal(Key lhs, Key rhs) { return lhs.case_compare(rhs); }
  };

  typedef common::hash::ObBuildInHashMap<ObHostnameIpItemHashing, HASH_MAP_SIZE> HostnameIPHashMap;

  int do_refresh_hostname_ip(const ObString& hostname, ObIArray<net::ObIpAddr>& ip_list);
private:
  HostnameIPHashMap hostname_ip_map_;
  common::DRWLock hostname_ip_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObHostnameIpRefreshProcessor);
};

ObHostnameIpRefreshProcessor& get_binlog_service_hostname_ip_processor();

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_HOSTNAME_IP_PROCESSOR_H */
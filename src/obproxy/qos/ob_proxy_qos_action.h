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

#ifndef OB_PROXY_QOS_ACTION_H
#define OB_PROXY_QOS_ACTION_H

#include "lib/string/ob_string.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/time/ob_hrtime.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{

#define LIMIT_ACTION_RATIO 1000

typedef enum ObProxyQosActionType
{
  OB_PROXY_QOS_ACTION_TYPE_NONE = 0,
  OB_PROXY_QOS_ACTION_TYPE_BREAKER,
  OB_PROXY_QOS_ACTION_TYPE_CIRCUIT_BREAKER,
  OB_PROXY_QOS_ACTION_TYPE_LIMIT,
  OB_PROXY_QOS_ACTION_TYPE_MAX,
}ObProxyQosActionType;

class ObProxyQosAction
{
public:
  explicit ObProxyQosAction() : type_(OB_PROXY_QOS_ACTION_TYPE_NONE) {}
  ObProxyQosAction(ObProxyQosActionType type) : type_(type) {}
  ~ObProxyQosAction() {}

  ObProxyQosActionType get_action_type() const { return type_; }

  virtual int calc(bool &is_pass) = 0;

  int64_t to_string(char *buf, int64_t buf_len) const;

private:
  ObProxyQosActionType type_;
};

class ObProxyQosActionBreaker : public ObProxyQosAction
{
public:
  ObProxyQosActionBreaker() :
    ObProxyQosAction(OB_PROXY_QOS_ACTION_TYPE_BREAKER) {}
  virtual int calc(bool &is_pass);
};

class ObProxyQosActionCircuitBreaker : public ObProxyQosAction
{
public:
  ObProxyQosActionCircuitBreaker() :
    ObProxyQosAction(OB_PROXY_QOS_ACTION_TYPE_CIRCUIT_BREAKER), qps_(-1), rt_(-1),
                        time_window_(-1), limit_conn_(0), limit_fuse_time_(0),
                        end_fuse_time_(0), is_circuit_(false) {}
  virtual int calc(bool &is_pass);

  void set_cluster_name(const common::ObString &cluster_name) { cluster_name_ = cluster_name; }
  void set_tenant_name(const common::ObString &tenant_name) { tenant_name_ = tenant_name; }
  void set_database_name(const common::ObString &database_name) { database_name_ = database_name; }
  void set_user_name(const common::ObString &user_name) { user_name_ = user_name; }
  void set_qps(int64_t qps) { qps_ = qps; }
  void set_rt(int64_t rt) { rt_ = rt; }
  void set_time_window(int64_t time_window) { time_window_ = time_window; }
  void set_limit_conn(double limit_conn) { limit_conn_ = limit_conn; }
  void set_limit_fuse_time(int64_t fuse_time) { limit_fuse_time_ = fuse_time; }

private:
  common::ObString cluster_name_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  common::ObString user_name_;
  int64_t qps_; // unit r/s
  int64_t rt_; // unit us
  int64_t time_window_; // unit second 
  double limit_conn_;
  int64_t limit_fuse_time_;
  ObHRTime end_fuse_time_;

  bool is_circuit_;
};

class ObProxyQosActionLimit : public ObProxyQosAction
{
public:
  ObProxyQosActionLimit() : ObProxyQosAction(OB_PROXY_QOS_ACTION_TYPE_LIMIT),
                            limit_qps_(-1), token_(0), next_free_token_micros_(0) {}
  void set_limit_qps(int64_t limit_qps) { limit_qps_ = limit_qps * LIMIT_ACTION_RATIO; }
  int64_t get_limit_qps() const { return limit_qps_; }

  virtual int calc(bool &is_pass);

private:
  int do_calc(bool &is_pass);

private:
  common::DRWLock lock_;
  int64_t limit_qps_; // limit threshold * 1000. so qps lower limit is 0.001/s
  int64_t token_;
  int64_t next_free_token_micros_;
};

} // end qos
} // end obproxy
} // end oceanbase

#endif

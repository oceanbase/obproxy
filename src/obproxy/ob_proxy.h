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

#ifndef OBPROXY_H
#define OBPROXY_H

#include "lib/stat/ob_session_stat.h"
#include "obutils/ob_proxy_reload_config.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_tenant_stat_manager.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_mysql_client_pool.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace obproxy
{
namespace obutils
{
class ObProxyConfig;
class ObVipTenantProcessor;
class ObResourcePoolProcessor;
class ObAsyncCommonTask;
class ObTenantStatManager;
}
namespace proxy
{
class ObMysqlConfigParams;
}

class ObLogFileProcessor;
class ObInternalCmdProcessor;

// this class is the definition of ObProxy, which stands for the
// observer proxy. it it designed as a singleton instance in program.
// this class is only a aggregated structure, so do not put complex
// logical process in it. here's a typical usage:
//
//   ObProxy proxy;
//   proxy.init(...);
//   proxy.start();    // blocked , until program is going to die
//   proxy.destory();

struct ObProxyOptions
{
  // used to test in mysql
  ObProxyOptions();
  ~ObProxyOptions(){};

  bool has_specified_config() const;

  bool nodaemon_;
  bool execute_cfg_sql_;

  int32_t listen_port_;
  int32_t prometheus_listen_port_;
  int64_t upgrade_version_;

  const char *optstr_;
  const char *rs_list_;
  const char *rs_cluster_name_;
  const char *app_name_;
  const char *regression_test_;
};

class ObProxy : public obutils::ObIProxyReloadConfig
{
public:
  ObProxy();
  virtual ~ObProxy();

  int init(ObProxyOptions &opts, ObAppVersionInfo &proxy_version);
  void destroy();

  // start proxy, this function is blocked after invoking
  // until the prxy itself stops it.
  int start();
  int init_meta_client_proxy(const bool is_raw_init);

private:
  virtual int do_reload_config(obutils::ObProxyConfig &config);

  int check_member_variables();
  int init_mmp_init_cont();
  int init_user_specified_config();
  int init_conn_pool(const bool load_local_config_succ);
  int schedule_mmp_init_task();
  int init_local_config(bool &load_local_config_succ);
  int init_remote_config(const bool load_local_config_succ);
  int dump_config();
  int init_config();
  int init_resource_pool();
  int init_inner_request_env();
  int get_meta_table_server(common::ObIArray<proxy::ObProxyReplicaLocation> &replicas,
                            obutils::ObProxyConfigString &username);
  bool need_dump_config() const;

  static int do_repeat_task();
  obutils::ObAsyncCommonTask *get_meta_proxy_init_cont() { return mmp_init_cont_; }

private:
  bool is_inited_;
  // indicate whether this obproxy instance can provide normal service
  bool is_service_started_;
  // obproxy start relay on remote MetaDataBase; if failed, could not start up;
  bool is_force_remote_start_;
  int64_t start_time_;

  // used to handle obproxy meta table: ob_all_proxy, ob_all_proxy_config, etc.
  proxy::ObMysqlProxy meta_client_proxy_;
  obutils::ObAsyncCommonTask *mmp_init_cont_;

  obutils::ObResourcePoolProcessor *rp_processor_;
  obutils::ObConfigServerProcessor *cs_processor_;
  obutils::ObTenantStatManager *tenant_stat_mgr_;

  //log file cleanup
  ObLogFileProcessor *log_file_processor_;
  ObInternalCmdProcessor *internal_cmd_processor_;

  // proxy vip to tenant related
  obutils::ObVipTenantProcessor *vt_processor_;

  // proxy config related
  obutils::ObProxyConfig *config_;
  obutils::ObProxyReloadConfig reload_config_;
  obutils::ObProxyTableProcessor &proxy_table_processor_;
  obutils::ObHotUpgradeProcessor &hot_upgrade_processor_;

  // only used at start up
  proxy::ObMysqlConfigParams *mysql_config_params_;

  ObProxyOptions *proxy_opts_;
  ObAppVersionInfo *proxy_version_;

#if OB_HAS_TESTS
  // regression test
  ObRegressionCont regression_cont_;
#endif

  DISALLOW_COPY_AND_ASSIGN(ObProxy);
}; // end of class ObProxy

} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OB_PROXY_H */

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

#include "ob_proxy.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "ob_proxy_main.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_net.h"

#include "utils/ob_proxy_table_define.h"
#include "utils/ob_layout.h"

#include "qos/ob_proxy_qos_stat_processor.h"
#include "prometheus/ob_prometheus_processor.h"
#include "stat/ob_proxy_warning_stats.h"
#include "stat/ob_processor_stats.h"
#include "stat/ob_resource_pool_stats.h"
#include "stat/ob_net_stats.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_vip_tenant_processor.h"
#include "obutils/ob_log_file_processor.h"
#include "obutils/ob_proxy_config_utils.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "obutils/ob_hot_upgrade_processor.h"
#include "obutils/ob_task_flow_controller.h"
#include "obutils/ob_metadb_create_cont.h"
#include "obutils/ob_tenant_stat_manager.h"
#include "obutils/ob_proxy_config_processor.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/ob_proxy_inotify_processor.h"

#include "proxy/mysql/ob_mysql_proxy_server_main.h"
#include "proxy/route/ob_table_processor.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_sql_table_cache.h"
#include "proxy/route/ob_cache_cleaner.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"

#include "cmd/ob_show_net_handler.h"
#include "cmd/ob_show_warning_handler.h"
#include "cmd/ob_show_config_handler.h"
#include "cmd/ob_alter_config_handler.h"
#include "cmd/ob_dds_config_handler.h"
#include "cmd/ob_show_stat_handler.h"
#include "cmd/ob_show_cluster_handler.h"
#include "cmd/ob_alter_resource_handler.h"
#include "cmd/ob_show_memory_handler.h"
#include "cmd/ob_show_congestion_handler.h"
#include "cmd/ob_show_resource_handler.h"
#include "cmd/ob_show_info_handler.h"
#include "cmd/ob_show_vip_handler.h"
#include "cmd/ob_show_sm_handler.h"
#include "cmd/ob_show_session_handler.h"
#include "cmd/ob_kill_op_handler.h"
#include "cmd/ob_show_sqlaudit_handler.h"
#include "cmd/ob_show_trace_handler.h"
#include "cmd/ob_show_route_handler.h"
#include "cmd/ob_sequence_info_handler.h"
#include "cmd/ob_show_global_session_handler.h"
#include "cmd/ob_kill_global_session_handler.h"
#include "obutils/ob_session_pool_processor.h"
#include "obutils/ob_config_processor.h"
#include "iocore/net/ob_ssl_processor.h"
#include "ob_proxy_init.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::qos;

namespace oceanbase
{
namespace obproxy
{
//---------------------ObProxy---------------------//
ObProxyOptions::ObProxyOptions()
    : nodaemon_(false), execute_cfg_sql_(false),
      listen_port_(0), prometheus_listen_port_(0), upgrade_version_(-1),
      optstr_(NULL), rs_list_(NULL), rs_cluster_name_(NULL),
      app_name_(NULL), regression_test_(NULL)
{
}

bool ObProxyOptions::has_specified_config() const
{
  bool  bret = false;
  bret = (execute_cfg_sql_
          || 0 != listen_port_
          || 0 != prometheus_listen_port_
          || NULL != optstr_
          || NULL != rs_list_
          || NULL != rs_cluster_name_
          || NULL != app_name_);
  return bret;
}

ObProxy::ObProxy()
    : is_inited_(false), is_service_started_(false), is_force_remote_start_(false),
      start_time_(0), meta_client_proxy_(), mmp_init_cont_(NULL),
      rp_processor_(&get_global_resource_pool_processor()),
      cs_processor_(&get_global_config_server_processor()),
      tenant_stat_mgr_(&get_global_tenant_stat_mgr()),
      log_file_processor_(&get_global_log_file_processor()),
      internal_cmd_processor_(&get_global_internal_cmd_processor()),
      vt_processor_(&get_global_vip_tenant_processor()),
      config_(&get_global_proxy_config()),
      reload_config_(this),
      proxy_table_processor_(get_global_proxy_table_processor()),
      hot_upgrade_processor_(get_global_hot_upgrade_processor()),
      mysql_config_params_(NULL), proxy_opts_(NULL), proxy_version_(NULL)
{
}

ObProxy::~ObProxy()
{
  destroy();
}

int ObProxy::init(ObProxyOptions &opts, ObAppVersionInfo &proxy_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObProxy init twice", K(ret));
  } else if (OB_FAIL(check_member_variables())) {
    LOG_ERROR("fail to check member variables", K(ret));
  } else {
    proxy_opts_ = &opts;
    proxy_version_ = &proxy_version;
    start_time_ = ObTimeUtility::current_time();
    ObTableCache &table_cache = get_global_table_cache();
    ObPartitionCache &partition_cache = get_global_partition_cache();
    ObRoutineCache &routine_cache = get_global_routine_cache();
    ObSqlTableCache &sql_table_cache = get_global_sql_table_cache();
    ObTableProcessor &table_processor = get_global_table_processor();
    ObProxyConfigProcessor &app_config_processor = get_global_proxy_config_processor();
    ObDbConfigProcessor &dbconfig_processor = get_global_db_config_processor();

    if (OB_FAIL(log_file_processor_->init())) {
      LOG_ERROR("fail to init log file processor", K(ret));
    } else if (OB_FAIL(log_file_processor_->cleanup_log_file())) {
      LOG_WARN("fail to cleanup log file before start proxy", K(ret));
    } else if (OB_FAIL(table_cache.init(ObTableCache::TABLE_CACHE_MAP_SIZE))) {
      LOG_ERROR("fail to init table cache", K(ret));
    } else if (OB_FAIL(partition_cache.init(ObPartitionCache::PARTITION_CACHE_MAP_SIZE))) {
      LOG_ERROR("fail to init partition cache", K(ret));
    } else if (OB_FAIL(routine_cache.init(ObRoutineCache::ROUTINE_CACHE_MAP_SIZE))) {
      LOG_ERROR("fail to init routine cache", K(ret));
    } else if (OB_FAIL(sql_table_cache.init(ObSqlTableCache::SQL_TABLE_CACHE_MAP_SIZE))) {
      LOG_ERROR("fail to init sql table cache", K(ret));
    } else if (OB_FAIL(table_processor.init(&table_cache))) {
      LOG_ERROR("fail to init table processor", K(ret));
    } else if (OB_FAIL(g_ssl_processor.init())) {
      LOG_ERROR("fail to init ssl processor", K(ret));
    } else if (OB_FAIL(init_config())) {
      LOG_ERROR("fail to init config", K(ret));
    } else if (OB_FAIL(get_global_config_processor().init())) {
      LOG_ERROR("fail to init config processor", K(ret));
    } else if (OB_FAIL(config_->enable_sharding
                       && dbconfig_processor.init(config_->grpc_client_num, ObProxyMain::get_instance()->get_startup_time()))) {
      LOG_ERROR("fail to init dbconfig processor", K(ret));
    } else if (OB_FAIL(init_resource_pool())) {
      LOG_ERROR("fail to init resource pool", K(ret));
    } else if (OB_FAIL(g_stat_processor.init(meta_client_proxy_))) {
      LOG_ERROR("fail to init stat processor", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(vt_processor_->init())) {
        LOG_ERROR("fail to init vip tenant processor", K(ret));
      } else if (OB_FAIL(init_inner_request_env())) {
        LOG_ERROR("fail to init inner request env", K(ret));
      } else if (OB_FAIL(init_event_system(EVENT_SYSTEM_MODULE_VERSION))){
        LOG_ERROR("fail to init event system", K(ret));
      } else if (OB_FAIL(app_config_processor.init())) {
        LOG_ERROR("fail to init app config processor", K(ret));
      } else {
        if (RUN_MODE_PROXY == g_run_mode) {
          // if fail to init prometheus, do not stop the startup of obproxy
          if (OB_FAIL(g_ob_prometheus_processor.init())) {
            LOG_WARN("fail to init prometheus processor", K(ret));
          }
        }

#if OB_HAS_TESTS
        regression_cont_.set_regression_test(opts.regression_test_);
#endif
        ObNetOptions net_options;
        net_options.default_inactivity_timeout_ = usec_to_sec(config_->default_inactivity_timeout);
        net_options.max_client_connections_ = config_->client_max_connections;

        if (OB_FAIL(init_net(NET_SYSTEM_MODULE_VERSION, net_options))) {
          LOG_WARN("fail to init net", K(NET_SYSTEM_MODULE_VERSION), K(ret));
        } else if (OB_FAIL(init_net_stats())) {
          LOG_WARN("fail to init net_stats", K(ret));
        } else if (OB_FAIL(ObMysqlProxyServerMain::init_mysql_proxy_server(*mysql_config_params_))) {
          LOG_WARN("fail to init mysql_proxy_server", K(ret));
        } else if (OB_FAIL(init_processor_stats())) {
          LOG_WARN("fail to init processor_stats", K(ret));
        } else if (OB_FAIL(init_congestion_control())) {
          LOG_WARN("fail to init congestion_control", K(ret));
        } else if (OB_FAIL(init_resource_pool_stats())) {
          LOG_WARN("fail to init resource_pool_stats", K(ret));
        } else if (OB_FAIL(init_lock_stats())) {
          LOG_WARN("fail to init lock_stats", K(ret));
        } else if (OB_FAIL(init_warning_stats())) {
          LOG_WARN("fail to init warning_stats", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("obproxy init succeed", K(ret));
      is_inited_ = true;
    } else {
      LOG_WARN("fail to init obproxy", K(ret));
    }
  }
  return ret;
}

void ObProxy::destroy()
{
  if (!is_inited_) {
    LOG_WARN("destroy uninitialized ObProxy");
  } else {
    is_service_started_ = false;
    is_inited_ = false;
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(mmp_init_cont_))) {
      LOG_WARN("fail to destroy meta proxy init task", K(ret));
    }
    ObThreadId tid = 0;
    for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_ && OB_SUCC(ret); ++i) {
      tid = g_event_processor.all_dedicate_threads_[i]->tid_;
      if (OB_FAIL(thread_cancel(tid))) {
        PROXY_NET_LOG(WARN, "fail to do thread_cancel", K(tid), K(ret));
      } else if (OB_FAIL(thread_join(tid))) {
        PROXY_NET_LOG(WARN, "fail to do thread_join", K(tid), K(ret));
      } else {
        PROXY_NET_LOG(INFO, "graceful exit, dedicated thread exited", K(tid));
      }
      ret = OB_SUCCESS;//ignore error
    }

    for (int64_t i = 0; i < g_event_processor.event_thread_count_ && OB_SUCC(ret); ++i) {
      tid = g_event_processor.all_event_threads_[i]->tid_;
      if (OB_FAIL(thread_cancel(tid))) {
        PROXY_NET_LOG(WARN, "fail to do thread_cancel", K(tid), K(ret));
      } else if (OB_FAIL(thread_join(tid))) {
        PROXY_NET_LOG(WARN, "fail to do thread_join", K(tid), K(ret));
      } else {
        PROXY_NET_LOG(INFO, "graceful exit, event thread exited", K(tid));
      }
      ret = OB_SUCCESS;//ignore error
    }

    OB_LOGGER.destroy_async_log_thread();
    _exit(0);
  }
}

int ObProxy::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObProxy not init", K(ret));
  } else if (OB_FAIL(ObMysqlProxyServerMain::start_mysql_proxy_server(*mysql_config_params_))) {
    LOG_ERROR("fail to start mysql proxy server", K(ret));
  } else if (OB_FAIL(ObProxyMain::get_instance()->schedule_detect_task())) {
    LOG_ERROR("fail to schedule detect task", K(ret));
  } else {

    // we can't strongly dependent on the OCP.
    // So if register_proxy fails here,
    // we can re register_proxy by proxy_table_processor_ check_task later
    if (meta_client_proxy_.is_inited()) {
      // when start in server service mode and need_convert_vip_to_tname,
      // we should update vip tenant cache at start time
      if (config_->need_convert_vip_to_tname && !config_->is_client_service_mode()) {
        if (OB_SUCCESS != proxy_table_processor_.update_vip_tenant_cache()) {
          LOG_WARN("fail to update vip tenant cache");
        }
      }
    }

  }

  if (OB_SUCC(ret)) {
    char *password1 = NULL;
    char *password2 = NULL;
    password1 = getenv("observer_sys_password");
    password2 = getenv("observer_sys_password1");
    if (NULL != password1) {
      ObString key_string("observer_sys_password");
      ObString value_string(password1);
      if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, value_string))) {
        LOG_WARN("fail to update config", K(key_string));
      }
    }

    if (OB_SUCC(ret) && NULL != password2) {
      ObString key_string("observer_sys_password1");
      ObString value_string(password2);
      if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, value_string))) {
        LOG_WARN("fail to update config", K(key_string));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(get_global_proxy_config().dump_config_to_local())) {
      LOG_WARN("fail to dump config to local", K(ret));
    }
  }

  if (OB_SUCC(ret) && config_->is_metadb_used()) {
    if (is_force_remote_start_ && meta_client_proxy_.is_inited()) {
      if (OB_FAIL(meta_client_proxy_.clear_raw_execute())) {
         LOG_WARN("fail to clear raw execute", K(ret));
      } else if (OB_FAIL(ObMetadbCreateCont::create_metadb(&meta_client_proxy_))) {
        LOG_ERROR("fail to create metadb", K(ret));
      }
    } else if (OB_FAIL(schedule_mmp_init_task())) {
      LOG_WARN("fail to schedule meta mysql client proxy init task", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_reload_config(*config_))) {
      LOG_ERROR("fail to reload config", K(ret));
    } else if(OB_FAIL(get_global_mysql_config_processor().release(mysql_config_params_))){
      LOG_WARN("fail to release mysql config params", K(ret));
    } else if (OB_FAIL(ObCacheCleaner::schedule_cache_cleaner())) {
      LOG_WARN("fail to alloc and schedule cache cleaner", K(ret));
    } else if (config_->is_metadb_used() && OB_FAIL(proxy_table_processor_.start_check_table_task())) {
      LOG_WARN("fail to start check table task", K(ret));
    } else if (OB_FAIL(hot_upgrade_processor_.start_hot_upgrade_task())) {
      LOG_WARN("fail to start hot upgrade task", K(ret));
    } else if (OB_FAIL(log_file_processor_->start_cleanup_log_file())) {
      LOG_WARN("fail to start cleanup log file task", K(ret));
    } else if (config_->with_config_server_ && OB_FAIL(cs_processor_->start_refresh_task())) {
      LOG_WARN("fail to start refresh config server task", K(ret));
    } else if (config_->is_metadb_used() && OB_FAIL(g_stat_processor.start_stat_task())) {
      LOG_ERROR("fail to start stat task", K(ret));
    } else if (OB_FAIL(tenant_stat_mgr_->start_tenant_stat_dump_task())) {
      LOG_ERROR("fail to start_tenant_stat_dump_task", K(ret));
    } else if (OB_FAIL(g_ob_qos_stat_processor.start_qos_stat_clean_task())) {
      LOG_ERROR("fail to start_qos_stat_clean_task", K(ret));
    } else if (config_->enable_sharding
               && OB_FAIL(get_global_db_config_processor().start())) {
      LOG_WARN("fail to start sharding", K(ret));
    } else if (OB_FAIL(config_->is_pool_mode && get_global_session_pool_processor().start_session_pool_task())) {
      LOG_WARN("fail to start_session_pool_task", K(ret));
    } else if (OB_FAIL(ObMysqlProxyServerMain::start_mysql_proxy_acceptor())) {
      LOG_ERROR("fail to start accept server", K(ret));
    } else {
      if (RUN_MODE_PROXY == g_run_mode) {
        // if fail to init prometheus, do not stop the startup of obproxy
        if (OB_FAIL(g_ob_prometheus_processor.start_prometheus())) {
          LOG_WARN("fail to start prometheus", K(ret));
        }
      }

      mysql_config_params_ = NULL;
      ob_print_mod_memory_usage();
      ObMemoryResourceTracker::dump();
#if OB_HAS_TESTS
      ObTransformTest::run();
      g_event_processor.schedule_every(&regression_cont_, HRTIME_SECONDS(1));
      force_link_test();
#endif
      is_service_started_ = true;

      LOG_INFO("obproxy start up successful and can provide normal service", "cost time(us)",
               ObTimeUtility::current_time() - start_time_, K_(is_service_started));
      ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
      if (info.is_inherited_) {
        pid_t parent_pid= getppid();
        if (parent_pid != 1) {
          LOG_INFO("obproxy child will send SIGTERM to parent", K(parent_pid));
          kill(parent_pid, SIGUSR1);
        }
        info.is_parent_ = true;
      }
      if (RUN_MODE_PROXY == g_run_mode) {
        this_ethread()->execute();
      }
    }
  }
  return ret;
}

int ObProxy::check_member_variables()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rp_processor_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("rp_processor_ not init", K(ret));
  } else if (OB_ISNULL(cs_processor_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("cs_processor_ not init", K(ret));
  } else if (OB_ISNULL(tenant_stat_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_stat_mgr_ not init", K(ret));
  } else if (OB_ISNULL(log_file_processor_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("log_file_processor_ not init", K(ret));
  } else if (OB_ISNULL(internal_cmd_processor_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("internal_cmd_processor_ not init", K(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("config_ not init", K(ret));
  } else if (OB_ISNULL(vt_processor_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("vt_processor_ not init", K(ret));
  }
  return ret;
}

int ObProxy::init_user_specified_config()
{
  int ret = OB_SUCCESS;
  //1. set config from cmd -o name=value
  if (NULL != proxy_opts_->optstr_ && OB_FAIL(config_->add_extra_config_from_opt(proxy_opts_->optstr_))) {
    LOG_WARN("fail to add extra config", K(ret));

  //2. set config from cmd other opts
  } else if (NULL != proxy_opts_->rs_list_ && !config_->rootservice_list.set_value(proxy_opts_->rs_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to add rs list", "rs list", proxy_opts_->rs_list_, K(ret));
  } else if (NULL != proxy_opts_->rs_cluster_name_ && !config_->rootservice_cluster_name.set_value(proxy_opts_->rs_cluster_name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to add cluster_name", "cluster_name", proxy_opts_->rs_cluster_name_, K(ret));
  } else if (NULL != proxy_opts_->app_name_ && !config_->app_name.set_value(proxy_opts_->app_name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to add app_name", "app_name", proxy_opts_->app_name_, K(ret));

  } else {
    //3.1 if obproxy_config_server_url is empty, it must without config server
    config_->with_config_server_ = (STRLEN(config_->obproxy_config_server_url.str()) > 0 ? true : false);
    config_->with_control_plane_ = (STRLEN(config_->dataplane_host.str()) > 0 ? true : false);

    if (proxy_opts_->listen_port_ > 0) {
      config_->listen_port = proxy_opts_->listen_port_;
    }

    if (proxy_opts_->prometheus_listen_port_ > 0) {
      config_->prometheus_listen_port = proxy_opts_->prometheus_listen_port_;
    }

    //3.2 if change appname, we need reset local_config_version
    if (ObString::make_string(config_->app_name_str_) != config_->app_name.str()) {
      config_->set_app_name(config_->app_name);
      config_->current_local_config_version = 0;
    }
  }

  //4. check config
  if (OB_SUCC(ret)) {
    if (OB_FAIL(config_->check_proxy_serviceable())) {
      LOG_ERROR("fail to check proxy serviceable", K(ret));
    }
  }

  //5. fill inherited info
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_SUCC(ret)) {
    const bool is_server_service_mode = !config_->is_client_service_mode();
    if (OB_FAIL(info.fill_inherited_info(is_server_service_mode, proxy_opts_->upgrade_version_))) {
      LOG_ERROR("fail to fill inherited info", K(info), K(ret));
    }
  }

  return ret;
}

int ObProxy::init_meta_client_proxy(const bool is_raw_init)
{
  // 1. if proxy start with "-e" or has no config bin file, we should strongly
  //    depend on metadb, so we must init meta client proxy before start;
  // 2. if proxy start with local config bin file, we can schedule a repeat task to init
  //    meta client proxy so as to start more quickly, it will be stoped until meta client proxy init successfully,
  //    and do remember create metadb cluster resource
  //    after meta client proxy init successfully
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != config_) && OB_LIKELY(config_->is_metadb_used())) {
    ObProxyLoginInfo login_info;
    if (OB_FAIL(cs_processor_->get_proxy_meta_table_login_info(login_info))) {
      LOG_WARN("fail to get meta table login info", K(ret));
    } else if (OB_UNLIKELY(!login_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid proxy meta table login info", K(login_info), K(ret));
    }

    ObSEArray<ObProxyReplicaLocation, 3> replicas;
    if (is_raw_init) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_meta_table_server(replicas, login_info.username_))) {
          LOG_WARN("fail to get meta table server", K(ret));
        } else {
          LOG_DEBUG("get meta table server succ", K(replicas));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t timeout_ms = usec_to_msec(config_->short_async_task_timeout);
      char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
      ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
      if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(login_info.password_, passwd_string))) {
        LOG_WARN("fail to encrypt_passwd_to_stage1", K(login_info), K(ret));
      } else {
        passwd_string += 1;//trim the head'*'
        if (OB_FAIL(meta_client_proxy_.init(timeout_ms, login_info.username_, passwd_string, login_info.db_))) {
          LOG_WARN("fail to init client proxy", K(ret));
        } else {
          if (is_raw_init) {
            if (OB_FAIL(meta_client_proxy_.set_raw_execute(replicas))) {
              LOG_WARN("fail to set raw execute", K(ret));
            }
          } else {
            if (OB_FAIL(ObMetadbCreateCont::create_metadb(&meta_client_proxy_))) {
              // after meta_mysql_proxy init, start create_metadb cluster resource
              LOG_WARN("fail to create metadb cluster resource", K(ret));
              ret = OB_SUCCESS; // ignore ret, proxy can start without metadb if not raw_init
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("fail to init meta client proxy", K(is_raw_init), K(ret));
    } else {
      LOG_INFO("init meta client proxy succ", K(is_raw_init), K(replicas));
    }
  }
  return ret;
}

int ObProxy::do_repeat_task()
{
  int ret = OB_SUCCESS;
  ObProxyMain *proxy_main = ObProxyMain::get_instance();
  if (OB_LIKELY(NULL != proxy_main)) {
    ObProxy &proxy = proxy_main->get_proxy();
    const bool is_raw_init = false;
    if (OB_FAIL(proxy.init_meta_client_proxy(is_raw_init))) {
      LOG_WARN("fail to init meta mysql proxy, will retry");
    } else {
      LOG_INFO("succ to init meta mysql proxy");
      // stop the task
      ObAsyncCommonTask *cont = proxy.get_meta_proxy_init_cont();
      if (OB_LIKELY(NULL != cont)) {
        int64_t interval_us = 0;
        cont->set_interval(interval_us);
      }
    }
  }
  return ret;
}

int ObProxy::schedule_mmp_init_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != mmp_init_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mmp_init_cont should be null here", K_(mmp_init_cont), K(ret));
  } else {
    int64_t interval_us = 1 * 1000 * 1000; // 1s
    int64_t rdm_us = ObRandomNumUtils::get_random_half_to_full(interval_us);
    if (OB_ISNULL(mmp_init_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(rdm_us,
                                   "meta_mysql_proxy_init_task", ObProxy::do_repeat_task, NULL))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start meta proxy init task", K(ret));
    } else {
      LOG_INFO("succ to schedule meta proxy init task", K(interval_us));
    }
  }
  return ret;
}

int ObProxy::init_conn_pool(const bool load_local_config_succ)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cs_processor_->init(load_local_config_succ))) {
    LOG_ERROR("fail to init config server processor", K(ret));
  } else if (config_->is_metadb_used()
             && OB_FAIL(proxy_table_processor_.init(meta_client_proxy_, *proxy_version_, reload_config_))) {
    LOG_ERROR("fail to init proxy processor", K(ret));
  } else if (!config_->is_metadb_used()
             && OB_FAIL(hot_upgrade_processor_.init(meta_client_proxy_))) {
    LOG_ERROR("fail to init hot upgrade processor", K(ret));
  } else {
    // if we need init tables on metadb, or load local config failed, we need do force start refresh table
    is_force_remote_start_ = (proxy_opts_->execute_cfg_sql_ || !load_local_config_succ);

    if (config_->with_config_server_) {
      if (is_force_remote_start_ && config_->is_metadb_used()) {
        const bool is_raw_init = true;
        if (OB_FAIL(init_meta_client_proxy(is_raw_init))) {
          LOG_WARN("fail to init meta client proxy, maybe without metadb, continue", K(is_raw_init), K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        // will schedule mmp init task after server started
      }
    } else {
      // in test mod, do not depend on config server
      LOG_INFO("use test mode, do not depend on config server",
               "cluster_name", config_->rootservice_cluster_name.initial_value(),
               "cluster_rs_list", config_->rootservice_list.str());
      const ObString username (ObProxyTableInfo::TEST_MODE_USERNAME);
      const ObString password(ObProxyTableInfo::TEST_MODE_PASSWORD);
      const ObString db(ObProxyTableInfo::TEST_MODE_DATABASE);
      const ObString cluster_name(config_->rootservice_cluster_name.initial_value());
      if (OB_FAIL(cs_processor_->set_default_rs_list(cluster_name))) {
        LOG_WARN("fail to set opt rs list to web rs list", K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObProxy::init_local_config(bool &load_local_config_succ)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObProxyConfigUtils::load_config_from_file(*config_))) {
    load_local_config_succ = false;
    if (get_global_hot_upgrade_info().is_inherited_) {
      //if we failed to local config from local, proxy will get the config from OCP
      //and use the default value of SYS config item, it is not what we wanted,
      //so terminate startup
      LOG_WARN("fail to load config from file during hot upgrade, terminate startup",  K(ret));
    } else {
      if (OB_FAIL(config_->reset())) {
        LOG_WARN("fail to reset config",  K(ret));
      } else {
        LOG_INFO("fail to load config from file, but we can get the config from OCP by sql later", K(ret));
        ret = OB_SUCCESS;
      }
    }
  } else {
    load_local_config_succ = true;
    LOG_DEBUG("succ to load config from file", "current_local_config_version", config_->current_local_config_version.get());
  }

  if (OB_SUCC(ret)) {
    //user specified config has the highest priority
    if (OB_FAIL(init_user_specified_config())) {
      LOG_WARN("fail to init user assigned config", K(ret));
    }
  }
  return ret;
}

int ObProxy::init_remote_config(const bool load_local_config_succ)
{
  int ret = OB_SUCCESS;
  if (!config_->with_config_server_) {
    LOG_INFO("use test mode, no need init remote config");
  } else {
    if (proxy_opts_->execute_cfg_sql_) {
      LOG_INFO("we need execute config update sql", "execute_cfg_sql", proxy_opts_->execute_cfg_sql_);
      if (OB_FAIL(ObProxyConfigUtils::execute_config_update_sql(meta_client_proxy_))) {
        LOG_WARN("fail to execute config update sql",
                 "execute_cfg_sql", proxy_opts_->execute_cfg_sql_, K(ret));
      }
    }

    if (OB_SUCC(ret) && !load_local_config_succ) {
      LOG_INFO("we need get the config from OCP by sql now", K(load_local_config_succ));
      if (!meta_client_proxy_.is_inited()) {
        LOG_WARN("meta_client_proxy is not inited, can not init_remote_config, ignore");
      } else if (OB_FAIL(init_user_specified_config())) {
        LOG_ERROR("fail to init user specified config", K(ret));
      } else {
        if (OB_FAIL(proxy_table_processor_.load_remote_config())) {
          LOG_ERROR("fail to load remote config", K(ret));
          if (OB_MYSQL_ROUTING_MODE == ObProxyConfig::get_routing_mode(config_->server_routing_mode)) {
            LOG_INFO("in mysql mode, we just use default proxy config");
            ret = OB_SUCCESS;
          }
        }
        if (FAILEDx(init_user_specified_config())) {//user specified config has the highest priority
          LOG_ERROR("fail to init user specified config", K(ret));
        } else {/*do nothing*/}
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(proxy_table_processor_.init_kv_rows_name())) {
        LOG_ERROR("fail to init kv rows name", K(ret));
      }
    }
  }
  return ret;
}

bool ObProxy::need_dump_config() const
{
  bool  bret = false;
  bret = (NULL != proxy_opts_ && proxy_opts_->has_specified_config());
  return bret;
}

int ObProxy::dump_config()
{
  int ret = OB_SUCCESS;
  if (need_dump_config() && OB_FAIL(config_->dump_config_to_local())) {
    LOG_ERROR("fail to dump config to local config", K(ret));
  } else if (OB_FAIL(get_global_mysql_config_processor().reconfigure(*config_))) {
    LOG_ERROR("fail to reconfig mysql config", K(ret));
  } else if (OB_ISNULL(mysql_config_params_ = get_global_mysql_config_processor().acquire())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to acquire mysql config params", K_(mysql_config_params), K(ret));
  } else {
    config_->print_need_reboot_config();
    config_->print();
  }
  return ret;
}

int ObProxy::init_resource_pool()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rp_processor_->init(*config_, meta_client_proxy_))) {
    LOG_WARN("fail to init resource poll processor", K(ret));
  } else {
    if (config_->with_config_server_) {
      char cluster_name[OB_MAX_USER_NAME_LENGTH_STORE + 1];
      cluster_name[0] = '\0';
      if (OB_FAIL(cs_processor_->get_default_cluster_name(cluster_name, OB_MAX_USER_NAME_LENGTH_STORE + 1))) {
        LOG_WARN("fail to get default cluster name", K(ret));
      } else {
        ObString default_cname = '\0' == cluster_name[0]
                                 ? ObString::make_string(OB_PROXY_DEFAULT_CLUSTER_NAME)
                                 : ObString::make_string(cluster_name);
        if (OB_FAIL(rp_processor_->set_first_cluster_name(default_cname))) {
          LOG_WARN("fail to set default cluster name", K(default_cname), K(ret));
        }
      }
    } else {
      if (OB_FAIL(rp_processor_->set_first_cluster_name(ObString::make_string(config_->rootservice_cluster_name.initial_value())))) {
        LOG_WARN("fail to set default cluster name", "cluster_name", config_->rootservice_cluster_name.initial_value(), K(ret));
      }
    }
  }
  return ret;
}

int ObProxy::init_inner_request_env()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(internal_cmd_processor_->init(&reload_config_))) {
    LOG_ERROR("fail to init internal cmd processor", K(ret));
  } else if (OB_FAIL(show_net_cmd_init())) {
    LOG_ERROR("fail to init show_net_cmd", K(ret));
  } else if (OB_FAIL(show_sm_cmd_init())) {
    LOG_ERROR("fail to init show_sm_cmd", K(ret));
  } else if (OB_FAIL(show_session_cmd_init())) {
    LOG_ERROR("fail to init show_session_cmd", K(ret));
  } else if (OB_FAIL(kill_op_cmd_init())) {
    LOG_ERROR("fail to init kill_op_cmd", K(ret));
  } else if (OB_FAIL(show_config_cmd_init())) {
    LOG_ERROR("fail to init show_config_cmd", K(ret));
  } else if (OB_FAIL(alter_config_set_cmd_init())) {
    LOG_ERROR("fail to init alter_config_set_cmd", K(ret));
  } else if (OB_FAIL(dds_config_cmd_init())) {
    LOG_ERROR("fail to init dds_config_cmd", K(ret));
  } else if (OB_FAIL(alter_resource_delete_cmd_init())) {
    LOG_ERROR("fail to init alter_resource_delete_cmd_init", K(ret));
  } else if (OB_FAIL(show_stat_cmd_init())) {
    LOG_ERROR("fail to init show_stat_cmd", K(ret));
  } else if (OB_FAIL(show_cluster_cmd_init())) {
    LOG_ERROR("fail to init show_cluster_cmd", K(ret));
  } else if (OB_FAIL(show_memory_cmd_init())) {
    LOG_ERROR("fail to init show_memory_cmd", K(ret));
  } else if (OB_FAIL(show_congestion_cmd_init())) {
    LOG_ERROR("fail to init show_congestion_cmd", K(ret));
  } else if (OB_FAIL(show_resource_cmd_init())) {
    LOG_ERROR("fail to init show_resource_cmd", K(ret));
  } else if (OB_FAIL(show_sqlaudit_cmd_init())) {
    LOG_ERROR("fail to init show sqlaudit cmd", K(ret));
  } else if (OB_FAIL(show_info_cmd_init())) {
    LOG_ERROR("fail to init show info cmd", K(ret));
  } else if (OB_FAIL(show_vip_cmd_init())) {
    LOG_ERROR("fail to init show_vip_cmd", K(ret));
  } else if (OB_FAIL(show_route_cmd_init())) {
    LOG_ERROR("fail to init show_route_cmd", K(ret));
  } else if (OB_FAIL(show_trace_cmd_init())) {
    LOG_ERROR("fail to init show_trace_cmd", K(ret));
  } else if (OB_FAIL(show_warning_cmd_init())) {
    LOG_ERROR("fail to init show_warning_cmd", K(ret));
  } else if (OB_FAIL(sequence_info_cmd_init())) {
    LOG_ERROR("fail to init sequence_info_cmd_init", K(ret));
  } else if (OB_FAIL(show_global_session_info_cmd_init())) {
    LOG_ERROR("fail to init show_global_session_info_cmd_init", K(ret));
  } else if (OB_FAIL(kill_global_session_info_cmd_init())){
    LOG_ERROR("fail to init kill_global_session_info_cmd_init", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObProxy::init_config()
{
  int ret = OB_SUCCESS;
  bool load_local_config_succ = false;

  if (OB_FAIL(init_local_config(load_local_config_succ))) {
    LOG_WARN("fail to init local config", K(ret));
  } else if (OB_FAIL(config_->init_need_reboot_config())) {
    LOG_WARN("fail to init need reboot config", K(ret));
  } else if (OB_FAIL(init_conn_pool(load_local_config_succ))) {
    LOG_WARN("fail to init connection pool", K(ret));
  } else if (OB_FAIL(init_remote_config(load_local_config_succ))) {
    LOG_WARN("fail to init remote config", K(ret));
  } else if (OB_FAIL(config_->init_need_reboot_config())) {
    LOG_WARN("fail to init need reboot config", K(ret));
  } else if (OB_FAIL(dump_config())) {
    LOG_WARN("fail to dump config", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObProxy::do_reload_config(obutils::ObProxyConfig &config)
{
  int ret = OB_SUCCESS;

  // notify to modify config value
  LOG_INFO("obproxy reload load new config", K(ret));
  {
    // set sys mod log
    obsys::CRLockGuard guard(config.rwlock_);
    if (OB_FAIL(OB_LOGGER.set_mod_log_levels(config.syslog_level))) {
      LOG_ERROR("fail to set sys log levels", "value", config.syslog_level.str(), K(ret));
    } else if (OB_FAIL(OB_LOGGER.set_monitor_log_level(config.monitor_log_level))) {
      LOG_ERROR("fail to set monitor log level", "value", config.monitor_log_level.str(), K(ret));
    } else if (OB_FAIL(OB_LOGGER.set_xflush_log_level(config.xflush_log_level))) {
      LOG_ERROR("fail to set xflush log level", "value", config.xflush_log_level.str(), K(ret));
    }
    OB_LOGGER.set_max_file_size(config.max_log_file_size);
  }

  OB_LOGGER.set_enable_async_log(config.enable_async_log);

  if (OB_SUCC(ret)) {
    int64_t relative_expire_time_ms = config.partition_location_expire_relative_time;
    int64_t relative_sql_table_expire_time_ms = config.sql_table_cache_expire_relative_time;
    if (0 != relative_expire_time_ms) {
      ObTableCache &table_cache = get_global_table_cache();
      ObPartitionCache &part_cache = get_global_partition_cache();
      ObSqlTableCache &sql_table_cache = get_global_sql_table_cache();
      table_cache.set_cache_expire_time(relative_expire_time_ms);
      part_cache.set_cache_expire_time(relative_expire_time_ms);
      sql_table_cache.set_cache_expire_time(relative_sql_table_expire_time_ms);
      LOG_INFO("current table cache and part cache will exipre", K(relative_expire_time_ms), K(relative_sql_table_expire_time_ms),
               "table entry expire_time_us", table_cache.get_cache_expire_time_us(),
               "part entry expire_time_us", part_cache.get_cache_expire_time_us(),
               "sql table entry expire time us", sql_table_cache.get_cache_expire_time_us());
      config.partition_location_expire_relative_time = 0;
      config.sql_table_cache_expire_relative_time = 0;
    }
  }

  if (OB_SUCC(ret)) {
    // username separator related
    obsys::CRLockGuard guard(config.rwlock_);
    const int64_t len = std::min(config.username_separator.size(),
                                 static_cast<int64_t>(ObProxyAuthParser::MAX_UNFORMAL_FORMAT_SEPARATOR_COUNT));
    const int64_t MAX_SEPARATOR_STR_LENGTH = 2;// one separator + one eof
    char separator_str[MAX_SEPARATOR_STR_LENGTH];
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (OB_FAIL(config.username_separator.get(i, separator_str, static_cast<int64_t>(sizeof(separator_str))))) {
        LOG_WARN("fail to get username_separator segment", K(i), K(separator_str), K(ret));
      } else {
        ObProxyAuthParser::unformal_format_separator[i] = separator_str[0];
      }
    }
    ObProxyAuthParser::unformal_format_separator[len] = '\0';
  }

  if (OB_SUCC(ret)) {
    int64_t timeout_ms = usec_to_msec(config.short_async_task_timeout);
    if (OB_FAIL(meta_client_proxy_.set_timeout_ms(timeout_ms))) {
      LOG_WARN("fail to set client proxy timeout", K(timeout_ms), K(ret));
    }
  }

  if (OB_SUCC(ret) && is_service_started_) {
    // resource pool related
    if (OB_FAIL(rp_processor_->update_config_param())) {
      LOG_WARN("fail to update resource pool config param", K(ret));
    } else if (OB_FAIL(tenant_stat_mgr_->set_stat_table_sync_interval())) {
      LOG_WARN("fail to update tenant stat dump interval", K(ret));

    // share timer related
    } else if (OB_FAIL(g_stat_processor.set_stat_table_sync_interval())) {
      LOG_WARN("fail to update stat table sync task interval", K(ret));
    } else if (OB_FAIL(g_stat_processor.set_stat_dump_interval())) {
      LOG_WARN("fail to update stat dump interval", K(ret));
    } else if (OB_FAIL(log_file_processor_->set_log_cleanup_interval())) {
      LOG_WARN("fail to update log cleanup interval", K(ret));
    } else if (config_->with_config_server_ && OB_FAIL(cs_processor_->set_refresh_interval())) {
      LOG_WARN("fail to update config server refresh interval", K(ret));
    } else if (config_->is_metadb_used() && OB_FAIL(proxy_table_processor_.set_check_interval())) {
      LOG_WARN("fail to update table processor check interval", K(ret));
    } else if (OB_FAIL(ObCacheCleaner::update_clean_interval())) {
      LOG_WARN("fail to update clean interval", K(ret));
    } else {/*do nothing*/}
  }

  if (OB_SUCC(ret)) {
    // net related
    ObNetOptions net_options;
    net_options.default_inactivity_timeout_ = usec_to_sec(config.default_inactivity_timeout);
    net_options.max_client_connections_ = config.client_max_connections;
    update_net_options(net_options);
    ObMysqlConfigProcessor &mysql_config_processor = get_global_mysql_config_processor();
    if (OB_FAIL(mysql_config_processor.reconfigure(*config_))) {
      LOG_ERROR("fail to reconfig mysql config", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    get_pl_task_flow_controller().set_normal_threshold(config_->normal_pl_update_threshold);
    get_pl_task_flow_controller().set_limited_threshold(config_->limited_pl_update_threshold);
  }

  return ret;
}

int ObProxy::get_meta_table_server(ObIArray<ObProxyReplicaLocation> &replicas, ObProxyConfigString &username)
{
  int ret = OB_SUCCESS;
  replicas.reset();
  ObRawMysqlClient raw_client;
  const int64_t timeout_ms = usec_to_msec(config_->short_async_task_timeout);
  const ObString cluster_name(OB_META_DB_CLUSTER_NAME);
  const ObString user_name(ObProxyTableInfo::READ_ONLY_USERNAME);
  const ObString database(ObProxyTableInfo::READ_ONLY_DATABASE);
  ObString password(config_->observer_sys_password.str());
  ObString password1(config_->observer_sys_password1.str());
  ObSEArray<ObAddr, 5> rs_list;
  int64_t cluster_version = 0;

  // first get from local
  if (OB_FAIL(cs_processor_->get_cluster_rs_list(cluster_name, OB_DEFAULT_CLUSTER_ID, rs_list))) {
    LOG_INFO("fail to get rs list from local, try to get from remote", K(cluster_name));
    rs_list.reset();
    ret = OB_SUCCESS;
  }

  // if failed, try to get from remote
  if (rs_list.empty()) {
    if (OB_FAIL(cs_processor_->get_newest_cluster_rs_list(cluster_name, OB_DEFAULT_CLUSTER_ID, rs_list))) {
      LOG_WARN("fail to get newest rs list through config server processor", K(cluster_name), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (rs_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs_list count must > 0", K(ret));
    } else if (OB_FAIL(raw_client.init(user_name, password, database, cluster_name, password1))) {
      LOG_WARN("fail to init raw mysql client", K(ret));
    } else if (OB_FAIL(raw_client.set_server_addr(rs_list))) {
      LOG_WARN("fail to set server addr", K(ret));
    }

    if (OB_SUCC(ret)) {
      const char *sql = "select ob_version() as cluster_version";
      ObClientMysqlResp *resp = NULL;
      ObResultSetFetcher *rs_fetcher = NULL;
      if (OB_FAIL(raw_client.sync_raw_execute(sql, timeout_ms, resp))) {
        LOG_WARN("fail to sync raw execute", K(resp), K(timeout_ms), K(ret));
      } else if (OB_ISNULL(resp)) {
        ret = OB_NO_RESULT;
        LOG_WARN("resp is null", K(ret));
      } else if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WARN("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rs fetcher is NULL", K(ret));
      }

      while (OB_SUCC(ret) && OB_SUCC(rs_fetcher->next())) {
        ObString cluster_version_str;
        PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(*rs_fetcher, "cluster_version", cluster_version_str);
        int64_t version = 0;
        for (int64_t i = 0; i < cluster_version_str.length() && cluster_version_str[i] != '.'; i++) {
          version = version * 10 + cluster_version_str[i] - '0';
        }
        cluster_version = version;
      }

      ret = OB_SUCCESS;

      if (NULL != resp) {
        op_free(resp);
        resp = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      ObString meta_tenant_name = username.after('@');
      if (meta_tenant_name.empty()) {
        meta_tenant_name.assign_ptr(OB_SYS_TENANT_NAME,
                                    static_cast<ObString::obstr_size_t>(strlen(OB_SYS_TENANT_NAME)));
      }
      char sql[OB_SHORT_SQL_LENGTH];
      sql[0] = '\0';
      ObTableEntryName name;
      name.cluster_name_ = cluster_name;
      name.tenant_name_ = meta_tenant_name;
      name.database_name_ = database;
      name.table_name_.assign_ptr(OB_ALL_DUMMY_TNAME,
                                  static_cast<ObString::obstr_size_t>(strlen(OB_ALL_DUMMY_TNAME)));
      ObClientMysqlResp *resp = NULL;
      ObResultSetFetcher *rs_fetcher = NULL;
      ObTableEntry *entry = NULL;
      ObProxyPartitionLocation pl;
      if (OB_FAIL(ObRouteUtils::get_table_entry_sql(sql, OB_SHORT_SQL_LENGTH, name, false, cluster_version))) {
        LOG_WARN("fail to get table entry sql", K(sql), K(ret));
      } else if (OB_FAIL(raw_client.sync_raw_execute(sql, timeout_ms, resp))) {
        LOG_WARN("fail to sync raw execute", K(sql), K(resp), K(timeout_ms), K(ret));
      } else if (OB_ISNULL(resp)) {
        ret = OB_NO_RESULT;
        LOG_WARN("resp is NULL", K(ret));
      } else if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WARN("fail to get resultset fetcher", K(sql), K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rs fetcher is NULL", K(ret));
      } else if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(name, 0, OB_DEFAULT_CLUSTER_ID, entry))) {
        LOG_WARN("fail to alloc and init table entry", K(name), K(ret));
      } else if (OB_FAIL(ObRouteUtils::fetch_table_entry(*rs_fetcher, *entry, cluster_version))) {
        LOG_WARN("fail to fetch one table entry info", K(ret));
      } else if (OB_FAIL(entry->get_random_servers(pl))) {
        LOG_WARN("fail to get random servers", K(ret));
      } else {
        const ObProxyReplicaLocation *rl = NULL;
        const int64_t valid_count = pl.replica_count();
        for (int64_t i = 0; (i < valid_count) && OB_SUCC(ret); ++i) {
          if (NULL != (rl = pl.get_replica(i))
              && rl->is_valid()
              && OB_FAIL(replicas.push_back(*rl))) {
            LOG_WARN("fail to push back", KPC(rl), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (replicas.empty()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("can not find addr", K(ret));
        }
      }

      if (NULL != resp) {
        op_free(resp);
        resp = NULL;
      }

      if (NULL != entry) {
        entry->dec_ref();
        entry = NULL;
      }
    }
  }
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase

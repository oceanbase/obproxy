/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "ob_proxy_main.h"

#include <malloc.h>
#include <sys/utsname.h>

#include "lib/ob_errno.h"
#include "lib/utility/ob_preload.h"
#include "lib/alloc/alloc_func.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/wide_integer/ob_wide_integer.h"

#include "utils/ob_proxy_utils.h"
#include "utils/ob_layout.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obproxy/obutils/ob_hot_upgrade_processor.h"
#include "obproxy/proxy/rpc/redis/ob_rpc_redis_stat.h"
#include "obproxy/proxy/rpc/rpclib/ob_rpc_throttle.h"

#include "stat/ob_proxy_warning_stats.h"

#include "obutils/ob_proxy_config_utils.h"
#include "obutils/ob_async_common_task.h"

#include "cmd/ob_show_sqlaudit_handler.h"
#include "ob_proxy_init.h"
#include "lib/charset/ob_charset.h"

#include "obproxy/prometheus/ob_memory_prometheus.h"
#include "obproxy/prometheus/ob_prometheus_info.h"

#include "obproxy/proxy/mysqllib/ob_session_field_mgr.h"

#include "lib/signal/ob_signal_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::prometheus;

namespace oceanbase
{
namespace obproxy
{

static const int64_t OB_MAX_LOG_FILE_NAME_LEN = 128;

ObProxyMain *ObProxyMain ::instance_ = NULL;

static const int64_t INTERNAL_MAX_MEM_LIMIT = 100 * (1LL << 30); // 100G

int ObProxyMain::print_args(const int argc, char *const argv[]) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc <= 0) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid argument, argc=%d, argv=%p, ret=%d", argc, argv, ret);
  } else {
    for (int i = 0; i < argc - 1; ++i) {
      fprintf(stderr, "%s ", argv[i]);
    }
    MPRINT("%s", argv[argc - 1]);
  }
  return ret;
}

void ObProxyMain::destroy()
{
  struct sigaction action;
  add_sig_default_catched(action, SIGINT);
  add_sig_default_catched(action, SIGTERM);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(sig_detect_cont_))) {
    LOG_WDIAG("fail to destroy sig detect task", K(ret));
  }
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(mem_monitor_cont_))) {
    LOG_WDIAG("fail to destroy mem monitor task", K(ret));
  }
  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(sqlaudit_detect_cont_))) {
    LOG_WDIAG("fail to destroy sqlaudit detect task", K(ret));
  }
  obproxy_.destroy();
}

int ObProxyMain::get_opts_setting(struct option long_opts[], const int64_t long_opts_cnt,
                                  char short_opts[], const int64_t short_opts_cnt) const
{
  int ret = OB_SUCCESS;
  static struct {
    const char *long_name_;
    char short_name_;
    bool has_arg_;
  } ob_opts[] = {
    {"help", 'h', 0},
    {"listen_port", 'p', 1},
    {"prometheus_listen_port", 'l', 1},
    {"rpc_listen_port", 's', 1},
    {"upgrade_ver", 'u', 1},
    {"dump_config_sql", 'd', 0},
    {"execute_config_sql", 'e', 0},
    {"test_mysql_port", 'P', 1},
    {"test_mysql_ip", 'H', 1},
    {"optstr", 'o', 1},
    {"appname", 'n', 1},
    {"rs_list", 'r', 1},
    {"cluster_name", 'c', 1},
    {"nodaemon", 'N', 0},
    {"version", 'V', 0},
    {"releaseid", 'R', 0},
    {"regression_test", 't', 1},
    {"config_yaml", 'C', 0},
  };

  int64_t opts_cnt = sizeof (ob_opts) / sizeof (ob_opts[0]);

  if (OB_UNLIKELY(opts_cnt >= long_opts_cnt)
      || OB_UNLIKELY((opts_cnt * 2 + 1) >= short_opts_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("parse option fail: opts array is too small, ret=%d", ret);
  } else {
    int64_t short_idx = 0;
    for (int64_t i = 0; i < opts_cnt; ++i) {
      long_opts[i].name = ob_opts[i].long_name_;
      long_opts[i].has_arg = ob_opts[i].has_arg_;
      long_opts[i].flag = NULL;
      long_opts[i].val = ob_opts[i].short_name_;

      short_opts[short_idx++] = ob_opts[i].short_name_;
      if (ob_opts[i].has_arg_) {
        short_opts[short_idx++] = ':';
      }
    }
  }
  return ret;
}

void ObProxyMain::print_usage() const
{
  MPRINT("----------------------------------------------------------------------------------");
  MPRINT("obproxy [OPTIONS]");
  MPRINT("  -h,--help                              print this help");
  MPRINT("  -p,--listen_port         LPORT         obproxy listen port");
  MPRINT("  -l,--promethues_listen_port  PLPORT    obproxy prometheus listen port");
  MPRINT("  -s,--rpc_listen_port     PLPORT        obproxy rpc service listen port");
  MPRINT("  -o,--optstr              OPTSTR        extra options string");
  MPRINT("  -n,--appname             APPNAME       application name");
  MPRINT("  -r,--rs_list             RS_LIST       root server list(format ip:sql_port)");
  MPRINT("  -c,--cluster_name        CLUSTER_NAME  root server cluster name");
  MPRINT("  -d,--dump_config_sql     DSQL          dump config sql to file");
  MPRINT("  -e,--execute_config_sql  ESQL          exectue config sql(create tables, insert initial data)");
  MPRINT("  -N,--nodaemon                          don't run in daemon");
  MPRINT("  -V,--version             VERSION       current obproxy version");
  MPRINT("  -R,--releaseid           RELEASEID     current obproxy kernel release id");
  MPRINT("  -t,--regression_test     TEST_NAME     regression test");
  MPRINT("example:");
  MPRINT("  run without config server:");
  MPRINT("    ./bin/obproxy -p6789 -r'ip:port;ip:port' -n test -o enable_cluster_checkout=false,syslog_level=INFO");
  MPRINT(" OR ./bin/obproxy -p6789 -r'ip:port;ip:port' -c 'ob_test' -n test -o syslog_level=INFO \n");
  MPRINT("  run with config server:");
  MPRINT("    ./bin/obproxy -p6789 -e -n test -o obproxy_config_server_url='your config url',syslog_level=INFO\n");
  MPRINT("  Non-first start with local config file:");
  MPRINT("    ./bin/obproxy\n");
  MPRINT("  dump config update sql:");
  MPRINT("    ./bin/obproxy -d\n");
  MPRINT("  run regression tests:");
  MPRINT("    ./bin/obproxy -p6789 -rip:port -ntest -o obproxy_config_server_url='' -t ''");
  MPRINT("----------------------------------------------------------------------------------");
}

int ObProxyMain::to_int64(const char *sval, int64_t &ival) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sval)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid argument sval=%p ret=%d", sval, ret);
  } else {
    char *endp = NULL;
    ival = static_cast<int64_t>(strtol(sval, &endp, 10));
    if (OB_UNLIKELY('\0' != *endp)) {
      ret = OB_INVALID_ARGUMENT;
      MPRINT("invalid argument sval=%s ret=%d", sval, ret);
    }
  }
  return ret;
}

int ObProxyMain::str_to_port(const char *sval, int32_t &port) const
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_FAIL(to_int64(sval, value))) {
    MPRINT("need port number: %s", sval);
  } else if (OB_UNLIKELY(value <= 1024) || OB_UNLIKELY(value >= 65536)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("port number should greater than 1024 and less than 65536: %ld, ret=%d", value, ret);
  } else {
    port = static_cast<int32_t>(value);
  }
  return ret;
}

int ObProxyMain::str_to_version(const char *sval, int64_t &version) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(to_int64(sval, version))) {
    MPRINT("fail to convert str to int64: %s", sval);
  } else if (OB_UNLIKELY(version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("version=%ld should greater than 0, ret=%d", version, ret);
  } else { }
  return ret;
}

int ObProxyMain::parse_short_opt(const int32_t c, const char *value, ObProxyOptions &opts) const
{
  int ret = OB_SUCCESS;
  switch (c) {
    case 'p': {
      MPRINT("listen port: %s", value);
      if (OB_FAIL(str_to_port(value, opts.listen_port_))) {
        MPRINT("fail to parse short opt: listen port, ret=%d", ret);
      }
      break;
    } case 'l': {
      MPRINT("prometheus listen port: %s", value);
      if (OB_FAIL(str_to_port(value, opts.prometheus_listen_port_))) {
        MPRINT("fail to parse short opt: prometheus listen port, ret=%d", ret);
      }
      break;
    } case 's': {
      MPRINT("rpc service listen port: %s", value);
      if (OB_FAIL(str_to_port(value, opts.rpc_listen_port_))) {
        MPRINT("fail to parse short opt: rpc service listen port, ret=%d", ret);
      }
      break;
    } case 'u': {
      MPRINT("upgrade version for connection id: %s", value);
      if (OB_FAIL(str_to_version(value, opts.upgrade_version_))) {
        MPRINT("fail to parse version: upgrade version for connection id, ret=%d", ret);
      }
      break;
    } case 'd': {
      MPRINT("|--->start to dump config update sql to file...");
      MPRINT("|");
      MPRINT("|");
      if (OB_FAIL(ObProxyConfigUtils::dump_config_update_sql())) {
        MPRINT("|--->fail to dump config update sql, ret=%d", ret);
      } else {
        MPRINT("|--->dump config update sql succ!");
      }
      break;
    } case 'e': {
      MPRINT("e: execute config update sql");
      opts.execute_cfg_sql_ = true;
      break;
    } case 'o': {
      MPRINT("optstr: %s", value);
      opts.optstr_ = value;
      break;
    } case 'n': {
      MPRINT("appname: %s", value);
      opts.app_name_ = value;
      break;
    } case 'r': {
      MPRINT("rs list: %s", value);
      opts.rs_list_ = value;
      break;
    } case 'c': {
      MPRINT("cluster_name: %s", value);
      opts.rs_cluster_name_ = value;
      break;
    } case 'N': {
      MPRINT("N: nodaemon");
      opts.nodaemon_ = true;
      break;
    } case 'V': {
      print_version();
      ret = OB_NOT_RUNNING;
      break;
    } case 'R': {
      print_releaseid();
      ret = OB_NOT_RUNNING;
      break;
    } case 't': {
      MPRINT("regression test name: %s", value);
      opts.regression_test_ = value;
      break;
    } case 'h': {
      print_usage();
      ret = OB_NOT_RUNNING;
      break;
    } case 'C': {
      dump_config_to_yaml();
      dump_version_to_file();
      ret = OB_NOT_RUNNING;
      break;
    }
    default:{
      ret = OB_INIT_FAIL;
    }
  }
  return ret;
}

int ObProxyMain::parse_cmd_line(const int argc, char *const argv[], ObProxyOptions &opts) const
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_LONG_OPTS_CNT = 128;
  static const int64_t MAX_SHORT_OPTS_CNT = MAX_LONG_OPTS_CNT * 2 + 1;

  static struct option long_opts[MAX_LONG_OPTS_CNT];
  static char short_opts[MAX_SHORT_OPTS_CNT];

  if (OB_UNLIKELY(argc <= 0) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("fail to parse cmd line, argc=%d, argv=%p, ret=%d", argc, argv, ret);
  } else if (OB_FAIL(get_opts_setting(long_opts, MAX_LONG_OPTS_CNT,
                                      short_opts, MAX_SHORT_OPTS_CNT))) {
    MPRINT("fail to get opts setting, ret=%d", ret);
  } else {
    int32_t long_opts_idx = 0;
    int c = 0;

    while (OB_SUCC(ret)) {
      c = getopt_long(argc, argv, short_opts, long_opts, &long_opts_idx);

      if (-1 == c) {
        break;
      }

      if (0 == c) {
        if (OB_FAIL(parse_short_opt(long_opts[long_opts_idx].val, optarg, opts))) {
          MPRINT("fail to parse short opt, ret=%d", ret);
          break;
        }
      } else {
        if (OB_FAIL(parse_short_opt(c, optarg, opts))) {
          break;
        }
      }
    }
  }
  return ret;
}

ObProxyMain *ObProxyMain::get_instance()
{
  if (OB_ISNULL(instance_)) {
    instance_ = new(std::nothrow) ObProxyMain();
    if (OB_ISNULL(instance_)) {
      LOG_EDIAG("fail to alloc mem for ObProxyMain");
    }
  }
  return instance_;
}

void ObProxyMain::print_version() const
{
  MPRINT("obproxy (%s %s)", PACKAGE_STRING, RELEASEID);
  MPRINT("REVISION: %s", build_version());
  MPRINT("BUILD_TIME: %s %s", build_date(), build_time());
  MPRINT("BUILD_FLAGS: %s\n", build_flags());
  MPRINT("Copyright (c) 2021 OceanBase");
  MPRINT("SPDX-License-Identifier: Apache-2.0");
}

void ObProxyMain::print_releaseid() const
{
  MPRINT_STDOUT("%s.", RELEASEID);
}

int ObProxyMain::use_daemon()
{
  int ret = OB_SUCCESS;
  const int nochdir = 1;
  const int noclose = 0;
  if (daemon(nochdir, noclose) < 0) {
    ret = OB_ERR_SYS;
    LOG_EDIAG("fail to create daemon process", KERRMSGS, K(ret));
  }
  reset_tid_cache();
  return ret;
}

int ObProxyMain::get_log_file_name(const ObLogFDType type, char *file_name, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_name) || OB_UNLIKELY(type >= MAX_FD_FILE) || OB_UNLIKELY(len < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", K(file_name), K(type), K(len), K(ret));
  } else {
    int64_t ret_len = 0;
    char log_file_name[OB_MAX_LOG_FILE_NAME_LEN];

    if (FD_XFLUSH_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_xflush.log");
    } else if (FD_DIGEST_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_digest.log");
    } else if (FD_ERROR_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_error.log");
    } else if (FD_SLOW_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_slow.log");
    } else if (FD_DIAGNOSIS_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_diagnosis.log");
    } else if (FD_STAT_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_stat.log");
    } else if (FD_LIMIT_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_limit.log");
    } else if (get_global_proxy_config().enable_sharding && FD_CONFIG_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_config.log");
    } else if (FD_POOL_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_pool.log");
    } else if (FD_POOL_STAT_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_pool_stat.log");
    } else if (FD_TRACE_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_trace.log");
    } else if (g_run_mode == RUN_MODE_CLIENT && FD_DRIVER_CLIENT_FILE == type) {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy_client.log");
    } else {
      ret_len = snprintf(log_file_name, OB_MAX_LOG_FILE_NAME_LEN, "obproxy.log");
    }

    if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= OB_MAX_LOG_FILE_NAME_LEN)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_EDIAG("fail to snprintf log_file_name", K(type), K(ret_len), K(ret));
    } else {
      ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
      char *path = NULL;
      int64_t path_len = 0;

      if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_log_dir(), log_file_name, allocator, path))) {
        LOG_EDIAG("fail to merge file path", K(log_file_name), K(ret));
      } else {
        path_len = static_cast<int64_t>(STRLEN(path));
        if (OB_UNLIKELY(len <= path_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_EDIAG("buf is to small", K(len), "need_len", path_len+1, K(ret));
        } else {
          MEMCPY(file_name, path, path_len);
          file_name[path_len] = '\0';
        }
      }
    }
  }
  return ret;
}

void ObProxyMain::dump_config_to_yaml() const
{
  int ret = OB_SUCCESS;
  ObProxyConfig &config = get_global_proxy_config();
  const bool is_yaml_format = true;
  if (OB_FAIL(config.dump_config_to_local(is_yaml_format))) {
    MPRINT("fail to dump_config_to_ymal, ret=%d", ret);
  }

}

void ObProxyMain::dump_version_to_file() const
{
  int ret = OB_SUCCESS;
  constexpr int64_t MAX_VERSION_LEN = 512;
  char version_info[MAX_VERSION_LEN] = "";
  struct utsname uts;
  if (0 != ::uname(&uts)) {
    ret = OB_ERR_SYS;
    LOG_WDIAG("call uname failed", K(ret));
  } else {
    // obproxy-4.2.1.8-11503.el7.x86_64.rpm
    // PACKAGE_VERSION: 4.3.5.0
    // RELEASEID: 11503.el7
    // uts.machine: x86_64
    int len = 0;
    len = snprintf(version_info, MAX_VERSION_LEN,
                "{\"software package\":\"obproxy-%s-%s.%s.rpm\"}",
                PACKAGE_VERSION, RELEASEID, uts.machine);
    if (OB_UNLIKELY(len < 0 || len > MAX_VERSION_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to snprintf version_info", K(ret));
    } else {
      char cwd[ObLayout::MAX_PATH_LENGTH]{};
      if (OB_ISNULL(getcwd(cwd, sizeof(cwd)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to get current dir", K(ret));
      } else if (OB_FAIL(ObProxyFileUtils::write_to_file(cwd, CFG_DUMP_SOFTWARE_PACKAGE, version_info,
                                                         len, false))) {
        LOG_WDIAG("fail to write file to dir", K(cwd), K(CFG_DUMP_SOFTWARE_PACKAGE), K(len), K(ret));
      }
    }
  }

}

int ObProxyMain::start(const int argc, char *const argv[])
{
  int ret = OB_SUCCESS;

  ObProxyOptions opts;
  memset(&opts, 0, sizeof(opts));

  if (RUN_MODE_PROXY == g_run_mode && OB_FAIL(print_args(argc, argv))) {
    MPRINT("fail to print args, ret=%d", ret);
  } else if (OB_FAIL(parse_cmd_line(argc, argv, opts))) {
    if (OB_NOT_RUNNING != ret) {
      MPRINT("fail to parse cmd line, ret=%d", ret);
    }
  } else if (OB_FAIL(handle_inherited_sockets(argc, argv))) {
    MPRINT("fail to handle inherited socket, ret=%d", ret);
  }

  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  // if inherited, don not use daemon
  if (RUN_MODE_PROXY == g_run_mode && OB_SUCC(ret)) {
    if (!opts.nodaemon_ && !info.is_inherited_) {
      if (OB_FAIL(use_daemon())) {
        MPRINT("fail to use deamon, ret=%d", ret);
      }
    }
  }

  if (RUN_MODE_PROXY == g_run_mode && OB_SUCC(ret)) {
    if (info.is_inherited_) {
      if (OB_FAIL(close_all_fd(info.ipv4_fd_, info.ipv6_fd_, info.rpc_ipv4_fd_, info.rpc_ipv6_fd_))) {
        MPRINT("fail to close all fd, ret=%d", ret);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(obkv::get_global_rpc_throttle().init())) {
      MPRINT("fail to init rpc throttle, ret=%d", ret);
    } else if (OB_FAIL(get_global_layout().init(argv[0]))) {
      MPRINT("fail to init global layout, ret=%d", ret);
    } else if (OB_FAIL(init_log())) {
      MPRINT("fail to init log, ret=%d", ret);
    } else if (OB_FAIL(oceanbase::common::init_signal())) {
      LOG_EDIAG("fail to init signal", K(ret));
    } else if (OB_FAIL(ObRandomNumUtils::init_seed())) {
      LOG_EDIAG("fail to init random seed", K(ret));
    } else if (OB_FAIL(init_data_type())) {
      LOG_EDIAG("fail to init partition calculation related", K(ret));
    } else if (OB_FAIL(Trie::init_sys_var_trie())) {
      LOG_EDIAG("fail to init sys var trie", K(ret));
    } else {
      print_all_limits();
      init_proc_map_info();
      ObMemLeakChecker::init_all_mem_leak_checker();
      app_info_.setup(PACKAGE_STRING, APP_NAME, RELEASEID);
      proxy::get_global_redis_info_stat().set_rpc_port(opts.rpc_listen_port_);
      proxy::get_global_redis_info_stat().gen_run_id();
      _LOG_INFO("%s-%s", app_info_.full_version_info_str_, build_version());
      if (info.is_inherited_) {
        LOG_INFO("obproxy will start by hot upgrade", "listen ipv4 fd", info.ipv4_fd_,
                 "listen ipv6 fd", info.ipv6_fd_, "rpc listen ipv4 fd", info.rpc_ipv4_fd_,
                 "rpc listen ipv6 fd", info.rpc_ipv6_fd_, K(info));
      } else {
        LOG_INFO("has no inherited sockets, start new obproxy", K(info));
      }
      if (OB_FAIL(do_start_work(opts))) {
        LOG_EDIAG("fail to do start work", K(ret));
      }
    }
  }

  return ret;
}

int ObProxyMain::handle_inherited_sockets(const int argc, char *const argv[])
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  if (RUN_MODE_CLIENT == g_run_mode) {
    // do nothing
  } else if (OB_UNLIKELY(argc <= 0) || OB_ISNULL(argv) || OB_ISNULL(argv[0])) {
    ret = OB_INVALID_ARGUMENT;
    MPRINT("invalid argument, argc=%d, argv=%p, ret=%d", argc, argv, ret);
  } else {
    info.set_main_arg(argc, argv);
    char *inherited_ipv4 = NULL;
    char *inherited_ipv6 = NULL;

    inherited_ipv4 = getenv(OBPROXY_INHERITED_IPV4_FD);
    inherited_ipv6 = getenv(OBPROXY_INHERITED_IPV6_FD);
    if (NULL == inherited_ipv4 && NULL == inherited_ipv6) {
      // has no inherited sockets, will start new obproxy,
      // and set it HU_STATE_WAIT_HU_CMD state
      info.update_state(HU_STATE_WAIT_HU_CMD);
      info.is_parent_ = true;
    } else if (NULL == inherited_ipv4) {
      // Allow IPv6 to be empty
      // because it may be a hot upgrade from a lower version to a higher version
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("ipv4 should not be NULL", K(ret));
    } else if (info.is_inherited_) {
      ret = OB_ERR_UNEXPECTED;
      MPRINT("hot upgrade info is_inherited can't be true, ret=%d", ret);
    } else {
      // obproxy will start by hot upgrade
      // we will set it HU_STATE_WAIT_CR_CMD state and HU_STATUS_NEW_PROXY_CREATED_SUCC status
      info.is_inherited_ = true;
      info.ipv4_fd_ = atoi(inherited_ipv4);

      if (NULL != inherited_ipv6) {
        info.ipv6_fd_ = atoi(inherited_ipv6);
      }

      info.update_state(HU_STATE_WAIT_HU_CMD);
      info.is_parent_ = false;
      if (OB_FAIL(unsetenv(OBPROXY_INHERITED_IPV4_FD))) {
        MPRINT("fail to unsetenv OBPROXY_INHERITED_IPV4_FD, ret=%d", ret);
      } else if (OB_FAIL(unsetenv(OBPROXY_INHERITED_IPV6_FD))) {
        MPRINT("fail to unsetenv OBPROXY_INHERITED_IPV6_FD, ret=%d", ret);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(inherited_ipv4 = getenv(OBPROXY_INHERITED_RPC_IPV4_FD))) {
        info.rpc_ipv4_fd_ = atoi(inherited_ipv4);
        if (OB_FAIL(unsetenv(OBPROXY_INHERITED_RPC_IPV4_FD))) {
          MPRINT("fail to unsetenv OBPROXY_INHERITED_RPC_IPV4_FD, ret=%d", ret);
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(inherited_ipv6 = getenv(OBPROXY_INHERITED_RPC_IPV6_FD))) {
        info.rpc_ipv6_fd_ = atoi(inherited_ipv6);
        if (OB_FAIL(unsetenv(OBPROXY_INHERITED_RPC_IPV6_FD))) {
          MPRINT("fail to unsetenv OBPROXY_INHERITED_RPC_IPV6_FD, ret=%d", ret);
        }
      }
    }
  }

  return ret;
}

int ObProxyMain::init_log()
{
  int ret = OB_SUCCESS;
  static char log_file_name[OB_MAX_LOG_FILE_NAME_LEN];

  for (ObLogFDType type = FD_DEFAULT_FILE; OB_SUCC(ret) && type < MAX_FD_FILE; type = (ObLogFDType)(type + 1)) {
    if (OB_FAIL(get_log_file_name(type, log_file_name, OB_MAX_LOG_FILE_NAME_LEN))) {
      LOG_EDIAG("fail to get log file name", K(type), K(ret));
    } else {
      if (FD_DEFAULT_FILE == type) {
        if (RUN_MODE_PROXY == g_run_mode) {
          OB_LOGGER.set_file_name(type, log_file_name, true, false);
        } else if (RUN_MODE_CLIENT == g_run_mode) {
          OB_LOGGER.set_file_name(type, log_file_name, false, false);
        } else {
          MPRINT("invalid g_run_mode, mode=%d", g_run_mode);
        }
      } else {
        OB_LOGGER.set_file_name(type, log_file_name);
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObProxyConfig &config = get_global_proxy_config();
    OB_LOGGER.set_log_level("DEBUG");
    OB_LOGGER.set_monitor_log_level("DEBUG");
    OB_LOGGER.set_xflush_log_level("DEBUG");
    OB_LOGGER.set_max_file_size(config.max_log_file_size);
    // OB_LOGGER.set_logger_callback_handler(&oceanbase::obproxy::logger_callback);

    if (OB_FAIL(OB_LOGGER.init_async_log_thread(config.stack_size))) {
      // In special cases if set_xflush_log_name fails, we ignore the error.
      LOG_WDIAG("fail to init_async_log_thread, use sync logging instead", K(ret));
    } else {
      OB_LOGGER.set_enable_async_log(config.enable_async_log);
    }
    LOG_INFO("succ to init logger", "max_log_file_size", config.max_log_file_size.get(), "async_tid", OB_LOGGER.get_async_tid());
  }

  return ret;
}

int ObProxyMain::do_start_work(ObProxyOptions &opts)
{
  int ret = OB_SUCCESS;
  startup_time_us_ = hrtime_to_usec(get_hrtime_internal());
  proxy::get_global_redis_info_stat().set_up_time(startup_time_us_);
  if (OB_FAIL(obproxy_.init(opts, app_info_))) {
    LOG_EDIAG("obproxy init failed", K(ret));
  } else if (OB_FAIL(obproxy_.start(app_info_))) {
    LOG_EDIAG("obproxy start failed", K(ret));
  } else {
    LOG_INFO("obproxy init and start succ");
  }

  obproxy_.print_start_info(ret, app_info_);

  return ret;
}

int ObProxyMain::schedule_detect_task()
{
  int ret = OB_SUCCESS;
  int64_t interval_us = 0;
  if (OB_UNLIKELY(NULL != sig_detect_cont_) || OB_UNLIKELY(NULL != mem_monitor_cont_)
      || OB_UNLIKELY(NULL != sqlaudit_detect_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("detect cont should be null here", K_(sig_detect_cont), K_(mem_monitor_cont),
             K_(sqlaudit_detect_cont), K(ret));
  }
  const bool is_repeat = true;
  if (OB_SUCC(ret)) {
    interval_us = msec_to_usec(100); // 100ms
    if (OB_ISNULL(sig_detect_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                     "sig_detect_task", common::do_detect_sig, NULL, is_repeat))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start sig detect task", K(ret));
    } else {
      LOG_INFO("succ to schedule sig detect task", K(interval_us), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    interval_us = sec_to_usec(2); // 2s
    if (OB_ISNULL(mem_monitor_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                      "mem_monitor_task", ObProxyMain::do_monitor_mem,
                                      NULL, is_repeat))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start memory monitor task", K(ret));
    } else {
      LOG_INFO("succ to schedule mem monitor task", K(interval_us), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    interval_us = sec_to_usec(1); // 1s
    if (OB_ISNULL(sqlaudit_detect_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                          "sqlaudit_detect_task", ObProxyMain::do_detect_sqlaudit,
                                          NULL, is_repeat))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start sql_audit detect task", K(ret));
    } else {
      LOG_INFO("succ to schedule sqlaudit detect task", K(interval_us), K(ret));
    }
  }
  return ret;
}


void ObProxyMain::print_memory_usage()
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (OB_LIKELY(NULL != allocator)) {
    allocator->print_tenant_memory_usage(OB_SERVER_TENANT_ID);
  }

  // print glibc memory
  print_glibc_memory_usage();

  // print sqlaudit memory
  print_sqlaudit_memory_usage();

  // print object pool memory
  ObObjFreeListList::get_freelists().dump();
}

void ObProxyMain::print_glibc_memory_usage()
{
#ifdef EL9_PLATFORM
  struct mallinfo2 mi = mallinfo2();
#else
  struct mallinfo mi = mallinfo();
#endif
  int64_t hold = mi.arena + mi.hblkhd;
  int64_t used = hold - mi.fordblks;
  int64_t count = mi.hblks;
  print_memory_usage(hold, used, count, "GLIBC");
}

void ObProxyMain::print_sqlaudit_memory_usage()
{
  int64_t hold = 0;
  int64_t count = 0;
  ObSqlauditRecordQueue *sqlaudit_record_queue = NULL;
  if (OB_LIKELY(NULL != (sqlaudit_record_queue = get_global_sqlaudit_processor().acquire()))) {
    hold = sqlaudit_record_queue->get_current_memory_size();
    sqlaudit_record_queue->refcount_dec();
    sqlaudit_record_queue = NULL;
    count = 1;
  }
  print_memory_usage(hold, hold, count, "OB_SQL_AUDIT");

  if (0 != (hold = get_global_sqlaudit_processor().get_last_record_queue_memory_size())) {
    count = 1;
  } else {
    count = 0;
  }
  print_memory_usage(hold, hold, count, "OB_SQL_AUDIT_LAST");
}

void ObProxyMain::print_memory_usage(const int64_t hold, const int64_t used,
                                     const int64_t count, const char *name)
{
  int64_t pos = 0;
  char buf[PRINT_SQL_LEN]; // 1K
  buf[0] = '\0';
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid name", K(name), K(ret));
  } else if (OB_FAIL(databuff_printf(buf, PRINT_SQL_LEN, pos, "\n[MEMORY] hold=% '15ld used=% '15ld "
                     "count=% '8ld avg_used=% '15ld mod=%s", hold, used,
                     count, (0 == count) ? 0 : used / count, name))) {
    LOG_WDIAG("fail to print memory usage", K(name), K(ret));
  }
  LOG_INFO(buf);
}

int ObProxyMain::do_monitor_mem()
{
  int ret = OB_SUCCESS;
  ObProxyMain *proxy_main = ObProxyMain::get_instance();
  int64_t mem_hold = get_memory_hold();
  int64_t mem_used = get_memory_used();
  int64_t glibc_hold = get_glibc_memory_hold();
  int64_t rpc_req_mem_hold = get_rpc_mod_memory() + obkv::get_global_rpc_throttle().get_holding_resource();
  uint64_t cur_pos = proxy_main->pos_ % HISTORY_MEMORY_RECORD_COUNT;
  LOG_DEBUG("MemoryMonitor", "current memory hold size", mem_hold, "glibc hold", glibc_hold,
            "current rpc memory hold size", rpc_req_mem_hold, K(mem_used), K(cur_pos));
  mem_hold += glibc_hold;
  proxy::get_global_redis_info_stat().set_hold_memory(mem_hold);
  proxy::get_global_redis_info_stat().set_used_memory(mem_used);
  MEMORY_PROMETHEUS_STAT(PROMETHEUS_MEMORY_HOLD, mem_hold);
  MEMORY_PROMETHEUS_STAT(PROMETHEUS_MEMORY_USED, mem_used);
  MEMORY_PROMETHEUS_STAT(PROMETHEUS_PS_COUNT, get_global_ps_entry_cache().get_ps_entry_num()
                                              + get_global_text_ps_entry_cache().get_ps_entry_num());
  MEMORY_PROMETHEUS_STAT(PROMETHEUS_PS_MEMORY_USED, get_global_ps_entry_cache().get_ps_entry_mem_count()
                                                    + get_global_text_ps_entry_cache().get_ps_entry_mem_count());

  proxy_main->history_mem_size_[cur_pos] = mem_hold;
  proxy_main->history_rpc_mem_size_[cur_pos] = rpc_req_mem_hold;
  ++proxy_main->pos_;

  const int64_t mem_limited = get_global_proxy_config().proxy_mem_limited;
  const int64_t mem_warn_limited = mem_limited * 8 / 10;
  const int64_t mem_error_limited = mem_limited * 9 / 10;
  const int64_t rpc_req_mem_warn_limited = mem_limited * 7 / 10;
  const int64_t rpc_throttle_trigger_mem_limited = mem_limited * get_global_proxy_config().rpc_throttle_trigger_percentage / 100;
  bool is_out_of_mem_limit = true;
  bool is_out_of_warn_mem_limit = true;
  bool is_out_of_error_mem_limit = true;
  obkv::get_global_rpc_throttle().set_trigger_throttle(true);
  obkv::get_global_rpc_throttle().set_freeze_request(true);
  int64_t cur_mem_size = 0;
  int64_t cur_rpc_mem_size = 0;
  for (int64_t i = 0; i < HISTORY_MEMORY_RECORD_COUNT; ++i) {
    cur_mem_size = proxy_main->history_mem_size_[i] + OTHER_MEMORY_SIZE;
    cur_rpc_mem_size = proxy_main->history_rpc_mem_size_[i];
    if (cur_rpc_mem_size < rpc_throttle_trigger_mem_limited) {
      obkv::get_global_rpc_throttle().set_trigger_throttle(false);
    }
    if (cur_rpc_mem_size < rpc_req_mem_warn_limited) {
      obkv::get_global_rpc_throttle().set_freeze_request(false);
    }
    if (cur_mem_size < mem_warn_limited) {
      is_out_of_warn_mem_limit = false;
    }
    if (cur_mem_size < mem_error_limited) {
      is_out_of_error_mem_limit = false;
    }
    if (cur_mem_size < mem_limited) {
      is_out_of_mem_limit = false;
      break;
    }
  }

  if (is_out_of_mem_limit && RUN_MODE_PROXY == g_run_mode) {
    LOG_ERROR("obproxy's memory is out of limit, will disable alloc memory from the OS",
              K(mem_limited), "OTHER_MEMORY_SIZE", static_cast<int64_t>(OTHER_MEMORY_SIZE),
              K(is_out_of_mem_limit), K(cur_pos));
    for (int64_t i = 0; i < HISTORY_MEMORY_RECORD_COUNT; ++i) {
      _LOG_EDIAG("history memory size, history_mem_size[%ld]=%ld", i,  proxy_main->history_mem_size_[i]);
    }

    // print memory usage
    ObProxyMain::print_memory_usage();
    ObMemoryResourceTracker::dump();

    ObProxyMain::freeze_mem_alloc();
  } else if (is_out_of_error_mem_limit) {
    //will print every 2s
    LOG_ERROR("obproxy's memory is out of limit's 90% !!!",
              K(mem_limited), "OTHER_MEMORY_SIZE", static_cast<int64_t>(OTHER_MEMORY_SIZE),
              K(is_out_of_error_mem_limit), K(cur_pos));
    for (int64_t i = 0; i < HISTORY_MEMORY_RECORD_COUNT; ++i) {
      _LOG_WDIAG("history memory size, history_mem_size[%ld]=%ld", i,  proxy_main->history_mem_size_[i]);
    }

    // print memory usage
    ObProxyMain::print_memory_usage();
    ObMemoryResourceTracker::dump();
    ObProxyMain::unfreeze_mem_alloc();
    get_global_hot_upgrade_processor().do_hot_upgrade_internal();
  } else if (is_out_of_warn_mem_limit) {
    if (0 == cur_pos) {
      //only print every 20s
      LOG_WDIAG("obproxy's memory is out of limit's 80% !!!",
                K(mem_limited), "OTHER_MEMORY_SIZE", static_cast<int64_t>(OTHER_MEMORY_SIZE),
                K(is_out_of_error_mem_limit), K(cur_pos));
      for (int64_t i = 0; i < HISTORY_MEMORY_RECORD_COUNT; ++i) {
        _LOG_INFO("history memory size, history_mem_size[%ld]=%ld", i,  proxy_main->history_mem_size_[i]);
      }

      // print memory usage
      get_global_mem_leak_checker().print();
      get_global_objpool_leak_checker().print();
      ObProxyMain::print_memory_usage();
      ObMemoryResourceTracker::dump();
    }
    ObProxyMain::unfreeze_mem_alloc();
    get_global_hot_upgrade_processor().do_hot_upgrade_internal();
  } else if (obkv::get_global_rpc_throttle().is_freeze_request()) {
    if (0 == cur_pos) {
      LOG_WDIAG("rpc req reach max limit, stop rpc req read", K(rpc_req_mem_hold),K(rpc_req_mem_warn_limited));
    }
    ObProxyMain::unfreeze_mem_alloc();
  } else if (obkv::get_global_rpc_throttle().is_trigger_throttle()) {
    if (0 == cur_pos) {
      LOG_WDIAG("obkv begin rpc req throttle", K(rpc_req_mem_hold), K(rpc_throttle_trigger_mem_limited));
    }
    ObProxyMain::unfreeze_mem_alloc();
  } else {
    ObProxyMain::unfreeze_mem_alloc();
  }
  return ret;
}


int ObProxyMain::do_detect_sqlaudit()
{
  int ret = OB_SUCCESS;
  int64_t cur_sqlaudit_mem_limited = get_global_proxy_config().sqlaudit_mem_limited;
  ObSqlauditProcessor &g_sqlaudit_processor = get_global_sqlaudit_processor();
  ObSqlauditProcessorStatus status = g_sqlaudit_processor.get_status();

  if (0 == cur_sqlaudit_mem_limited) {
    if (AVAILABLE == status) {
      g_sqlaudit_processor.set_status(STOPPING);
    } else if (STOPPING == status) {
      g_sqlaudit_processor.destroy_queue();
    } else {
      // do nothing
    }
  } else if (cur_sqlaudit_mem_limited > 0) {
    if (UNAVAILABLE == status) {
      if (g_sqlaudit_processor.set_status(INITIALIZING)) {
        if (OB_FAIL(g_sqlaudit_processor.init_sqlaudit_record_queue(cur_sqlaudit_mem_limited))) {
          LOG_WDIAG("fail to init_sqlaudit_record_queue", K(ret));
        }
      }
    } else if (STOPPING == status) {
      g_sqlaudit_processor.destroy_queue();
    } else {
      // do nothing
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid sqlaudit_mem_limited", K(cur_sqlaudit_mem_limited), K(ret));
  }
  return ret;
}

// TODO add rpc
int ObProxyMain::close_all_fd(const int32_t listen_ipv4_fd, const int32_t listen_ipv6_fd, const int32_t rpc_listen_ipv4_fd, const int32_t rpc_listen_ipv6_fd)
{
  //this func can't print log, because log isn't initialized
  int ret = OB_SUCCESS;
  int fd = -1;
  pid_t pid = getpid();
  DIR *fd_dir = NULL;
  char fd_dir_path [OB_MAX_FILE_NAME_LENGTH];

  if (OB_UNLIKELY(listen_ipv4_fd < 3 && listen_ipv6_fd < 3 && rpc_listen_ipv4_fd < 3 && rpc_listen_ipv6_fd < 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int n = snprintf(fd_dir_path, OB_MAX_FILE_NAME_LENGTH, "/proc/%d/fd", pid);
    if (OB_UNLIKELY(n < 0) || OB_UNLIKELY(n >= OB_MAX_FILE_NAME_LENGTH)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      fd_dir = opendir(fd_dir_path);
      if (OB_ISNULL(fd_dir)) {
        ret = OB_FILE_NOT_OPENED;
      } else {
        struct dirent *de = NULL;
        while (OB_SUCC(ret) && (NULL != (de = readdir(fd_dir)))) {
          errno = 0;
          if ('.' == de->d_name[0]) {
            continue;
          } else {
            fd = static_cast<int32_t>(strtol(de->d_name, NULL, 10));
            if (fd < 0) {
              continue;
            } else if (fd < 3 || fd == dirfd(fd_dir) || fd == listen_ipv4_fd
                       || fd == listen_ipv6_fd || fd  == rpc_listen_ipv4_fd
                       || fd == rpc_listen_ipv6_fd) {
              continue;
            } else {
              if (OB_UNLIKELY(0 != close(fd))) {
                // do nothing
              }
            }
          }
        }
        if (OB_UNLIKELY(0 != closedir(fd_dir))) {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObProxyMain::init_data_type()
{
  int ret = OB_SUCCESS;
  // character collation conversion
  if (OB_FAIL(ObCharset::init_charset())) {
    LOG_EDIAG("fail to init charset", K(ret));
  // decimal int precision conversion
  } else if (OB_FAIL(common::wide::ObDecimalIntConstValue::init_const_values())) {
    LOG_EDIAG("fail to init decimal int const value", K(ret));
  } else {
    /* do nothing */
  }

  return ret;
}

void ObProxyMain::freeze_mem_alloc()
{
  CHUNK_MGR.disable_mem_alloc();
}

void ObProxyMain::unfreeze_mem_alloc()
{
  CHUNK_MGR.enable_mem_alloc();
}

void ObProxyMain::cancel_mem_limit()
{
  get_global_proxy_config().proxy_mem_limited = INTERNAL_MAX_MEM_LIMIT;
  unfreeze_mem_alloc();
}

} //end of namespace obproxy
} //end of namespace oceanbase

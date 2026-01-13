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

#include "lib/signal/ob_signal_handler.h"

#include "utils/ob_proxy_hot_upgrader.h"
#include "obproxy/obutils/ob_hot_upgrade_processor.h"
#include "obproxy/ob_proxy_main.h"
#include "lib/signal/ob_libunwind.h"
#include "lib/utility/ob_backtrace.h"
#include "lib/signal/ob_signal_utils.h"

using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace common
{

static __thread int64_t g_coredump_num = 0;
uint64_t g_rlimit_core = 0;
#define COMMON_FMT "timestamp=%ld, tid=%ld, tname=%s, trace_id=%s, \nlbt=%s"

static const int SIG_USER_MIN = SIGRTMIN; // 34 for  linux
static const int SIG_UNLIMIT_MEM = SIG_USER_MIN + 0;

static bool crash_error_signal_catched = false;

int init_signal()
{
  int ret = OB_SUCCESS;
  struct sigaction action;
  if (OB_FAIL(add_sig_ignore_catched(action, SIGPIPE))) {
    LOG_WDIAG("fail to add_sig_ignore_catched", K(ret));

  // Handle the SIGTERM and SIGINT signal:
  // We will stop accepted connect and exit immediately
  } else if (OB_FAIL(add_sig_direct_catched(action, SIGINT))) {
    LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
  } else if (OB_FAIL(add_sig_direct_catched(action, SIGTERM))) {
    LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
  } else if (OB_FAIL(add_sig_direct_catched(action, SIGUSR1))) {
    LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
  } else if (OB_FAIL(add_sig_direct_catched(action, SIGUSR2))) {
    LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
  } else if (OB_FAIL(add_sig_direct_catched(action, 43))) {
    LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
  // when a process terminates, the SIGHUP signal can be catch by its sub process
  } else if (OB_FAIL(prctl(PR_SET_PDEATHSIG, SIGHUP))) {
    LOG_WDIAG("fail to prctl PR_SET_PDEATHSIG for SIGHUP", K(ret));
  } else if (OB_FAIL(add_sig_async_catched(action, SIGHUP))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));

  // when a process terminates, the SIGCHLD signal will be sent to its parent process
  } else if (OB_FAIL(add_sig_async_catched(action, SIGCHLD))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));

  } else if (OB_FAIL(add_sig_direct_catched(action, SIG_UNLIMIT_MEM))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));
  } else if (OB_FAIL(add_sig_async_catched(action, 40))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));
  } else if (OB_FAIL(add_sig_async_catched(action, 41))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));
  } else if (OB_FAIL(add_sig_async_catched(action, 42))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));
  } else if (OB_FAIL(add_sig_async_catched(action, 49))) {
    LOG_WDIAG("fail to add_sig_async_catched", K(ret));
  } else {
    LOG_DEBUG("succ to init_signal");
  }
  return ret;
}

int catch_crash_error_signal()
{
  int ret = OB_SUCCESS;

#ifndef USING_ASAN
  if (crash_error_signal_catched) {
    LOG_INFO("no need to catch crash error signal, it has been catched");
  } else {
    struct sigaction action;
    int flag = SA_SIGINFO | SA_RESTART | SA_NODEFER;
    if (OB_FAIL(add_sig_direct_catched(action, SIGABRT, flag))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_direct_catched(action, SIGBUS, flag))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_direct_catched(action, SIGFPE, flag))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_direct_catched(action, SIGSEGV, flag))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else {
      crash_error_signal_catched = true;
      LOG_INFO("succ to catch crash error signal");
    }
  }
#endif

  return ret;
}

int ignore_crash_error_signal()
{
  int ret = OB_SUCCESS;

#ifndef USING_ASAN
  if (!crash_error_signal_catched) {
    LOG_INFO("no need to ignore crash error signal, it has been ignored");
  } else {
    struct sigaction action;
    if (OB_FAIL(add_sig_default_catched(action, SIGABRT))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_default_catched(action, SIGBUS))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_default_catched(action, SIGFPE))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else if (OB_FAIL(add_sig_default_catched(action, SIGSEGV))) {
      LOG_WDIAG("fail to add_sig_direct_catched", K(ret));
    } else {
      crash_error_signal_catched = false;
      LOG_INFO("succ to ignore crash error signal");
    }
  }
#endif

  return ret;
}

int do_detect_sig()
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  int sig = info.received_sig_;

  if (OB_INVALID_INDEX != sig) {
    info.received_sig_ = OB_INVALID_INDEX;
    switch (sig) {
      case SIGCHLD: {
        pid_t pid = OB_INVALID_INDEX;
        int stat = OB_INVALID_INDEX;
        // WNOHANG : if the sub process specified by pid is not over,
        //           the waitpid () function returns 0, do not be waiting.
        //           If completed, the sub process ID is returned.
        // -1      : wait for any sub process
        while((pid = waitpid(-1, &stat, WNOHANG)) > 0) {
          LOG_INFO("sub process exit", K(info), K(pid), "status", stat, KERRMSGS);
          if (OB_LIKELY(common::OB_SUCCESS == lib::mutex_acquire(&info.hot_upgrade_mutex_))) {
            if (info.sub_pid_ == pid) {
              info.reset_sub_pid();
              // after sub was exited, we need passing this status
              info.update_sub_status(HU_STATUS_EXITED);
              info.parent_hot_upgrade_flag_ = false;
              lib::mutex_release(&info.hot_upgrade_mutex_);
            } else {
              LOG_WDIAG("sub process exit, but recv it late");
            }
            lib::mutex_release(&info.hot_upgrade_mutex_);
          }
        }
        break;
      }
      case SIGHUP: {
        //only in wait cr cmd and wait cr finish state, we care about SIGHUP;
        if (!info.is_parent() && (!info.is_in_single_service())) {
          // after parent was exited, we need passing this status
          info.update_parent_status(HU_STATUS_EXITED);
          LOG_INFO("parent process exit", K(info));
        }
        if (OB_FAIL(OB_LOGGER.reopen_monitor_log())) {
          LOG_WDIAG("fail to reopen_monitor_log_name", K(ret));
        }
        break;
      }
      case 40: {
        OB_LOGGER.check_file();
        break;
      }
      case 41:
      case 42: {
        const bool level_flag = (41 == sig) ? false : true;
        get_global_proxy_config().update_log_level(level_flag);
        LOG_INFO("now:", K(OB_LOGGER.get_level_str()));
        break;
      }
      case 49: { // for print memory usage
        ObProxyMain::print_memory_usage();
        ObMemoryResourceTracker::dump();
        break;
      }
      default: {
        break;
      }
    }
  }
  return ret;
}

void sig_async_handler(const int sig)
{
  get_global_hot_upgrade_info().received_sig_ = sig;
}

#ifdef TEST_COVER
extern "C" {
  extern void __gcov_flush();
}
#endif

void sig_direct_handler(int sig, siginfo_t *si, void *contextg)
{
  switch (sig) {
    case SIGUSR1: {
      ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
      if (OB_LIKELY(common::OB_SUCCESS == lib::mutex_acquire(&info.hot_upgrade_mutex_))) {
        // If an exit signal has been sent to the child process,
        // the parent process ignores the signal sent by the child process
        if (OB_LIKELY(!info.parent_hot_upgrade_flag_)) {
#ifdef TEST_COVER
          LOG_INFO("gcov flush now");
          __gcov_flush();
#endif
          info.received_sig_ = sig;
          LOG_INFO("recv SIGUSR1 signal, will graceful exit", K(info));
          if (info.need_conn_accept_) {
            info.disable_net_accept();  // disable accecpt new connection
          }
          MEM_BARRIER();
          info.graceful_exit_start_time_ = get_hrtime_internal();
          info.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().hot_upgrade_exit_timeout)
                                         + info.graceful_exit_start_time_;
          info.parent_hot_upgrade_flag_ = true;
        }
        lib::mutex_release(&info.hot_upgrade_mutex_);
      }
      break;
    }
    case SIGUSR2: {
      ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
      if (OB_LIKELY(common::OB_SUCCESS == lib::mutex_acquire(&info.hot_upgrade_mutex_))) {
        // If the SIGUSR2 signal is received,
        // the child process is considered to exit normally,
        // and the child process is ignored
        info.reset_sub_pid();
        lib::mutex_release(&info.hot_upgrade_mutex_);
      }
      break;
    }
    case SIGTERM:
    case SIGINT: {
      ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
      #ifdef TEST_COVER
        LOG_INFO("gcov flush now");
        __gcov_flush();
      #endif
      LOG_INFO("recv signal, will graceful exit", K(sig), K(info));
      info.received_sig_ = sig;
      if (info.need_conn_accept_) {
        info.disable_net_accept();  // disable accecpt new connection
      }
      // 1. disable_net_accept will call pthread_kill() to accept thread
      // 2. when graceful_exit_start_time_ is changed, ObInactivityCop::check_inactivity
      //    call pthread_cancel() and pthread_join() to accept thread
      // - to avoid complex parallel multi-thread status and a lower-probability core problem (2024062100102854439),
      //   need use MEM_BARRIER to restrict pthread_kill() happens before pthread_cancel()
      MEM_BARRIER();
      info.graceful_exit_start_time_ = get_hrtime_internal();
      info.graceful_exit_end_time_ = HRTIME_USECONDS(get_global_proxy_config().delay_exit_time)
                                     + info.graceful_exit_start_time_;
      g_proxy_fatal_errcode = OB_GOT_SIGNAL_ABORTING;
      break;
    }
    case SIGABRT:
    case SIGBUS:
    case SIGFPE:
    case SIGSEGV: {
      coredump_cb(sig, si->si_code, si->si_addr, contextg);
      break;
      // the program won't execute from now on
    }
    default: {
      // Attention: real-time signal is platform-dependent,
      // so the signals cannot be judged in the 'switch-case' statements
      // here is handling real-time signal defined by user
      if (SIG_UNLIMIT_MEM == sig) {
        ObProxyMain::cancel_mem_limit();
        LOG_INFO("obproxy memory limit is canceled", K(sig));
      } else {
        LOG_INFO("receive unknown signal, ignore it", K(sig));
      }
      break;
    }
  }

  if (OB_GOT_SIGNAL_ABORTING == g_proxy_fatal_errcode) {
    if (SIGTERM == sig || SIGINT == sig) {
      LOG_ERROR("receive signal", K(sig));
    }
  }
}

void coredump_cb(volatile int sig, volatile int sig_code, void* volatile sig_addr, void *context)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  if (g_coredump_num++ < 1) {
    timespec time = {0, 0};
    clock_gettime(CLOCK_REALTIME, &time);
    int64_t ts = time.tv_sec * 1000000 + time.tv_nsec / 1000;
    // thread_name
    char tname[16];
    prctl(PR_GET_NAME, tname);
    auto *trace_id = ObCurTraceId::get_trace_id();
    char trace_id_buf[128] = {'\0'};
    if (trace_id != nullptr) {
      int64_t pos = trace_id->safe_to_string(trace_id_buf, sizeof(trace_id_buf));
      if (pos < sizeof(trace_id_buf)) {
        trace_id_buf[pos]= '\0';
      }
    }

    // backtrace
    char bt[512] = {'\0'};
    int64_t len = 0;
    const ucontext_t *con = (ucontext_t *)context;
#if defined(__x86_64__)
    int64_t ip = con->uc_mcontext.gregs[REG_RIP];
    int64_t bp = con->uc_mcontext.gregs[REG_RBP]; // stack base
    safe_backtrace(bt, sizeof(bt) - 1, &len);
#elif defined(__aarch64__)
    int64_t ip = con->uc_mcontext.regs[30];
    int64_t bp = con->uc_mcontext.regs[29];
    void* addrs[64];
    int n_addr = light_backtrace(addrs, ARRAYSIZEOF(addrs), bp);
    len += safe_parray(bt, sizeof(bt) - 1, (int64_t*)addrs, n_addr);
#else
    int64_t ip = -1;
    int64_t bp = -1;
#endif
    bt[len++] = '\0';

    char print_buf[1024];
    char rlimit_core[32] = "unlimited";
    if (UINT64_MAX != g_rlimit_core) {
      snprintf(rlimit_core, sizeof(rlimit_core), "%lu", g_rlimit_core);
    }
    char crash_info[128] = "CRASH ERROR!!!";
    ssize_t print_len = snprintf(print_buf, sizeof(print_buf),
                                 "%s IP=%lx, RBP=%lx, sig=%d, sig_code=%d, sig_addr=%p, RLIMIT_CORE=%s, " COMMON_FMT,
                                  crash_info, ip, bp, sig, sig_code, sig_addr, rlimit_core,
                                  ts, GETTID(), tname, trace_id_buf, bt);
    if (print_len <= 0
        || print_len > sizeof(print_buf)) {
      print_len = sizeof(print_buf);
    }

    char end[] = "\n";
    struct iovec iov[2];
    memset(iov, 0, sizeof(iov));
    iov[0].iov_base = print_buf;
    iov[0].iov_len = print_len;
    iov[1].iov_base = end;
    iov[1].iov_len = strlen(end);
    writev(STDERR_FILENO, iov, sizeof(iov) / sizeof(iov[0]));
  }
  // Reset back to the default handler
  signal(sig, SIG_DFL);
  raise(sig);
}


int add_sig_ignore_catched(struct sigaction &action, const int sig)
{
  int ret = OB_SUCCESS;
  sigemptyset(&action.sa_mask);
  action.sa_handler = SIG_IGN;
  action.sa_flags = 0;
  if (OB_UNLIKELY(0 != sigaction(sig, &action, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to sigaction sig", K(sig), KERRMSGS, K(ret));
  }
  return ret;
}

int add_sig_default_catched(struct sigaction &action, const int sig)
{
  int ret = OB_SUCCESS;
  sigemptyset(&action.sa_mask);
  action.sa_handler = SIG_DFL;
  action.sa_flags = 0;
  if (OB_UNLIKELY(0 != sigaction(sig, &action, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to sigaction sig", K(sig), KERRMSGS, K(ret));
  }
  return ret;
}

int add_sig_direct_catched(struct sigaction &action, const int sig, const int flag/*0*/)
{
  int ret = OB_SUCCESS;
  sigemptyset(&action.sa_mask);
  action.sa_sigaction = sig_direct_handler;
  action.sa_flags = flag;
  if (OB_UNLIKELY(0 != sigaction(sig, &action, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to sigaction sig", K(sig), KERRMSGS, K(ret));
  }
  return ret;
}

int add_sig_async_catched(struct sigaction &action, const int sig, const int flag/*0*/)
{
  int ret = OB_SUCCESS;
  sigemptyset(&action.sa_mask);
  action.sa_handler = sig_async_handler;
  action.sa_flags = flag;
  if (OB_UNLIKELY(0 != sigaction(sig, &action, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to sigaction sig", K(sig), KERRMSGS, K(ret));
  }
  return ret;
}

void print_limit(const char *name, const int resource)
{
  struct rlimit limit;
  if (0 == getrlimit(resource, &limit)) {
    if (RLIM_INFINITY == limit.rlim_cur) {
      MPRINT("[%s] %-24s = %s", __func__, name, "unlimited");
    } else {
      MPRINT("[%s] %-24s = %ld", __func__, name, limit.rlim_cur);
    }
  }
  if (RLIMIT_CORE == resource) {
    g_rlimit_core = limit.rlim_cur;
  }
}

void print_all_limits()
{
  OB_LOG(INFO, "============= *begin obproxy limit report * =============");
  print_limit("RLIMIT_CORE",RLIMIT_CORE);
  print_limit("RLIMIT_CPU",RLIMIT_CPU);
  print_limit("RLIMIT_DATA",RLIMIT_DATA);
  print_limit("RLIMIT_FSIZE",RLIMIT_FSIZE);
  print_limit("RLIMIT_LOCKS",RLIMIT_LOCKS);
  print_limit("RLIMIT_MEMLOCK",RLIMIT_MEMLOCK);
  print_limit("RLIMIT_NOFILE",RLIMIT_NOFILE);
  print_limit("RLIMIT_NPROC",RLIMIT_NPROC);
  print_limit("RLIMIT_STACK",RLIMIT_STACK);
  OB_LOG(INFO, "============= *stop obproxy limit report* ===============");
}

} // end of namespace common
} // end of namespace oceanbase

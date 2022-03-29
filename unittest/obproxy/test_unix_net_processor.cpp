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

#include <gtest/gtest.h>
#include <pthread.h>
#define private public
#define protected public
#include "test_eventsystem_api.h"
#include "obproxy/stat/ob_mysql_stats.h"
#include "ob_event_io.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;
using namespace net;

#define TEST_PORT_START              9000

int g_server_fd = -1;
int32_t g_index = 0;
bool g_test_throttle_msg = false;
bool g_hot_upgrade = false;
bool g_test_hot_upgrade_blocking = false;
char g_buff[TEST_G_BUFF_SIZE] = {};

struct TestAcceptSM : public event::ObContinuation
{
  ObVIO *read_vio_;
  ObIOBufferReader *reader_;
  ObNetVConnection *vc_;
  ObMIOBuffer *buf_;
  volatile bool read_finish_;
  volatile int32_t result_;

  TestAcceptSM(ObProxyMutex *mutex)
    : ObContinuation(mutex)
  {
    SET_HANDLER(&TestAcceptSM::main_handler);
    read_vio_ = NULL;
    reader_ = NULL;
    vc_ = NULL;
    buf_ = NULL;
    read_finish_= false;
    result_ = RESULT_DEFAULT;
  }

  virtual ~TestAcceptSM()
  {
    free();
  }

  int main_handler(int event, void *data)
  {
    int ret = EVENT_DONE;
    ObUnixNetVConnection *vc = static_cast<ObUnixNetVConnection *>(data);
    INFO_NET("TEST", "main_handler VC=%p, this = %p", vc, this);
    switch (event) {
    case NET_EVENT_ACCEPT_SUCCEED: {
      INFO_NET("TEST", "TestAcceptSM : Listen succeed");
      result_ = RESULT_NET_EVENT_LISTEN_SUCCEED;
      break;
    }
    case NET_EVENT_ACCEPT_FAILED: {
      ERROR_NET("TestAcceptSM : Listen failed");
      result_ = RESULT_NET_EVENT_LISTEN_FAILED;
      break;
    }
    case NET_EVENT_ACCEPT: {
      INFO_NET("TEST", "TestAcceptSM : Accepted a connection");
      if (g_test_hot_upgrade_blocking) {
        vc->closed_ = 1;
        INFO_NET("TEST", "TestAcceptSM : close father's blocking accept");
      } else {
        vc_ = vc;
        SET_HANDLER(&TestAcceptSM::handle_do_io_read);
        ObEThread *ethread = this_ethread();
        ethread->schedule_imm_local(this);

        result_ = RESULT_NET_EVENT_ACCEPT_SUCCEED;
        INFO_NET("TEST", "TestNetSMCont : create test sm");
      }
      break;
    }
    case EVENT_ERROR: {
      INFO_NET("TEST", "TestAcceptSM : error, action_ maybe cancelled");
      result_ = RESULT_NET_EVENT_ERROR;
      break;
    }
    default:
      ERROR_NET("event = %d\n", event);
      ob_release_assert(!"unknown event");
    }
    return ret;
  }

  int handle_do_io_read(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "TestAcceptSM : handle_do_io_read");
    SET_HANDLER(&TestAcceptSM::handle_read);
    buf_ = new_miobuffer(32 * 1024);
    reader_ = buf_->alloc_reader();
    read_vio_ = vc_->do_io_read(this, INT64_MAX, buf_);
    result_ = RESULT_NET_EVENT_IO_READ_SUCCEED;
    return 1;
  }

  int handle_read(int event, void *data)
  {
    UNUSED(data);
    int64_t size;
    result_ = event;
    switch (event) {
    case VC_EVENT_READ_READY: {
      size = reader_->read_avail();
      ob_assert(size <= TEST_G_BUFF_SIZE);
      memset(g_buff, 0, TEST_G_BUFF_SIZE);
      reader_->copy(g_buff, size);
      reader_->consume(size);
      printf("g_buff[]:%s.\n", g_buff);
      fflush(stdout);
      read_finish_ = true;
      break;
    }
    case VC_EVENT_READ_COMPLETE://FALLSTHROUGH
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ERROR: {
      INFO_NET("TEST", "do_io_close, event = %d\n", event);
//      SET_HANDLER(&TestAcceptSM::main_handler);//set back to main_handler
      vc_->do_io_close();
      break;
    }
    case NET_EVENT_ACCEPT: {
      INFO_NET("TEST", "TestAcceptSM : Accepted a connection");
      ObUnixNetVConnection *vc = static_cast<ObUnixNetVConnection *>(data);
      INFO_NET("TEST", "main_handler VC=%p, this = %p", vc, this);
      if (g_test_hot_upgrade_blocking) {
        vc->closed_ = 1;
        INFO_NET("TEST", "TestAcceptSM : close father's blocking accept");
      } else {
        vc_ = vc;
        SET_HANDLER(&TestAcceptSM::handle_do_io_read);
        ObEThread *ethread = this_ethread();
        ethread->schedule_imm_local(this);

        result_ = RESULT_NET_EVENT_ACCEPT_SUCCEED;
        INFO_NET("TEST", "TestNetSMCont : create test sm");
      }
      break;
    }
    default:
      ERROR_NET("event = %d\n", event);
      ob_release_assert(!"unknown event");
    }
    INFO_NET("TEST", "handle_read result_=%d", result_);
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestAcceptSM free");
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      read_vio_ = NULL;
      reader_ = NULL;
    } else {}
    mutex_.release();
    vc_ = NULL;
  }
};

struct TestProcAcceptCont : public event::ObContinuation
{
  ObNetProcessor::ObAcceptOptions *options_;
  ObNetAcceptAction *net_accept_action_;
  TestAcceptSM *accept_sm_;
  volatile bool started_;
  bool null_mutex_;

  TestProcAcceptCont(ObProxyMutex *mutex, ObNetProcessor::ObAcceptOptions *options)
    : ObContinuation(mutex), options_(options)
  {
    SET_HANDLER(&TestProcAcceptCont::handle_start_accept);
    net_accept_action_ = NULL;
    accept_sm_ = NULL;
    started_ = false;
    null_mutex_ = false;
  }

  virtual ~TestProcAcceptCont()
  {
    free();
  }

  int handle_start_accept(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "TestProcAcceptCont : handle_start_accept");
    accept_sm_ = new(std::nothrow) TestAcceptSM(null_mutex_ ? (ObProxyMutex *)NULL : mutex_);
    if (!g_hot_upgrade) {
      net_accept_action_ = static_cast<ObNetAcceptAction *>(g_net_processor.accept(
          *accept_sm_, *options_));
      if (NULL != net_accept_action_) {
        g_server_fd = net_accept_action_->server_->fd_;
      }
    } else {
      net_accept_action_ = static_cast<ObNetAcceptAction *>(g_net_processor.main_accept(
          *accept_sm_, g_server_fd, *options_));
    }
    started_ = true;
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestProcAcceptCont free");
    if (!g_test_hot_upgrade_blocking) {
      if (NULL != net_accept_action_) {
        net_accept_action_->cancelled_ = true;
      }
      delete accept_sm_;
    } else { }
    net_accept_action_ = NULL;
    accept_sm_ = NULL;
    options_ = NULL;
    mutex_.release();
  }
};

struct TestConnectSM : public event::ObContinuation
{
  int64_t timeout_;
  bool check_connect_;
  ObUnixNetVConnection *vc_;
  ObIOBufferReader *reader_;
  ObMIOBuffer *buf_;
  volatile int32_t result_;
  volatile bool write_finish_;

  TestConnectSM(ObProxyMutex *mutex, int64_t timeout, bool check_connect)
    : ObContinuation(mutex), timeout_(timeout), check_connect_(check_connect)
  {
    if (!check_connect) {
      SET_HANDLER(&TestConnectSM::main_handler_connect);
    } else {
      SET_HANDLER(&TestConnectSM::handle_check_connect);
    }
    vc_ = NULL;
    reader_ = NULL;
    buf_ = NULL;
    result_ = RESULT_DEFAULT;
    write_finish_ = false;
  }

  virtual ~TestConnectSM() { free(); }

  int main_handler_connect(int event, void *data)
  {
    int ret = EVENT_DONE;
    ObUnixNetVConnection *vc = static_cast<ObUnixNetVConnection *>(data);
    INFO_NET("TEST", "NetVC=%p, this = %p", vc, this);
    switch (event) {
    case NET_EVENT_OPEN_FAILED: {
      WARN_NET("handle_connect connect net open failed");
      result_ = RESULT_NET_CONNECT_OPEN_FAILED;
      break;
    }
    case NET_EVENT_OPEN: {
      INFO_NET("TEST", "handle_connect connect net open succeed");
      vc_ = vc;
      SET_HANDLER(&TestConnectSM::handle_do_io_write);
      ObEThread *ethread = this_ethread();
      ethread->schedule_imm_local(this);

      result_ = RESULT_NET_CONNECT_OPEN;
      ret = EVENT_CONT;
      break;
    }
    default: {
      result_ = RESULT_NET_ERROR;
      ob_assert(!"unknown connect event");
    }
    }
    INFO_NET("TEST", "handle_connect result_=%d", result_);
    return ret;
  }

  int handle_check_connect(int event, void *data)
  {
    int ret = EVENT_DONE;
    ObUnixNetVConnection *vc = static_cast<ObUnixNetVConnection *>(data);
    INFO_NET("TEST", "NetVC=%p, this = %p", vc, this);
    switch (event) {
    case NET_EVENT_OPEN_FAILED: {
      WARN_NET("handle_check_connect connect net open failed");
      result_ = RESULT_NET_CONNECT_OPEN_FAILED;
      break;
    }
    case NET_EVENT_OPEN: {
      INFO_NET("TEST", "handle_check_connect connect net open succeed");
      vc_ = vc;
      SET_HANDLER(&TestConnectSM::handle_do_io_write);
      ObEThread *ethread = this_ethread();
      ethread->schedule_imm_local(this);

      result_ = RESULT_NET_CONNECT_OPEN;
      ret = EVENT_CONT;
      break;
    }
    case VC_EVENT_INACTIVITY_TIMEOUT: {
      WARN_NET("handle_connect VC_EVENT_INACTIVITY_TIMEOUT");
      break;
    }
    default: {
      result_ = RESULT_NET_ERROR;
      ob_assert(!"unknown connect event");
    }
    }
    return ret;
  }

  int handle_do_io_write(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    SET_HANDLER(&TestConnectSM::handle_write);
    buf_ = new_miobuffer(TEST_G_BUFF_SIZE);
    reader_ = buf_->alloc_reader();
    vc_->do_io_write(this, TEST_G_BUFF_SIZE, reader_);
    vc_->set_inactivity_timeout(timeout_);
    result_ = RESULT_NET_EVENT_IO_WRITE_SUCCEED;
    write_finish_ = false;
    return 1;
  }

  int handle_write(int event, void *data)
  {
    UNUSED(data);
    int64_t size = 0;
    int64_t written_len = 0;
    result_ = event;
    switch (event) {
    case VC_EVENT_WRITE_READY: {
      INFO_NET("TEST", "handle_write VC_EVENT_WRITE_READY");
      size = buf_->write_avail();
      ob_assert(size <= TEST_G_BUFF_SIZE);
      for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
        size = (int64_t)strlen(g_input[i]);
        if (buf_->write_avail() >= size) {
          printf("write[%ld]:%s.\n", i, g_input[i]);
          fflush(stdout);
          buf_->write(g_input[i], size, written_len);
        } else {
          result_ = RESULT_NET_WRITE_UNFINISH;
          ERROR_NET("i=%ld : write_avail()=%ld < %ld\n", i, buf_->write_avail(), size);
        }
      }
      if (result_ != RESULT_NET_WRITE_UNFINISH) {
        write_finish_ = true;
      } else {}
      break;
    }
    case VC_EVENT_WRITE_COMPLETE: //FALLSTHROUGH
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      if (!check_connect_) {
        SET_HANDLER(&TestConnectSM::main_handler_connect);
      } else {
        SET_HANDLER(&TestConnectSM::handle_check_connect);
      }
      break;
    }
    default:
      result_ = RESULT_NET_ERROR;
      ob_release_assert(!"unknown event");
    }
    INFO_NET("TEST", "handle_write result_ %d", result_);
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestConnectSM free");
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      reader_ = NULL;
    } else { }
    mutex_.release();
    vc_ = NULL;
  }

};

struct TestProcConnectCont : public event::ObContinuation
{
  ObAction *action_;
  ObNetVCOptions *options_;
  sockaddr addr_;
  bool check_connect_;
  int64_t timeout_;
  volatile int32_t result_;
  TestConnectSM *connect_sm_;

  TestProcConnectCont(ObProxyMutex *mutex, ObNetVCOptions *options,
      sockaddr &addr, int64_t connect_timeout)
    : ObContinuation(mutex), options_(options),
      addr_(addr), timeout_(connect_timeout)
  {
    SET_HANDLER(&TestProcConnectCont::handle_start_connect);
    action_ = NULL;
    result_ = RESULT_DEFAULT;
    connect_sm_ = NULL;
    check_connect_ = connect_timeout > 0 ? true : false;
  }

  virtual ~TestProcConnectCont()
  {
    free();
  }

  int handle_start_connect(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "TestProcConnectCont : handle_start_connect");
    connect_sm_ = new(std::nothrow) TestConnectSM(mutex_, timeout_, check_connect_);

    if (NULL != connect_sm_) {
      if (check_connect_) {
        if (NULL != options_) {
          options_->ethread_ = this_ethread();
        }
        int ret = g_net_processor.connect(*connect_sm_, addr_, action_, timeout_, options_);
        if (OB_SUCC(ret) && NULL != action_) {
          result_ = RESULT_NET_CALL_CONNECT_SUCCEED;
          INFO_NET("TEST", "TestProcConnectCont : call connect_s succeed");
        } else {
          result_ = RESULT_NET_CALL_CONNECT_FAILED;
          ERROR_NET("TestProcConnectCont : call connect_s failed");
        }
      } else {
        g_unix_net_processor.connect_re(*connect_sm_, addr_, options_);
        result_ = RESULT_NET_CALL_CONNECT_SUCCEED;
      }
    } else {
      result_ = RESULT_NET_CALL_CONNECT_FAILED;
      ERROR_NET("TestProcConnectCont : failed to new TestConnectSM");
    }
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestProcConnectCont free");
    if (NULL != action_) {
      action_->cancel();
      delete connect_sm_;
    }
    action_ = NULL;
    options_ = NULL;
    connect_sm_ = NULL;
    mutex_.release();
  }
};


class TestUnixNetProcessor : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void check_start();
  void check_accept_call(const ObNetProcessor::ObAcceptOptions &options);
  bool accept_shell_popen(FILE *&test_fp, int32_t local_port);
  void check_accept_result(volatile bool &read_finish);
  void check_accept(ObNetProcessor::ObAcceptOptions &options, ObHRTime timeout);
  void common_accept(ObNetProcessor::ObAcceptOptions &options, ObHRTime timeout = 0,
      bool null_mutex = false);
  void common_hot_upgrade(const char test_case[], int32_t local_port = -1,
      int64_t accept_threads = 0);
  bool connect_shell_popen(uint16_t local_port);
  void check_connect_call(const ObNetVCOptions *options);
  bool init_server(int32_t port);
  void check_connect_result();
  void check_connect(int32_t check_connect_type);
  void common_connect(ObNetVCOptions *options, ObIpEndpoint &ip_addr,
      int64_t connect_timeout = 0, int32_t check_connect_type = 0);

public:
  TestProcAcceptCont *proc_accept_cont_;
  TestProcConnectCont *proc_connect_cont_;
  FILE *test_fp_;
  bool test_passed_;
};

bool check_input(int &argc, char **&argv);
void pclose_fp(FILE *&fp);

void TestUnixNetProcessor::SetUp()
{
  test_fp_ = NULL;
  proc_accept_cont_ = NULL;
  proc_connect_cont_ = NULL;
  test_passed_ = false;
}

void TestUnixNetProcessor::TearDown()
{
  pclose_fp(test_fp_);

  if (NULL != proc_accept_cont_) {
    delete proc_accept_cont_;
    proc_accept_cont_ = NULL;
  } else {}

  if (NULL != proc_connect_cont_) {
    delete proc_connect_cont_;
    proc_connect_cont_ = NULL;
  } else {}
}

void TestUnixNetProcessor::check_accept_call(const ObNetProcessor::ObAcceptOptions &options)
{
  static int64_t last_accpet_threads =  0 ;
  if (options.frequent_accept_ && options.accept_threads_ > 0) {
    ASSERT_EQ(last_accpet_threads + options.accept_threads_, g_event_processor.dedicate_thread_count_);
    last_accpet_threads =  g_event_processor.dedicate_thread_count_;
  } else { }
}

bool TestUnixNetProcessor::accept_shell_popen(FILE *&test_fp, int32_t local_port)
{
  bool ret = true;
  char fp_str[TEST_MAX_STRING_LENGTH];
  sprintf(fp_str, "telnet 127.0.0.1 %d", local_port);
  if (NULL == (test_fp = popen(fp_str, "w"))) {
    ERROR_NET("failed to popen test_fp");
    ret = false;
  } else {
    INFO_NET("TEST", "accept_shell_popen success");
  }
  return ret;
}

void TestUnixNetProcessor::check_accept_result(volatile bool &read_finish)
{
  INFO_NET("TEST", "check_accept_result");

  g_index = 0;
  memset(g_output, 0 , TEST_G_BUFF_SIZE);
  for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
    fputs(g_input[i], test_fp_);
    fflush(test_fp_);
    ObHRTime last_time = get_hrtime();
    while (!read_finish && (get_hrtime() - last_time) <= HRTIME_SECONDS(5)) { }
    EXPECT_LE(strlen(g_buff), TEST_MAX_STRING_LENGTH);
    memcpy(g_output[i], g_buff, TEST_MAX_STRING_LENGTH);
    read_finish = false;
  }

  fputs("^]q", test_fp_);
  fflush(test_fp_);

  INFO_NET("TEST", "send data success");
  for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
    printf("input [%ld]:%s.\n", i, g_input[i]);
    printf("output[%ld]:%s.\n", i, g_output[i]);
    fflush(stdout);
    EXPECT_EQ(0, memcmp(g_input[i], g_output[i], TEST_MAX_STRING_LENGTH));
  }
}

void TestUnixNetProcessor::check_accept(ObNetProcessor::ObAcceptOptions &options, ObHRTime timeout)
{
  while(!proc_accept_cont_->started_) { }//wait for finish calling accept
  volatile int32_t &sm_result = proc_accept_cont_->accept_sm_->result_;
  if (RESULT_NET_EVENT_LISTEN_SUCCEED == sm_result) {//all the f_callback_on_open_ should set true
    check_accept_call(options);
    ObHRTime last_time = get_hrtime();
    ObHRTime cur_time = get_hrtime();
    if (accept_shell_popen(test_fp_, options.local_port_)) {
      while (RESULT_NET_EVENT_ACCEPT_SUCCEED != sm_result
          && RESULT_NET_EVENT_IO_READ_SUCCEED != sm_result
          && (cur_time - last_time) <= HRTIME_SECONDS(5)) {
        cur_time = get_hrtime();
      }
      if (RESULT_NET_EVENT_ACCEPT_SUCCEED == sm_result || RESULT_NET_EVENT_IO_READ_SUCCEED == sm_result) {
        ObHRTime diff_time = cur_time - last_time - timeout
            - HRTIME_SECONDS(options.defer_accept_timeout_);
        EXPECT_GE(diff_time, 0);
        if (0 == options.defer_accept_timeout_) {
          EXPECT_LE(diff_time, HRTIME_MSECONDS(50));
        } else {
          EXPECT_LE(diff_time, HRTIME_MSECONDS(1500));
        }
        check_accept_result(proc_accept_cont_->accept_sm_->read_finish_);
        test_passed_ = true;
      } else {
        if (!g_test_throttle_msg) {
          ERROR_NET("telnet failed, sm_result=%d", sm_result);
        } else {
          test_passed_ = true;
          INFO_NET("TEST","check throttle msg succeed");
        }
      }
    } else {
      ERROR_NET("accept_shell_popen failed");
      EXPECT_TRUE(false);
    }
  } else {
    ERROR_NET("net_accept_ has been cancelled %d", sm_result);
  }
}

void TestUnixNetProcessor::common_accept(ObNetProcessor::ObAcceptOptions &options,
    ObHRTime timeout, bool null_mutex)
{
  proc_accept_cont_ = new(std::nothrow) TestProcAcceptCont(new_proxy_mutex(), &options);
  proc_accept_cont_->null_mutex_ = null_mutex;
  g_event_processor.schedule_imm(proc_accept_cont_);
  check_accept(options, timeout);
}

bool TestUnixNetProcessor::init_server(int32_t port)
{
  bool ret = false;
  ObNetProcessor::ObAcceptOptions opt;
  opt.local_port_ = port;
  opt.frequent_accept_ = false;
  opt.f_callback_on_open_ = true;
  proc_accept_cont_ = new(std::nothrow) TestProcAcceptCont(new_proxy_mutex(), &opt);
  g_event_processor.schedule_imm(proc_accept_cont_);

  ObHRTime last_time = get_hrtime();
  while (!proc_accept_cont_->started_ && (get_hrtime() - last_time) <= HRTIME_SECONDS(5)) { }

  volatile int32_t &accept_result = proc_accept_cont_->accept_sm_->result_;
  if (RESULT_NET_EVENT_LISTEN_SUCCEED == accept_result) {
    INFO_NET("TEST", "server listen succeed %d", accept_result);
    ret = true;
  } else {
    ERROR_NET("server listen failed %d", accept_result);
  }
  return ret;
}

void TestUnixNetProcessor::check_connect_result()
{
  INFO_NET("TEST", "check_connect_result %p", test_fp_);

  printf("g_buff[]:%s.\n", g_buff);
  memset(g_output, 0, TEST_G_BUFF_SIZE);
  int64_t last_size = 0;
  for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
    int64_t size = strlen(g_input[i]);
    memcpy(g_output[i], g_buff + last_size, size);
    last_size = size;

    printf("input [%ld]:%s.\n", i, g_input[i]);
    printf("output[%ld]:%s.\n", i, g_output[i]);
    fflush(stdout);
    EXPECT_EQ(0, memcmp(g_input[i], g_output[i], TEST_MAX_STRING_LENGTH));
  }
}

void TestUnixNetProcessor::check_connect(int32_t check_connect_type)
{
  while(RESULT_DEFAULT == proc_connect_cont_->result_) { }
  if (RESULT_NET_CALL_CONNECT_SUCCEED == proc_connect_cont_->result_) {
    volatile int32_t &connect_result = proc_connect_cont_->connect_sm_->result_;

    ObHRTime last_time = get_hrtime();
    while (!proc_connect_cont_->connect_sm_->write_finish_
        && (get_hrtime() - last_time) <= HRTIME_SECONDS(5)) { }
    while (!proc_accept_cont_->accept_sm_->read_finish_
        && (get_hrtime() - last_time) <= HRTIME_SECONDS(5)) { }

    if (proc_connect_cont_->connect_sm_->write_finish_
        && proc_accept_cont_->accept_sm_->read_finish_) {
      INFO_NET("TEST", "net_conncet_ has been finish write %d", connect_result);
      check_connect_result();
      test_passed_ = true;
    } else if (!proc_connect_cont_->connect_sm_->write_finish_) {
      if (NET_EVENT_OPEN_FAILED == check_connect_type
          && RESULT_NET_CONNECT_OPEN_FAILED == proc_connect_cont_->connect_sm_->result_) {
        test_passed_ = true;
        INFO_NET("INFO", "test NET_EVENT_OPEN_FAILED succeed");
      } else {
        ERROR_NET("client call connect failed %d", proc_connect_cont_->connect_sm_->result_);
      }
    } else {
      ERROR_NET("server failed to read %d", proc_accept_cont_->accept_sm_->result_);
    }
  }
}

void TestUnixNetProcessor::common_connect(ObNetVCOptions *options,
    ObIpEndpoint &ip_addr, int64_t connect_timeout, int32_t check_connect_type)
{
  int32_t port = (ntohs)(ip_addr.sin_.sin_port);
  if (NET_EVENT_OPEN_FAILED == check_connect_type) {
    ++port;
  } else if (VC_EVENT_INACTIVITY_TIMEOUT == check_connect_type) {
    connect_timeout = HRTIME_NSECONDS(1);
  } else { }
  if (init_server(port)) {
    proc_connect_cont_ = new(std::nothrow) TestProcConnectCont(new_proxy_mutex(),
        options, ip_addr.sa_, connect_timeout);
    g_event_processor.schedule_imm(proc_connect_cont_);

    check_connect(check_connect_type);
  } else {
    ERROR_NET("client connect is not started");
  }
}

void TestUnixNetProcessor::common_hot_upgrade(const char test_case[],
    int32_t local_port, int64_t accept_threads)
{
  char cmd[TEST_MAX_STRING_LENGTH * 2] = {};
  sprintf(cmd, "./test_unix_net_processor %d --gtest_filter=TestUnixNetProcessor.%s",
               g_server_fd, test_case);
  INFO_NET("TEST", "cmd : %s.\n", cmd);

  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  info.disable_net_accept();

  if (-1 != local_port) {
    for (int i = 0; i < accept_threads; ++i) {
      if (accept_shell_popen(test_fp_, local_port)) {
        fputs("^]q", test_fp_);
        fflush(test_fp_);
        pclose_fp(test_fp_);
      } else {
        ERROR_NET("accept_shell_popen failed");
        EXPECT_TRUE(false);
      }
    }
  } else {}

  int status = system(cmd);
  if (-1 == status) {
    ERROR_NET("fork failed");
  } else if (WIFEXITED(status)){
    if (0 == WEXITSTATUS(status)) {
      test_passed_ = true;
      INFO_NET("TEST", "run shell cmd successfully");
    } else {
      ERROR_NET("run shell cmd failed, exit code %d", WEXITSTATUS(status));
    }
  } else {
    ERROR_NET("call shell cmd failed, exit code %x", status);
  }
  EXPECT_TRUE(test_passed_);
  info.need_conn_accept_ = true;
}

void pclose_fp(FILE *&fp)
{
  if (NULL != fp) {
    if( -1 == pclose(fp)) {
      ERROR_NET("pclose fp failed");
    } else {
      fp = NULL;
    }
  } else { }
}

bool check_input(int &argc, char **&argv)
{
  bool ret = true;
  if (argc > 1) {
    INFO_NET("TEST", "test child server");
    g_server_fd = atoi(argv[1]);
    if (g_server_fd < 0 || g_server_fd > 1024) {
      printf("g_server_fd is error :%d, %s.\n", g_server_fd, argv[1]);
      exit(-1);
    } else {
      argc--;
      argv = argv + 1;
      g_event_start_threads = 1;
      g_hot_upgrade = true;
    }
  } else {
    ret = false;
    g_hot_upgrade = false;
  }
  return ret;
}

TEST_F(TestUnixNetProcessor, test_start)
{
  INFO_NET("TEST", "test_start");
  //g_net_processor has been started in init_g_net_processor()
  for (int64_t i = 0; i < g_event_start_threads; ++i) {
    ObEThread *thread = g_event_processor.event_thread_[ET_NET][i];
    ObNetHandler &handler = thread->get_net_handler();
    ASSERT_TRUE(NULL != handler.trigger_event_);
    ASSERT_TRUE(thread == handler.trigger_event_->ethread_);
    ASSERT_TRUE(EVENTIO_ASYNC_SIGNAL == thread->ep_->type_);
    ObNetPoll &net_poll = thread->get_net_poll();
    ASSERT_TRUE(net_poll.poll_descriptor_ == thread->ep_->event_loop_);
  }
}

TEST_F(TestUnixNetProcessor, test_accept_n_threads)
{
  INFO_NET("TEST", "test_accept_n_threads");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 1;
  options.accept_threads_ = 3;
  options.f_callback_on_open_ = true;
  options.send_bufsize_ = 6*1024;
  options.recv_bufsize_ = 6*1024;
  options.sockopt_flags_ = 3;

  common_accept(options);
  g_test_hot_upgrade_blocking = true;//set for test_hot_upgrade_n_threads in the below
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetProcessor, test_main_accept_n_threads)
{
  INFO_NET("TEST", "test_main_accept");
  common_hot_upgrade("test_hot_upgrade_n_threads", TEST_PORT_START + 1, 3);
}

TEST_F(TestUnixNetProcessor, test_hot_upgrade_n_threads)
{
  if (g_hot_upgrade) {
    INFO_NET("TEST", "test_hot_upgrade_n_threads");
    //g_net_processor has been started in init_g_net_processor()
    ObNetProcessor::ObAcceptOptions options;
    options.local_port_ = TEST_PORT_START + 1;
    options.accept_threads_ = 3;
    options.f_callback_on_open_ = true;

    ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    info.is_inherited_ = true;

    common_accept(options);
    EXPECT_TRUE(test_passed_);
  } else {
    g_test_hot_upgrade_blocking = false;
    INFO_NET("TEST", "test_main_accept_child, father need do nothing");
  }
}

TEST_F(TestUnixNetProcessor, test_accept_listen_twice)
{
  INFO_NET("TEST", "test_accept_listen_twice");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 1;
  options.accept_threads_ = 3;
  options.f_callback_on_open_ = true;

  common_accept(options);
  ASSERT_EQ(RESULT_NET_EVENT_LISTEN_FAILED, proc_accept_cont_->accept_sm_->result_);
}

TEST_F(TestUnixNetProcessor, test_accept_zero_threads)
{
  INFO_NET("TEST", "test_accept_zero_threads");

  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 2;
  options.localhost_only_ = true;
  options.f_callback_on_open_ = true;
  options.send_bufsize_ = 6*1024;
  options.recv_bufsize_ = 6*1024;
  options.sockopt_flags_ = 3;

  common_accept(options);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetProcessor, test_main_accept_zero_threads)
{
  INFO_NET("TEST", "test_main_accept");
  common_hot_upgrade("test_hot_upgrade_zero_threads");
}

TEST_F(TestUnixNetProcessor, test_hot_upgrade_zero_threads)
{
  if (g_hot_upgrade) {
    INFO_NET("TEST", "test_hot_upgrade_zero_threads");
    ObNetProcessor::ObAcceptOptions options;
    options.local_port_ = TEST_PORT_START + 2;
    options.localhost_only_ = true;
    options.f_callback_on_open_ = true;

    ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    info.is_inherited_ = true;

    common_accept(options);
    EXPECT_TRUE(test_passed_);
  } else {
    INFO_NET("TEST", "test_main_accept_child, father need do nothing");
  }
}

TEST_F(TestUnixNetProcessor, test_accept_not_frequent_accept)
{
  INFO_NET("TEST", "test_accept_not_frequent_accept");

  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 3;
  options.local_ip_.family_ = AF_INET;
  options.local_ip_.addr_.ip4_ = INADDR_ANY;
  options.frequent_accept_ = false;
  options.defer_accept_timeout_ = 3;
  options.f_callback_on_open_ = true;
  options.send_bufsize_ = 6*1024;
  options.recv_bufsize_ = 6*1024;
  options.sockopt_flags_ = 3;

  common_accept(options, 0, true);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetProcessor, test_main_accept_not_frequent_accept)
{
  INFO_NET("TEST", "test_main_accept");
  common_hot_upgrade("test_hot_upgrade_not_frequent_accept");
}

TEST_F(TestUnixNetProcessor, test_hot_upgrade_not_frequent_accept)
{
  if (g_hot_upgrade) {
    INFO_NET("TEST", "test_hot_upgrade_not_frequent_accept");
    ObNetProcessor::ObAcceptOptions options;
    options.local_port_ = TEST_PORT_START + 3;
    options.frequent_accept_ = false;
    options.defer_accept_timeout_ = 3;
    options.f_callback_on_open_ = true;

    ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    info.is_inherited_ = true;

    common_accept(options, 0, true);
    EXPECT_TRUE(test_passed_);
  } else {
    INFO_NET("TEST", "test_main_accept_child, father need do nothing");
  }
}

TEST_F(TestUnixNetProcessor, test_connect)
{
  INFO_NET("TEST", "test_connect");

  ObNetVCOptions options;
  options.f_blocking_connect_ = false;
  options.set_sock_param(0, 0, 0, 0 , 0);
  options.ip_family_ = AF_INET;

  ObIpEndpoint ip_addr;
  ip_addr.set_to_loopback(AF_INET);
  ip_addr.sin_.sin_port = (htons)(TEST_PORT_START + 4);

  common_connect(&options, ip_addr);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetProcessor, test_connect_re)
{
  INFO_NET("TEST", "test_connect_re");

  ObIpEndpoint ip_addr;
  ip_addr.set_to_loopback(AF_INET);
  ip_addr.sin_.sin_port = (htons)(TEST_PORT_START + 5);
  int64_t connect_timeout = HRTIME_SECONDS(5);

  common_connect(NULL, ip_addr, connect_timeout);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetProcessor, test_connect_re_open_failed)
{
  INFO_NET("TEST", "test_connect_re_open_failed");

  ObIpEndpoint ip_addr;
  ip_addr.set_to_loopback(AF_INET);
  ip_addr.sin_.sin_port = (htons)(TEST_PORT_START + 6);
  int64_t connect_timeout = HRTIME_SECONDS(5);

  int32_t check_connect_type = NET_EVENT_OPEN_FAILED;
  common_connect(NULL, ip_addr, connect_timeout, check_connect_type);
  EXPECT_TRUE(test_passed_);
}

} // end of namespace obproxy
} // end of namespace oceanbase


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::obproxy::check_input(argc, argv);
  oceanbase::obproxy::init_g_net_processor();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


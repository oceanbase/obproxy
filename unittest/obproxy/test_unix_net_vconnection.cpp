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

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;
using namespace net;

#define TEST_PORT_START              9200
#define TEST_MAX_SESSION_NUMBER      10
#define TEST_CHECK_ACTIVE_TIME       HRTIME_SECONDS(3)
#define TEST_CHECK_INACTIVE_TIME     HRTIME_SECONDS(2)
#define TEST_EACH_ACTIVE_TIME(j)     (TEST_CHECK_ACTIVE_TIME + HRTIME_SECONDS(j % TEST_MAX_SESSION_NUMBER))

char g_session_buff[TEST_MAX_SESSION_NUMBER][TEST_G_BUFF_SIZE] = {};

enum {
  CHECK_NULL = 0,
  CHECK_TIMEOUT,
  CHECK_NET_COMPLETE,
  CHECK_NET_ECONNRESET_CONNECT,
  CHECK_NET_ECONNRESET_CLIENT,
  CHECK_NET_WRITE_TODO_NULL,
  CHECK_NET_READ_TODO_NULL,
  CHECK_NET_GET_DATA,
  CHECK_NET_WRITE_SIGNAL_AND_UPDATE,
  CHECK_NET_READ_SIGNAL_AND_UPDATE,
};

struct TestFunParam {
  int32_t check_type_;
  int32_t vc_number_;
  ObNetVCOptions *vc_options_;
  int64_t inactivity_timeout_;

  TestFunParam(int32_t check_type, int32_t vc_number, ObNetVCOptions *vc_options,
      int64_t timeout = 10)
    : check_type_(check_type), vc_number_(vc_number), vc_options_(vc_options),
      inactivity_timeout_(timeout)
  {
    vc_options_->set_sock_param(0, 0, 0, 0, 0);
  }

  ~TestFunParam() { }
};

//connect
struct TestSessionConnect : public event::ObContinuation
{
  const int32_t id_;
  const TestFunParam &param_;
  ObUnixNetVConnection *vc_;
  ObIOBufferReader *reader_;
  ObMIOBuffer *buf_;
  volatile int32_t result_;
  volatile bool finish_;
  int32_t index_;
  ObHRTime start_time_;
  ObHRTime hold_time_;
  bool check_ok_;
  bool vc_closed_;

  TestSessionConnect(ObProxyMutex *mutex, const int32_t id, const TestFunParam &param)
    : ObContinuation(mutex), id_(id), param_(param)
  {
    SET_HANDLER(&TestSessionConnect::handle_connect);
    vc_ = NULL;
    buf_ = new_miobuffer(TEST_G_BUFF_SIZE);
    reader_ = buf_->alloc_reader();
    result_ = RESULT_DEFAULT;
    finish_ = false;
    index_ = 0;
    start_time_ = 0;
    hold_time_ = 0;
    check_ok_ = true;
    vc_closed_ = false;
  }

  virtual ~TestSessionConnect() { destroy(); }

  int handle_connect(int event, void *data)
  {
    int ret = EVENT_DONE;
    result_ = event;
    switch (event) {
      case NET_EVENT_OPEN_FAILED: {
        ERROR_NET("connect %d connect net open failed", id_);
        break;
      }
      case NET_EVENT_OPEN: {
        INFO_NET("TEST", "connect %d connect net open succeed", id_);
        vc_ = static_cast<ObUnixNetVConnection *>(data);
        SET_HANDLER(&TestSessionConnect::handle_do_io);
        ret = EVENT_CONT;
        break;
      }
      default: {
        ob_assert(!"unknown connect event");
      }
    }
    DEBUG_NET("TEST", "connect %d handle_connect result_=%d", id_, result_);
    return ret;
  }

  int handle_do_io(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "connect %d handle_do_io, type %d", id_, param_.check_type_);
    switch (param_.check_type_) {
      case CHECK_TIMEOUT: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_EQ(vc_->get_inactivity_timeout(), TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_LE(vc_->next_inactivity_timeout_at_ - get_hrtime(),
            TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_GE(vc_->next_inactivity_timeout_at_ - get_hrtime(),
            TEST_CHECK_INACTIVE_TIME - HRTIME_MSECONDS(50));

        vc_->cancel_inactivity_timeout();
        TEST_EXPECT_EQ(vc_->get_inactivity_timeout(), 0);
        TEST_EXPECT_EQ(vc_->next_inactivity_timeout_at_, 0);

        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_EQ(vc_->get_active_timeout(), TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_TRUE(NULL == vc_->active_timeout_action_);

        SET_HANDLER(&TestSessionConnect::handle_timeout);
        vc_->do_io_write(this, strlen(g_input[0]) + 1, reader_);
        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_TRUE(NULL != vc_->active_timeout_action_);
        TEST_EXPECT_TRUE(!vc_->active_timeout_action_->cancelled_);

        vc_->cancel_active_timeout();
        TEST_EXPECT_EQ(vc_->get_active_timeout(), 0);
        TEST_EXPECT_TRUE(NULL == vc_->active_timeout_action_);

        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        start_time_ = get_hrtime();
        break;
      }
      case CHECK_NET_COMPLETE: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestSessionConnect::handle_write);
        vc_->do_io_write(this, strlen(g_input[0]) + 1, reader_);
        break;
      }
      case CHECK_NET_ECONNRESET_CONNECT: {
        //Note:1. before write, if the opposite one closed, when we write, will return EPIPE
        //2. in writing, if the opposite one closed, when we write, will return already send bytes,
        //when write again, return ECONNRESET. Case 2 create difficultly, so we failed to test it
        break;
      }
      case CHECK_NET_READ_SIGNAL_AND_UPDATE:
      case CHECK_NET_ECONNRESET_CLIENT: {
        vc_->do_io_close();
        finish_ = true;
        vc_closed_ = true;
        break;
      }
      case CHECK_NET_WRITE_TODO_NULL: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestSessionConnect::handle_write_todo_null);
        vc_->do_io_write(this, 10, reader_);
        vc_->write_.vio_.ndone_ = 10;
        start_time_ = get_hrtime();
        break;
      }
      case CHECK_NET_READ_TODO_NULL: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestSessionConnect::handle_write);
        vc_->do_io_write(this, strlen(g_input[0]) + 1, reader_);
        start_time_ = get_hrtime();
        break;
      }
      case CHECK_NET_GET_DATA: {
        vc_->do_io_write(this, 10, NULL);
        ObVIO vio;
        TEST_EXPECT_TRUE(vc_->get_data(OB_API_DATA_WRITE_VIO, &vio));
        TEST_EXPECT_TRUE(NULL == vio.buffer_.mbuf_);
        TEST_EXPECT_TRUE(NULL == vio.buffer_.entry_);
        TEST_EXPECT_TRUE(!vc_->write_.enabled_);

        vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);
        TEST_EXPECT_EQ(vio.nbytes_, 0);
        TEST_EXPECT_TRUE(ObVIO::NONE == vio.op_);
        TEST_EXPECT_TRUE(NET_VC_SHUTDOWN_WRITE == vc_->f_.shutdown_);

        vc_->do_io_shutdown(IO_SHUTDOWN_READWRITE);
        TEST_EXPECT_TRUE((NET_VC_SHUTDOWN_READ | NET_VC_SHUTDOWN_WRITE) == vc_->f_.shutdown_);

        vc_->closed_ = 1;
        vc_closed_= true;
        finish_ = true;
        break;
      }
      default: {
        ob_assert(!"unknown check_type_");
      }
    }
    result_ = RESULT_NET_EVENT_IO_SUCCEED;
    return 1;
  }

  int handle_write_todo_null(int event, void *data)
  {
    UNUSED(data);
    result_ = event;
    DEBUG_NET("TEST", "connect %d handle_write_todo_null result_ %d", id_, result_);
    switch (event) {
      case VC_EVENT_INACTIVITY_TIMEOUT: {
        hold_time_ = get_hrtime() - start_time_;
        TEST_EXPECT_LE(hold_time_, HRTIME_SECONDS(param_.inactivity_timeout_) + HRTIME_SECONDS(2));
        TEST_EXPECT_GE(hold_time_, HRTIME_SECONDS(param_.inactivity_timeout_));
        vc_->do_io_close();
        vc_closed_ = true;
        finish_ = true;
        break;
      }
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_WRITE_READY:
      case VC_EVENT_WRITE_COMPLETE:
      case VC_EVENT_EOS:
      case VC_EVENT_ACTIVE_TIMEOUT: {
        break;
      }
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  int handle_write(int event, void *data)
  {
    UNUSED(data);
    int64_t written_len = 0;
    result_ = event;
    DEBUG_NET("TEST", "connect %d handle_write result_ %d", id_, result_);
    switch (event) {
      case VC_EVENT_WRITE_READY: {
        buf_->write(g_input[0], (int64_t)strlen(g_input[0]) + 1, written_len);
        printf("write[%d]:%s.\n", id_, g_input[0]);
        fflush(stdout);
        break;
      }
      case VC_EVENT_WRITE_COMPLETE: {
        TEST_EXPECT_EQ(vc_->write_.vio_.ntodo(), 0);
        TEST_EXPECT_TRUE(!vc_->write_.enabled_);
        TEST_EXPECT_EQ(vc_->recursion_, 1);
        vc_->closed_ = 1;
        finish_ = true;
        vc_closed_ = true;
        break;
      }
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_EOS:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  int handle_timeout(int event, void *data)
  {
    UNUSED(data);
    int64_t written_len = 0;
    result_ = event;
    DEBUG_NET("TEST", "connect %d handle_timeout result_ %d", id_, result_);
    switch (event) {
    case VC_EVENT_WRITE_READY: {
      buf_->write(g_input[0], (int64_t)strlen(g_input[0]), written_len);
      printf("write[%d]:%s.\n", id_, g_input[0]);
      fflush(stdout);
      break;
    }
    case VC_EVENT_ACTIVE_TIMEOUT: {
      finish_ = true;
      vc_->do_io_close();
      hold_time_ = get_hrtime() - start_time_;
      TEST_EXPECT_GE(hold_time_, TEST_EACH_ACTIVE_TIME(id_) - HRTIME_MSECONDS(100));
      TEST_EXPECT_LE(hold_time_, TEST_EACH_ACTIVE_TIME(id_) + HRTIME_MSECONDS(100));
      INFO_NET("TEST", "connect %d VC_EVENT_ACTIVE_TIMEOUT succeed, %ld:%ld",
          id_, hold_time_, hrtime_to_nsec(TEST_EACH_ACTIVE_TIME(id_)));
      vc_closed_ = true;
      break;
    }
    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_WRITE_COMPLETE:
    case VC_EVENT_INACTIVITY_TIMEOUT: {
    case VC_EVENT_EOS:
      break;
    }
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      vc_closed_ = true;
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  void destroy()
  {
    INFO_NET("TEST", "TestSessionConnect destroy %d", id_);
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      reader_ = NULL;
    } else { }
    mutex_.release();
    if (!vc_closed_) {
      vc_->closed_ = -1;
    }
    vc_ = NULL;
  }
};

struct TestProcessorConnect : public event::ObContinuation
{
  sockaddr addr_;
  const TestFunParam param_;
  volatile bool start_;
  volatile int32_t count_;
  ObAction *action_[TEST_MAX_SESSION_NUMBER];
  TestSessionConnect *session_connect_[TEST_MAX_SESSION_NUMBER];

  TestProcessorConnect(ObProxyMutex *mutex, sockaddr addr, const TestFunParam param)
    : ObContinuation(mutex), addr_(addr), param_(param)
  {
    SET_HANDLER(&TestProcessorConnect::handle_start_connect);
    start_ = false;
    count_ = 0;
    memset(action_, 0, sizeof(action_));
    memset(session_connect_, 0, sizeof(session_connect_));
  }

  virtual ~TestProcessorConnect() { destroy(); }

  int handle_start_connect(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "handle_start_connect");
    for (int32_t i = 0; i < param_.vc_number_; ++i) {
      if (NULL == (session_connect_[i] = new(std::nothrow) TestSessionConnect(mutex_, i, param_))) {
        ERROR_NET("failed to new TestSessionConnect %d", i);
        start_ = false;
        break;
      } else {
        g_net_processor.connect_re(*(session_connect_[i]), addr_, param_.vc_options_);
        start_ = true;
        INFO_NET("TEST", "call connect_re succeed %d", i);
      }
    }
    return EVENT_CONT;
  }

  void destroy()
  {
    INFO_NET("TEST", "TestProcessorConnect destroy");
    for (int32_t i = 0; i < param_.vc_number_ && NULL != session_connect_[i]; i++) {
      delete session_connect_[i];
    }
    sleep(1);// wait for close vc by inactivitycop
    mutex_.release();
  }
};

//accpet
struct TestClientSession : public event::ObContinuation
{
  ObUnixNetVConnection *vc_;
  const int32_t id_;
  const TestFunParam &param_;

  ObIOBufferReader *reader_;
  ObMIOBuffer *buf_;
  volatile int32_t result_;
  volatile bool finish_;
  int32_t index_;
  ObHRTime start_time_;
  ObHRTime hold_time_;
  bool check_ok_;
  bool vc_closed_;

  TestClientSession(ObProxyMutex *mutex, ObUnixNetVConnection *netvc, const int32_t id,
        const TestFunParam &param)
      : ObContinuation(mutex), vc_(netvc), id_(id),
        param_(param)
  {
    SET_HANDLER(&TestClientSession::handle_do_io);
    result_ = RESULT_DEFAULT;
    finish_ = false;
    buf_ = new_miobuffer(TEST_G_BUFF_SIZE);
    reader_ = buf_->alloc_reader();
    index_ = 0;
    start_time_ = 0;
    hold_time_ = 0;
    check_ok_ = true;
    vc_closed_ = false;
  }

  virtual ~TestClientSession() { destroy(); }

  int handle_do_io(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "Client %d handle_do_io, type %d", id_, param_.check_type_);
    switch (param_.check_type_){
      case CHECK_TIMEOUT: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_EQ(vc_->get_inactivity_timeout(), TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_LE(vc_->next_inactivity_timeout_at_ - get_hrtime(),
            TEST_CHECK_INACTIVE_TIME);
        TEST_EXPECT_GE(vc_->next_inactivity_timeout_at_ - get_hrtime(),
            TEST_CHECK_INACTIVE_TIME - HRTIME_MSECONDS(50));

        vc_->cancel_inactivity_timeout();
        TEST_EXPECT_EQ(vc_->get_inactivity_timeout(), 0);
        TEST_EXPECT_EQ(vc_->next_inactivity_timeout_at_, 0);

        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_EQ(vc_->get_active_timeout(), TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_TRUE(NULL == vc_->active_timeout_action_);

        SET_HANDLER(&TestClientSession::handle_timeout);
        vc_->do_io_read(this, INT64_MAX, buf_);
        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        TEST_EXPECT_TRUE(NULL != vc_->active_timeout_action_);
        TEST_EXPECT_TRUE(!vc_->active_timeout_action_->cancelled_);

        vc_->cancel_active_timeout();
        TEST_EXPECT_EQ(vc_->get_active_timeout(), 0);
        TEST_EXPECT_TRUE(NULL == vc_->active_timeout_action_);

        vc_->set_active_timeout(TEST_EACH_ACTIVE_TIME(id_));
        start_time_ = get_hrtime();
        break;
      }
      case CHECK_NET_COMPLETE: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestClientSession::handle_read);
        vc_->do_io_read(this, strlen(g_input[index_]) + 1, buf_);
        break;
      }
      case CHECK_NET_ECONNRESET_CLIENT: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestClientSession::handle_read_econnreset);
        vc_->do_io_read(this, strlen(g_input[index_]) + 1, buf_);
        break;
      }
      case CHECK_NET_WRITE_TODO_NULL:
      case CHECK_NET_READ_TODO_NULL: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestClientSession::handle_read_todo_null);
        vc_->do_io_read(this, strlen(g_input[index_]) + 1, buf_);
        vc_->read_.vio_.ndone_ = strlen(g_input[index_]) + 1;
        start_time_ = get_hrtime();
        break;
      }
      case CHECK_NET_GET_DATA: {
        vc_->do_io_read(this, 10, NULL);
        ObVIO vio;
        TEST_EXPECT_TRUE(vc_->get_data(OB_API_DATA_READ_VIO, &vio));
        TEST_EXPECT_TRUE(NULL == vio.buffer_.mbuf_);
        TEST_EXPECT_TRUE(NULL == vio.buffer_.entry_);
        TEST_EXPECT_TRUE(!vc_->read_.enabled_);

        vc_->do_io_shutdown(IO_SHUTDOWN_READ);
        TEST_EXPECT_EQ(vio.nbytes_, 0);
        TEST_EXPECT_TRUE(ObVIO::NONE == vio.op_);
        TEST_EXPECT_TRUE(NET_VC_SHUTDOWN_READ == vc_->f_.shutdown_);

        vc_->do_io_shutdown(IO_SHUTDOWN_READWRITE);
        TEST_EXPECT_TRUE((NET_VC_SHUTDOWN_READ | NET_VC_SHUTDOWN_WRITE) == vc_->f_.shutdown_);

        vc_->closed_ = 1;
        vc_closed_= true;
        finish_ = true;
        break;
      }
      case CHECK_NET_READ_SIGNAL_AND_UPDATE: {
        vc_->set_inactivity_timeout(TEST_CHECK_INACTIVE_TIME);
        SET_HANDLER(&TestClientSession::handle_check_vc_alive);
        vc_->do_io_read(NULL, strlen(g_input[index_]) + 1, buf_);
        this_ethread()->schedule_in_local(this, HRTIME_SECONDS(2));
        break;
      }
      default: {
        ob_assert(!"unknown check_type_");
      }
    }
    result_ = RESULT_NET_EVENT_IO_SUCCEED;
    return 1;
  }

  int handle_check_vc_alive(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    DEBUG_NET("TEST", "Client %d handle_check_vc_alive", id_);
    ObNetHandler &nh = self_ethread().get_net_handler();
    TEST_EXPECT_TRUE(nh.open_list_.empty());
    finish_ = true;
    vc_closed_ = true;
    return EVENT_CONT;
  }

  int handle_read_econnreset(int event, void *data)
  {
    UNUSED(data);
    result_ = event;
    DEBUG_NET("TEST", "Client %d handle_read_econnreset result_ %d", id_, result_);

    switch (event) {
      case VC_EVENT_EOS: {
        TEST_EXPECT_TRUE(!vc_->read_.enabled_);
        TEST_EXPECT_EQ(vc_->recursion_, 1);
        finish_ = true;
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  int handle_read_todo_null(int event, void *data)
  {
    UNUSED(data);
    result_ = event;
    DEBUG_NET("TEST", "Client %d handle_read_todo_null result_ %d", id_, result_);

    switch (event) {
      case VC_EVENT_INACTIVITY_TIMEOUT: {
        hold_time_ = get_hrtime() - start_time_;
        ObHRTime wait_time = 0;
        if(CHECK_NET_WRITE_TODO_NULL == param_.check_type_) {
          wait_time = TEST_CHECK_INACTIVE_TIME;
        } else {
          wait_time = HRTIME_SECONDS(param_.inactivity_timeout_);
        }
        TEST_EXPECT_LE(hold_time_, wait_time + HRTIME_SECONDS(2));
        TEST_EXPECT_GE(hold_time_, wait_time);
        vc_->do_io_close();
        vc_closed_ = true;
        finish_ = true;
        break;
      }
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_EOS: {
        break;
      }
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  int handle_read(int event, void *data)
  {
    UNUSED(data);
    result_ = event;
    DEBUG_NET("TEST", "Client %d handle_read result_ %d", id_, result_);

    switch (event) {
      case VC_EVENT_READ_COMPLETE: {
        memset(g_session_buff[id_], 0, TEST_G_BUFF_SIZE);
        TEST_EXPECT_EQ(strlen(g_input[index_]) + 1, reader_->read_avail());
        reader_->copy(g_session_buff[id_], reader_->read_avail());
        reader_->consume_all();
        printf("read [%d]:%s.\n", id_, g_session_buff[id_]);
        fflush(stdout);
        TEST_EXPECT_EQ(vc_->read_.vio_.ntodo(), 0);
        TEST_EXPECT_TRUE(!vc_->read_.enabled_);
        vc_->closed_ = 1;
        vc_closed_ = true;
        finish_ = true;
        break;
      }
      case VC_EVENT_READ_READY:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_EOS:
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  int handle_timeout(int event, void *data)
  {
    UNUSED(data);
    UNUSED(event);
    result_ = event;
    DEBUG_NET("TEST", "Client %d handle_timeout result_ %d", id_, result_);

    switch (event) {
      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_WRITE_READY:
      case VC_EVENT_WRITE_COMPLETE:
      case VC_EVENT_INACTIVITY_TIMEOUT: {
      case VC_EVENT_EOS:
        break;
      }
      case VC_EVENT_READ_READY: {
        memset(g_session_buff[id_], 0, TEST_G_BUFF_SIZE);
        reader_->copy(g_session_buff[id_], reader_->read_avail());
        reader_->consume_all();
        printf("read [%d]:%s.\n", id_, g_session_buff[id_]);
        fflush(stdout);
        break;
      }
      case VC_EVENT_ACTIVE_TIMEOUT: {
        finish_ = true;
        vc_->do_io_close();
        hold_time_ = get_hrtime() - start_time_;
        TEST_EXPECT_GE(hold_time_, TEST_EACH_ACTIVE_TIME(id_) - HRTIME_MSECONDS(50));
        TEST_EXPECT_LE(hold_time_, TEST_EACH_ACTIVE_TIME(id_) + HRTIME_MSECONDS(950))
        INFO_NET("TEST", "Client %d VC_EVENT_ACTIVE_TIMEOUT succeed, %ld:%ld",
            id_, hold_time_, hrtime_to_nsec(TEST_EACH_ACTIVE_TIME(id_)));
        vc_closed_ = true;
        break;
      }
      case VC_EVENT_ERROR: {
        vc_->do_io_close();
        vc_closed_ = true;
        break;
      }
      default:
        ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }

  void destroy()
  {
    INFO_NET("TEST", "Client %d TestClientSession destroy", id_);
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      reader_ = NULL;
    }
    mutex_.release();
    if (!vc_closed_) {
      vc_->closed_ = -1;
    }
    vc_ = NULL;
  }
};

struct TestSessionAccept : public event::ObContinuation
{
  const TestFunParam &param_;
  volatile int32_t result_;
  volatile int32_t count_;
  TestClientSession *client_session_[TEST_MAX_SESSION_NUMBER];

  TestSessionAccept(ObProxyMutex *mutex, const TestFunParam &param)
      : ObContinuation(mutex), param_(param)
  {
    SET_HANDLER(&TestSessionAccept::main_event);
    UNUSED(param_);
    result_ = RESULT_DEFAULT;
    count_ = 0;
    memset(client_session_, 0, sizeof(client_session_));
  }

  virtual ~TestSessionAccept() { destroy(); }

  int main_event(int event, void *data)
  {
    switch (event) {
    case NET_EVENT_ACCEPT_SUCCEED: {
      DEBUG_NET("TEST", "TestSessionAccept do listen succ");
      result_ = RESULT_NET_EVENT_LISTEN_SUCCEED;
      break;
    }
    case NET_EVENT_ACCEPT_FAILED: {
      int tmp_errno = ((int)(intptr_t)data);
      DEBUG_NET("TEST", "TestSessionAccept fail to do listen, errno=%d, err_msg=%s",
          tmp_errno, strerror(tmp_errno));
      result_ = RESULT_NET_EVENT_LISTEN_FAILED;
      break;
    }
    case NET_EVENT_ACCEPT: {
      ObUnixNetVConnection *net_vc = static_cast<ObUnixNetVConnection *>(data);
      DEBUG_NET("TEST", "TestSessionAccept Accepted a connection %d %p", count_, net_vc);
      if (NULL == (client_session_[count_] = new(std::nothrow) TestClientSession(
          net_vc->mutex_, net_vc, count_, param_))) {
        ERROR_NET("failed to allocate memory for TestClientSession");
        result_ = RESULT_NET_EVENT_ERROR_NO_MEMORY;
      } else if (count_ >= TEST_MAX_SESSION_NUMBER){
        ERROR_NET("TestClientSession accept too many");
        result_ = RESULT_NET_EVENT_ERROR_TOO_MANY;
      } else {
        g_event_processor.schedule_imm(client_session_[count_]);
        ATOMIC_FAA(&count_, 1);
        result_ = RESULT_NET_EVENT_ACCEPT_SUCCEED;
      }
      break;
    }
    case EVENT_ERROR: {
     if (-ECONNABORTED == ((long)data)) {
        ERROR_NET("client hang, accept failed");
      }
      ERROR_NET("TestSessionAccept received fatal error: errno = %d, "
                "obproxy will exit", -((int)(intptr_t)data));
      result_ = RESULT_NET_EVENT_ERROR;
      break;
    }
    default:
      ob_release_assert(!"error, never run here!");
    }
    return EVENT_CONT;
  }

  void destroy()
  {
    INFO_NET("TEST", "TestSessionAccept destroy");
    for (int32_t i = 0; i < count_ && NULL != client_session_[i]; i++) {
      delete client_session_[i];
    }
    sleep(1);
    mutex_.release();
  }
};

struct TestProcessorAccept : public event::ObContinuation
{
  ObNetProcessor::ObAcceptOptions &options_;
  const TestFunParam param_;
  ObNetAcceptAction *accept_action_;
  TestSessionAccept *session_accept_;
  volatile bool started_;

  TestProcessorAccept(ObProxyMutex *mutex, ObNetProcessor::ObAcceptOptions &options,
      const TestFunParam param)
    : ObContinuation(mutex), options_(options), param_(param)
  {
    SET_HANDLER(&TestProcessorAccept::handle_start_accept);
    accept_action_ = NULL;
    session_accept_ = NULL;
    started_ = false;
  }

  virtual ~TestProcessorAccept() { destroy(); }

  int handle_start_accept(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "TestProcessorAccept : handle_start_accept");
    if (NULL == (session_accept_ = new(std::nothrow) TestSessionAccept(mutex_, param_))) {
      ERROR_NET("failed to allocate memory for TestSessionAccept");
    } else {
      accept_action_ = static_cast<ObNetAcceptAction *>(g_net_processor.accept(
          *session_accept_, options_));
      started_ = true;
    }
    return EVENT_CONT;
  }

  void destroy()
  {
    INFO_NET("TEST", "TestProcessorAccept destroy");
    if (NULL != accept_action_) {
      accept_action_->cancelled_ = true;
      accept_action_ = NULL;
    }
    delete session_accept_;
    session_accept_ = NULL;
    mutex_.release();
  }
};


class TestUnixNetVConnection : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void common_accept(ObNetProcessor::ObAcceptOptions &options, const TestFunParam &param);
  bool start_connect(const uint16_t port, const TestFunParam &param);
  bool check_vconnection(const TestFunParam &param);
  bool check_vc_net_common(const TestFunParam &param);

public:
  TestProcessorAccept *processor_accept_;
  TestProcessorConnect *processor_connect_;
  bool test_passed_;
};


void TestUnixNetVConnection::SetUp()
{
  processor_accept_ = NULL;
  processor_connect_ = NULL;
  test_passed_ = false;
  memset(g_session_buff, 0, sizeof(g_session_buff));
}

void TestUnixNetVConnection::TearDown()
{
  if (NULL != processor_connect_) {
    delete processor_connect_;
    processor_connect_ = NULL;
  } else {}

  if (NULL != processor_accept_) {
    delete processor_accept_;
    processor_accept_ = NULL;
  } else {}

  sleep(2);
}

bool TestUnixNetVConnection::check_vc_net_common(const TestFunParam &param)
{
  INFO_NET("TEST", "check_vc_net_common %d type %d", param.vc_number_, param.check_type_);
  bool ret = true;
  TestSessionConnect **connect = processor_connect_->session_connect_;
  TestClientSession **client = processor_accept_->session_accept_->client_session_;
  ObHRTime last_time = get_hrtime_internal();
  ObHRTime wait_time = TEST_CHECK_ACTIVE_TIME;
  int32_t pass_count = 0;
  bool pass[TEST_MAX_SESSION_NUMBER] = {0};

  if (CHECK_TIMEOUT == param.check_type_) {
    wait_time += HRTIME_SECONDS(TEST_MAX_SESSION_NUMBER + 2);
  } else if (CHECK_NET_WRITE_TODO_NULL == param.check_type_
      || CHECK_NET_READ_TODO_NULL == param.check_type_) {
    wait_time = HRTIME_SECONDS(param.inactivity_timeout_ + 2);
  }
  //timeout and wait
  while (pass_count < (param.vc_number_ * 2)
      && (get_hrtime_internal() - last_time)  <= wait_time) {
    for (int32_t j = 0; j < param.vc_number_; ++j) {
      if (!pass[j] && connect[j]->finish_) {
        ++pass_count;
        pass[j] = true;
      }
      if (!pass[param.vc_number_ + j] && NULL != client[j] && client[j]->finish_) {
        ++pass_count;
        pass[param.vc_number_ + j] = true;
      }
    }
  }

  //check timeout
  for (int32_t j = 0; j < param.vc_number_; ++j) {
    EXPECT_TRUE(connect[j]->finish_);
    EXPECT_TRUE(connect[j]->check_ok_);
    ret &= (connect[j]->finish_ && connect[j]->check_ok_);
    EXPECT_TRUE(NULL != client[j] && client[j]->finish_);
    EXPECT_TRUE(client[j]->check_ok_);
    ret &= (NULL != client[j] && client[j]->finish_ && client[j]->check_ok_);
  }

  return ret;
}

bool TestUnixNetVConnection::check_vconnection(const TestFunParam &param)
{
  UNUSED(param);
  INFO_NET("TEST", "check_vconnection %d type %d", param.vc_number_, param.check_type_);
  bool ret = true;
  TestSessionConnect **connect = processor_connect_->session_connect_;

  sleep(1);//make it idling in inactivitycop
  for (int32_t j = 0; j < param.vc_number_; ++j) {
    g_event_processor.schedule_imm(connect[j]);
  }

  switch (param.check_type_){
    case CHECK_TIMEOUT:
    case CHECK_NET_COMPLETE:
    case CHECK_NET_ECONNRESET_CLIENT:
    case CHECK_NET_WRITE_TODO_NULL:
    case CHECK_NET_READ_TODO_NULL:
    case CHECK_NET_GET_DATA:
    case CHECK_NET_WRITE_SIGNAL_AND_UPDATE:
    case CHECK_NET_READ_SIGNAL_AND_UPDATE: {
      ret = check_vc_net_common(param);
      break;
    }
    default: {
      ob_assert(!"unknown check_type_");
    }
  }

  return ret;
}

bool TestUnixNetVConnection::start_connect(const uint16_t port, const TestFunParam &param)
{
  int ret = true;
  ObIpEndpoint ip_addr;
  ip_addr.set_to_loopback(AF_INET);
  ip_addr.sin_.sin_port = (htons)(port);

  if (NULL == (processor_connect_ = new(std::nothrow) TestProcessorConnect(new_proxy_mutex(),
      ip_addr.sa_, param))) {
    ERROR_NET("failed to allocate memory for TestProcessorConnect");
    EXPECT_TRUE(false);
  } else {
    g_event_processor.schedule_imm(processor_connect_);

    ObHRTime last_time = get_hrtime_internal();
    while (!processor_connect_->start_
        && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(3)) { }
    if (!processor_connect_->start_) {
      ret = false;
      ERROR_NET("processor_connect_ start failed");
      EXPECT_TRUE(false);
    } else {
      for (int32_t i = 0; i < param.vc_number_; ++i) {
        ObHRTime last_time = get_hrtime_internal();
        while ((NULL == processor_connect_->session_connect_[i]
                || NET_EVENT_OPEN != processor_connect_->session_connect_[i]->result_)
            && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(3)) { }
        if (NET_EVENT_OPEN != processor_connect_->session_connect_[i]->result_) {
          ret = false;
          ERROR_NET("connect %d connect failed", i);
          EXPECT_TRUE(false);
          break;
        } else {
          INFO_NET("TEST", "connect %d connect succeed", i);
        }
      }
    }
  }
  return ret;
}

void TestUnixNetVConnection::common_accept(ObNetProcessor::ObAcceptOptions &options,
    const TestFunParam &param)
{
  if (NULL == (processor_accept_ = new(std::nothrow) TestProcessorAccept(
      new_proxy_mutex(), options, param))) {
    ERROR_NET("failed to allocate memory for TestProcessorAccept");
    EXPECT_TRUE(false);
  } else {
    g_event_processor.schedule_imm(processor_accept_);

    ObHRTime last_time = get_hrtime_internal();//wait for finish calling accept
    while (!processor_accept_->started_
        && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(2)) { }
    if (!processor_accept_->started_) {
      ERROR_NET("processor_accept_ start failed");
      EXPECT_TRUE(false);
    } else if (RESULT_NET_EVENT_LISTEN_SUCCEED != processor_accept_->session_accept_->result_) {
      ERROR_NET("processor_accept_ is cancelled %d", processor_accept_->session_accept_->result_);
      EXPECT_TRUE(false);
    } else if (!start_connect((uint16_t)(options.local_port_), param)) {
      ERROR_NET("start_connect failed");
      EXPECT_TRUE(false);
    } else {
      test_passed_ = check_vconnection(param);
    }
  }
}

TEST_F(TestUnixNetVConnection, active_timeout)
{
  INFO_NET("TEST", "active_timeout");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 1;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_TIMEOUT, 3, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, active_inactivity_timeout_frequent_accept_)
{
  INFO_NET("TEST", "active_inactivity_timeout_frequent_accept_");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 2;
  options.frequent_accept_ = true;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_TIMEOUT, 10, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_complete)
{
  INFO_NET("TEST", "net_complete");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 3;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_NET_COMPLETE, 3, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_complete_frequent_accept_)
{
  INFO_NET("TEST", "net_complete_frequent_accept_");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 4;
  options.frequent_accept_ = true;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_NET_COMPLETE, 3, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_write_todo_null)
{
  INFO_NET("TEST", "net_write_todo_null");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 5;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  //start update max_connections
  int64_t inactivity_timeout = 10;
  update_cop_config(inactivity_timeout, 0);

  TestFunParam param(CHECK_NET_WRITE_TODO_NULL, 3, &vc_options, inactivity_timeout);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_read_todo_null)
{
  INFO_NET("TEST", "net_read_todo_null");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 6;
  options.frequent_accept_ = true;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  int64_t inactivity_timeout = 10;
  update_cop_config(inactivity_timeout, 0);

  TestFunParam param(CHECK_NET_READ_TODO_NULL, 10, &vc_options, inactivity_timeout);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_econnreset_client)
{
  INFO_NET("TEST", "net_econnreset_client");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 7;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_NET_ECONNRESET_CLIENT, 5, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_get_data)
{
  INFO_NET("TEST", "net_get_data");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 8;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_NET_GET_DATA, 5, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNetVConnection, net_read_signal_and_update)
{
  INFO_NET("TEST", "net_read_signal_and_update");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 10;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;
  ObNetVCOptions vc_options;

  TestFunParam param(CHECK_NET_READ_SIGNAL_AND_UPDATE, 10, &vc_options);
  common_accept(options, param);
  EXPECT_TRUE(test_passed_);
}

} // end of namespace obproxy
} // end of namespace oceanbase


int main(int argc, char **argv)
{
//  system("rm -f test_unix_net_vconnection.log*");
//  OB_LOGGER.set_file_name("test_unix_net_vconnection.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::obproxy::init_g_net_processor();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

struct TestClientDoIO;

#define TEST_PORT_START              9100
#define MAX_SESSION_NUMBER           10
#define TEST_BLOCK_SEC               4
#define TEST_INACTIVE_TIME           HRTIME_SECONDS(3)
#define TEST_EACH_INACTIVE_TIME(j)   (TEST_INACTIVE_TIME + HRTIME_SECONDS(j))

char g_session_buff[MAX_SESSION_NUMBER][TEST_G_BUFF_SIZE] = {};
ObEThread *g_epoll_ethread;
TestClientDoIO *g_client_do_io[MAX_SESSION_NUMBER] = {};

//connect
struct TestSessionConnect : public event::ObContinuation
{
  const int32_t id_;
  const bool multi_writer_;
  const bool check_cop_;
  ObUnixNetVConnection *vc_;
  ObIOBufferReader *reader_;
  ObMIOBuffer *buf_;
  volatile int32_t result_;
  volatile bool finish_;
  int32_t index_;
  ObHRTime start_time_;
  ObHRTime diff_time_;

  TestSessionConnect(ObProxyMutex *mutex, const int32_t id,
      const bool multi_writer,  const bool check_cop)
    : ObContinuation(mutex), id_(id),
      multi_writer_(multi_writer), check_cop_(check_cop)
  {
    SET_HANDLER(&TestSessionConnect::handle_connect);
    vc_ = NULL;
    buf_ = new_miobuffer(TEST_G_BUFF_SIZE);
    reader_ = buf_->alloc_reader();
    result_ = RESULT_DEFAULT;
    finish_ = false;
    index_ = 0;
    start_time_ = 0;
    diff_time_ = 0;
  }

  virtual ~TestSessionConnect() { free(); }

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
    if (check_cop_) {
      INFO_NET("TEST", "connect %d set handle_keep_alive", id_);
      SET_HANDLER(&TestSessionConnect::handle_keep_alive);
      vc_->do_io_write(this, 0, reader_);
      vc_->write_.enabled_ = false;//disable main_net_event
      vc_->set_inactivity_timeout(TEST_EACH_INACTIVE_TIME(id_));
      vc_->cancel_active_timeout();
      start_time_ = get_hrtime_internal();
    } else if (multi_writer_) {
      INFO_NET("TEST", "connect %d handle_do_io read", id_);
      SET_HANDLER(&TestSessionConnect::handle_read);
      vc_->do_io_read(this, strlen(g_input[index_]) + 1, buf_);
    } else {
      INFO_NET("TEST", "connect %d handle_do_io write", id_);
      SET_HANDLER(&TestSessionConnect::handle_write);
      vc_->do_io_write(this, strlen(g_input[index_]) + 1, reader_);
      vc_->set_inactivity_timeout(TEST_EACH_INACTIVE_TIME(id_));
    }
    result_ = RESULT_NET_EVENT_IO_SUCCEED;
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
      INFO_NET("TEST", "connect %d handle_write index=%d", id_, index_);
      ob_assert(buf_->write_avail() <= TEST_G_BUFF_SIZE);
      size = (int64_t)strlen(g_input[index_]) + 1;
      if (buf_->write_avail() >= size) {
        printf("----writer[%d]:%s.\n", id_, g_input[index_]);
        fflush(stdout);
        buf_->write(g_input[index_], size, written_len);
      } else {
        result_ = RESULT_NET_WRITE_UNFINISH;
        ERROR_NET("connect %d i=%d : write_avail()=%ld < %ld\n", id_, index_, buf_->write_avail(), size);
      }
      if (result_ != RESULT_NET_WRITE_UNFINISH) {
        finish_ = true;
        index_++;
      } else {}
      break;
    }
    case VC_EVENT_WRITE_COMPLETE: {
      vc_->do_io_write(this, 0, reader_);
      break;
    }
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    DEBUG_NET("TEST", "connect %d handle_write result_ %d", id_, result_);
    return EVENT_CONT;
  }

  int handle_read(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    return EVENT_CONT;
  }

  int handle_keep_alive(int event, void *data)
  {
    UNUSED(data);
    UNUSED(event);
    INFO_NET("TEST", "connect %d handle_keep_alive", id_);
    result_ = event;
    switch (event) {
    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:{
      break;
    }
    case EVENT_IMMEDIATE:
    case VC_EVENT_INACTIVITY_TIMEOUT:{
      finish_ = true;
      vc_->do_io_close();
      diff_time_ = get_hrtime_internal() - start_time_;
      break;
    }
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    DEBUG_NET("TEST", "connect %d handle_keep_alive result_ %d", id_, result_);

    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestSessionConnect free %d", id_);
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      reader_ = NULL;
    } else { }
    mutex_.release();
    if (0 == vc_->closed_) {
      vc_->closed_ = -1;
    }
    vc_ = NULL;
  }
};

struct TestProcessorConnect : public event::ObContinuation
{
  sockaddr addr_;
  const int32_t vc_number_;
  const bool multi_writer_;
  const bool check_cop_;
  ObNetVCOptions *options_;
  volatile bool start_;
  ObAction *action_[MAX_SESSION_NUMBER];
  TestSessionConnect *session_connect_[MAX_SESSION_NUMBER];

  TestProcessorConnect(ObProxyMutex *mutex, sockaddr addr,
      const int32_t vc_number, const bool multi_writer, const bool check_cop)
    : ObContinuation(mutex), addr_(addr),
      vc_number_(vc_number), multi_writer_(multi_writer), check_cop_(check_cop)
  {
    SET_HANDLER(&TestProcessorConnect::handle_start_connect);
    options_ = NULL;
    start_ = false;
    memset(action_, 0, sizeof(action_));
    memset(session_connect_, 0, sizeof(session_connect_));
  }

  virtual ~TestProcessorConnect() { free(); }

  int handle_start_connect(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "handle_start_connect");
    for (int32_t i = 0; i < vc_number_; ++i) {
      if (NULL == (session_connect_[i] = new(std::nothrow) TestSessionConnect(mutex_,
          i, multi_writer_, check_cop_))) {
        ERROR_NET("failed to new TestSessionConnect %d", i);
        start_ = false;
        break;
      } else {
        g_net_processor.connect_re(*(session_connect_[i]), addr_, options_);
        start_ = true;
        INFO_NET("TEST", "call connect_re succeed %d", i);
      }
    }
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestProcessorConnect free");
    for (int32_t i = 0; i < vc_number_; i++) {
      delete session_connect_[i];
    }
    sleep(1);// wait for close vc by inactivitycop
    if (NULL != options_) {
      delete options_;
    }
    mutex_.release();
  }
};

//accpet
struct TestClientSession : public event::ObContinuation
{
  ObUnixNetVConnection *vc_;
  int32_t id_;
  const bool multi_writer_;
  const bool check_cop_;
  ObIOBufferReader *reader_;
  ObMIOBuffer *buf_;
  bool handle_longer_;
  volatile int32_t result_;
  volatile bool finish_;
  volatile bool block_finish_;
  int32_t index_;
  ObHRTime start_time_;
  ObHRTime diff_time_;

  TestClientSession(ObProxyMutex *mutex, ObUnixNetVConnection *netvc, int32_t id,
      const bool multi_writer, const bool check_cop)
      : ObContinuation(mutex), vc_(netvc), id_(id),
        multi_writer_(multi_writer), check_cop_(check_cop)
  {
    SET_HANDLER(&TestClientSession::handle_do_io);
    result_ = RESULT_DEFAULT;
    handle_longer_ = false;
    finish_ = false;
    block_finish_ = false;
    buf_ = new_miobuffer(TEST_G_BUFF_SIZE);
    reader_ = buf_->alloc_reader();
    index_ = 0;
    start_time_ = 0;
    diff_time_ = 0;
  }

  virtual ~TestClientSession() { free(); }

  int handle_do_io(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    if (check_cop_) {
      INFO_NET("TEST", "chiled %d set handle_keep_alive", id_);
      SET_HANDLER(&TestClientSession::handle_keep_alive);
      vc_->do_io_read(this, INT64_MAX, buf_);
      vc_->read_.enabled_ = false;//disable main_net_event
      vc_->add_to_keep_alive_lru();
      vc_->add_to_keep_alive_lru();//twice just for check
      vc_->set_inactivity_timeout(TEST_EACH_INACTIVE_TIME(id_));
      vc_->cancel_active_timeout();
      start_time_ = get_hrtime_internal();
    } else if (!multi_writer_) {
      INFO_NET("TEST", "Client %d handle_do_io read", id_);
      SET_HANDLER(&TestClientSession::handle_read);
      vc_->do_io_read(this, strlen(g_input[index_]) + 1, buf_);
    } else {
      INFO_NET("TEST", "Client %d handle_do_io write", id_);
      SET_HANDLER(&TestClientSession::handle_write);
      vc_->do_io_write(this, strlen(g_input[index_]) + 1, reader_);
    }
    result_ = RESULT_NET_EVENT_IO_SUCCEED;
    return 1;
  }

  int handle_read(int event, void *data)
  {
    UNUSED(data);
    int64_t size;
    result_ = event;
    switch (event) {
    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE: {
      INFO_NET("TEST", "Client %d handle_read VC_EVENT_READ_*", id_);
      size = reader_->read_avail();
      ob_assert(size <= TEST_G_BUFF_SIZE && size > 0);
      memset(g_session_buff[id_], 0, TEST_G_BUFF_SIZE);
      reader_->copy(g_session_buff[id_], size);
      reader_->consume(size);
      printf("----g_buff[%d]:%s.\n", id_, g_session_buff[id_]);
      fflush(stdout);
      finish_ = true;
      sleep(handle_longer_ ? TEST_BLOCK_SEC : 0);
      block_finish_ = true;
      index_++;
      break;
    }
    case VC_EVENT_EOS: {
      vc_->do_io_read(this, 0, buf_);
      break;
    }
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ERROR: {
      INFO_NET("TEST", "Client %d do_io_close", id_);
      vc_->do_io_close();
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    DEBUG_NET("TEST", "Client %d handle_read result_ %d", id_, result_);

    return EVENT_CONT;
  }

  int handle_write(int event, void *data)
  {
    UNUSED(data);
    UNUSED(event);

    return EVENT_CONT;
  }

  int handle_keep_alive(int event, void *data)
  {
    UNUSED(data);
    UNUSED(event);
    result_ = event;
    switch (event) {
    case EVENT_IMMEDIATE:
    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_EOS: {
      break;
    }
    case VC_EVENT_INACTIVITY_TIMEOUT: {
      finish_ = true;
      vc_->do_io_close();
      diff_time_ = get_hrtime_internal() - start_time_;
      break;
     }
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    DEBUG_NET("TEST", "Client %d handle_keep_alive result_ %d", id_, result_);

    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "Client %d TestClientSession free", id_);
    if (NULL != buf_) {
      free_miobuffer(buf_);
      buf_ = NULL;
      reader_ = NULL;
    } else {}
    mutex_.release();
    if (0 == vc_->closed_) {
      vc_->closed_ = -1;
    }
    vc_ = NULL;
  }

};

struct TestSessionAccept : public event::ObContinuation
{
  const int32_t vc_number_;
  const bool multi_writer_;
  const bool check_cop_;
  volatile int32_t result_;
  volatile int32_t count_;
  TestClientSession *client_session_[MAX_SESSION_NUMBER];

  TestSessionAccept(ObProxyMutex *mutex, const int32_t vc_number,
      const bool multi_writer = false, const bool check_cop = false)
      : ObContinuation(mutex), vc_number_(vc_number),
        multi_writer_(multi_writer), check_cop_(check_cop)
  {
    SET_HANDLER(&TestSessionAccept::main_event);
    result_ = RESULT_DEFAULT;
    count_ = 0;
    memset(client_session_, 0, sizeof(client_session_));
  }

  virtual ~TestSessionAccept() { free(); }

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
      DEBUG_NET("TEST", "TestSessionAccept fail to do listen,"
                "errno=%d, err_msg=%s, obproxy will exit.", tmp_errno,
                strerror(tmp_errno));
      result_ = RESULT_NET_EVENT_LISTEN_FAILED;
      break;
    }
    case NET_EVENT_ACCEPT: {
      ObUnixNetVConnection *net_vc = static_cast<ObUnixNetVConnection *>(data);
      DEBUG_NET("TEST", "TestSessionAccept Accepted a connection %d %p", count_, net_vc);
      if (NULL == (client_session_[count_] = new(std::nothrow) TestClientSession(
          net_vc->mutex_, net_vc, count_, multi_writer_, check_cop_))) {
        ERROR_NET("failed to allocate memory for TestClientSession");
        result_ = RESULT_NET_EVENT_ERROR;
      } else {
        if (!check_cop_ && 0 == count_) {
            g_epoll_ethread = this_ethread();
        } else if (!check_cop_ && (vc_number_ - 1) == count_) {//the last one need run longer time
            client_session_[count_]->handle_longer_ = true;
        } else {}
        this_ethread()->schedule_imm(client_session_[count_]);
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

  void free()
  {
    INFO_NET("TEST", "TestSessionAccept free");
    for (int32_t i = 0; i < vc_number_; i++) {
      delete client_session_[i];
      client_session_[i] = NULL;
    }
    sleep(1);
    mutex_.release();
  }
};

struct TestProcessorAccept : public event::ObContinuation
{
  ObNetProcessor::ObAcceptOptions &options_;
  ObNetAcceptAction *accept_action_;
  TestSessionAccept *session_accept_;
  const int32_t vc_number_;
  const bool multi_writer_;
  const bool check_cop_;
  volatile bool started_;

  TestProcessorAccept(ObProxyMutex *mutex, ObNetProcessor::ObAcceptOptions &options,
      const int32_t vc_number, const bool multi_writer, const bool check_cop)
    : ObContinuation(mutex), options_(options), vc_number_(vc_number),
      multi_writer_(multi_writer), check_cop_(check_cop)
  {
    SET_HANDLER(&TestProcessorAccept::handle_start_accept);
    accept_action_ = NULL;
    session_accept_ = NULL;
    started_ = false;
  }

  virtual ~TestProcessorAccept() { free(); }

  int handle_start_accept(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    INFO_NET("TEST", "TestProcessorAccept : handle_start_accept");
    if (NULL == (session_accept_ = new(std::nothrow) TestSessionAccept(mutex_, vc_number_,
        multi_writer_, check_cop_))) {
      ERROR_NET("failed to allocate memory for TestSessionAccept");
    } else {
      accept_action_ = static_cast<ObNetAcceptAction *>(g_net_processor.accept(
          *session_accept_, options_));
      started_ = true;
    }
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestProcessorAccept free");
    accept_action_->cancelled_ = true;
    accept_action_ = NULL;
    delete session_accept_;
    session_accept_ = NULL;
    mutex_.release();
  }
};

struct TestClientDoIO : public event::ObContinuation
{
  volatile int32_t result_;
  TestClientSession *client_session_;
  TestSessionConnect *connect_;

  TestClientDoIO(ObProxyMutex *mutex, TestClientSession *client, TestSessionConnect *connect)
      : ObContinuation(mutex), client_session_(client), connect_(connect)
  {
    SET_HANDLER(&TestClientDoIO::handle_set_do_io);
    result_ = RESULT_DEFAULT;
    ob_assert(connect_->id_ == client_session_->id_);
  }

  virtual ~TestClientDoIO() { free(); }

  int handle_set_do_io(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    if (!client_session_->multi_writer_) {
      INFO_NET("TEST", "client %d handle_set_do_io read", connect_->id_);
      MUTEX_TRY_LOCK(lock, connect_->mutex_, this_ethread());
      if (lock.is_locked()) {
        client_session_->vc_->do_io_read(client_session_, strlen(g_input[client_session_->index_]) + 1, client_session_->buf_);
        connect_->vc_->do_io_write(connect_, strlen(g_input[connect_->index_]) + 1, connect_->reader_);
        result_ = RESULT_NET_EVENT_IO_SUCCEED;
      } else {
        ERROR_NET("connect %d failed to try lock", connect_->id_);
        this_ethread()->schedule_in(this, NET_RETRY_DELAY);
      }
    } else {
      INFO_NET("TEST", "Client %d handle_set_do_io write", client_session_->id_);
      client_session_->vc_->write_.vio_.reenable();
      result_ = RESULT_NET_EVENT_IO_SUCCEED;
    }
    return EVENT_CONT;
  }

  void free()
  {
    INFO_NET("TEST", "TestClientDoIO free %d", client_session_->id_);
    mutex_.release();
    client_session_ = NULL;
    connect_ = NULL;
  }
};


class TestUnixNet : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  bool check_accept(const int32_t local_port, const int32_t vc_number,
      const bool multi_writer, const int64_t max_connections = 0);
  bool start_mutil_connect(const uint16_t port, const int32_t vc_number, const bool multi_writer,
      const int64_t max_connections = 0);
  void common_accept(ObNetProcessor::ObAcceptOptions &options,
      const int32_t vc_number, const bool multi_writer, const int64_t max_connections = 0);
  void check_accept_call(const ObNetProcessor::ObAcceptOptions &options);
  bool check_read_result(const int32_t vc_number);
  bool check_read_result_internel(int32_t client_id, int32_t buf_id);
  bool check_write_result(const int32_t vc_number);
  bool check_write_result_internel(int32_t client_id, int32_t buf_id);
  bool call_client_do_io(int32_t client_id);
  bool check_inactivitycop(const int32_t vc_number, const int64_t max_connections);

public:
  TestProcessorAccept *processor_accept_;
  TestProcessorConnect *processor_connect_;
  bool test_passed_;
};

void TestUnixNet::SetUp()
{
  processor_accept_ = NULL;
  processor_connect_ = NULL;
  test_passed_ = false;
  memset(g_client_do_io, 0, sizeof(g_client_do_io));
  memset(g_session_buff, 0, MAX_SESSION_NUMBER * TEST_G_BUFF_SIZE);
  g_epoll_ethread = NULL;
}

void TestUnixNet::TearDown()
{
  g_epoll_ethread = NULL;
  if (NULL != processor_connect_) {
    delete processor_connect_;
    processor_connect_ = NULL;
  } else {}

  if (NULL != processor_accept_) {
    delete processor_accept_;
    processor_accept_ = NULL;
  } else {}

  for (int32_t i = 0; i < MAX_SESSION_NUMBER; ++i) {
    if (NULL != g_client_do_io[i]) {
      delete g_client_do_io[i];
    } else {}
  }

  sleep(2);
}

bool TestUnixNet::check_read_result(const int32_t vc_number)
{
  INFO_NET("TEST", "check_read_result");
  bool ret = true;

  UNUSED(vc_number);
  TestClientSession **client = processor_accept_->session_accept_->client_session_;

  for (int32_t j = 0; j < (vc_number - 1); ++j) {
    g_event_processor.schedule_imm(processor_connect_->session_connect_[j]);
  }

  for (int32_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
    printf("----input [%d]:%s.\n", i, g_input[i]);
    fflush(stdout);
    for (int32_t j = 0; j < (vc_number - 1); ++j) {
      ret &= check_read_result_internel(j, i);
    }

    if (0 == i) { //the last one will handler longer
      g_event_processor.schedule_imm(processor_connect_->session_connect_[vc_number - 1]);
    }

    ObHRTime last_time = get_hrtime_internal();
    while (!client[vc_number - 1]->finish_
        && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(3)) { }
    if (!client[vc_number - 1]->finish_) {
      ERROR_NET("client %d failed to read data from connect", vc_number - 1);
      ret &= false;
      EXPECT_TRUE(ret);
    } else if(client[vc_number - 1]->block_finish_) {
      ERROR_NET("client %d run too fast", vc_number - 1);
      ret &= false;
      EXPECT_TRUE(ret);
    } else if (i < (TEST_STRING_GROUP_COUNT - 1)){
      for (int32_t j = 0; j < (vc_number - 1); ++j) {
        ret &=  call_client_do_io(j);
        EXPECT_TRUE(ret);
      }
      ret &= check_read_result_internel(vc_number - 1, i);
      ret &= call_client_do_io(vc_number - 1);
      EXPECT_TRUE(ret);
    } else {
      ret &= check_read_result_internel(vc_number - 1, i);
      EXPECT_TRUE(ret);
    }
  }
  return ret;
}

bool TestUnixNet::check_read_result_internel(int32_t client_id, int32_t buf_id)
{
  int ret = false;
  INFO_NET("TEST", "check_read_result_internel %d", client_id);

  ObHRTime last_time = get_hrtime_internal();
  while (!processor_connect_->session_connect_[client_id]->finish_
      && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(3)) { }
  if (!processor_connect_->session_connect_[client_id]->finish_) {
    ERROR_NET("connect %d failed to write data", client_id);
    EXPECT_TRUE(false);
  } else {
    TestClientSession **client = processor_accept_->session_accept_->client_session_;
    last_time = get_hrtime_internal();
    while ((NULL == client[client_id] || !client[client_id]->block_finish_)
        && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(5)) { }
    if (!client[client_id]->block_finish_) {
      ERROR_NET("client %d failed to check_read_result_internel", client_id);
      EXPECT_TRUE(false);
    } else {
      EXPECT_LE(strlen(g_session_buff[client_id]), TEST_MAX_STRING_LENGTH);
      EXPECT_EQ(0, memcmp(g_input[buf_id], g_session_buff[client_id], TEST_MAX_STRING_LENGTH));
      client[client_id]->block_finish_ = false;
      client[client_id]->finish_ = false;
      ret = true;
      INFO_NET("TEST", "client %d check_read_result_internel succeed", client_id);
    }
  }
  return ret;
}

bool TestUnixNet::call_client_do_io(int32_t client_id)
{
  int ret = false;
  INFO_NET("TEST", "call_client_do_io %d", client_id);

  if (NULL != g_client_do_io[client_id]) {
    delete g_client_do_io[client_id];
  }
  TestClientSession *client = processor_accept_->session_accept_->client_session_[client_id];

  if (NULL == (g_client_do_io[client_id] = new(std::nothrow) TestClientDoIO(
      client->vc_->read_.vio_.mutex_, client, processor_connect_->session_connect_[client_id]))) {
    ERROR_NET("failed to allocate memory for TestClientDoIO %d", client_id);
  } else {
    ObEThread *ethread = NULL;
    while (g_epoll_ethread == (ethread = g_event_processor.assign_thread(ET_CALL))) { }
    ethread->schedule_imm_local(g_client_do_io[client_id]);
    ret = true;
  }
  return ret;
}

bool TestUnixNet::check_write_result_internel(int32_t client_id, int32_t buf_id)
{
  int ret = true;
  UNUSED(buf_id);
  UNUSED(client_id);
  return ret;
}

bool TestUnixNet::check_write_result(const int32_t vc_number)
{
  INFO_NET("TEST", "check_write_result");
  bool ret = true;
  UNUSED(vc_number);
  return ret;
}

bool TestUnixNet::check_inactivitycop(const int32_t vc_number, const int64_t max_connections)
{
  INFO_NET("TEST", "check_inactivitycop %d:%ld", vc_number, max_connections);
  bool ret = true;
  TestSessionConnect **connect = processor_connect_->session_connect_;
  TestClientSession **client = processor_accept_->session_accept_->client_session_;

  //use the same ethrad to add_to_keep_alive_lru
  //the lru is control by the seq of add_to_keep_alive_lru
  ObEThread *ethread = g_event_processor.assign_thread(ET_NET);

  sleep(1);//make it idling in inactivitycop

  for (int32_t j = 0; j < vc_number; ++j) {
    ethread->schedule_imm_local(connect[j]);
  }

  //start update max_connections
  int64_t inactivity_timeout = 8;
  update_cop_config(inactivity_timeout, max_connections);
  //reset the start time
  for (int32_t j = 0; j < vc_number; ++j) {
    if (!connect[j]->finish_) {
      connect[j]->start_time_ = get_hrtime_internal();
    }
    if (NULL != client[j] && !client[j]->finish_) {
      client[j]->start_time_ = get_hrtime_internal();
    }
  }

  //make one to close in inactivitycop
  ethread->schedule_imm_local(connect[vc_number - 1]);

  ObHRTime last_time = get_hrtime_internal();
  int32_t pass_count = 0;
  bool pass[MAX_SESSION_NUMBER] = {0};
  while (pass_count < (vc_number * 2)
      && (get_hrtime_internal() - last_time)  <= (TEST_INACTIVE_TIME + HRTIME_SECONDS(MAX_SESSION_NUMBER + 1))) {
    for (int32_t j = 0; j < vc_number; ++j) {
      if (!pass[j] && connect[j]->finish_) {
        ++pass_count;
        pass[j] = true;
      }
      if (!pass[vc_number + j] && NULL != client[j] && client[j]->finish_) {
        ++pass_count;
        pass[vc_number + j] = true;
      }
    }
  }

  //check timeout
  for (int32_t j = 0; j < vc_number; ++j) {
    if (!connect[j]->finish_) {
      EXPECT_TRUE(false);
      ERROR_NET("connect %d check_inactivitycop_internel failed", j);
    } else if (j != vc_number - 1){
      EXPECT_LE(connect[j]->diff_time_, HRTIME_SECONDS(1) + TEST_EACH_INACTIVE_TIME(j) + HRTIME_MSECONDS(500));
      EXPECT_GE(connect[j]->diff_time_, TEST_EACH_INACTIVE_TIME(j));
      INFO_NET("TEST", "connect %d check_inactivitycop_internel succeed, %ld:%ld",
          j, connect[j]->diff_time_, hrtime_to_nsec(HRTIME_SECONDS(1) + TEST_EACH_INACTIVE_TIME(j)));
    } else {
      EXPECT_LE(connect[j]->diff_time_, HRTIME_MSECONDS(100));
      EXPECT_GE(connect[j]->diff_time_, 0);
      INFO_NET("TEST", "connect %d check_inactivitycop_internel succeed, %ld", j,
          connect[j]->diff_time_);
    }
    if (NULL == client[j] || !client[j]->finish_) {
      ERROR_NET("client %d check_inactivitycop_internel failed", j);
      EXPECT_TRUE(false);
    } else if (j != vc_number - 1){
      EXPECT_LE(client[j]->diff_time_, HRTIME_SECONDS(1) + HRTIME_MSECONDS(500));
      EXPECT_GE(client[j]->diff_time_, 0);
      INFO_NET("TEST", "client %d check_inactivitycop_internel succeed, %ld", j,
          client[j]->diff_time_);
    } else {
      EXPECT_LE(client[j]->diff_time_, TEST_EACH_INACTIVE_TIME(j) + HRTIME_MSECONDS(500));
      EXPECT_GE(client[j]->diff_time_, TEST_EACH_INACTIVE_TIME(j) - HRTIME_SECONDS(1));
      INFO_NET("TEST", "client %d check_inactivitycop_internel succeed, %ld:%ld",
          j, client[j]->diff_time_, hrtime_to_nsec(TEST_EACH_INACTIVE_TIME(j)));
    }
  }
  return ret;
}

bool TestUnixNet::start_mutil_connect(const uint16_t port, const int32_t vc_number,
    const bool multi_writer, const int64_t max_connections)
{
  int ret = true;
  ObIpEndpoint ip_addr;
  ip_addr.set_to_loopback(AF_INET);
  ip_addr.sin_.sin_port = (htons)(port);

  if (NULL == (processor_connect_ = new(std::nothrow) TestProcessorConnect(new_proxy_mutex(),
      ip_addr.sa_, vc_number, multi_writer, (0 != max_connections ? true : false)))) {
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
      EXPECT_TRUE(ret);
    } else {
      for (int32_t i = 0; i < vc_number; ++i) {
        ObHRTime last_time = get_hrtime_internal();
        while ((NULL == processor_connect_->session_connect_[i]
                || NET_EVENT_OPEN != processor_connect_->session_connect_[i]->result_)
            && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(3)) { }
        if (NET_EVENT_OPEN != processor_connect_->session_connect_[i]->result_) {
          ret = false;
          ERROR_NET("connect %d connect failed", i);
          EXPECT_TRUE(ret);
          break;
        } else {
          INFO_NET("TEST", "connect %d connect succeed", i);
        }
      }
    }
  }
  return ret;
}

bool TestUnixNet::check_accept(const int32_t local_port, const int32_t vc_number,
    const bool multi_writer, const int64_t max_connections)
{
  bool ret = false;
  ObHRTime last_time = get_hrtime_internal();//wait for finish calling accept
  while (!processor_accept_->started_
      && (get_hrtime_internal() - last_time) <= HRTIME_SECONDS(2)) { }
  if (!processor_accept_->started_) {
    ERROR_NET("processor_accept_ start failed");
    EXPECT_TRUE(false);
  } else if (RESULT_NET_EVENT_LISTEN_SUCCEED != processor_accept_->session_accept_->result_) {
    ERROR_NET("processor_accept_ is cancelled %d", processor_accept_->session_accept_->result_);
    EXPECT_TRUE(false);
  } else if (!start_mutil_connect((uint16_t)local_port, vc_number, multi_writer, max_connections)) {
    ERROR_NET("start_mutil_connect failed");
  } else if (0 != max_connections) {
    ret = check_inactivitycop(vc_number, max_connections);
  } else if(!multi_writer) {
    ret = check_read_result(vc_number);
  } else {
    ret = check_write_result(vc_number);
  }
  return ret;
}

void TestUnixNet::common_accept(ObNetProcessor::ObAcceptOptions &options,
    const int32_t vc_number, const bool multi_writer, const int64_t max_connections)
{
  if (NULL == (processor_accept_ = new(std::nothrow) TestProcessorAccept(new_proxy_mutex(), options,
      vc_number, multi_writer, (0 != max_connections ? true : false)))) {
    ERROR_NET("failed to allocate memory for TestProcessorAccept");
    EXPECT_TRUE(false);
  } else {
    g_event_processor.schedule_imm(processor_accept_);
    test_passed_ = check_accept(options.local_port_, vc_number, multi_writer, max_connections);
  }
}

TEST_F(TestUnixNet, read_multi_vconnection_read)
{
  INFO_NET("TEST", "read_multi_vconnection_read");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 1;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;

  int32_t vc_number = 3;
  common_accept(options, vc_number, false);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNet, read_multi_vconnection_read_large)
{
  INFO_NET("TEST", "read_multi_vconnection_read_large");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 2;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;

  int32_t vc_number = 10;
  common_accept(options, vc_number, false);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNet, InactivityCop_keep_alive_lru_first_zero)
{
  INFO_NET("TEST", "InactivityCop_keep_alive_lru_first_zero");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 3;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;

  int32_t vc_number = 3;
  int64_t max_connections = g_event_start_threads ;
  common_accept(options, vc_number, false, max_connections);
  EXPECT_TRUE(test_passed_);
}

TEST_F(TestUnixNet, InactivityCop_keep_alive_lru_first_large)
{
  INFO_NET("TEST", "InactivityCop_keep_alive_lru_first_large");
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = TEST_PORT_START + 4;
  options.frequent_accept_ = false;
  options.f_callback_on_open_ = true;

  int32_t vc_number = 10;
  int64_t max_connections = g_event_start_threads ;

  //start update max_connections
  int64_t inactivity_timeout = 10;
  update_cop_config(inactivity_timeout, (int64_t)(vc_number * max_connections));

  common_accept(options, vc_number, false, max_connections);
  EXPECT_TRUE(test_passed_);
}


} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
//  system("rm -f test_unix_net.log*");
//  OB_LOGGER.set_file_name("test_unix_net.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::obproxy::init_g_net_processor();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

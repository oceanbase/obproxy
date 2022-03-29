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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY_TUNNEL
#include "proxy/mysql/ob_mysql_tunnel.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_debug_names.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define get_based_hrtime() (sm_->trans_state_.mysql_config_params_->enable_strict_stat_time_ ? get_hrtime_internal() : get_hrtime())

ObPacketAnalyzer::ObPacketAnalyzer()
    : producer_(NULL), request_analyzer_(NULL), resp_analyzer_(NULL),
      packet_reader_(NULL), server_response_(NULL), packet_type_(MYSQL_NONE),
      last_server_event_(VC_EVENT_NONE), skip_bytes_(0)
{
}

inline int ObPacketAnalyzer::process_content(bool &cmd_complete, bool &trans_complete, uint8_t &request_pkt_seq)
{
  int ret = OB_SUCCESS;

  if (MYSQL_REQUEST == packet_type_) {
    if (OB_FAIL(process_request_content(cmd_complete, trans_complete, request_pkt_seq))) {
      LOG_WARN("fail to process_request_content", K(ret));
    }
  } else if (MYSQL_RESPONSE == packet_type_) {
    if (OB_FAIL(process_response_content(cmd_complete, trans_complete))) {
      LOG_WARN("fail to process_response_content", K(ret));
    }
  } else {
    ret = OB_ERR_SYS;
    LOG_WARN("unsupported packet_type", K(packet_type_), K(ret));
  }
  return ret;
}

inline int ObPacketAnalyzer::process_request_content(bool &cmd_complete, bool &trans_complete, uint8_t &request_pkt_seq)
{
  int ret = OB_SUCCESS;
  cmd_complete = false;
  trans_complete = false;
  if (VC_EVENT_READ_COMPLETE == last_server_event_) {
    // if last_server_event_ is VC_EVENT_READ_COMPLETE,
    // this means the network layer reads the data you specify
    trans_complete = true;
  } else if (VC_EVENT_READ_READY == last_server_event_ && NULL != request_analyzer_) {
    // network layer return VC_EVENT_READ_READY
    // need check client request is finished
    if (OB_ISNULL(packet_reader_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("packet_reader is null", K(ret));
    } else  {
      int64_t data_size = packet_reader_->read_avail();
      if (data_size > 0) {
        if (OB_FAIL(request_analyzer_->is_request_finished(*packet_reader_, trans_complete))) {
          LOG_WARN("fail to analyze_request_is_finish", K(ret));
        } else {
          if (trans_complete) {
            // if request packet larger than 16MB, then we will received more than one packet,
            // and here should set request seq to the last packet's seq. (or the seq will mismatch,
            // especilly in compression protocol, we assume request seq == response seq,
            // when ok or err packet received)
            request_pkt_seq = request_analyzer_->get_packet_seq();
          }
        }
      } else {
        LOG_DEBUG("no need analyze request", K(data_size));
      }
    }
  }

  if (OB_SUCC(ret) && (NULL != packet_reader_) && OB_FAIL(packet_reader_->consume_all())) {
    LOG_WARN("fail to consume all", K(ret));
  }

  LOG_DEBUG("process_request_content",
            K_(packet_type), "vc_type", producer_->vc_type_, K(trans_complete));
  return ret;
}

inline int ObPacketAnalyzer::process_response_content(bool &cmd_complete, bool &trans_complete)
{
  int ret = OB_SUCCESS;

  cmd_complete = false;
  trans_complete = false;
  if (NULL != server_response_ && server_response_->get_analyze_result().is_resp_completed()) {
    // non-resultset protocol
    cmd_complete = true;
    trans_complete = server_response_->get_analyze_result().is_trans_completed();
    LOG_DEBUG("process_response_content, is_resp_completed = true",
              K_(packet_type), "vc_type", producer_->vc_type_, K(trans_complete));
  } else if (NULL != resp_analyzer_ && NULL != server_response_) {
    // resultset protocol
    int64_t data_size = packet_reader_->read_avail();

    if (data_size > 0) {
      if (OB_FAIL(resp_analyzer_->analyze_response(*packet_reader_, server_response_))) {
        LOG_WARN("fail to analyze response", K(ret));
      } else {
        trans_complete = server_response_->get_analyze_result().is_trans_completed();
        cmd_complete = server_response_->get_analyze_result().is_resp_completed();
        LOG_DEBUG("process_response_content", K_(packet_type),
                  K(data_size), "vc_type", producer_->vc_type_, K(cmd_complete), K(trans_complete),
                  "reserved_len", server_response_->get_analyze_result().get_reserved_len(), K(packet_reader_));
      }
    } else {
      LOG_DEBUG("no need analyze response", K(data_size));
    }
  }

  if (NULL != packet_reader_ && OB_SUCC(ret)) {
    if (OB_FAIL(packet_reader_->consume_all())) {
      LOG_WARN("fail to consume all", K(ret));
    }
  }

  return ret;
}

ObMysqlTunnelProducer::ObMysqlTunnelProducer()
    : consumer_list_(), self_consumer_(NULL),
      vc_(NULL), vc_handler_(NULL), read_vio_(NULL), read_buffer_(NULL),
      buffer_start_(NULL), vc_type_(MT_MYSQL_SERVER),
      init_bytes_done_(0), nbytes_(0), ntodo_(0), bytes_read_(0),
      handler_state_(0), memory_flow_control_count_(0),
      cpu_flow_control_count_(0), consumer_reenable_count_(0),
      num_consumers_(0), alive_(false), read_success_(false),
      own_iobuffer_(true), cost_time_(0), flow_control_source_(0), name_(NULL)
{
}

int64_t ObMysqlTunnelProducer::backlog(const int64_t limit)
{
  int64_t ret = 0;
  int64_t n = 0;
  ObIOBufferReader *r = NULL;
  ObMysqlTunnelProducer *dsp = NULL;

  // Calculate the total backlog, the # of bytes inside obproxy for this
  // producer. We go all the way through each chain to the ending sink and take
  // the maximum over those paths. Do need to be careful about loops which can occur.
  for (ObMysqlTunnelConsumer *c = consumer_list_.head_; NULL != c && ret < limit; c = c->link_.next_) {
    if (c->alive_ && NULL != c->write_vio_) {
      n = 0;

      if (MT_TRANSFORM == c->vc_type_) {
        if (NULL != c->vc_) {
          n += static_cast<ObTransformVCChain *>(c->vc_)->backlog(limit);
        }
      } else {
        r = c->write_vio_->get_reader();
        if (NULL != r) {
          n += r->read_avail();
        }
      }

      if (n >= limit) {
        ret = n;
      } else {
        if (!c->is_sink()) {
          dsp = c->self_producer_;
          if (NULL != dsp) {
            n += dsp->backlog();
          }
        }

        if (n >= limit) {
          ret = n;
        } else if (n > ret) {
          ret = n;
        }
      }
    }
  }

  if (ret < limit && NULL != packet_analyzer_.packet_reader_) {
    ret += packet_analyzer_.packet_reader_->read_avail();
  }

  return ret;
}

ObMysqlTunnelConsumer::ObMysqlTunnelConsumer()
    : link_(), producer_(NULL), self_producer_(NULL),
      vc_type_(MT_MYSQL_CLIENT), vc_(NULL), buffer_reader_(NULL),
      vc_handler_(NULL), write_vio_(NULL), skip_bytes_(0),
      bytes_written_(0), handler_state_(0), alive_(false),
      write_success_(false),  cost_time_(0), name_(NULL)
{
}

ObMysqlTunnel::ObMysqlTunnel()
    : ObContinuation(NULL), num_producers_(0),
      num_consumers_(0), sm_(NULL), last_handler_event_time_(0), active_(false)
{
}

inline ObMysqlTunnelProducer *ObMysqlTunnel::alloc_producer()
{
  ObMysqlTunnelProducer *ret = NULL;

  if (OB_LIKELY(num_producers_ < MAX_PRODUCERS)) {
    if (OB_LIKELY(NULL == producers_[0].vc_)) {
      ++num_producers_;
      ret = producers_;
    } else {
      for (int64_t i = 1; i < MAX_PRODUCERS && NULL == ret; ++i) {
        if (OB_LIKELY(NULL == producers_[i].vc_)) {
          ++num_producers_;
          ret = producers_ + i;
        }
      }
    }
  }

  return ret;
}

inline ObMysqlTunnelConsumer *ObMysqlTunnel::alloc_consumer()
{
  ObMysqlTunnelConsumer *ret = NULL;

  if (OB_LIKELY(num_consumers_ < MAX_CONSUMERS)) {
    if (OB_LIKELY(NULL == consumers_[0].vc_)) {
      ++num_consumers_;
      ret = consumers_;
    } else {
      for (int64_t i = 1; i < MAX_CONSUMERS && NULL == ret; ++i) {
        if (OB_LIKELY(NULL == consumers_[i].vc_)) {
          ++num_consumers_;
          ret = consumers_ + i;
        }
      }
    }
  }

  return ret;
}

// Adds a new producer to the tunnel
ObMysqlTunnelProducer *ObMysqlTunnel::add_producer(
    ObVConnection *vc, const int64_t nbytes_arg, ObIOBufferReader *reader_start,
    MysqlProducerHandler sm_handler, ObMysqlTunnelType vc_type, const char *name_arg,
    const bool own_iobuffer)
{
  int ret = OB_SUCCESS;
  ObMysqlTunnelProducer *p = NULL;
  int64_t read_avail = 0;
  int64_t ntodo = 0;

  if (OB_ISNULL(vc) || OB_ISNULL(reader_start) || OB_ISNULL(name_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_producer, invalid argument", K(vc), K(reader_start), K(name_arg), K(ret));
  } else {
    LOG_DEBUG("adding producer", K_(sm_->sm_id), K(name_arg), K(nbytes_arg));
    read_avail = reader_start->read_avail();
    if (nbytes_arg < 0) {
      ntodo = nbytes_arg;
    } else {
      // The byte count given us includes bytes
      // that already may be in the buffer.
      // ntodo represents the number of bytes
      // the tunneling mechanism needs to read
      // for the producer
      ntodo = nbytes_arg - read_avail;
      if (OB_UNLIKELY(ntodo < 0)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("add_producer, invalid producer ntodo",
                  K(nbytes_arg), K(read_avail), K(ntodo), K(ret));
      }
    }

    if (OB_UNLIKELY(OB_SUCCESS == ret && MYSQL_TUNNEL_STATIC_PRODUCER == vc && 0 != ntodo)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("add_producer, invalid static producer ntodo",
                K(nbytes_arg), K(read_avail), K(ntodo), K(ret));
    }

    if (OB_SUCC(ret) && OB_LIKELY(NULL != (p = alloc_producer()))) {
      p->vc_ = vc;
      p->nbytes_ = nbytes_arg;
      p->buffer_start_ = reader_start;
      p->read_buffer_ = reader_start->mbuf_;
      p->vc_handler_ = sm_handler;
      p->vc_type_ = vc_type;
      p->name_ = name_arg;
      p->own_iobuffer_ = own_iobuffer;

      p->memory_flow_control_count_ = 0;
      p->cpu_flow_control_count_ = 0;
      p->consumer_reenable_count_ = 0;

      p->init_bytes_done_ = read_avail;
      p->ntodo_ = ntodo;

      p->cost_time_ = 0;

      // We are static, the producer is never "alive"
      // It just has data in the buffer
      if (MYSQL_TUNNEL_STATIC_PRODUCER == vc) {
        p->alive_ = false;
        p->read_success_ = true;
      } else {
        p->alive_ = true;
      }
    }
  }

  return p;
}

// Adds a new consumer to the tunnel.  The producer must
// be specified and already added to the tunnel. Attaches
// the new consumer to the entry for the existing producer
//
// Returns true if the consumer successfully added.
// Returns false if the consumer was not added because the
// source failed
ObMysqlTunnelConsumer *ObMysqlTunnel::add_consumer(
    ObVConnection *vc, ObVConnection *producer, MysqlConsumerHandler sm_handler,
    ObMysqlTunnelType vc_type, const char *name_arg, const int64_t skip_bytes)
{
  int ret = OB_SUCCESS;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(vc) || OB_ISNULL(producer) || OB_ISNULL(name_arg) || OB_UNLIKELY(skip_bytes < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_consumer, invalid argument", K(vc), K(producer), K(name_arg), K(skip_bytes), K(ret));
  } else if (OB_ISNULL(p = get_producer(producer))) {
    ret = OB_ERR_SYS;
    LOG_WARN("add_consumer, failed to get producer", K(producer), K(ret));
  } else {
    LOG_DEBUG("adding consumer", K_(sm_->sm_id), K(name_arg));

    // Check to see if the producer terminated
    // without sending all of its data
    if (!p->alive_ && !p->read_success_) {
      LOG_DEBUG("add_consumer, consumer not added due to producer failure",
                K_(sm_->sm_id), K(name_arg), K_(p->alive), K_(p->read_success));
    } else if (OB_LIKELY(NULL != (c = alloc_consumer()))) {
      c->producer_ = p;
      c->vc_ = vc;
      c->alive_ = true;
      c->skip_bytes_ = skip_bytes;
      c->vc_handler_ = sm_handler;
      c->vc_type_ = vc_type;
      c->name_ = name_arg;
      c->cost_time_ = 0;

      // Register the consumer with the producer
      p->consumer_list_.push(c);
      ++(p->num_consumers_);
    }
  }

  return c;
}

// Makes the tunnel go
int ObMysqlTunnel::tunnel_run(ObMysqlTunnelProducer *p_arg)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("tunnel_run started", K_(sm_->sm_id), K(p_arg));

  if (OB_LIKELY(NULL != p_arg)) {
    if (OB_FAIL(producer_run(*p_arg))) {
      LOG_WARN("failed to run producer", K(ret));
    }
  } else {
    ObMysqlTunnelProducer *p = NULL;

    if (active_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tunnel_run, rerun active tunnel", K_(active));
    } else {
      for (int64_t i = 0; i < MAX_PRODUCERS && OB_SUCC(ret); ++i) {
        p = producers_ + i;
        if (NULL != p->vc_ && (p->alive_ || (MT_STATIC == p->vc_type_ && NULL != p->buffer_start_))) {
          ret = producer_run(*p);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to run producer", K(ret));
          }
        }
      }
    }
  }

  // It is possible that there was nothing to do
  // due to a all transfers being zero length
  // If that is the case, call the state machine
  // back to say we are done
  if (OB_UNLIKELY(!is_tunnel_alive())) {
    active_ = false;
    sm_->handle_event(MYSQL_TUNNEL_EVENT_DONE, this);
    ret = OB_SUCCESS; // tunnel complete
  }
  return ret;
}

int ObMysqlTunnel::producer_run(ObMysqlTunnelProducer &p)
{
  int ret = OB_SUCCESS;
  int64_t consumer_n = 0;
  int64_t producer_n = 0;
  int64_t c_write = 0;
  ObMysqlClientSession *client_vc = NULL;

  if (OB_ISNULL(p.vc_) || OB_UNLIKELY(num_producers_ <= 0)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("producer_run, invalid internal state", K_(p.vc), K_(num_producers));
  } else {
    active_ = true;

    if (p.nbytes_ >= 0) {
      consumer_n = p.nbytes_;
      producer_n = p.ntodo_;
    } else {
      producer_n = INT64_MAX;
      consumer_n = producer_n;
    }

    if (OB_UNLIKELY(producer_n < 0)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid producer ntodo", K(producer_n));
    } else {
      LOG_DEBUG("producer_run", K_(sm_->sm_id), K(consumer_n), K(producer_n),
                "this_thread", this_ethread());
    }
  }

  last_handler_event_time_ = get_based_hrtime();

  // Do the IO on the consumers first so data doesn't disappear out from under the tunnel
  for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c && OB_SUCC(ret); c = c->link_.next_) {
    c_write = consumer_n;

    // Create a reader for each consumer.  The reader allows
    // us to implement skip bytes
    if (OB_ISNULL(c->buffer_reader_ = p.buffer_start_->clone())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("failed to clone buffer reader", K(ret));
    } else if (c->skip_bytes_ > 0) {
      // Consume bytes of the reader if we skipping bytes
      if (c->skip_bytes_ > c->buffer_reader_->read_avail()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("skip bytes is more than read avail bytes",
                  K_(c->skip_bytes), "read_avail", c->buffer_reader_->read_avail());
      } else {
        // - if we don't know the length leave it at
        // INT64_MAX or else the cache may bounce the write
        // because it thinks the document is too big. INT64_MAX
        // is a special case for the max document size code
        // in the cache
        if (INT64_MAX != c_write) {
          c_write -= c->skip_bytes_;
        }
        if (OB_FAIL(c->buffer_reader_->consume(c->skip_bytes_))) {
          PROXY_TXN_LOG(WARN, "fail to consume ", K(c->skip_bytes_), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(c_write < 0)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("consumer to write length < 0", K(c_write));
      } else if (OB_UNLIKELY(0 == c_write)) {
        // Nothing to do, call back the cleanup handlers
        c->write_vio_ = NULL;
        consumer_handler(VC_EVENT_WRITE_COMPLETE, *c);
      } else {
        // In the client half close case, all the data that will be sent
        // from the client is already in the buffer.  Go ahead and set
        // the amount to read since we know it.  We will forward the FIN
        // to the server on VC_EVENT_WRITE_COMPLETE.
        if (MT_MYSQL_CLIENT == p.vc_type_) {
          client_vc = static_cast<ObMysqlClientSession *>(p.vc_);
          if (client_vc->get_half_close_flag()) {
            c_write = c->buffer_reader_->read_avail();
            p.alive_ = false;
            p.handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SUCCESS;
          }
        }

        c->write_vio_ = c->vc_->do_io_write(this, c_write, c->buffer_reader_);
        if (OB_ISNULL(c->write_vio_)) {
          ret = OB_ERR_SYS;
          LOG_WARN("failed to do_io_write", K(ret));
        } else {
          LOG_DEBUG("producer_run", K_(sm_->sm_id), K(c), K_(c->write_vio_->nbytes),
                    K_(c->write_vio_->ndone));
        }
      }
    }
  }

  if (OB_SUCC(ret) && MYSQL_TUNNEL_STATIC_PRODUCER != p.vc_) {//internal request don't come into this
    if (p.packet_analyzer_.skip_bytes_ > 0
        && NULL != p.packet_analyzer_.packet_reader_
        && p.packet_analyzer_.packet_reader_->read_avail() >= p.packet_analyzer_.skip_bytes_) {
      p.packet_analyzer_.packet_reader_->consume(p.packet_analyzer_.skip_bytes_);
    }
    producer_handler(VC_EVENT_READ_READY, p);

    if (p.alive_) {
      if (0 == producer_n) {
        // Everything is already in the buffer so mark the producer as done. We need to notify
        // state machine that everything is done. We use a special event to say the producers is
        // done but we didn't do anything
        p.alive_ = false;
        p.read_success_ = true;
        LOG_DEBUG("producer_run, producer already done", K_(sm_->sm_id));
        producer_handler(MYSQL_TUNNEL_EVENT_PRECOMPLETE, p);
      } else {
        p.read_vio_ = p.vc_->do_io_read(this, producer_n, p.read_buffer_);
        if (OB_ISNULL(p.read_vio_)) {
          ret = OB_ERR_SYS;
          LOG_WARN("failed to do_io_read", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // Now that the tunnel has started, we must remove producer's reader so
    // that it doesn't act like a buffer guard
    p.buffer_start_->dealloc();
    p.buffer_start_ = NULL;
  }

  return ret;
}

// Handles events from producers. It calls the handlers if appropriate
// and then translates the event we got into a suitable // event
inline int ObMysqlTunnel::producer_handler_packet(int event, ObMysqlTunnelProducer &p)
{
  int ret = event;
  bool cmd_complete = false;
  bool trans_complete = false;

  LOG_DEBUG("producer_handler_packet", K_(sm_->sm_id), K_(p.name),
            "event", ObMysqlDebugNames::get_event_name(event));

  // We only interested in translating certain events
  switch (event) {
    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE:
    case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_EOS: {
      p.packet_analyzer_.last_server_event_ = event;

      // If we couldn't understand the encoding, return an error
      ObHRTime packet_analyze_begin = get_based_hrtime();
      uint8_t &request_pkt_seq = sm_->trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_;
      if (OB_UNLIKELY(OB_SUCCESS != p.packet_analyzer_.process_content(cmd_complete, trans_complete, request_pkt_seq))) {
        LOG_WARN("process response content analyze response error",
                 K_(sm_->sm_id), K_(p.name), K(cmd_complete), K(trans_complete));
        // FIX ME: we return EOS here since it will cause the
        // the client to be reenabled. ERROR makes more sense
        // but no reenables follow
        ret = VC_EVENT_EOS;
        break;
      }

      sm_->cmd_time_stats_.server_response_analyze_time_ +=
        milestone_diff(packet_analyze_begin, get_based_hrtime());

      // For resultset protocol, there is an extra ok packet at the tail. After analyze the
      // resultset response, we known the length of the extra ok packet, we tell all the consumers
      // not to consume the extra ok packet.
      // If it doesn't receive the last eof or error packet and extra ok packet, the consumer can't
      // consume the last eof or error packet. So if the extra ok packet isn't received, the reserved_len
      // isn't 0, we can hold the last eof or error packet.
      if (NULL != p.packet_analyzer_.server_response_) {
        int64_t reserved_size = p.packet_analyzer_.server_response_->get_analyze_result().get_reserved_len();
        for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c; c = c->link_.next_) {
          LOG_DEBUG("reader avail size", "size", c->buffer_reader_->read_avail());
          if (c->alive_) {
            c->buffer_reader_->reserved_size_ = reserved_size;
          }
          LOG_DEBUG("after reserved, reader avail size", "size", c->buffer_reader_->read_avail());
          if (OB_UNLIKELY(reserved_size > 0) && OB_UNLIKELY(c->buffer_reader_->read_avail() < 0)) {
            LOG_ERROR("unexpected read_avail size, BUG!", K(reserved_size),
                      "read avail size", c->buffer_reader_->read_avail());
            // set reserved_size_ to INT64_MAX, so we can reserver all data then disconnect
            c->buffer_reader_->reserved_size_ = INT64_MAX;
            ret = VC_EVENT_ERROR;
            break;
          }
        }
      }

      if (VC_EVENT_READ_READY == event) {
        if (trans_complete) {
          ret = VC_EVENT_READ_COMPLETE;
        } else if (cmd_complete) {
          ret = MYSQL_TUNNEL_EVENT_CMD_COMPLETE;
        }
      }
      break;
    }

    default:
      break;
  }

  return ret;
}

// Handles events from producers.
//
// If the event is interesting only to the tunnel, this
// handler takes all necessary actions and returns false
// If the event is interesting to the state_machine,
// it calls back the state machine and returns true
bool ObMysqlTunnel::producer_handler(int event, ObMysqlTunnelProducer &p)
{
  ObMysqlTunnelConsumer *c = NULL;
  MysqlProducerHandler jump_point = NULL;
  bool sm_callback = false;

  LOG_DEBUG("producer_handler", K_(sm_->sm_id), K_(p.name),
            "event", ObMysqlDebugNames::get_event_name(event));

  ObMysqlClientSession *client_vc = NULL;
  event = producer_handler_packet(event, p);
  switch (event) {
    case VC_EVENT_READ_READY:
      // Data read from producer, reenable consumers
      for (c = p.consumer_list_.head_; NULL != c; c = c->link_.next_) {
        if (c->alive_) {
          if (MT_MYSQL_CLIENT == c->vc_type_) {
            client_vc = static_cast<ObMysqlClientSession *>(c->vc_);
            if (NULL != client_vc) {
              client_vc->set_net_write_timeout();
            }
          }

          if (p.is_source()) {
            if (p.is_throttled()) {
              LOG_DEBUG("producer is throttled, don't reenable");
            } else {
              c->write_vio_->reenable();
            }
          } else {
            c->write_vio_->reenable();
          }
        }
      }
      break;

    case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
      // the producer had finished before the tunnel
      // started so just call the state machine back
      // We don't need to reenable since the consumers
      // were just activated.  Likewise, we can't be
      // done because the consumer couldn't have
      // called us back yet
      p.bytes_read_ = 0;
      jump_point = p.vc_handler_;
      if (OB_ISNULL(jump_point)) {
        LOG_ERROR("producer vc handler is NULL");
      } else if (OB_SUCCESS != (sm_->*jump_point)(event, p)) {
        LOG_WARN("failed to call producer vc handler");
      }
      sm_callback = true;
      p.update_state_if_not_set(MYSQL_SM_REQUEST_TRANSFER_SUCCESS);
      break;

    case MYSQL_TUNNEL_EVENT_CMD_COMPLETE:
    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_EOS:
      // The producer completed
      p.alive_ = false;
      if (NULL != p.read_vio_) {
        p.bytes_read_ = p.read_vio_->ndone_;
      } else {
        p.bytes_read_ = 0;
      }

      // no request/response data to read, reset read trigger and avoid unnecessary reading
      if (MT_MYSQL_SERVER == p.vc_type_) {
        static_cast<ObUnixNetVConnection *>(static_cast<ObMysqlServerSession *>(p.vc_)->get_netvc())->reset_read_trigger();
        int64_t active_count = static_cast<ObUnixNetVConnection *>(static_cast<ObMysqlServerSession *>(p.vc_)->get_netvc())->read_.active_count_;
        if (active_count > 0) {
          MYSQL_SUM_DYN_STAT(TOTAL_SERVER_RESPONSE_REREAD_COUNT, (active_count - 1));
        }
      } else if (MT_MYSQL_CLIENT == p.vc_type_ && !static_cast<ObMysqlClientSession *>(p.vc_)->is_proxy_mysql_client_) {
        static_cast<ObUnixNetVConnection *>(static_cast<ObMysqlClientSession *>(p.vc_)->get_netvc())->reset_read_trigger();
      }

      // callback the SM to notify of completion
      // Note: we need to callback the SM before
      // reenabling the consumers as the reenable may
      // make the data visible to the consumer and
      // initiate async I/O operation. The SM needs to
      // set how much I/O to do before async I/O is
      // initiated
      jump_point = p.vc_handler_;
      p.cost_time_ += (get_based_hrtime() - last_handler_event_time_);
      if (OB_ISNULL(jump_point)) {
        LOG_ERROR("producer vc handler is NULL");
      } else if (OB_SUCCESS != (sm_->*jump_point)(event, p)) {
        LOG_WARN("failed to call producer vc handler");
      }
      sm_callback = true;
      p.update_state_if_not_set(MYSQL_SM_REQUEST_TRANSFER_SUCCESS);

      // Data read from producer, reenable consumers
      for (c = p.consumer_list_.head_; NULL != c; c = c->link_.next_) {
        if (c->alive_) {
          if (MT_MYSQL_CLIENT == c->vc_type_) {
            client_vc = static_cast<ObMysqlClientSession *>(c->vc_);
            if (NULL != client_vc) {
              client_vc->set_net_write_timeout();
            }
          }
          c->write_vio_->reenable();
        }
      }
      break;

    case VC_EVENT_ERROR:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case MYSQL_TUNNEL_EVENT_CONSUMER_DETACH:
      p.alive_ = false;
      p.bytes_read_ = p.read_vio_->ndone_;
      // Interesting tunnel event, call SM
      jump_point = p.vc_handler_;
      p.cost_time_ += (get_based_hrtime() - last_handler_event_time_);
      if (OB_ISNULL(jump_point)) {
        LOG_ERROR("producer vc handler is NULL");
      } else if (OB_SUCCESS != (sm_->*jump_point)(event, p)) {
        LOG_WARN("failed to call producer vc handler");
      }
      sm_callback = true;
      p.update_state_if_not_set(MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL);
      break;

    case VC_EVENT_WRITE_READY:
    case VC_EVENT_WRITE_COMPLETE:
    default:
      // Producers should not get these events
      LOG_ERROR("producer_handler, Unknown event", "event", ObMysqlDebugNames::get_event_name(event));
      break;
  }

  return sm_callback;
}

void ObMysqlTunnel::consumer_reenable(ObMysqlTunnelConsumer &c)
{
  ObMysqlTunnelProducer *p = c.producer_;
  ObMysqlTunnelProducer *srcp = NULL;
  ObMysqlConfigParams *params = sm_->trans_state_.mysql_config_params_;

  LOG_DEBUG("consumer_reenable", K(p), K(p->alive_), K(p->read_buffer_->write_avail()), K(params));
  if (OB_LIKELY(NULL != p) && p->alive_
      && OB_LIKELY(NULL != p->read_buffer_)
      && p->read_buffer_->write_avail() >= 0
      && OB_LIKELY(NULL != params)) {
    // Only do flow control if enabled and the producer is an external
    // source. Otherwise disable by making the backlog zero. Because
    // the backlog short cuts quit when the value is equal (or
    // greater) to the target, we use strict comparison only for
    // checking low water, otherwise the flow control can stall out.
    int64_t backlog = (params->enable_flow_control_ && p->is_source())
        ? p->backlog(params->flow_high_water_mark_) : 0;

    LOG_DEBUG("consumer_reenable", K(backlog), "enable_flow_control",
              params->enable_flow_control_, "flow_high_water_mark",
              params->flow_high_water_mark_, K(p), K_(p->name), K(p->is_source()),
              K(p->vc_type_), K(p->flow_control_source_));
    if (backlog > params->flow_high_water_mark_) {
      LOG_DEBUG("Throttle", K(p), K(backlog), "producer_backlog", p->backlog());
      ++(p->memory_flow_control_count_);
      p->throttle(); // p becomes srcp for future calls to this method
    } else {
      srcp = p->flow_control_source_;
      if (NULL != srcp && srcp->alive_ && c.is_sink()) {
        // Check if backlog is below low water - note we need to check
        // against the source producer, not necessarily the producer
        // for this consumer. We don't have to recompute the backlog
        // if they are the same because we know low water <= high
        // water so the value is sufficiently accurate.
        if (srcp != p) {
          backlog = srcp->backlog(params->flow_low_water_mark_);
        }
        LOG_DEBUG("handle srcp", K(backlog), K(srcp), K_(srcp->name),
                  "flow_low_water_mark", params->flow_low_water_mark_);

        if (backlog <= params->flow_low_water_mark_) {
          // if backlog less than low water mark,
          // 1. inform producer to read data(srcp->read_vio_->reenable());
          // 2. infrom consumer to consume data(producer_handler(VC_EVENT_READ_READY, *srcp))
          LOG_DEBUG("Unthrottle", K(p), K(backlog), "producer_backlog", p->backlog());
          srcp->unthrottle();
          srcp->read_vio_->reenable();
          // Kick source producer to get flow ... well, flowing.
          producer_handler(VC_EVENT_READ_READY, *srcp);
        } else {
          // We can stall for small thresholds on network sinks because this event happens
          // before the actual socket write. So we trap for the buffer becoming empty to
          // make sure we get an event to unthrottle after the write.
          if (MT_MYSQL_CLIENT == c.vc_type_) {
            ObNetVConnection *netvc = dynamic_cast<ObNetVConnection *>(c.write_vio_->vc_server_);
            if (OB_LIKELY(NULL != netvc)) {// really, this should always be true.
              netvc->trap_write_buffer_empty();
            }
          }
        }
      }

      if (MT_MYSQL_SERVER == p->vc_type_) {
        ++(p->consumer_reenable_count_);
        ObHRTime atimeout_in = 0;

        if (params->enable_flow_control_) {
          int64_t local_thread_queue_size = get_local_thread_queue_size();
          if (p->consumer_reenable_count_ >= params->flow_consumer_reenable_threshold_ &&
              local_thread_queue_size >= params->flow_event_queue_threshold_) {
            atimeout_in = CONSUMER_REENABLE_RETRY_TIME;
            ++(p->cpu_flow_control_count_);
            LOG_DEBUG("consumer reenable",
                      K(atimeout_in), K_(p->consumer_reenable_count),
                      K(local_thread_queue_size), K_(p->name));
          }
        }
        p->read_vio_->reenable_in(atimeout_in);
      } else {
        p->read_vio_->reenable();
      }
    }
  }
}

// Handles events from consumers.
//
// If the event is interesting only to the tunnel, this
// handler takes all necessary actions and returns false
// If the event is interesting to the state_machine,
// it calls back the state machine and returns true
bool ObMysqlTunnel::consumer_handler(int event, ObMysqlTunnelConsumer &c)
{
  bool sm_callback = false;
  MysqlConsumerHandler jump_point = NULL;
  ObMysqlTunnelProducer *p = c.producer_;

  LOG_DEBUG("consumer_handler", K_(sm_->sm_id), K_(c.name),
            "event", ObMysqlDebugNames::get_event_name(event));

  if (OB_ISNULL(p) || OB_UNLIKELY(!c.alive_) || OB_ISNULL(c.buffer_reader_)) {
    LOG_ERROR("consumer_handler, invalid internal tunnel state",
             K(p), K_(c.alive), K_(c.buffer_reader));
  }

  ObMysqlClientSession *client_vc = NULL;
  if (MT_MYSQL_CLIENT == c.vc_type_) {
     client_vc = static_cast<ObMysqlClientSession *>(c.vc_);
     // cancel net_write_timeout, set wait_timeout
     if (NULL != client_vc) {
       client_vc->set_wait_timeout();
     }
  }

  switch (event) {
    case VC_EVENT_WRITE_READY:
      consumer_reenable(c);
      break;

    case VC_EVENT_WRITE_COMPLETE:
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT: {
      c.alive_ = false;
      c.bytes_written_ = c.write_vio_ ? c.write_vio_->ndone_ : 0;

      // Interesting tunnel event, call SM
      jump_point = c.vc_handler_;
      c.cost_time_ += (get_based_hrtime() - last_handler_event_time_);
      if (OB_ISNULL(jump_point)) {
        LOG_ERROR("consumer vc handler is NULL");
      } else if (OB_SUCCESS != (sm_->*jump_point)(event, c)) {
        LOG_WARN("failed to call consumer vc handler");
      }
      sm_callback = true;

      // Make sure the handler_state is set
      // Necessary for post tunnel end processing
      if (0 == p->handler_state_) {
        if (VC_EVENT_WRITE_COMPLETE == event) {
          p->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SUCCESS;
        } else if (MT_MYSQL_SERVER == c.vc_type_) {
          p->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL;
        } else if (MT_MYSQL_CLIENT == c.vc_type_) {
          p->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL;
        }
      }

      // when VC_EVENT_ERROR, we should set state to MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL anyway,
      // for example when compress or decompress error.
      if (VC_EVENT_ERROR == event) {
        p->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL;
      }

      if (OB_NOT_NULL(c.buffer_reader_)) {
        int64_t reserved_size = c.buffer_reader_->reserved_size_;
        if (reserved_size > 0) {
          c.buffer_reader_->reserved_size_ = 0;
          int ret = OB_SUCCESS;
          if (OB_FAIL(c.buffer_reader_->consume(reserved_size))) {
            LOG_WARN("fail to consume ", K(reserved_size), K(ret));
          }
        }
        // Deallocate the reader after calling back the sm
        // because buffer problems are easier to debug
        // in the sm when the reader is still valid
        c.buffer_reader_->dealloc();
        c.buffer_reader_ = NULL;
      }

      // Since we removed a consumer, it may now be
      // possbile to put more stuff in the buffer
      // Note: we reenable only after calling back
      // the SM since the reenabling has the side effect
      // updating the buffer state for the ObVConnection
      // that is being reenabled
      if (p->alive_ && NULL != p->read_vio_
#ifndef LAZY_BUF_ALLOC
          && OB_UNLIKELY(NULL != p->read_buffer_)
          && p->read_buffer_->write_avail() > 0
#endif
         ) {
        if (p->is_throttled()) {
          consumer_reenable(c);
        } else {
          p->read_vio_->reenable();
        }
      }

      // I don't think this happens but we'll leave a debug trap
      // here just in case.
      if (p->is_throttled()) {
        LOG_DEBUG("producer throttled", "event", ObMysqlDebugNames::get_event_name(event), K(p));
      }
      break;
    }

    case VC_EVENT_READ_READY:
    case VC_EVENT_READ_COMPLETE:
    default:
      // Consumers should not get these events
      LOG_ERROR("consumer_handler, Unknown event", "event", ObMysqlDebugNames::get_event_name(event));
      break;
  }

  return sm_callback;
}

// Abort the producer and everyone still alive
// downstream of the producer
void ObMysqlTunnel::chain_abort_all(ObMysqlTunnelProducer &p)
{
  ObMysqlTunnelProducer *selfp = NULL;

  for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c; c = c->link_.next_) {
    if (c->alive_) {
      c->alive_ = false;
      c->write_vio_ = NULL;
      if (NULL != c->vc_) {
        c->vc_->do_io_close(EMYSQL_ERROR);
      }
    }

    if (NULL != c->self_producer_) {
      // Must snip the link before recursively
      // freeing to avoid looks introduced by
      // blind tunneling
      selfp = c->self_producer_;
      c->self_producer_ = NULL;
      if (NULL != selfp) {
        chain_abort_all(*selfp);
      }
    }
  }

  if (p.alive_) {
    p.alive_ = false;
    p.bytes_read_ = p.read_vio_->ndone_;
    if (NULL != p.self_consumer_) {
      p.self_consumer_->alive_ = false;
    }
    p.read_vio_ = NULL;
    if (NULL != p.vc_) {
      p.vc_->do_io_close(EMYSQL_ERROR);
    }
  }
}

// Internal function for finishing all consumers. Takes
// chain argument about where to finish just immediate
// consumer or all those downstream
int ObMysqlTunnel::finish_all_internal(ObMysqlTunnelProducer &p, const bool chain)
{
  int ret = OB_SUCCESS;
  int64_t total_bytes = 0;

  if (OB_UNLIKELY(p.alive_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("finish_all_internal, invalid alive producer", K_(p.alive));
  }

  for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c && OB_SUCC(ret); c = c->link_.next_) {
    if (c->alive_) {
      if (OB_ISNULL(c->write_vio_) || OB_ISNULL(c->buffer_reader_)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("finish_all_internal, invalid member variables", K_(c->write_vio), K_(c->buffer_reader));
      } else {
        total_bytes = p.bytes_read_ + p.init_bytes_done_;
        c->write_vio_->nbytes_ = total_bytes - c->skip_bytes_ - c->buffer_reader_->reserved_size_;
        LOG_DEBUG("finish_all_internal", K(&p), K(p.bytes_read_), K(p.init_bytes_done_), K(total_bytes),
                  K(c->skip_bytes_), K(c->buffer_reader_->reserved_size_), K(c->write_vio_->nbytes_));

        if (c->write_vio_->nbytes_ < 0) {
          ret = OB_ERR_SYS;
          LOG_ERROR("finish_all_internal, Incorrect nbytes",
                    K_(c->write_vio_->nbytes), K(total_bytes), K_(c->skip_bytes),
                    K_(c->buffer_reader_->reserved_size));
        } else if (chain && NULL != c->self_producer_
            && OB_FAIL(chain_finish_all(*c->self_producer_))) {
          LOG_WARN("failed to chain_finish_all", K(ret));
        } else {
          // The IO Core will not call us back if there
          // is nothing to do. Check to see if there is
          // nothing to do and take the appripriate
          // action
          if (c->write_vio_->nbytes_ == c->write_vio_->ndone_) {
            consumer_handler(VC_EVENT_WRITE_COMPLETE, *c);
          }
        }
      }

      if (OB_FAIL(ret)) {
        consumer_handler(VC_EVENT_ERROR, *c);
      }
    }
  }

  return ret;
}

// Main handler for the tunnel. Vectors events
// based on whether they are from consumers or
// producers
int ObMysqlTunnel::main_handler(int event, void *data)
{
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;
  bool sm_callback = false;
  int ret = EVENT_CONT;
  int64_t current_time = 0;

  LOG_DEBUG("mysql tunnel main_handler",
            K_(sm_->sm_id), "event", ObMysqlDebugNames::get_event_name(event), K(data));

  if (OB_UNLIKELY(MYSQL_SM_MAGIC_ALIVE != sm_->magic_)) {
    LOG_WARN("failed to check sm magic", K_(sm_->magic), "expected", MYSQL_SM_MAGIC_ALIVE);
  }

  // Find the appropriate entry
  if (NULL != (p = get_producer(reinterpret_cast<ObVIO *>(data)))) {
    sm_callback = producer_handler(event, *p);
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_ && !sm_callback)) {
      current_time = get_based_hrtime();
      p->cost_time_ += (current_time - last_handler_event_time_);
      last_handler_event_time_ = current_time;
    }
  } else {
    if (OB_LIKELY(NULL != (c = get_consumer(reinterpret_cast<ObVIO *>(data))))) {
      if (c->write_vio_ != data) {
        LOG_WARN("failed to check vio, data must equal to write vio", K_(c->write_vio), K(data));
      }
      sm_callback = consumer_handler(event, *c);
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_ && !sm_callback)) {
        current_time = get_based_hrtime();
        c->cost_time_ += (current_time - last_handler_event_time_);
        last_handler_event_time_ = current_time;
      }
    } else {
      LOG_WARN("ObMysqlTunnel::main_handler, unknown data",
               "event", ObMysqlDebugNames::get_event_name(event), K(data), K(this),
               K_(num_producers), K_(num_consumers));
    }
  }

  // We called a vc handler, the tunnel might be finished.
  // Check to see if there are any remaining VConnections
  // alive. If not, notify the state machine
  // sm_callback variable is only for decreasing call times of is_tunnel_alive()
  if (sm_callback && !is_tunnel_alive()) {
    if (active_) {
      active_ = false;
      sm_->handle_event(MYSQL_TUNNEL_EVENT_DONE, this);
    }
    ret = EVENT_DONE;
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

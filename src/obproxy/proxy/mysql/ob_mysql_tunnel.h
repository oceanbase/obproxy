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

#ifndef OBPROXY_MYSQL_TUNNEL_H
#define OBPROXY_MYSQL_TUNNEL_H

#include "iocore/eventsystem/ob_vconnection.h"
#include "proxy/mysqllib/ob_mysql_transaction_analyzer.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define MYSQL_TUNNEL_EVENT_DONE             (MYSQL_TUNNEL_EVENTS_START + 1)
#define MYSQL_TUNNEL_EVENT_PRECOMPLETE      (MYSQL_TUNNEL_EVENTS_START + 2)
#define MYSQL_TUNNEL_EVENT_CONSUMER_DETACH  (MYSQL_TUNNEL_EVENTS_START + 3)
#define MYSQL_TUNNEL_EVENT_CMD_COMPLETE     (MYSQL_TUNNEL_EVENTS_START + 4)

#define MYSQL_TUNNEL_STATIC_PRODUCER  (ObVConnection*)!0

struct ObMysqlTunnelProducer;
class ObMysqlSM;
typedef int (ObMysqlSM::*MysqlSMHandler)(int event, void *data);

struct ObMysqlTunnelConsumer;
struct ObMysqlTunnelProducer;
typedef int (ObMysqlSM::*MysqlProducerHandler)(int event, ObMysqlTunnelProducer &p);
typedef int (ObMysqlSM::*MysqlConsumerHandler)(int event, ObMysqlTunnelConsumer &c);

enum ObMysqlTunnelType
{
  MT_MYSQL_SERVER,
  MT_MYSQL_CLIENT,
  MT_TRANSFORM,
  MT_STATIC
};

enum ObMysqlPacketType
{
  MYSQL_REQUEST,
  MYSQL_RESPONSE,
  MYSQL_NONE
};

struct ObPacketAnalyzer
{
  ObPacketAnalyzer();
  ~ObPacketAnalyzer();

  // Returns true if complete, false otherwise
  int process_content(bool &cmd_complete, bool &trans_complete, uint8_t &request_pkt_seq, obmysql::ObMySQLCmd cmd);

  int process_response_content(bool &cmd_complete, bool &trans_complete);
  int process_request_content(bool &cmd_complete, bool &trans_complete, uint8_t &request_pkt_seq, obmysql::ObMySQLCmd cmd);
  ObMysqlTunnelProducer *producer_;

  ObMysqlRequestAnalyzer *request_analyzer_;
  ObIMysqlRespAnalyzer *resp_analyzer_;

  event::ObIOBufferReader *packet_reader_;

  ObMysqlResp *server_response_;
  ObMysqlPacketType packet_type_;
  int last_server_event_;
  int64_t skip_bytes_;
  ObProtocolDiagnosis *protocol_diagnosis_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPacketAnalyzer);
};

struct ObMysqlTunnelConsumer
{
  ObMysqlTunnelConsumer();
  ~ObMysqlTunnelConsumer() { }

  // Check if this is a sink (final data destination).
  // @return true if data exits the obproxy process at this consumer.
  bool is_sink() const;

  LINK(ObMysqlTunnelConsumer, link_);
  ObMysqlTunnelProducer *producer_;
  ObMysqlTunnelProducer *self_producer_;

  ObMysqlTunnelType vc_type_;
  event::ObVConnection *vc_;
  event::ObIOBufferReader *buffer_reader_;
  MysqlConsumerHandler vc_handler_;
  event::ObVIO *write_vio_;

  int64_t skip_bytes_;               // bytes to skip at beginning of stream
  int64_t bytes_written_;            // total bytes written to the vc
  int handler_state_;                // state used the handlers

  bool alive_;
  bool write_success_;

  int64_t cost_time_;
  const char *name_;
};

struct ObMysqlTunnelProducer
{
  ObMysqlTunnelProducer();
  ~ObMysqlTunnelProducer() { }

  // Get the largest number of bytes any consumer has not consumed.
  // Use limit if you only need to check if the backlog is at least limit.
  // @return The actual backlog or a number at least limit.
  int64_t backlog(const int64_t limit = INT64_MAX);

  // Check if producer is original (to obproxy) source of data.
  // @return true if this producer is the source of bytes from outside obproxy.
  bool is_source() const;

  // Throttle the flow.
  void throttle();

  // Unthrottle the flow.
  void unthrottle();

  // Check throttled state.
  bool is_throttled() const;

  // Check ever been controlled
  bool is_flow_controlled() const;

  // Update the handler_state member if it is still 0
  void update_state_if_not_set(int new_handler_state);

  // Set the flow control source producer for the flow.
  // This sets the value for this producer and all downstream producers.
  // @note This is the implementation for throttle and unthrottle.
  // @see throttle
  // @see unthrottle
  void set_throttle_src(ObMysqlTunnelProducer *srcp); //Source producer of flow.

  int set_request_packet_analyzer(ObMysqlPacketType packet_type,
                                  ObMysqlRequestAnalyzer *analyzer,
                                  ObProtocolDiagnosis *protocol_diagnosis);

  int set_response_packet_analyzer(const int64_t skip_bytes,
                                   ObMysqlPacketType packet_type,
                                   ObIMysqlRespAnalyzer *analyzer,
                                   ObMysqlResp *server_response);
  void reset();

  common::DLL<ObMysqlTunnelConsumer> consumer_list_;
  ObMysqlTunnelConsumer *self_consumer_;
  event::ObVConnection *vc_;
  MysqlProducerHandler vc_handler_;
  event::ObVIO *read_vio_;
  event::ObMIOBuffer *read_buffer_;
  event::ObIOBufferReader *buffer_start_;
  ObMysqlTunnelType vc_type_;

  ObPacketAnalyzer packet_analyzer_;

  int64_t init_bytes_done_;          // bytes passed in buffer
  int64_t nbytes_;                   // total bytes (client's perspective)
  int64_t ntodo_;                    // what this vc needs to do
  int64_t bytes_read_;               // total bytes read from the vc
  int handler_state_;                // state used the handlers

  int64_t memory_flow_control_count_;
  int64_t cpu_flow_control_count_;
  int64_t consumer_reenable_count_;

  int32_t num_consumers_;

  bool alive_;
  bool read_success_;
  bool own_iobuffer_;

  int64_t cost_time_;

  // Flag and pointer for active flow control throttling.
  // If this is set, it points at the source producer that is under flow control.
  // If NULL then data flow is not being throttled.
  ObMysqlTunnelProducer *flow_control_source_;
  const char *name_;
};

class ObMysqlTunnel : public event::ObContinuation
{
  // Data for implementing flow control across a tunnel.
  //
  // The goal is to bound the amount of data buffered for a
  // transaction flowing through the tunnel to (roughly)
  // between the high_water and low_water water marks.
  // Due to the chunky water of data flow this always approximate.
  friend class ObShowSMHandler;

public:
  ObMysqlTunnel();
  virtual ~ObMysqlTunnel() { }

  void init(ObMysqlSM &sm_arg, event::ObProxyMutex &mutex);
  void reset();
  void kill_tunnel();
  bool is_tunnel_active() const { return active_; }
  bool is_tunnel_alive() const;

  ObMysqlTunnelProducer *add_producer(event::ObVConnection *vc,
                                      const int64_t nbytes,
                                      event::ObIOBufferReader *reader_start,
                                      MysqlProducerHandler sm_handler,
                                      ObMysqlTunnelType vc_type,
                                      const char *name,
                                      const bool own_iobuffer = true);

  ObMysqlTunnelConsumer *add_consumer(event::ObVConnection *vc,
                                      event::ObVConnection *producer,
                                      MysqlConsumerHandler sm_handler,
                                      ObMysqlTunnelType vc_type,
                                      const char *name,
                                      const int64_t skip_bytes = 0);

  void deallocate_buffers();
  common::DLL<ObMysqlTunnelConsumer> *get_consumers(event::ObVConnection *vc);
  ObMysqlTunnelProducer *get_producer(event::ObVConnection *vc);
  ObMysqlTunnelConsumer *get_consumer(event::ObVConnection *vc);
  int tunnel_run(ObMysqlTunnelProducer *p = NULL);

  int main_handler(int event, void *data);
  void consumer_reenable(ObMysqlTunnelConsumer &c);
  bool consumer_handler(int event, ObMysqlTunnelConsumer &c);
  bool producer_handler(int event, ObMysqlTunnelProducer &p);
  int producer_handler_packet(int event, ObMysqlTunnelProducer &p);
  int local_finish_all(ObMysqlTunnelProducer &p);
  int chain_finish_all(ObMysqlTunnelProducer &p);
  void chain_abort_all(ObMysqlTunnelProducer &p);

  // Mark a producer and consumer as the same underlying object.
  //
  // This is use to chain producer/consumer pairs together to
  // indicate the data flows through them sequentially. The primary
  // example is a transform which serves as a consumer on the server
  // side and a producer on the cache/client side.
  void chain(ObMysqlTunnelConsumer &c, ObMysqlTunnelProducer &p);

private:
  int finish_all_internal(ObMysqlTunnelProducer &p, const bool chain);
  int producer_run(ObMysqlTunnelProducer &p);

  ObMysqlTunnelProducer *get_producer(event::ObVIO *vio);
  ObMysqlTunnelConsumer *get_consumer(event::ObVIO *vio);

  ObMysqlTunnelProducer *alloc_producer();
  ObMysqlTunnelConsumer *alloc_consumer();

  int64_t get_local_thread_queue_size() const;

private:
  static const int64_t MAX_PRODUCERS = 2;
  static const int64_t MAX_CONSUMERS = 4;
  static const ObHRTime CONSUMER_REENABLE_RETRY_TIME = HRTIME_MSECONDS(1);

  ObMysqlTunnelConsumer consumers_[MAX_CONSUMERS];
  ObMysqlTunnelProducer producers_[MAX_PRODUCERS];

  int64_t num_producers_;
  int64_t num_consumers_;
  ObMysqlSM *sm_;
  int64_t last_handler_event_time_;

  bool active_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlTunnel);
};

// After the producer has finished, causes direct consumers
// to finish their writes
inline int ObMysqlTunnel::local_finish_all(ObMysqlTunnelProducer &p)
{
  return finish_all_internal(p, false);
}

// After the producer has finished, cause everyone
// downstream in the tunnel to send everything
// that producer has placed in the buffer
inline int ObMysqlTunnel::chain_finish_all(ObMysqlTunnelProducer &p)
{
  return finish_all_internal(p, true);
}

inline bool ObMysqlTunnel::is_tunnel_alive() const
{
  bool tunnel_alive = producers_[0].alive_;

  if (OB_UNLIKELY(!tunnel_alive)) {
    for (int64_t i = 1; i < MAX_PRODUCERS && !tunnel_alive; ++i) {
      if (producers_[i].alive_) {
        tunnel_alive = true;
      }
    }
  }

  if (OB_UNLIKELY(!tunnel_alive)) {
    for (int64_t i = 0; i < MAX_CONSUMERS && !tunnel_alive; ++i) {
      if (consumers_[i].alive_) {
        tunnel_alive = true;
      }
    }
  }

  return tunnel_alive;
}

inline ObMysqlTunnelProducer *ObMysqlTunnel::get_producer(event::ObVConnection *vc)
{
  ObMysqlTunnelProducer *ret = NULL;
  if (OB_LIKELY(producers_[0].vc_ == vc)) {
    ret = producers_;
  } else {
    for (int64_t i = 1; i < MAX_PRODUCERS && NULL == ret; ++i) {
      if (producers_[i].vc_ == vc) {
        ret = producers_ + i;
      }
    }

  }
  return ret;
}

inline ObMysqlTunnelConsumer *ObMysqlTunnel::get_consumer(event::ObVConnection *vc)
{
  ObMysqlTunnelConsumer *ret = NULL;
  if (OB_LIKELY(consumers_[0].vc_ == vc)) {
    ret = consumers_;
  } else {
    for (int64_t i = 1; i < MAX_CONSUMERS && NULL == ret; ++i) {
      if (consumers_[i].vc_ == vc) {
        ret = consumers_ + i;
      }
    }
  }
  return ret;
}

inline ObMysqlTunnelProducer *ObMysqlTunnel::get_producer(event::ObVIO *vio)
{
  ObMysqlTunnelProducer *ret = NULL;
  if (OB_LIKELY(producers_[0].read_vio_ == vio)) {
    ret = producers_;
  } else {
    for (int64_t i = 1; i < MAX_PRODUCERS && NULL == ret; ++i) {
      if (producers_[i].read_vio_ == vio) {
        ret = producers_ + i;
      }
    }
  }
  return ret;
}

inline ObMysqlTunnelConsumer *ObMysqlTunnel::get_consumer(event::ObVIO *vio)
{
  ObMysqlTunnelConsumer *ret = NULL;
  if (OB_LIKELY(consumers_[0].write_vio_ == vio)) {
    ret = consumers_;
  } else {
    for (int64_t i = 1; i < MAX_CONSUMERS && NULL == ret; ++i) {
      if (consumers_[i].write_vio_ == vio) {
        ret = consumers_ + i;
      }
    }
  }
  return ret;
}

inline bool ObMysqlTunnelConsumer::is_sink() const
{
  return MT_MYSQL_SERVER == vc_type_ || MT_MYSQL_CLIENT == vc_type_;
}

inline bool ObMysqlTunnelProducer::is_source() const
{
  // If a producer is marked as a client, then it's part of a bidirectional tunnel
  // and so is an actual source of data.
  return MT_MYSQL_SERVER == vc_type_ || MT_MYSQL_CLIENT == vc_type_;
}

inline bool ObMysqlTunnelProducer::is_throttled() const
{
  return NULL != flow_control_source_;
}

inline void ObMysqlTunnelProducer::throttle()
{
  if (!is_throttled()) {
    set_throttle_src(this);
  }
}

inline void ObMysqlTunnelProducer::unthrottle()
{
  if (is_throttled()) {
    set_throttle_src(NULL);
  }
}

inline void ObMysqlTunnelProducer::update_state_if_not_set(int new_handler_state)
{
  if (0 == handler_state_) {
    handler_state_ = new_handler_state;
  }
}

inline int ObMysqlTunnelProducer::set_request_packet_analyzer(
    ObMysqlPacketType packet_type,
    ObMysqlRequestAnalyzer *analyzer,
    ObProtocolDiagnosis *protocol_diagnosis)
{
  int ret = common::OB_SUCCESS;
  packet_analyzer_.producer_ = this;
  packet_analyzer_.skip_bytes_ = 0;
  packet_analyzer_.packet_type_ = packet_type;
  packet_analyzer_.resp_analyzer_ = NULL;
  packet_analyzer_.server_response_ = NULL;
  packet_analyzer_.request_analyzer_ = analyzer;
  INC_SHARED_REF(packet_analyzer_.protocol_diagnosis_, protocol_diagnosis);
  if (NULL != analyzer && NULL != buffer_start_) {
    // request also need analyzer
    packet_analyzer_.packet_reader_ = buffer_start_->clone();
    if (OB_ISNULL(packet_analyzer_.packet_reader_)) {
      ret = common::OB_ERR_SYS;
    }
  } else {
    packet_analyzer_.packet_reader_ = NULL;
  }
  return ret;
}

inline int ObMysqlTunnelProducer::set_response_packet_analyzer(
    const int64_t skip_bytes, ObMysqlPacketType packet_type,
    ObIMysqlRespAnalyzer *analyzer, ObMysqlResp *server_response)
{
  int ret = common::OB_SUCCESS;
  packet_analyzer_.producer_ = this;
  packet_analyzer_.skip_bytes_ = skip_bytes;
  packet_analyzer_.packet_type_ = packet_type;
  packet_analyzer_.resp_analyzer_ = analyzer;
  packet_analyzer_.server_response_ = server_response;
  packet_analyzer_.request_analyzer_ = NULL;

  if (NULL != analyzer && NULL != buffer_start_) {
    // resultset protocol
    packet_analyzer_.packet_reader_ = buffer_start_->clone();
    if (OB_ISNULL(packet_analyzer_.packet_reader_)) {
      ret = common::OB_ERR_SYS;
    }
  } else {
    packet_analyzer_.packet_reader_ = NULL;
  }
  return ret;
}

inline void ObMysqlTunnelProducer::reset()
{
  if (NULL != packet_analyzer_.packet_reader_) {
    packet_analyzer_.packet_reader_->dealloc();
    packet_analyzer_.packet_reader_ = NULL;
  }
}

inline bool ObMysqlTunnelProducer::is_flow_controlled() const
{
  return (0 != memory_flow_control_count_ || 0 != cpu_flow_control_count_);
}

// We set the producers in a flow chain specifically rather than using
// a tunnel level variable in order to handle bi-directional tunnels
// correctly. In such a case the flow control on producers is
// not related so a single value for the tunnel won't work.
inline void ObMysqlTunnelProducer::set_throttle_src(ObMysqlTunnelProducer *srcp)
{
  ObMysqlTunnelProducer *p = this;

  p->flow_control_source_ = srcp;
  for (ObMysqlTunnelConsumer *c = consumer_list_.head_; NULL != c; c = c->link_.next_) {
    if (!c->is_sink()) {
      p = c->self_producer_;
      if (NULL != p) {
        p->set_throttle_src(srcp);
      }
    }
  }
}

inline void ObMysqlTunnel::chain(ObMysqlTunnelConsumer &c, ObMysqlTunnelProducer &p)
{
  p.self_consumer_ = &c;
  c.self_producer_ = &p;
  // If the flow is already throttled update the chained producer.
  if (NULL != c.producer_ && c.producer_->is_throttled()) {
    p.set_throttle_src(c.producer_->flow_control_source_);
  }
}

inline int64_t ObMysqlTunnel::get_local_thread_queue_size() const
{
  event::ObEThread &thread = event::self_ethread();
  return thread.event_queue_external_.get_atomic_list_size() +
      thread.event_queue_external_.get_local_queue_size();
}

inline void ObMysqlTunnel::init(ObMysqlSM &sm_arg, event::ObProxyMutex &mutex)
{
  sm_ = &sm_arg;
  active_ = false;
  mutex_ = &mutex;
  SET_HANDLER(&ObMysqlTunnel::main_handler);
}

inline void ObMysqlTunnel::deallocate_buffers()
{
  for (int64_t i = 0; i < MAX_PRODUCERS; ++i) {
    if (NULL != producers_[i].read_buffer_) {
      if (OB_ISNULL(producers_[i].vc_)) {
        PROXY_TUNNEL_LOG(EDIAG, "deallocate_buffers, producer with NULL vc",
                         K(i), "producer", &producers_[i]);
      }
      producers_[i].reset();
      if (producers_[i].own_iobuffer_) {
        free_miobuffer(producers_[i].read_buffer_);
      }
      producers_[i].read_buffer_ = NULL;
      producers_[i].buffer_start_ = NULL;
    }
  }
}

inline void ObMysqlTunnel::reset()
{
  if (OB_UNLIKELY(active_)) {
    PROXY_TUNNEL_LOG(WDIAG, "reset active tunnel", K(this), K_(active));
  }
#ifdef DEBUG
  for (int i = 0; i < MAX_PRODUCERS; ++i) {
    if (producers_[i].alive_) {
      PROXY_TUNNEL_LOG(WDIAG, "reset alive producer",
                       K(i), "producer", &producers_[i], "alive", producers_[i].alive_);
    }
  }
  for (int j = 0; j < MAX_CONSUMERS; ++j) {
    if (consumers_[j].alive_) {
      PROXY_TUNNEL_LOG(WDIAG, "reset alive consumer",
                       K(i), "consumer", &consumers_[i], "alive", consumers_[i].alive_);
    }
  }
#endif
  deallocate_buffers();
  num_producers_ = 0;
  num_consumers_ = 0;
  memset(consumers_, 0, sizeof(consumers_));
  memset(producers_, 0, sizeof(producers_));
}

inline void ObMysqlTunnel::kill_tunnel()
{
  for (int64_t i = 0; i < MAX_PRODUCERS; ++i) {
    if (NULL != producers_[i].vc_) {
      chain_abort_all(producers_[i]);
    }
    if (OB_UNLIKELY(producers_[i].alive_)) {
      PROXY_TUNNEL_LOG(WDIAG, "killed tunnel with alive producer",
               K(i), "producer", &producers_[i], "alive", producers_[i].alive_);
    }
  }
  active_ = false;
  reset();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_TUNNEL_H

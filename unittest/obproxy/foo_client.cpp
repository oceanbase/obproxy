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
#include "obutils/ob_safe_snapshot_manager.h"
#include <iostream>
#include <fstream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define SLEEP_TIME_US 1000
#define MAX_BUF_LEN 256
#define DEFAULT_SERVER_COUNT 128
using namespace std;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::common;

struct ServerEntry
{
  ServerEntry() : entry_(NULL), is_valid_(true) {}
  ServerEntry(const ObSafeSnapshotEntry *entry) : entry_(entry), is_valid_(true) {}
  const ObSafeSnapshotEntry *entry_;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(KPC_(entry),
         K_(is_valid));
    J_OBJ_END();
    return pos;
  }
  bool is_valid_;
};

class FooClient
{
public:
  FooClient() : sock_fd_(-1), servers_(), manager_() {}
  void run();
private:
  void init();
  void refresh_server_addr();
  void sort_servers();
  bool query_to_server(ObAddr addr);
  int sock_fd_;
  ObSEArray<ServerEntry, DEFAULT_SERVER_COUNT> servers_;
  ObSafeSnapshotManager manager_;
};

void FooClient::init()
{
}

void FooClient::refresh_server_addr()
{
  char buffer[MAX_BUF_LEN];
  char ip[MAX_BUF_LEN];
  int port = 0;
  ifstream in("server_list.data");
  if (!in.is_open()) {
    printf("failed to open server list");
    exit(1);
  }

  // put the server into valid
  for (int i = 0; i < servers_.count(); ++i) {
    servers_.at(i).is_valid_ = true;
  }

  while (!in.eof()) {
    if (in.getline(buffer, MAX_BUF_LEN) <= 0) {
      break;
    }
    sscanf(buffer, "%[^:]:%d", ip, &port);
    ObAddr addr;
    addr.set_ipv4_addr(ip, port);

    if (NULL == manager_.get(addr)) {
      manager_.add(addr);
      ServerEntry server_entry(manager_.get(addr));
      servers_.push_back(server_entry);
      sort_servers();
    }
  }
}

void FooClient::sort_servers()
{
  // sort this server list
  int64_t cur_priority = INT64_MAX;
  int64_t min_priority = 0;
  int64_t min_priority_idx = 0;
  ServerEntry tmp_entry;
  // selection sort
  for (int64_t i = 0; i < servers_.count(); ++i) {
    if (NULL == servers_[i].entry_) {
      continue;
    }
    min_priority = servers_[i].entry_->get_priority();
    min_priority_idx = i;
    // find the min priority and swap
    for (int64_t j = i + 1; j < servers_.count(); ++j) {
      cur_priority = servers_[j].entry_->get_priority();
      if (min_priority >  cur_priority) {
        min_priority = cur_priority;
        min_priority_idx = j;
      }
    }
    if (min_priority_idx != i) {
      tmp_entry = servers_[i];
      servers_.at(i) = servers_[min_priority_idx];
      servers_.at(min_priority_idx) = tmp_entry;
    }
  }
}

bool FooClient::query_to_server(ObAddr addr)
{
  sock_fd_ = socket(AF_INET, SOCK_STREAM, 0);

  struct timeval timeo = {3, 0};
  socklen_t len = sizeof(timeo);
  timeo.tv_sec = 1;
  setsockopt(sock_fd_, SOL_SOCKET, SO_SNDTIMEO, &timeo, len);

  struct sockaddr_in servaddr;
  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  inet_pton(AF_INET, "127.0.0.1", &servaddr.sin_addr);
  servaddr.sin_port = htons(static_cast<uint16_t>(addr.get_port()));

  // connect
  if (connect(sock_fd_, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
    LOG_INFO("connect error", K(addr));
    return false;
  }

  // send
  char sendbuff[MAX_BUF_LEN];
  sprintf(sendbuff, "%d", 1);
  if (send(sock_fd_, sendbuff, strlen(sendbuff) + 1, MSG_NOSIGNAL) <= 0) {
    LOG_INFO("failed to send ", K(addr));
    return false;
  }

  return true;
}

void FooClient::run()
{
  int i = 0;
  while (true) {
    if (i++ % 100 == 0) {
      refresh_server_addr();
    }

    for (int i = 0; i < servers_.count(); ++i) {
      if (servers_[i].entry_ == NULL || !servers_[i].is_valid_) {
        continue;
      }
      // if failed add server to invalid server
      if (!query_to_server(servers_[i].entry_->get_addr())) {
        servers_.at(i).is_valid_ = false;
      } else {
        break;
      }
    }
    usleep(1000000); // 1s
  }
}

int main()
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");

  FooClient client;
  client.run();
  return 0;
}

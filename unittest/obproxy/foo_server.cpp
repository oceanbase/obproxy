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

#include <iostream>
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

#define MAX_EP_FD 256
#define LISTEN_QUEUE 256
using namespace std;

class FooServer
{
public:
  static const int MAX_EP_EVENT = 32;
  static const int MAX_LINE = 1024;
  FooServer(uint16_t port)
  {
    listenfd_ = -1;
    port_ = port;
    epfd_ = -1;
  }
  void run();
private:
  void init();
  void set_nonblocking(int sock);

  int listenfd_;
  uint16_t port_;
  int epfd_;
};

void FooServer::set_nonblocking(int sock)
{
  int opts = fcntl(sock,F_GETFL);
  if (opts < 0) {
    perror("fcntl(sock,GETFL)");
    exit(1);
  }
  opts = opts | O_NONBLOCK;

  if (fcntl(sock,F_SETFL,opts) < 0) {
    perror("fcntl(sock,SETFL,opts)");
    exit(1);
  }
}

void FooServer::init()
{
  epfd_ = epoll_create(MAX_EP_FD);
  listenfd_ = socket(AF_INET, SOCK_STREAM, 0);
  set_nonblocking(listenfd_); // Set the socket used for monitoring to non-blocking mode

  struct epoll_event ev;
  ev.data.fd = listenfd_; // Set the file descriptor associated with the event to be processed
  ev.events = EPOLLIN | EPOLLET; // Set the type of event to be processed
  epoll_ctl(epfd_, EPOLL_CTL_ADD, listenfd_, &ev); // Register epoll event

  struct sockaddr_in serveraddr;
  bzero(&serveraddr, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  const char *local_addr = "127.0.0.1";
  inet_aton(local_addr, &(serveraddr.sin_addr));
  serveraddr.sin_port = htons(port_);

  if (bind(listenfd_, (sockaddr *)&serveraddr, sizeof(serveraddr)) != 0) {
    printf("failed to bind, port = %d", port_);
    exit(1);
  }

  if (listen(listenfd_, LISTEN_QUEUE) != 0) {
    printf("failed to listen, port = %d", port_);
    exit(1);
  }
  printf("succ to listen, port = %d\n", port_);
  fflush(stdout);
}

void FooServer::run()
{
  int event_num = 0;
  int conn_fd = -1;
  int sock_fd = -1;
  struct epoll_event ev;
  struct epoll_event events[MAX_EP_EVENT];
  char line[MAX_LINE];
  struct sockaddr_in client_addr;
  socklen_t clilen;
  ssize_t total_size = 0;
  ssize_t read_size = 0;
  int query_count = 0;

  time_t last_time;
  time_t cur_time;
  time(&last_time);

  init();
  while (true) {
    event_num = epoll_wait(epfd_, events, MAX_EP_EVENT, -1);

    // print per sec
    time(&cur_time);
    if (cur_time > last_time) {
      struct tm *cur_timeinfo = localtime(&cur_time);
      printf("%02d:%02d:%02d %d\n", cur_timeinfo->tm_hour,
                                    cur_timeinfo->tm_min,
                                    cur_timeinfo->tm_sec,
                                    query_count);
      fflush(stdout);
      last_time = cur_time;
      query_count = 0;
    }

    for (int i = 0; i < event_num; ++i) {
      if (events[i].data.fd == listenfd_) {
        while((conn_fd = accept(listenfd_, (sockaddr *)&client_addr, &clilen)) > 0){
          set_nonblocking(conn_fd);
          ev.data.fd = conn_fd;
          ev.events = EPOLLIN | EPOLLET;
          epoll_ctl(epfd_, EPOLL_CTL_ADD, conn_fd, &ev);
        }
      } else if (events[i].events & EPOLLIN) {
        if ((sock_fd = events[i].data.fd) < 0) {
          continue;
        }
        memset(line, 0, MAX_LINE);
        total_size = 0;
        while ((read_size = read(sock_fd, line + total_size, MAX_LINE - total_size)) > 0) {
          total_size += read_size;
          if (total_size >= MAX_LINE) {
            break;
          }
        }

        if (total_size == 0) {
          close(sock_fd);
          continue;
        } else {
          query_count++;
        }
      }
    }
  }
}

int main(int argc, char *argv[])
{
  uint16_t port = 0;
  if (argc >= 2) {
    port = static_cast<uint16_t>(atoi(argv[1]));
  }
  if (port == 0) {
    printf("invalid port = %u\n", port);
    return -1;
  }
  FooServer server(port);
  server.run();
  return 0;
}

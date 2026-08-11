/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SIGNAL_HANDLER_H_
#define OCEANBASE_SIGNAL_HANDLER_H_

#include <signal.h>

namespace oceanbase
{
namespace common
{

int init_signal();
int catch_crash_error_signal();
int ignore_crash_error_signal();
int do_detect_sig();
void sig_async_handler(const int sig);
void sig_direct_handler(int sig, siginfo_t *si, void *contextg);
void coredump_cb(volatile int sig, volatile int sig_code, void* volatile sig_addr, void *context);

int add_sig_ignore_catched(struct sigaction &action, const int sig);
int add_sig_default_catched(struct sigaction &action, const int sig);
int add_sig_direct_catched(struct sigaction &action, const int sig, const int flag = 0);
int add_sig_async_catched(struct sigaction &action, const int sig, const int flag = 0);

void print_limit(const char *name, const int resource);
void print_all_limits();

} // end of namespace common
} // end of namespace oceanbase

#endif // OCEANBASE_SIGNAL_HANDLER_H_
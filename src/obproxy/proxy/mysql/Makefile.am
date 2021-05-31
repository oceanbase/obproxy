AM_CPPFLAGS := $(AM_CPPFLAGS) -I${DEP_DIR}/include/tbsys             \
             -I${top_srcdir}/include                                    \
             -I${top_srcdir}/src                                        \
             -I${top_srcdir}/src/obproxy/iocore/eventsystem             \
             -I${top_srcdir}/src/obproxy/iocore/net                     \
             -I${top_srcdir}/src/obproxy/stat                           \
             -I${top_srcdir}/src/obproxy/proxy/api                      \
             -I${top_srcdir}/src/obproxy/proxy/plugins                  \
             -I${top_srcdir}/src/obproxy/proxy/mysqllib                 \
             -I${top_srcdir}/src/obproxy/obutils                        \
             -I${top_srcdir}/src/obproxy/utils                          \
             -fPIC

LIBTOOLFLAGS=--preserve-dup-deps

AM_LDFLAGS = -lpthread -lc -lm -lrt -lmysqlclient_r

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libmysqlproxy.a

common_sources =                      \
  ob_proxy_client_session.cpp         \
  ob_proxy_client_session.h           \
  ob_mysql_client_session.cpp         \
  ob_mysql_client_session.h           \
  ob_mysql_stats.cpp                  \
  ob_mysql_stats.h                    \
  ob_mysql_debug_names.cpp            \
  ob_mysql_debug_names.h              \
  ob_mysql_proxy_port.cpp             \
  ob_mysql_proxy_port.h               \
  ob_mysql_proxy_server_main.cpp      \
  ob_mysql_proxy_server_main.h        \
  ob_mysql_server_session.cpp         \
  ob_mysql_server_session.h           \
  ob_mysql_session_manager.cpp        \
  ob_mysql_session_manager.h          \
  ob_mysql_global_session_utils.cpp   \
  ob_mysql_global_session_utils.h     \
  ob_mysql_global_session_manager.cpp \
  ob_mysql_global_session_manager.h   \
  ob_mysql_session_accept.cpp         \
  ob_mysql_session_accept.h           \
  ob_mysql_sm.cpp                     \
  ob_mysql_sm.h                       \
  ob_mysql_transact.cpp               \
  ob_mysql_transact.h                 \
  ob_mysql_tunnel.cpp                 \
  ob_mysql_tunnel.h                   \
  ob_mysql_vctable.cpp                \
  ob_mysql_vctable.h                  \
  ob_show_sm_handler.cpp              \
  ob_show_sm_handler.h                \
  ob_show_session_handler.cpp         \
  ob_show_session_handler.h           \
  ob_kill_op_handler.cpp              \
  ob_kill_op_handler.h                \
  ob_prepare_statement_struct.cpp     \
  ob_prepare_statement_struct.h       \
  ob_cursor_struct.cpp                \
  ob_cursor_struct.h

libmysqlproxy_a_SOURCES = ${common_sources}

clean-local:
	-rm -f *.gcov *.gcno *.gcda

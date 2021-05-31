AM_CPPFLAGS= -I${DEP_DIR}/include/tbsys \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src \
			 -fPIC
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LTLIBRARIES = libsqlclient.la

libsqlclient_la_SOURCES = ob_mysql_connection.h    ob_mysql_connection.cpp		\
	ob_mysql_connection_pool.h    ob_mysql_connection_pool.cpp			\
	ob_server_connection_pool.h   ob_server_connection_pool.cpp			\
	ob_single_mysql_connection_pool.h    ob_single_mysql_connection_pool.cpp	\
	ob_mysql_result.h                    ob_mysql_result.cpp			\
	ob_mysql_result_impl.h             ob_mysql_result_impl.cpp			\
	ob_mysql_prepared_param.h     ob_mysql_prepared_param.cpp			\
	ob_mysql_prepared_result.h    ob_mysql_prepared_result.cpp			\
	ob_mysql_statement.h          ob_mysql_statement.cpp				\
	ob_mysql_prepared_statement.h ob_mysql_prepared_statement.cpp			\
	ob_connection_allocator.h							\
	ob_mysql_server_provider.h \
  ob_isql_connection.h \
  ob_isql_connection_pool.h \
  ob_isql_result_handler.h \
  ob_mysql_read_context.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda

AM_CPPFLAGS += -I${top_srcdir}/src\
               -I${top_srcdir}/src/lib\
               -I${top_srcdir}/unittest\
							 -DSQL_PARSER_COMPILATION

AM_CPPFLAGS += -I${top_srcdir}/src/obproxy/iocore/eventsystem               \
               -I${top_srcdir}/src/obproxy/iocore/net                       \
               -I${top_srcdir}/src/obproxy/proxy/mysql                      \
               -I${top_srcdir}/src/obproxy/proxy/mysqllib                   \
               -I${top_srcdir}/src/obproxy/proxy/api                        \
               -I${top_srcdir}/src/obproxy/obutils                          \
               -I${top_srcdir}/src/obproxy/utils                            \
							 -I${top_srcdir}/src/lib/hash \
							 -I${top_srcdir}/deps/easy/src/include\
							 -I${top_srcdir}/deps/easy/src/io\
							 -I${top_srcdir}/deps/easy/src/util\
							 -I${top_srcdir}/deps/easy/src/thread\
							 -I${DEP_DIR}/include/mariadb\
               -I${top_srcdir}/src/obproxy
LIBTOOLFLAGS=--preserve-dup-deps


LDADD = $(top_builddir)/src/obproxy/libobproxy.la           \
        $(top_builddir)/src/lib/compress/libzlib_1.0.la     \
        ${DEP_DIR}/lib64/libprometheus-cpp-pull.a \
        ${DEP_DIR}/lib64/libprometheus-cpp-core.a \
        ${RUNTIME_DIR}/lib/libob_sql_proxy_parser_static.a \
				${DEP_DIR}/lib64/libprotobuf.a \
				${DEP_DIR}/lib/libgrpc++.a \
				${DEP_DIR}/lib/libgrpc.a \
				${DEP_DIR}/lib/libgtest.a \
        ${DEP_DIR}/lib/libgpr.a \
				${DEP_DIR}/lib/libgmock.a \
				${DEP_DIR}/lib/libaddress_sorting.a \
				${DEP_DIR}/lib/libcares.a \
				${DEP_DIR}/lib/libssl.a \
				${DEP_DIR}/lib/libcurl.a \
				${DEP_DIR}/lib/libcrypto.a \
        -lc -lm -lrt -ldl -lpthread

pub_sources=                                           \
                 test_eventsystem_api.h                \
                 test_eventsystem_api.cpp              \
								 ob_mysql_test_utils.h

bin_PROGRAMS =                                         \
                 test_proxy_config                     \
                 test_event_processor                  \
                 test_ethread                          \
                 test_event                            \
                 test_continuation                     \
                 test_protected_queue                  \
                 test_priority_event_queue             \
                 test_io_buffer                        \
                 test_unix_net_processor               \
                 test_unix_net                         \
                 test_unix_net_vconnection             \
                 test_field_heap                       \
                 test_proxy_table_processor_utils      \
                 test_proxy_auth_parser                \
                 test_mysql_transaction_analyzer       \
                 test_resultset_stream_analyzer        \
                 test_resultset_fetcher                \
                 test_config_server_processor          \
                 test_vip_tenant_cache                 \
                 test_proxy_json_config_info           \
                 test_mysql_transact									 \
                 test_ldc_location  									 \
                 obproxy_task_flow_controller_checker  \
								 ob_expr_parser_checker                \
								 ob_func_expr_parser_checker           \
								 test_zlib_stream_compressor           \
								 test_fast_zlib_stream_compressor      \
								 test_mysql_compress_analyzer          \
								 obproxy_parser_checker                \
								 test_safe_snapshot_manager            \
								 foo_client                            \
								 foo_server                            \
                 test_mysql_request_analyzer                           \
								 test_dual_parser \
								 obproxy_parser_test \
								 test_ob_blowfish \
                 test_mysql_version
##               test_layout


test_dual_parser_SOURCES = test_dual_parser.cpp ${pub_sources}
obproxy_parser_test_SOURCES = obproxy_parser_test.cpp
test_proxy_config_SOURCES = test_proxy_config.cpp
test_proxy_auth_parser_SOURCES = test_proxy_auth_parser.cpp ${pub_sources}
test_field_heap_SOURCES = test_field_heap.cpp  ${pub_sources}
#test_session_field_mgr_SOURCES = test_session_field_mgr.cpp ${pub_sources}
#test_proxy_session_info_SOURCES = test_proxy_session_info.cpp  ${pub_sources}
test_mysql_transaction_analyzer_SOURCES = test_mysql_transaction_analyzer.cpp
test_proxy_table_processor_utils_SOURCES = test_proxy_table_processor_utils.cpp
test_config_server_processor_SOURCES = test_config_server_processor.cpp
test_event_processor_SOURCES = test_event_processor.cpp  ${pub_sources}
test_ethread_SOURCES = test_ethread.cpp  ${pub_sources}
test_event_SOURCES = test_event.cpp  ${pub_sources}
test_continuation_SOURCES = test_continuation.cpp  ${pub_sources}
test_priority_event_queue_SOURCES = test_priority_event_queue.cpp  ${pub_sources}
test_resultset_stream_analyzer_SOURCES = test_resultset_stream_analyzer.cpp
test_protected_queue_SOURCES = test_protected_queue.cpp  ${pub_sources}
test_io_buffer_SOURCES = test_io_buffer.cpp  ${pub_sources}
test_unix_net_processor_SOURCES = test_unix_net_processor.cpp  ${pub_sources}
test_unix_net_SOURCES = test_unix_net.cpp  ${pub_sources}
test_unix_net_vconnection_SOURCES = test_unix_net_vconnection.cpp  ${pub_sources}
test_resultset_fetcher_SOURCES = test_resultset_fetcher.cpp  ${pub_sources}
test_vip_tenant_cache_SOURCES = test_vip_tenant_cache.cpp
test_proxy_json_config_info_SOURCES = test_proxy_json_config_info.cpp
test_mysql_transact_SOURCES = test_mysql_transact.cpp
test_ldc_location_SOURCES = test_ldc_location.cpp
obproxy_task_flow_controller_checker_SOURCES = obproxy_task_flow_controller_checker.h obproxy_task_flow_controller_checker.cpp
obproxy_parser_checker_SOURCES = obproxy_parser_checker.h obproxy_parser_checker.cpp
ob_expr_parser_checker_SOURCES = ob_expr_parser_checker.h ob_expr_parser_checker.cpp
ob_func_expr_parser_checker_SOURCES = ob_func_expr_parser_checker.h ob_func_expr_parser_checker.cpp
test_zlib_stream_compressor_SOURCES = test_zlib_stream_compressor.cpp
test_fast_zlib_stream_compressor_SOURCES = test_fast_zlib_stream_compressor.cpp
test_mysql_request_analyzer_SOURCES = test_mysql_request_analyzer.cpp
test_mysql_compress_analyzer_SOURCES = test_mysql_compress_analyzer.cpp ${pub_sources}
test_safe_snapshot_manager_SOURCES = test_safe_snapshot_manager.cpp
foo_client_SOURCES = foo_client.cpp
foo_server_SOURCES = foo_server.cpp
test_ob_blowfish_SOURCES = test_ob_blowfish.cpp
test_mysql_version_SOURCES = test_mysql_version.cpp
##test_layout_SOURCES = test_layout.cpp

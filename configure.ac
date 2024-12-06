    AC_INIT([OceanBase],
            [4.3.3.0],
            [wgs13579@gmail.com],
            [obproxy-ce],
            [http://oceanbase.taobao.org/])

    obapi_version="4.3.3.0"
    AC_SUBST(obapi_version)

    AC_ARG_VAR([TOPDIR], [The path of the project])
    TOPDIR="$(dirname $(readlink -f "$0"))"
    AC_SUBST(TOPDIR)

    AC_ARG_VAR([DEP_DIR], [the path of devel files])
    DEP_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/deps/devel
    AC_SUBST(DEP_DIR)

    AC_ARG_VAR([TOOLS_DIR], [the path of devtools])
    TOOLS_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/devtools
    AC_SUBST(TOOLS_DIR)


    AC_ARG_VAR([RUNTIME_DIR], [the path of runtime])
    RUNTIME_DIR=${TOPDIR}/deps/3rd/usr
    AC_SUBST(RUNTIME_DIR)

    # 设置依赖的路径变量
    AC_ARG_VAR([PATH], [The path to search for executables])
    PATH="$TOOLS_DIR/bin::$PATH"
    AC_SUBST(PATH)

    # 设置 CC 和 CXX 变量的值
    AC_ARG_VAR([CC], [set clang executable path])
    CC="$TOOLS_DIR/bin/clang"
    AC_SUBST(CC)

    AC_ARG_VAR([CXX], [set clang++ executable path])
    CXX="$TOOLS_DIR/bin/clang++"
    AC_SUBST(CXX)

    AC_DISABLE_STATIC
    AC_PROG_LIBTOOL()
    AC_PROG_RANLIB
    AM_INIT_AUTOMAKE([subdir-objects silent-rules])
    ac_default_prefix=${HOME}/oceanbase_bin # for convenience
    AC_LANG([C++])
    AC_PROG_CC([clang])
    AC_PROG_CXX([clang++])
    AC_SUBST([host_cpu])

    AC_CANONICAL_HOST

    dnl configure compiler flags
    AC_ARG_WITH([release],
    AS_HELP_STRING([--with-release],
    [use optimize (default is NO)]),
    [
    if test "$withval" = "yes"; then
      case "$host_cpu" in
        *aarch64* )
          AM_CXXFLAGS="-g -O2 -D_OB_VERSION=1000 -D_NO_EXCEPTION -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Wformat -Wno-deprecated -Wno-invalid-offsetof -finline-functions -fno-strict-aliasing -mtune=generic -march=armv8-a+crc -Wno-psabi -Wno-address-of-packed-member -fno-omit-frame-pointer -Wl,-z,noexecstack,-z,relro,-z,now,-z,notext -fPIC"
          AM_CFLAGS="-g -O2 -D_OB_VERSION=1000 -DCOMPATIBLE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG -finline-functions -fno-strict-aliasing -mtune=generic -Wno-psabi -Wno-sign-compare -fPIC"
        ;;
        * )
          AM_CXXFLAGS="-g -O2 -D_OB_VERSION=1000 -D_NO_EXCEPTION -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Wformat -Wno-deprecated -Wno-invalid-offsetof -finline-functions -fno-strict-aliasing -mtune=core2 -Wno-psabi -Wno-address-of-packed-member -fno-omit-frame-pointer -fPIC"
          AM_CFLAGS="-g -O2 -D_OB_VERSION=1000 -DCOMPATIBLE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG -finline-functions -fno-strict-aliasing -mtune=core2 -Wno-psabi -Wno-sign-compare -fPIC"
        ;;
      esac
    fi
    ],
    [ AM_CXXFLAGS="-D__STDC_LIMIT_MACROS -D_OB_VERSION=1000 -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION -g -Wformat -Wno-invalid-offsetof -Wno-deprecated -fno-strict-aliasing -fno-omit-frame-pointer -mtune=generic -Wno-psabi -Wno-sign-compare -Wno-address-of-packed-member -Wl,-z,noexecstack,-z,relro,-z,now,-z,notext -fPIC"
      AM_CFLAGS="-D_OB_VERSION=1000 -DCOMPATIBLE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -fno-omit-frame-pointer -g -Wno-psabi  -Wno-sign-compare -Wl,-z,noexecstack,-z,relro,-z,now,-z,notext -fPIC"
        ]
    )

    AC_ARG_WITH([opt],
    AS_HELP_STRING([--with-opt],
    [use clang's compile optimize (default is NO)]),
    [
    if test "$withval" = "yes"; then
      case "$host_cpu" in
        *aarch64* )
        # do nothing
        ;;
        * )
          # AutoFDO
          AM_CXXFLAGS="${AM_CXXFLAGS} -fprofile-sample-use=${TOPDIR}/fdo/obproxy.prof -funique-internal-linkage-names -fdebug-info-for-profiling -Wl,--no-rosegment"
          AM_CFLAGS="${AM_CFLAGS} -fprofile-sample-use=${TOPDIR}/fdo/obproxy.prof -funique-internal-linkage-names -fdebug-info-for-profiling -Wl,--no-rosegment"

          # BOLT
          AM_CXXFLAGS="${AM_CXXFLAGS} -Wl,--emit-relocs"
          AM_CFLAGS="${AM_CFLAGS} -Wl,--emit-relocs"

          # LTO
          # AM_CXXFLAGS="${AM_CXXFLAGS} -flto"
          # AM_CFLAGS="${AM_CFLAGS} -flto"
        ;;
      esac
    fi
    ]
    )

    # 设置依赖的库和头文件路径
    AM_CXXFLAGS="${AM_CXXFLAGS} -isystem $DEP_DIR/include -isystem ${RUNTIME_DIR}/include -L$DEP_DIR/lib"
    AM_CFLAGS="${AM_CFLAGS} -isystem $DEP_DIR/include -isystem ${RUNTIME_DIR}/include -L$DEP_DIR/lib"
    AM_CXXFLAGS="${AM_CXXFLAGS} -D_GLIBCXX_USE_CXX11_ABI=0"
    AM_CFLAGS="${AM_CFLAGS} -D_GLIBCXX_USE_CXX11_ABI=0"

    AC_ARG_WITH([so],
           AS_HELP_STRING([--with-so],
                          [with so support (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                support_so=yes
                                AM_CXXFLAGS="${AM_CXXFLAGS} -fPIC"
                                AM_CFLAGS="${AM_CFLAGS} -fPIC"
                            fi
                          ],
                          [support_so=no]
           )

    AM_CONDITIONAL([HAVE_SO], test x$support_so = xyes )

    #check el9, add -DEL9_PLATFORM for el9
    is_el9=`cat /proc/version | grep el9`
    if test "$is_el9" != ""; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -DEL9_PLATFORM"
        AM_CFLAGS="${AM_CFLAGS} -DEL9_PLATFORM"
    fi

    #check el9, add -DAL8_PLATFORM for al8
    is_al8=`cat /proc/version | grep al8`
    if test "$is_al8" != ""; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -DAL8_PLATFORM"
        AM_CFLAGS="${AM_CFLAGS} -DAL8_PLATFORM"
    fi

    CLANG_VERSION=$(echo `$CC --version` | awk '{print $3}')
    if test $? -eq 0; then
        major=`echo $CLANG_VERSION | cut -d. -f1`
        minor=`echo $CLANG_VERSION | cut -d. -f2`
    fi

    echo "now clang is: $CLANG_VERSION"

    AM_CONDITIONAL([BUILD_OPENSOURCE], [true])
    AM_CXXFLAGS="${AM_CXXFLAGS} -DBUILD_OPENSOURCE"
    AM_CFLAGS="${AM_CFLAGS} -DBUILD_OPENSOURCE"

    OSVER=`cat /etc/issue|head -1|awk -F. '{print $1}' | awk '{print $NF}'`
    if test $OSVER -le 6; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -DUSE_POSIX_FALLOCATE"
    fi

    support_sse4_2=`cat /proc/cpuinfo|grep flags|head -1|grep -oE sse4_2`
    if test "$support_sse4_2" = "sse4_2" ; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -DSUPPORT_SSE4_2"
    fi

    AC_ARG_WITH([atomic-time],
        AS_HELP_STRING([--with-atomic-time],
                       [record atomic instructions time (default is NO)]),
        [
            if test "$withval" == "yes"; then
                AM_CXXFLAGS="${AM_CXXFLAGS} -DATOMIC_TIME_PERF"
            fi
        ],
        [])
    AC_ARG_WITH([memory-check],
        AS_HELP_STRING([--with-memory-check],
                       [memory check (defualt is NO)]),
        [
            if test "$withval" = "yes"; then
                AM_CXXFLAGS="${AM_CXXFLAGS} -DMEMCHK_LEVEL=1"
            fi
        ],
        [])

    AC_ARG_WITH([asan],
           AS_HELP_STRING([--with-asan],
                          [with asan (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                AM_CXXFLAGS="${AM_CXXFLAGS} -fsanitize=address -fstack-protector-strong -fno-optimize-sibling-calls -fno-omit-frame-pointer -static-libsan -fno-inline -fsanitize-recover=address -DUSING_ASAN"
                                AM_CFLAGS="${AM_CFLAGS} -fsanitize=address -fstack-protector-strong -fno-optimize-sibling-calls -fno-omit-frame-pointer -static-libsan -fno-inline -fsanitize-recover=address -DUSING_ASAN"
                            fi
                          ],
                          [with_asan=no]
           )

    AC_ARG_WITH([5u-support],
           AS_HELP_STRING([--with-5u-support],
                          [with 5u support (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                support5u=yes
                            fi
                          ],
                          [support5u=no]
           )
    if test $support5u = "no"
    then
      AM_CXXFLAGS="${AM_CXXFLAGS} -DHAVE_SCHED_GETCPU -DHAVE_REALTIME_COARSE -DOB_HAVE_EVENTFD -DHAVE_FALLOCATE"
    fi
    AM_CONDITIONAL([SUPPORT5U], test x$support5u = xno )

    AC_ARG_WITH([preload],
           AS_HELP_STRING([--with-preload],
                          [with preload (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                with_preload=yes
                                AM_CXXFLAGS="${AM_CXXFLAGS} -D__ENABLE_PRELOAD__"
                            fi
                          ],
                          [
                          with_preload=no
                          ]
           )
    AC_ARG_WITH([mem-trace],
           AS_HELP_STRING([--with-mem-trace],
                          [with memory trace (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                memtrace=yes
                            fi
                          ],
                          [memtrace=no]
           )
    if test $memtrace = "yes"
    then
        AM_CXXFLAGS="${AM_CXXFLAGS}  -D__OB_MTRACE__ -rdynamic"
    fi
    AM_CONDITIONAL([MEMTRACE], test x$memtrace = xyes )

    AC_ARG_WITH([mem-debug],
           AS_HELP_STRING([--with-mem-debug],
                          [with memory debug features (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                memdebug=yes
                            fi
                          ],
                          [memdebug=no]
           )
    if test $memdebug = "yes"
    then
        AM_CXXFLAGS="${AM_CXXFLAGS}  -D__OB_MDEBUG__"
    fi
    AM_CONDITIONAL([MEMDEBUG], test x$memdebug = xyes )

    if test "X${DEP_DIR}" = "X"; then
       DEP_DIR=${ac_abs_confdir}/rpm/.dep_create
       echo "use default DEP_DIR=${DEP_DIR}"
       AC_SUBST([DEP_DIR])
    fi

    # if test ! -f "${DEP_DIR}/lib/libmysqlclient_r.so.18.1.1" ; then
    #    echo "can't find libmysqlclient_r.so.18.1.1 in ${DEP_DIR}/lib/; maybe you need use 'cd rpm; dep_create obproxy.deps'"
    #    exit 127
    # fi

    AM_LDFLAGS="${AM_LDFLAGS} -L${DEP_DIR}/lib -L${DEP_DIR}/lib/mysql"

    dnl global default INCLUDES
    AM_CPPFLAGS="${AM_CPPFLAGS} -I${DEP_DIR}/include -I${DEP_DIR}/include/mysql"

    if test "X${CXXFLAGS}" = "X-g -O2"; then
        CXXFLAGS=""
    fi
    if test "X${CFLAGS}" = "X-g -O2"; then
        CFLAGS=""
    fi

    dnl condition COVERAGE
    AC_ARG_WITH([coverage],
           AS_HELP_STRING([--with-coverage],
                          [with coverage (default is NO)]),
                          [case "${withval}" in
			   yes) coverage=true ;;
		           no)  coverage=false ;;
		           *) AC_MSG_ERROR([bad value ${withval} for --with-coverage]) ;;
		           esac
                          ],
                          [coverage=false]
           )
    AM_CONDITIONAL([COVERAGE], test x$coverage = xtrue )
    if test "X$coverage" = X"true"; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -fprofile-arcs -ftest-coverage -DTEST_COVER"
        AM_CFLAGS="${AM_CFLAGS} -fprofile-arcs -ftest-coverage -DTEST_COVER"
        AM_LDFLAGS="${AM_LDFLAGS} -lgcov"
    fi

    dnl condition WITH_BEYONDTRUST
    AC_ARG_WITH([beyondtrust],
        AS_HELP_STRING([--with-beyondtrust],
                       [whether use beyondtrust feature (default is NO)]),
                       [
                           if test "$withval" == "yes"; then
                               beyondtrust=yes
                               AM_CXXFLAGS="${AM_CXXFLAGS} -DHAVE_BEYONDTRUST"
                           fi
                       ],
                       [beyondtrust=no])
    AM_CONDITIONAL([HAVE_BEYONDTRUST], test x$beyondtrust = xyes )

    dnl define global flags
    AC_SUBST([AM_CPPFLAGS])
    AC_SUBST([AM_LDFLAGS])
    AC_SUBST([AM_CXXFLAGS])
    AC_SUBST([AM_CFLAGS])
    AC_SUBST([AM_CXXFLAGS_WITH_ARCH])
    AC_SUBST([AM_CFLAGS_WITH_ARCH])

    dnl condition WITH_TEST_CASE
    AC_ARG_WITH([test_case],
           AS_HELP_STRING([--with-test-case],
                          [with test case (default is YES)]),
                          [
                            if test "$withval" = "no"; then
                                test_case=no
                            fi
                          ],
                          [ test_case=yes ]
           )
    AM_CONDITIONAL([WITH_TEST_CASE], test x$test_case = xyes )

    AC_ARG_WITH([simplify_verification],
           AS_HELP_STRING([--with-simplify-verification],
                          [with simplify verification (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                AC_DEFINE(SIMPLIFY_VERIFICATION)
                            fi
                          ],
                          [ echo "simplify-verification is no" ]
           )

    dnl strip_ut
    AC_ARG_ENABLE([strip_ut],
    AS_HELP_STRING([--enable-strip-ut], [strip unit test binaries. (default is NO)]),
    [case "${enableval}" in
      yes) strip_ut=true ;;
      no)  strip_ut=false ;;
      *) AC_MSG_ERROR([bad value ${enableval} for --enable-strip-ut]) ;;
    esac],
    [strip_ut=false])
    AM_CONDITIONAL([STRIP_UT], [test x$strip_ut = xtrue])

    dnl dlink_observer
    AC_ARG_ENABLE([dlink_observer],
    AS_HELP_STRING([--enable-dlink-observer], [unit test dynamic link observer. (default is NO)]),
    [case "${enableval}" in
      yes) dlink_observer=true ;;
      no)  dlink_observer=false ;;
      *) AC_MSG_ERROR([bad value ${enableval} for --enable-dlink-observer]) ;;
    esac],
    [dlink_observer=false])
    AM_CONDITIONAL([DLINK_OBSERVER], [test x$dlink_observer = xtrue])

    AC_ARG_WITH([errsim],
      AS_HELP_STRING([--with-errsim],
        [whether use error inject (default is NO)]),
        [
          if test "$withval" = "yes"; then
              enable_errsim=yes
              AM_CXXFLAGS="${AM_CXXFLAGS} -DERRSIM"
          fi
        ],
        [
        enable_errsim=no
        ]
    )

    dnl build_version.mk related:
    dnl see whether git is installed
    AC_PATH_PROG(svnversioncommand, git)
    AC_ARG_WITH([svnfile],
           AS_HELP_STRING([--with-svnfile],
                          [use svnfile svn_dist_version instead of running git (default is NO)]),
                          [
                            if test "$withval" = "yes"; then
                                use_svnfile=yes
                            fi
                          ],
                          [use_svnfile=no]
           )
    dnl use git to record the current repository revision only if
    dnl git is installed and we are in a working copy
    if test "X$svnversioncommand" = "X" || test "X$use_svnfile" = "Xyes"; then
        revision="cat \$(top_srcdir)/svn_dist_version"
        build_num="$($revision)"
        AM_CONDITIONAL([HAVESVNWC], false)
    elif test "X$AONE_BUILD_NUMBER" != "X"; then
        build_num="$AONE_BUILD_NUMBER"
        revision="(cd \$(top_srcdir) && echo $AONE_BUILD_NUMBER-$(git rev-parse HEAD))"
        AM_CONDITIONAL([HAVESVNWC], true)
    else
        build_num="$(git rev-list --all | wc -l)"
        revision="(cd \$(top_srcdir) && echo $build_num-local-$(git rev-parse HEAD))"
        AM_CONDITIONAL([HAVESVNWC], true)
    fi
    dnl substitute revision in build_version.mk
    AC_SUBST(revision)

    dnl define RELEASEID
    AC_ARG_VAR([RELEASEID], [release number when building in Taobao ABS])
    if test "X$RELEASEID" = "X"; then
      RELEASEID="$build_num.el$(cat /etc/redhat-release | cut -d " " -f 7 | cut -d "." -f 1)"
    fi
    AC_DEFINE_UNQUOTED([RELEASEID], "$RELEASEID")

    dnl LDADD
    BIN_LDFLAGS="-lc -lrt -pie"

    AM_LDFLAGS="${AM_LDFLAGS} -static-libstdc++"
    BIN_LDFLAGS="${BIN_LDFLAGS} ${TOOLS_DIR}/lib64/libstdc++.a"
    postdeps_CXX=`echo " $postdeps_CXX " | sed 's, -lstdc++ ,,g'`

    AC_SUBST(BIN_LDFLAGS)

    dnl with-buildtime
    AC_ARG_ENABLE([buildtime],
    AS_HELP_STRING([--enable-buildtime], [auto-update build_time, build_version etc. (default is YES)]),
    [case "${enableval}" in
      yes) buildtime=true ;;
      no)  buildtime=false ;;
      *) AC_MSG_ERROR([bad value ${enableval} for --enable-buildtime]) ;;
    esac],
    [buildtime=true])
    AM_CONDITIONAL([UPDATE_BUILDTIME], [test x$buildtime = xtrue])

    dnl condition WITH_MYSQLTEST
    AC_ARG_WITH([mysqltest],
        AS_HELP_STRING([--with-mysqltest],
                [whether to build mysqltest (default is NO)]),
                 [],
                 [with_mysqltest="no"])
    AM_CONDITIONAL([WITH_MYSQLTEST], test x$with_mysqltest = xyes)

    dnl condition WITH_OBPROXY_MINIDUMP
    AC_ARG_WITH([obproxy-minidump],
        AS_HELP_STRING([--with-obproxy-minidump],
                       [whether use obproxy minidump feature (default is NO)]),
                       [
                           if test "$withval" == "yes"; then
                               obproxy_minidump=yes
                               AM_CXXFLAGS="${AM_CXXFLAGS} -DHAVE_MINIDUMP"
                           fi
                       ],
                       [obproxy_minidump="no"])
    AM_CONDITIONAL([HAVE_MINIDUMP], test x$obproxy_minidump = xyes )

    dnl condition 7U_SUPPORT
    AC_ARG_WITH([7u-support],
        AS_HELP_STRING([--with-7u-support],
                       [with 7u support (default is NO)]),
                       [
                           if test "$withval" == "yes"; then
                               BIN_LDFLAGS="-lc -lrt -pie -pthread ${TOOLS_DIR}/lib64/libstdc++.a"
                           fi
                       ],
                       [])


    AC_CONFIG_FILES([\
                     Makefile \
                     src/Makefile \
                     unittest/Makefile \
                     unittest/obproxy/Makefile
    ])

    # warnings todo
    AM_CXXFLAGS="${AM_CXXFLAGS} -fuse-ld=lld -Wall -Wextra -Wno-ignored-qualifiers -Wno-deprecated-copy -Wno-sign-compare -Wno-varargs -Wno-overloaded-virtual -Wno-sign-conversion -Wno-string-plus-int -Wno-shorten-64-to-32 -Wno-delete-non-abstract-non-virtual-dtor -Wno-overloaded-virtual -Wno-unused-command-line-argument -Wno-inconsistent-missing-override -Wno-mismatched-tags -Wno-dynamic-class-memaccess -Wno-format-security -Wno-reinterpret-base-class -Wl,--allow-multiple-definition"
    AM_CFLAGS="${AM_CFLAGS} -fuse-ld=lld -Wall -Wextra -Wno-unused-parameter -Wno-unused-function -Wno-ignored-qualifiers -Wno-deprecated-copy -Wno-sign-compare -Wno-varargs -Wno-overloaded-virtual -Wno-sign-conversion -Wno-string-plus-int -Wno-shorten-64-to-32 -Wno-delete-non-abstract-non-virtual-dtor -Wno-overloaded-virtual -Wno-unused-command-line-argument -Wno-inconsistent-missing-override -Wno-mismatched-tags -Wno-dynamic-class-memaccess -Wno-format-security -Wno-reinterpret-base-class -Wl,--allow-multiple-definition"

    # gcc toolchain
    AM_CXXFLAGS="${AM_CXXFLAGS} --gcc-toolchain=$TOOLS_DIR"
    AM_CFLAGS="${AM_CFLAGS} --gcc-toolchain=$TOOLS_DIR"

    AM_CXXFLAGS_WITH_ARCH="${AM_CXXFLAGS}"
    AM_CFLAGS_WITH_ARCH="${AM_CFLAGS}"

    AC_OUTPUT

    dnl print global flags
    AC_MSG_RESULT([Global Compile Flags:])
    AC_MSG_RESULT([DEP_DIR=${DEP_DIR}])
    AC_MSG_RESULT([TOOLS_DIR=${TOOLS_DIR}])
    AC_MSG_RESULT([RUNTIME_DIR=${RUNTIME_DIR}])
    AC_MSG_RESULT([AM_CPPFLAGS=${AM_CPPFLAGS}])
    AC_MSG_RESULT([AM_LDFLAGS=${AM_LDFLAGS}])
    AC_MSG_RESULT([AM_CXXFLAGS=${AM_CXXFLAGS}])
    AC_MSG_RESULT([AM_CFLAGS=${AM_CFLAGS}])
    AC_MSG_RESULT([AM_CXXFLAGS_WITH_ARCH=${AM_CXXFLAGS_WITH_ARCH}])
    AC_MSG_RESULT([AM_CFLAGS_WITH_ARCH=${AM_CFLAGS_WITH_ARCH}])
    AC_MSG_RESULT([BIN_LDFLAGS=${BIN_LDFLAGS}])

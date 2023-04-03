# Copyright (c) 2021 OceanBase
# OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
# You can use this software according to the terms and conditions of the Mulan PubL v2.
# You may obtain a copy of Mulan PubL v2 at:
#          http://license.coscl.org.cn/MulanPubL-2.0
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PubL v2 for more details.

Name: %NAME
Version: %VERSION
Release: %{RELEASE}%{?dist}
Summary: OceanBase Database Proxy
Group: Applications/Databases
URL: http://oceanbase.alibaba-inc.com/
Packager: yiming.czw
License: Mulan PubL v2
Prefix: %{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %(pwd)/%{name}-root
Autoreq: no

%description
OceanBase Database Proxy

%define _unpackaged_files_terminate_build 0
%undefine _missing_build_ids_terminate_build 0
%define __debug_install_post %{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} "%{_builddir}/%{?buildsubdir}" %{nil}
%define debug_package %{nil}
%define install_dir /home/admin/obproxy-%{version}

%prep
%setup

%build
mkdir -p lib
cp ${TOOLS_DIR}/lib64/libstdc++.so.6.0.28 lib/libstdc++.so.6

#./configure CXX=${CXX} CC=${CC} --with-gcc-version=9.3.0 --with-so --prefix=%{_prefix} --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common --with-easy-root=/usr --with-easy-lib-path=/usr/lib64 --with-svnfile --enable-shared=default --enable-silent-rules
CPU_CORES=`grep -c ^processor /proc/cpuinfo`
MAKE_ARGS="-j $CPU_CORES"
#make $MAKE_ARGS
#cp src/obproxy/.libs/libobproxy_so.so.0.0.0 lib/libobproxy_so.so

#make distclean >/dev/null 2>&1
#find . -path ./tools/codestyle/astyle/build -prune -o -path ./doc -prune -o -name Makefile -exec rm -f {} \;
#find . -name .deps -prune -exec rm -rf {} \;

./configure CXX=${CXX} CC=${CC} --with-gcc-version=9.3.0 RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common --with-easy-root=/usr --with-easy-lib-path=/usr/lib64 --with-svnfile --enable-shared=default --enable-silent-rules
mkdir -p unittest
make $MAKE_ARGS

%install
make DESTDIR=$RPM_BUILD_ROOT install
mkdir -p $RPM_BUILD_ROOT%{install_dir}/bin
mkdir -p $RPM_BUILD_ROOT%{install_dir}/lib
cp src/obproxy/obproxy $RPM_BUILD_ROOT%{install_dir}/bin
cp -r lib/* $RPM_BUILD_ROOT%{install_dir}/lib
cp script/deploy/obproxyd.sh $RPM_BUILD_ROOT%{install_dir}/bin

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-, admin, admin)
%dir %{install_dir}/bin
%{install_dir}/bin/obproxy
%{install_dir}/bin/obproxyd.sh
%{install_dir}/lib/libstdc++.so.6

%pre
rm -rf %{install_dir}/log
rm -rf %{install_dir}/bin
rm -rf %{install_dir}/etc
rm -rf %{install_dir}/.conf
rm -rf /u01/obproxy/lib
mkdir -p /u01/obproxy/lib

%post
chown -R admin:admin %{install_dir}
ln -s %{install_dir}/lib/libobproxy_so.so /u01/obproxy/lib/libobproxy_so.so.0
ln -s %{install_dir}/lib/libobproxy_so.so /u01/obproxy/lib/libobproxy_so.so
ln -s %{install_dir}/lib/libprotobuf.so.18 /u01/obproxy/lib/libprotobuf.so.18
ln -s %{install_dir}/lib/libstdc++.so.6 /u01/obproxy/lib/libstdc++.so.6

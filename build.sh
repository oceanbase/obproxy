#!/bin/sh

export AUTOM4TE="autom4te"
export AUTOCONF="autoconf"

OS_RELEASE=0
OS_ARCH="x86_64"
version=`cat /proc/version`

MINIDUMP_OPTION=
if [[ "$version" =~ "aarch64" ]]
then
  MINIDUMP_OPTION="--with-obproxy-minidump=no"
else
  MINIDUMP_OPTION="--with-obproxy-minidump=yes"
fi

TOPDIR="$(dirname $(readlink -f "$0"))"
DEP_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/deps/devel
TOOLS_DIR=${TOPDIR}/deps/3rd/usr/local/oceanbase/devtools
RUNTIME_DIR=${TOPDIR}/deps/3rd/usr
CPU_CORES=`grep -c ^processor /proc/cpuinfo`
MAKE_ARGS="-j $CPU_CORES"
BOLT_PATH=${TOPDIR}/deps/3rd/opt/alibaba-cloud-compiler/bin

export DEP_DIR;
export TOOLS_DIR;
export RUNTIME_DIR;

ALL_ARGS=("$@")
 PACKAGE=${2:-obproxy-ce}
OBPROXY_VERSION=${3:-`cat rpm/${PACKAGE}-VER.txt`}
RELEASE=${4:-1}
PREFIX=/home/admin/obproxy
SPEC_FILE=obproxy.spec

source /etc/os-release || exit 1

PNAME=${PRETTY_NAME:-"${NAME} ${VERSION}"}

function compat_centos8() {
  echo "[NOTICE] '$PNAME' is compatible with CentOS 8, use el8 dependencies list"
  OS_RELEASE=8
}

function compat_centos7() {
  echo "[NOTICE] '$PNAME' is compatible with CentOS 7, use el7 dependencies list"
  OS_RELEASE=7
}

function not_supported() {
  echo "[ERROR] '$PNAME' is not supported yet."
}

function version_ge() {
  test "$(awk -v v1=$VERSION_ID -v v2=$1 'BEGIN{print(v1>=v2)?"1":"0"}' 2>/dev/null)" == "1"
}

function get_os_release() {
  OS_ARCH="$(uname -m)" || exit 1
  if [[ "${OS_ARCH}x" == "x86_64x" ]]; then
    case "$ID" in
      alinux)
        version_ge "3" && OS_RELEASE=8 && return
        version_ge "2.1903" && OS_RELEASE=7 && return
        ;;
      alios)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.2" && compat_centos7 && return
        ;;
      anolis)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.0" && compat_centos7 && return
        ;;
      ubuntu)
        version_ge "16.04" && compat_centos7 && return
        ;;
      centos)
        version_ge "8.0" && OS_RELEASE=8 && return
        version_ge "7.0" && OS_RELEASE=7 && return
        ;;
      almalinux)
        version_ge "8.0" && compat_centos8 && return
        ;;
      debian)
        version_ge "9" && compat_centos7 && return
        ;;
      fedora)
        version_ge "33" && compat_centos7 && return
        ;;
      opensuse-leap)
        version_ge "15" && compat_centos7 && return
        ;;
      #suse
      sles)
        version_ge "15" && compat_centos7 && return
        ;;
      uos)
        version_ge "20" && compat_centos7 && return
        ;;
    esac
  elif [[ "${OS_ARCH}x" == "aarch64x" ]]; then
    case "$ID" in
      alinux)
        version_ge "3" && OS_RELEASE=8 && return
        ;;
      alios)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.0" && compat_centos7 && return
        ;;
      centos)
        version_ge "8.0" && OS_RELEASE=8 && return
        version_ge "7.0" && OS_RELEASE=7 && return
        ;;
    esac
  fi
  not_supported && return 1
}

function do_init()
{
  set -x
  aclocal
  libtoolize --force --copy --automake
  autoconf --force
  automake --foreign --copy --add-missing -Woverride -Werror

}

function do_dep_init()
{
  cd $TOPDIR/deps/3rd && bash dep_create.sh
  cd $TOPDIR
  do_init
}

function do_clean()
{
  echo 'cleaning...'
  make distclean >/dev/null 2>&1
  rm -rf autom4te.cache
  for fn in aclocal.m4 configure config.guess config.sub depcomp install-sh \
	ltmain.sh libtool missing mkinstalldirs config.log config.status Makefile; do
	rm -f $fn
  done

  find . -name Makefile.in -exec rm -f {} \;
  find . -path ./tools/codestyle/astyle/build -prune -o -path ./doc -prune -o -name Makefile -exec rm -f {} \;
  find . -name .deps -prune -exec rm -rf {} \;
  echo 'done'
}

function do_config()
{
  set -x
  get_os_release

  case "x$1" in
    xdebug)
      # configure for developers
      ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no
      echo -e "\033[31m ===build debug version=== \033[0m"
      ;;
    xgcov)
      # configure for gcov
      ./configure --with-coverage=yes --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no
      echo -e "\033[31m ===build gcov version=== \033[0m"
      ;;
    xasan)
      # configure for asan
     ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no --with-asan
      echo -e "\033[31m ===build asan version=== \033[0m"
      ;;
    xso)
      # configure for obproxy_so
      ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no --with-release --with-so
      echo -e "\033[31m ===build so version=== \033[0m"
      ;;
    xerrsim)
     # configure for error injection
      ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no --with-errsim=yes
      echo -e "\033[31m ===build errsim version=== \033[0m"
       ;;
    xopt)
      ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no --with-release --with-opt
      echo -e "\033[31m ===build release version with compile optimization(BOLT & AutoFDO) === \033[0m"
      ;;
    *)
      # configure for release
      ./configure --with-coverage=no --enable-buildtime=no --enable-strip-ut=no --enable-silent-rules --enable-dlink-observer=no --with-release
      echo -e "\033[31m ===build release version=== \033[0m"
      ;;
  esac
}

function do_make()
{
  set -x
  make $MAKE_ARGS
}


function do_rpm()
{
  set -x
  echo "[BUILD] make dist..."
  make dist-gzip || exit 1

  TMP_DIR=/${TOPDIR}/obproxy-tmp.$$
  echo "[BUILD] create tmp dirs...TMP_DIR=${TMP_DIR}"
  mkdir -p ${TMP_DIR}
  mkdir -p ${TMP_DIR}/BUILD
  mkdir -p ${TMP_DIR}/RPMS
  mkdir -p ${TMP_DIR}/SOURCES
  mkdir -p ${TMP_DIR}/SRPMS
  cp ${PACKAGE}-${OBPROXY_VERSION}.tar.gz ${TMP_DIR}/SOURCES
  cd ${TMP_DIR}/BUILD
  echo "[BUILD] make rpms..._prefix=${PREFIX} spec_file=${SPEC_FILE}"
  rpmbuild --define "_topdir ${TMP_DIR}" --define "NAME ${PACKAGE}" --define "VERSION ${OBPROXY_VERSION}" --define "_prefix ${PREFIX}" --define "RELEASEID ${RELEASE}" --define "rpm_path ${TOPDIR}" -ba ${TOPDIR}/deps/3rd/${SPEC_FILE} || exit 2
  echo "[BUILD] make rpms done."

  cd ${TOPDIR}
  find ${TMP_DIR}/RPMS/ -name "*.rpm" -exec mv '{}' ./ \;
  rm -rf ${TMP_DIR}
}

case "x$1" in
xinit)
  do_dep_init
	;;
xqinit)
  do_init
	;;
xclean)
  do_clean
	;;
xconfig)
  do_config $2
  ;;
xmake)
  do_make
  ;;
xrpm)
  do_dep_init
  do_config release
  do_rpm
  ;;
*)
  do_dep_init
  do_config release
  do_make
  ;;
esac

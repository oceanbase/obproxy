#!/bin/bash
# use by rpm build platform

SCRIPT_DIR=$(cd "$(dirname "$0")";pwd)
TOP_DIR=${1:-${SCRIPT_DIR}/../}
PACKAGE=$2
VERSION=$3
RELEASE=$4

# prepare rpm build dirs
cd $TOP_DIR
sh build.sh clean

# build rpm
cd $TOP_DIR
sh build.sh rpm $PACKAGE $VERSION $RELEASE

find $TOP_DIR/ -name "*.rpm" -maxdepth 1 -exec mv {} $SCRIPT_DIR 2>/dev/null \;
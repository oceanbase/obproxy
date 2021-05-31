EXTRA_DIST=build.sh svn_dist_version script/deploy unittest/Makefile.in unittest/obproxy/Makefile.in deps/easy
bin_SCRIPTS=script/deploy/obproxyd.sh

if WITH_TEST_CASE
TEST_DIR:=unittest
endif

SUBDIRS=src $(TEST_DIR)
DIST_SUBDIRS=src $(TEST_DIR)

utest: check
.PHONY: utest

# rules to auto generate build_version.c
include $(top_srcdir)/build_version.mk

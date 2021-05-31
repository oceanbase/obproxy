AM_CPPFLAGS += -I${top_srcdir}/src

# define empty targets
noinst_LTLIBRARIES=
lib_LTLIBRARIES=
bin_PROGRAMS=

EXTRA_DIST=

# modules
include $(srcdir)/lib/Makemodule.am
include $(srcdir)/common/Makemodule.am
include $(srcdir)/rpc/Makemodule.am
include $(srcdir)/share/Makemodule.am
include $(srcdir)/sql/Makemodule.am
include $(srcdir)/obproxy/Makemodule.am
include $(srcdir)/lib/compress/Makemodule.am

# rules to auto generate svn_version.c
include $(top_srcdir)/build_version.mk

#SUBDIRS=. liboblog


clean-local:
	-rm -f *.gcov *.gcno *.gcda

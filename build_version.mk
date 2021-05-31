BUILT_SOURCES=build_version.c
CLEANFILES=build_version.c

if UPDATE_BUILDTIME

if HAVESVNWC
$(top_srcdir)/svn_dist_version: $(top_srcdir)/FORCE
	@revision@ > $@
endif

build_version.c: $(top_srcdir)/FORCE
	echo -n 'const char* build_version() { return "' > $@ && @revision@ | tr -d '\n' >> $@ && echo '"; }' >> $@
	echo 'const char* build_date() { return __DATE__; }' >> $@
	echo 'const char* build_time() { return __TIME__; }' >> $@
	echo -n 'const char* build_flags() { return "' >> $@ && echo -n $(AM_CXXFLAGS) $(CXXFLAGS) |sed s/\"//g >> $@ && echo '"; }' >> $@

$(top_srcdir)/FORCE:

else

if HAVESVNWC
$(top_srcdir)/svn_dist_version:
	@revision@ > $@
endif

build_version.c:
	echo -n 'const char* build_version() { return "' > $@ && @revision@ | tr -d '\n' >> $@ && echo '"; }' >> $@
	echo 'const char* build_date() { return __DATE__; }' >> $@
	echo 'const char* build_time() { return __TIME__; }' >> $@
	echo -n 'const char* build_flags() { return "' >> $@ && echo -n $(AM_CXXFLAGS) $(CXXFLAGS) |sed s/\"//g >> $@ && echo '"; }' >> $@

endif

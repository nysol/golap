
#EXTRA_DIST =$(wildcard *.h) $(wildcard dat/*.h)

AM_CXXFLAGS = -std=gnu++11 -fpermissive -I$(top_srcdir)/golap/mod -I$(top_srcdir)/golap/lib @XML2HEAD@


bin_PROGRAMS = \
cregdb 

### excmd command end ###

cregdb_SOURCES = cregdb.cpp
#cregdb_LDADD = -lkgmod3  $(top_builddir)/golap/mod/libgolapmod@MOD_MAJOR_VERSION@.la $(top_builddir)/golap/lib/libgolaplib@MOD_MAJOR_VERSION@.la
cregdb_LDADD = -lkgmod3 @BTHREAD@ $(top_builddir)/golap/mod/libgolapmod@MOD_MAJOR_VERSION@.la $(top_builddir)/golap/lib/libgolaplib@MOD_MAJOR_VERSION@.la

#golap_SOURCES = golap.cpp
#golap_LDADD = -lkgmod3 $(top_builddir)/golap/mod/libgolapmod@MOD_MAJOR_VERSION@.la $(top_builddir)/golap/lib/libgolaplib@MOD_MAJOR_VERSION@.la
#golap_LDADD = -lkgmod3 @BTHREAD@ $(top_builddir)/golap/mod/libgolapmod@MOD_MAJOR_VERSION@.la $(top_builddir)/golap/lib/libgolaplib@MOD_MAJOR_VERSION@.la

install-exec-hook:
if DARWIN_CERT 
	list='$(bin_PROGRAMS)'; \
	for p in $$list; do codesign -s nysol-cert "$(DESTDIR)$(bindir)/$$p"; done
endif

pinstall: install 

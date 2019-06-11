#SUBDIRS=include .
#EXTRA_DIST =$(wildcard *.h) $(wildcard ../help/*.h )
#EXTRA_DIST = dat/*.h

AM_CXXFLAGS = -std=gnu++11 -I. -I../ -I ../lib @XML2HEAD@


## Define a libtool archive target "libexample-@EXAMPLE_API_VERSION@.la", with
## @EXAMPLE_API_VERSION@ substituted into the generated Makefile at configure
## time.
## The libtool archive file (.la) will be installed into the directory named
## by the predefined variable $(bindir), along with the actual shared library
## file (.so).
lib_LTLIBRARIES = libgolapmod@MOD_MAJOR_VERSION@.la

## Instruct libtool to include ABI version information in the generated shared
## library file (.so).  The library ABI version is defined in configure.ac, so
## that all version information is kept in one place.
libgolapmod@MOD_MAJOR_VERSION@_la_LDFLAGS = -version-info $(MOD_SO_VERSION) -single_module @XML2LIB@

#libgolapmod@MOD_MAJOR_VERSION@_la_LIBADD = $(top_srcdir)/kgmod/libkgmod@MOD_MAJOR_VERSION@.la

libgolapmod@MOD_MAJOR_VERSION@_la_SOURCES = \
kgcregdb.cpp \
kggolap.cpp \
kgcregdb.hpp \
kggolap.hpp


pkgconfigdir = $(libdir)/pkgconfig

pinstall: install 

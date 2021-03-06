#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
# generate　configure :
# run 
#  aclocal ; autoreconf -i ; automake --add-missing 
#  or
#  autoreconf -i ; automake --add-missing

# AC_PREREQ([2.69])
AC_INIT(nysol_golap, 1.0, info@nysol.jp)
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([golap/cmd/golap.cpp])

# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AC_PROG_LN_S
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stdlib.h string.h sys/time.h unistd.h wchar.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T

AC_SUBST([MOD_MAJOR_VERSION], ["1"])
AC_SUBST([MOD_SO_VERSION], ["1:0:0"])

# Checks for library functions.
AC_CHECK_FUNCS([floor isascii memset mkdir mkfifo modf pow select setlocale sqrt strchr strerror strstr])


XML2HEAD=`xml2-config --cflags`
XML2LIB=`xml2-config --libs`
AC_SUBST(XML2HEAD)
AC_SUBST(XML2LIB)

# BOOST PATH　指定
AC_ARG_WITH([boost],
  AC_HELP_STRING(
    [--with-boost=PATH],[PATH is path to boost library]),
  [BOOSTPATH=$with_boost],[BOOSTPATH=/usr/local])

CXXFLAGS="-I $BOOSTPATH/include $CXXFLAGS"

AC_SUBST([BOOSTPATH], [$BOOSTPATH])

BTHREAD=""
AC_CHECK_LIB([kgmod3], [main], [], [echo "Error. You need to have libkgmod3";exit -1])
AC_CHECK_LIB([boost_system], [main], [], [echo "Error. You need to have libboost";exit -1])
AC_CHECK_LIB([boost_filesystem], [main], [], [echo "Error. You need to have libboost";exit -1])
AC_CHECK_LIB([boost_thread], [main], [BTHREAD="-lboost_thread"], [])
AC_CHECK_LIB([boost_thread-mt], [main], [BTHREAD="-lboost_thread-mt"], [])

if test -z $BTHREAD ; then
	echo "Error. You need to have libboost_thread or libboost_thread-mt";
	exit -1;
fi

AC_SUBST(BTHREAD)



#OSチェック
AM_CONDITIONAL(DARWIN_CERT, false)
AC_CANONICAL_HOST
case $host_os in
  darwin* )
		#証明書チェック
		AS_IF([test "x$(security find-identity -p codesigning -v | grep nysol-cert | head -n 1)" != x],
			[AM_CONDITIONAL(DARWIN_CERT, true)]
		)
		;;
esac

# configure parameter
AC_ARG_ENABLE([jpnformat],
	[AS_HELP_STRING([--enable-jpnformat],
	    [jpnformat (default is yes)])],
	[],
	[enable_jpnformat=yes] 
)
AS_IF([test "x$enable_jpnformat" != xno],
    [AC_DEFINE([JPN_FORMAT], [1], [jpnformat])]
)

AC_CONFIG_FILES([Makefile
                 golap/Makefile
                 golap/lib/Makefile
                 golap/mod/Makefile
                 golap/cmd/Makefile])

AC_OUTPUT



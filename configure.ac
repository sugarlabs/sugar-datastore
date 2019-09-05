AC_INIT([sugar-datastore],[0.116],[],[sugar-datastore])

AC_PREREQ([2.59])

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([configure.ac])

AM_INIT_AUTOMAKE([1.9 foreign dist-xz no-dist-gzip])

AM_MAINTAINER_MODE

AC_DISABLE_STATIC
AC_PROG_LIBTOOL

PYTHON=python3
AM_PATH_PYTHON([3])
AM_CHECK_PYTHON_HEADERS(,[AC_MSG_ERROR(could not find Python headers)])

AC_OUTPUT([
Makefile
bin/Makefile
etc/Makefile
src/Makefile
src/carquinyol/Makefile
])

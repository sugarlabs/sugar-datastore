AC_INIT([sugar-datastore],[0.87.3],[],[sugar-datastore])

AC_PREREQ([2.59])

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([configure.ac])

AM_INIT_AUTOMAKE([1.9 foreign dist-bzip2 no-dist-gzip])

AM_MAINTAINER_MODE

AC_DISABLE_STATIC
AC_PROG_LIBTOOL

AM_PATH_PYTHON([2.5])
AM_CHECK_PYTHON_HEADERS(,[AC_MSG_ERROR(could not find Python headers)])

AC_OUTPUT([
Makefile
bin/Makefile
etc/Makefile
src/Makefile
src/carquinyol/Makefile
])

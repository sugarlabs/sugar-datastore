datastoredir = $(pythondir)/carquinyol
datastore_PYTHON = 		\
	__init__.py		\
	datastore.py		\
	filestore.py		\
	indexstore.py		\
	layoutmanager.py	\
	metadatastore.py	\
	migration.py		\
	optimizer.py

AM_CPPFLAGS = 			\
	$(WARN_CFLAGS)		\
	$(EXT_CFLAGS)		\
	$(PYTHON_INCLUDES)

AM_LDFLAGS = -module -avoid-version

pkgpyexecdir = $(pythondir)/carquinyol
pkgpyexec_LTLIBRARIES = metadatareader.la

metadatareader_la_SOURCES = 	\
	metadatareader.c

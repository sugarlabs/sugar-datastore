servicedir = $(datadir)/dbus-1/services
service_in_files =	\
	org.laptop.sugar.DataStore.service.in	\
	org.laptop.sugar.Indexer.service.in
service_DATA = $(service_in_files:.service.in=.service)

org.laptop.sugar.DataStore.service: org.laptop.sugar.DataStore.service.in
	@sed -e "s|\@bindir\@|$(bindir)|" $< > $@

org.laptop.sugar.Indexer.service: org.laptop.sugar.Indexer.service.in
	@sed -e "s|\@bindir\@|$(bindir)|" $< > $@

DISTCLEANFILES = $(service_DATA)

EXTRA_DIST = $(service_in_files)

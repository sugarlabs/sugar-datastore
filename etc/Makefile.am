servicedir = $(datadir)/dbus-1/services
service_in_files = org.laptop.Sugar.Datastore.in
service_DATA = $(service_in_files:.service.in=.service)

$(service_DATA): $(service_in_files) Makefile
	@sed -e "s|\@bindir\@|$(bindir)|" $< > $@

DISTCLEANFILES = $(service_DATA)

EXTRA_DIST = $(service_in_files)

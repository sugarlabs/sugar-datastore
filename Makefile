export PYTHON=python
export PROJECTNAME=olpc.datastore

CURRENT_VERSION=`shtool version -l python __version__.py`

all: build test

build: 
	@${PYTHON} setup.py build

test: build
	@${MAKE} -C tests test

doc: 
	@${MAKE} -C docs all

clean: 
	@find . -name "*.pyc" -exec rm {} \;
	@find . -name "*~" -exec rm {} \;
	@find src -name "*.c" -exec rm {} \;
	@find src -name "callgrind.out.*" -exec rm {} \;	
	@find src -name "core" -type f -exec rm {} \;	
	@rm -rf build	
	@${MAKE} -C docs clean

tags:
	@ctags -e -R -o TAGS

version:
	@echo Incrementing version level for package
	@shtool version -l python -n ${PROJECTNAME} -d long -i l __version__.py

install: build
	@${PYTHON} setup.py install

develop: build
	@${PYTHON} setup.py develop

release:
	@$(PYTHON) setup.py sdist
	@$(PYTHON) setup.py bdist_egg
	@echo Release ready, check dist/

upload:
	@$(PYTHON) setup.py sdist upload
	@$(PYTHON) setup.py bdist_egg upload

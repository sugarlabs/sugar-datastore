# howto inherit this properly from above?
# its not an option to configure
PYTHON=python

all: test

test: 
	@rm -rf fulltext 
	@${PYTHON} runalltests.py 

valgrind:
	@echo "Profiling the process. Run kcachegrind on the output"
	valgrind  --tool=callgrind --suppressions=valgrind-python.supp ${PYTHON} runalltests.py

profile:
	@find . -name "hotspot*" -exec rm {} \;
	@${PYTHON} ./profilealltests.py

clean: 
	@find . -name "*.pyc" -exec rm {} \;
	@find . -name "*~" -exec rm {} \;
	@find . -name "hotspot*" -exec rm {} \;
	@find . -name "callgrind.out*" -exec rm {} \;

tags:



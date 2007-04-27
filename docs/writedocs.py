#!/usr/bin/python
import pydoc
import sys, os


# IF YOU ARE NOT GETTING THE RESULTS YOU EXPECT WHILE TESTING
# THIS IS THE LIKELY CAUSE
# :: Use distutils to modify the pythonpath for inplace testing
from distutils.util import get_platform
plat_specifier = ".%s-%s" % (get_platform(), sys.version[0:3])
build_platlib = os.path.join("build", 'lib' + plat_specifier)
test_lib = os.path.join(os.path.abspath(".."), build_platlib)
sys.path.insert(0, test_lib)
# END PATH ADJUSTMENT CODE

sys.path.insert(0, sys.argv[1])

import olpc.datastore

pydoc.writedocs(olpc.datastore.__path__[0], "%s." % olpc.datastore.__name__)

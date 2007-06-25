#!/usr/bin/python
#
# Runs all tests in the current directory
#
# Execute like:
#   python runalltests.py
#
# Alternatively use the testrunner:
#   python /path/to/Zope/utilities/testrunner.py -qa
#

import os, sys
import unittest
import doctest
from pkg_resources import resource_filename

from sqlalchemy import clear_mappers

doctests = [
    resource_filename(__name__, "query.txt"),
    resource_filename(__name__, "milestone_1.txt"),
    resource_filename(__name__, "sugar_demo_may17.txt"),
    resource_filename(__name__, "milestone_2.txt"),
    resource_filename(__name__, "mountpoints.txt")
    
]

doctest_options = doctest.ELLIPSIS
doctest_options |= doctest.REPORT_ONLY_FIRST_FAILURE


# IF YOU ARE NOT GETTING THE RESULTS YOU EXPECT WHILE TESTING
# THIS IS THE LIKELY CAUSE
# :: Use distutils to modify the pythonpath for inplace testing
# using the build directory
from distutils.util import get_platform
plat_specifier = ".%s-%s" % (get_platform(), sys.version[0:3])
build_platlib = os.path.join("build", 'lib' + plat_specifier)
test_lib = os.path.join(os.path.abspath(".."), build_platlib)
sys.path.insert(0, test_lib)
# END PATH ADJUSTMENT CODE



def tearDownDS(test):
    # reset the module global mappers used in SQLAlchemy between tests
    clear_mappers()
    # and remove the test repository used in some tests
    os.system('rm -rf /tmp/test_ds')
    
def test_suite():
    suite = unittest.TestSuite()
    for dt in doctests:
        suite.addTest(doctest.DocFileSuite(dt,
    optionflags=doctest_options, tearDown=tearDownDS))

    tests = os.listdir(os.curdir)
    tests = [n[:-3] for n in tests if n.startswith('test') and
             n.endswith('.py')]

    for test in tests:
        m = __import__(test)
        if hasattr(m, 'test_suite'):
            suite.addTest(m.test_suite())
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=1)
    runner.run(test_suite())
                        

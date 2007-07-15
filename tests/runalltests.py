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
import logging

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)-15s %(name)s %(levelname)s: %(message)s",
                    stream=sys.stderr)


doctests = [
    resource_filename(__name__, "xapianindex.txt"),
    resource_filename(__name__, "milestone_1.txt"),
    resource_filename(__name__, "sugar_demo_may17.txt"),
    resource_filename(__name__, "milestone_2.txt"),
    resource_filename(__name__, "mountpoints.txt"),
    resource_filename(__name__, "properties.txt"),
    
]

doctest_options = doctest.ELLIPSIS
doctest_options |= doctest.REPORT_ONLY_FIRST_FAILURE


def test_suite():
    global doctests
    suite = unittest.TestSuite()
    if len(sys.argv) > 1:
        doctests = sys.argv[1:]
        
    for dt in doctests:
        suite.addTest(doctest.DocFileSuite(dt,
    optionflags=doctest_options))

    if len(sys.argv) <= 1:
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
    suite = test_suite()
    runner.run(suite)
                        

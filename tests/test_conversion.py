import unittest

from olpc.datastore.converter import converter
from StringIO import StringIO

class Test(unittest.TestCase):

    def test_unicode(self):
        # read each of the test files in doing conversion,
        # there should be no unicode errors
        fn_expectations = {
            'test.pdf' : 'Don\'t',
            'test.doc' : 'amazed.',
            'test.odt' : 'amazed.',
            'plugger.pdf' : 'Plugger'
            }
        for fn, expect in fn_expectations.iteritems():
            assert expect in converter(fn).read()

        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    

import unittest

from olpc.datastore.converter import Purify, converter
from StringIO import StringIO

class Test(unittest.TestCase):

    def setUp(self):
        self.file = StringIO(u"Don't\xa0peek.\n\n\x0c")
        
    def test_stripper_read(self):
        s = Purify(self.file)
        d = s.read()
        assert '\xa0' not in d

    def test_stripper_iter(self):
        s = Purify(self.file)
        # make sure we can iter it
        for line in s:
            # and that we are getting string
            # (not unicode) back
            assert isinstance(line, str)

    def test_unicode(self):
        # read each of the test files in doing conversion,
        # there should be no unicode errors
        fn_expectations = {
            'test.pdf' : 'Don\'t',
            'test.doc' : 'amazed.',
            'test.odt' : 'amazed.'
            }
        for fn, expect in fn_expectations.iteritems():
            assert expect in converter(fn).read()

        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    

import unittest

from olpc.datastore import model 
import datetime
import time

class Test(unittest.TestCase):
    def test_dateproperty(self):
        n = datetime.datetime.now()
        # we have to kill the microseconds as
        # time.strptime which we must use in 2.4 doesn't parse it
        n = n.replace(microsecond=0)
        p = model.DateProperty('ctime', n)
        assert p.key == "ctime"
        assert p.value.isoformat() == n.isoformat()

        
def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    return suite

if __name__ == "__main__":
    unittest.main()
                    

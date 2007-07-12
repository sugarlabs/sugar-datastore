#!/usr/bin/python
import os
import re
from ore.main import Application

filepattern = re.compile("(\w{8})\-(\w{4})\-(\w{4})\-(\w{4})\-(\w{12})")
tmppattern = re.compile("tmp\S{6}")

staticdirs = re.compile('test_ds|store\d')

filepatterns = [filepattern, tmppattern]
dirpatterns = [staticdirs]

class Cleaner(Application):
    def manage_options(self):
        self.parser.add_option("--base", dest="base_dir",
                               action="store", default='/tmp',
                               help="""Where to clean (/tmp)""")

    def main(self):
        """clean up files left from testing in /tmp"""
        # this is done using patterned names
        for root, dirs, files in os.walk(self.options.base_dir):
            for filename in files:
                for pat in filepatterns:
                    if pat.match(filename):
                        fn = os.path.join(root, filename)
                        os.remove(fn)
                        break
            for dirname in dirs:
                for pat in dirpatterns:
                    if pat.match(dirname):
                        dn = os.path.join(root, dirname)
                        os.system('rm -rf %s' % dn)
                        
if __name__ == "__main__":
    Cleaner("cleaner")()



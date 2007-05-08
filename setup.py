import sys
sys.path.insert(0, 'src') # for version

from setuptools import setup, find_packages

from olpc.datastore.__version__ import version

DEPENDS = ['sqlalchemy >= 0.3.6', ]


setup(
    name="olpc.datastore",
    version=version.v_short,
    packages=find_packages('src', exclude=["*.tests"]),
    package_dir= {'':'src'},
    namespace_packages=['olpc'],
    package_data = {
    '': ['*.txt', '*.db', '*.png', '*.svg'],
    },
    install_requires = DEPENDS,
    zip_safe=True,
    author='Benjamin Saller',
    author_email='bcsaller@objectrealms.net',
    description="""\
    DataStore used for the OLPC project (http://laptop.org)
    """,
    license='GPL',
    keywords="datastore journal repository",
    )

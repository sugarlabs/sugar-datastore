from setuptools import setup, find_packages
import sys, os

sys.path.insert(0,
                os.path.join(os.path.dirname(__file__),'src')) # for version


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
    data_files = [
        ('/usr/share/dbus-1/services', ['etc/org.laptop.sugar.DataStore.service']),
    ],
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

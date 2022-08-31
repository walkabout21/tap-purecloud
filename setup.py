#!/usr/bin/env python

from setuptools import setup

setup(name='tap-purecloud',
      version='0.0.11',
      description='Singer.io tap for extracting data from the Genesys Purecloud API',
      author='Fishtown Analytics',
      url='http://fishtownanalytics.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_purecloud'],
      install_requires=[
          'singer-python==5.12.0',
          'backoff==1.8.0',
          'requests==2.25.1',
          'python-dateutil==2.6.0',
          'PureCloudPlatformClientV2==151.0.0',
          # Using Python 3.7 so upgrade from 5.0.1 to 6.0.0
          # See https://github.com/aaugustin/websockets/issues/432
          'websockets==6.0.0'
      ],
      entry_points='''
          [console_scripts]
          tap-purecloud=tap_purecloud:main
      ''',
      packages=['tap_purecloud'],
      package_data={
        'schemas': ['tap_purecloud/schemas/*.json']
      },
      include_package_data=True,
)

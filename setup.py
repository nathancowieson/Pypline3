from setuptools import setup

setup(name='Pypline3',
      version='3.1',
      description='Data reduction pipeline for SAXS data on B21',
      url='http://github.com/nathancowieson/Pypline3',
      author='Nathan Cowieson',
      author_email='nathan.cowieson@diamond.ac.uk',
      license='MIT',
      packages=['Pypline3'],
      install_requires=[
          'pyepics',
          'pyyaml',
          'numpy',
          'h5py',
          'nexusformat',
          'linecache2',
          'nose2',
          'scipy',
          'setuptools',
          'six',
          'traceback2',
          'wheel',
          'python-dateutil'
      ],
      test_suite='bin/nose2.collector.collector',
      tests_require=['unittest2', 'nose2'],
      zip_safe=False
      )

"""
subsdatasink: Datasink for subsserver simple data protocol.
The datasink will store received data in a postgresql database.
"""

from setuptools import setup
from os.path import join as pjoin

doclines = __doc__.split("\n")

setup(name='subsdatasink',
      version='0.1.0',
      description='Datasink for subsserver simple data protocol.',
      long_description='\n'.join(doclines[2:]),
      url='http://github.com/proactivity-lab/subsserver-datasink',
      author='Raido Pahtma',
      author_email='raido.pahtma@ttu.ee',
      license='MIT',
      platforms=['any'],
      packages=['subsdatasink'],
      install_requires=["argconfparse", "flask", "flask-restful", "flask-httpauth", "tornado", "psycopg2"],
      test_suite='nose.collector',
      tests_require=['nose'],
      scripts=[pjoin('bin', 'subsdatasink')],
      zip_safe=False)

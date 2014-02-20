import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = [
    'PyWPS',
    'lxml',
    #'htmltmpl',
    'python-magic',
    'MyProxyClient',
    'netCDF4',
    'pymongo',
    'nose',
    'c3meta',
    ]

classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        ]

setup(name='Malleefowl',
      version='0.1',
      description='Climate Data Processing with PyWPS',
      long_description=README + '\n\n' + CHANGES,
      classifiers=classifiers,
      author='Carsten Ehbrecht',
      author_email='ehbrecht@dkrz.de',
      url='http://www.dkrz.de',
      license = "http://www.gnu.org/licenses/gpl.html",
      keywords='wps PyWPS Python Malleefowl netcdf esgf',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      test_suite='nose.collector',
      install_requires=requires,
      entry_points = {
          'console_scripts': [
              'wpsclient=malleefowl.wpsclient:main',
              ]}     
      ,
      )

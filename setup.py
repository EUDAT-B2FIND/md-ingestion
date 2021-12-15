#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages
from setuptools import setup

__author__ = "Heinrich Widmann"
__contact__ = "widmann@dkrz.de"
__copyright__ = "Copyright (c) 2013 Heinrich Widmann (DKRZ)"
__version__ = "3.1.0"
__license__ = "BSD"


from setuptools import setup, find_packages

# One strategy for storing the overall version is to put it in the top-level
# package's __init__ but Nb. __init__.py files are not needed to declare
# packages in Python 3
# from clisops import __version__ as _package_version

# Populate long description setting with content of README
#
# Use markdown format read me file as GitHub will render it automatically
# on package page
with open("README.md") as readme_file:
    _long_description = readme_file.read()


requirements = [line.strip() for line in open('requirements.txt')]

setup_requirements = ['pytest-runner', ]

test_requirements = ["pytest"]

docs_requirements = [
    "sphinx",
]


# dev_requirements = [line.strip() for line in open('requirements_dev.txt')]

setup(
    author=__author__,
    author_email=__contact__,

    # See:
    # https://www.python.org/dev/peps/pep-0301/#distutils-trove-classification
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration :: Authentication/Directory',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    description="b2find - metadata ingestion.",

    license=__license__,

    # This qualifier can be used to selectively exclude Python versions -
    # in this case early Python 2 and 3 releases
    python_requires='>=3.6.0',
    entry_points={
        'console_scripts': [
            'b2f=mdingestion.cli:cli',
        ],
    },
    install_requires=requirements,
    long_description=_long_description,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='b2find',
    name='mdingestion',
    packages=find_packages(include=['mdingestion', 'mdingestion.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    # extras_require={"docs": docs_requirements},
    url='https://github.com/EUDAT-B2FIND/md-ingestion',
    version=__version__,
    zip_safe=False,
)

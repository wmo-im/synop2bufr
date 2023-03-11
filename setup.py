###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import io
import os
import re
from setuptools import Command, find_packages, setup


class PyTest(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import subprocess
        errno = subprocess.call(['pytest'])
        raise SystemExit(errno)


def read(filename, encoding='utf-8'):
    """read file contents"""
    full_path = os.path.join(os.path.dirname(__file__), filename)
    with io.open(full_path, encoding=encoding) as fh:
        contents = fh.read().strip()
    return contents


def get_package_version():
    """get version from top-level package init"""
    version_file = read('synop2bufr/__init__.py')
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


KEYWORDS = [
    'WMO',
    'SYNOP',
    'BUFR',
    'decoding',
    'weather',
    'observations'
]

DESCRIPTION = 'Convert a SYNOP TAC messages or a SYNOP file to BUFR4.'

# ensure a fresh MANIFEST file is generated
if (os.path.exists('MANIFEST')):
    os.unlink('MANIFEST')


setup(
    name='synop2bufr',
    version=get_package_version(),
    description=DESCRIPTION,
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    license='Apache Software License',
    platforms='all',
    keywords=' '.join(KEYWORDS),
    author='Rory Burke',
    author_email='RBurke@wmo.int',
    maintainer='David I. Berry',
    maintainer_email='DBerry@wmo.int',
    install_requires=read('requirements.txt').splitlines(),
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
    ],
    entry_points={
        'console_scripts': [
            'synop2bufr=synop2bufr.cli:cli'
        ]
    },
    cmdclass={'test': PyTest},
    test_suite='tests.run_tests'
)

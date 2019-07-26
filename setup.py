#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

# the name of the project
name = 'yarn_kernel_provider'

import os
import sys
from setuptools import setup
from setuptools.command.bdist_egg import bdist_egg

v = sys.version_info
if v[:2] < (3, 4):
    error = "ERROR: %s requires Python version 3.4 or above." % name
    print(error, file=sys.stderr)
    sys.exit(1)

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))
pkg_root = pjoin(here, name)

packages = []
for d, _, _ in os.walk(pjoin(here, name)):
    if os.path.exists(pjoin(d, '__init__.py')):
        packages.append(d[len(here)+1:].replace(os.path.sep, '.'))

version_ns = {}
with open(pjoin(here, name, '_version.py')) as f:
    exec(f.read(), {}, version_ns)

class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg

    Prevents setup.py install from performing setuptools' default easy_install,
    which it should never ever do.
    """
    def run(self):
        sys.exit("Aborting implicit building of eggs. Use `pip install .` to install from source.")


setup_args = dict(
    name            = name,
    version         = version_ns['__version__'],
    packages        = packages,
    description     = "Provides support for launching Jupyter kernels within a YARN cluster",
    author          = 'Jupyter Development Team',
    author_email    = 'jupyter@googlegroups.com',
    url             = 'https://jupyter.org',
    license         = 'BSD',
    classifiers     = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    install_requires = [
        'remote_kernel_provider',
        'jupyter_kernel_mgmt',
        'entrypoints',
        'yarn-api-client>=0.3.6',
    ],
    extras_require   = {
        'test': ['mock', 'pytest', 'pytest-console-scripts'],
    },
    entry_points={
        'console_scripts': [
            'jupyter-yarn-kernelspec = yarn_kernel_provider.kernelspecapp:YarnKernelProviderApp.launch_instance',
        ],
        'jupyter_kernel_mgmt.kernel_type_providers' : [
            'yarn = yarn_kernel_provider.provider:YarnKernelProvider',
        ]
    },
    python_requires = ">=3.4",
    cmdclass         = {
        'bdist_egg': bdist_egg if 'bdist_egg' in sys.argv else bdist_egg_disabled,
    },
    include_package_data = True,
)

if __name__ == '__main__':
    setup(**setup_args)

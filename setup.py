#!/usr/bin/env python
# pylint: disable=W0142,W0403,W0404,W0613,W0622,W0622,W0704,R0904,C0103,E0611
#
# -*- coding: utf-8 -*-
# copyright 2016 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
# contact http://www.logilab.fr -- mailto:contact@logilab.fr
#
# This software is governed by the CeCILL license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL
# license as circulated by CEA, CNRS and INRIA at the following URL
# "http://www.cecill.info".
#
# As a counterpart to the access to the source code and rights to copy,
# modify and redistribute granted by the license, users are provided only
# with a limited warranty and the software's author, the holder of the
# economic rights, and the successive licensors have only limited liability.
#
# In this respect, the user's attention is drawn to the risks associated
# with loading, using, modifying and/or developing or reproducing the
# software by the user in light of its specific status of free software,
# that may mean that it is complicated to manipulate, and that also
# therefore means that it is reserved for developers and experienced
# professionals having in-depth computer knowledge. Users are therefore
# encouraged to load and test the software's suitability as regards their
# requirements in conditions enabling the security of their systemsand/or
# data to be ensured and, more generally, to use and operate it in the
# same conditions as regards security.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL-C license and that you accept its terms.
#
"""cubicweb_francearchives setup module using data from
cubicweb_frarchives_edition/__pkginfo__.py file
"""

from os.path import join, dirname, exists
import os
from distutils.command import build

from setuptools import find_packages, setup


here = dirname(__file__)

# load metadata from the __pkginfo__.py file so there is no risk of conflict
# see https://packaging.python.org/en/latest/single_source_version.html
pkginfo = join(here, 'cubicweb_frarchives_edition', '__pkginfo__.py')
__pkginfo__ = {}
with open(pkginfo) as f:
    exec(f.read(), __pkginfo__)

# get required metadatas
distname = __pkginfo__['distname']
version = __pkginfo__['version']
license = __pkginfo__['license']
description = __pkginfo__['description']
web = __pkginfo__['web']
author = __pkginfo__['author']
author_email = __pkginfo__['author_email']
classifiers = __pkginfo__['classifiers']

with open(join(here, 'README.rst')) as f:
    long_description = f.read()

# get optional metadatas
dependency_links = __pkginfo__.get('dependency_links', ())

requires = {}
for entry in ("__depends__",):  # "__recommends__"):
    requires.update(__pkginfo__.get(entry, {}))
install_requires = ["{0} {1}".format(d, v and v or "").strip()
                    for d, v in requires.items()]


class MyBuildCommand(build.build):

    def run(self):
        build.build.run(self)
        if (not os.environ.get('FRARCHIVES_NO_BUILD_DATA_FILES', False) and exists('/usr/bin/npm')):
            self.spawn(['npm', 'install'])
            os.environ['NODE_ENV'] = 'production'
            self.spawn(['npm', 'run', 'build'])


setup(
    name=distname,
    version=version,
    license=license,
    description=description,
    long_description=long_description,
    author=author,
    author_email=author_email,
    url=web,
    cmdclass={'build': MyBuildCommand},
    classifiers=classifiers,
    packages=find_packages(exclude=['test']),
    install_requires=install_requires,
    include_package_data=True,
    entry_points={
        'cubicweb.cubes': [
            'frarchives_edition=cubicweb_frarchives_edition',
        ],
    },
    zip_safe=False,
)

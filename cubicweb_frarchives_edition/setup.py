#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=W0142,W0403,W0404,W0613,W0622,W0622,W0704,R0904,C0103,E0611

# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
# Contact http://www.logilab.fr -- mailto:contact@logilab.fr
#
# This software is governed by the CeCILL-C license under French law and
# abiding by the rules of distribution of free software. You can use,
# modify and/ or redistribute the software under the terms of the CeCILL-C
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

"""Generic Setup script, takes package info from __pkginfo__.py file
"""
__docformat__ = "restructuredtext en"

import __pkginfo__
import os
import sys
import shutil
from os.path import exists, join, walk
from distutils.command import build

try:
    if os.environ.get('NO_SETUPTOOLS'):
        raise ImportError()  # do as there is no setuptools
    from setuptools import setup
    from setuptools.command import install_lib
    USE_SETUPTOOLS = True
except ImportError:
    from distutils.core import setup
    from distutils.command import install_lib
    USE_SETUPTOOLS = False
from distutils.command import install_data

# import required features
from __pkginfo__ import modname, version, license, description, web, \
    author, author_email, classifiers

if exists('README'):
    long_description = open('README').read()
else:
    long_description = ''

# import optional features
if USE_SETUPTOOLS:
    requires = {}
    for entry in ("__depends__",):  # "__recommends__"):
        requires.update(getattr(__pkginfo__, entry, {}))
    install_requires = [("%s %s" % (d, v and v or "")).strip()
                        for d, v in requires.items()]
else:
    install_requires = []

distname = getattr(__pkginfo__, 'distname', modname)
scripts = getattr(__pkginfo__, 'scripts', ())
include_dirs = getattr(__pkginfo__, 'include_dirs', ())
data_files = getattr(__pkginfo__, 'data_files', None)
ext_modules = getattr(__pkginfo__, 'ext_modules', None)
dependency_links = getattr(__pkginfo__, 'dependency_links', ())

BASE_BLACKLIST = ('CVS', '.svn', '.hg', 'debian', 'dist', 'build')
IGNORED_EXTENSIONS = ('.pyc', '.pyo', '.elc', '~')


def ensure_scripts(linux_scripts):
    """
    Creates the proper script names required for each platform
    (taken from 4Suite)
    """
    from distutils import util
    if util.get_platform()[:3] == 'win':
        scripts_ = [script + '.bat' for script in linux_scripts]
    else:
        scripts_ = linux_scripts
    return scripts_


def export(from_dir, to_dir,
           blacklist=BASE_BLACKLIST,
           ignore_ext=IGNORED_EXTENSIONS,
           verbose=True):
    """make a mirror of from_dir in to_dir, omitting directories and files
    listed in the black list
    """
    def make_mirror(arg, directory, fnames):
        """walk handler"""
        for norecurs in blacklist:
            try:
                fnames.remove(norecurs)
            except ValueError:
                pass
        for filename in fnames:
            # don't include binary files
            if filename[-4:] in ignore_ext:
                continue
            if filename[-1] == '~':
                continue
            src = join(directory, filename)
            dest = to_dir + src[len(from_dir):]
            if verbose:
                sys.stderr.write('%s -> %s\n' % (src, dest))
            if os.path.isdir(src):
                if not exists(dest):
                    os.mkdir(dest)
            else:
                if exists(dest):
                    os.remove(dest)
                shutil.copy2(src, dest)
    try:
        os.mkdir(to_dir)
    except OSError as ex:
        # file exists ?
        import errno
        if ex.errno != errno.EEXIST:
            raise
    walk(from_dir, make_mirror, None)


class MyInstallLib(install_lib.install_lib):
    """extend install_lib command to handle  package __init__.py and
    include_dirs variable if necessary
    """

    def run(self):
        """overridden from install_lib class"""
        install_lib.install_lib.run(self)
        # manually install included directories if any
        if include_dirs:
            base = modname
            for directory in include_dirs:
                dest = join(self.install_dir, base, directory)
                export(directory, dest, verbose=False)


# re-enable copying data files in sys.prefix
old_install_data = install_data.install_data
if USE_SETUPTOOLS:
    # overwrite InstallData to use sys.prefix instead of the egg directory
    class MyInstallData(old_install_data):
        """A class that manages data files installation"""

        def run(self):
            _old_install_dir = self.install_dir
            if self.install_dir.endswith('egg'):
                self.install_dir = sys.prefix
            old_install_data.run(self)
            self.install_dir = _old_install_dir
    try:
        # only if easy_install available
        import setuptools.command.easy_install  # noqa
        # monkey patch: Crack SandboxViolation verification
        from setuptools.sandbox import DirectorySandbox as DS
        old_ok = DS._ok

        def _ok(self, path):
            """Return True if ``path`` can be written during installation."""
            out = old_ok(self, path)  # here for side effect from setuptools
            realpath = os.path.normcase(os.path.realpath(path))
            allowed_path = os.path.normcase(sys.prefix)
            if realpath.startswith(allowed_path):
                out = True
            return out
        DS._ok = _ok
    except ImportError:
        pass


class MyBuildCommand(build.build):

    def run(self):
        build.build.run(self)
        if (not os.environ.get('FRARCHIVES_NO_BUILD_DATA_FILES', False)
                and exists('/usr/bin/npm')):
            self.spawn(['npm', 'install'])
            os.environ['NODE_ENV'] = 'production'
            self.spawn(['npm', 'run', 'build'])
            # rebuild data_files so that output of `npm run build` is included
            self.distribution.data_files = __pkginfo__.build_datafiles()


def install(**kwargs):
    """setup entry point"""
    if USE_SETUPTOOLS:
        if '--force-manifest' in sys.argv:
            sys.argv.remove('--force-manifest')
    # install-layout option was introduced in 2.5.3-1~exp1
    elif sys.version_info < (2, 5, 4) and '--install-layout=deb' in sys.argv:
        sys.argv.remove('--install-layout=deb')
    cmdclass = {'install_lib': MyInstallLib}
    if USE_SETUPTOOLS:
        kwargs['install_requires'] = install_requires
        kwargs['dependency_links'] = dependency_links
        kwargs['zip_safe'] = False
        cmdclass['install_data'] = MyInstallData
        cmdclass['build'] = MyBuildCommand

    return setup(name=distname,
                 version=version,
                 license=license,
                 description=description,
                 long_description=long_description,
                 author=author,
                 author_email=author_email,
                 url=web,
                 scripts=ensure_scripts(scripts),
                 data_files=data_files,
                 ext_modules=ext_modules,
                 cmdclass=cmdclass,
                 classifiers=classifiers,
                 **kwargs
                 )


if __name__ == '__main__':
    install()

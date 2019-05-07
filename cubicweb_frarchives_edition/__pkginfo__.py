# -*- coding: utf-8 -*-
#
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
"""cubicweb-frarchives-edition application packaging information"""

from os.path import join

modname = 'frarchives_edition'
distname = 'cubicweb-frarchives-edition'

numversion = (0, 30, 0)
version = '.'.join(str(num) for num in numversion)

license = 'CeCILL-C'
author = 'LOGILAB S.A. (Paris, FRANCE)'
author_email = 'contact@logilab.fr'
description = 'Edition components for FranceArchives'
web = 'https://github.com/culturecommunication/francearchives-cubicweb-edition'

__depends__ = {
    'cubicweb[pyramid,crypto]': '>= 3.24.0,<3.25.0',
    'cubicweb-francearchives': '>= 1.21.0',
    'pyramid': '< 1.10.0',
    'six': '>= 1.4.0',
    'jsl': '>= 0.2.2',
    'cubicweb-jsonschema': None,
    'cubicweb-pwd_policy': None,
    'rq': None,
    # 'pyramid-chameleon': None,  # debian package should fix
    'jinja2': None,
    'nazca': None,
}
__recommends__ = {}

classifiers = [
    'Environment :: Web Environment',
    'Framework :: CubicWeb',
    'Programming Language :: Python',
    'Programming Language :: JavaScript',
    'License :: CeCILL-C Free Software License Agreement (CECILL-C)',
]

THIS_CUBE_DIR = join('share', 'cubicweb', 'cubes', modname)

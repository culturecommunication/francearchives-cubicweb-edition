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

from __future__ import print_function

import atexit
import os

from cubicweb.devtools import (
    DEFAULT_PSQL_SOURCES, startpgcluster,
    stoppgcluster,
    ApptestConfiguration,
    get_test_db_handler,
    PostgresApptestConfiguration,
)
import utils  # noqa


is_master = False


def pytest_xdist_setupnodes(config, specs):
    """ called before any remote node is set up. """
    global is_master
    is_master = True
    # FIRT create template for sqlite
    testconf = ApptestConfiguration('data', 'fake.py')
    sqlite_db_handler = get_test_db_handler(testconf)
    sqlite_db_handler.build_db_cache()
    # THEN start pg cluster
    startpgcluster(__file__)
    # FINALLY create template for pg
    testconf = PostgresApptestConfiguration('data', 'fake.py')
    pg_db_handler = get_test_db_handler(testconf)
    pg_db_handler.build_db_cache()
    # give slave pg cluster coordinate
    os.environ['TEST-PG-HOST'] = DEFAULT_PSQL_SOURCES['system']['db-host']
    os.environ['TEST-PG-PORT'] = DEFAULT_PSQL_SOURCES['system']['db-port']


def master_teardown():
    if is_master:
        stoppgcluster(__file__)


atexit.register(master_teardown)

# -*- coding: utf-8 -*-
#
# flake8: noqa
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2021
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

import logging

from cubicweb_francearchives.dataimport.oai import parse_oai_url
from cubicweb_francearchives.migration.utils import add_column_to_published_table

logger = logging.getLogger("frarchives-edition.migration")
logger.setLevel(logging.INFO)

logger.info("drop dataimport_ead_index table")

sql("DROP TABLE IF EXISTS dataimport_ead_index;")
sql("DROP TABLE IF EXISTS dao_to_remove;")
sql("DROP TABLE IF EXISTS tmp_eid_to_remove;")


logger.info("add missing primary keys")

sql('ALTER TABLE "authority_history" REPLICA IDENTITY FULL;')

sql('ALTER TABLE "entities_id_seq" ADD PRIMARY KEY ("last");')
sql('ALTER TABLE "sameas_history" ADD PRIMARY KEY ("sameas_uri", "autheid");')

sql('ALTER TABLE "tx_entity_actions" ADD PRIMARY KEY ("tx_uuid");')
sql('ALTER TABLE "tx_relation_actions" ADD PRIMARY KEY ("tx_uuid");')


sql('ALTER TABLE "geodata" ADD PRIMARY KEY ("geonameid");')
sql('ALTER TABLE "executed_command" ADD PRIMARY KEY ("start");')  # ??? is it a good idea ?

sql("set schema 'published';")
sql('ALTER TABLE "cw_facomponent" ADD PRIMARY KEY ("cw_eid");')
sql('ALTER TABLE "cw_findingaid" ADD PRIMARY KEY ("cw_eid");')
sql('ALTER TABLE "fa_referenced_files_relation" ADD PRIMARY KEY ("eid_from", "eid_to");')
sql('ALTER TABLE "related_content_suggestion_relation" ADD PRIMARY KEY ("eid_from", "eid_to");')
sql("set schema 'public';")

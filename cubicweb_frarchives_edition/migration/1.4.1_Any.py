# flake8: noqa
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

# regenerate published.cw_findingaid_delete function
# to delete a FindingAid related FAComponents from the published schema
import logging

from cubicweb_frarchives_edition.migration.utils import update_published_triggers

logger = logging.getLogger("francearchives.migration")

logger.info("-> Create a missing published.related_content_suggestion_relation")

sql = cnx.system_sql

cnx.system_sql(
    """create table if not exists published.related_content_suggestion_relation as
       select * from public.related_content_suggestion_relation;"""
)

cnx.commit()

logger.info(
    "-> Update BaseContent, ExternRef and CommorationItem postgres published triggers to add related_authority for BaseContent and related_content_suggestion to all of them"
)

update_published_triggers(cnx, ("BaseContent", "ExternRef", "CommemorationItem"), sql=sql)


logger.info("-> Update missing published.related_authority_relation for BaseContent")

sql(
    """INSERT INTO published.related_authority_relation
SELECT DISTINCT rel.eid_from, rel.eid_to
FROM related_authority_relation as rel,
published.cw_basecontent as PUB_BC
where rel.eid_from = PUB_BC.cw_eid
EXCEPT (SELECT eid_from, eid_to from published.related_authority_relation)
ON CONFLICT (eid_from, eid_to) DO NOTHING;
"""
)

cnx.commit()

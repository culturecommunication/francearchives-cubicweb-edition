# flake8: noqa
# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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
from cubicweb_frarchives_edition import workflows
from cubicweb_francearchives import CMS_I18N_OBJECTS

for etype in CMS_I18N_OBJECTS:
    print("make workflowable ", etype)
    if etype in ("SectionTranslation"):
        workflows.section_workflow(add_workflow, etype)
    else:
        workflows.cmsobject_workflow(add_workflow, etype)

# add published triggers

from cubicweb_frarchives_edition.migration.utils import update_published_triggers

update_published_triggers(cnx, CMS_I18N_OBJECTS)

for etype in CMS_I18N_OBJECTS:
    # add referenced_files
    add_relation_definition(etype, "referenced_files", "File")

# Add GlossaryTerm

print("Add GlossaryTerm")

etype = "GlossaryTerm"

workflows.cmsobject_workflow(add_workflow, "GlossaryTerm")

# add published triggers

from cubicweb_frarchives_edition.migration.utils import update_published_triggers

update_published_triggers(cnx, [etype])


def publish_glossaryterm(add_workflow, rql, sql):
    rset = rql(
        'Any S WHERE S is State, S state_of WF, WF workflow_of X, X name %(etype)s, S name ILIKE "%\\_publish%"',
        {"etype": "GlossaryTerm"},
    )
    if len(rset) != 1:
        print(rset)
        return
    sql(
        "insert into in_state_relation (eid_from, eid_to) select cw_eid, %(eid_to)s from cw_glossaryterm",
        {"eid_to": rset[0][0]},
    )


# publish GlossaryTerm if any
publish_glossaryterm(add_workflow, rql, sql)

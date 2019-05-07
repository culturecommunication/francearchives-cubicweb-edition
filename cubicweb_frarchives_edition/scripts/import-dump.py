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

from cubicweb_francearchives.schema import cms as schema_cms

from cubicweb_frarchives_edition import workflows

from cubicweb_francearchives.dataimport.sqlutil import sudocnx
from cubicweb_francearchives.utils import setup_published_schema
from cubicweb_frarchives_edition.mviews import (
    setup_published_triggers, get_published_tables, build_indexes)


try:
    add_cube('frarchives_edition')
except AssertionError:
    print('direct attempt to add frarchives_edition failed (as expected)')

for etype in ('FindingAid',) + schema_cms.CMS_OBJECTS:
    make_workflowable(etype)

for etype in ('FindingAid',) + schema_cms.CMS_OBJECTS:
    if etype == 'Section':
        workflows.section_workflow(add_workflow, etype)
    else:
        workflows.cmsobject_workflow(add_workflow, etype)

commit()

with sudocnx(cnx, interactive=False) as su_cnx:
    skipped_relations = ('in_state', )  # in_state is useless there
    etypes, _, rnames = get_published_tables(
        cnx, skipped_relations=skipped_relations, skipped_etypes=('CWUser',))
    etypes = set(etypes) | {'FAComponent', 'CWUser'}
    dbuser = cnx.vreg.config.system_source_config['db-user']
    with su_cnx.cursor() as crs:
        setup_published_schema(crs.execute, etypes, rnames,
                               user=dbuser, dumpfiles='/tmp')
        crs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'published'")
        for tablename, in crs.fetchall():
            crs.execute('ALTER TABLE published.{} OWNER TO {}'.format(tablename, dbuser))
    su_cnx.commit()

setup_published_triggers(cnx, dumpfiles='/tmp')
cnx.system_sql('\n'.join(build_indexes(cnx, 'FAComponent')))
commit()


for etype in ('FindingAid',) + schema_cms.CMS_OBJECTS:
    print('migrating', etype)
    rset = rql('Any S WHERE S is State, S state_of WF, WF workflow_of X, X name %(etype)s, S name "wfs_cmsobject_published"',
               {'etype': etype})
    if len(rset) != 1:
        print(rset)
        continue
    if etype == 'FindingAid':
        sql('ALTER TABLE in_state_relation DISABLE TRIGGER published_in_state_update')
        print 'trigger disabled'
    sql('INSERT INTO in_state_relation (eid_from, eid_to) SELECT cw_eid, %(eid_to)s FROM cw_{}'.format(etype.lower()),
        {'eid_to': rset[0][0]})
    if etype == 'FindingAid':
        sql('ALTER TABLE in_state_relation ENABLE TRIGGER published_in_state_update')
        print 'trigger reenabled'

print 'copy findingaid into published schema'
sql('INSERT INTO published.cw_findingaid SELECT * FROM public.cw_findingaid')
print 'copy facomponent into published schema'
sql('INSERT INTO published.cw_facomponent SELECT * FROM public.cw_facomponent')

commit()


def remove_card_workflow(cnx, drop_relation_definition):
    drop_relation_definition('Card', 'in_state', 'State')
    cnx.execute('DELETE Workflow W WHERE W workflow_of ET, ET name "Card"')
    cnx.commit()


remove_card_workflow(cnx, drop_relation_definition)  # noqa


sql("update cw_file set cw_data = regexp_replace(encode(cw_data, 'escape'), 'francearchives[AB]/appfiles', 'frarchives_editions/appfiles')::bytea")

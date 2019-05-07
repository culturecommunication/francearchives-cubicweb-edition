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

from cubicweb_frarchives_edition.mviews import setup_published_triggers, get_published_tables
add_cube('pwd_policy')



def part_of_setup_published_schema(rtables, sqlschema='published'):
    sql_parts = []
    for rtable in rtables:
        sql_parts.append(
            'create table if not exists {schema}.{table} as '
            '  select * from {table} where null;'.format(
                table=rtable,
                schema=sqlschema))
        sql_parts.append(
            'alter table {schema}.{table} '
            '  add primary key (eid_from, eid_to);'.format(
                table=rtable,
                schema=sqlschema))
    # create indexes on those relation tables
    for rtable in rtables:
        for col in ('eid_from', 'eid_to'):
            sql_parts.append('create index {rtable}_{col}_idx on '
                           '{schema}.{rtable}({col});'.format(
                               schema=sqlschema,
                               rtable=rtable,
                               col=col,
                           ))
    sql('\n'.join(sql_parts))


expected_rtables = {
    r + '_relation'
    for r in get_published_tables(
        cnx, skipped_etypes=('CWUser', 'CWProperty'), skipped_relations=('in_state',)
    )[-1]
}
effected_rtables = {r for r, in sql(
    "SELECT table_name FROM information_schema.tables WHERE "
    "table_schema = 'published' AND table_name ILIKE '%_relation'"
)}


part_of_setup_published_schema(expected_rtables - effected_rtables)


setup_published_triggers(cnx, bootstrap=False)

# modify Section workflow

wf = rql('Any WF WHERE WF is Workflow, ET default_workflow WF, ET name %(et)s',
         {'et': 'Section'}).one()

publish = wf.transition_by_name('wft_cmsobject_publish')
publish.set_permissions(requiredgroups=('managers',),
                        reset=True)
unpublish = wf.transition_by_name('wft_cmsobject_unpublish')
unpublish.set_permissions(requiredgroups=('managers',),
                          reset=True)

commit()

# republish ExternRef to force images synchronization

for ext in rql('Any X WHERE X is ExternRef, X in_state S, S name "wfs_cmsobject_published"').entities():
    adapted = ext.cw_adapt_to('ImageFileSync')
    if adapted:
        adapted.copy()

# delete
sql('DELETE FROM published.cw_cwproperty where cw_pkey=%(k)s', {'k': 'system.version.pwd_policy'})
commit()

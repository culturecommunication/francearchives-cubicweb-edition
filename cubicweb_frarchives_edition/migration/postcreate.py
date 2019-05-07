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

"""cubicweb-frarchives-edition postcreate script, executed at instance creation time or when
the cube is added to an existing instance.
"""
from cubicweb_francearchives.schema.cms import CMS_OBJECTS

from cubicweb_frarchives_edition import workflows

from cubicweb_francearchives.utils import setup_published_schema
from cubicweb_frarchives_edition.mviews import (
    setup_published_triggers, get_published_tables, build_indexes)

for etype in CMS_OBJECTS + ('FindingAid',):
    if etype == 'Section':
        workflows.section_workflow(add_workflow, etype)
    else:
        workflows.cmsobject_workflow(add_workflow, etype)

if repo.system_source.dbdriver == 'postgres':
    skipped_relations = ('in_state', )  # in_state is useless there
    etypes, _, rnames = get_published_tables(
        cnx, skipped_relations=skipped_relations, skipped_etypes=('CWUser',))
    etypes = set(etypes) | {'FAComponent', 'CWUser'}
    setup_published_schema(sql, etypes, rnames)
    setup_published_triggers(cnx)
    cnx.system_sql('\n'.join(build_indexes(cnx, 'FAComponent')))

statement = '''
CREATE TABLE sameas_history (
 sameas_uri varchar(256) NOT NULL,
 autheid int NOT NULL,
 action boolean NOT NULL,
 UNIQUE (sameas_uri, autheid)
)
'''

cnx.system_sql(statement)

indexes = '''
CREATE INDEX sameas_history_action_idx ON sameas_history(action);
'''
cnx.system_sql(indexes)

# this table is created here only for test purposes
# otherwise it is done by cubicweb-ctl setup-geonames <instance> commande
cnx.system_sql('''
create table geonames_altnames (
    alternateNameId integer not null,
    geonameid integer not null,
    isolanguage varchar(7),
    alternate_name varchar(400),
    isPreferredName boolean,
    isShortName boolean,
    isColloquial boolean,
    isHistoric boolean,
    rank integer
);''')

indexes = '''
CREATE INDEX geonames_altnames_geonameid_idx ON geonames_altnames USING btree(geonameid);
CREATE INDEX geonames_altnames_isolanguage_idx ON geonames_altnames(isolanguage);
CREATE INDEX geonames_altnames_rank_idx ON geonames_altnames(rank);
'''
cnx.system_sql(indexes)

# create limited BANO table
# this table is created here only for test purposes
# otherwise it is done by cubicweb-ctl setup-bano <instance> commande
cnx.system_sql(
    '''
    CREATE TABLE bano_whitelisted (
        banoid varchar(200),
        voie varchar(200),
        nom_comm varchar(200),
        lat double precision,
        lon double precision
    )
    '''
)
commit()

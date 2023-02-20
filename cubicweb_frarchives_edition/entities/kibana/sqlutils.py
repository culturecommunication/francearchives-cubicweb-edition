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

"""creation of helpers views"""

import logging
import time

from cubicweb_frarchives_edition import AUTHORITIES

SUBJECT_AUTHORITY_SAMEAS_QUERY = """SELECT DISTINCT {field}cw_label.cw_label,
    cw_concept.cw_cwuri,
    cw_conceptscheme.cw_title
    FROM cw_concept, cw_conceptscheme, cw_label, in_scheme_relation, same_as_relation
    WHERE same_as_relation.eid_to=cw_concept.cw_eid
    AND in_scheme_relation.eid_to=cw_conceptscheme.cw_eid
    AND cw_label.cw_label_of=cw_concept.cw_eid
    AND cw_label.cw_kind=%(l)s
    AND cw_label.cw_language_code='fr-fr'
    {cond}
    """


def create_auth_services(cnx, etypes, log):
    creation_query = """CREATE TEMPORARY TABLE IF NOT EXISTS {table} AS
      (SELECT DISTINCT
                it.cw_authority as autheid,
                service.cw_eid AS service_eid,
                service.cw_code AS code,
                service.cw_level AS level,
                service.cw_name AS name,
                service.cw_name2 AS name2
            FROM cw_{authtable} AS at
                JOIN cw_{index_table_name} AS it ON (it.cw_authority=at.cw_eid)
                JOIN published.index_relation AS rel_index
                    ON (rel_index.eid_from=it.cw_eid)
                JOIN published.cw_findingaid as fa ON (rel_index.eid_to=fa.cw_eid)
                JOIN cw_service as service ON (service.cw_eid=fa.cw_service)
                )
       UNION
           (SELECT DISTINCT
               it.cw_authority as autheid,
               service.cw_eid AS service_eid,
               service.cw_code AS code,
               service.cw_level AS level,
               service.cw_name AS name,
               service.cw_name2 AS name2
            FROM cw_{authtable} AS at
                JOIN cw_{index_table_name} AS it ON (it.cw_authority=at.cw_eid)
                JOIN published.index_relation AS rel_index
                    ON (rel_index.eid_from=it.cw_eid)
                JOIN published.cw_facomponent as comp ON (rel_index.eid_to=comp.cw_eid)
                JOIN published.cw_findingaid as fa ON (fa.cw_eid=comp.cw_finding_aid)
                JOIN cw_service as service ON (service.cw_eid=fa.cw_service)
            );

      CREATE INDEX IF NOT EXISTS {table}_0_idx ON {table}
        (autheid);
"""
    for index, etype in (
        ("AgentName", "AgentAuthority"),
        ("Geogname", "LocationAuthority"),
        ("Subject", "SubjectAuthority"),
    ):
        if etype not in etypes:
            log.info(f"{time.ctime()}: skip {etype} as unwanted option")
            continue
        tablename = "kibana_{etype}_services".format(etype=etype.lower())
        log.info(f"{time.ctime()}: creating {tablename} table")
        cnx.system_sql(
            creation_query.format(index_table_name=index, authtable=etype.lower(), table=tablename)
        )
        cnx.commit()
        log.info(f"{time.ctime()}: table {tablename} created")


def create_auth_sameas(cnx, log):
    same_as_queries = [
        """(SELECT DISTINCT rel_same_as0.eid_from as autheid,
                    _E.cw_label AS label,
                    _E.cw_uri AS uri,
                    _E.cw_source AS source
             FROM cw_ExternalUri AS _E, same_as_relation AS rel_same_as0
             WHERE rel_same_as0.eid_to=_E.cw_eid)""",
        """(SELECT DISTINCT rel_same_as0.eid_from as autheid,
                   _E.cw_label AS label,
                   _E.cw_extid AS uri,
                   _E.cw_source AS source
            FROM cw_ExternalId AS _E, same_as_relation AS rel_same_as0
            WHERE rel_same_as0.eid_to=_E.cw_eid)""",
        """(SELECT DISTINCT rel_same_as0.eid_from as autheid,
                    _E.cw_record_id AS label,
                    _E.cw_record_id AS uri,
                    'EAC-CPF' AS source
            FROM cw_AuthorityRecord AS _E, same_as_relation AS rel_same_as0
            WHERE rel_same_as0.eid_to=_E.cw_eid)""",
        SUBJECT_AUTHORITY_SAMEAS_QUERY.format(
            field="same_as_relation.eid_from,",
            cond="AND in_scheme_relation.eid_from=cw_concept.cw_eid",
        ),
    ]
    query = """CREATE TEMPORARY TABLE IF NOT EXISTS {table} AS
    SELECT DISTINCT tmp.autheid, tmp.label, tmp.uri, tmp.source
    FROM ({queries}) AS tmp;

        CREATE INDEX IF NOT EXISTS {table}_0_idx ON {table}
        (autheid);
        """
    tablename = "kibana_auth_sameas"
    log.info(f"{time.ctime()}: creating {tablename} table")
    cnx.system_sql(
        query.format(table=tablename, queries=" UNION ALL ".join(same_as_queries)),
        {"l": "preferred"},
    )
    cnx.commit()
    log.info(f"{time.ctime()}: table {tablename} created")


def create_kibana_authorities_sql(cnx, etypes=None, log=None):
    """
    Create temporary tables to speed up kibana authority indexation
    """
    log = log or logging.getLogger("sql.kibana")
    log.setLevel(logging.DEBUG)
    log.info(f"{time.ctime()}: creating sql tables")
    if not etypes:
        etypes = AUTHORITIES
    create_auth_services(cnx, etypes, log)
    create_auth_sameas(cnx, log)

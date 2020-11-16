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


def create_auth_services(cnx, log):
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
        tablename = "kibana_{etype}_services".format(etype=etype.lower())
        log.info("creating {} table".format(tablename))
        print("creating {} table".format(tablename))
        cnx.system_sql(
            creation_query.format(index_table_name=index, authtable=etype.lower(), table=tablename)
        )
        cnx.commit()


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
        """(SELECT DISTINCT rel_same_as0.eid_from, _CL.cw_label, _C.cw_cwuri, _S.cw_title
               FROM cw_Concept AS _C, cw_ConceptScheme AS _S, cw_Label AS _CL,
                    in_scheme_relation AS rel_in_scheme1,
                    same_as_relation AS rel_same_as0
               WHERE rel_same_as0.eid_to=_C.cw_eid AND
                     rel_in_scheme1.eid_from=_C.cw_eid AND
                     rel_in_scheme1.eid_to=_S.cw_eid AND
                     _CL.cw_label_of=_C.cw_eid AND
                     _CL.cw_kind=%(l)s)

        """,
    ]
    query = """CREATE TEMPORARY TABLE IF NOT EXISTS {table} AS
    SELECT DISTINCT tmp.autheid, tmp.label, tmp.uri, tmp.source
    FROM ({queries}) AS tmp;

        CREATE INDEX IF NOT EXISTS {table}_0_idx ON {table}
        (autheid);
        """
    tablename = "kibana_auth_sameas"
    log.info("creating {} table".format(tablename))
    print("creating {} table".format(tablename))
    cnx.system_sql(
        query.format(table=tablename, queries=" UNION ALL ".join(same_as_queries)),
        {"l": "preferred"},
    )
    cnx.commit()


def create_kibana_authorities_sql(cnx, log=None):
    """
    Create temporary tables to speed up kibana authority indexation
    """
    log = log or logging.getLogger("sql.kibana")
    print("creating sql tables")
    create_auth_services(cnx, log)
    create_auth_sameas(cnx, log)

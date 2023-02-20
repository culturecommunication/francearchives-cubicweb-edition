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

"""indexes authoritie/service for kibana"""
import datetime

from cubicweb.predicates import is_instance

from cubicweb_frarchives_edition import AUTHORITIES

from cubicweb_frarchives_edition.entities.kibana import (
    AbstractKibanaSerializable,
    AbstractKibanaIndexer,
)
from cubicweb_frarchives_edition.entities.kibana.sqlutils import SUBJECT_AUTHORITY_SAMEAS_QUERY


class PniaAuthorityDocumentsKibanaIndexer(AbstractKibanaIndexer):
    """Authority indexer for kibana"""

    __regid__ = "kibana-auth-indexer"

    etypes = AUTHORITIES

    mapping = {
        "properties": {
            "eid": {"type": "integer"},
            "cw_etype": {"type": "keyword"},
            "label": {"type": "keyword", "fields": {"raw": {"type": "text"}}},
            "location": {"type": "geo_point"},
            "grouped_with": {"type": "integer"},
            "grouped_with_count": {"type": "integer"},
            # used for filter, may use a scripted field doc['grouped_with'].length
            "is_grouped": {"type": "boolean"},
            "services": {
                "properties": {
                    "eid": {"type": "integer"},
                    "code": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "title": {"type": "keyword"},
                },
            },
            "quality": {"type": "boolean"},
            "services_count": {
                "type": "integer"
            },  # used for filter, may use a scripted field doc['services.eid'].length
            "documents_count": {"type": "integer"},
            "same_as": {
                "properties": {
                    "label": {"type": "keyword"},
                    "uri": {"type": "keyword"},
                    "source": {"type": "keyword"},
                },
            },
            "same_as_count": {"type": "integer"},  # use a scripted field doc['same_as.uri'].length
            "creation_date": {"type": "date"},
            "reindex_date": {"type": "date"},
            "urlpath": {"type": "keyword"},
        }
    }

    @property
    def index_name(self):
        return self._cw.vreg.config["kibana-authorities-index-name"]


class AbstractAuthorityKibanaSerializable(AbstractKibanaSerializable):
    @property
    def es_doc_type(self):
        return "_doc"

    def index_types(self):
        """Get the list of Idexes types

        :returns: list of types
        :rtype: list
        """
        query = """DISTINCT Any T WHERE I authority X, I type T, X eid %(eid)s"""
        return [t for t, in self._cw.execute(query, {"eid": self.entity.eid}, build_descr=False)]

    def ir_documents_count(self):
        """Get the list of linked FindingAid and FAComponents

        :returns: linked FindingAid and FAComponents count
        :rtype: list
        """
        sql_query = """
                SELECT COUNT(DISTINCT rel_index.eid_to)
                FROM cw_{authtable} AS at
                    LEFT OUTER JOIN cw_{index_table_name} AS it ON (it.cw_authority=at.cw_eid)
                    LEFT OUTER JOIN published.index_relation AS rel_index
                              ON (rel_index.eid_from=it.cw_eid)
                WHERE it.cw_authority={eid}"""
        cu = self._cw.system_sql(
            sql_query.format(
                eid=self.entity.eid,
                authtable=self.entity.cw_etype.lower(),
                index_table_name=self.index_table_name,
            )
        )
        return cu.fetchone()[0]

    def services(self):
        """Get the list of services from linked FindingAid and FAComponents

        :returns: list of services data
        :rtype: list
        """
        sql_query = """
           (SELECT DISTINCT
                service.cw_eid AS service_eid,
                service.cw_code AS code,
                service.cw_level AS level,
                service.cw_name AS name,
                service.cw_name2 AS name2
            FROM cw_{index_table_name} AS index,
                published.cw_FindingAid AS fa,
                cw_Service AS service,
                index_relation AS rel_index0
            WHERE index.cw_authority={eid} AND
                rel_index0.eid_from=index.cw_eid AND
                rel_index0.eid_to=fa.cw_eid AND
                fa.cw_service=service.cw_eid)
       UNION ALL
           (SELECT DISTINCT
               service.cw_eid AS service_eid,
               service.cw_code AS code,
               service.cw_level AS level,
               service.cw_name AS name,
               service.cw_name2 AS name2
           FROM cw_{index_table_name} AS index,
               published.cw_FAComponent AS comp,
               published.cw_FindingAid AS fa,
               cw_Service AS service,
               index_relation AS rel_index0
           WHERE index.cw_authority={eid} AND
              rel_index0.eid_from=index.cw_eid AND
              rel_index0.eid_to=comp.cw_eid AND
              comp.cw_finding_aid=fa.cw_eid AND
              fa.cw_service=service.cw_eid)
        """

        def dc_title(level, name, name2):
            if level == "level-D":
                return name2 or name
            else:
                terms = [name, name2]
                return " - ".join(t for t in terms if t)

        cu = self._cw.system_sql(
            sql_query.format(
                eid=self.entity.eid,
                authtable=self.entity.cw_etype.lower(),
                index_table_name=self.index_table_name,
            )
        )
        return [
            {
                "eid": eid,
                "code": code,
                "level": self._cw._(level),
                "title": dc_title(level, name, name2),
            }
            for eid, code, level, name, name2 in cu.fetchall()
        ]

    def location(self):
        if self.entity.cw_etype == "LocationAuthority":
            rset = self._cw.execute(
                """Any LONG, LAT LIMIT 1 WHERE
                X latitude LAT, X longitude LONG,
                NOT X latitude NULL, NOT X longitude NULL,
                X eid %(eid)s""",
                {"eid": self.entity.eid},
                build_descr=False,
            )
            if rset:
                return rset[0]
        return []

    @property
    def sameas_queries(self):
        return [
            """SELECT DISTINCT
                    ext.cw_label AS label,
                    ext.cw_uri AS uri,
                    ext.cw_source AS source
                FROM cw_ExternalUri AS ext, same_as_relation AS rel_same_as0
                WHERE rel_same_as0.eid_from=%(eid)s AND rel_same_as0.eid_to=ext.cw_eid""",
            """SELECT DISTINCT
                   ext.cw_label AS label,
                   ext.cw_extid AS uri,
                   ext.cw_source AS source
               FROM cw_ExternalId AS ext, same_as_relation AS rel_same_as0
               WHERE rel_same_as0.eid_from=%(eid)s AND rel_same_as0.eid_to=ext.cw_eid""",
        ]

    def same_as(self):
        """Get the list of same_as relations except AuthorityRecords

        :returns: list of same_as data
        :rtype: list
        """
        queries = " UNION ALL ".join(self.sameas_queries)
        cu = self._cw.system_sql(queries, {"l": "preferred", "eid": self.entity.eid})
        return [{"label": lbl, "uri": u, "source": s} for lbl, u, s in cu.fetchall()]

    def serialize(self, complete=True):
        entity = self.entity
        if complete:
            entity.complete()
        etype = entity.cw_etype
        services = self.services()
        same_as = self.same_as()
        grouped_with = entity.related("grouped_with", role="object")
        return {
            "cw_etype": etype,
            "eid": entity.eid,
            "label": entity.label,
            # do not use type from Geogname, Subject, AgentName
            # because user could have group authorities so
            # one authority could have 2 AgentName with two different
            # type
            "location": self.location(),
            "types": self.index_types(),
            "grouped_with": [x[0] for x in grouped_with],
            "grouped_with_count": len(grouped_with),
            "is_grouped": bool(entity.grouped_with),
            "services": services,
            "services_count": len(services),
            "documents_count": self.ir_documents_count(),
            "same_as": same_as,
            "same_as_count": len(same_as),
            "quality": entity.quality,
            "creation_date": entity.creation_date,
            "reindex_date": datetime.date.today().strftime("%Y-%m-%d"),
            "urlpath": f"{self._cw.base_url()}{etype.split('Authority')[0].lower()}/{entity.eid}",
        }


class AgentAuthorityKibanaSerializable(AbstractAuthorityKibanaSerializable):
    __select__ = is_instance("AgentAuthority")
    index_table_name = "AgentName"

    @property
    def sameas_queries(self):
        return [
            """SELECT DISTINCT
                    ext.cw_record_id AS label,
                    ext.cw_record_id AS uri,
                    'EAC-CPF' AS source
                FROM cw_AuthorityRecord AS ext, same_as_relation AS rel_same_as0
                WHERE rel_same_as0.eid_from=%(eid)s AND rel_same_as0.eid_to=ext.cw_eid""",
        ]


class LocationAuthorityKibanaSerializable(AbstractAuthorityKibanaSerializable):
    __select__ = is_instance("LocationAuthority")
    index_table_name = "Geogname"


class SubjectAuthorityKibanaSerializable(AbstractAuthorityKibanaSerializable):
    __select__ = is_instance("SubjectAuthority")
    index_table_name = "Subject"

    @property
    def sameas_queries(self):
        queries = super(SubjectAuthorityKibanaSerializable, self).sameas_queries
        queries.append(
            SUBJECT_AUTHORITY_SAMEAS_QUERY.format(
                field="",
                cond="AND same_as_relation.eid_from=%(eid)s",
            )
        )

        return queries


class AbastractAuthorityInitialKibanaSerializable(AbstractAuthorityKibanaSerializable):
    """This adaptor is only used for the initial population on kibana_auth_index
    add use some temporary tables to speed up the serialization
    """

    __abstract__ = True
    __regid__ = "IKibanaInitiaLAuthorityIndexSerializable"

    def services(self):
        """Get the list of services from linked FindingAid and FAComponents

        :returns: list of services data
        :rtype: list
        """
        sql_query = """
        SELECT service_eid, code, level, name, name2
        FROM {table}
        WHERE autheid={eid}""".format(
            table="kibana_{etype}_services".format(etype=self.entity.cw_etype.lower()),
            eid=self.entity.eid,
        )
        cu = self._cw.system_sql(sql_query, {"eid": self.entity.eid})

        def dc_title(level, name, name2):
            if level == "level-D":
                return name2 or name
            else:
                terms = [name, name2]
                return " - ".join(t for t in terms if t)

        return [
            {
                "eid": eid,
                "code": code,
                "level": self._cw._(level),
                "title": dc_title(level, name, name2),
            }
            for eid, code, level, name, name2 in cu.fetchall()
        ]

    def same_as(self):
        """Get the list of same_as relations except AuthorityRecords

        :returns: list of same_as data
        :rtype: list
        """
        sql_query = """
        SELECT label, uri, source
        FROM {table} WHERE autheid={eid}""".format(
            table="kibana_auth_sameas",
            eid=self.entity.eid,
        )
        cu = self._cw.system_sql(sql_query, {"eid": self.entity.eid})
        return [{"label": lbl, "uri": u, "source": s} for lbl, u, s in cu.fetchall()]


class AgentAuthorityInitialKibanaSerializable(AbastractAuthorityInitialKibanaSerializable):
    __select__ = is_instance("AgentAuthority")
    index_table_name = "AgentName"


class LocationAuthorityInitialKibanaSerializable(AbastractAuthorityInitialKibanaSerializable):
    __select__ = is_instance("LocationAuthority")
    index_table_name = "Geogname"


class SubjectAuthorityInitialKibanaSerializable(AbastractAuthorityInitialKibanaSerializable):
    __select__ = is_instance("SubjectAuthority")
    index_table_name = "Subject"

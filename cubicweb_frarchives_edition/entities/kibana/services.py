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

from cubicweb.predicates import is_instance

from cubicweb_frarchives_edition.entities.kibana import (
    AbstractKibanaSerializable,
    AbstractKibanaIndexer,
)


class PniaServiceKibanaIndexer(AbstractKibanaIndexer):
    """Service indexer for kibana"""

    __regid__ = "kibana-service-indexer"
    etypes = ["Service"]

    mapping = {
        "properties": {
            "alltext": {"type": "text"},
            "eid": {"type": "integer"},
            "cw_etype": {"type": "keyword"},
            "code": {"type": "keyword", "copy_to": "alltext"},
            "level": {"type": "keyword"},
            "title": {"type": "keyword", "copy_to": "alltext"},
            "name": {"type": "keyword", "copy_to": "alltext"},
            "name2": {"type": "keyword", "copy_to": "alltext"},
            "short_name": {"type": "keyword", "copy_to": "alltext"},
            "category": {"type": "keyword"},
            "search_form_url": {"type": "keyword"},
            "thumbnail_url": {"type": "keyword"},
            "viewer_url": {"type": "keyword"},
            "documents_count": {"type": "integer"},
            "archives": {"type": "integer"},
            "siteres": {"type": "integer"},
            "nomina": {"type": "integer"},
            "urlpath": {"type": "keyword"},
        }
    }

    @property
    def index_name(self):
        return self._cw.vreg.config["kibana-services-index-name"]


class ServiceKibanaSerializable(AbstractKibanaSerializable):
    __select__ = is_instance("Service")

    @property
    def es_doc_type(self):
        return "_doc"

    def ir_documents_count(self):
        """Get the list of linked FindingAid and FAComponent count

        :returns: linked FindingAid and FAComponent count
        :rtype: list
        """
        query = """Any COUNT(F) WITH F BEING (
        (DISTINCT Any F WHERE  F service X, X eid %(eid)s, F is FindingAid
          , F in_state S, S name %(state)s)
        UNION
        (DISTINCT Any FA WHERE F service X, X eid %(eid)s, FA finding_aid F
          , F in_state S, S name %(state)s)
        )
        """
        return self._cw.execute(
            query, {"eid": self.entity.eid, "state": "wfs_cmsobject_published"}, build_descr=False
        )[0][0]

    def siteref_documents_count(self):
        """Get the list of linked CMS documents count

        :returns: linked BaseContent and ExternRef count
        :rtype: list
        """
        query = """Any COUNT(X) WITH X BEING (
        (DISTINCT Any X WHERE X basecontent_service S, S eid %(eid)s, X is BaseContent
          , X in_state ST, ST name %(state)s)
        UNION
        (DISTINCT Any X WHERE X exref_service S, S eid %(eid)s, X is ExternRef
          , X in_state ST, ST name %(state)s)
        )
        """

        return self._cw.execute(
            query, {"eid": self.entity.eid, "state": "wfs_cmsobject_published"}, build_descr=False
        )[0][0]

    def nomina_documents_count(self):
        """Get the list of linked NominaRecord documents count

        :returns: linked NominaRecord count
        :rtype: list
        """
        query = "Any COUNT(X) WHERE X service S, S eid %(eid)s, X is NominaRecord"
        return self._cw.execute(query, {"eid": self.entity.eid}, build_descr=False)[0][0]

    def serialize(self, complete=True):
        entity = self.entity
        if complete:
            entity.complete()
        etype = entity.cw_etype
        ir_count = self.ir_documents_count()
        siteref_count = self.siteref_documents_count()
        nomina_count = self.nomina_documents_count()
        return {
            "cw_etype": etype,
            "eid": entity.eid,
            "level": self._cw._(entity.level),
            "name": entity.name,
            "name2": entity.name2,
            "short_name": entity.short_name,
            "category": entity.category,
            "search_form_url": entity.search_form_url,
            "thumbnail_url": entity.thumbnail_url,
            "viewer_url": entity.thumbnail_dest,
            "title": entity.dc_title(),
            "code": entity.code,
            "archives": ir_count,
            "siteres": siteref_count,
            "nomina": nomina_count,
            "documents_count": ir_count + siteref_count,
            "urlpath": "{}/{}".format(entity.cw_etype.lower(), entity.eid),
        }

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

"""indexes for kibana"""

from elasticsearch import helpers as es_helpers


from logilab.common.decorators import cachedproperty

from cubicweb.predicates import is_instance

from cubicweb_elasticsearch.es import get_connection

from cubicweb_frarchives_edition.entities.kibana import (
    AbstractKibanaSerializable,
    AbstractKibanaIndexer,
)


class PniaDocumentsKibanaIndexer(AbstractKibanaIndexer):
    """FindingAid/FAComponents indexer for kibana"""

    __regid__ = "kibana-ir-indexer"

    mapping = {
        "properties": {
            "cw_etype": {
                "type": "keyword",
            },
            "dates": {"type": "integer_range"},
            "did": {
                "properties": {
                    "eid": {"type": "long"},
                    "note": {"type": "text"},
                    "unitid": {
                        "type": "text",
                    },
                    "unittitle": {"type": "text"},
                },
            },
            "digitized": {"type": "boolean"},
            "digitized_versions": {
                "properties": {
                    "url": {"type": "keyword"},
                    "illustration_url": {"type": "keyword"},
                    "role": {"type": "keyword"},
                },
            },
            "eadid": {
                "type": "keyword",
            },
            "eid": {
                "type": "integer",
            },
            "in_state": {"type": "keyword"},
            "index_entries": {
                "type": "nested",
                "include_in_parent": "true",
                "properties": {
                    "authfilenumber": {
                        "type": "text",
                    },
                    "authority": {"type": "long"},
                    "label": {
                        "type": "keyword",
                    },
                    "normalized": {"type": "keyword"},
                    "role": {"type": "text"},
                    "type": {"type": "keyword"},
                },
            },
            "fa_stable_id": {
                "type": "keyword",
            },
            "originators": {"type": "keyword"},
            "publisher": {"type": "keyword"},
            "startyear": {"type": "date", "format": "yyyy"},
            "stopyear": {"type": "date", "format": "yyyy"},
            "sortdate": {"type": "date", "format": "yyyy-MM-dd"},
            "service": {
                "properties": {
                    "eid": {"type": "integer"},
                    "code": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "title": {"type": "keyword"},
                },
            },
            "stable_id": {
                "type": "keyword",
            },
            "titleproper": {"type": "text"},
            "creation_date": {
                "type": "date",
            },
            "modification_date": {
                "type": "date",
            },
        }
    }

    @property
    def index_name(self):
        return self._cw.vreg.config["kibana-ir-index-name"]

    @property
    def source_es_params(self):
        """index published data for kibana"""
        return {
            "elasticsearch-locations": self._cw.vreg.config["elasticsearch-locations"],
            "index-name": self._cw.vreg.config["published-index-name"],
        }

    @cachedproperty
    def source_index_name(self):
        return self.source_es_params["index-name"] + "_all"

    def populate_index(self):
        es = get_connection(self.source_es_params)
        for etype in ("FindingAid", "FAComponent"):
            es_helpers.reindex(
                es,
                source_index=self.source_index_name,
                target_index=self.index_name,
                query={"query": {"match": {"cw_etype": etype}}},
            )


class IrKibanaSerializable(AbstractKibanaSerializable):
    __select__ = is_instance("FindingAid", "FAComponent")

    def serialize(self, complete=True, es_doc=None):
        return self.entity.cw_adapt_to("IFullTextIndexSerializable").serialize(
            complete=complete, es_doc=es_doc
        )

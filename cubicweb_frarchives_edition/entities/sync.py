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
from inspect import isclass

from elasticsearch import helpers as es_helpers

from logilab.common.decorators import cachedproperty

from cubicweb.predicates import is_instance, relation_possible, is_in_state
from cubicweb.entity import EntityAdapter
from cubicweb.server import Service

from cubicweb_elasticsearch.es import get_connection, INDEXABLE_TYPES

from cubicweb_francearchives import CMS_I18N_OBJECTS

from cubicweb_francearchives.entities import sync as fa_sync
from cubicweb_francearchives.dataimport import es_bulk_index
from cubicweb_francearchives.views.search import ETYPES_MAP

from cubicweb_frarchives_edition import VarnishPurgeMixin

INDEXABLE_DOC_TYPES = INDEXABLE_TYPES + list(ETYPES_MAP.keys())


def set_selectable_if_published(adapter):
    """object should be syncable only if publishable _and_ published"""
    adapter.__select__ = adapter.__select__ & (
        ~relation_possible("in_state") | is_in_state("wfs_cmsobject_published")
    )


class SyncService(VarnishPurgeMixin, Service):
    __regid__ = "sync"  # XXX

    @cachedproperty
    def cms_es_params(self):
        return {
            "elasticsearch-locations": self._cw.vreg.config["elasticsearch-locations"],
            "index-name": self._cw.vreg.config["index-name"],
            "elasticsearch-verify-certs": self._cw.vreg.config["elasticsearch-verify-certs"],
            "elasticsearch-ssl-show-warn": self._cw.vreg.config["elasticsearch-ssl-show-warn"],
        }

    @cachedproperty
    def public_es_params(self):
        return {
            "elasticsearch-locations": self._cw.vreg.config["elasticsearch-locations"],
            "index-name": self._cw.vreg.config["published-index-name"],
            "elasticsearch-verify-certs": self._cw.vreg.config["elasticsearch-verify-certs"],
            "elasticsearch-ssl-show-warn": self._cw.vreg.config["elasticsearch-ssl-show-warn"],
        }

    @cachedproperty
    def cms_index_name(self):
        return self.cms_es_params["index-name"] + "_all"

    @cachedproperty
    def public_index_name(self):
        return self.public_es_params["index-name"] + "_all"

    @staticmethod
    def published_entity(entity):
        # if entity is FAComponent, rely on its parent's state
        if entity.cw_etype == "FAComponent":
            entity = entity.finding_aid[0]
        wf = entity.cw_adapt_to("IWorkflowable")
        if wf is None:
            return True
        return wf.state == "wfs_cmsobject_published"

    def sync(self, sync_operations):
        urls_to_purge = []
        for op_type, entity in sync_operations:
            try:
                ivarnish = entity.cw_adapt_to("IVarnish")
                if ivarnish is not None:
                    urls_to_purge += ivarnish.urls_to_purge()
                if op_type == "index":
                    self.fs_sync_index(entity)
                    self.es_sync_index(entity)
                elif op_type == "delete":
                    self.fs_sync_delete(entity)
                    self.es_sync_delete(entity)
                elif op_type == "index-children":
                    self.es_sync_children(entity)
            except Exception:
                import traceback

                traceback.print_exc()
        self.purge_varnish(urls_to_purge, self._cw.vreg.config)

    def es_sync_children(self, entity):
        if not self.cms_es_params.get("elasticsearch-locations"):
            self.error('no "elasticsearch-locations" config found')
            return
        es = get_connection(self.cms_es_params)
        es_docs = self._reindex_children(es, self.cms_index_name, entity.eid)
        es_bulk_index(es, es_docs, raise_on_error=True)
        if self.published_entity(entity):
            for index_name in (self.public_index_name,):
                es_docs = self._reindex_children(es, index_name, entity.eid)
                es_bulk_index(es, es_docs, raise_on_error=True)

    def _reindex_children(self, es, index_name, ancestor_eid):
        """reindex all docs whith ancestor = ancestor_eid stored in ES"""
        if index_name:
            for doc in es_helpers.scan(
                es,
                index=index_name,
                query={"query": {"match": {"ancestors": ancestor_eid}}},
            ):
                if doc["_source"]["cw_etype"] not in INDEXABLE_DOC_TYPES:
                    continue
                try:
                    entity = self._cw.entity_from_eid(doc["_source"]["eid"])
                except Exception as err:
                    self.error(f"[es] index Section {ancestor_eid}: {err}")
                    continue
                adaptor = entity.cw_adapt_to("IFullTextIndexSerializable")
                if adaptor:
                    yield {
                        "_op_type": "index",
                        "_index": index_name,
                        "_type": "_doc",
                        "_id": doc["_id"],
                        "_source": adaptor.serialize(),
                    }

    def es_sync_index(self, entity):
        if not self.cms_es_params.get("elasticsearch-locations"):
            self.error('no "elasticsearch-locations" config found')
            return
        es = get_connection(self.cms_es_params)
        if entity.cw_etype == "FindingAid":
            for index_name in (self.public_index_name,):
                if index_name:
                    es_helpers.reindex(
                        es,
                        source_index=self.cms_index_name,
                        target_index=index_name,
                        query={"query": {"match": {"fa_stable_id": entity.stable_id}}},
                    )
        else:
            if entity.cw_etype in CMS_I18N_OBJECTS:
                self.sync_translatables(es, entity)
            else:
                es_helpers.reindex(
                    es,
                    source_index=self.cms_index_name,
                    target_index=self.public_index_name,
                    query={"query": {"match": {"eid": entity.eid}}},
                )

    def fs_sync_index(self, entity):
        ifilesync = entity.cw_adapt_to("IFileSync")
        if ifilesync is not None:
            ifilesync.copy()

    @staticmethod
    def _delete_fa_documents(es, index_name, stable_id):
        """return all documents of type ``etype`` stored in ES"""
        for doc in es_helpers.scan(
            es,
            index=index_name,
            docvalue_fields=(),
            query={"query": {"match": {"fa_stable_id": stable_id}}},
        ):
            yield {
                "_op_type": "delete",
                "_index": index_name,
                "_type": "_doc",
                "_id": doc["_id"],
            }

    def sync_translatables(self, es, entity):
        if self.published_entity(entity.original_entity):
            adaptor = entity.original_entity.cw_adapt_to("IFullTextIndexSerializable")
            docs = [
                {
                    "_op_type": "index",
                    "_index": self.public_index_name,
                    "_type": "_doc",
                    "_id": adaptor.es_id,
                    "_source": adaptor.serialize(published=True),
                }
            ]
            es_bulk_index(es, docs, raise_on_error=False)

    def es_sync_delete(self, entity):
        if not self.cms_es_params.get("elasticsearch-locations"):
            self.error('no "elasticsearch-locations" config found')
            return
        es = get_connection(self.cms_es_params)
        serializable = entity.cw_adapt_to("IFullTextIndexSerializable")
        if entity.cw_etype == "FindingAid":
            for index_name in (self.public_index_name,):
                if index_name:
                    es_docs = self._delete_fa_documents(es, index_name, entity.stable_id)
                    es_bulk_index(es, es_docs, raise_on_error=False)
        elif entity.cw_etype in ("AuthorityRecord",):
            es.delete_by_query(
                self.public_index_name,
                doc_type=serializable.es_doc_type,
                body={"query": {"match": {"eid": entity.eid}}},
            )
        else:
            if entity.cw_etype in CMS_I18N_OBJECTS:
                self.sync_translatables(es, entity)
            else:
                es.delete(
                    self.public_index_name, doc_type=serializable.es_doc_type, id=serializable.es_id
                )

    def fs_sync_delete(self, entity):
        ifilesync = entity.cw_adapt_to("IFileSync")
        if ifilesync is not None:
            ifilesync.delete()


class SuggestSyncService(Service):
    __regid__ = "reindex-suggest"

    @cachedproperty
    def cms_es_params(self):
        return {
            "elasticsearch-locations": self._cw.vreg.config["elasticsearch-locations"],
            "index-name": self._cw.vreg.config["index-name"],
            "elasticsearch-verify-certs": self._cw.vreg.config["elasticsearch-verify-certs"],
            "elasticsearch-ssl-show-warn": self._cw.vreg.config["elasticsearch-ssl-show-warn"],
        }

    @cachedproperty
    def cms_index_name(self):
        return "{}_suggest".format(self._cw.vreg.config["index-name"])

    @cachedproperty
    def public_index_name(self):
        return "{}_suggest".format(self._cw.vreg.config["published-index-name"])

    def index_authorities(self, authorities):
        if not self.cms_es_params.get("elasticsearch-locations"):
            self.error('no "elasticsearch-locations" config found')
            return
        es = get_connection(self.cms_es_params)
        cms_docs, public_docs = [], []
        for entity in authorities:
            serializable = entity.cw_adapt_to("ISuggestIndexSerializable")
            for _docs, index_name in (
                (cms_docs, self.cms_index_name),
                (public_docs, self.public_index_name),
            ):
                json = serializable.serialize(published=index_name == self.public_index_name)
                if not json:
                    continue
                _docs.append(
                    {
                        "_op_type": "index",
                        "_id": entity.eid,
                        "_type": "_doc",
                        "_index": index_name,
                        "_source": json,
                    }
                )
        for docs in (cms_docs, public_docs):
            if docs:
                es_bulk_index(es, docs, raise_on_error=False)


for obj in list(vars(fa_sync).values()):
    if isclass(obj) and obj is not EntityAdapter and issubclass(obj, EntityAdapter):
        set_selectable_if_published(obj)


def has_transition(entity, *trnames):
    wfable = entity.cw_adapt_to("IWorkflowable")
    if wfable is not None:
        for trname in trnames:
            transition = wfable.main_workflow.transition_by_name(trname)
            if transition is not None:
                return True
    return False


class ICompoundAdapter(EntityAdapter):
    __abstract__ = True
    __regid__ = "ICompound"

    parent_rtypes = ()

    @property
    def roots(self):
        """some parent relations can be multiple, we must consider all parents"""
        roots = []
        for rtype, role in self.parent_rtypes:
            parent_rset = self.entity.related(rtype, role)
            for parent in parent_rset.entities():
                icompound = parent.cw_adapt_to("ICompound")
                if icompound is None:
                    roots.append(parent)
                else:
                    roots.extend(icompound.roots)
        return roots


class MetadataICompound(ICompoundAdapter):
    __select__ = is_instance("Metadata")
    parent_rtypes = [("metadata", "object")]


class ImageICompound(ICompoundAdapter):
    __select__ = is_instance("Image")
    parent_rtypes = [
        (rtype, "object")
        for rtype in (
            "commemoration_image",
            "news_image",
            "basecontent_image",
            "section_image",
            "subject_image",
            "service_image",
            "map_image",
            "externref_image",
        )
    ]


class CssImageICompound(ICompoundAdapter):
    __select__ = is_instance("CssImage")
    parent_rtypes = [
        ("cssimage_of", "subject"),
    ]


class FileICompound(ICompoundAdapter):
    __select__ = is_instance("File")
    parent_rtypes = [
        ("image_file", "object"),
        ("attachment", "object"),
        ("additional_attachment", "object"),
    ]


class OfficialTextICompound(ICompoundAdapter):
    __select__ = is_instance("OfficialText")
    parent_rtypes = [("circular", "subject")]

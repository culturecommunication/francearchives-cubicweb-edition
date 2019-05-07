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
from copy import copy

import urlparse
from elasticsearch import Elasticsearch
from elasticsearch import helpers as es_helpers

from logilab.common.decorators import cachedproperty

from cubicweb.predicates import is_instance, relation_possible, is_in_state
from cubicweb.view import EntityAdapter
from cubicweb.server import Service

from cubicweb_varnish.varnishadm import varnish_cli_connect_from_config

from cubicweb_francearchives.entities import sync as fa_sync
from cubicweb_francearchives.dataimport import es_bulk_index


def set_selectable_if_published(adapter):
    """object should be syncable only if publishable _and_ published"""
    adapter.__select__ = adapter.__select__ & (
        ~relation_possible('in_state') | is_in_state('wfs_cmsobject_published')
    )


class SyncService(Service):
    __regid__ = 'sync'  # XXX

    @cachedproperty
    def cms_es_params(self):
        return {
            'elasticsearch-locations': self._cw.vreg.config['elasticsearch-locations'],
            'index-name': self._cw.vreg.config['index-name'],
        }

    @cachedproperty
    def public_es_params(self):
        return {
            'elasticsearch-locations': self._cw.vreg.config['elasticsearch-locations'],
            'index-name': self._cw.vreg.config['published-index-name'],
        }

    @cachedproperty
    def cms_index_name(self):
        return self.cms_es_params['index-name'] + '_all'

    @cachedproperty
    def public_index_name(self):
        return self.public_es_params['index-name'] + '_all'

    def sync(self, sync_operations):
        urls_to_purge = []
        for op_type, entity in sync_operations:
            try:
                ivarnish = entity.cw_adapt_to('IVarnish')
                if ivarnish is not None:
                    urls_to_purge += ivarnish.urls_to_purge()
                if op_type == 'index':
                    self.fs_sync_index(entity)
                    self.es_sync_index(entity)
                elif op_type == 'delete':
                    self.fs_sync_delete(entity)
                    self.es_sync_delete(entity)
            except Exception:
                import traceback
                traceback.print_exc()
        self.purge_varnish(urls_to_purge)

    def purge_varnish(self, urls):
        config = self._cw.vreg.config
        if not config.get('varnishcli-hosts'):
            return
        purge_cmd = 'ban req.url ~'
        cnxs = varnish_cli_connect_from_config(config)
        for url in urls:
            for varnish_cli in cnxs:
                varnish_cli.execute(purge_cmd, '^%s' % urlparse.urlparse(url).path)
        for varnish_cli in cnxs:
            varnish_cli.close()

    def es_sync_index(self, entity):
        if not self.cms_es_params.get('elasticsearch-locations'):
            return
        es = Elasticsearch(self.cms_es_params['elasticsearch-locations'])
        if entity.cw_etype == 'FindingAid':
            es_helpers.reindex(es,
                               source_index=self.cms_index_name,
                               target_index=self.public_index_name,
                               query={'query': {"match": {"fa_stable_id": entity.stable_id}}})
        else:
            es_helpers.reindex(es,
                               source_index=self.cms_index_name,
                               target_index=self.public_index_name,
                               query={'query': {"match": {"eid": entity.eid}}})

    def fs_sync_index(self, entity):
        ifilesync = entity.cw_adapt_to('IFileSync')
        if ifilesync is not None:
            ifilesync.copy()

    @staticmethod
    def _delete_fa_documents(es, index_name, stable_id):
        """return all documents of type ``etype`` stored in ES"""
        for doc in es_helpers.scan(es,
                                   index=index_name, doc_type='_doc',
                                   docvalue_fields=(),
                                   query={"query": {"match": {"fa_stable_id": stable_id}}}):
            yield {
                '_op_type': 'delete',
                '_index': index_name,
                '_type': '_doc',
                '_id': doc['_id'],
            }

    def es_sync_delete(self, entity):
        es = Elasticsearch(self.cms_es_params['elasticsearch-locations'])
        serializable = entity.cw_adapt_to('IFullTextIndexSerializable')
        if entity.cw_etype == 'FindingAid':
            es_docs = self._delete_fa_documents(
                es,
                self.public_index_name,
                entity.stable_id)
            es_bulk_index(es, es_docs, raise_on_error=False)
        else:
            es.delete(self.public_index_name,
                      doc_type=serializable.es_doc_type,
                      id=serializable.es_id)

    def fs_sync_delete(self, entity):
        ifilesync = entity.cw_adapt_to('IFileSync')
        if ifilesync is not None:
            ifilesync.delete()


class SuggestSyncService(Service):
    __regid__ = 'reindex-suggest'

    @cachedproperty
    def cms_index_name(self):
        return '{}_suggest'.format(self._cw.vreg.config['index-name'])

    @cachedproperty
    def public_index_name(self):
        return '{}_suggest'.format(self._cw.vreg.config['published-index-name'])

    def index_authorities(self, authorities):
        es = Elasticsearch(self._cw.vreg.config['elasticsearch-locations'])
        cms_docs, public_docs = [], []
        for entity in authorities:
            serializable = entity.cw_adapt_to('ISuggestIndexSerializable')
            json = serializable.serialize()
            if not json:
                continue
            for _docs, index_name, count in ((cms_docs, self.cms_index_name,
                                              json['count']),
                                             (public_docs, self.public_index_name,
                                              serializable.published_count())):
                json = copy(json)
                json['count'] = count
                _docs.append({
                    '_op_type': 'index' if count else 'delete',
                    '_id': entity.eid,
                    '_type': '_doc',
                    '_index': index_name,
                    '_source': json
                })
        for docs in (cms_docs, public_docs):
            if docs:
                es_bulk_index(es, docs, raise_on_error=False)


for obj in vars(fa_sync).values():
    if isclass(obj) and obj is not EntityAdapter and issubclass(obj, EntityAdapter):
        set_selectable_if_published(obj)


def has_transition(entity, *trnames):
    wfable = entity.cw_adapt_to('IWorkflowable')
    if wfable is not None:
        for trname in trnames:
            transition = wfable.main_workflow.transition_by_name(trname)
            if transition is not None:
                return True
    return False


class ICompoundAdapter(EntityAdapter):
    __abstract__ = True
    __regid__ = 'ICompound'

    parent_rtypes = ()

    @property
    def root(self):
        for rtype, role in self.parent_rtypes:
            parent_rset = self.entity.related(rtype, role)
            if parent_rset:
                parent = parent_rset.one()
                icompound = parent.cw_adapt_to('ICompound')
                if icompound is None:
                    return parent
                return icompound.root
        return None


class MetadataICompound(ICompoundAdapter):
    __select__ = is_instance('Metadata')
    parent_rtypes = [('metadata', 'object')]


class ImageICompound(ICompoundAdapter):
    __select__ = is_instance('Image')
    parent_rtypes = [(rtype, 'object')
                     for rtype in ('commemoration_image', 'news_image',
                                   'basecontent_image', 'section_image',
                                   'service_image', 'map_image',
                                   'externref_image')]


class CssImageICompound(ICompoundAdapter):
    __select__ = is_instance('CssImage')
    parent_rtypes = [('cssimage_of', 'subject'), ]


class FileICompound(ICompoundAdapter):
    __select__ = is_instance('File')
    parent_rtypes = [('image_file', 'object'),
                     ('attachment', 'object'),
                     ('additional_attachment', 'object')]


class OfficialTextICompound(ICompoundAdapter):
    __select__ = is_instance('OfficialText')
    parent_rtypes = [('circular', 'subject')]

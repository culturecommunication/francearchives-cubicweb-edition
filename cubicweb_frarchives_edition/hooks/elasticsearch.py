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
from __future__ import absolute_import

from urllib3.exceptions import ProtocolError

from elasticsearch.exceptions import ConnectionError, NotFoundError

from cubicweb.server import hook
from cubicweb.predicates import is_instance

from cubicweb_elasticsearch.entities import ESTransactionQueue
from cubicweb_elasticsearch.hooks import IndexEsOperation

from cubicweb_francearchives.schema.cms import CMS_OBJECTS

from cubicweb_frarchives_edition.hooks import custom_on_fire_transition


def es_published_indexer(cnx):
    return cnx.vreg['es'].select('indexer', cnx, published=True)


class EsDocumentHook(hook.Hook):
    """update related entity to EsDocument in Elasticsearch database"""

    __regid__ = 'elasticsearch.contentupdatetoes.esdocument'
    __select__ = hook.Hook.__select__ & is_instance('EsDocument')
    events = ('after_update_entity',)
    category = 'es'

    def __call__(self):
        IndexEsOperation.get_instance(self._cw).add_data({
            'op_type': 'index',
            'entity': self.entity.entity[0],
        })


class FAESTransactionQueue(ESTransactionQueue):
    sync_service = 'sync'

    @staticmethod
    def published_entity(entity):
        # if entity is FAComponent, rely on its parent's state
        if entity.cw_etype == 'FAComponent':
            entity = entity.finding_aid[0]
        wf = entity.cw_adapt_to('IWorkflowable')
        if wf is None:
            return True
        return wf.state == 'wfs_cmsobject_published'

    def process_operations(self, es_operations):
        done = set()
        super(FAESTransactionQueue, self).process_operations(es_operations)
        sync_operations = []
        for es_operation in es_operations:
            op_type = es_operation['op_type']
            entity = es_operation['entity']
            if (op_type, entity.eid) in done:
                continue
            done.add((op_type, entity.eid))
            is_published = self.published_entity(entity)
            if op_type == 'delete' or op_type == 'sync-delete':
                sync_operations.append(('delete', entity))
            elif is_published:
                sync_operations.append(('index', entity))
        if sync_operations:
            sync_service = self._cw.vreg['services'].select(self.sync_service, self._cw)
            try:
                sync_service.sync(sync_operations)
            except (ConnectionError, ProtocolError, NotFoundError):
                op_str = ', '.join('{} #{}'.format(op_type, entity.eid)
                                   for op_type, entity in sync_operations)
                self.warning('[ES] Failed sync operations %s', op_str)


class UpdateStateInESHook(hook.Hook):
    """Launch finding aid file import threaded task."""
    __regid__ = 'frarchives_edition.update-state-in-es'
    __select__ = (hook.Hook.__select__
                  & custom_on_fire_transition(CMS_OBJECTS + ('FindingAid',),
                                              {'wft_cmsobject_publish', 'wft_cmsobject_unpublish'}))
    events = ('after_add_entity', )
    category = 'es'

    def __call__(self):
        trname = self.entity.transition.name
        target = self.entity.for_entity
        op = IndexEsOperation.get_instance(self._cw)
        if trname == 'wft_cmsobject_unpublish':
            op.add_data({
                'op_type': 'sync-delete',
                'entity': target,
            })
        op.add_data({
            'op_type': 'index',
            'entity': target,
        })


def registration_callback(vreg):
    vreg.register_all(globals().values(), __name__,
                      (FAESTransactionQueue,))
    vreg.register_and_replace(FAESTransactionQueue, ESTransactionQueue)

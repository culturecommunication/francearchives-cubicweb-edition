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


from urllib3.exceptions import ProtocolError

from elasticsearch.exceptions import ConnectionError, NotFoundError

from logilab.common.decorators import cachedproperty

from cubicweb.server import hook
from cubicweb.predicates import is_instance

from cubicweb_elasticsearch.entities import ESTransactionQueue
from cubicweb_elasticsearch import hooks as es_hooks
from cubicweb_elasticsearch.hooks import IndexEsOperation

from cubicweb_francearchives.schema.cms import CMS_OBJECTS
from cubicweb_francearchives import ES_CMS_I18N_OBJECTS

from cubicweb_frarchives_edition.hooks import custom_on_fire_transition


def es_published_indexer(cnx):
    return cnx.vreg["es"].select("indexer", cnx, published=True)


class EsDocumentHook(hook.Hook):
    """update related entity to EsDocument in Elasticsearch database"""

    __regid__ = "elasticsearch.contentupdatetoes.esdocument"
    __select__ = hook.Hook.__select__ & is_instance("EsDocument")
    events = ("after_update_entity",)
    category = "es"

    def __call__(self):
        IndexEsOperation.get_instance(self._cw).add_data(
            {
                "op_type": "index",
                "entity": self.entity.entity[0],
            }
        )


class FAESTransactionQueue(ESTransactionQueue):
    sync_service = "sync"

    @staticmethod
    def published_entity(entity):
        # if entity is FAComponent, rely on its parent's state
        if entity.cw_etype == "FAComponent":
            entity = entity.finding_aid[0]
        wf = entity.cw_adapt_to("IWorkflowable")
        if wf is None:
            return True
        return wf.state == "wfs_cmsobject_published"

    def process_operations(self, es_operations):
        done = set()
        super(FAESTransactionQueue, self).process_operations(es_operations)
        sync_operations = []
        for es_operation in es_operations:
            op_type = es_operation["op_type"]
            entity = es_operation["entity"]
            if (op_type, entity.eid) in done:
                continue
            done.add((op_type, entity.eid))
            is_published = self.published_entity(entity)
            if op_type == "delete" or op_type == "sync-delete":
                sync_operations.append(("delete", entity))
            elif op_type == "index-children":
                sync_operations.append(("index-children", entity))
            elif is_published:
                sync_operations.append(("index", entity))
        if sync_operations:
            sync_service = self._cw.vreg["services"].select(self.sync_service, self._cw)
            try:
                sync_service.sync(sync_operations)
            except (ConnectionError, ProtocolError, NotFoundError):
                op_str = ", ".join(
                    "{} #{}".format(op_type, entity.eid) for op_type, entity in sync_operations
                )
                self.warning("[ES] Failed sync operations %s", op_str)


class ServiceUpdateStateInESHook(hook.Hook):
    """update Service in Elasticsearch database"""

    __regid__ = "frarchives_edition.update-state-in-es.service"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        ("FindingAid", "BaseContent", "ExternRef"),
        {"wft_cmsobject_publish", "wft_cmsobject_unpublish"},
    )
    events = ("after_add_entity",)
    category = "es"

    def __call__(self):
        target = self.entity.for_entity
        for service in target.services:
            ServiceIndexEsOperation.get_instance(self._cw).add_data(
                {"op_type": "index", "entity": service}
            )


class ServiceSiteRefHook(hook.Hook):
    """update Service in Elasticsearch database"""

    __regid__ = "elasticsearch.relations.service"
    __select__ = hook.Hook.__select__ & hook.match_rtype("basecontent_service", "exref_service")
    events = ("after_add_relation", "after_delete_relation")
    category = "es"

    def __call__(self):
        ServiceIndexEsOperation.get_instance(self._cw).add_data(
            {"op_type": "index", "entity": self._cw.entity_from_eid(self.eidto)}
        )


class ServiceESTransactionQueue(ESTransactionQueue):
    __regid__ = "es.service.opqueue"
    sync_service = "sync-service"

    @cachedproperty
    def default_indexer(self):
        return self._cw.vreg["es"].select("kibana-service-indexer", self._cw)


class NominaESTransactionQueue(ESTransactionQueue):
    __regid__ = "es.nomina.opqueue"
    sync_service = "sync-nomina"

    @cachedproperty
    def default_indexer(self):
        return self._cw.vreg["es"].select("nomina-indexer", self._cw)


class UpdateStateInESHook(hook.Hook):
    """Launch finding aid file import threaded task."""

    __regid__ = "frarchives_edition.update-state-in-es"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        CMS_OBJECTS + ES_CMS_I18N_OBJECTS + ("FindingAid",),
        {"wft_cmsobject_publish", "wft_cmsobject_unpublish"},
    )
    events = ("after_add_entity",)
    category = "es"

    def __call__(self):
        trname = self.entity.transition.name
        target = self.entity.for_entity
        op = IndexEsOperation.get_instance(self._cw)
        if trname == "wft_cmsobject_unpublish":
            op.add_data(
                {
                    "op_type": "sync-delete",
                    "entity": target,
                }
            )
        op.add_data(
            {
                "op_type": "index",
                "entity": target,
            }
        )


class ChildenRelationUpdateIndexES(es_hooks.RelationsUpdateIndexES):
    """updates ES indexing for ancestor field"""

    __select__ = hook.Hook.__select__ & hook.match_rtype("children")

    def __call__(self):
        target = self._cw.entity_from_eid(self.eidto)
        if target.cw_etype == "Section" and es_hooks.entity_indexable(target):
            es_hooks.IndexEsOperation.get_instance(self._cw).add_data(
                {
                    "op_type": "index-children",
                    "entity": target,
                }
            )


class ServiceUpdateIndexES(hook.Hook):
    """detect content change and updates ES indexing"""

    __regid__ = "elasticsearch.service.contentupdatetoes"
    __select__ = hook.Hook.__select__ & is_instance("Service")
    events = ("after_update_entity", "after_add_entity", "after_delete_entity")
    category = "es"

    def __call__(self):
        op_type = "delete" if self.event == "after_delete_entity" else "index"
        ServiceIndexEsOperation.get_instance(self._cw).add_data(
            {
                "op_type": op_type,
                "entity": self.entity,
            }
        )


class NominaRecordUpdateIndexES(hook.Hook):
    """detect content change and updates ES indexing"""

    __regid__ = "elasticsearch.nomina.contentupdatetoes"
    __select__ = hook.Hook.__select__ & is_instance("NominaRecord")
    events = ("after_update_entity", "after_add_entity", "after_delete_entity")
    category = "es"

    def __call__(self):
        op_type = "delete" if self.event == "after_delete_entity" else "index"
        NominaIndexEsOperation.get_instance(self._cw).add_data(
            {
                "op_type": op_type,
                "entity": self.entity,
            }
        )


class NominaRecordUpdateSameAsRel(hook.Hook):
    """reindex NominaRecord on same_as changes"""

    __regid__ = "elasticsearch.nomina.update_sameas"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_add_relation", "after_delete_relation")

    def __call__(self):
        obj = self._cw.entity_from_eid(self.eidto)
        subj = self._cw.entity_from_eid(self.eidfrom)
        if subj.cw_etype == "NominaRecord" and obj.cw_etype == "AgentAuthority":
            # reindex NominaRecord
            op_type = "delete" if self.event == "after_delete_entity" else "index"
            NominaIndexEsOperation.get_instance(self._cw).add_data(
                {
                    "op_type": op_type,
                    "entity": subj,
                }
            )


class ESAgentAuthorityRenameHook(hook.Hook):
    __regid__ = "francearchives.agent-rename"
    __select__ = hook.Hook.__select__ & is_instance("AgentAuthority")
    events = ("before_update_entity",)

    def __call__(self):
        old_label, new_label = self.entity.cw_edited.oldnewvalue("label")
        if old_label != new_label:
            for nomina in self.entity.same_as_links.get("NominaRecord", []):
                NominaIndexEsOperation.get_instance(self._cw).add_data(
                    {"op_type": "index", "entity": nomina}
                )


class NominaIndexEsOperation(hook.DataOperationMixIn, hook.Operation):
    """mixin class to process ES indexing as a postcommit event"""

    containercls = list

    def postcommit_event(self):
        queue = self.cnx.vreg["es"].select("es.nomina.opqueue", req=self.cnx)
        queue.process_operations(self.get_data())


class ServiceIndexEsOperation(hook.DataOperationMixIn, hook.Operation):
    """mixin class to process ES indexing as a postcommit event"""

    containercls = list

    def postcommit_event(self):
        queue = self.cnx.vreg["es"].select("es.service.opqueue", req=self.cnx)
        queue.process_operations(self.get_data())


def registration_callback(vreg):
    vreg.register_all(list(globals().values()), __name__, (FAESTransactionQueue,))
    vreg.register_and_replace(FAESTransactionQueue, ESTransactionQueue)

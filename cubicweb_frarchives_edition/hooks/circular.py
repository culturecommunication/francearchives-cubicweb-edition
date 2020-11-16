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


"""cubicweb-frarchives-edition specific hooks and operations"""

from cubicweb.predicates import is_instance
from cubicweb.server import hook


class CircularAttributesHook(hook.Hook):
    __regid__ = "francearchives.circular-attrs"
    __select__ = hook.Hook.__select__ & is_instance("Circular")
    events = ("before_add_entity", "before_update_entity")
    category = "circular-csv"

    def __call__(self):
        CircularDataOperation.get_instance(self._cw).add_data(self.entity.eid)


class CircularAllPossibleRels(hook.Hook):
    """for all possible circular relations"""

    __regid__ = "francearchives.cirular-rels"
    possibles_subj_rels = (
        "attachment",
        "additional_attachment",
        "additional_link",
        "historical_context",
        "business_field",
        "document_type",
        "action",
        "modified_text",
        "modifying_text",
        "revoked_text",
    )
    possible_obj_rels = ("circular",)
    events = ("before_add_relation", "before_delete_relation")

    def __call__(self):
        eid = None
        if self.rtype in self.possibles_subj_rels:
            eid = self.eidfrom
        if self.rtype in self.possible_obj_rels:
            eid = self.eidto
        if eid:
            CircularDataOperation.get_instance(self._cw).add_data(eid)


class CircularDataOperation(hook.DataOperationMixIn, hook.SingleLastOperation):
    def precommit_event(self):
        cnx = self.cnx
        for eid in self.get_data():
            if cnx.deleted_in_transaction(eid):
                continue
            entity = cnx.entity_from_eid(eid)
            with cnx.allow_all_hooks_but("circular-csv"):
                entity.cw_set(json_values=entity.values_as_json)


class CircularUpdateIndexSuggest(hook.Hook):
    __regid__ = "francearchives.circular-index-suggest"
    __select__ = hook.Hook.__select__ & hook.match_rtype(
        "business_field",
        "historical_context",
        "document_type",
        "action",
    )
    events = ("after_add_relation", "after_delete_relation")

    def __call__(self):
        CircularUpdateIndexSuggestOperation.get_instance(self._cw).add_data(self.eidto)


class CircularUpdateIndexSuggestOperation(hook.DataOperationMixIn, hook.Operation):
    def postcommit_event(self):
        for eidto in self.get_data():
            subject_authorities = self.cnx.execute(
                "DISTINCT Any X WHERE X same_as C, C eid %(eid)s", {"eid": eidto}
            ).entities()
            self.cnx.vreg["services"].select("reindex-suggest", self.cnx).index_authorities(
                subject_authorities
            )

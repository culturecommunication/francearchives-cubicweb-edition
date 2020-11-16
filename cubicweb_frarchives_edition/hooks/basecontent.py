# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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

from cubicweb import ValidationError
from cubicweb.predicates import is_instance
from cubicweb.server import hook

from cubicweb_frarchives_edition.xmlutils import generate_summary, InvalidHTMLError


class BaseContentSummaryHook(hook.Hook):
    """ summary"""

    __regid__ = "frarchives_edition.base_content.toc"
    __select__ = hook.Hook.__select__ & is_instance("BaseContent")
    events = ("before_add_entity", "before_update_entity")

    def __call__(self):
        # jsonschema changes CW default behavor so that all edited entities editable
        # attributes are systematically put into self.entity.cw_edited dict.
        # We should thus compare here old and new values here to decide if an attribute
        # value had been changed.
        entity = self.entity
        old_content, new_content = entity.cw_edited.oldnewvalue("content")
        old_policy, new_policy = entity.cw_edited.oldnewvalue("summary_policy")
        changed_policy = new_policy != old_policy
        if new_content != old_content or changed_policy:
            in_summary = self._cw.transaction_data.setdefault("bc-summary", set())
            if entity.eid not in in_summary:
                in_summary.add(entity.eid)
                BaseContentSummaryOp.get_instance(self._cw).add_data((entity.eid))
                if changed_policy:
                    # regenerate summaries for all translations
                    for tr in entity.reverse_translation_of:
                        BaseContentSummaryOp.get_instance(self._cw).add_data((tr.eid))


class BaseContentTranslationSummaryHook(hook.Hook):
    """ summary"""

    __regid__ = "frarchives_edition.bt_translation.toc"
    __select__ = hook.Hook.__select__ & is_instance("BaseContentTranslation")
    events = ("before_update_entity",)

    def __call__(self):
        # jsonschema changes CW default behavor so that all edited entities editable
        # attributes are systematically put into self.entity.cw_edited dict.
        # We should thus compare here old and new values here to decide if an attribute
        # value had been changed.
        old_content, new_content = self.entity.cw_edited.oldnewvalue("content")
        if new_content != old_content:
            in_summary = self._cw.transaction_data.setdefault("bc-summary", set())
            if self.entity.eid not in in_summary:
                in_summary.add(self.entity.eid)
                BaseContentSummaryOp.get_instance(self._cw).add_data((self.entity.eid))


class BaseContentTranslationSummaryRelHook(hook.Hook):
    __regid__ = "frarchives_edition.bctranslation.toc.rel"
    __select__ = hook.Hook.__select__ & hook.match_rtype("translation_of")
    events = ("after_add_relation",)

    def __call__(self):
        if self._cw.entity_from_eid(self.eidto).cw_etype == "BaseContent":
            BaseContentSummaryOp.get_instance(self._cw).add_data((self.eidfrom))


class BaseContentSummaryOp(hook.DataOperationMixIn, hook.Operation):
    """generate summary (toc) for BaseContent and BaseContentTranslation"""

    def precommit_event(self):
        for eid in self.get_data():
            entity = self.cnx.entity_from_eid(eid)
            summary_policy = entity.summary_policy
            if summary_policy == "no_summary":
                if entity.summary:
                    entity.cw_set(summary=None)
                continue
            # at this point summary_policy value must be "summary_headers_X"
            _, last_heading = summary_policy.rsplit("_", 1)
            assert _, "summary_headers"
            try:
                summary, modified_content = generate_summary(entity.content, int(last_heading))
            except InvalidHTMLError:
                msg = self._cw._(
                    """The "content" field HTML is not valid. Please,
                correct the HTML or choose "no_summary" value for "summary_policy"
                field"""
                )
                raise ValidationError(self.entity.eid, {"summary_policy": msg})
            kwargs = {"summary": summary}
            if modified_content:
                # anchors have been added to the content
                kwargs["content"] = modified_content
            entity.cw_set(**kwargs)

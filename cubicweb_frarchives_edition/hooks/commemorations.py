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


class CommemorationItemInSectionHook(hook.Hook):
    """after creation, store CommemorationItems in "pages_histoire" Section"""

    __regid__ = "frarchives_edition.commemo.in_section"
    __select__ = hook.Hook.__select__ & is_instance("CommemorationItem")
    events = ("after_add_entity",)

    def __call__(self):
        CommemorationItemInSectionHookOp.get_instance(self._cw).add_data(self.entity.eid)


class CommemorationItemInSectionHookOp(hook.DataOperationMixIn, hook.Operation):
    """add newly created CommemorationItems in "pages_histoire" section"""

    def precommit_event(self):
        rset = self.cnx.find("Section", name="pages_histoire")
        if not rset:
            return
        eids = []
        for eid in self.get_data():
            if self.cnx.deleted_in_transaction(eid):
                continue
            eids.append(str(eid))
            self.cnx.execute(
                """SET S children X WHERE X is CommemorationItem,
                   S is Section, S name "pages_histoire",
                   X eid IN (%(eids)s),
                   NOT EXISTS(S children X)""",
                {"eids": ", ".join(eids)},
            )


class CommemorationDatesHook(hook.Hook):
    """check dates consitence"""

    __regid__ = "frarchives_edition.commemo_externref_dates"
    __select__ = hook.Hook.__select__ & is_instance("CommemorationItem", "ExternRef")
    events = ("after_add_entity", "after_update_entity")

    def __call__(self):
        start_year = self.entity.cw_edited.get("start_year", None)
        stop_year = self.entity.cw_edited.get("stop_year", None)
        if stop_year is not None:
            if start_year is None:
                msg = self._cw._("Please, add a start_year")
                raise ValidationError(self.entity.eid, {"start_year": msg})
            if stop_year < start_year:
                msg = self._cw._("Start_year must be less than stop_year")
                raise ValidationError(self.entity.eid, {"stop_year": msg})

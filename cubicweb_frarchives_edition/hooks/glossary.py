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

"""cubicweb-frarchives-edition GlossaryTerm specific hooks and operations"""

from cubicweb import ValidationError
from cubicweb.predicates import is_instance
from cubicweb.server import hook

from cubicweb_francearchives import GLOSSARY_CACHE
from cubicweb_francearchives.utils import populate_terms_cache


class GlossaryTermSortKeyHook(hook.Hook):
    """set GlossaryTerm anchor and sort_letter"""

    __regid__ = "frarchives.glossary.sort"
    __select__ = hook.Hook.__select__ & is_instance("GlossaryTerm")
    events = ("before_add_entity", "before_update_entity")

    def __call__(self):
        entity = self.entity
        if not entity.anchor and "anchor" not in entity.cw_edited:
            entity.cw_edited["anchor"] = str(entity.eid)
        if "term" in entity.cw_edited:
            entity.cw_edited["sort_letter"] = self.entity.term[0].lower()
        # ensure mandatory HTML attributes
        for attr in ("description", "short_description"):
            if attr in entity.cw_edited and not entity.cw_edited[attr].strip():
                raise ValidationError(self.entity.eid, {attr: self._cw._("required property")})


class GlossaryTermsUpdateCacheOp(hook.DataOperationMixIn, hook.LateOperation):
    def precommit_event(self):
        GLOSSARY_CACHE[:] = []
        populate_terms_cache(self.cnx)

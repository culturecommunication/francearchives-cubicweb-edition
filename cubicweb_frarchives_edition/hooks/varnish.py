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

"""cubicweb-frarchives-edition non es-indexable varnish specific hooks and operations"""

from cubicweb.server import hook

from cubicweb_frarchives_edition.hooks import custom_on_fire_transition
from cubicweb_frarchives_edition import VarnishPurgeMixin


class VarnishPurgeHook(VarnishPurgeMixin, hook.Hook):
    __abstract__ = True
    category = "varnish"

    def __call__(self):
        entity = self.get_entity()
        ivarnish = entity.cw_adapt_to("IVarnish")
        urls_to_purge = [url for url in ivarnish.urls_to_purge()]
        self.purge_varnish(urls_to_purge)


class PurgeStateInNotEsIndexableHook(VarnishPurgeHook):
    """a GlossaryTerm has modified, purge its URLs"""

    __regid__ = "frarchives_edition.update-state-in-es"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        ("GlossaryTerm", "FaqItem"),
        {"wft_cmsobject_publish", "wft_cmsobject_unpublish"},
    )
    events = ("after_add_entity",)

    def get_entity(self):
        return self.entity.for_entity


class PurgeAuthoritiesUrlHook(VarnishPurgeHook):
    """an authority has been grouped with an other, purge its URL"""

    __regid__ = "frarchives_edition.authority.varnish"
    __select__ = hook.Hook.__select__ & hook.match_rtype("grouped_with")
    events = ("after_add_relation",)

    def get_entity(self):
        return self._cw.entity_from_eid(self.eidfrom)

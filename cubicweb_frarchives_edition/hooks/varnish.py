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
from cubicweb.predicates import is_instance


class PurgeStateInNotEsIndexableHook(hook.Hook):
    """a GlossaryTerm has modified, purge its URLs"""

    __regid__ = "frarchives_edition.update-state-in-es"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        ("GlossaryTerm", "FaqItem"),
        {"wft_cmsobject_publish", "wft_cmsobject_unpublish"},
    )
    events = ("after_add_entity",)
    category = "varnish"

    def __call__(self):
        VarnishPurgeHookOperation.get_instance(self._cw).add_data(self.entity.for_entity.eid)


class PurgeAuthoritiesUrlHook(hook.Hook):
    """an authority has been grouped with an other, purge its URL"""

    __regid__ = "frarchives_edition.authority.varnish"
    __select__ = hook.Hook.__select__ & hook.match_rtype("grouped_with")
    events = ("after_add_relation",)
    category = "varnish"

    def __call__(self):
        VarnishPurgeHookOperation.get_instance(self._cw).add_data(self.eidfrom)


class PurgeSiteUrlHook(hook.Hook):
    """purge its SiteUrl data"""

    __regid__ = "frarchives_edition.siteurl.varnish"
    __select__ = hook.Hook.__select__ & is_instance("SiteLink")
    events = ("after_update_entity", "after_delete_entity")
    category = "varnish"

    def __call__(self):
        VarnishPurgeHookOperation.get_instance(self._cw).add_data(self.entity.eid)


class VarnishPurgeHookOperation(VarnishPurgeMixin, hook.DataOperationMixIn, hook.LateOperation):
    def postcommit_event(self):
        for eid in self.get_data():
            entity = self.cnx.entity_from_eid(eid)
            ivarnish = entity.cw_adapt_to("IVarnish")
            urls_to_purge = [url for url in ivarnish.urls_to_purge()]
            self.purge_varnish(urls_to_purge, self.cnx.vreg.config)

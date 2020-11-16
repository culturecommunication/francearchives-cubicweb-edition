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

from logilab.common.decorators import monkeypatch

from cubicweb_francearchives.entities.cms import TranslatableCmsObject
from cubicweb_francearchives.entities.es import PniaIFullTextIndexSerializable
from cubicweb_francearchives.entities.ead import FAComponentIFTIAdapter

_orig_serialize = PniaIFullTextIndexSerializable.serialize


@monkeypatch(PniaIFullTextIndexSerializable)
def serialize(self, *args, **kwargs):
    data = _orig_serialize(self, *args, **kwargs)
    wf = self.entity.cw_adapt_to("IWorkflowable")
    if wf and wf.state:
        data["in_state"] = wf.state
    return data


_orig_fa_serialize = FAComponentIFTIAdapter.serialize


@monkeypatch(FAComponentIFTIAdapter)  # noqa
def serialize(self, *args, **kwargs):  # noqa
    data = _orig_fa_serialize(self, *args, **kwargs)
    if self.entity.cw_etype == "FindingAid":
        wf = self.entity.cw_adapt_to("IWorkflowable")
        if wf and wf.state:
            data["in_state"] = wf.state
    return data


_orig_i18n_query = TranslatableCmsObject.i18n_query


@monkeypatch(TranslatableCmsObject)  # noqa
def i18n_query(self, *args, **kwargs):
    query = _orig_i18n_query(self, *args, **kwargs)
    if kwargs.get("published"):
        query += ", X in_state S, S name 'wfs_cmsobject_published'"
    return query

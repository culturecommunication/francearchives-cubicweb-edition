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

from pyramid.httpexceptions import HTTPNotFound

from logilab.common.decorators import monkeypatch
from cubicweb import NoResultError


from cubicweb_jsonschema.resources.entities import EntityResource

from cubicweb_jsonschema.resources import parent
from cubicweb_jsonschema.resources.schema import EntitySchema

_orig_eresource_getitem = EntityResource.__getitem__


@monkeypatch(EntityResource)  # noqa patch jsonschema EntityResource
def __getitem__(self, value):
    try:
        entity = self.rset.one()
    except NoResultError:
        raise HTTPNotFound()
    if value == "transitions":
        if "in_state" not in entity.e_schema.subject_relations():
            raise HTTPNotFound("entity type {0} not workflowable".format(entity.cw_etype))
        return WorkflowTransitionResource(self.request, parent=self)
    return _orig_eresource_getitem(self, value)


_orig_eschema_getitem = EntitySchema.__getitem__


@monkeypatch(EntitySchema)  # noqa patch jsonschema EntityShema
def __getitem__(self, value):
    try:
        entity = self.rset.one()
    except NoResultError:
        raise HTTPNotFound()
    if value == "transitions":
        if "in_state" not in entity.e_schema.subject_relations():
            raise HTTPNotFound("entity type {0} not workflowable".format(entity.cw_etype))
        return WorkflowTransitionResource(self.request, parent=self)
    return _orig_eschema_getitem(self, value)


class WorkflowTransitionResource(object):
    @parent
    def __init__(self, request, **kwargs):
        self.request = request

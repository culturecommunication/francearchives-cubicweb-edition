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

from pyramid.decorator import reify

from rql import TypeResolverException


class SameAsResource(object):

    def __init__(self, request, authority):
        self.request = request
        self.authority = authority


class IndexResources(object):

    def __init__(self, request, target):
        self.request = request
        self.target = target

    @reify
    def rset(self):
        cnx = self.request.cw_cnx
        return cnx.execute(
            '''
            (
              Any X, XL, XT WHERE X index A, A eid %(e)s, X label XL, X type XT, X is AgentName
            )
            UNION
            (
              Any X, XL, 'geogname' WHERE X index A, A eid %(e)s, X label XL, X is Geogname
            )
            UNION
            (
              Any X, XL, 'subject' WHERE X index A, A eid %(e)s, X label XL, X is Subject
            )
            ''', {'e': self.target.eid}
        )


class AuthorityResource(object):
    known_resources = {
        'same_as': SameAsResource,
    }

    def __init__(self, request, eid):
        self.request = request
        self.eid = eid

    @reify
    def entity(self):
        cnx = self.request.cw_cnx
        return cnx.execute(
            'Any X WHERE X eid %(e)s, X is IN '
            '(LocationAuthority, AgentAuthority, SubjectAuthority)',
            {'e': self.eid}
        ).one()

    def __getitem__(self, value):
        return self.known_resources[value](self.request, self.entity)


class FAComponentResource(object):
    known_resources = {
        'indexes': IndexResources,
    }

    def __init__(self, request, eid):
        self.request = request
        self.eid = eid

    @reify
    def entity(self):
        cnx = self.request.cw_cnx
        return cnx.find('FAComponent', eid=self.eid).one()

    def __getitem__(self, value):
        return self.known_resources[value](self.request, self.entity)


class FindingaidResource(object):
    known_resources = {
        'indexes': IndexResources,
    }

    def __init__(self, request, eid):
        self.request = request
        self.eid = eid

    @reify
    def entity(self):
        cnx = self.request.cw_cnx
        return cnx.find('FindingAid', eid=self.eid).one()

    def __getitem__(self, value):
        return self.known_resources[value](self.request, self.entity)


class RelatedAuthorityResource(object):

    def __init__(self, request, index):
        self.request = request
        self.index = index


class IndexResource(object):
    known_resources = {
        'authority': RelatedAuthorityResource,
    }

    def __init__(self, request, eid):
        self.request = request
        self.eid = eid

    @reify
    def entity(self):
        cnx = self.request.cw_cnx
        return cnx.execute(
            'Any X WHERE X eid %(e)s, X is IN (AgentName, Geogname, Subject)',
            {'e': self.eid}
        ).one()

    def __getitem__(self, value):
        return self.known_resources[value](self.request, self.entity)


class FAComponentsResource(object):

    def __init__(self, request):
        self.request = request

    def __getitem__(self, value):
        if value.isdigit():
            resource = FAComponentResource(self.request, int(value))
            try:
                resource.entity
                return resource
            except TypeResolverException:
                # no entity found for this resource
                raise KeyError(value)
        raise KeyError(value)


class FindingaidsResource(object):

    def __init__(self, request):
        self.request = request

    def __getitem__(self, value):
        if value.isdigit():
            resource = FindingaidResource(self.request, int(value))
            try:
                resource.entity
                return resource
            except TypeResolverException:
                # no entity found for this resource
                raise KeyError(value)
        raise KeyError(value)


class IndexesResource(object):

    def __init__(self, request):
        self.request = request

    def __getitem__(self, value):
        if value.isdigit():
            resource = IndexResource(self.request, int(value))
            try:
                resource.entity
                return resource
            except TypeResolverException:
                # no entity found for this resource
                raise KeyError(value)
        raise KeyError(value)


class AuthoritiesResource(object):

    def __init__(self, request):
        self.request = request

    def __getitem__(self, value):
        if value.isdigit():
            resource = AuthorityResource(self.request, int(value))
            try:
                resource.entity
                return resource
            except TypeResolverException:
                # no entity found for this resource
                raise KeyError(value)
        raise KeyError(value)


class RootResource(object):
    known_resources = {
        'facomponent': FAComponentsResource,
        'findingaid': FindingaidsResource,
        'index': IndexesResource,
        'authority': AuthoritiesResource,
    }

    def __init__(self, request):
        self.request = request

    def __getitem__(self, value):
        return self.known_resources[value.lower()](self.request)

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

from cubicweb.server import hook
from cubicweb.predicates import score_entity, is_instance

from cubicweb_frarchives_edition import (update_samesas_history,
                                         GEONAMES_RE)
from cubicweb_francearchives.entities.es import SUGGEST_ETYPES


def type_sameas_entities(cnx, eidto, eidfrom):
    exturi = cnx.entity_from_eid(eidto)
    autheid = eidfrom
    if exturi.cw_etype != 'ExternalUri':
        # same_as as a symmetric relation
        exturi = cnx.entity_from_eid(eidfrom)
        autheid = eidto
    return (exturi, autheid)


def suggest_indexable(entity):
    return entity.cw_etype in SUGGEST_ETYPES


class UpdateSuggestIndexES(hook.Hook):

    """detects content change and updates Suggest ES indexing"""

    __regid__ = 'pnia.contentupdatesuggest'
    __select__ = (hook.Hook.__select__
                  & score_entity(suggest_indexable))
    events = ('after_update_entity', )
    category = 'reindex-suggest-es'

    def __call__(self):
        SuggestIndexEsOperation.get_instance(self._cw).add_data(
            self.entity.eid)


class SuggestIndexEsOperation(hook.DataOperationMixIn, hook.LateOperation):
    def postcommit_event(self):
        cnx = self.cnx
        data = []
        for eid in self.get_data():
            data.append(cnx.entity_from_eid(eid))
        if data:
            service = cnx.vreg['services'].select('reindex-suggest', cnx)
            service.index_authorities(data)


class AddSameAsHistory(hook.Hook):
    __regid__ = 'frarchives_edition.add-sameas-history'
    __select__ = hook.Hook.__select__ & hook.match_rtype('same_as')
    events = ('after_add_relation',)
    category = 'align'

    def __call__(self):
        RegisterSameAsHistoryOp.get_instance(self._cw).add_data(
            (self.eidfrom, self.eidto, 1)
        )


class DeleteSameAsHistory(hook.Hook):
    __regid__ = 'frarchives_edition.delete-sameas-history'
    __select__ = hook.Hook.__select__ & hook.match_rtype('same_as')
    events = ('after_delete_relation',)
    category = 'align'

    def __call__(self):
        RegisterSameAsHistoryOp.get_instance(self._cw).add_data(
            (self.eidfrom, self.eidto, 0)
        )


class RegisterSameAsHistoryOp(hook.DataOperationMixIn, hook.Operation):

    def precommit_event(self):
        cnx = self.cnx
        records = []
        for eidfrom, eidto, action in self.get_data():
            exturi, autheid = type_sameas_entities(cnx, eidto, eidfrom)
            records.append((exturi.uri, autheid, action),)
        update_samesas_history(cnx, records)


class AddSameAsLocalisation(hook.Hook):
    __regid__ = 'frarchives_edition.add-sameas-localisation'
    __select__ = hook.Hook.__select__ & hook.match_rtype('same_as')
    events = ('after_add_relation',)
    category = 'align'

    def __call__(self):
        RegisterSameAsLocalisationOp.get_instance(self._cw).add_data(
            (self.eidto, self.eidfrom, 1)
        )


class DeleteSameAsLocalisation(hook.Hook):
    __regid__ = 'frarchives_edition.delete-sameas-localisation'
    __select__ = hook.Hook.__select__ & hook.match_rtype('same_as')
    events = ('after_delete_relation',)
    category = 'align'

    def __call__(self):
        RegisterSameAsLocalisationOp.get_instance(self._cw).add_data(
            (self.eidto, self.eidfrom, 0)
        )


def get_geonames_coordinates(cnx, uri):
    m = GEONAMES_RE.search(uri)
    if m:
        gid = int(m.group(1))
        q = '''
        SELECT latitude, longitude
        FROM geonames WHERE geonameid=%(gid)s
        '''
        cr = cnx.system_sql(q, {'gid': gid})
        if cr:
            return cr.fetchall()


class RegisterSameAsLocalisationOp(hook.DataOperationMixIn, hook.Operation):

    def precommit_event(self):
        cnx = self.cnx
        data = []
        for eidfrom, eidto, action in self.get_data():
            exturi, autheid = type_sameas_entities(cnx, eidto, eidfrom)
            if cnx.deleted_in_transaction(autheid):
                continue
            auth = cnx.entity_from_eid(autheid)
            if auth.__regid__ != 'LocationAuthority':
                continue
            m = GEONAMES_RE.search(exturi.uri)
            if m:
                data.append((int(m.group(1)), auth, action))
        if data:
            q = '''
            SELECT geonameid, latitude, longitude
            FROM geonames WHERE geonameid IN (%(geonameids)s)
            '''
            geonameids = ", ".join(str(gid) for gid, auth, action in data)
            georecords = dict((gid, (lat, longt)) for gid, lat, longt in
                              cnx.system_sql(q % {"geonameids": geonameids}).fetchall())
        for gid, auth, action in data:
            coordinates = georecords.get(gid)
            if not coordinates:
                continue
            lat, longt = coordinates
            if lat and longt:
                if action == 1:
                    auth.cw_set(latitude=lat, longitude=longt)
                else:
                    if auth.latitude == lat and auth.longitude == longt:
                        # search for other links : take the fist geoname link
                        # for coordinates
                        for uri in auth.same_as:
                            coor = get_geonames_coordinates(cnx, uri.uri)
                            if coor:
                                auth.cw_set(latitude=coor[0][0], longitude=coor[0][1])
                                break
                        else:
                            auth.cw_set(latitude=None, longitude=None)


def update_external_uri_source(entity):
    source = u'geoname'
    m = GEONAMES_RE.search(entity.uri)
    if m and entity.source != source:
        entity.cw_edited['source'] = source


class AddSourceExternalUri(hook.Hook):
    """always set geoname source on geoname uri"""
    __regid__ = 'facms.frarchives_edition.exturi.add.source'
    __select__ = hook.Hook.__select__ & is_instance('ExternalUri')
    events = ('before_add_entity', 'before_update_entity')
    category = 'external_uri_source'

    def __call__(self):
        update_external_uri_source(self.entity)

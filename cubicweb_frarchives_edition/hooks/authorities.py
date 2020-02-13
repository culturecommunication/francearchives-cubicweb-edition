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
from collections import defaultdict

from cubicweb.server import hook
from cubicweb.predicates import score_entity, is_instance

from cubicweb_frarchives_edition import get_leaflet_cache_entities
from cubicweb_francearchives.entities.es import SUGGEST_ETYPES

from cubicweb_frarchives_edition import update_samesas_history, GEONAMES_RE

from cubicweb_frarchives_edition.alignments import (
    compute_label_from_url,
    DATABNF_RE,
    DATABNF_ARK_RE,
    DATABNF_SOURCE,
    WIKIDATA_RE,
    WIKIDATA_SOURCE,
)

from cubicweb_frarchives_edition.alignments.databnf import DataBnfDatabase
from cubicweb_frarchives_edition.alignments.wikidata import WikidataDatabase


def type_sameas_uri(cnx, eidto, eidfrom):
    """if none of subject or object is an ExternalUri or ExternalId
       return None
    """
    obj = cnx.entity_from_eid(eidto)
    subj = cnx.entity_from_eid(eidfrom)
    if obj.cw_etype in ("ExternalUri", "ExternalId"):
        return (obj, subj)
    # same_as as a symmetric relation
    if subj.cw_etype in ("ExternalUri", "ExternalId"):
        return (subj, obj)
    return (None, None)


def suggest_indexable(entity):
    return entity.cw_etype in SUGGEST_ETYPES


class UpdateSuggestIndexES(hook.Hook):

    """detects content change and updates Suggest ES indexing"""

    __regid__ = "pnia.contentupdatesuggest"
    __select__ = hook.Hook.__select__ & score_entity(suggest_indexable)
    events = ("after_update_entity",)
    category = "reindex-suggest-es"

    def __call__(self):
        SuggestIndexEsOperation.get_instance(self._cw).add_data(self.entity.eid)


class SuggestIndexEsOperation(hook.DataOperationMixIn, hook.LateOperation):
    def postcommit_event(self):
        cnx = self.cnx
        data = []
        for eid in self.get_data():
            data.append(cnx.entity_from_eid(eid))
        if data:
            if cnx.vreg.config.mode == "test":
                return
            service = cnx.vreg["services"].select("reindex-suggest", cnx)
            service.index_authorities(data)


class AddSameAsHistory(hook.Hook):
    __regid__ = "frarchives_edition.add-sameas-history"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_add_relation",)
    category = "align"

    def __call__(self):
        RegisterSameAsHistoryOp.get_instance(self._cw).add_data((self.eidfrom, self.eidto, 1))


class DeleteSameAsHistory(hook.Hook):
    __regid__ = "frarchives_edition.delete-sameas-history"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_delete_relation",)
    category = "align"

    def __call__(self):
        RegisterSameAsHistoryOp.get_instance(self._cw).add_data((self.eidfrom, self.eidto, 0))


class RegisterSameAsHistoryOp(hook.DataOperationMixIn, hook.Operation):
    def precommit_event(self):
        cnx = self.cnx
        records = []
        for eidfrom, eidto, action in self.get_data():
            exturi, auth = type_sameas_uri(cnx, eidto, eidfrom)
            if exturi:
                records.append((exturi.samesas_history_id, auth.eid, action),)
        if records:
            update_samesas_history(cnx, records)


class AddSameAsLocalisation(hook.Hook):
    __regid__ = "frarchives_edition.add-sameas-localisation"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_add_relation",)
    category = "align"

    def __call__(self):
        RegisterSameAsLocalisationOp.get_instance(self._cw).add_data((self.eidto, self.eidfrom, 1))


class DeleteSameAsLocalisation(hook.Hook):
    __regid__ = "frarchives_edition.delete-sameas-localisation"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_delete_relation",)
    category = "align"

    def __call__(self):
        RegisterSameAsLocalisationOp.get_instance(self._cw).add_data((self.eidto, self.eidfrom, 0))


def get_auth_coordinates(uri):
    igeo = uri.cw_adapt_to("IGeoDB")
    if igeo:
        return (igeo.latitude, igeo.longitude)


class RegisterSameAsLocalisationOp(hook.DataOperationMixIn, hook.Operation):
    def get_geo_coordinates(self, candidates):
        records = []
        cnx = self.cnx
        extids = candidates["externalid"]
        if extids:
            q = """
            SELECT lat, lon
            FROM bano_whitelisted WHERE banoid IN (%(banoids)s)
            """
            extid_ids = ", ".join(str(extid) for extid in extids)
            res = cnx.system_sql(q, {"banoids": extid_ids}).fetchall()
            if res:
                records.extend(res)
        gids = candidates["externaluri"]
        if gids:
            q = """
            SELECT latitude, longitude
            FROM geonames WHERE geonameid IN (%(geonameids)s)
            """
            geonameids = ", ".join(str(gid) for gid in gids)
            res = cnx.system_sql(q % {"geonameids": geonameids}).fetchall()
            if res:
                records.extend(res)
        return records

    def change_autority_coordinates(self, auth):
        kwargs = {"latitude": None, "longitude": None}
        uris = auth.same_as
        if uris:
            uris = list(uris)
            # search for other coordonnates: first consider Bano alignments
            uris.sort(key=lambda x: x.cw_etype != "ExternalId")
            for uri in uris:
                igeo = uri.cw_adapt_to("IGeoDB")
                if igeo:
                    latitude, longitude = igeo.latitude, igeo.longitude
                    if latitude:
                        kwargs = {"latitude": latitude, "longitude": longitude}
                        break
        auth.cw_set(**kwargs)

    def precommit_event(self):
        cnx = self.cnx
        to_delete_candidates = defaultdict(lambda: defaultdict(list))
        to_add_candidates = defaultdict(lambda: defaultdict(list))
        for eidfrom, eidto, action in self.get_data():
            if cnx.deleted_in_transaction(eidfrom) or cnx.deleted_in_transaction(eidto):
                continue
            exturi, auth = type_sameas_uri(cnx, eidto, eidfrom)
            if exturi is None or auth.__regid__ != "LocationAuthority":
                continue
            geoid = None
            if exturi.cw_etype == "ExternalUri":
                if exturi.source == "geoname" and exturi.extid:
                    geoid = exturi.extid
                else:
                    m = GEONAMES_RE.search(exturi.uri)
                    if m:
                        geoid = m.group(1)
                        if not exturi.extid:
                            # add extid if missing which
                            # this can happen on shared external_uri
                            with cnx.allow_all_hooks_but("external_uri_source"):
                                exturi.cw_set(extid=geoid)
            elif exturi.cw_etype == "ExternalId":
                geoid = exturi.extid
            if geoid:
                if action == 0:
                    to_delete_candidates[auth][exturi.cw_etype.lower()].append(geoid)
                else:
                    # we take a random coordinates to set on authority
                    to_add_candidates[auth][exturi.cw_etype.lower()] = [geoid]
        for auth, exturis in list(to_delete_candidates.items()):
            coords = self.get_geo_coordinates(exturis)
            if (auth.latitude, auth.longitude) in coords:
                self.change_autority_coordinates(auth)
        for auth, exturi in list(to_add_candidates.items()):
            # only ExternalUri can be added by users
            if not any((u for u in auth.same_as if u.cw_etype == "ExternalId")):
                coords = self.get_geo_coordinates(exturi)
                if coords:
                    auth.cw_set(latitude=coords[0][0], longitude=coords[0][1])


class UpdateExternalUriSourceHook(hook.Hook):
    """1/ Set a source on ExternalUri uri
       2/ Set an extid on ExternalUri extid """

    __regid__ = "facms.frarchives_edition.exturi.add.source"
    __select__ = hook.Hook.__select__ & is_instance("ExternalUri")
    events = ("before_add_entity", "before_update_entity")
    category = "external_uri_source"

    def __call__(self):
        entity = self.entity
        for source, regx in (
            ("geoname", GEONAMES_RE),
            (DATABNF_SOURCE, DATABNF_RE),
            (DATABNF_SOURCE, DATABNF_ARK_RE),
            (WIKIDATA_SOURCE, WIKIDATA_RE),
        ):
            m = regx.search(entity.uri)
            if m:
                entity.cw_edited["source"] = source
                entity.cw_edited["extid"] = m.group(1)
                break


class SameAsRelHook(hook.Hook):
    """
    Try to align with data.bnf.fr or wikidata.org
    """

    __regid__ = "frarchives_edition.persons_data"
    __select__ = hook.Hook.__select__ & hook.match_rtype("same_as")
    events = ("after_add_relation",)

    def __call__(self):
        exturi, auth = type_sameas_uri(self._cw, self.eidto, self.eidfrom)
        if exturi:
            if exturi.source == DATABNF_SOURCE:
                AuthorityBnFDataOperation.get_instance(self._cw).add_data(self.eidto)
            if exturi.source == WIKIDATA_SOURCE:
                AuthorityWikiDataOperation.get_instance(self._cw).add_data(self.eidto)


class AgentAuthorityDataOperation(hook.DataOperationMixIn, hook.LateOperation):
    def precommit_event(self):
        cnx = self.cnx
        aligner = self.aligner()
        for eid in self.get_data():
            if cnx.deleted_in_transaction(eid):
                continue
            exturi = cnx.entity_from_eid(eid)
            try:
                data = aligner.agent_infos(exturi.extid)
            except Exception:
                continue
            if data:
                # add ExternalUri label
                label = data.pop("label", None)
                if label and not exturi.label:
                    exturi.cw_set(label=label)
                # create a new AgentInfo
                cnx.execute(
                    """DELETE AgentInfo X WHERE X agent_info_of U,
                       U eid {eid}""".format(
                        eid=exturi.eid
                    )
                )
                data["agent_info_of"] = eid
                cnx.create_entity("AgentInfo", **data)


class AuthorityBnFDataOperation(AgentAuthorityDataOperation):
    aligner = DataBnfDatabase


class AuthorityWikiDataOperation(AgentAuthorityDataOperation):
    aligner = WikidataDatabase


# leaflet map related hooks


class LocationAuthorityLeafletMap(hook.Hook):
    __regid__ = "francearchives.geo-map"
    __select__ = hook.Hook.__select__ & is_instance("LocationAuthority")
    events = ("after_add_entity", "after_update_entity")

    def __call__(self):
        if "latitude" in self.entity.cw_edited:
            latitude = self.entity.cw_edited["latitude"]
            action = False if latitude is None else True
            leaflet_op = LocationAuthorityLeafletMapOp.get_instance(self._cw)
            leaflet_op.add_data((self.entity.eid, action))


class IndexLeafletMap(hook.Hook):
    __regid__ = "francearchives.geo-map.index"
    __select__ = hook.Hook.__select__ & hook.match_rtype("authority")
    events = (
        "after_add_relation",
        "after_delete_relation",
    )

    def __call__(self):
        leaflet_op = LocationAuthorityLeafletMapOp.get_instance(self._cw)
        if self._cw.entity_from_eid(self.eidto).cw_adapt_to("ILeaflet"):
            leaflet_op.add_data(((self.eidto, self.event == "after_add_relation")))


class LocationAuthorityLeafletMapOp(hook.DataOperationMixIn, hook.SingleLastOperation):
    def precommit_event(self):
        cnx = self.cnx
        caches = get_leaflet_cache_entities(cnx)
        if not caches:
            self.warning("no leaflet cache found")
            return
        values = {}
        for cache in caches.entities():
            values[cache] = cache.values
        for loc_eid, action in self.get_data():
            for cache, ljson in list(values.items()):
                # remove old locationauthority values
                loc_records = [r for r in ljson if r["eid"] == loc_eid]
                for record in loc_records:
                    ljson.remove(record)
                values[cache] = ljson
            if action == 1:
                # add location to json
                loc = cnx.entity_from_eid(loc_eid)
                for cache, ljson in list(values.items()):
                    loc_json = loc.cw_adapt_to("ILeaflet").json(cache.instance_type)
                    if loc_json:
                        ljson.extend(loc_json)
                        values[cache] = ljson
        for cache, ljson in list(values.items()):
            cache.cw_set(values=ljson)


class GeonamesLabelCreationHook(hook.Hook):
    __regid__ = "francearchives.geonames.create.label"
    __select__ = hook.Hook.__select__ & is_instance("ExternalUri")
    events = ("after_add_entity",)
    category = "sameas-label"

    def __call__(self):
        self.process_label()

    def process_label(self):
        entity = self.entity
        label = entity.cw_edited.get("label")
        if not label:
            uri = entity.cw_edited.get("uri")
            new_label = compute_label_from_url(self._cw, uri)
            if new_label:
                GeonamesLabelCreationOperation.get_instance(self._cw).add_data(
                    (self.entity.eid, new_label)
                )


class GeonamesLabelCreationOperation(hook.DataOperationMixIn, hook.Operation):
    def precommit_event(self):
        cnx = self.cnx
        for (eid, label) in self.get_data():
            if cnx.deleted_in_transaction(eid):
                continue
            entity = cnx.entity_from_eid(eid)
            with cnx.allow_all_hooks_but("sameas-label"):
                entity.cw_set(label=label)

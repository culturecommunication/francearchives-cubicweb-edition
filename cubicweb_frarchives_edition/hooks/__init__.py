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
import re
import io
from itertools import chain

import urllib.parse

from lxml import etree

import requests

from cubicweb.predicates import is_instance, relation_possible, score_entity

from cubicweb.server import hook
from cubicweb import Unauthorized, ValidationError

from rql import RQLSyntaxError

from cubicweb_varnish.hooks import InvalidateVarnishCacheOp

from cubicweb_francearchives.schema.cms import CMS_OBJECTS
from cubicweb_francearchives.entities.cms import MapCSVReader


def custom_on_fire_transition(etypes, tr_names):
    def match_etype_and_transition(trinfo):
        # take care trinfo.transition is None when calling change_state
        return (
            trinfo.transition
            and trinfo.transition.name in tr_names
            # is_instance() first two arguments are 'cls' (unused, so giving
            # None is fine) and the request/session
            and is_instance(*etypes)(None, trinfo._cw, entity=trinfo.for_entity)
        )

    return is_instance("TrInfo") & score_entity(match_etype_and_transition)


def is_undeletable_card(entity):
    undeletable_wikiid = ("alert",)
    if entity.cw_etype == "Card" and entity.wikiid in undeletable_wikiid:
        return True
    return False


class UndeletableCards(hook.Hook):
    __regid__ = "francearchives.delete-card"
    __select__ = hook.Hook.__select__ & score_entity(lambda x: is_undeletable_card(x))
    events = ("before_delete_entity",)

    def __call__(self):
        raise Unauthorized("delete", self._cw._("Impossible to delete this card"))


# should match download_url() generated for File entities
# (cf. cubicweb_francearchives.entities.FAFile.rest_path and
# cubicweb_francearchives.entities.adapters.FAFileAdapter.download_url)
FILE_URL_RE = re.compile(r"/file/([a-f0-9]{40})/([^/]+)$")


def files_query_from_content(content):
    try:
        tree = etree.HTML(content)
    except Exception:
        return ()
    all_matches = []
    for el in chain(tree.findall(".//a"), tree.findall(".//img")):
        src = el.get("href") or el.get("src")
        if not src:
            continue
        src = urllib.parse.unquote(src)
        match = FILE_URL_RE.search(src)
        if match is None:
            continue
        all_matches.append(match.groups())
    return [
        (
            "Any F ORDERBY O, F LIMIT 1 WHERE F is File, O? referenced_files F,"
            'F data_hash "%s", F data_name "%s"'
        )
        % m
        for m in all_matches
    ]


class PniaCreateMentionFilesRel(hook.Hook):
    """ rich text strings"""

    __regid__ = "francearchives.referenced_files"
    __select__ = hook.Hook.__select__ & relation_possible("referenced_files")
    events = ("before_add_entity", "before_update_entity")

    def __call__(self):
        CreateReferencedFilesOp.get_instance(self._cw).add_data(self.entity)


class DeleteMixin(object):
    def delete_entity(self, uuid, etype, sync_url):
        if uuid is None:
            return
        uuid_attr, uuid_value = uuid
        try:
            url = "{}/_update/{}/{}".format(sync_url, etype, uuid_value)
            self.debug("will delete %s", url)
            res = requests.delete(url)
            if res.status_code == 400:
                # in ``edit.get_by_uuid`` we raise ``HTTPBadRequest`` if no entity found for
                # this uuid
                self.debug(
                    "%s with %s: %s does not exists on %s", etype, uuid_attr, uuid_value, sync_url
                )
                return
            res.raise_for_status()
        except Exception:
            self.exception("failed to sync %s with %s %s", etype, uuid_attr, uuid_value)


class CreateReferencedFilesOp(hook.DataOperationMixIn, hook.Operation):
    """update `referenced_files` relation for entity with an attribute edited
    with TinyMCE"""

    def postcommit_event(self):
        for entity in self.get_data():
            edited = entity.cw_edited
            uischema = entity.cw_adapt_to("IJsonFormEditable").ui_schema()
            attrs = [
                attr
                for attr, descr in list(uischema.items())
                if descr.get("ui:widget") and attr in edited
            ]
            if not attrs:
                # do not execute the hook if no attrs other than html have been modified
                return
            already_linked = {e.eid for e in entity.referenced_files}
            files = set()
            for attr in attrs:
                value = edited[attr]
                queries = files_query_from_content(value)
                if not queries:
                    continue
                query = " UNION ".join("(%s)" % q for q in queries)
                try:
                    files |= {eid for eid, in self.cnx.execute(query)}
                except RQLSyntaxError:
                    self.exception('fail to execute query "%r"', query)
            to_remove = already_linked - files
            if to_remove:
                self.cnx.execute(
                    "DELETE X referenced_files Y WHERE X eid %(e)s, Y eid IN ({})".format(
                        ",".join(str(e) for e in to_remove)
                    ),
                    {"e": entity.eid},
                )
            to_add = files - already_linked
            if to_add:
                self.cnx.execute(
                    "SET X referenced_files Y WHERE X eid %(e)s, Y eid IN ({})".format(
                        ",".join(str(e) for e in to_add)
                    ),
                    {"e": entity.eid},
                )


class PniaRemoveReferencedFilesRel(hook.Hook):
    """ rich text strings"""

    __regid__ = "francearchives.remove_referenced_files"
    __select__ = hook.Hook.__select__ & hook.match_rtype("referenced_files")
    events = ("before_delete_relation",)

    def __call__(self):
        PniaRemoveReferencedFilesOperation.get_instance(self._cw).add_data(self.eidto)


class PniaRemoveReferencedFilesOperation(hook.DataOperationMixIn, DeleteMixin, hook.Operation):
    """delete files which are no more linked by referenced_files relation"""

    def postcommit_event(self):
        eids = list(self.get_data())
        eschema = self.cnx.vreg.schema.eschema("File")
        assert len([e for e in eschema.subjrels if not e.meta and not e.final]) == 0
        rels = [
            "NOT EXISTS(X{i} {rel} F) ".format(i=i, rel=rel)
            for i, rel in enumerate([e.type for e in eschema.objrels if not e.meta])
        ]
        query = "DELETE File F WHERE {rels}, F eid IN ({eids})".format(
            rels=", ".join(rels), eids=",".join([str(e) for e in eids])
        )
        self.cnx.execute(query)


class PublishWebPage(hook.Hook):
    """register publish-webpage operation"""

    __regid__ = "frarchives_edition.publish-webpage"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        ("Card",) + CMS_OBJECTS, {"wft_cmsobject_publish"}
    )
    events = ("after_add_entity",)
    category = "sync"

    def __call__(self):
        cmsobject = self.entity.for_entity
        PublishWebPageOperation.get_instance(self._cw).add_data(cmsobject)


class UnPublishWebPage(hook.Hook):
    """register unpublish-webpage operation"""

    __regid__ = "frarchives_edition.unpublish-webpage"
    __select__ = hook.Hook.__select__ & custom_on_fire_transition(
        ("Card",) + CMS_OBJECTS, {"wft_cmsobject_unpublish"}
    )
    events = ("after_add_entity",)
    category = "sync"

    def __call__(self):
        cmsobject = self.entity.for_entity
        UnPublishWebPageOperation.get_instance(self._cw).add_data(cmsobject)


class MonitorChanges(hook.Hook):
    __regid__ = "frarchives_edition.monitor-changes"
    __select__ = hook.Hook.__select__ & (
        score_entity(lambda e: hasattr(e, "uuid_attr")) | relation_possible("uuid")
    )
    events = ("before_add_entity", "before_update_entity")
    category = "sync"

    def __call__(self):
        entity = self.entity
        # if only modification date has changed, it's an artifcat on some
        # metadata hooks, ignore it
        if list(entity.cw_edited.keys()) == ["modification_date"]:
            return
        SyncEntityChangesOperation.get_instance(self._cw).add_data((entity.eid, entity.cw_edited))


class MonitorDeletes(hook.Hook):
    __regid__ = "frarchives_edition.monitor-deletes"
    __select__ = hook.Hook.__select__ & (
        score_entity(lambda e: hasattr(e, "uuid_attr")) | relation_possible("uuid")
    )
    events = ("before_delete_entity",)
    category = "sync"

    def __call__(self):
        entity = self.entity
        # if only modification date has changed, it's an artifcat on some
        # metadata hooks, ignore it
        if hasattr(entity, "cw_edited") and list(entity.cw_edited.keys()) == ["modification_date"]:
            return
        uuid = get_uuid(entity)
        DeleteEntitiesOperation.get_instance(self._cw).add_data((uuid, entity.cw_etype))


def get_uuid(entity):
    eschema = entity.e_schema
    try:
        eschema.subjrels["uuid"]
        return "uuid", entity.uuid
    except KeyError:
        pass
    uuid_attr = getattr(entity, "uuid_attr", None)
    if uuid_attr is None:
        return
    return uuid_attr, getattr(entity, uuid_attr)


class MonitorCompoudEntityChanges(hook.Hook):
    """change the modification date on the composite parent in ordre to force
       ContentUpdateIndexES on it"""

    events = ("after_update_entity",)
    __regid__ = "frarchives_edition.compoud-monitor-changes"
    category = "sync"

    def __call__(self):
        entity = self.entity
        icompound = entity.cw_adapt_to("ICompound")
        if icompound is not None:
            for related in icompound.roots:
                if "modification_date" not in getattr(related, "cw_edited", ()):
                    related.cw_set(modification_date=entity.modification_date)


class MonitorRelationChanges(hook.Hook):
    __regid__ = "frarchives_edition.relation-monitor-changes"
    events = ("after_add_relation", "after_delete_relation")
    category = "sync"

    @staticmethod
    def has_uuid(entity):
        eschema = entity.e_schema
        try:
            eschema.subjrels["uuid"]
            return True
        except KeyError:
            return hasattr(entity, "uuid_attr")

    def __call__(self):
        rschema = self._cw.vreg.schema.rschema(self.rtype)
        # ignore meta relations or wf relations (already hanlded in another hook)
        if rschema.meta or rschema.type in {"wf_info_for", "in_state"}:
            return
        for eid in (self.eidfrom, self.eidto):
            entity = self._cw.entity_from_eid(eid)
            if not self.has_uuid(entity):
                return
        SyncRelationChangesOperation.get_instance(self._cw).add_data((self.eidfrom, self.eidto))


class UnPublishWebPageOperation(hook.DataOperationMixIn, DeleteMixin, hook.Operation):
    def postcommit_event(self):
        sync_url = self.cnx.vreg.config.get("consultation-sync-url")
        if not sync_url:
            return
        for entity in self.get_data():
            uuid = get_uuid(entity)
            self.delete_entity(uuid, entity.cw_etype, sync_url)


class PublishWebPageOperation(hook.DataOperationMixIn, hook.Operation):
    def postcommit_event(self):
        for entity in self.get_data():
            isync = entity.cw_adapt_to("ISync")
            isync.put_entity()
            try:
                # HACK update parent section to make sure "children" relation is set
                if entity.reverse_children:
                    section = entity.reverse_children[0]
                    section_state = section.cw_adapt_to("IWorkflowable").state
                    if section_state == "wfs_cmsobject_published":
                        sync_url = self.cnx.vreg.config.get("consultation-sync-url")
                        if sync_url:
                            res = requests.post(
                                "{}/_update/move/{}/{}".format(
                                    sync_url, entity.cw_etype, entity.uuid
                                ),
                                json={"to-section": section.uuid,},
                            )
                            res.raise_for_status()
            except Exception:
                self.exception(
                    "failed to put %s %s in its parent section", entity.cw_etype, entity.uuid
                )


class SyncCompoundMixin(object):
    def is_draft(self, entity):
        icompound = entity.cw_adapt_to("ICompound")
        if icompound is None:
            iwf = entity.cw_adapt_to("IWorkflowable")
            return iwf is not None and iwf.state != "wfs_cmsobject_published"
        if not icompound.roots:
            self.debug(
                "compound with no root %s (already deteled ? %s)",
                entity,
                self.cnx.deleted_in_transaction(entity.eid),
            )
            return False
        else:
            for root in icompound.roots:
                roots_wf = root.cw_adapt_to("IWorkflowable")
                if roots_wf is not None and roots_wf.state != "wfs_cmsobject_published":
                    return True


class SyncRelationChangesOperation(SyncCompoundMixin, hook.DataOperationMixIn, hook.Operation):
    """sync relation subject only if subject and object are published
    """

    def postcommit_event(self):
        done = set()
        for eid_from, eid_to in self.get_data():
            if eid_from in done:
                continue
            entity_from = self.cnx.entity_from_eid(eid_from)
            entity_to = self.cnx.entity_from_eid(eid_to)
            if self.is_draft(entity_from) or self.is_draft(entity_to):
                continue
            isync = entity_from.cw_adapt_to("ISync")
            isync.put_entity()
            done.add(entity_from.eid)


class SyncEntityChangesOperation(SyncCompoundMixin, hook.DataOperationMixIn, hook.Operation):
    """sync edited changes if one of root entity is already published.

    Otherwise ignore changes, they will be sync-ed when the "root" entity
    will be published.
    """

    def build_body(self, entity):
        body = {}
        eschema = entity.e_schema
        for attr in entity.cw_edited:
            rdef = eschema.rdef(attr)
            if rdef.final:
                body[attr] = entity.cw_edited[attr]
            else:
                related = self.cnx.entity_from_eid(entity.cw_edited[attr])
                uuid = get_uuid(related)
                if uuid is None:
                    return
                uuid_attr, uuid_value = uuid
                body[attr] = [{uuid_attr: uuid_value, "cw_etype": related.cw_etype,}]
        return body

    def postcommit_event(self):
        done = set()
        for eid, cw_edited in self.get_data():
            if len(cw_edited) == 1 and list(cw_edited.keys()) == ["modification_date"]:
                # ignore changes on modification date only (occurs when
                # publishing the object)
                continue
            entity = self.cnx.entity_from_eid(eid)
            if entity.eid in done:
                continue
            done.add(entity.eid)
            if self.is_draft(entity):
                continue
            self.debug("will call put_entity %s #%s (%s)", entity.cw_etype, entity.eid, cw_edited)
            isync = entity.cw_adapt_to("ISync")
            isync.put_entity(self.build_body(entity))


class DeleteEntitiesOperation(hook.DataOperationMixIn, DeleteMixin, hook.LateOperation):
    """sync deleted entities"""

    def postcommit_event(self):
        sync_url = self.cnx.vreg.config.get("consultation-sync-url")
        if not sync_url:
            return
        for uuid, etype in self.get_data():
            self.delete_entity(uuid, etype, sync_url)


class ValidateMapCSVFileSupportHook(hook.Hook, MapCSVReader):
    """Validate the uploaded csv has the right format."""

    __regid__ = "facms.validate-map-file"
    __select__ = hook.Hook.__select__ & is_instance("Map")
    events = ("before_add_entity", "before_update_entity")
    category = "bytes"

    def __call__(self):
        map_file = self.entity.cw_edited.get("map_file")
        if map_file:
            fp = io.StringIO(map_file.getvalue().decode("utf-8"))
            headers = self.csv_headers(fp)
            if headers != list(self.fieldnames.keys()):
                msg = self._cw._(
                    'CSV file invalid. It must contain "Code_insee", "URL", '
                    '"Couleur" and "Legende" headers'
                    'columns separated by ","'
                )
                raise ValidationError(self.entity.eid, {"map_file-subject": msg})
            errors = []
            fp.seek(0)
            for idx, line in enumerate(self.csv_reader(fp)):
                missing = [
                    k
                    for k, v in list(line.items())
                    if k in self.required_fields and not (v and v.strip())
                ]
                if missing:
                    errors.append(
                        self._cw._("line {}: missing value for {} columns").format(
                            idx + 1, ", ".join(missing)
                        )
                    )
            if errors:
                msg = self._cw._("Missing data")
                msg += "\n{}".format("\n".join('"{}"'.format(e) for e in errors))
                raise ValidationError(self.entity.eid, {"map_file-subject": msg})


class UniqueDServiceNameHook(hook.Hook):
    """Department services dpt_code and annex_of must be unique"""

    __regid__ = "facms.service-d-name"
    __select__ = hook.Hook.__select__ & is_instance("Service")
    events = ("before_add_entity", "before_update_entity")
    unique_attrs = set(("level", "dpt_code"))

    def __call__(self):
        if self.entity.cw_edited.get("code"):
            UniqueServiceCodeOperation.get_instance(self._cw).add_data(self.entity)


class UniqueServiceCodeOperation(hook.DataOperationMixIn, hook.Operation):
    def precommit_event(self):
        cnx = self.cnx
        for entity in self.get_data():
            if cnx.deleted_in_transaction(entity.eid):
                continue
            if entity.code:
                rset = cnx.execute(
                    """Any X WHERE X is Service, X code %(c)s,
                       NOT X eid %(eid)s""",
                    {"eid": entity.eid, "c": entity.code},
                )
            if rset:
                msg = cnx._('A Service with "%s" code already exists' % entity.code)
                raise ValidationError(entity.eid, {"code-subject": msg})


class CircularUpdateOfficialTextsHook(hook.Hook):
    __regid__ = "frarchives_edition.circular.update_official_text"
    __select__ = hook.Hook.__select__ & is_instance("OfficialText")
    events = ("before_update_entity",)

    def __call__(self):
        if "code" in self.entity.cw_edited:
            CircularAddOfficialTextsOp.get_instance(self._cw).add_data(self.entity.eid)


class CircularAddOfficialTextsHook(hook.Hook):
    __regid__ = "frarchives_edition.circular.add_official_text"
    events = ("before_add_relation",)
    __select__ = hook.Hook.__select__ & hook.match_rtype(
        "modified_text", "modifying_text", "revoked_text"
    )

    def __call__(self):
        CircularAddOfficialTextsOp.get_instance(self._cw).add_data(self.eidto)


class CircularAddOfficialTextsOp(hook.DataOperationMixIn, hook.LateOperation):
    queries = (
        "Any X WHERE X is Circular, X circ_id %(code)s",
        "Any X WHERE X is Circular, X siaf_daf_code %(code)s",
    )

    def postcommit_event(self):
        for eid in self.get_data():
            text = self.cnx.entity_from_eid(eid)
            code = text.code
            # find the related circular
            related = None
            for query in self.queries:
                related = self.cnx.execute(query, {"code": code})
                if related:
                    text.cw_set(circular=related.one())
                    break


class PurgeAuthoritiesUrl(hook.Hook):
    """an authority has been grouped with an other, purge its URL"""

    __regid__ = "frarchives_edition.authority.varnish"
    category = "varnish"
    __select__ = hook.Hook.__select__ & hook.match_rtype("grouped_with")
    events = ("after_add_relation",)

    def __call__(self):
        invalidate_cache_op = InvalidateVarnishCacheOp.get_instance(self._cw)
        entity = self._cw.entity_from_eid(self.eidfrom)
        ivarnish = entity.cw_adapt_to("IVarnish")
        for url in ivarnish.urls_to_purge():
            invalidate_cache_op.add_data(url)


def registration_callback(vreg):
    from cubicweb_varnish.hooks import PurgeUrlsOnUpdate
    from cubicweb_francearchives.hooks import PurgeUrlsOnAddOrDelete, UpdateVarnishOnRelationChanges

    vreg.register_all(list(globals().values()), __name__)
    vreg.unregister(PurgeUrlsOnAddOrDelete)
    vreg.unregister(UpdateVarnishOnRelationChanges)
    vreg.unregister(PurgeUrlsOnUpdate)

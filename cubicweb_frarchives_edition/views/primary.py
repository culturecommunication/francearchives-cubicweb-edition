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
import logging


from cwtags import tag as T

from logilab.mtconverter import xml_escape, html_unescape

from cubicweb.predicates import anonymous_user, is_instance
from cubicweb.web.views.primary import PrimaryView, URLAttributeView, uicfg
from cubicweb.utils import json_dumps
from cubicweb.view import EntityView

from cubicweb_francearchives import FIRST_LEVEL_SECTIONS
from cubicweb_francearchives.views import primary, circular, index, exturl_link
from cubicweb_francearchives.views.service import Service as ServiceView

from cubicweb_frarchives_edition.views import get_template

_pvs = uicfg.primaryview_section
for rel in (
    "referenced_files",
    "findingaid_support",
    "attachment",
    "fa_referenced_files",
    "output_file",
    "ape_ead_file",
    "alignment_result",
    "additional_attachment",
):
    _pvs.tag_object_of(("*", rel, "File"), "relations")


class SitemapView(primary.SitemapView):
    __select__ = primary.SitemapView.__select__ & ~anonymous_user()

    def call(self, **kw):
        self._cw.add_js("bundle-ext.js")
        self._cw.add_js("bundle-sitemap.js")
        super(SitemapView, self).call(**kw)


class EditionMixin(object):
    def entity_call(self, entity):
        self.add_editor_links(entity)
        self._cw.html_headers.define_var("INITIAL_STATE", self.initial_state(entity))
        self._cw.html_headers.define_var(
            "CONSULTATION_BASE_URL", self._cw.vreg.config.get("consultation-base-url")
        )
        super(EditionMixin, self).entity_call(entity)

    def initial_state(self, entity):
        rset = self._cw.execute("Any S, T WHERE S is Section, NOT EXISTS(X children S), S title T")
        sections = {
            eid: {"title": title, "eid": eid, "top": True, "issection": True} for eid, title in rset
        }
        state = {
            "model": {"ancestors": [], "related": {}, "top": list(sections.keys()), "entity": {}},
            "app": {"errors": [], "initialFetch": True},
        }
        if entity:
            state["model"]["entity"] = {
                "cw_etype": entity.cw_etype,
                "i18n_cw_etype": self._cw._(entity.cw_etype),
                "eid": entity.eid,
                "uuid": getattr(entity, "uuid", None),
                "rest_path": entity.rest_path(),
                "dc_title": entity.dc_title(),
            }
            adapted = entity.cw_adapt_to("IJsonFormEditable")
            if adapted:
                state["model"]["related"] = adapted.related()
                state["model"]["ancestors"] = adapted.get_ancestors()
        return state

    def add_publish_link(self, entity):
        iwa = entity.cw_adapt_to("IWorkflowable")
        if iwa and any(iwa.possible_transitions()):
            self.w('<link rel="cms-js" url="publish">')

    def add_update_links(self, entity):
        if entity.cw_has_perm("update"):
            self.w('<link rel="cms-js" url="tree">')
            self.w('<link rel="cms-js" url="edit-form">')
            self.w('<link rel="cms-js" url="relation">')

    def add_translate_links(self, entity):
        translatable = entity.cw_adapt_to("ITranslatable")
        if translatable:
            self.w('<link rel="cms-js" url="translate">')

    def add_editor_links(self, entity):
        self.add_publish_link(entity)
        self.w('<link rel="cms-js" url="consultation-link">')
        self.add_update_links(entity)
        self.add_translate_links(entity)
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class BaseContentPrimaryView(EditionMixin, primary.BaseContentPrimaryView):
    __select__ = primary.BaseContentPrimaryView.__select__ & ~anonymous_user()


class CircularPrimaryView(EditionMixin, primary.CircularPrimaryView):
    __select__ = primary.CircularPrimaryView.__select__ & ~anonymous_user()


class OnPageMixin(object):
    def initial_state(self, entity):
        defs = super(OnPageMixin, self).initial_state(entity)
        defs["app"].update({"showOnHomepage": entity.on_homepage})
        return defs

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="mark-home">')
        super(OnPageMixin, self).add_editor_links(entity)


class NewsContentPrimaryView(OnPageMixin, EditionMixin, primary.NewsContentPrimaryView):
    __select__ = primary.NewsContentPrimaryView.__select__ & ~anonymous_user()


class CommemorationItemPrimaryView(OnPageMixin, EditionMixin, primary.CommemorationItemPrimaryView):
    __select__ = primary.CommemorationItemPrimaryView.__select__ & ~anonymous_user()


class FilePathContextView(EntityView):
    __regid__ = "datahash"
    __select__ = EntityView.__select__ & is_instance("File") & ~anonymous_user()

    def cell_call(self, row, col):
        entity = self.cw_rset.get_entity(row, col)
        url = self._cw.build_url("file/{}".format(entity.eid))
        self.w('<a href="%s">%s</a>' % (xml_escape(url), xml_escape(entity.dc_long_title())))


class FilePrimaryView(EditionMixin, PrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("File") & ~anonymous_user()

    def entity_call(self, entity, **kw):
        _ = self._cw._
        super(FilePrimaryView, self).entity_call(entity, **kw)
        self.w("<hr/>")
        url = self._cw.build_url("file/{}".format(entity.eid))
        self.w(T.div(T.a(_("See the file (access by eid)"), href=url)))
        # files with identical content
        identicals_rset = self._cw.execute(
            """Any X1 WHERE X is File,
            X data_hash D, X1 data_hash D,
            NOT X identity X1, X eid %(e)s""",
            {"e": entity.eid},
        )
        # files with the same content (may by different filepathes)
        if identicals_rset:
            self.w(_("See other files referencing documents with identical content"))
            self._cw.view("list", subvid="datahash", rset=identicals_rset, w=self.w)
        # files with the same paths
        sql_query = """
            SELECT f.cw_eid FROM cw_file f
            JOIN cw_file f1 ON f.cw_data=f1.cw_data
            WHERE f1.cw_eid =%(eid)s AND f.cw_eid !=%(eid)s;
            """
        cu = self._cw.cnx.system_sql(sql_query, {"eid": entity.eid})
        cwfiles_eids = [str(f[0]) for f in cu.fetchall()]
        if cwfiles_eids:
            self.w(_("See other files referencing the same document (same filename)"))
            same_as_rset = self._cw.execute(
                """Any X WHERE X eid IN (%(eids)s)""" % {"eids": ", ".join(cwfiles_eids)}
            )
            self._cw.view("list", subvid="datahash", rset=same_as_rset, w=self.w)


class ImagePrimaryView(EditionMixin, PrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("Image") & ~anonymous_user()


class SectionPrimaryView(EditionMixin, primary.SectionPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("Section") & ~anonymous_user()

    def add_publish_link(self, entity):
        if entity.name in FIRST_LEVEL_SECTIONS:
            return
        iwa = entity.cw_adapt_to("IWorkflowable")
        if iwa and any(iwa.possible_transitions()):
            self.w('<link rel="cms-js" url="publish">')

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        self.add_publish_link(entity)
        self.add_update_links(entity)
        self.w('<link rel="cms-js" url="add">')
        self.w('<link rel="cms-js" url="translate"')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class CommemoCollectionPrimaryView(EditionMixin, primary.CommemoCollectionPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("CommemoCollection") & ~anonymous_user()

    def render_left_block_with_date(self, subsection, children):
        with T.section(self.w, Class="commemoration-side-content"):
            with T.div(self.w, Class="commemoration-side-content-header"):
                self.w(T.span(xml_escape(subsection.title or ""), Class="header-title"))
                # --> patch start
                with T.a(self.w, href=subsection.absolute_url()):
                    self.w(T.i(Class="fa fa-external-link-square", aria_hidden="true"))
                # patch end <--
            with T.div(self.w, Class="commemoration-side-content-item"):
                for rset in children:
                    with T.div(self.w, Class="event-item"):
                        with T.div(self.w, Class="event-timeline"):
                            self.w(T.span(str(rset[0][-1]), Class="date"))
                            self.w(T.span(Class="line"))
                        with T.div(self.w, Class="event-title"):
                            with T.ul(self.w):
                                for child in rset.entities():
                                    self.w(T.li(T.a(child.title, href=child.absolute_url())))

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        self.add_publish_link(entity)
        self.add_update_links(entity)
        self.w('<link rel="cms-js" url="add">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class ServicePrimaryView(EditionMixin, ServiceView):
    __select__ = ServiceView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        self.w('<link rel="cms-js" url="edit-service-list">')
        self.w('<link rel="cms-js" url="add-service">')

    def initial_state(self, entity):
        state = {"app": {"errors": [], "initialFetch": True}}
        if entity:
            state["model"] = {}
            state["model"]["entity"] = {
                "cw_etype": entity.cw_etype,
                "eid": entity.eid,
                "uuid": getattr(entity, "uuid", None),
                "rest_path": entity.rest_path(),
            }
        return state


class CardPrimaryView(EditionMixin, primary.PniaCardPrimaryView):
    __select__ = primary.PniaCardPrimaryView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        if entity.cw_has_perm("update"):
            self.w('<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class CircularTablePrimaryView(EditionMixin, circular.CircularTable):
    __select__ = circular.CircularTable.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        if entity.cw_has_perm("update"):
            self.w('<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class ExternRefPrimaryView(EditionMixin, primary.ExternRefPrimaryView):
    __select__ = primary.ExternRefPrimaryView.__select__ & ~anonymous_user()


class VirtualExhibitExternRefPrimaryView(EditionMixin, primary.VirtualExhibitExternRefPrimaryView):
    __select__ = primary.VirtualExhibitExternRefPrimaryView.__select__ & ~anonymous_user()


class MapPrimaryView(EditionMixin, primary.MapPrimaryView):
    __select__ = primary.MapPrimaryView.__select__ & ~anonymous_user()


class CWUserPrimaryView(EditionMixin, PrimaryView):
    __select__ = PrimaryView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="cwusers">')
        if entity.cw_has_perm("add"):
            self.w('<link rel="cms-js" url="add-user">')
        if entity.cw_has_perm("update"):
            self.w('<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class FindingAidPrimaryView(EditionMixin, primary.FindingAidPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("FindingAid") & ~anonymous_user()

    def add_editor_links(self, entity):
        self.add_publish_link(entity)
        self.w('<link rel="cms-js" url="edit-index">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')


class FAComponentPrimaryView(EditionMixin, primary.FindingAidPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("FAComponent") & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="edit-index">')


class AuthorityPrimaryView(EditionMixin, index.AuthorityPrimaryView):
    __select__ = (
        PrimaryView.__select__
        & is_instance("LocationAuthority", "SubjectAuthority", "AgentAuthority")
        & ~anonymous_user()
    )

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="edit-form">')
        self.w('<link rel="cms-js" url="edit-same-as">')
        self.w('<link rel="cms-js" url="group-authorities">')


class AuthorityRecordPrimaryView(EditionMixin, primary.AuthorityRecordPrimaryView):
    __select__ = primary.AuthorityRecordPrimaryView.__select__ & ~anonymous_user()

    def add_update_links(self, entity):
        pass


class TranslationEditionMixin(EditionMixin):
    def add_publish_link(self, entity):
        iwa = entity.cw_adapt_to("IWorkflowable")
        if iwa and any(iwa.possible_transitions()):
            published_state = "wfs_cmsobject_published"
            if iwa.state != published_state and entity.original_entity_state() != published_state:
                return
            self.w('<link rel="cms-js" url="publish">')


class EditablePniaTranslationsPrimaryView(primary.PniaTranslationsPrimaryView):
    __abstract__ = True
    editable = True

    def content_meta_props(self, entity):
        data = super(EditablePniaTranslationsPrimaryView, self).content_meta_props(entity)
        state = entity.cw_adapt_to("IWorkflowable").state
        data.append((self._cw._(state), "flag" if state == "wfs_cmsobject_published" else "flag-o"))
        return data

    def main_props(self, entity):
        _ = self._cw._
        main_props = []
        original = entity.original_entity
        main_props.append(
            (
                _("original ressource"),
                "{} ({})".format(
                    original.view("incontext"), _(original.cw_adapt_to("IWorkflowable").state)
                ),
            )
        )
        for attr in original.i18nfields:
            main_props.append((_(attr), entity.printable_value(attr)))
        return main_props


class SectionTranslationPrimaryView(TranslationEditionMixin, EditablePniaTranslationsPrimaryView):
    __select__ = (
        EditablePniaTranslationsPrimaryView.__select__
        & is_instance("SectionTranslation")
        & ~anonymous_user()
    )


class BaseContentTranslationPrimaryView(
    TranslationEditionMixin, EditablePniaTranslationsPrimaryView
):
    __select__ = (
        EditablePniaTranslationsPrimaryView.__select__
        & is_instance("BaseContentTranslation")
        & ~anonymous_user()
    )


class CommemorationItemTranslationPrimaryView(
    TranslationEditionMixin, EditablePniaTranslationsPrimaryView
):
    __select__ = (
        EditablePniaTranslationsPrimaryView.__select__
        & is_instance("CommemorationItemTranslation")
        & ~anonymous_user()
    )


class FaqItemTranslationPrimaryView(TranslationEditionMixin, EditablePniaTranslationsPrimaryView):
    __select__ = (
        EditablePniaTranslationsPrimaryView.__select__
        & is_instance("FaqItemTranslation")
        & ~anonymous_user()
    )


class AdminGlossaryView(primary.GlossaryView):
    __select__ = primary.GlossaryView.__select__ & ~anonymous_user()
    editable = True

    def call(self, **kw):
        super(AdminGlossaryView, self).call(**kw)
        self.w('<link rel="cms-js" url="add-glossaryterm">')


class EditionWorkflowablePrimaryMix(object):
    def content_meta_props(self, entity):
        data = []
        adapted = entity.cw_adapt_to("IWorkflowable")
        if adapted:
            state = entity.cw_adapt_to("IWorkflowable").state
            flag = "flag" if state == "wfs_cmsobject_published" else "flag-o"
            data = [(entity.fmt_creation_date, "calendar"), (self._cw._(state), flag)]
        data.extend(super(EditionWorkflowablePrimaryMix, self).content_meta_props(entity))
        return data


class GlossaryTermPrimaryView(
    EditionWorkflowablePrimaryMix, EditionMixin, primary.GlossaryTermPrimaryView
):
    __select__ = (
        primary.GlossaryTermPrimaryView.__select__ & is_instance("GlossaryTerm") & ~anonymous_user()
    )

    def main_props(self, entity):
        eschema = self._cw.vreg.schema.eschema(entity.cw_etype)
        return [
            (self._cw._(rschema), getattr(entity, rschema.type))
            for rschema in eschema.subjrels
            if not rschema.meta and rschema.final
        ]

    def add_editor_links(self, entity):
        super(GlossaryTermPrimaryView, self).add_editor_links(entity)
        if entity.cw_has_perm("add"):
            self.w('<link rel="cms-js" url="add-glossaryterm">')


class AdminFaqStartView(EditionMixin, primary.FaqStartView):
    __select__ = primary.FaqStartView.__select__ & ~anonymous_user()
    editable = True

    def call(self, **kw):
        super(AdminFaqStartView, self).call(**kw)
        self.w('<link rel="cms-js" url="add-faq">')


class FaqItemPrimaryView(EditionWorkflowablePrimaryMix, EditionMixin, primary.FaqItemPrimaryView):
    __select__ = primary.FaqItemPrimaryView.__select__ & is_instance("FaqItem") & ~anonymous_user()
    editable = True

    def main_props(self, entity):
        return [
            (self._cw._(attr), entity.printable_value(attr))
            for attr in ("category", "question", "answer", "order")
        ]

    def add_editor_links(self, entity):
        super(FaqItemPrimaryView, self).add_editor_links(entity)
        if entity.cw_has_perm("add"):
            self.w('<link rel="cms-js" url="add-faq">')


class AnonRqTaskPrimaryView(EditionMixin, primary.ContentPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("RqTask") & anonymous_user()

    def entity_call(self, entity, **kw):
        """XXX add security"""
        return


SEVERITY_LVL = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.FATAL,
    "CRITICAL": logging.CRITICAL,
}


def log_to_table(loglines):
    rows = []
    for line in loglines:
        line = line.strip()
        if not line:
            continue
        try:
            severity, date, time, info = line.split(None, 3)
            SEVERITY_LVL[severity]
            try:
                hour, time = time.split(",")
                date = "{} {}".format(date, hour)
            except Exception:
                pass
            rows.append([severity, date, time, html_unescape(info)])
        except (ValueError, KeyError):
            line = html_unescape(line)
            if rows:
                rows[-1][-1] += "\n" + line
            else:
                rows.append([logging.INFO, "", "", line])
    return rows


class OAIRepositoryURLAttributeView(URLAttributeView):
    """ open the url in a new tab"""

    __select__ = URLAttributeView.__select__ & is_instance("OAIRepository")

    def entity_call(self, entity, rtype, **kwargs):
        url = entity.printable_value(rtype)
        if url:
            self.w(exturl_link(self._cw, html_unescape(url)))


class RqTaskPrimaryView(EditionMixin, primary.ContentPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance("RqTask") & ~anonymous_user()
    template = get_template("rqtask.jinja2")
    default_level = "Debug"

    def add_editor_links(self, entity):
        self.w('<link rel="cms-js" url="consultation-link">')
        self.w('<link rel="cms-js" url="fa-import">')
        self.w('<link rel="cms-js" url="fa-tasks">')
        self.w('<link rel="cms-js" url="fa-bord">')
        if entity.cw_has_perm("delete"):
            self.w('<link rel="cms-js" url="delete">')
        draft = self._cw.execute(
            "Any FA LIMIT 1 WHERE R is RqTask, R eid %(e)s, "
            "R fatask_findingaid FA, "
            "FA in_state SS, SS name %(sn)s ",
            {"e": entity.eid, "sn": "wfs_cmsobject_draft"},
        )
        if draft:
            self.w('<link rel="cms-js" url="fa-publish-task">')
        rqjob = entity.cw_adapt_to("IRqJob")
        if rqjob.status is not None and not rqjob.is_finished():
            self._cw.add_onload("window.setTimeout(function() {document.location.reload();}, 5000)")

    def display_logs(self, entity, limit=50000):
        logs = entity.cw_adapt_to("IRqJob").log
        if logs:
            headers = ["severity", "date", "time", "message"]
            logs = [line for line in xml_escape(logs).splitlines() if line]
            logs_size = len(logs)
            if logs_size > limit:
                logs = logs[-limit:]
                self.w(
                    '<div class="alert alert-warning">This task logs have {size} entries. '
                    "Only {limit} lines are displayed</div>".format(size=logs_size, limit=limit)
                )
            return [dict(list(zip(headers, line))) for line in log_to_table(logs)]
        return None

    def display_progress(self, entity):
        progress = entity.cw_adapt_to("IRqJob").progress
        return T.progress(
            "%.0f %%" % (100.0 * progress), min="0", max=str(1.0), value=str(progress)
        )

    def imported_findingaids(self, entity):
        rset = self._cw.execute(
            "Any FA, S, TP, UT, UID, ST WHERE R is RqTask, R eid %(e)s, "
            "R fatask_findingaid FA, FA stable_id S, "
            "FA in_state SS, SS name ST, "
            "FA fa_header FAH, FAH titleproper TP, "
            "FA did D, D unitid UID, D unittitle UT",
            {"e": entity.eid},
        )
        if rset:
            _ = self._cw._
            rows = []
            for eid, stable_id, titleproper, unittitle, unitid, state in rset:
                title = "{}".format(titleproper or unittitle or unitid or "???")
                link = self._cw.build_url("findingaid/" + stable_id)
                rows.append({"eid": eid, "title": (title, link), "state": _(state)})
            return {
                "title": "{} ({})".format(_("fatask_findingaid"), rset.rowcount),
                "data": json_dumps(rows) if rows else None,
            }

    def imported_authrecords(self, entity):
        rset = self._cw.execute(
            "Any AR, RI  WHERE R is RqTask, R eid %(e)s, "
            "R fatask_authorityrecord AR, AR record_id RI",
            {"e": entity.eid},
        )
        if rset:
            _ = self._cw._
            rows = []
            for eid, record_id in rset:
                title = "{}".format(record_id)
                link = self._cw.build_url("authorityrecord/{}".format(record_id))
                rows.append({"eid": eid, "title": (title, link)})
            return {
                "title": "{} ({})".format(_("fatask_authorityrecord"), rset.rowcount),
                "data": json_dumps(rows) if rows else None,
            }

    def imported_persons(self, entity):
        rset = self._cw.execute(
            """Any P, N, FN WHERE R is RqTask, R eid %(e)s,
               R fatask_person P, P name N, P forenames FN""",
            {"e": entity.eid},
        )
        if rset:
            _ = self._cw._
            rows = []
            for eid, name, forenames in rset:
                title = " ".join([e for e in [forenames, name] if e])
                link = self._cw.build_url("person/{}".format(eid))
                if not title:
                    title = link
                rows.append({"eid": eid, "title": (title, link)})
            return {
                "title": "{} ({})".format(_("fatask_person"), rset.rowcount),
                "data": json_dumps(rows) if rows else None,
            }

    def template_attrs(self, entity):
        req = self._cw
        req.add_css("react-bootstrap-table-all.min.css")
        req.add_js("bundle-rq-table.js")
        attrs = super(RqTaskPrimaryView, self).template_attrs(entity)
        _ = req._
        attrs["_"] = _
        attrs["state"] = entity.cw_adapt_to("IRqJob").status
        logs = self.display_logs(entity)
        if logs:
            attrs["logs"] = {"label": _("task_logs"), "data": json_dumps(logs)}
        findingaids = self.imported_findingaids(entity)
        if findingaids:
            attrs["findingaids"] = findingaids
        authrecords = self.imported_authrecords(entity)
        if authrecords:
            attrs["authrecords"] = authrecords
        persons = self.imported_persons(entity)
        if persons:
            attrs["persons"] = persons
        main_props = []
        oai_repository = None
        for label, value in (
            (_("task_name"), _(entity.name)),
            (_("progress bar"), self.display_progress(entity)),
            (_("output_descr"), entity.printable_value("output_descr")),
            (_("output_file"), ", ".join(e.view("incontext") for e in entity.output_file)),
            (_("subtasks"), ", ".join(e.view("incontext") for e in entity.subtasks)),
        ):
            if value:
                main_props.append((label, value))
        if entity.oaiimport_task:
            oai_repository = entity.oaiimport_task[0].oai_repository[0]
            last_import_date = oai_repository.last_successful_import
            if last_import_date:
                last_import_date = last_import_date.strftime("%Y-%m-%d")
            main_props.extend(
                (
                    (_("service"), oai_repository.service[0].view("incontext")),
                    (_("context_service"), _("yes") if oai_repository.context_service else _("no")),
                    (
                        _("should_normalize"),
                        _("yes") if oai_repository.should_normalize else _("no"),
                    ),
                    (_("OAIRepository"), oai_repository.view("urlattr", rtype="url")),
                    (_("last_successful_import"), last_import_date or ""),
                )
            )
        attrs["main_props"] = main_props
        return attrs

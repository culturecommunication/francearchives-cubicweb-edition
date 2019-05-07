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

from six import text_type as unicode

from cwtags import tag as T

from logilab.mtconverter import xml_escape, html_unescape

from cubicweb.predicates import (anonymous_user,
                                 is_instance)
from cubicweb.web.views.primary import PrimaryView
from cubicweb.utils import json_dumps


from cubicweb_francearchives.views import primary, circular, index
from cubicweb_francearchives.views.service import Service as ServiceView

from cubicweb_frarchives_edition.views import get_template


class SitemapView(primary.SitemapView):
    __select__ = primary.SitemapView.__select__ & ~anonymous_user()

    def call(self, **kw):
        self._cw.add_js('bundle-ext.js')
        self._cw.add_js('bundle-sitemap.js')
        super(SitemapView, self).call(**kw)


class EditionMixin(object):

    def entity_call(self, entity):
        self.add_editor_links(entity)
        self._cw.html_headers.define_var(
            'INITIAL_STATE', self.initial_state(entity))
        self._cw.html_headers.define_var(
            'CONSULTATION_BASE_URL', self._cw.vreg.config.get('consultation-base-url'))
        super(EditionMixin, self).entity_call(entity)

    def initial_state(self, entity):
        rset = self._cw.execute(
            'Any S, T WHERE S is Section, NOT EXISTS(X children S), S title T')
        sections = {eid: {'title': title, 'eid': eid,
                          'top': True, 'issection': True}
                    for eid, title in rset}
        state = {
            'model': {
                'ancestors': [],
                'related': {},
                'top': sections.keys(),
                'entity': {}
            },
            'app': {'errors': [], 'initialFetch': True},
        }
        if entity:
            state['model']['entity'] = {
                'cw_etype': entity.cw_etype,
                'i18n_cw_etype': self._cw._(entity.cw_etype),
                'eid': entity.eid,
                'uuid': getattr(entity, 'uuid', None),
                'rest_path': entity.rest_path(),
                'dc_title': entity.dc_title(),
            }
            adapted = entity.cw_adapt_to('IJsonFormEditable')
            if adapted:
                state['model']['related'] = adapted.related()
                state['model']['ancestors'] = adapted.get_ancestors()
        return state

    def add_publish_link(self, entity):
        iwa = entity.cw_adapt_to('IWorkflowable')
        if iwa and any(iwa.possible_transitions()):
            self.w(u'<link rel="cms-js" url="publish">')

    def add_update_links(self, entity):
        if entity.cw_has_perm('update'):
            self.w(u'<link rel="cms-js" url="tree">')
            self.w(u'<link rel="cms-js" url="edit-form">')
            self.w(u'<link rel="cms-js" url="relation">')

    def add_editor_links(self, entity):
        self.add_publish_link(entity)
        self.w(u'<link rel="cms-js" url="consultation-link">')
        self.add_update_links(entity)
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class BaseContentPrimaryView(EditionMixin, primary.BaseContentPrimaryView):
    __select__ = (primary.BaseContentPrimaryView.__select__
                  & ~anonymous_user())


class CircularPrimaryView(EditionMixin, primary.CircularPrimaryView):
    __select__ = (primary.CircularPrimaryView.__select__
                  & ~anonymous_user())


class OnPageMixin(object):

    def initial_state(self, entity):
        defs = super(OnPageMixin, self).initial_state(entity)
        defs['app'].update({'showOnHomepage': entity.on_homepage})
        return defs

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="mark-home">')
        super(OnPageMixin, self).add_editor_links(entity)


class NewsContentPrimaryView(OnPageMixin, EditionMixin,
                             primary.NewsContentPrimaryView):
    __select__ = (primary.NewsContentPrimaryView.__select__
                  & ~anonymous_user())


class CommemorationItemPrimaryView(OnPageMixin, EditionMixin,
                                   primary.CommemorationItemPrimaryView):
    __select__ = (primary.CommemorationItemPrimaryView.__select__
                  & ~anonymous_user())


class FilePrimaryView(EditionMixin, PrimaryView):
    __select__ = (PrimaryView.__select__
                  & is_instance('File')
                  & ~anonymous_user())


class ImagePrimaryView(EditionMixin, PrimaryView):
    __select__ = (PrimaryView.__select__
                  & is_instance('Image')
                  & ~anonymous_user())


class SectionPrimaryView(EditionMixin, primary.SectionPrimaryView):
    __select__ = (PrimaryView.__select__
                  & is_instance('Section')
                  & ~anonymous_user())

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        self.add_publish_link(entity)
        self.add_update_links(entity)
        self.w(u'<link rel="cms-js" url="add">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class CommemoCollectionPrimaryView(EditionMixin, primary.CommemoCollectionPrimaryView):
    __select__ = (PrimaryView.__select__
                  & is_instance('CommemoCollection')
                  & ~anonymous_user())

    def render_left_block_with_date(self, subsection, children):
        with T.section(self.w, Class='commemoration-side-content'):
            with T.div(self.w, Class='commemoration-side-content-header'):
                self.w(T.span(xml_escape(subsection.title or ''),
                              Class='header-title'))
                # --> patch start
                with T.a(self.w, href=subsection.absolute_url()):
                    self.w(T.i(Class="fa fa-external-link-square",
                               aria_hidden="true"))
                # patch end <--
            with T.div(self.w, Class='commemoration-side-content-item'):
                for rset in children:
                    with T.div(self.w, Class='event-item'):
                        with T.div(self.w, Class='event-timeline'):
                            self.w(T.span(unicode(rset[0][-1]), Class='date'))
                            self.w(T.span(Class='line'))
                        with T.div(self.w, Class='event-title'):
                            with T.ul(self.w):
                                for child in rset.entities():
                                    self.w(T.li(T.a(child.title, href=child.absolute_url())))

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        self.add_publish_link(entity)
        self.add_update_links(entity)
        self.w(u'<link rel="cms-js" url="add">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class ServicePrimaryView(EditionMixin, ServiceView):
    __select__ = ServiceView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        self.w(u'<link rel="cms-js" url="edit-service-list">')
        self.w(u'<link rel="cms-js" url="add-service">')

    def initial_state(self, entity):
        state = {'app': {'errors': [], 'initialFetch': True}}
        if entity:
            state['model'] = {}
            state['model']['entity'] = {
                'cw_etype': entity.cw_etype,
                'eid': entity.eid,
                'uuid': getattr(entity, 'uuid', None),
                'rest_path': entity.rest_path(),
            }
        return state


class CardPrimaryView(EditionMixin, primary.PniaCardPrimaryView):
    __select__ = primary.PniaCardPrimaryView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        if entity.cw_has_perm('update'):
            self.w(u'<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class CircularTablePrimaryView(EditionMixin, circular.CircularTable):
    __select__ = circular.CircularTable.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        if entity.cw_has_perm('update'):
            self.w(u'<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class ExternRefPrimaryView(EditionMixin,
                           primary.ExternRefPrimaryView):
    __select__ = (primary.ExternRefPrimaryView.__select__
                  & ~anonymous_user())


class VirtualExhibitExternRefPrimaryView(EditionMixin,
                                         primary.VirtualExhibitExternRefPrimaryView):
    __select__ = (primary.VirtualExhibitExternRefPrimaryView.__select__
                  & ~anonymous_user())


class MapPrimaryView(EditionMixin, primary.MapPrimaryView):
    __select__ = (primary.MapPrimaryView.__select__
                  & ~anonymous_user())


class CWUserPrimaryView(EditionMixin, PrimaryView):
    __select__ = PrimaryView.__select__ & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="cwusers">')
        if entity.cw_has_perm('add'):
            self.w(u'<link rel="cms-js" url="add-user">')
        if entity.cw_has_perm('update'):
            self.w(u'<link rel="cms-js" url="edit-form">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class FindingAidPrimaryView(EditionMixin, primary.FindingAidPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance('FindingAid') & ~anonymous_user()

    def add_editor_links(self, entity):
        self.add_publish_link(entity)
        self.w(u'<link rel="cms-js" url="edit-index">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')


class FAComponentPrimaryView(EditionMixin, primary.FindingAidPrimaryView):
    __select__ = PrimaryView.__select__ & is_instance('FAComponent') & ~anonymous_user()

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="edit-index">')


class AuthorityPrimaryView(EditionMixin, index.AuthorityPrimaryView):
    __select__ = (
        PrimaryView.__select__
        & is_instance('LocationAuthority', 'SubjectAuthority', 'AgentAuthority')
        & ~anonymous_user()
    )

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="edit-form">')
        self.w(u'<link rel="cms-js" url="edit-same-as">')
        self.w(u'<link rel="cms-js" url="group-authorities">')


class AnonRqTaskPrimaryView(EditionMixin, primary.ContentPrimaryView):
    __select__ = (PrimaryView.__select__ & is_instance('RqTask')
                  & anonymous_user())

    def entity_call(self, entity, **kw):
        """XXX add security"""
        return


SEVERITY_LVL = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'FATAL': logging.FATAL,
    'CRITICAL': logging.CRITICAL,
}


def log_to_table(logs):
    rows = []
    for line in logs.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            severity, date, time, info = line.split(None, 3)
            SEVERITY_LVL[severity]
            try:
                hour, time = time.split(',')
                date = '{} {}'.format(date, hour)
            except Exception:
                pass
            rows.append([severity, date, time, html_unescape(info)])
        except (ValueError, KeyError):
            line = html_unescape(line)
            if rows:
                rows[-1][-1] += '\n' + line
            else:
                rows.append([logging.INFO, '', '', line])
    return rows


class RqTaskPrimaryView(EditionMixin, primary.ContentPrimaryView):
    __select__ = (PrimaryView.__select__ & is_instance('RqTask')
                  & ~anonymous_user())
    template = get_template('rqtask.jinja2')
    default_level = 'Debug'

    def add_editor_links(self, entity):
        self.w(u'<link rel="cms-js" url="consultation-link">')
        self.w(u'<link rel="cms-js" url="fa-import">')
        self.w(u'<link rel="cms-js" url="fa-tasks">')
        if entity.cw_has_perm('delete'):
            self.w(u'<link rel="cms-js" url="delete">')
        draft = self._cw.execute(
            'Any FA LIMIT 1 WHERE R is RqTask, R eid %(e)s, '
            'R fatask_findingaid FA, '
            'FA in_state SS, SS name %(sn)s ',
            {'e': entity.eid, 'sn': 'wfs_cmsobject_draft'})
        if draft:
            self.w(u'<link rel="cms-js" url="fa-publish-task">')
        rqjob = entity.cw_adapt_to('IRqJob')
        if rqjob.status is not None and not rqjob.is_finished():
            self._cw.add_onload(
                'window.setTimeout(function() {document.location.reload();}, 5000)'
            )

    def display_logs(self, entity):
        logs = entity.cw_adapt_to('IRqJob').log
        if logs:
            logs = xml_escape(logs)
            headers = ['severity', 'date', 'time', 'message']
            return [dict(zip(headers, l)) for l in log_to_table(logs)]
        return None

    def display_progress(self, entity):
        progress = entity.cw_adapt_to('IRqJob').progress
        return T.progress(u'%.0f %%' % (100. * progress),
                          min=u'0', max=unicode(1.0), value=unicode(progress))

    def template_attrs(self, entity):
        req = self._cw
        req.add_css('react-bootstrap-table-all.min.css')
        req.add_js('bundle-rq-table.js')
        attrs = super(RqTaskPrimaryView, self).template_attrs(entity)
        _ = req._
        attrs['_'] = _
        attrs['state'] = entity.cw_adapt_to('IRqJob').status
        logs = self.display_logs(entity)
        if logs:
            attrs['logs'] = {'label': _('task_logs'),
                             'data': json_dumps(logs)}
        rset = self._cw.execute(
            'Any FA, S, TP, UT, UID, ST WHERE R is RqTask, R eid %(e)s, '
            'R fatask_findingaid FA, FA stable_id S, '
            'FA in_state SS, SS name ST, '
            'FA fa_header FAH, FAH titleproper TP, '
            'FA did D, D unitid UID, D unittitle UT',
            {'e': entity.eid})
        if rset:
            rows = []
            for eid, stable_id, titleproper, unittitle, unitid, state in rset:
                title = u'{}'.format(titleproper or unittitle or unitid or '???')
                link = self._cw.build_url('findingaid/' + stable_id)
                rows.append({'eid': eid, 'title': (title, link), 'state': _(state)})
            attrs['findingaids'] = {'title': u'{} ({})'.format(
                _('fatask_findingaid'), rset.rowcount),
                'data': json_dumps(rows) if rows else None}
        main_props = []
        for label, value in (
            (_('task_name'), _(entity.name)),
            (_('progress bar'), self.display_progress(entity)),
            (_('output_descr'), entity.printable_value('output_descr')),
            (_('output_file'),
             u', '.join(e.view('incontext') for e in entity.output_file)),
            (_('subtasks'),
             u', '.join(e.view('incontext') for e in entity.subtasks)),
        ):
            if value:
                main_props.append((label, value))
        attrs['main_props'] = main_props

        return attrs

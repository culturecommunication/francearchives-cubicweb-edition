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
from cubicweb.web.views.basetemplates import LogInTemplate

from cubicweb_francearchives.views import JinjaViewMixin
from cubicweb_francearchives.utils import find_card
from cubicweb_francearchives.views.templates import PniaMainTemplate
from cubicweb_frarchives_edition.views import get_template


class EditionMainTemplate(PniaMainTemplate):
    template = get_template('editiontmpl.jinja2')

    def alert(self):
        alert = find_card(self._cw, 'alert')
        if alert:
            wf = alert.cw_adapt_to('IWorkflowable')
            if wf and wf.state == 'wfs_cmsobject_published':
                return alert.content

    def template_context(self, view):
        req = self._cw
        ctx = super(EditionMainTemplate, self).template_context(view)
        ctx['user'] = req.user.login
        isanon = req.session.anonymous_session
        ctx['isanon'] = isanon
        ctx['current_url'] = req.relative_path()
        if not isanon:
            # req._request is pyramid request
            req.html_headers.define_var('SCRIPT_NAME', req._request.script_name)
            req.add_js('//cdn.tinymce.com/4/tinymce.min.js', False)
            for js in ('bundle-vendor.js', 'bundle-cms.js', 'cropper/cropper.js'):
                req.add_js(js)
            for css in ('react-widgets/react-widgets.css',
                        'react-select.min.css',
                        'cropper/cropper.css'):
                req.add_css(css)
        return ctx


class PniaLogin(JinjaViewMixin, LogInTemplate):
    template = get_template('login.jinja2')

    def call(self):
        self.set_request_content_type()
        context = {
            'postlogin_path': self._cw.form.get('postlogin_path', ''),
            'title': self._cw.property_value('ui.site-title'),
            'post_url': self._cw.base_url() + 'login',
            'cssfiles': [
                self._cw.data_url('css/bootstrap.min.css')
            ],
            'message': self._cw.message
        }
        self.call_template(**context)


def registration_callback(vreg):
    vreg.register_and_replace(EditionMainTemplate, PniaMainTemplate)
    vreg.register_and_replace(PniaLogin, LogInTemplate)

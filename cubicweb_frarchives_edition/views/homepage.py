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

from cubicweb.predicates import authenticated_user

from cubicweb_francearchives.views.homepage import PniaIndexView


class EditionIndexView(PniaIndexView):
    __select__ = PniaIndexView.__select__ & authenticated_user()

    def initial_state(self):
        rset = self._cw.find('Section', name=u'non-repris')
        return {
            'model': {
                'entity': {
                    'cw_etype': 'Section',
                    'eid': rset[0][0] if rset else None,
                }
            }
        }

    def call(self):
        self.w(u'<link rel="cms-js" url="fa-import">')
        self.w(u'<link rel="cms-js" url="fa-tasks">')
        self.w(u'<link rel="cms-js" url="todos">')
        self.w(u'<link rel="cms-js" url="add">')
        self.w(u'<link rel="cms-js" url="add-service">')
        self.w(u'<link rel="cms-js" url="cwusers">')
        eschema = self._cw.vreg.schema['CWUser']
        if eschema.has_perm(self._cw, 'add'):
            self.w(u'<link rel="cms-js" url="add-user">')
        rset = self._cw.execute('Any X WHERE X is Metadata, X uuid "metadata-homepage"')
        if rset:
            self.w(u'<link rel="cms-js" url="homepage-metadata">')
            meta = rset.one()
            self._cw.html_headers.define_var('HOME_METADATA', {
                'eid': meta.eid,
                'cw_etype': 'Metadata'
            })
        rset = self._cw.execute('Any X WHERE X is Card, X wikiid "alert"')
        if rset:
            self.w(u'<link rel="cms-js" url="alert">')
            alert = rset.one()
            self._cw.html_headers.define_var('ALERT_CARD', {
                'eid': alert.eid,
                'cw_etype': 'Card'
            })
        self._cw.html_headers.define_var(
            'INITIAL_STATE', self.initial_state())
        super(EditionIndexView, self).call()

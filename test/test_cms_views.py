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
"""cubicweb-frarchives_edition unit tests for "cms" views."""
import fakeredis

from cubicweb.pyramid.test import PyramidCWTest

import utils


class CMSEntitiesTest(utils.FrACubicConfigMixIn, PyramidCWTest):

    settings = {
        'cubicweb.bwcompat': False,
        'cubicweb.auth.authtkt.session.secret': 'top secret',
        'pyramid.debug_notfound': True,
        'cubicweb.session.secret': 'stuff',
        'cubicweb.auth.authtkt.persistent.secret': 'stuff',
        'francearchives.autoinclude': 'no',
    }

    def setUp(self):
        super(CMSEntitiesTest, self).setUp()

    def includeme(self, config):
        config.registry.settings['frarchives_edition.rq.redis'] = fakeredis.FakeStrictRedis()
        config.include('cubicweb_frarchives_edition.cms')
        config.include('cubicweb_francearchives.pviews')

    def test_add_related_metadata(self):
        """POST on /CommemorationItem/<eid>/relationships/metadata with primary entity as
        subject of <rtype>.
        """
        with self.admin_access.repo_cnx() as cnx:
            ci_entry = utils.create_default_commemoitem(cnx)
            cnx.commit()
        url = '/CommemorationItem/%d/relationships/metadata' % ci_entry.eid
        data = {
            'title': u'metadata',
        }
        self.login()
        res = self.webapp.post_json(url, data, status=201,
                                    headers={'accept': 'application/json'})
        doc = res.json
        self.assertEqual(doc['title'], u'metadata')
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute(
                'Any A WHERE B metadata A, B eid %(b)s, A title %(a)s',
                {'b': ci_entry.eid, 'a': doc['title']})
        self.assertTrue(rset)

    def test_get_relationship_metadata_schema(self):
        """GET on /CommemorationItem/relationships/metadata/schema to retrieve
        target schema.
        """
        url = '/CommemorationItem/relationships/metadata/schema?role=creation'
        res = self.webapp.get(url, status=200,
                              headers={'accept': 'application/schema+json'})
        metadata_def = res.json
        expected_properties = ['creator', 'description', 'keywords',
                               'subject', 'title', 'type']
        self.assertCountEqual(metadata_def['properties'],
                              expected_properties)

    def _test_add_index(self, etype, data):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            ci_eid = utils.create_default_commemoitem(cnx).eid
            index_eid = ce(etype, **data).eid
            cnx.commit()
        url = '/CommemorationItem/{}/relationships/related_authority/targets?role=creation'.format(
            ci_eid)
        self.login()
        self.webapp.post_json(url, [{'value': index_eid}], status=201,
                              headers={'accept': 'application/json'})
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute(
                'Any I WHERE COMMEMO related_authority I, I eid %(i)s, COMMEMO eid %(commemo)s',
                {'i': index_eid, 'commemo': ci_eid})
            self.assertTrue(rset)

    def test_add_agent(self):
        data = {
            'label': u'Jean Valjean',
        }
        self._test_add_index('AgentAuthority', data)

    def test_add_location(self):
        data = {
            'label': u'Paris ',
            'longitude': 2.35,
        }
        self._test_add_index('LocationAuthority', data)

    def test_add_subject(self):
        data = {
            'label': u'Subject'
        }
        self._test_add_index('SubjectAuthority', data)

    def test_available_pnia_agent(self):
        with self.admin_access.repo_cnx() as cnx:
            pa1 = cnx.create_entity('AgentAuthority', label=u'person one')
            pa2 = cnx.create_entity('AgentAuthority', label=u'person two')
            cnx.create_entity('AgentAuthority', label=u'agent two')
            ci_eid = utils.create_default_commemoitem(cnx).eid
            cnx.commit()
            url = '/CommemorationItem/{}/relationships/related_authority/available-targets'.format(ci_eid)  # noqa
            self.login()
            data = {'q': 'pers', 't': 'agent'}
            res = self.webapp.get(url, data, headers={'Accept': 'application/json'})
            doc = res.json
            eids = [d['eid'] for d in doc['data']]
            self.assertEqual(set((pa1.eid, pa2.eid)), set(eids))

    def test_available_pnia_location(self):
        with self.admin_access.repo_cnx() as cnx:
            pa1 = cnx.create_entity('LocationAuthority', label=u'Saint Denis')
            pa2 = cnx.create_entity('LocationAuthority', label=u'Saint Etienne')
            cnx.create_entity('LocationAuthority', label=u'Paris')
            ci_eid = utils.create_default_commemoitem(cnx).eid
            cnx.commit()
            url = ('/CommemorationItem/{}/relationships/'
                   'related_authority/available-targets?q=sain'.format(ci_eid))
            self.login()
            res = self.webapp.get(url, headers={'Accept': 'application/json'})
            doc = res.json
            eids = [d['eid'] for d in doc['data']]
            self.assertEqual(set((pa1.eid, pa2.eid)), set(eids))

    def test_add_pnia_subject_index(self):
        with self.admin_access.repo_cnx() as cnx:
            s1 = cnx.create_entity('SubjectAuthority', label=u'guerre')
            s2 = cnx.create_entity('SubjectAuthority', label=u'paix')
            ci_eid = utils.create_default_commemoitem(cnx).eid
            cnx.commit()
            self.login()
            url = '/CommemorationItem/{}/relationships/related_authority/targets'.format(ci_eid)
            data = [{"label": s.label, "value": s.eid} for s in [s1, s2]]
            self.webapp.post_json(url, data, status=201,
                                  headers={'accept': 'application/json'})
            query = ('Any A WHERE X related_authority A, X eid %(x)s, A eid %(a)s')
            for eid in [s1.eid, s2.eid]:
                rset = cnx.execute(query, {'a': eid, 'x': ci_eid})
                self.assertTrue(rset)

    def test_move_section(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            s1 = ce('Section', title=u'child')
            ce('Section', title=u'parent', children=s1)
            s3 = ce('Section', title=u'new parent')
            cnx.commit()
        self.login()
        self.webapp.post_json('/section', {'target': s3.eid,
                                           'child': s1.eid,
                                           'newOrder': 0},
                              status=200,
                              headers={'accept': 'application/json'})
        with self.admin_access.cnx() as cnx:
            s3 = cnx.find('Section', eid=s3.eid).one()
            self.assertEqual(len(s3.children), 1)
            self.assertEqual(s1.eid, s3.children[0].eid)

    def test_move_section_on_itself(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            s1 = ce('CommemoCollection', title=u'child', year=2000)
            ce('Section', title=u'parent', children=s1)
            cnx.commit()
        self.login()
        resp = self.webapp.post_json('/section', {'target': s1.eid,
                                                  'child': s1.eid,
                                                  'newOrder': 0},
                                     status=400,
                                     headers={'accept': 'application/json'})
        self.assertIn('errors', resp.json_body)


if __name__ == '__main__':
    import unittest
    unittest.main()

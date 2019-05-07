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
"""cubicweb-frarchives_edition unit tests for security"""

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb import Unauthorized, ValidationError
import utils


class SecurityTC(utils.FrACubicConfigMixIn, CubicWebTC):

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            self.create_user(cnx, 'user',
                             email=u'user@frar.fr',
                             password=u'oi8e+ZEL*!sdIE',
                             groups=('users',), commit=True)
            self.create_user(cnx, 'other_user',
                             password=u'di7ZEL*!sdIE',
                             email=u'other_user@frar.fr',
                             groups=('users',), commit=True)
            cnx.commit()

    def test_add_section(self):
        """Only managers can add sections"""
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity('Section', title=u's1')
            cnx.commit()
        with self.new_access('user').client_cnx() as cnx:
            with self.assertRaises(Unauthorized):
                cnx.create_entity('Section', title=u's2')
                cnx.commit()

    def test_update_add_section(self):
        """Only managers can update sections"""
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.create_entity('Section',
                                        title=u's1')
            cnx.commit()
            section.cw_set(title=u's2')
            cnx.commit()
        with self.new_access('user').client_cnx() as cnx:
            section = cnx.find('Section', eid=section.eid).one()
            with self.assertRaises(Unauthorized):
                section.cw_set(title=u's3')
                cnx.commit()

    def test_delete_section(self):
        """Only managers can delete sections"""
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.create_entity('Section',
                                        title=u's1')
            cnx.commit()
            section.cw_set(title=u's2')
            cnx.commit()
        with self.new_access('user').client_cnx() as cnx:
            section = cnx.find('Section', eid=section.eid).one()
            with self.assertRaises(Unauthorized):
                section.cw_delete()
                cnx.commit()
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.find('Section', eid=section.eid).one()
            section.cw_delete()
            cnx.commit()

    def test_add_circular(self):
        for login in ('user', 'admin'):
            with self.new_access(login).client_cnx() as cnx:
                cnx.create_entity(
                    'Circular',
                    circ_id=u'circ_{}'.format(login),
                    status=u'revoked',
                    title=u'circ {}'.format(login))
                cnx.commit()

    def test_modify_circular(self):
        """admin and users can modify all circulars"""
        with self.new_access('user').client_cnx() as cnx:
            circ_eid = cnx.create_entity(
                'Circular',
                circ_id=u'circ1', status=u'revoked',
                title=u'circ').eid
            cnx.commit()
        for login in ('admin', 'other_user'):
            with self.new_access(login).client_cnx() as cnx:
                circ = cnx.find('Circular', eid=circ_eid).one()
                new_title = u'title {}'.format(login)
                circ.cw_set(title=new_title)
                cnx.commit()
                self.assertEqual(circ.title, new_title)

    def test_delete_circular(self):
        """admin and users can delete all circulars"""
        logins = (u'admin', u'other_user')
        with self.new_access('user').client_cnx() as cnx:
            for login in logins:
                cnx.create_entity(
                    'Circular',
                    circ_id=u'circ_{}'.format(login),
                    status=u'revoked',
                    title=u'circ {}'.format(login))
                cnx.commit()
        for login in logins:
            with self.new_access(login).client_cnx() as cnx:
                circ = cnx.find('Circular',
                                circ_id=u'circ_{}'.format(login)).one()
                circ.cw_delete()
                cnx.commit()


class WorkflowTC(utils.FrACubicConfigMixIn, CubicWebTC):

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            self.create_user(cnx, 'user',
                             email=u'user@frar.fr',
                             password=u'oi8e+ZEL*!sdIE',
                             groups=('users',), commit=True)
            self.create_user(cnx, 'other_user',
                             password=u'di7ZEL*!sdIE',
                             email=u'other_user@frar.fr',
                             groups=('users',), commit=True)
            section = cnx.create_entity('Section', title=u'draft')
            cnx.create_entity(
                'Circular', title=u'circ1',
                circ_id=u'circ1', status=u'revoked',
                reverse_children=section)
            published_section = cnx.create_entity('Section', title=u'published')
            cnx.commit()
            published_section.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()

    def test_users_can_not_publish_section(self):
        """users can't publish section"""
        with self.new_access('user').client_cnx() as cnx:
            section = cnx.find('Section', title=u'draft').one()
            self.assertEqual("wfs_cmsobject_draft",
                             section.cw_adapt_to('IWorkflowable').state)
            with self.assertRaises(ValidationError):
                section.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
                cnx.commit()

    def test_adim_can_unpublish_section(self):
        """admin can unpublish section"""
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.find('Section', title=u'published').one()
            section.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_unpublish')
            cnx.commit()
            self.assertEqual("wfs_cmsobject_draft",
                             section.cw_adapt_to('IWorkflowable').state)

    def test_users_can_not_unpublish_section(self):
        """users can't unpublish section"""
        with self.new_access('user').client_cnx() as cnx:
            section = cnx.find('Section', title=u'published').one()
            self.assertEqual("wfs_cmsobject_published",
                             section.cw_adapt_to('IWorkflowable').state)
            with self.assertRaises(ValidationError):
                section.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_unpublish')
                cnx.commit()


if __name__ == '__main__':
    import unittest
    unittest.main()

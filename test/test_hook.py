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
"""cubicweb-frarchives_edition unit tests for hooks"""
from datetime import datetime
import os.path as osp
from six import text_type as unicode

from cubicweb import Binary, Unauthorized, ValidationError
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_frarchives_edition import get_samesas_history

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa


class ReferencedFileTC(FrACubicConfigMixIn, CubicWebTC):

    def setUp(self):
        super(ReferencedFileTC, self).setUp()
        self.config.global_set_option('compute-sha1hex', 'yes')

    def test_basecontent(self):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                'File',
                data=Binary('some-file-data'),
                data_name=u'file.pdf',
                data_format=u'application/pdf')
            bc = cnx.create_entity(
                'BaseContent',
                title=u'bc',
                content=u'''\
<p>
<h1>bc</h1>
<a href="%s">file.pdf</a>
</p>''' % fobj.cw_adapt_to('IDownloadable').download_url())
            cnx.commit()
            self.assertEqual(bc.referenced_files[0].eid, fobj.eid)


class DeleteHookTests(FrACubicConfigMixIn, CubicWebTC):

    def test_undeletable_card(self):
        with self.admin_access.repo_cnx() as cnx:
            card = cnx.find('Card', wikiid='alert').one()
            with self.assertRaises(Unauthorized):
                card.cw_delete()

    def test_delete_article(self):
        with self.admin_access.repo_cnx() as cnx:
            article = cnx.create_entity(
                'BaseContent', title=u'article')
            cnx.commit()
            article.cw_delete()
            cnx.commit()


class MapHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Map hooks."""

    def test_invalid_map_file(self):
        with self.admin_access.cnx() as cnx:
            with self.assertRaises(ValidationError) as cm:
                cnx.create_entity('Map', title=u'map',
                                  map_file=Binary(b'"a","b"\n"1","2"\n'))
                error = (
                    u'CSV file invalid. It must contain "Code_insee", "URL", '
                    u'"Couleur" and "Legende" headers'
                    u'columns separated by ","')
                self.assertEqual(cm.exception.errors,
                                 {'map_file': error})

    def test_valid_map_file(self):
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir,
                               'Carte_Cadastres.csv'), 'rb') as stream:
                cnx.create_entity(
                    'Map', title=u'map',
                    map_file=Binary(stream.read()))
            cnx.commit()


class ServiceHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Service hooks."""

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            service = cnx.create_entity(
                'Service', category=u'foo',
                level=u'level-D',
                dpt_code=u'75',
                name=u'Service de Paris')
            cnx.commit()
            self.service_eid = service.eid
        super(ServiceHookTC, self).setup_database()

    def test_service_d_unique_level_code(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity('Service',
                              category=u'foo1',
                              level=u'level-D',
                              dpt_code=u'75',
                              name=u'Service de Paris')
            with self.assertRaises(ValidationError) as cm:
                cnx.commit()
                error = (
                    u'A department archive with "75" code already exists')
                self.assertEqual(cm.exception.errors,
                                 {'name': error})

    def test_service_unique_level_code_annex(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity('Service',
                              category=u'foo1',
                              level=u'level-D',
                              dpt_code=u'93',
                              name=u'93 service',
                              annex_of=self.service_eid)
            cnx.commit()
            cnx.create_entity('Service',
                              category=u'foo',
                              level=u'level-D',
                              dpt_code=u'93',
                              name=u'93 service 2',
                              annex_of=self.service_eid)
            # make sure no ValidationError is raised: we should be able
            # to have 2 annexes with level-D in the same department.
            cnx.commit()

    def test_service_d_annex_of_unique(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity('Service',
                              category=u'foo1',
                              level=u'level-D',
                              dpt_code=u'75',
                              annex_of=self.service_eid)
            cnx.commit()

    def test_service_unique_level(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity('Service',
                              category=u'foo1',
                              level=u'level-R',
                              dpt_code=u'75',
                              name=u'Service de Paris')
            cnx.commit()

    def test_service_unique_update_level(self):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                'Service', category=u'foo1',
                level=u'level-R', dpt_code=u'75',
                name=u'Service de Paris')
            cnx.commit()
            service.cw_set(level=u'level-D')
            with self.assertRaises(ValidationError):
                cnx.commit()


class CWUserHooksTC(FrACubicConfigMixIn, CubicWebTC):

    def test_cwuser_password_policy(self):
        with self.admin_access.cnx() as cnx:
            for wrong_psw in (
                    u'p', u'toto',  u'password', u'toto1TITI@',
                    u'o2ieuUYétrz4ud',
                    u'o2uaa$rzudpo*d2',
                    u'O2UAA$REZ3ED*D',
                    u'Iuz1YEr7azrIE',
                    u'123456-456745'):
                with self.assertRaises(ValidationError):
                    self.create_user(cnx, u'toto',
                                     password=wrong_psw,
                                     groups=('users',), commit=True)
                    cnx.commit()
                cnx.rollback()
        with self.admin_access.cnx() as cnx:
            self.create_user(cnx, u'toto',
                             password=u'one35OPt^çpp3',
                             groups=('users',), commit=True)
            cnx.commit()
            # XXX this psw fails in test_pswd.py
            self.create_user(cnx, u'titi',
                             password=u'Iùz1YEr7az$rIE',
                             groups=('users',), commit=True)
            cnx.commit()


class FileHookTests(FrACubicConfigMixIn, CubicWebTC):

    def test_update_image_file(self):
        """simulate InlinedRelationMapper behavior: drop and recreate inlined
           Image.image_file File object"""
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                'File', data=Binary('data'), data_name=u'data')
            image = cnx.create_entity(
                'Image', caption=u'image-caption',
                image_file=fobj)
            cnx.commit()
            fpath = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                {'e': fobj.eid})[0][0].getvalue()
            self.assertTrue(osp.exists(fpath))
            fobj1 = cnx.create_entity(
                'File', data=Binary('data'), data_name=u'data',
                reverse_image_file=image)
            cnx.execute('DELETE File X WHERE X eid %(e)s', {'e': fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                 {'e': fobj1.eid})[0][0].getvalue()
            self.assertEqual(fpath, fpath1)
            self.assertTrue(osp.exists(fpath))

    def test_delete_image_file(self):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                'File', data=Binary('data'), data_name=u'data')
            image = cnx.create_entity(
                'Image', caption=u'image-caption',
                image_file=fobj)
            cnx.commit()
            fpath = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                {'e': fobj.eid})[0][0].getvalue()
            self.assertTrue(osp.exists(fpath))
            fobj1 = cnx.create_entity(
                'File', data=Binary('data1'), data_name=u'data1')
            image.cw_set(image_file=fobj1)
            cnx.execute('DELETE File X WHERE X eid %(e)s', {'e': fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                 {'e': fobj1.eid})[0][0].getvalue()
            self.assertTrue(osp.exists(fpath1))
            self.assertFalse(osp.exists(fpath))

    def test_delete_same_images_file(self):
        """This test tests a deviant behaviour:
               if one and same file is used for two
               diffrent CWFiles - they share exactly the same file in the fs.
           """
        with self.admin_access.cnx() as cnx:
            data = 'data'
            fobj = cnx.create_entity(
                'File', data=Binary(data), data_name=unicode(data))
            fobj1 = cnx.create_entity(
                'File', data=Binary(data), data_name=unicode(data))
            cnx.commit()
            fpath = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                {'e': fobj.eid})[0][0].getvalue()
            fpath1 = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                 {'e': fobj1.eid})[0][0].getvalue()
            self.assertEqual(fpath, fpath1)
            self.assertTrue(osp.exists(fpath))
            cnx.execute('DELETE File X WHERE X eid %(e)s', {'e': fobj.eid})
            cnx.commit()
            self.assertFalse(cnx.find('File', eid=fobj.eid))
            cnx.find('File', eid=fobj1.eid).one()
            fpath1 = cnx.execute('Any fspath(D) WHERE X data D, X eid %(e)s',
                                 {'e': fobj1.eid})[0][0].getvalue()
            self.assertFalse(osp.exists(fpath1))


class CircularHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Circular hooks."""

    @classmethod
    def init_config(cls, config):
        super(CircularHookTC, cls).init_config(config)
        config.set_option('consultation-base-url',
                          'https://francearchives.fr')

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            ce = cnx.create_entity
            self.fobj1 = ce('File',
                            data=Binary('some-file-data'),
                            data_name=u'file1.pdf',
                            data_format=u'application/pdf')
            self.circular1 = ce(
                'Circular',
                circ_id=u'c1_AB_EC',
                status=u'in-effect',
                title=u'c1',
                attachment=self.fobj1)
            self.circular2 = ce(
                'Circular',
                circ_id=u'c2_AT_ER',
                status=u'in-effect',
                title=u'c1')
            cnx.commit()
        super(CircularHookTC, self).setup_database()

    def test_add_official_text(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find(
                'Circular', eid=self.circular2.eid).one()
            ot = cnx.create_entity(
                'OfficialText',
                code=circular.circ_id,
                reverse_modified_text=self.circular1.eid)
            cnx.commit()
            self.assertEqual(ot.circular[0].circ_id,
                             circular.circ_id)
            circ = cnx.find('Circular', circ_id='c1_AB_EC').one()
            ot.cw_set(code=circ.circ_id)
            cnx.commit()
            ot = cnx.find('OfficialText', eid=ot.eid).one()
            self.assertEqual(ot.circular[0].circ_id,
                             circ.circ_id)

    def test_json_attributes_values(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find(
                'Circular', eid=self.circular2.eid).one()
            expected = [
                [u'circular_title_label', u'c1'],
                [u'circular_url', u'https://francearchives.fr/circulaire/c2_AT_ER'],
                [u'circular_circ_id_label', u'c2_AT_ER'],
                [u'circular_kind_label', None],
                [u'circular_code_label', None],
                [u'circular_nor_label', None],
                [u'circular_status_label', u'in-effect'],
                [u'circular_link_label', None],
                [u'circular_additional_link_label', u''],
                [u'circular_attachment_label', u''],
                [u'circular_additional_attachment_label', u''],
                [u'circular_signing_date_label', u''],
                [u'circular_siaf_daf_kind_label', None],
                [u'circular_siaf_daf_code_label', None],
                [u'circular_siaf_daf_signing_date_label', u''],
                [u'circular_producer_label', None],
                [u'circular_producer_acronym_label', None],
                [u'circular_modification_date_label', u''],
                [u'circular_abrogation_date_label', u''],
                [u'circular_abrogation_text_label', None],
                [u'circular_archival_field_label', None],
                [u'circular_historical_context_label', u''],
                [u'circular_business_field_label', u''],
                [u'circular_document_type_label', u''],
                [u'circular_action_label', u''],
                [u'circular_modified_text_label', u''],
                [u'circular_modifying_text_label', u''],
                [u'circular_revoked_text_label', u'']
            ]
            self.assertEqual(expected, circular.values_from_json)
            daf_date = datetime(2014, 3, 2)
            circular.cw_set(nor=u'AAA', siaf_daf_signing_date=daf_date)
            cnx.commit()
            for expected in ([u'circular_nor_label', u'AAA'],
                             [u'circular_siaf_daf_signing_date_label', u'02/03/2014']):
                self.assertIn([u'circular_nor_label', u'AAA'], circular.values_from_json)

    def test_json_relations_values(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find(
                'Circular', eid=self.circular1.eid).one()
            scheme = cnx.create_entity('ConceptScheme', title=u'some classification')
            concept = cnx.create_entity(
                'Concept', in_scheme=scheme,
                cwuri=u'uri1',
                reverse_label_of=cnx.create_entity(
                    'Label', label=u'administration', language_code=u'fr',
                    kind=u'preferred'),
                reverse_business_field=circular.eid)
            circular.cw_set(additional_attachment=self.fobj1)
            concept.cw_clear_all_caches()
            cnx.commit()
            circular = cnx.find(
                'Circular', eid=circular.eid).one()
            path = 'file/87b2b0e8c632bfbace95bf693f8e99f4acc92eca/file1.pdf'
            attachment = 'https://francearchives.fr/{}'.format(path)
            for expected in (
                    [u'circular_attachment_label', attachment],
                    [u'circular_additional_attachment_label', attachment],
                    [u'circular_business_field_label', u'administration']):
                self.assertIn(expected, circular.values_from_json)


class ExternalUriHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Circular hooks."""
    configcls = PostgresApptestConfiguration

    @classmethod
    def init_config(cls, config):
        super(ExternalUriHookTC, cls).init_config(config)
        config.set_option('consultation-base-url',
                          'https://francearchives.fr')

    def setUp(self):
        super(ExternalUriHookTC, self).setUp()
        with self.admin_access.cnx() as cnx:
            values = [[6455259, 48.86, 2.34444],
                      [3020686, 51.03297, 2.377],
                      [2988507, 48.85341, 2.3488]]
            cnx.cnxset.cu.executemany('''
            INSERT INTO geonames (geonameid, latitude, longitude)
            VALUES (%s, %s, %s)
            ''', values)
            cnx.commit()

    def test_sameas_history(self):
        """
        Test samesas_history table is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            uri = u'http://www.geonames.org/2988507'
            paris = cnx.create_entity(
                'ExternalUri',
                source=u'source',
                label=u'Paris (France)',
                uri=uri)
            loc = cnx.create_entity(
                'LocationAuthority',
                label=u'Dunkerque (Nord, France)',
                same_as=paris
            )
            cnx.commit()
            self.assertEqual([(uri, loc.eid, True)],
                             get_samesas_history(cnx, complete=True))
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual([(uri, loc.eid, False)],
                             get_samesas_history(cnx, complete=True))
            new_uri = u'http://www.geonames.org/3020686/'
            dunkerque = cnx.create_entity(
                'ExternalUri',
                source=u'source',
                label=u'Dunkerque (Nord, France)',
                uri=new_uri)
            dunkerque.cw_set(same_as=loc)
            cnx.commit()
            self.assertEqual([(uri, loc.eid, False),
                             (new_uri, loc.eid, True)],
                             get_samesas_history(cnx, complete=True))

    def test_sameas_geoname_location(self):
        """
        Test Authority location is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            moscou = cnx.create_entity(
                'ExternalUri',
                source=u'source',
                label=u'Moscou (Russie)',
                uri=u'http://www.geonames.org/6455259')
            loc = cnx.create_entity(
                'LocationAuthority',
                label=u'Moscou (Russie)',
            )
            cnx.commit()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            loc.cw_set(same_as=moscou)
            cnx.commit()
            expected = (48.86, 2.34444)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            dunkerque = cnx.create_entity(
                'ExternalUri',
                source=u'source',
                label=u'Dunkerque (Nord, France)',
                uri=u'http://www.geonames.org/3020686/')
            dunkerque.cw_set(same_as=loc)
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            expected = (51.03297, 2.377)
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            # add a second uri
            loc.cw_set(same_as=moscou)
            cnx.commit()
            self.assertEqual(2, len(loc.same_as))
            expected = (48.86, 2.34444)
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            cnx.execute(
                'DELETE A same_as E WHERE A eid %(a)s, E eid %(e)s',
                {'a': loc.eid, 'e': moscou.eid}
            )
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertEqual(loc.related('same_as').one().eid,
                             dunkerque.eid)
            # we still have dunkerque
            expected = (51.03297, 2.377)
            self.assertEqual(expected, (loc.latitude, loc.longitude))

    def test_sameas_location(self):
        """
        Test Authority location is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            moscou = cnx.create_entity(
                'ExternalUri',
                source=u'source',
                label=u'Moscou (Russie)',
                uri=u'https://yandex.com/moscow')
            loc = cnx.create_entity(
                'LocationAuthority',
                label=u'Moscou (Russie)',
                same_as=moscou
            )
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            cnx.commit()

    def test_add_source_to_geonames_exturi(self):
        """Add 'geoname' source if not exists
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                'ExternalUri',
                label=u'Dunkerque (Nord, France)',
                uri=u'http://www.geonames.org/3020686/')
            cnx.commit()
            self.assertEqual('geoname', dunkerque.source)

    def test_no_source_exturi(self):
        """Do not add source on ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                'ExternalUri',
                label=u'Dunkerque (Nord, France)',
                uri=u'http://www.othergeoname.org/3020686/')
            cnx.commit()
            self.assertIsNone(dunkerque.source)

    def test_update_source_to_geonames_exturi(self):
        """Update 'geoname' source if not exists
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                'ExternalUri',
                label=u'Dunkerque (Nord, France)',
                source=u'toto',
                uri=u'http://www.geonames.org/3020686/')
            cnx.commit()
            self.assertEqual('geoname', dunkerque.source)


if __name__ == '__main__':
    import unittest
    unittest.main()

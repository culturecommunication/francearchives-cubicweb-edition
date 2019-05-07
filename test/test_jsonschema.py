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
"""cubicweb-frarchives_edition tests for JSON schema."""
import base64
from datetime import date
from unittest import TestCase
import urllib

from cubicweb import Binary, ValidationError
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.pyramid.test import PyramidCWTest

from cubicweb_frarchives_edition.entities import parse_dataurl

import utils


class ParseDataURLTC(utils.FrACubicConfigMixIn, TestCase):

    def test_invalid_scheme(self):
        with self.assertRaises(ValueError) as cm:
            parse_dataurl('blah:pif')
        self.assertEqual(str(cm.exception), 'invalid scheme blah')

    def test_no_mediatype(self):
        data, mediatype, parameters = parse_dataurl('data:,A%20brief%20note')
        self.assertEqual(mediatype, 'text/plain')
        self.assertEqual(parameters, {'charset': 'US-ASCII'})
        self.assertEqual(data, b'A brief note')

    def test_no_mediatype_base64(self):
        data, mediatype, parameters = parse_dataurl('data:;base64,{}'.format(
            urllib.quote(base64.b64encode('A brief note'))))
        self.assertEqual(mediatype, 'text/plain')
        self.assertEqual(parameters, {'charset': 'US-ASCII'})
        self.assertEqual(data, b'A brief note')

    def test_mediatype_without_parameters(self):
        data, mediatype, parameters = parse_dataurl('data:text/plain,coucou')
        self.assertEqual(mediatype, 'text/plain')
        self.assertEqual(parameters, {})
        self.assertEqual(data, 'coucou')

    def test_mediatype_with_parameters(self):
        data, mediatype, parameters = parse_dataurl(
            'data:text/plain;charset=latin1,%E7a%20va%20%3F')
        self.assertEqual(mediatype, 'text/plain')
        self.assertEqual(parameters, {'charset': 'latin1'})
        self.assertEqual(data, '\xe7a va ?')

    def test_mediatype_base64(self):
        data, mediatype, parameters = parse_dataurl(
            'data:;base64,Y2hhdA==')
        self.assertEqual(mediatype, 'text/plain')
        self.assertEqual(parameters, {'charset': 'US-ASCII'})
        self.assertEqual(data, 'chat')


class JSONSchemaTC(utils.FrACubicConfigMixIn, PyramidCWTest):

    settings = {
        'cubicweb.bwcompat': False,
        'pyramid.debug_notfound': True,
        'cubicweb.session.secret': 'stuff',
        'cubicweb.auth.authtkt.session.secret': 'stuff',
        'cubicweb.auth.authtkt.persistent.secret': 'stuff',
        'francearchives.autoinclude': 'no',
    }

    def includeme(self, config):
        config.include('cubicweb_jsonschema.api.schema')
        config.include('cubicweb_jsonschema.api.entities')

    def assertHasProperties(self, jsonschema, expected_properties,
                            definition_key=None):
        if definition_key:
            self.assertIn(definition_key, jsonschema['definitions'])
            definition = jsonschema['definitions'][definition_key]
            properties = definition['properties']
        else:
            properties = jsonschema['properties']
        etype = definition_key or ''
        missing = set(expected_properties) - set(properties)
        if missing:
            self.fail('"{}" missing from {} properties ({})'.format(
                ', '.join(list(missing)), etype, list(properties)))

    def test_findingaid_etype_schema(self):
        for role in ('creation', 'view'):
            with self.subTest(role=role):
                self.login()
                res = self.webapp.get(
                    '/findingaid/schema?role={}'.format(role), status=200,
                    headers={'accept': 'application/schema+json'})
                self.assertHasProperties(
                    res.json,  ('description', 'fatype', 'keywords'))

    def test_findingaid_entity_schema(self):
        with self.admin_access.cnx() as cnx:
            fa = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
            adapted = fa.cw_adapt_to('IJSONSchema')
            for role in ('view', 'edition'):
                with self.subTest(role=role):
                    schema = getattr(adapted, role + '_schema')()
                    self.assertHasProperties(
                        schema, ('description', 'fatype', 'keywords'))


class RelationMapperTC(utils.FrACubicConfigMixIn, CubicWebTC):

    def test_filedataattribute_mapper(self):
        with self.admin_access.cnx() as cnx:
            mapper = cnx.vreg['mappers'].select(
                'jsonschema.relation', cnx,
                etype='File', rtype='data', role='subject',
                target_types={'Bytes'})
            instance = {
                u'data': u'data:text/pdf;name=mypdf;base64,{}'.format(
                    base64.b64encode(b'1234')),
            }
            expected = {
                'data_name': u'mypdf',
                'data_format': u'text/pdf',
                'data': b'1234',
                'data_encoding': None,
            }
            values = mapper.values(None, instance)
            assert 'data' in values
            values['data'] = values['data'].read()
            self.assertEqual(values, expected)

    def test_filedataattribute_mapper_validationerror(self):
        with self.admin_access.cnx() as cnx:
            mapper = cnx.vreg['mappers'].select(
                'jsonschema.relation', cnx,
                etype='File', rtype='data', role='subject',
                target_types={'Bytes'})
            for instance in [
                {'data': u'who cares?'},
                {'data': u'badprefix:blah blah'},
                {'data': u'not in a list'},
            ]:
                with self.subTest(data=instance['data']):
                    with self.assertRaises(ValidationError):
                        mapper.values(None, instance)
            # Missing "name" parameter, a log message should appear.
            instance = {'data': u'data:text/plain;base64,{}'.format(
                    base64.b64encode('hello'))}
            with self.assertLogs('cubicweb.appobject', level='WARNING') as cm:
                mapper.values(None, instance)
            self.assertIn('uploaded data-url field', str(cm.output[0]))

    def test_file_creation(self):
        instance = {
            u'data': u'data:text/pdf;name=mypdf;base64,{}'.format(
                base64.b64encode(b'1234')),
        }
        with self.admin_access.cnx() as cnx:
            adapter = self.vreg['adapters'].select(
                'IJSONSchema', cnx, etype='File')
            f = adapter.create_entity(instance)
            cnx.commit()
            self.assertIsNone(f.title)
            self.assertEqual(f.data_format, u'text/pdf')
            self.assertEqual(f.data_name, u'mypdf')
            self.assertEqual(f.data.read(), '1234')

    def test_file_edition(self):
        with self.admin_access.cnx() as cnx:
            f = cnx.create_entity('File', data=Binary(b'ahah'),
                                  data_name=u'hehe', data_format=u'text/plain')
            cnx.commit()
            instance = {
                u'data': u'data:text/pdf;name=mypdf;base64,{}'.format(
                    base64.b64encode(b'1234')),
            }
            f.cw_adapt_to('IJSONSchema').edit_entity(instance)
            cnx.commit()
            self.assertEqual(f.data_format, u'text/pdf')
            self.assertEqual(f.data_name, u'mypdf')
            self.assertEqual(f.data.read(), '1234')

    def test_file_creation_as_related(self):
        """Create a file as target of a relation to an existing entity."""
        instance = {
            u'caption': u'my image',
            u'image_file': [{
                u'data': u'data:image/jpeg;name=test.jpeg;base64,{}'.format(
                    base64.b64encode(b'hello')),
                u'title': u'my photo',
            }],
        }
        with self.admin_access.cnx() as cnx:
            news = cnx.create_entity('NewsContent', title=u'new',
                                     start_date=date(2012, 1, 2))
            cnx.commit()
            adapter = self.vreg['adapters'].select(
                'IJSONSchema', cnx,
                etype='Image', rtype='news_image', role='object')
            adapter.create_entity(instance, news)
            cnx.commit()
            news.cw_clear_all_caches()
            self.assertEqual(len(news.news_image), 1)
            img = news.news_image[0]
            self.assertEqual(img.caption, u'my image')
            self.assertEqual(len(img.image_file), 1)
            imgfile = img.image_file[0]
            self.assertEqual(imgfile.title, u'my photo')
            self.assertEqual(imgfile.data_format, u'image/jpeg')
            self.assertEqual(imgfile.data_name, 'test.jpeg')
            self.assertEqual(imgfile.data.read(), 'hello')

    def test_file_serialization(self):
        with self.admin_access.cnx() as cnx:
            f = cnx.create_entity('File', data_name=u'blob',
                                  data=Binary(b'data'),
                                  data_format=u'application/octet-stream')
            cnx.commit()
            f.cw_clear_all_caches()
            value = f.cw_adapt_to('IJSONSchema').serialize()
            data, mediatype, parameters = parse_dataurl(value['data'])
            self.assertEqual(data, b'data')
            self.assertEqual(mediatype, b'application/octet-stream')
            # XXX cropperjs does not handle correctly attribute, value pairs in data url
            # self.assertEqual(parameters, {'name': 'blob'})

    def test_bytes_creation(self):
        instance = {
            u'title': u'my map',
            u'map_file': u'data:text/comma-separated-values;name=map;base64,{}'.format(
                base64.b64encode(b'a,b,c')),
        }
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but('bytes'):
                adapter = self.vreg['adapters'].select(
                    'IJSONSchema', cnx, etype='Map')
                f = adapter.create_entity(instance)
                cnx.commit()
                self.assertEqual(f.title, 'my map')
                self.assertEqual(f.map_file.read(), 'a,b,c')

    def test_bytes_edition(self):
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but('bytes'):
                cw_map = cnx.create_entity(
                    'Map', title=u'map', map_file=Binary(b'ahah'))
                cnx.commit()
                instance = {
                    u'title': u'the map',
                    u'map_file': u'data:;base64,{}'.format(
                        base64.b64encode(b'a,b,c')),
                }
                cw_map.cw_adapt_to('IJSONSchema').edit_entity(instance)
                cnx.commit()
                self.assertEqual(cw_map.title, 'the map')
                self.assertEqual(cw_map.map_file.read(), 'a,b,c')

    def test_bytes_serialization(self):
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but('bytes'):
                cw_map = cnx.create_entity(
                    'Map', title=u'map', map_file=Binary(b'ahah'))
                cnx.commit()
                cw_map.cw_clear_all_caches()
                value = cw_map.cw_adapt_to('IJSONSchema').serialize()
                data, mediatype, parameters = parse_dataurl(value['map_file'])
                self.assertEqual(data, b'ahah')
                self.assertEqual(mediatype, b'text/plain')


if __name__ == '__main__':
    import unittest
    unittest.main()

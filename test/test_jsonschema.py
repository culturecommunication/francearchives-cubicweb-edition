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
import urllib.request
import urllib.parse
import urllib.error

from cubicweb import Binary, ValidationError
from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_jsonschema import CREATION_ROLE, VIEW_ROLE

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin
from cubicweb_frarchives_edition.entities import parse_dataurl

import utils


class ParseDataURLTC(utils.FrACubicConfigMixIn, TestCase):
    def test_invalid_scheme(self):
        with self.assertRaises(ValueError) as cm:
            parse_dataurl("blah:pif")
        self.assertEqual(str(cm.exception), "invalid scheme blah")

    def test_no_mediatype(self):
        data, mediatype, parameters = parse_dataurl("data:,A%20brief%20note")
        self.assertEqual(mediatype, "text/plain")
        self.assertEqual(parameters, {"charset": "US-ASCII"})
        self.assertEqual(data, b"A brief note")

    def test_no_mediatype_base64(self):
        data, mediatype, parameters = parse_dataurl(
            "data:;base64,{}".format(
                urllib.parse.quote(base64.b64encode(b"A brief note").decode("utf-8"))
            )
        )
        self.assertEqual(mediatype, "text/plain")
        self.assertEqual(parameters, {"charset": "US-ASCII"})
        self.assertEqual(data, b"A brief note")

    def test_mediatype_without_parameters(self):
        data, mediatype, parameters = parse_dataurl("data:text/plain,coucou")
        self.assertEqual(mediatype, "text/plain")
        self.assertEqual(parameters, {})
        self.assertEqual(data, b"coucou")

    def test_mediatype_with_parameters(self):
        data, mediatype, parameters = parse_dataurl(
            "data:text/plain;charset=latin1,%E7a%20va%20%3F"
        )
        self.assertEqual(mediatype, "text/plain")
        self.assertEqual(parameters, {"charset": "latin1"})
        self.assertEqual(data, b"\xe7a va ?")

    def test_mediatype_base64(self):
        data, mediatype, parameters = parse_dataurl("data:;base64,Y2hhdA==")
        self.assertEqual(mediatype, "text/plain")
        self.assertEqual(parameters, {"charset": "US-ASCII"})
        self.assertEqual(data, b"chat")


class JSONSchemaTC(S3BfssStorageTestMixin, utils.FranceArchivesCMSTC):
    def includeme(self, config):
        config.include("cubicweb_jsonschema.api.schema")
        config.include("cubicweb_jsonschema.api.entities")

    def assertHasProperties(self, jsonschema, expected_properties, definition_key=None):
        if definition_key:
            self.assertIn(definition_key, jsonschema["definitions"])
            definition = jsonschema["definitions"][definition_key]
            properties = definition["properties"]
        else:
            properties = jsonschema["properties"]
        etype = definition_key or ""
        missing = set(expected_properties) - set(properties)
        if missing:
            self.fail(
                '"{}" missing from {} properties ({})'.format(
                    ", ".join(list(missing)), etype, list(properties)
                )
            )

    def test_findingaid_etype_schema(self):
        for role in ("creation", "view"):
            with self.subTest(role=role):
                self.login()
                res = self.webapp.get(
                    "/findingaid/schema?role={}".format(role),
                    status=200,
                    headers={"accept": "application/schema+json"},
                )
                self.assertHasProperties(res.json, ("description", "fatype", "keywords"))

    def test_findingaid_entity_schema(self):
        with self.admin_access.cnx() as cnx:
            fa = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
            adapted = fa.cw_adapt_to("IJSONSchema")
            for role in ("view", "edition"):
                with self.subTest(role=role):
                    schema = getattr(adapted, role + "_schema")()
                    self.assertHasProperties(schema, ("description", "fatype", "keywords"))

    def test_circular_entity_schema(self):
        """
        Trying: get Circular jsl_document fields
        Excepting: jsl_document fields are in expected order
        """
        with self.admin_access.cnx() as cnx:
            mapper = cnx.vreg["mappers"].select("jsonschema.entity", cnx, etype="Circular")
            expected = [
                "circ_id",
                "siaf_daf_code",
                "nor",
                "code",
                "title",
                "kind",
                "siaf_daf_kind",
                "status",
                "signing_date",
                "siaf_daf_signing_date",
                "circular_modification_date",
                "abrogation_date",
                "link",
                "order",
                "producer",
                "producer_acronym",
                "abrogation_text",
                "archival_field",
            ]
            for role in (CREATION_ROLE, VIEW_ROLE):
                document = mapper.jsl_document(role)
                self.assertEqual(expected, [e.title for e in document.iter_fields()])


class RelationMapperTC(S3BfssStorageTestMixin, utils.FrACubicConfigMixIn, CubicWebTC):
    def test_filedataattribute_mapper(self):
        with self.admin_access.cnx() as cnx:
            mapper = cnx.vreg["mappers"].select(
                "jsonschema.relation",
                cnx,
                etype="File",
                rtype="data",
                role="subject",
                target_types={"Bytes"},
            )
            instance = {
                "data": "data:text/pdf;name=mypdf;base64,{}".format(
                    base64.b64encode(b"1234").decode("utf-8")
                ),
            }
            expected = {
                "data_name": "mypdf",
                "data_format": "text/pdf",
                "data": b"1234",
                "data_encoding": None,
            }
            values = mapper.values(None, instance)
            assert "data" in values
            values["data"] = values["data"].read()
            self.assertEqual(values, expected)

    def test_filedataattribute_mapper_validationerror(self):
        with self.admin_access.cnx() as cnx:
            mapper = cnx.vreg["mappers"].select(
                "jsonschema.relation",
                cnx,
                etype="File",
                rtype="data",
                role="subject",
                target_types={"Bytes"},
            )
            for instance in [
                {"data": "who cares?"},
                {"data": "badprefix:blah blah"},
                {"data": "not in a list"},
            ]:
                with self.subTest(data=instance["data"]):
                    with self.assertRaises(ValidationError):
                        mapper.values(None, instance)
            # Missing "name" parameter, a log message should appear.
            instance = {
                "data": "data:text/plain;base64,{}".format(
                    base64.b64encode(b"hello").decode("utf-8")
                )
            }
            with self.assertLogs("cubicweb.appobject", level="WARNING") as cm:
                mapper.values(None, instance)
            self.assertIn("uploaded data-url field", str(cm.output[0]))

    def test_file_creation(self):
        instance = {
            "data": "data:text/pdf;name=mypdf;base64,{}".format(
                base64.b64encode(b"1234").decode("utf-8")
            ),
        }
        with self.admin_access.cnx() as cnx:
            adapter = self.vreg["adapters"].select("IJSONSchema", cnx, etype="File")
            f = adapter.create_entity(instance)
            cnx.commit()
            self.assertIsNone(f.title)
            self.assertEqual(f.data_format, "text/pdf")
            self.assertEqual(f.data_name, "mypdf")
            self.assertEqual(f.data.read(), b"1234")

    def test_file_edition(self):
        with self.admin_access.cnx() as cnx:
            f = cnx.create_entity(
                "File", data=Binary(b"ahah"), data_name="hehe", data_format="text/plain"
            )
            cnx.commit()
            instance = {
                "data": "data:text/pdf;name=mypdf;base64,{}".format(
                    base64.b64encode(b"1234").decode("utf-8")
                ),
            }
            f.cw_adapt_to("IJSONSchema").edit_entity(instance)
            cnx.commit()
            self.assertEqual(f.data_format, "text/pdf")
            self.assertEqual(f.data_name, "mypdf")
            self.assertEqual(f.data.read(), b"1234")

    def test_newsconent_ijsonschema(self):
        """
        Trying: Create a NewsContent with an image.
        Expecting: jsonschema.collection mapper return a list of IJSONSchema adapted images
        """
        with self.admin_access.cnx() as cnx:
            news = cnx.create_entity("NewsContent", title="new", start_date=date(2012, 1, 2))
            cnx.commit()
            fobj = cnx.create_entity("File", data=Binary(b"data"), data_name="data")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            news = cnx.create_entity(
                "NewsContent", title="new", start_date=date(2012, 1, 2), news_image=image
            )
            cnx.commit()
            news.cw_clear_all_caches()
            self.assertEqual(len(news.news_image), 1)
            data = news.cw_adapt_to("IJSONSchema").serialize()
            mapper = cnx.vreg["mappers"].select(
                "jsonschema.collection", cnx, rtype="news_image", role="subject"
            )
            data = mapper.serialize(news.related("news_image").entities())
            self.assertTrue(isinstance(data[0]["image_file"], list))
            self.assertTrue(fobj.data_hash, data[0]["image_file"][0]["data_hash"])

    def test_file_creation_as_related(self):
        """Create a file as target of a relation to an existing entity."""
        instance = {
            "caption": "my image",
            "image_file": [
                {
                    "data": "data:image/jpeg;name=test.jpeg;base64,{}".format(
                        base64.b64encode(b"hello").decode("utf-8")
                    ),
                    "title": "my photo",
                }
            ],
        }
        with self.admin_access.cnx() as cnx:
            news = cnx.create_entity("NewsContent", title="new", start_date=date(2012, 1, 2))
            cnx.commit()
            adapter = self.vreg["adapters"].select(
                "IJSONSchema", cnx, etype="Image", rtype="news_image", role="object"
            )
            adapter.create_entity(instance, news)
            cnx.commit()
            news.cw_clear_all_caches()
            self.assertEqual(len(news.news_image), 1)
            img = news.news_image[0]
            self.assertEqual(img.caption, "my image")
            self.assertEqual(len(img.image_file), 1)
            imgfile = img.image_file[0]
            self.assertEqual(imgfile.title, "my photo")
            self.assertEqual(imgfile.data_format, "image/jpeg")
            self.assertEqual(imgfile.data_name, "test.jpeg")
            self.assertEqual(imgfile.data.read(), b"hello")

    def test_file_serialization(self):
        with self.admin_access.cnx() as cnx:
            f = cnx.create_entity(
                "File",
                data_name="blob",
                data=Binary(b"data"),
                data_format="application/octet-stream",
            )
            cnx.commit()
            f.cw_clear_all_caches()
            value = f.cw_adapt_to("IJSONSchema").serialize()
            data, mediatype, parameters = parse_dataurl(value["data"])
            self.assertEqual(data, b"data")
            self.assertEqual(mediatype, "application/octet-stream")
            # XXX cropperjs does not handle correctly attribute, value pairs in data url
            # self.assertEqual(parameters, {'name': 'blob'})

    def test_bytes_creation(self):
        instance = {
            "title": "my map",
            "order": 1,
            "map_file": "data:text/comma-separated-values;name=map;base64,{}".format(
                base64.b64encode(b"a,b,c").decode("utf-8")
            ),
        }
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but("bytes"):
                adapter = self.vreg["adapters"].select("IJSONSchema", cnx, etype="Map")
                f = adapter.create_entity(instance)
                cnx.commit()
                self.assertEqual(f.title, "my map")
                self.assertEqual(f.map_file.read(), b"a,b,c")

    def test_bytes_edition(self):
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but("bytes"):
                cw_map = cnx.create_entity("Map", title="map", map_file=Binary(b"ahah"))
                cnx.commit()
                instance = {
                    "title": "the map",
                    "order": cw_map.order,
                    "map_file": "data:;base64,{}".format(
                        base64.b64encode(b"a,b,c").decode("utf-8")
                    ),
                }
                cw_map.cw_adapt_to("IJSONSchema").edit_entity(instance)
                cnx.commit()
                self.assertEqual(cw_map.title, "the map")
                self.assertEqual(cw_map.map_file.read(), b"a,b,c")

    def test_bytes_serialization(self):
        with self.admin_access.cnx() as cnx:
            with cnx.allow_all_hooks_but("bytes"):
                cw_map = cnx.create_entity("Map", title="map", map_file=Binary(b"ahah"))
                cnx.commit()
                cw_map.cw_clear_all_caches()
                value = cw_map.cw_adapt_to("IJSONSchema").serialize()
                data, mediatype, parameters = parse_dataurl(value["map_file"])
                self.assertEqual(data, b"ahah")
                self.assertEqual(mediatype, "text/plain")


if __name__ == "__main__":
    import unittest

    unittest.main()

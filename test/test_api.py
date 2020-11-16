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
"""cubicweb-frarchives_edition unit tests for Pyramid JSON API."""

import base64
from unittest import skip
import datetime as dt
import mimetypes

from mock import patch
from pytest import xfail

from cubicweb import Binary, ValidationError
from cubicweb.pyramid.test import PyramidCWTest

from cubicweb_jsonschema.views import jsonschema_section

from cubicweb_francearchives.testutils import HashMixIn

import utils


class BaseTC(utils.FrACubicConfigMixIn, PyramidCWTest):

    settings = {
        "cubicweb.bwcompat": False,
        "pyramid.debug_notfound": True,
        "pyramid.debug_routematch": True,
        "cubicweb.session.secret": "stuff",
        "cubicweb.auth.authtkt.session.secret": "stuff",
        "cubicweb.auth.authtkt.persistent.secret": "stuff",
        "francearchives.autoinclude": "no",
    }

    def includeme(self, config):
        config.include("cubicweb_frarchives_edition.api")
        config.include("cubicweb_francearchives.pviews")

    def setUp(self):
        self.config.global_set_option("anonymous-password", "a%o8ps650RDw")
        self.config.global_set_option("anonymous-user", "toto")
        super(BaseTC, self).setUp()


class RestApiJSONTest(HashMixIn, BaseTC):
    def setUp(self):
        super(RestApiJSONTest, self).setUp()
        self.uicfg_backups = {}

    def tearDown(self):
        super(RestApiJSONTest, self).tearDown()
        for key, oldvalue in list(self.uicfg_backups.items()):
            if oldvalue is None:
                jsonschema_section.del_rtag(*key)
            else:
                jsonschema_section.tag_relation(key, oldvalue)

    def uicfg_set(self, key, value):
        oldvalue = jsonschema_section.etype_get(*key)
        self.uicfg_backups[key] = oldvalue
        jsonschema_section.tag_relation(key, value)

    def test_get_json_accept(self):
        with self.admin_access.repo_cnx() as cnx:
            blog_entry = cnx.create_entity("BlogEntry", title="tmp", content="content")
            user_account = cnx.create_entity("UserAccount", name="name")
            cnx.add_relation(blog_entry.eid, "has_creator", user_account.eid)
            cnx.commit()
            url = "/blogentry/%d" % blog_entry.eid
        self.login()
        res = self.webapp.get(url, headers={"accept": "application/json"})
        self.assertEqual(res.headers["content-type"], "application/json")
        data = res.json
        self.assertEqual(data["content"], "content")

    def test_get_multiple_json_accept(self):
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("CWGroup", name="tmp1")
            cnx.create_entity("CWGroup", name="tmp2")
            cnx.commit()
        self.login()
        res = self.webapp.get("/cwgroup/", headers={"Accept": "application/json"})
        data = res.json
        self.assertEqual(res.headers["content-type"], "application/json")
        with self.admin_access.repo_cnx() as cnx:
            self.assertEqual(cnx.find("CWGroup").rowcount, len(data))

    def test_post_json_data(self):
        data = {"name": "tmp"}
        self.login()

        res = self.webapp.post_json(
            "/cwgroup/", data, status=201, headers={"Accept": "application/json"}
        )
        with self.admin_access.repo_cnx() as cnx:
            rset = cnx.find("CWGroup", name="tmp")
            self.assertEqual(rset.rowcount, 1)
            self.assertEqual(res.json["dc_title"], rset.one().name)
        self.assertEqual(res.location, "https://localhost:80/CWGroup/%d" % rset[0][0])

    def test_get_entity_schema(self):
        """GET on /<etype>/eid/schema to retrieve entype schmea"""
        with self.admin_access.repo_cnx() as cnx:
            group_eid = cnx.create_entity("CWGroup", name="tmp1").eid
            cnx.commit()
        self.login()
        res = self.webapp.get(
            "/CWGroup/{}/schema".format(group_eid),
            status=200,
            headers={"Accept": "application/schema+json"},
        )
        jschema = res.json
        items_key = list(jschema["properties"])
        self.assertCountEqual(items_key, ["name"])

    @skip("customization of validation error needs porting in cubicweb-jsonschema")
    def test_validationerror(self):
        """Test validation_failed view, by POSTing incomplete data."""
        data = {}
        self.login()
        res = self.webapp.post_json(
            "/cwuser/", data, status=400, headers={"Accept": "application/json"}
        )
        errors = res.json_body["errors"]
        expected = [
            {"status": 422, "details": "required attribute", "source": {"pointer": "login"}},
            {"status": 422, "details": "required attribute", "source": {"pointer": "upassword"}},
        ]
        self.assertCountEqual(errors, expected)

    @skip("customization of validation error needs porting in cubicweb-jsonschema")
    def test_validationerror_nosource(self):
        """Test validation_failed view with no specific source entry."""
        with patch(
            "cubicweb.req.RequestSessionBase.create_entity",
            side_effect=ValidationError(None, {None: "unmapped"}),
        ):
            res = self.webapp.post_json(
                "/cwuser/", {}, status=400, headers={"Accept": "application/json"}
            )
            errors = res.json_body["errors"]
            expected = [{"status": 422, "details": "unmapped"}]
            self.assertCountEqual(errors, expected)

    def test_post_json_file_upload(self):
        """Posting some JSON data along with files to create a Circular with
        a file.
        """
        data = {
            "circ_id": "C1",
            "title": "the-circular",
            "status": "revoked",
            "attachment": [
                {
                    "data": "data:text/xml;name=test.xml;base64,{}".format(
                        base64.b64encode(b"hello").decode("utf-8")
                    ),
                    "title": "my file",
                }
            ],
        }
        self.uicfg_set(("Circular", "attachment", "File", "subject"), "inlined")
        self.login()
        resp = self.webapp.post_json(
            "/Circular/", data, status=201, headers={"Accept": "application/json"}
        )
        with self.admin_access.cnx() as cnx:
            rset = cnx.find("Circular")
            entity = rset.one()
            self.assertEqual(entity.circ_id, "C1")
            self.assertTrue(entity.attachment)
            f = entity.attachment[0]
            self.assertEqual(f.data_name, "test.xml")
            self.assertEqual(f.read(), b"hello")
            self.assertEqual(f.title, "my file")
        self.assertEqual(resp.location, "https://localhost:80/Circular/%d" % entity.eid)

    def test_post_json_file_title_mimetype(self):
        data = {
            "title": "toto.txt",
            "data": "data:text/plain;base64,{}".format(base64.b64encode(b"hello").decode("utf-8")),
        }
        self.login()
        self.webapp.post_json("/File/", data, status=201, headers={"Accept": "application/json"})
        with self.admin_access.cnx() as cnx:
            f = cnx.find("File", title="toto.txt").one()
            self.assertEqual(f.data_name, "toto.txt")
            self.assertEqual(f.data_format, "text/plain")

    def test_post_json_file_without_title_mimetype(self):
        data = {
            "data": "data:text/plain;base64,{}".format(base64.b64encode(b"hello").decode("utf-8")),
        }
        self.login()
        resp = self.webapp.post_json(
            "/File/", data, status=201, headers={"Accept": "application/json"}
        )
        with self.admin_access.cnx() as cnx:
            f = cnx.find("File", eid=resp.json_body["eid"]).one()
            self.assertEqual(
                f.data_name,
                "%s%s" % ("<unspecified file name>", mimetypes.guess_extension("text/plain")),
            )
            self.assertEqual(f.data_format, "text/plain")

    def test_post_json_file_with_name_mimetype(self):
        data = {
            "data": "data:text/plain;name=toto;base64,{}".format(
                base64.b64encode(b"hello").decode("utf-8")
            ),
        }
        self.login()
        resp = self.webapp.post_json(
            "/File/", data, status=201, headers={"Accept": "application/json"}
        )
        with self.admin_access.cnx() as cnx:
            f = cnx.find("File", eid=resp.json_body["eid"]).one()
            self.assertEqual(
                f.data_name, "%s%s" % ("toto", mimetypes.guess_extension("text/plain"))
            )
            self.assertEqual(f.data_format, "text/plain")

    def test_post_json_file_upload_badrequest(self):
        self.login()
        for rtype, value in [
            ("unknown", [{"data": "who cares?"}]),
            ("attachment", [{"data": "badprefix:blah blah"}]),
            ("attachment", {"data": "not in a list"}),
        ]:
            data = {rtype: value}
            with self.subTest(**data):
                # Rely on "status=400" for test assertion.
                self.webapp.post_json(
                    "/Circular/", data, status=400, headers={"Accept": "application/json"}
                )

    def test_post_json_file_upload_missing_name(self):
        self.uicfg_set(("Circular", "attachment", "File", "subject"), "inlined")
        # Missing "name" parameter, response is OK.
        data = {
            "circ_id": "C1",
            "title": "the-circular",
            "status": "revoked",
            "attachment": [
                {
                    "data": "data:text/plain;base64,{}".format(
                        base64.b64encode(b"hello").decode("utf-8")
                    ),
                }
            ],
        }
        self.login()
        with self.assertLogs("cubicweb.appobject", level="WARNING") as cm:
            self.webapp.post_json(
                "/Circular/", data, status=201, headers={"Accept": "application/json"}
            )
        expected_msg = "uploaded data-url field"
        self.assertTrue(
            any(expected_msg in str(line) for line in cm.output),
            '"%s" not found in %s' % (expected_msg, cm.output),
        )
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute(
                "Any F WHERE C attachment F," ' F data_name ILIKE "<unspecified file name%"'
            )
            self.assertTrue(rset)

    def test_put_json(self):
        """PATCH request are no more accepted, use PUT instead"""
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("CWGroup", name="tmp")
            cnx.commit()
        self.login()
        data = {"name": "new"}
        res = self.webapp.put_json(
            "/cwgroup/tmp/", data, status=200, headers={"Accept": "application/json"}
        )
        self.assertEqual(res.location, "https://localhost:80/cwgroup/tmp/")
        data = res.json
        self.assertEqual(data["name"], "new")
        with self.admin_access.repo_cnx() as cnx:
            self.assertEqual(cnx.find("CWGroup", name="tmp").rowcount, 0)
            self.assertEqual(cnx.find("CWGroup", name="new").rowcount, 1)

    def test_put_json_error(self):
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("CWGroup", name="tmp")
            cnx.commit()
        self.login()
        with patch("cubicweb.entity.Entity.cw_set", side_effect=Exception("Failed!")):
            res = self.webapp.put_json(
                "/cwgroup/tmp/", {"name": "paf"}, status=400, headers={"Accept": "application/json"}
            )
            errors = res.json_body["errors"]
            expected = [{"status": 400, "details": "Failed!"}]
            self.assertCountEqual(errors, expected)

    def test_put_json_with_incomplete_data(self):
        """A PUT request *replaces* entity attributes, so if fields are missing
        from JSON request body, respective attributes are reset. Reset
        subtitle value on a CommemorationItem
        """
        with self.admin_access.repo_cnx() as cnx:
            coll = cnx.create_entity("CommemoCollection", title="recueil 2010", year=2010)
            commemo = cnx.create_entity(
                "CommemorationItem",
                title="item1",
                alphatitle="item1",
                subtitle="subtitle",
                commemoration_year=2010,
                collection_top=coll,
            )
            cnx.commit()
        self.login()
        data = {
            "title": "title2",
            "alphatitle": "item1",
            "on_homepage_order": 0,
            "commemoration_year": 2011,
        }
        url = "/commemorationitem/{}/".format(commemo.eid)
        self.webapp.put_json(url, data, headers={"Accept": "application/json"})
        with self.admin_access.repo_cnx() as cnx:
            commemo = cnx.find("CommemorationItem", eid=commemo.eid).one()
            self.assertEqual(commemo.title, "title2")
            self.assertEqual(commemo.subtitle, None)
            self.assertEqual(commemo.commemoration_year, 2011)

    def test_delete_entity(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity("CWGroup", name="tmp")
            cnx.commit()
        self.login()
        self.webapp.delete("/cwgroup/tmp/", status=204)
        with self.admin_access.cnx() as cnx:
            rset = cnx.find("CWGroup", name="tmp")
            self.assertFalse(rset)

    def test_delete_unauthorizederror(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity("CWGroup", name="tmp")
            cnx.commit()
        res = self.webapp.delete("/cwgroup/tmp", status=400)
        errors = res.json_body["errors"]
        expected = [{"details": "not authorized", "status": 401}]
        self.assertCountEqual(errors, expected)

    def test_delete_validationerror(self):
        with self.admin_access.cnx() as cnx:
            section_eid = cnx.create_entity("TestSection", title="section").eid
            article_eid = cnx.create_entity(
                "TestBaseContent", in_section=section_eid, content="content", title="article"
            ).eid
            cnx.commit()
        self.login()
        res = self.webapp.delete("/TestSection/{}".format(section_eid), status=400)
        errors = res.json_body["errors"]
        expected = [
            {
                "details": (
                    "at least one relation in_section is required on TestBaseContent "
                    "(%s)" % article_eid
                ),
                "source": {"pointer": "in_section"},
                "status": 422,
            }
        ]
        self.assertCountEqual(errors, expected)

    def test_get_related(self):
        with self.admin_access.cnx() as cnx:
            findingaid_eid = utils.create_findingaid(cnx, with_file=True).eid
            cnx.create_entity(
                "Service", name="the-service", category="s", reverse_service=findingaid_eid
            )
            cnx.commit()
        url = "/findingaid/%s/service" % findingaid_eid
        res = self.webapp.get(url, headers={"accept": "application/json"})
        data = res.json
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["dc_title"], "the-service")

    def test_get_related_image(self):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity("File", data=Binary(b"data"), data_name="data")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            news_eid = cnx.create_entity(
                "NewsContent", title="News", start_date=dt.date(2011, 1, 1), news_image=image
            ).eid
            cnx.commit()
        url = "/NewsContent/%s/news_image" % news_eid
        res = self.webapp.get(url, headers={"accept": "application/json"})
        data = res.json
        self.assertEqual(data[0]["eid"], image.eid)

    def test_get_related_authorities(self):
        with self.admin_access.cnx() as cnx:
            ci = utils.create_default_commemoitem(cnx)
            agent = cnx.create_entity(
                "AgentAuthority", label="the-preflabel", reverse_related_authority=ci
            )
            cnx.commit()
        url = "/CommemorationItem/{}/related_authority?target_type=AgentAuthority".format(ci.eid)
        res = self.webapp.get(url, headers={"accept": "application/json"})
        data = res.json
        self.assertEqual(data[0]["eid"], agent.eid)

    def test_get_target_schema(self):
        """GET on /<etype>/relationships/<rtype>/schema to retrieve
        target schema with a particular
        """
        url = (
            "/commemorationitem/relationships/related_authority/"
            "schema?role=creation&target_type=AgentAuthority"
        )
        self.login()
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        items_key = list(res.json["properties"])
        self.assertCountEqual(items_key, ["label"])

    @skip("indices are being rewriting")
    def test_index_update(self):
        with self.admin_access.cnx() as cnx:
            authority = utils.create_default_agent_authority(cnx)
            cnx.commit()
            instance = {"birthyear": 1999, "label": "new"}
            adapter = cnx.vreg["adapters"].select("IJSONSchema", cnx, entity=authority)
            adapter.edit_entity(instance)
            authority.cw_clear_all_caches()
            self.assertEqual(authority.birthyear, 1999)
            self.assertEqual(authority.label, "new")

    def test_index_serialize(self):
        with self.admin_access.cnx() as cnx:
            authority = utils.create_default_agent_authority(cnx)
            cnx.commit()
            jschema = authority.cw_adapt_to("IJSONSchema").serialize()
            self.assertEqual(jschema["label"], "the-preflabel")

    def test_section_relationship_add_basecontent(self):
        with self.admin_access.cnx() as cnx:
            sect = cnx.create_entity("Section", title="sect-2").eid
            cnx.commit()
        path = "/section/{}/relationships/children?" "target_type=BaseContent".format(sect)
        self.login()
        data = {"title": "title", "summary_policy": "no_summary"}
        res = self.webapp.post_json(path, data, status=201, headers={"accept": "application/json"})
        self.assertEqual(res.json["title"], "title")

    def test_section_relationship_schema(self):
        url = "/section/relationships/children/schema?role=creation&target_type=BaseContent"
        self.login()
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        items_key = list(res.json["properties"])
        self.assertCountEqual(
            items_key,
            [
                "basecontent_service",
                "content",
                "on_homepage",
                "order",
                "title",
                "summary",
                "summary_policy",
            ],
        )

    def test_add_related(self):
        """POST on /<etype>/<eid>/relationships/<rtype> with primary entity as
        subject of <rtype>.
        """
        with self.admin_access.repo_cnx() as cnx:
            blog_entry = cnx.create_entity("BlogEntry", title="tmp", content="content")
            cnx.commit()
        url = "/blogentry/%d/relationships/has_creator" % blog_entry.eid
        data = {
            "name": "bob",
        }
        self.login()
        res = self.webapp.post_json(url, data, status=201, headers={"accept": "application/json"})
        doc = res.json
        self.assertEqual(doc["name"], "bob")
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute(
                "Any A WHERE B has_creator A, B eid %(b)s, A name %(a)s",
                {"b": blog_entry.eid, "a": doc["name"]},
            )
        self.assertTrue(rset)

    def test_section_relationship_add_translation(self):
        with self.admin_access.cnx() as cnx:
            sect = cnx.create_entity("Section", title="titre").eid
            cnx.commit()
        path = "/section/{}/relationships/translation_of?" "target_type=SectionTranslation".format(
            sect
        )
        self.login()
        data = {"title": "title", "language": "en"}
        res = self.webapp.post_json(path, data, status=201, headers={"accept": "application/json"})
        self.assertEqual(res.json["title"], "title")

    def test_section_translation_of_schema(self):
        url = "/section/relationships/translation_of/schema?role=creation&target_type=SectionTranslation"  # noqa
        self.login()
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        items_key = list(res.json["properties"])
        self.assertCountEqual(
            items_key, ["title", "content", "language", "subtitle", "short_description"]
        )

    def test_add_findingaid_service(self):
        """POST on /<etype>/<eid>/relationships/<rtype> with primary entity as
        object of <rtype>.
        """
        with self.admin_access.repo_cnx() as cnx:
            findingaid = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
        url = "/findingaid/%s/relationships/service" % findingaid.eid
        data = {
            "category": "s1",
        }
        self.login()
        res = self.webapp.post_json(url, data, status=201, headers={"accept": "application/json"})
        doc = res.json
        self.assertEqual(doc["category"], "s1")
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute("Any S WHERE F service S, F eid %(f)s", {"f": findingaid.eid})
        self.assertTrue(rset)

    def test_get_rqtask_schema_role(self):
        """GET on /<etype>/schema?role=creation to retrieve
        the entity schema.
        """
        url = "/rqtask/schema?role=creation"
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        properties_key = sorted(list(res.json["properties"]))
        self.assertCountEqual(properties_key, ["name", "title"])

    def test_rqtask_schema_import_ead(self):
        """GET on /<etype>/schema?role=creation to retrieve
        the entity schema.
        """
        url = "/rqtask/schema?role=creation&schema_type=import_ead"
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        properties_key = sorted(list(res.json["properties"]))
        self.assertCountEqual(
            properties_key,
            [
                "file",
                "name",
                "title",
                "force-delete",
                "service",
                "should_normalize",
                "context_service",
            ],
        )

    def test_rqtask_schema_export_ape(self):
        """GET on /<etype>/schema?role=creation to retrieve
        the entity schema.
        """
        url = "/rqtask/schema?role=creation&schema_type=export_ape"
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        properties_key = sorted(list(res.json["properties"]))
        self.assertCountEqual(properties_key, ["services", "name", "title"])

    def test_get_relationship_schema(self):
        """GET on /<etype>/relationships/<rtype>/schema to retrieve
        target schema.
        """
        url = "/findingaid/relationships/service/schema?role=creation"
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        properties_key = list(res.json["properties"])
        self.assertIn("category", properties_key)

    def test_get_relationship_schema_reversed(self):
        """GET on /<etype>/relationships/<rtype>/schema to retrieve
        target schema on reverse side.
        """
        url = "/service/relationships/exref_service/schema/?role=creation"
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        self.assertEqual(res.json["title"], "ExternRef")


class WorkflowTests(BaseTC):
    def test_get_transitions_not_workflowable(self):
        """Ensure "transitions" route/view is not selectable for
        non-workflowable entity types.
        """
        with self.admin_access.cnx() as cnx:
            eid = cnx.execute("Any X LIMIT 1 WHERE X is CWEType")[0][0]
        url = "/cwetype/{0}/transitions/".format(eid)
        # No assert since we rely on status keyword to check the response
        # status.
        self.webapp.get(url, status=404, headers={"accept": "application/json"})

    def test_get_transitions_not_found(self):
        url = "/cwuser/6666/transitions/"
        self.login()
        self.webapp.get(url, status=404, headers={"accept": "application/json"})

    def test_get_transitions(self):
        with self.admin_access.cnx() as cnx:
            findingaid = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
        url = "/findingaid/{0.eid}/transitions/".format(findingaid)
        res = self.webapp.get(url, status=200, headers={"accept": "application/json"})
        data = res.json
        self.assertEqual(len(data), 0)
        # self.assertEqual(data[0]['comment'], 'binary content')
        with self.admin_access.cnx() as cnx:
            entity = cnx.entity_from_eid(findingaid.eid)
            entity.cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_publish", comment="this is ready"
            )
            cnx.commit()
        res = self.webapp.get(url, status=200, headers={"accept": "application/json"})
        data = res.json
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["comment"], "this is ready")

    def test_add_transition(self):
        with self.admin_access.cnx() as cnx:
            findingaid = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
        url = "/findingaid/{0.eid}/transitions/".format(findingaid)
        data = {
            "name": "wft_cmsobject_publish",
            "comment": "ready, go!",
        }
        self.login()
        res = self.webapp.post_json(url, data, status=200, headers={"accept": "application/json"})
        with self.admin_access.cnx() as cnx:
            rset = cnx.find("TrInfo", comment=res.json["comment"])
            self.assertEqual(len(rset), 1)

    def test_transition_schema(self):
        """XXX this test fails sometimes"""
        xfail("this test fails sometimes")
        with self.admin_access.cnx() as cnx:
            findingaid = utils.create_findingaid(cnx, with_file=True)
            cnx.commit()
        self.login()
        url = "/findingaid/{0.eid}/transitions/schema/?role=creation".format(findingaid)
        res = self.webapp.get(url, status=200, headers={"accept": "application/schema+json"})
        trinfo = res.json["definitions"]["TrInfo"]
        self.assertEqual(trinfo["properties"]["name"]["enum"], ["wft_cmsobject_publish"])


class UISchemaViewTC(BaseTC):
    def test_etype_uischema_view(self):
        url = "/basecontent/uischema"
        res = self.webapp.get(url, status=200, headers={"accept": "application/json"})
        expected = {
            "basecontent_service": {"ui:field": "autocompleteField"},
            "content": {
                "ui:widget": "wysiwygEditor",
            },
            "summary": {"ui:widget": "wysiwygEditor"},
            "ui:order": [
                "title",
                "content",
                "summary",
                "summary_policy",
                "on_homepage",
                "order",
                "basecontent_service",
            ],
        }
        self.assertEqual(res.json, expected)

    def test_commemoitemtranslation_uischema_view(self):
        url = "/commemorationitemtranslation/uischema"
        res = self.webapp.get(url, status=200, headers={"accept": "application/json"})
        expected = {
            "content": {"ui:widget": "wysiwygEditor"},
            "language": {"ui:disabled": {}},
            "ui:order": ["title", "subtitle", "content", "language"],
        }

        self.assertEqual(res.json, expected)


if __name__ == "__main__":
    import unittest

    unittest.main()

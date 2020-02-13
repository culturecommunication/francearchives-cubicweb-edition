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
from copy import deepcopy
import mock
import urllib.request
import urllib.parse
import urllib.error

from cubicweb import Binary, Unauthorized, ValidationError
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_francearchives.testutils import HashMixIn, PostgresTextMixin
from cubicweb_frarchives_edition import get_samesas_history

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa


class ReferencedFileTC(PostgresTextMixin, HashMixIn, FrACubicConfigMixIn, CubicWebTC):
    configcls = PostgresApptestConfiguration

    def _create_data(self, cnx):
        fobj = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="file.pdf",
            data_format="application/pdf",
        )
        bc1 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
<p>
<h1>bc</h1>
<a href="%s">file.pdf</a>
</p>"""
            % fobj.cw_adapt_to("IDownloadable").download_url(),
        )
        bc2 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
            <a href="%s">file.pdf</a>
            """
            % fobj.cw_adapt_to("IDownloadable").download_url(),
        )
        cnx.commit()
        return fobj, bc1, bc2

    def test_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File
        Expecting: File references the both BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X is File")[0][0].getvalue()
            self.assertCountEqual(
                [f.eid for f in fobj.reverse_referenced_files], [bc1.eid, bc2.eid]
            )
            self.assertTrue(osp.exists(fpath))

    def test_delete_one_referenced_file(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove one of the reference
        Expecting: File references only one BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X is File")[0][0].getvalue()
            bc2.cw_set(content="remove file")
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            self.assertCountEqual([f.eid for f in fobj.reverse_referenced_files], [bc1.eid])
            self.assertTrue(osp.exists(fpath))

    def test_delete_all_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove both references
        Expecting: file no longer exists
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X is File")[0][0].getvalue()
            bc1.cw_set(content="remove file")
            bc2.cw_set(content="remove file")
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(osp.exists(fpath))

    def test_delete_one_entity_with_referenced_file(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove one of BaseContent
        Expecting: file references only one BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X is File")[0][0].getvalue()
            bc2.cw_delete()
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            self.assertCountEqual([f.eid for f in fobj.reverse_referenced_files], [bc1.eid])
            self.assertTrue(osp.exists(fpath))

    def test_delete_all_entities_with_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove both references
        Expecting: file no longer exists
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X is File")[0][0].getvalue()
            bc1.cw_delete()
            bc2.cw_delete()
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(osp.exists(fpath))

    def test_publish_entity_with_referenced_files(self):
        """
        Trying: create a BaseContent referencing a file then publish and depublish it
        Expecting : only one file is created
        """
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="file.pdf",
                data_format="application/pdf",
            )
            bc = cnx.create_entity(
                "BaseContent",
                title="bc",
                content="""\
                <p>
<h1>bc</h1>
                <a href="%s">file.pdf</a>
                </p>"""
                % fobj.cw_adapt_to("IDownloadable").download_url(),
            )
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)
            bc = cnx.entity_from_eid(bc.eid)
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)
        with self.admin_access.cnx() as cnx:
            bc = cnx.entity_from_eid(bc.eid)
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)


class DeleteHookTests(FrACubicConfigMixIn, CubicWebTC):
    def test_undeletable_card(self):
        with self.admin_access.repo_cnx() as cnx:
            card = cnx.find("Card", wikiid="alert").one()
            with self.assertRaises(Unauthorized):
                card.cw_delete()

    def test_delete_article(self):
        with self.admin_access.repo_cnx() as cnx:
            article = cnx.create_entity("BaseContent", title="article")
            cnx.commit()
            article.cw_delete()
            cnx.commit()


class MapHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Map hooks."""

    def test_invalid_map_file(self):
        with self.admin_access.cnx() as cnx:
            with self.assertRaises(ValidationError) as cm:
                cnx.create_entity("Map", title="map", map_file=Binary(b'"a","b"\n"1","2"\n'))
                error = (
                    'CSV file invalid. It must contain "Code_insee", "URL", '
                    '"Couleur" and "Legende" headers'
                    'columns separated by ","'
                )
                self.assertEqual(cm.exception.errors, {"map_file": error})

    def test_valid_map_file(self):
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir, "Carte_Cadastres.csv"), "rb") as stream:
                cnx.create_entity("Map", title="map", map_file=Binary(stream.read()))
            cnx.commit()


class ServiceHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for Service hooks."""

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            service = cnx.create_entity(
                "Service",
                category="foo",
                level="level-D",
                code="FRADO75",
                dpt_code="75",
                name="Service de Paris",
            )
            cnx.commit()
            self.service_eid = service.eid
        super(ServiceHookTC, self).setup_database()

    def test_service_unique_level_code_annex(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "Service",
                category="foo1",
                level="level-D",
                dpt_code="93",
                name="93 service",
                annex_of=self.service_eid,
            )
            cnx.commit()
            cnx.create_entity(
                "Service",
                category="foo",
                level="level-D",
                dpt_code="93",
                name="93 service 2",
                annex_of=self.service_eid,
            )
            # make sure no ValidationError is raised: we should be able
            # to have 2 annexes with level-D in the same department.
            cnx.commit()

    def test_service_d_annex_of_unique(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "Service",
                category="foo1",
                level="level-D",
                dpt_code="75",
                annex_of=self.service_eid,
            )
            cnx.commit()

    def test_service_unique_level(self):
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "Service", category="foo1", level="level-R", dpt_code="75", name="Service de Paris"
            )
            cnx.commit()

    def test_service_unique_code_empty(self):
        """
        Trying: create two Services with empty code
        Expecting: Services are created
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity("Service", category="foo1", name="Service de Paris")
            cnx.create_entity("Service", category="foo1", name="Service de Paris")
            cnx.commit()

    def test_service_unique_code(self):
        """
        Trying: create two Services with the same code
        Expecting: the second Service is not created
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "Service", category="foo1", code="FRADO71", dpt_code="75", name="Service de Paris"
            )
            cnx.commit()
            with self.assertRaises(ValidationError):
                cnx.create_entity("Service", code="FRADO75")
                cnx.commit()

    def test_service_unique_code_update(self):
        """
        Trying: create a Services with an inexisting code and try
                to update the code to an existing one
        Expecting: the new code is not accepted
        """
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", category="foo1", code="FRADO71", dpt_code="75", name="Service de Paris"
            )
            cnx.commit()
            service.cw_set(code="FRADO75")
            with self.assertRaises(ValidationError):
                cnx.commit()


class CWUserHooksTC(FrACubicConfigMixIn, CubicWebTC):
    def test_cwuser_password_policy(self):
        with self.admin_access.cnx() as cnx:
            for wrong_psw in (
                "p",
                "toto",
                "password",
                "toto1TITI@",
                "o2ieuUYétrz4ud",
                "o2uaa$rzudpo*d2",
                "O2UAA$REZ3ED*D",
                "Iuz1YEr7azrIE",
                "123456-456745",
            ):
                with self.assertRaises(ValidationError):
                    self.create_user(
                        cnx, "toto", password=wrong_psw, groups=("users",), commit=True
                    )
                    cnx.commit()
                cnx.rollback()
        with self.admin_access.cnx() as cnx:
            self.create_user(cnx, "toto", password="one35OPt^çpp3", groups=("users",), commit=True)
            cnx.commit()
            # XXX this psw fails in test_pswd.py
            self.create_user(cnx, "titi", password="Iùz1YEr7az$rIE", groups=("users",), commit=True)
            cnx.commit()


class FileHookTests(PostgresTextMixin, HashMixIn, FrACubicConfigMixIn, CubicWebTC):
    configcls = PostgresApptestConfiguration

    def test_update_image_file(self):
        """simulate InlinedRelationMapper behavior: drop and recreate inlined
           Image.image_file File object"""
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity("File", data=Binary(b"data"), data_name="data")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            cnx.commit()
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid})[0][
                0
            ].getvalue()
            self.assertTrue(osp.exists(fpath))
            fobj1 = cnx.create_entity(
                "File", data=Binary(b"data"), data_name="data", reverse_image_file=image
            )
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertEqual(fpath, fpath1)
            self.assertTrue(osp.exists(fpath))

    def test_delete_image_file(self):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity("File", data=Binary(b"data"), data_name="data")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            cnx.commit()
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid})[0][
                0
            ].getvalue()
            self.assertTrue(osp.exists(fpath))
            fobj1 = cnx.create_entity("File", data=Binary(b"data1"), data_name="data1")
            image.cw_set(image_file=fobj1)
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertTrue(osp.exists(fpath1))
            self.assertFalse(osp.exists(fpath))

    def test_delete_same_images_file(self):
        """
        Trying: create 2 CWFiles with the same fpath. Delete one if CWFiles.
        Expecting: The file on the FS still exists
        """
        with self.admin_access.cnx() as cnx:
            data = b"data"
            fobj = cnx.create_entity("File", data=Binary(data), data_name=data.decode("utf-8"))
            fobj1 = cnx.create_entity("File", data=Binary(data), data_name=data.decode("utf-8"))
            cnx.commit()
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid})[0][
                0
            ].getvalue()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertEqual(fpath, fpath1)
            self.assertTrue(osp.exists(fpath))
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            cnx.find("File", eid=fobj1.eid).one()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertTrue(osp.exists(fpath1))


class CircularHookTC(HashMixIn, FrACubicConfigMixIn, CubicWebTC):
    """Tests for Circular hooks."""

    @classmethod
    def init_config(cls, config):
        super(CircularHookTC, cls).init_config(config)
        config.set_option("consultation-base-url", "https://francearchives.fr")

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            ce = cnx.create_entity
            self.fobj1 = ce(
                "File",
                data=Binary(b"some-file-data"),
                data_name="file1.pdf",
                data_format="application/pdf",
            )
            self.circular1 = ce(
                "Circular",
                circ_id="c1_AB_EC",
                status="in-effect",
                title="c1",
                attachment=self.fobj1,
            )
            self.circular2 = ce("Circular", circ_id="c2_AT_ER", status="in-effect", title="c1")
            cnx.commit()
        super(CircularHookTC, self).setup_database()

    def test_add_official_text(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find("Circular", eid=self.circular2.eid).one()
            ot = cnx.create_entity(
                "OfficialText", code=circular.circ_id, reverse_modified_text=self.circular1.eid
            )
            cnx.commit()
            self.assertEqual(ot.circular[0].circ_id, circular.circ_id)
            circ = cnx.find("Circular", circ_id="c1_AB_EC").one()
            ot.cw_set(code=circ.circ_id)
            cnx.commit()
            ot = cnx.find("OfficialText", eid=ot.eid).one()
            self.assertEqual(ot.circular[0].circ_id, circ.circ_id)

    def test_json_attributes_values(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find("Circular", eid=self.circular2.eid).one()
            expected = [
                ["circular_title_label", "c1"],
                ["circular_url", "https://francearchives.fr/circulaire/c2_AT_ER"],
                ["circular_circ_id_label", "c2_AT_ER"],
                ["circular_kind_label", None],
                ["circular_code_label", None],
                ["circular_nor_label", None],
                ["circular_status_label", "in-effect"],
                ["circular_link_label", None],
                ["circular_additional_link_label", ""],
                ["circular_attachment_label", ""],
                ["circular_additional_attachment_label", ""],
                ["circular_signing_date_label", ""],
                ["circular_siaf_daf_kind_label", None],
                ["circular_siaf_daf_code_label", None],
                ["circular_siaf_daf_signing_date_label", ""],
                ["circular_producer_label", None],
                ["circular_producer_acronym_label", None],
                ["circular_modification_date_label", ""],
                ["circular_abrogation_date_label", ""],
                ["circular_abrogation_text_label", None],
                ["circular_archival_field_label", None],
                ["circular_historical_context_label", ""],
                ["circular_business_field_label", ""],
                ["circular_document_type_label", ""],
                ["circular_action_label", ""],
                ["circular_modified_text_label", ""],
                ["circular_modifying_text_label", ""],
                ["circular_revoked_text_label", ""],
            ]
            self.assertEqual(expected, circular.values_from_json)
            daf_date = datetime(2014, 3, 2)
            circular.cw_set(nor="AAA", siaf_daf_signing_date=daf_date)
            cnx.commit()
            for expected in (
                ["circular_nor_label", "AAA"],
                ["circular_siaf_daf_signing_date_label", "02/03/2014"],
            ):
                self.assertIn(["circular_nor_label", "AAA"], circular.values_from_json)

    def test_json_relations_values(self):
        with self.admin_access.cnx() as cnx:
            circular = cnx.find("Circular", eid=self.circular1.eid).one()
            scheme = cnx.create_entity("ConceptScheme", title="some classification")
            concept = cnx.create_entity(
                "Concept",
                in_scheme=scheme,
                cwuri="uri1",
                reverse_label_of=cnx.create_entity(
                    "Label", label="administration", language_code="fr", kind="preferred"
                ),
                reverse_business_field=circular.eid,
            )
            circular.cw_set(additional_attachment=self.fobj1)
            concept.cw_clear_all_caches()
            cnx.commit()
            circular = cnx.find("Circular", eid=circular.eid).one()
            path = urllib.parse.quote("file/87b2b0e8c632bfbace95bf693f8e99f4acc92eca/file1.pdf")
            attachment = "https://francearchives.fr/{}".format(path)
            for expected in (
                ["circular_attachment_label", attachment],
                ["circular_additional_attachment_label", attachment],
                ["circular_business_field_label", "administration"],
            ):
                self.assertIn(expected, circular.values_from_json)


class ExternalUriHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for ExternalUri hooks."""

    configcls = PostgresApptestConfiguration

    @classmethod
    def init_config(cls, config):
        super(ExternalUriHookTC, cls).init_config(config)
        config.set_option("consultation-base-url", "https://francearchives.fr")

    def setup_database(self):
        super(ExternalUriHookTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            values = [
                [524901, 48.86, 2.34444],
                [3020686, 51.03297, 2.377],
                [2988507, 48.85341, 2.3488],
            ]
            cnx.cnxset.cu.executemany(
                """
            INSERT INTO geonames (geonameid, latitude, longitude)
            VALUES (%s, %s, %s)
            """,
                values,
            )
            cnx.commit()
            values = [
                [1, 524901, "Moskva", "fr"],
                [2, 3020686, "Dankerk", "fr"],
                [3, 498817, "Saint Petersburg", "fr"],
                [4, 2988507, "Parij", "fr"],
            ]
            cnx.cnxset.cu.executemany(
                """
            INSERT INTO geonames_altnames
                (alternateNameId, geonameid, alternate_name, isolanguage, rank)
            VALUES (%s, %s, %s, %s, 1)""",
                values,
            )
            values = [(498817, "Saint Petersburg", "RU", "66", 59.93863, 30.31413)]
            cnx.cnxset.cu.executemany(
                """
            INSERT INTO geonames
                (geonameid, name, country_code, admin1_code,
                 latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s)""",
                values,
            )
            cnx.commit()

    def test_sameas_history(self):
        """
        Test samesas_history table is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            uri = "http://www.geonames.org/2988507"
            paris = cnx.create_entity(
                "ExternalUri", source="source", label="Paris (France)", uri=uri
            )
            loc = cnx.create_entity(
                "LocationAuthority", label="Dunkerque (Nord, France)", same_as=paris
            )
            cnx.commit()
            self.assertEqual([(uri, loc.eid, True)], get_samesas_history(cnx, complete=True))
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual([(uri, loc.eid, False)], get_samesas_history(cnx, complete=True))
            new_uri = "http://www.geonames.org/3020686/"
            dunkerque = cnx.create_entity(
                "ExternalUri", source="source", label="Dunkerque (Nord, France)", uri=new_uri
            )
            dunkerque.cw_set(same_as=loc)
            cnx.commit()
            self.assertEqual(
                [(uri, loc.eid, False), (new_uri, loc.eid, True)],
                get_samesas_history(cnx, complete=True),
            )

    def test_sameas_geoname_location(self):
        """
        Test Authority location is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            moscou = cnx.create_entity(
                "ExternalUri",
                source="source",
                label="Moscou (Russie)",
                uri="http://www.geonames.org/524901",
            )
            loc = cnx.create_entity("LocationAuthority", label="Moscou (Russie)",)
            cnx.commit()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            loc.cw_set(same_as=moscou)
            cnx.commit()
            expected = (48.86, 2.34444)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            dunkerque = cnx.create_entity(
                "ExternalUri",
                source="source",
                label="Dunkerque (Nord, France)",
                uri="http://www.geonames.org/3020686/",
            )
            dunkerque.cw_set(same_as=loc)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            expected = (51.03297, 2.377)
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            # add a second uri
            loc.cw_set(same_as=moscou)
            cnx.commit()
            self.assertEqual(2, len(loc.same_as))
            expected = (48.86, 2.34444)
            self.assertEqual(expected, (loc.latitude, loc.longitude))
            cnx.execute(
                "DELETE A same_as E WHERE A eid %(a)s, E eid %(e)s", {"a": loc.eid, "e": moscou.eid}
            )
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertEqual(loc.related("same_as").one().eid, dunkerque.eid)
            # we still have dunkerque
            expected = (51.03297, 2.377)
            self.assertEqual(expected, (loc.latitude, loc.longitude))

    def test_sameas_location(self):
        """
        Test Authority location is updated on add/remove ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            moscou = cnx.create_entity(
                "ExternalUri",
                source="source",
                label="Moscou (Russie)",
                uri="https://yandex.com/moscow",
            )
            loc = cnx.create_entity("LocationAuthority", label="Moscou (Russie)", same_as=moscou)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertEqual((None, None), (loc.latitude, loc.longitude))
            cnx.commit()

    def test_add_source_to_geonames_exturi(self):
        """Add 'geoname' source if not exists
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                "ExternalUri",
                label="Dunkerque (Nord, France)",
                uri="http://www.geonames.org/3020686/",
            )
            cnx.commit()
            self.assertEqual("geoname", dunkerque.source)
            self.assertEqual("3020686", dunkerque.extid)

    def test_no_source_exturi(self):
        """Do not add source on ExternalUri
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                "ExternalUri",
                label="Dunkerque (Nord, France)",
                uri="http://www.othergeoname.org/3020686/",
            )
            cnx.commit()
            self.assertIsNone(dunkerque.source)

    def test_update_source_to_geonames_exturi(self):
        """Update 'geoname' source if not exists
        """
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                "ExternalUri",
                label="Dunkerque (Nord, France)",
                source="toto",
                uri="http://www.geonames.org/3020686/dunkerque.html",
            )
            cnx.commit()
            self.assertEqual("geoname", dunkerque.source)
            self.assertEqual("3020686", dunkerque.extid)

    def test_compute_geonames_label(self):
        """A empty ExternalUri label is replaced by one computed in the hook

        """
        with self.admin_access.cnx() as cnx:
            cnx.lang = "fr"
            spt = cnx.create_entity(
                "ExternalUri", uri="http://http://www.geonames.org/498817/saint-petersburg.html"
            )
            cnx.commit()
            self.assertEqual(cnx.lang, "fr")
            self.assertEqual("Saint Petersburg", spt.label)

    def test_add_source_to_databnf_exturi(self):
        """Add 'databnf' source on ExternalUri"""
        with self.admin_access.cnx() as cnx:
            herve = cnx.create_entity(
                "ExternalUri", label="", uri="https://data.bnf.fr/fr/13517695/herve/"
            )
            cnx.commit()
            self.assertEqual("databnf", herve.source)
            self.assertEqual("13517695", herve.extid)
            hugo = cnx.create_entity(
                "ExternalUri", label="", uri="http://data.bnf.fr/11907966/victor_hugo/"
            )
            cnx.commit()
            self.assertEqual("databnf", hugo.source)
            self.assertEqual("11907966", hugo.extid)

    def test_agentinfo_from_databnf(self):
        """
        Retrieve some data from data.bnf.fr
        Trying: add an same_as relation to a databnf ExternalUri
        Expecting: retrive dates (full date) and notes
        """
        mock_method = "cubicweb_frarchives_edition.alignments.databnf.DataBnfDatabase.agent_infos"
        return_value = {
            "label": "Igor Stravinsky (1882-1971)",
            "description": "Compositeur. - Pianiste. - Chef d'orchestre",
            "dates": {
                "birthdate": {"timestamp": "1882-06-17", "precision": "d"},
                "deathdate": {"timestamp": "1971-04-06", "precision": "d"},
            },
        }
        # use deepcopy because return_value is modified during hook execution
        with mock.patch(mock_method, return_value=deepcopy(return_value)):
            with self.admin_access.client_cnx() as cnx:
                uri = "http://data.bnf.fr/12405560/igor_stravinsky/"
                url = cnx.create_entity("ExternalUri", uri=uri)
                cnx.create_entity("AgentAuthority", label="Igor Strawinsky", same_as=url)
                cnx.commit()
                agent_info = cnx.execute(
                    """Any X, D, DD WHERE X is AgentInfo,
                    X dates D,
                    X description DD,
                    X agent_info_of U, U eid {eid}""".format(
                        eid=url.eid
                    )
                ).one()
                birthdate = agent_info.dates["birthdate"]
                deathdate = agent_info.dates["deathdate"]
                self.assertEqual(
                    datetime(1882, 6, 17).date(),
                    datetime.strptime(birthdate["timestamp"], "%Y-%m-%d").date(),
                )
                self.assertEqual(
                    return_value["dates"]["birthdate"]["precision"], birthdate["precision"]
                )
                self.assertEqual(
                    datetime(1971, 4, 6).date(),
                    datetime.strptime(deathdate["timestamp"], "%Y-%m-%d").date(),
                )
                self.assertEqual(
                    return_value["dates"]["deathdate"]["precision"], deathdate["precision"]
                )
                self.assertEqual(return_value["description"], agent_info.description)
                url = cnx.find("ExternalUri", eid=url.eid).one()
                self.assertEqual(return_value["label"], url.label)

    def test_add_source_to_wikidata_exturi(self):
        """Add 'wikidata' source on ExternalUri"""
        with self.admin_access.cnx() as cnx:
            strav = cnx.create_entity(
                "ExternalUri", label="", uri="https://www.wikidata.org/wiki/Q7314"
            )
            cnx.commit()
            self.assertEqual("wikidata", strav.source)
            self.assertEqual("Q7314", strav.extid)

    def test_agentinfo_from_wikidata(self):
        """
        Retrieve some data from Wikidata.

        Trying: add an same_as relation to a Wikidata ExternalUri
        Expecting: retrive dates and notes
        """
        mock_method = "cubicweb_frarchives_edition.alignments.wikidata.WikidataDatabase.agent_infos"
        return_value = {
            "label": "Igor Stravinsky",
            "description": "pianiste et compositeur",
            "dates": {
                "birthdate": {"timestamp": "1882-06-18", "precision": "d"},
                "deathdate": {"timestamp": "1971-04-06", "precision": "d"},
            },
        }
        # use deepcopy because return_value is modified during hook execution
        with mock.patch(mock_method, return_value=deepcopy(return_value)):
            with self.admin_access.client_cnx() as cnx:
                uri = "https://www.wikidata.org/wiki/Q7314/"
                url = cnx.create_entity("ExternalUri", uri=uri)
                cnx.create_entity("AgentAuthority", label="Igor Strawinsky", same_as=url)
                cnx.commit()
                agent_info = cnx.execute(
                    """Any X, D, DD WHERE X is AgentInfo,
                    X dates D,
                    X description DD,
                    X agent_info_of U, U eid {eid}""".format(
                        eid=url.eid
                    )
                ).one()
                birthdate = agent_info.dates["birthdate"]
                deathdate = agent_info.dates["deathdate"]
                self.assertEqual(
                    datetime(1882, 6, 18).date(),
                    datetime.strptime(birthdate["timestamp"], "%Y-%m-%d").date(),
                )
                self.assertEqual(
                    return_value["dates"]["birthdate"]["precision"], birthdate["precision"]
                )
                self.assertEqual(
                    datetime(1971, 4, 6).date(),
                    datetime.strptime(deathdate["timestamp"], "%Y-%m-%d").date(),
                )
                self.assertEqual(
                    return_value["dates"]["deathdate"]["precision"], deathdate["precision"]
                )
                self.assertEqual(return_value["description"], agent_info.description)
                url = cnx.find("ExternalUri", eid=url.eid).one()
                self.assertEqual(return_value["label"], url.label)


class LeafletMapCacheHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for LeafletMapCache hooks."""

    configcls = PostgresApptestConfiguration

    @classmethod
    def init_config(cls, config):
        super(LeafletMapCacheHookTC, cls).init_config(config)
        config.set_option("consultation-base-url", "https://francearchives.fr")

    def create_findingaid(self, cnx, eadid, service):
        return cnx.create_entity(
            "FindingAid",
            name=eadid,
            stable_id="stable_id{}".format(eadid),
            eadid=eadid,
            publisher="publisher",
            did=cnx.create_entity(
                "Did", unitid="unitid{}".format(eadid), unittitle="title{}".format(eadid)
            ),
            fa_header=cnx.create_entity("FAHeader"),
            service=service,
        )

    def test_geo_map_on_create_location(self):
        with self.admin_access.cnx() as cnx:
            loc = cnx.create_entity(
                "LocationAuthority", label="example location", latitude=1.22, longitude=2.33
            )
            fa = self.create_findingaid(
                cnx, "eadid1", service=cnx.create_entity("Service", code="FRAD054", category="foo")
            )
            cnx.create_entity("Geogname", label="index location 1", index=fa, authority=loc)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            geomap_json = cnx.execute('Any V WHERE X is Caches, X name "geomap", X values V')[0][0]
            expected = [
                {
                    "count": 1,
                    "dashLabel": False,
                    "url": loc.absolute_url(),
                    "label": loc.label,
                    "eid": loc.eid,
                    "lat": loc.latitude,
                    "lng": loc.longitude,
                }
            ]
            self.assertEqual(expected, geomap_json)

    def test_geo_map_on_update_location(self):
        with self.admin_access.cnx() as cnx:
            loc = cnx.create_entity("LocationAuthority", label="example location")
            fa = self.create_findingaid(
                cnx, "eadid1", service=cnx.create_entity("Service", code="FRAD054", category="foo")
            )
            cnx.create_entity("Geogname", label="index location 1", index=fa, authority=loc)
            cnx.commit()
            geomap_json = cnx.execute('Any V WHERE X is Caches, X name "geomap", X values V')[0][0]
            self.assertEqual([], geomap_json)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            loc.cw_set(latitude=1.22, longitude=2.33)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            for instance_type in ("cms", "consultation"):
                geomap_json = cnx.execute(
                    """Any V WHERE X is Caches, X name "geomap",
                       X instance_type %(instance_type)s,
                       X values V""",
                    {"instance_type": instance_type},
                )[0][0]
                if instance_type == "cms":
                    url = loc.absolute_url()
                else:
                    url = "https://francearchives.fr/location/{}".format(loc.eid)
                expected = [
                    {
                        "count": 1,
                        "dashLabel": False,
                        "url": url,
                        "label": loc.label,
                        "eid": loc.eid,
                        "lat": loc.latitude,
                        "lng": loc.longitude,
                    }
                ]
            self.assertEqual(expected, geomap_json)

    def test_geo_map_on_delete_coordinates(self):
        with self.admin_access.cnx() as cnx:
            loc = cnx.create_entity(
                "LocationAuthority", label="example location", latitude=1.22, longitude=2.33
            )
            fa = self.create_findingaid(
                cnx, "eadid1", service=cnx.create_entity("Service", code="FRAD054", category="foo")
            )
            cnx.create_entity("Geogname", label="index location 1", index=fa, authority=loc)
            cnx.commit()
            loc.cw_set(latitude=None, longitude=None)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            geomap_json = cnx.execute('Any V WHERE X is Caches, X name "geomap", X values V')[0][0]
            self.assertEqual([], geomap_json)

    def test_geo_map_on_remove_geogname(self):
        with self.admin_access.cnx() as cnx:
            loc = cnx.create_entity(
                "LocationAuthority", label="example location", latitude=1.22, longitude=2.33
            )
            fa = self.create_findingaid(
                cnx, "eadid1", service=cnx.create_entity("Service", code="FRAD054", category="foo")
            )
            cnx.create_entity("Geogname", label="index location 1", index=fa, authority=loc)
            cnx.commit()
            geomap_json = cnx.execute('Any V WHERE X is Caches, X name "geomap", X values V')[0][0]
            self.assertTrue(geomap_json)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            # remove geoname
            loc.cw_set(reverse_authority=None)
            cnx.commit()
            loc = cnx.create_entity(
                "LocationAuthority", label="example location", latitude=1.22, longitude=2.33
            )
            geomap_json = cnx.execute('Any V WHERE X is Caches, X name "geomap", X values V')[0][0]
            self.assertFalse(geomap_json)


if __name__ == "__main__":
    import unittest

    unittest.main()

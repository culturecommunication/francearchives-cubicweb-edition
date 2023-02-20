# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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

import mock

import urllib.request
import urllib.parse
import urllib.error

from cubicweb import Binary, Unauthorized, ValidationError
from cubicweb.devtools.testlib import CubicWebTC

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa

from cubicweb_francearchives import GLOSSARY_CACHE
from cubicweb_francearchives.utils import reveal_glossary

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin
from cubicweb_frarchives_edition.alignments import DataGouvQuerier


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
            with open(self.datapath("Carte_Cadastres.csv"), "rb") as stream:
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
        Trying: create a second Service for a same 75 "dpt_code"
        Expecting: the second Service is not created.
                   We can still create a Service with no "dpt_code", but the same "code"
        """
        with self.admin_access.cnx() as cnx:
            cnx.find("Service", code="FRADO75", dpt_code="75").one()
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

    def test_service_code_lower_case(self):
        """
        Trying: create a Services with a code containing lower case caracters
        Expecting: the new code is not accepted
        """
        with self.admin_access.cnx() as cnx:
            with self.assertRaises(ValidationError):
                service = cnx.create_entity(
                    "Service",
                    category="foo1",
                    code="FRADOd_71",
                    dpt_code="75",
                    name="Service de Paris",
                )
                cnx.commit()
            cnx.rollback()
            service = cnx.create_entity(
                "Service", category="foo1", code="FRADO_71", dpt_code="75", name="Service de Paris"
            )
            cnx.commit()
            with self.assertRaises(ValidationError):
                service.cw_set(code="FRADOa71")
                cnx.commit()

    def test_service_geo(self):
        """
        Trying: create a Service with address, city and zip_code
        Expecting: service is geolocalised
        """
        return_value = [1.325116, 48.068913]
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service",
                category="Département d'Eure-et-Loir",
                name="Commune de Châteaudun",
                dpt_code="28",
                address="Place Cap de La Madeleine",
                level="level-C",
                zip_code="28200",
                city="Châteaudun",
            )
            with mock.patch.object(DataGouvQuerier, "geo_query", return_value=return_value):
                cnx.commit()
            service.cw_clear_all_caches()
            self.assertEqual(service.longitude, 1.325116)
            self.assertEqual(service.latitude, 48.068913)

    def test_service_dpt_code(self):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service",
                category="foo1",
                level="level-D",
                name="93 service",
                dpt_code="28",
                code_insee_commune="31078",
            )
            cnx.commit()
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, "28")
            cnx.commit()
            # remove dpt_code
            service.cw_set(dpt_code=None)
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, None)
            # remove code_insee_commune
            service.cw_set(code_insee_commune=None)
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, None)
            # add a new code_insee_commune
            service.cw_set(code_insee_commune="97302")
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, "973")

            service = cnx.create_entity(
                "Service",
                category="foo",
                level="level-D",
                code_insee_commune="31078",
                name="93 service 2",
            )
            cnx.commit()
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, "31")
            service.cw_set(dpt_code="21")
            cnx.commit()
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, "21")

    def test_corse_service_dpt_code(self):
        """Test Corse 2A particular case"""
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service",
                category="foo1",
                level="level-D",
                name="Ajaccio; (préfecture) service",
                code_insee_commune="2A004",
            )
            cnx.commit()
            service.cw_clear_all_caches()
            self.assertEqual(service.dpt_code, "20")
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


class CircularHookTC(S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
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


class GlossaryHooksTC(FrACubicConfigMixIn, CubicWebTC):
    def setUp(self):
        GLOSSARY_CACHE[:] = []
        super(GlossaryHooksTC, self).setUp()
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity(
                "GlossaryTerm",
                term="Dr Who",
                short_description="doctor Who?",
                description="doctor Who?",
            )
            cnx.commit()

    def test_glossary_reveal(self):
        with self.new_access("anon").repo_cnx() as cnx:
            term = cnx.find("GlossaryTerm", term="Dr Who").one()
            text = "Who is the best dr Who ever?"
            expected = f"""Who is the best <a data-bs-content="doctor Who?" data-bs-toggle="popover" class="glossary-term" data-bs-placement="auto" data-bs-trigger="hover focus" data-bs-html="true" href="http://testing.fr/cubicweb/glossaire#{term.eid}" target="_blank">dr Who
<i class="fa fa-question"></i>
</a> ever?"""  # noqa
            got = reveal_glossary(cnx, text)
            self.assertEqual(got, expected)
            cached = dict(GLOSSARY_CACHE)["dr who"]
            expected = f"""<a data-bs-content="doctor Who?" data-bs-toggle="popover" class="glossary-term" data-bs-placement="auto" data-bs-trigger="hover focus" data-bs-html="true" href="http://testing.fr/cubicweb/glossaire#{term.eid}" target="_blank">{{term}}
<i class="fa fa-question"></i>
</a>"""  # noqa
            self.assertEqual(cached, expected)

    def test_update_glossary_description(self):
        with self.admin_access.repo_cnx() as cnx:
            term = cnx.find("GlossaryTerm", term="Dr Who").one()
            with self.assertRaises(ValidationError):
                term.cw_set(description="")


class SiteLinkHooksTC(FrACubicConfigMixIn, CubicWebTC):
    def test_sitelinktranlsations(self):
        with self.admin_access.repo_cnx() as cnx:
            link = cnx.create_entity(
                "SiteLink",
                link="@doc",
                label_fr="@doc",
                description_fr="@doc",
                order=0,
                context="main_menu_links",
            )
            cnx.commit()
            link = cnx.find("SiteLink", eid=link).one()
            for attr in ("label", "description"):
                for lang in ("en", "es", "de"):
                    self.assertEqual(
                        link.printable_value("label_fr"),
                        link.printable_value("{}_{}".format(attr, lang)),
                    )


class OnFrontPageHooksTC(FrACubicConfigMixIn, CubicWebTC):
    def test_on_homepage_basecontent_mandatory_fields(self):
        """test OnFrontPageHook: add an entity to HP

        Trying: add a BaseContent to HP
        Expecting: header and on_homepage_order attributes are mandatory
        """
        with self.admin_access.repo_cnx() as cnx:
            with self.assertRaises(ValidationError):
                cnx.create_entity("BaseContent", title="article", on_homepage="onhp_hp")
            with self.assertRaises(ValidationError):
                cnx.create_entity(
                    "BaseContent", title="article", on_homepage="onhp_hp", header="header"
                )
            with self.assertRaises(ValidationError):
                cnx.create_entity(
                    "BaseContent", title="article", on_homepage="onhp_hp", on_homepage_order=0
                )

    def test_on_homepage_section_mandatory_fields(self):
        """test OnFrontPageHook: add an entity to HP archivistes

        Trying: add a Section to HP archivistes
        Expecting: header and on_homepage_order attributes are mandatory
        """
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.create_entity("Section", title="Section")
            cnx.commit()
            section = cnx.find("Section", eid=section.eid).one()
            self.assertIsNone(section.on_homepage_order)
            with self.assertRaises(ValidationError):
                section.cw_set(on_homepage="onhp_arch")
            section.cw_clear_all_caches()
            self.assertIsNone(section.on_homepage_order)
            with self.assertRaises(ValidationError):
                section.cw_set(on_homepage="onhp_arch", on_homepage_order=0)
            section.cw_clear_all_caches()
            self.assertIsNone(section.on_homepage_order)
            with self.assertRaises(ValidationError):
                section.cw_set(on_homepage="onhp_arch", header="header")
            cnx.rollback()
            section.cw_clear_all_caches()
            section.cw_set(on_homepage="onhp_arch", on_homepage_order=0, header="header")
            cnx.commit()

    def test_on_homepage_order(self):
        """test OnFrontPageHook: remove an entity from HP

        Trying: remove a BaseContent from HP
        Expecting: on_homepage_order attribute is set to None
        """
        with self.admin_access.repo_cnx() as cnx:
            article = cnx.create_entity(
                "BaseContent",
                title="article",
                header="header",
                on_homepage="onhp_hp",
                on_homepage_order=0,
            )
            cnx.commit()
            article.cw_clear_all_caches()
            article.cw_set(on_homepage="")
            cnx.commit()
            article.cw_clear_all_caches()
            self.assertEqual(None, article.on_homepage_order)


class CommemorationItemHookTC(S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
    """Tests for CommemorationItem hooks."""

    @classmethod
    def init_config(cls, config):
        super(CommemorationItemHookTC, cls).init_config(config)
        config.set_option("consultation-base-url", "https://francearchives.fr")

    def setup_database(self):
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("Section", title="Pages d'histoire", name="pages_histoire")
            cnx.commit()
        super(CommemorationItemHookTC, self).setup_database()

    def test_create_commemoitem_in_section(self):
        """
        Trying: create a CommemorationItem
        Expecting: CommemorationItem must be stored in "pages_histoire" Section
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "CommemorationItem",
                title="Commemoration 2045",
                content="Commemoration 2045",
                commemoration_year=1945,
                start_year=2045,
            )
            cnx.commit()
            commemo = cnx.find("CommemorationItem").one()
            section = cnx.find("Section", name="pages_histoire").one()
            self.assertEqual(commemo.reverse_children[0].eid, section.eid)

    def test_commemoitem_remove_from_section(self):
        """
        Trying: create a CommemorationItem, remove it from a Section
        Expecting: CommemorationItem must not be store in a Section
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "CommemorationItem",
                title="Commemoration 2045",
                content="Commemoration 2045",
                commemoration_year=1945,
                start_year=2045,
            )
            cnx.commit()
            commemo = cnx.find("CommemorationItem").one()
            commemo.cw_set(reverse_children=None)
            cnx.commit()
            commemo.cw_clear_all_caches()
            self.assertFalse(commemo.reverse_children)


if __name__ == "__main__":
    import unittest

    unittest.main()

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
from copy import deepcopy
import mock

from cubicweb import Binary
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin, PostgresTextMixin
from cubicweb_frarchives_edition import get_samesas_history

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa


class FileHookTests(PostgresTextMixin, S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
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
            self.assertTrue(self.fileExists(fpath))
            fobj1 = cnx.create_entity(
                "File", data=Binary(b"data"), data_name="data", reverse_image_file=image
            )
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertEqual(fpath, fpath1)
            self.assertTrue(self.fileExists(fpath))

    def test_delete_image_file(self):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity("File", data=Binary(b"data"), data_name="data")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            cnx.commit()
            fpath = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid})[0][
                0
            ].getvalue()
            self.assertTrue(self.fileExists(fpath))
            fobj1 = cnx.create_entity("File", data=Binary(b"data1"), data_name="data1")
            image.cw_set(image_file=fobj1)
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertTrue(self.fileExists(fpath1))
            self.assertFalse(self.fileExists(fpath))

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
            self.assertTrue(self.fileExists(fpath))
            cnx.execute("DELETE File X WHERE X eid %(e)s", {"e": fobj.eid})
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            cnx.find("File", eid=fobj1.eid).one()
            fpath1 = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertTrue(self.fileExists(fpath1))


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
            loc = cnx.create_entity(
                "LocationAuthority",
                label="Moscou (Russie)",
            )
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
        """Add 'geoname' source if not exists"""
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
        """Add source on ExternalUri"""
        with self.admin_access.cnx() as cnx:
            dunkerque = cnx.create_entity(
                "ExternalUri",
                label="Dunkerque (Nord, France)",
                uri="http://www.othergeoname.org/3020686/",
            )
            cnx.commit()
            self.assertEqual(dunkerque.source, "www.othergeoname.org")

    def test_update_source_to_geonames_exturi(self):
        """Update 'geoname' source if not exists"""
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
        """A empty ExternalUri label is replaced by one computed in the hook"""
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


class BaseContentHookTC(FrACubicConfigMixIn, CubicWebTC):
    """Tests for BaseContent hooks."""

    configcls = PostgresApptestConfiguration

    def test_basecontent_summary(self):
        with self.admin_access.repo_cnx() as cnx:
            content = """
            <h1>titre 0</h1><p>text</p>
            <h1>titre <em>1</em></h1><p>text</p>
              <h2>titre 1.1</h2><p>text</p>
              <h2>titre 1.2</h2><p>text</p>
                <h3>titre 1.2.1</h3><p>text</p>
                  <h4>titre 1.2.1.1</h4><p>text</p>
                  <h4>titre 1.2.1.2</h4><p>text<h5>titre 1.2.1.2.1</h5></p>
              <h2>titre 1.3</h3><p>text</p>
            <h1>titre 2</h1><p>text</p>
              <h2>titre 2.1</h2><p>text</p>
                 <h3>titre 2.1.1</h3><p>text</p>
                 <h3>titre 2.1.2</h3><p>text</p>
              <h2>titre 2.2</h2><p>text</p>
            <h1>titre 3</h1><p>text</p>
            <h3>titre 3.1.1</h3><p>text</p>
            """
            article = cnx.create_entity("BaseContent", title="article", content=content)
            cnx.commit()
            article = cnx.find("BaseContent", eid=article.eid).one()
            # self.assertFalse(article.summary)
            # self.assertEqual(content, article.content)
            article.cw_set(summary_policy="summary_headers_6")
            cnx.commit()
            # self.assertEqual(expected_content, article.content)
            article = cnx.find("BaseContent", eid=article.eid).one()
            expected_summary = """<ul class="toc"><li><a href="#h1_faa036576b46d00ef12b4b3bb7ce60057778f7c40">titre 0</a></li><li><a href="#h1_3c6a6e826c9eab5c5664dfe165b12f2a74c9f01c1">titre 1</a><ul><li><a href="#h2_5caf4b8a5d2840462f1b2507d51a7f93ebf6273c2">titre 1.1</a></li><li><a href="#h2_32e0af574db150b0fb054c449cbb6cd4093c76373">titre 1.2</a><ul><li><a href="#h3_ffbe23d3d38eebf40f7bb405af19726c36127cbf4">titre 1.2.1</a><ul><li><a href="#h4_35616c6badde6e8f36762dd7ec986c564ae68f5b5">titre 1.2.1.1</a></li><li><a href="#h4_36f499b3464ed60c129eb60a975e5b0362a689b16">titre 1.2.1.2</a><ul><li><a href="#h5_33b11befcefad75ca9602e0fc6c12d9a47d44f4c7">titre 1.2.1.2.1</a></li></ul></li></ul></li></ul></li><li><a href="#h2_6f5a4d5c9b4878a89628406b85331d565d2d9a008">titre 1.3</a></li></ul></li><li><a href="#h1_4b57f04302a12c751ef9f2722f31fc18062d349f9">titre 2</a><ul><li><a href="#h2_3f5d8ca99f611ac96293ca83d3c98292165e2faf10">titre 2.1</a><ul><li><a href="#h3_814fc158ecc6b9c7514920470acffa2cadcdc27911">titre 2.1.1</a></li><li><a href="#h3_be6845c32e1c0bd3f6a778e34d146c58f52daa0012">titre 2.1.2</a></li></ul></li><li><a href="#h2_a0671650e27ddd018be1ed959ba41cc0c1352aab13">titre 2.2</a></li></ul></li><li><a href="#h1_56b8eda9341ffec3ab23b2d2c4335fe668f6fc0714">titre 3</a><ul><li><ul><li><a href="#h3_b5323ccf25ee5acd553180a7e0b72d09d5a7acc715">titre 3.1.1</a></li></ul></li></ul></li></ul>"""  # noqa
            self.assertEqual(expected_summary, article.summary)
            article.cw_set(summary_policy="summary_headers_2")
            cnx.commit()
            article = cnx.find("BaseContent", eid=article.eid).one()
            expected_summary = """<ul class="toc"><li><a href="#h1_faa036576b46d00ef12b4b3bb7ce60057778f7c40">titre 0</a></li><li><a href="#h1_3c6a6e826c9eab5c5664dfe165b12f2a74c9f01c1">titre 1</a><ul><li><a href="#h2_5caf4b8a5d2840462f1b2507d51a7f93ebf6273c2">titre 1.1</a></li><li><a href="#h2_32e0af574db150b0fb054c449cbb6cd4093c76373">titre 1.2</a></li><li><a href="#h2_6f5a4d5c9b4878a89628406b85331d565d2d9a008">titre 1.3</a></li></ul></li><li><a href="#h1_4b57f04302a12c751ef9f2722f31fc18062d349f9">titre 2</a><ul><li><a href="#h2_3f5d8ca99f611ac96293ca83d3c98292165e2faf10">titre 2.1</a></li><li><a href="#h2_a0671650e27ddd018be1ed959ba41cc0c1352aab13">titre 2.2</a></li></ul></li><li><a href="#h1_56b8eda9341ffec3ab23b2d2c4335fe668f6fc0714">titre 3</a></li></ul>"""  # noqa
            self.assertEqual(expected_summary, article.summary)

    def test_all_basecontenttranslation_summary(self):
        """
        Trying: generate a toc for a BaseContent
        Expecting: all BaseContent translations have the toc generated
        """
        with self.admin_access.repo_cnx() as cnx:
            content = """
            <h1>titre 1 {lang}</h1><p>{lang}</p>
              <h2>titre 2 {lang}</h2><p>{lang}</p>"""
            article = cnx.create_entity(
                "BaseContent", title="article", content=content.format(lang="fr")
            )
            article.cw_set(summary_policy="summary_headers_2")
            cnx.commit()
            expected_summary = """<ul class="toc"><li><a href="#h1_12868415d909aeff2cc18c45fa0e45aa4da4719d0">titre 1 fr</a><ul><li><a href="#h2_c3a140c54e2db560dd43637fa9bcc9d74aacba9c1">titre 2 fr</a></li></ul></li></ul>"""  # noqa
            article = cnx.find("BaseContent", eid=article.eid).one()
            self.assertEqual(expected_summary, article.summary)
            # create translations
            for lang in ("en", "de", "es"):
                cnx.create_entity(
                    "BaseContentTranslation",
                    title="{} article".format(lang),
                    language=lang,
                    content=content.format(lang=lang),
                    translation_of=article,
                )
            cnx.commit()
            for tr in cnx.find("BaseContentTranslation").entities():
                self.assertIn(">titre 1 {lang}</a>".format(lang=tr.language), tr.summary)
                self.assertIn(">titre 2 {lang}</a>".format(lang=tr.language), tr.summary)
            article.cw_set(summary_policy="no_summary")
            cnx.commit()
            for tr in cnx.find("BaseContentTranslation").entities():
                self.assertFalse(tr.summary)


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

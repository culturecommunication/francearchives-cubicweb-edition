# -*- coding: utf-8 -*-
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


# standard library imports
import os
import json

# third party imports
import mock

# CubicWeb specific imports
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

# library specific imports
from cubicweb_francearchives.views import STRING_SEP
from cubicweb_frarchives_edition.alignments import get_externaluri_data
from cubicweb_frarchives_edition.alignments.align import AgentAligner, AgentRecord
from cubicweb_frarchives_edition.alignments.databnf import DataBnfDatabase
from cubicweb_frarchives_edition.alignments.wikidata import WikidataDatabase

from cubicweb_frarchives_edition.alignments import DATABNF_RE, DATABNF_ARK_RE

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa


class AlignementUtilsTC(CubicWebTC):
    """Agent alignements utils tests class."""

    def test_databnf_re(self):
        uri = "https://data.bnf.fr/fr/11894985/albert_camus/"
        self.assertEqual("11894985", DATABNF_RE.search(uri).group(1))
        uri = "https://data.bnf.fr/11907966/victor_hugo"
        self.assertEqual("11907966", DATABNF_RE.search(uri).group(1))
        uri = "https://data.bnf.fr/11907966"
        self.assertFalse(DATABNF_RE.search(uri))
        uri = "https://data.bnf.fr/ark:/12148/cb13005429m"
        self.assertFalse(DATABNF_RE.search(uri))

    def test_databnf_ark_re(self):
        uri = "https://data.bnf.fr/fr/11894985/albert_camus/"
        self.assertFalse(DATABNF_ARK_RE.search(uri))
        uri = "https://data.bnf.fr/11907966/victor_hugo"
        self.assertFalse(DATABNF_ARK_RE.search(uri))
        uri = "https://data.bnf.fr/11907966"
        self.assertFalse(DATABNF_ARK_RE.search(uri))
        uri = "https://data.bnf.fr/ark:/12148/cb11909252t"
        self.assertEqual("11909252", DATABNF_ARK_RE.search(uri).group(1))
        uri = "https://data.bnf.fr/fr/ark:/12148/cb13005429m"
        self.assertEqual("13005429", DATABNF_ARK_RE.search(uri).group(1))

    def test_uri_source(self):
        for source, extid, uri in (
            ("databnf", "11907966", "https://data.bnf.fr/11907966/victor_hugo"),
            ("databnf", "11907966", "https://data.bnf.fr/en/11907966/victor_hugo"),
            ("databnf", "13005429", "https://data.bnf.fr/fr/ark:/12148/cb13005429m"),
            ("databnf", "11921467", "http://data.bnf.fr/ark:/12148/cb11921467w"),
            ("databnf", "13946072", "https://data.bnf.fr/ark:/12148/cb139460728"),
            ("databnf", "13926902", "https://data.bnf.fr/ark:/12148/cb139269025"),
            ("databnf", "39269025", "https://data.bnf.fr/ark:/12148/cb39269025q"),
            ("wikidata", "Q158768", "https://www.wikidata.org/wiki/Q158768"),
            ("wikidata", "Q131412", "http://www.wikidata.org/wiki/Q131412"),
            ("fr.wikipedia.org", None, "https://fr.wikipedia.org/wiki/Edmond_Maire"),
            ("geoname", "3033123", "https://www.geonames.org/3033123"),
            ("geoname", "2986302", "http://www.geonames.org/2986302"),
        ):
            got_source, got_extid = get_externaluri_data(uri)
            self.assertEqual(source, got_source)
            self.assertEqual(extid, got_extid)


class AlignmentTC(FrACubicConfigMixIn, CubicWebTC):
    """Manual alignment test cases base class."""

    configcls = PostgresApptestConfiguration


class WikidataTC(AlignmentTC):
    """Wikidata test cases."""

    def test_agentinfo_from_wikidata(self):
        """Retrieve some data from Wikidata.

        Trying: retrieve and process mock data
        Expecting: data is processed correctly
        """
        return_value = (
            (
                "Igor Stravinsky",
                "+1882-06-18T00:00:00Z",
                "+1971-04-06T00:00:00Z",
                "11",
                "11",
                "pianiste et compositeur",
            ),
        )
        with mock.patch.object(WikidataDatabase, "agent_query", return_value=return_value):
            wikidata = WikidataDatabase()
            data_infos = wikidata.agent_infos("Q7314")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual("1882-06-18", birthdate["timestamp"])
            self.assertEqual("d", birthdate["precision"])
            self.assertTrue(birthdate["isiso"])
            deathdate = dates["deathdate"]
            self.assertEqual("1971-04-06", deathdate["timestamp"])
            self.assertEqual("d", deathdate["precision"])
            self.assertTrue(deathdate["isiso"])
            self.assertEqual("Igor Stravinsky", data_infos["label"])
            self.assertEqual("pianiste et compositeur", data_infos["description"])

    def test_agentinfo_from_wikidata_bc(self):
        """Retrieve some data from Wikidata.

        Trying: birthdate and deathdate are BC
        Expecting: timestamp is unsigned and isbc flag is True
        """
        return_value = (
            (
                "Jules César",
                "-0099-07-01T00:00:00Z",
                "-0043-03-13T00:00:00Z",
                "10",
                "11",
                "homme politique et général romain du Ier siècle avant J-C.",
            ),
        )
        with mock.patch.object(WikidataDatabase, "agent_query", return_value=return_value):
            wikidata = WikidataDatabase()
            data_infos = wikidata.agent_infos("Q1048")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual("0099-07-01", birthdate["timestamp"])
            self.assertEqual("m", birthdate["precision"])
            self.assertTrue(birthdate["isiso"])
            self.assertTrue(birthdate["isbc"])
            deathdate = dates["deathdate"]
            self.assertEqual("0043-03-13", deathdate["timestamp"])
            self.assertEqual("d", deathdate["precision"])
            self.assertTrue(deathdate["isiso"])
            self.assertTrue(deathdate["isbc"])
            self.assertEqual("Jules César", data_infos["label"])
            self.assertEqual(
                "homme politique et général romain du Ier siècle avant J-C.",
                data_infos["description"],
            )

    def test_agentinfo_from_wikidata_year(self):
        """Retrieve some data from Wikidata.

        Trying: birthdate has precision '9'
        Expecting: birthdate has precision 'y'(ear)
        """
        return_value = (
            (
                "François Rabelais",
                "+1494-01-01T00:00:00Z",
                "+1553-04-19T00:00:00Z",
                "9",
                "11",
                "auteur français du 16e siècle",
            ),
        )
        with mock.patch.object(WikidataDatabase, "agent_query", return_value=return_value):
            wikidata = WikidataDatabase()
            data_infos = wikidata.agent_infos("Q131018")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual("1494-01-01", birthdate["timestamp"])
            self.assertEqual("y", birthdate["precision"])
            self.assertTrue(birthdate["isiso"])
            deathdate = dates["deathdate"]
            self.assertEqual("1553-04-19", deathdate["timestamp"])
            self.assertEqual("d", deathdate["precision"])
            self.assertTrue(deathdate["isiso"])
            self.assertEqual("François Rabelais", data_infos["label"])
            self.assertEqual("auteur français du 16e siècle", data_infos["description"])


class DataBnfTC(AlignmentTC):
    """Data.bnf.fr test cases."""

    def test_agentinfo_from_databnf_date(self):
        """
        Retrieve some data from data.bnf.fr
        Trying: add an same_as relation to a databnf ExternalUri
        Expecting: retrive dates (full date) and notes
        """
        return_value = (
            (
                "Igor Stravinsky (1882-1971)",
                None,
                "1882-06-17",
                None,
                "1971-04-06",
                "Compositeur. - Pianiste. - Chef d'orchestre",
            ),
        )
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("12405560")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual("1882-06-17", birthdate["timestamp"])
            self.assertEqual("d", birthdate["precision"])
            self.assertFalse(birthdate["isiso"])
            deathdate = dates["deathdate"]
            self.assertEqual("1971-04-06", deathdate["timestamp"])
            self.assertEqual("d", deathdate["precision"])
            self.assertFalse(deathdate["isiso"])
            self.assertEqual("Igor Stravinsky (1882-1971)", data_infos["label"])
            self.assertEqual(
                "Compositeur. - Pianiste. - Chef d'orchestre", data_infos["description"]
            )

    def test_agentinfo_description_from_databnf_date(self):
        """
        Retrieve some data from data.bnf.fr
        Trying: add an same_as relation to a databnf ExternalUri
        Expecting: retrive dates all notes
        """
        descriptions = (
            "A utilisé le pseudonyme de Morland dans la Résistance",
            "Avocat et homme politique. - Quatrième président de la Ve République (1981-1995)",
        )
        return_value = (
            (
                "François Mitterrand (1916-1996)",
                None,
                "1916-10-26",
                None,
                "1996-01-08",
                descriptions[0],
            ),
            (
                "François Mitterrand (1916-1996)",
                None,
                "1916-10-26",
                None,
                "1996-01-08",
                descriptions[1],
            ),
        )
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("11916320")
            self.assertCountEqual(descriptions, data_infos["description"].split(STRING_SEP))

    def test_agentinfo_from_databnf_year(self):
        """
        Retrieve some data from data.bnf.fr
        Trying: add an same_as relation to a databnf ExternalUri
        Expecting: retrive year and notes
        """
        return_value = (
            (
                "Hervé (saint, 05..-0575?)",
                "05..",
                None,
                "0575",
                None,
                "Barde d'origine galloise. - Fonda un ermitage en Bretagne",
            ),
        )
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("14978856")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual(birthdate["timestamp"], "05..")
            self.assertFalse(birthdate["isdate"])
            self.assertFalse(birthdate["isiso"])
            deathdate = dates["deathdate"]
            self.assertEqual(deathdate["timestamp"], "0575-01-01")
            self.assertTrue(deathdate["isdate"])
            self.assertFalse(deathdate["isiso"])
            self.assertEqual(deathdate["precision"], "y")

    def test_agentinfo_from_databnf_bc(self):
        """Test processing BCE dates.

        Trying: retrieve BCE dates from data.bnf.fr
        Expecting: valid date as timestamp and marked as BCE
        """
        return_value = (
            (
                "Sénèque (0004 av. J.-C.-0065)",
                None,
                "- 4",
                None,
                "65",
                (
                    "Philosophe stoïcien. - Auteur de tragédies. - Fils de : "
                    "Sénèque le rhéteur (0060 av. J.-C.?-0039?)"
                ),
            ),
        )
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("11887555")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual(birthdate["timestamp"], "0004-01-01")
            self.assertTrue(birthdate["isdate"])
            self.assertTrue(birthdate["isbc"])
            self.assertFalse(birthdate["isiso"])
            self.assertEqual(birthdate["precision"], "y")
            deathdate = dates["deathdate"]
            self.assertEqual(deathdate["timestamp"], "0065-01-01")
            self.assertFalse(deathdate["isbc"])
            self.assertFalse(deathdate["isiso"])

    def test_agentinfo_one_date_from_databnf_date(self):
        """
        Retrieve some data from data.bnf.fr
        Trying: add an same_as relation to a databnf ExternalUri
        Expecting: only the start date exist
        """
        return_value = (("Alain Jupp\xe9", 1945, None, None, None, "Inspecteur des finances"),)
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("11909252")
            dates = json.loads(data_infos["dates"])
            birthdate = dates["birthdate"]
            self.assertEqual("1945-01-01", birthdate["timestamp"])
            self.assertEqual("y", birthdate["precision"])
            self.assertFalse(birthdate["isiso"])
            self.assertIsNone(dates.get("deathdate"))
            self.assertEqual("Alain Juppé", data_infos["label"])
            self.assertEqual("Inspecteur des finances", data_infos["description"])

    def test_agentinfo_cast(self):
        """Test retrieving information from data.bnf.fr.

        Trying: using int
        Expecting: no birthdate is returned and deathdate is of the proper type (str)
        despite failing date evaluation (no dates > 9999 allowed in datetime)
        """
        return_value = (("Max Mustermann", None, None, 10000, None, ""),)
        with mock.patch.object(DataBnfDatabase, "author_query", return_value=return_value):
            databnf = DataBnfDatabase()
            data_infos = databnf.agent_infos("1234567890")
            dates = json.loads(data_infos["dates"])
            self.assertIsNone(dates.get("birthdate"))
            self.assertEqual("10000", dates["deathdate"]["timestamp"])


class AgentAlignerTC(AlignmentTC):
    """Wikidata and data.bnf.fr test cases (automatic)."""

    configcls = PostgresApptestConfiguration
    # TODO remove test_isdate and test_isbc as soon as
    # manual alignment and automatic alignment behaviour is
    # the same and test compute_dates function instead of
    # process_alignment (Wikidata)

    def test_process_csv(self):
        """Test processing CSV file.

        Trying: processing CSV file
        Expecting: corresponding alignments
        """
        with self.admin_access.cnx() as cnx:
            file_name = os.path.join(self.datapath(), "agent-alignments.csv")
            aligner = AgentAligner(cnx)
            with open(file_name) as fp:
                expected = AgentRecord(
                    {
                        "eid": "19034398",
                        "date naissance": "+1600-01-01T00:00:00Z",
                        "precision date naissance": "9",
                        "date mort": "+1674-01-01T00:00:00Z",
                        "precision date mort": "9",
                        "description": "Marin",
                        "uri": "https://www.wikidata.org/wiki/Q646986",
                        "source": "wikidata",
                        "confidence": "0.9",
                        "extlabel": "Marin le Roy de Gomberville",
                    }
                )
                alignments = aligner.process_csv(fp)
                self.assertEqual(1, len(alignments))
                actual = alignments[("19034398", "https://www.wikidata.org/wiki/Q646986")]
                self.assertEqual(actual.autheid, expected.autheid)
                self.assertEqual(actual.date_birth, expected.date_birth)
                self.assertEqual(actual.date_death, expected.date_death)
                self.assertEqual(actual.description, expected.description)
                self.assertEqual(actual.exturi, expected.exturi),
                self.assertEqual(actual.source, expected.source)
                self.assertEqual(actual.confidence, expected.confidence)
                self.assertEqual(actual.extlabel, expected.extlabel)

    def test_process_alignments_existing_externaluris(self):
        """Test updating database.

        Trying: existing ExternalUris
        Expecting: ExternalUri is kept
        """
        with self.admin_access.cnx() as cnx:
            external_uri = cnx.create_entity(
                "ExternalUri",
                source="wikidata",
                uri="https://www.wikidata.org/wiki/Q646986",
                label="Marin le Roy de Gomberville",
            )
            cnx.commit()
            agent_authority = cnx.create_entity("AgentAuthority")
            cnx.commit()
            record = AgentRecord(
                {
                    "eid": str(agent_authority.eid),
                    "date naissance": "+1600-01-01T00:00:00Z",
                    "precision date naissance": "9",
                    "date mort": "+1674-01-01T00:00:00Z",
                    "precision date mort": "9",
                    "description": "Marin",
                    "uri": "https://www.wikidata.org/wiki/Q646986",
                    "source": "wikidata",
                    "confidence": "0.9",
                    "external label": "Marin le Roy de Gomberville",
                }
            )
            alignments = {(agent_authority.eid, "https://www.wikidata.org/wiki/Q646986"): record}
            dates = {
                "birthdate": {
                    "timestamp": "1600-01-01",
                    "isbc": False,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
                "deathdate": {
                    "timestamp": "1674-01-01",
                    "isbc": False,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
            }
            aligner = AgentAligner(cnx)
            aligner.process_alignments(alignments)
            agent_info = cnx.execute(
                "Any X WHERE X is AgentInfo, X agent_info_of %(eid)s", {"eid": external_uri.eid}
            ).one()
            self.assertEqual(agent_info.dates, dates)
            self.assertEqual(agent_info.description, record.description)
            self.assertEqual(external_uri.label, record.extlabel)

    def test_process_alignments_nonexisting_externaluris(self):
        """Test updating database.

        Trying: nonexisting ExternalUris
        Expecting: new ExternalUris and AgentInfos are created
        """
        with self.admin_access.cnx() as cnx:
            self.assertFalse(cnx.find("ExternalUri"))
            agent_authority = cnx.create_entity("AgentAuthority")
            cnx.commit()
            record = AgentRecord(
                {
                    "eid": str(agent_authority.eid),
                    "date naissance": "+1600-01-01T00:00:00Z",
                    "precision date naissance": "9",
                    "date mort": "+1674-01-01T00:00:00Z",
                    "precision date mort": "9",
                    "description": "Marin",
                    "uri": "https://www.wikidata.org/wiki/Q646986",
                    "source": "wikidata",
                    "confidence": "0.9",
                    "external label": "Marin le Roy de Gomberville",
                }
            )
            alignments = {(agent_authority.eid, "https://www.wikidata.org/wiki/Q646986"): record}
            dates = {
                "birthdate": {
                    "timestamp": "1600-01-01",
                    "isbc": False,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
                "deathdate": {
                    "timestamp": "1674-01-01",
                    "isbc": False,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
            }
            aligner = AgentAligner(cnx)
            aligner.process_alignments(alignments)
            agent_info = cnx.find("AgentInfo").one()
            self.assertEqual(agent_info.dates, dates)
            self.assertEqual(agent_info.description, record.description)
            external_uri = cnx.find("ExternalUri").one()
            self.assertEqual(external_uri.label, record.extlabel)

    def test_process_date_wikidata(self):
        """Test processing date.

        Trying: source is Wikidata
        Expecting: is successfully processed
        """
        with self.admin_access.cnx() as cnx:
            aligner = AgentAligner(cnx)
            self.assertEqual(
                aligner._process_date("+1600-01-01T00:00:00Z", "9", "wikidata"),
                {
                    "timestamp": "1600-01-01",
                    "isbc": False,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
            )

    def test_process_date_databnf(self):
        """Test processing date.

        Trying: source is data.bnf.fr
        Expecting: is successfully processed
        """
        with self.admin_access.cnx() as cnx:
            aligner = AgentAligner(cnx)
            self.assertEqual(
                aligner._process_date("05..", "", "databnf"),
                {
                    "timestamp": "05..",
                    "isbc": False,
                    "isdate": False,
                    "precision": "d",
                    "isiso": False,
                },
            )

    def test_isbc(self):
        """Test updating database.

        Trying: date is Before Christ
        Expecting: timestamp is formatted date and it is flagged as Before Christ
        """
        with self.admin_access.cnx() as cnx:
            agent_authority = cnx.create_entity("AgentAuthority")
            cnx.commit()
            record = AgentRecord(
                {
                    "eid": str(agent_authority.eid),
                    "date naissance": "-0400-01-01T00:00:00Z",
                    "precision date naissance": "9",
                    "date mort": "+0065-04-12T00:00:00Z",
                    "precision date mort": "11",
                    "description": "philosophe stoïcien, dramaturge et homme d'État romain",
                    "uri": "https://www.wikidata.org/wiki/Q2054",
                    "source": "wikidata",
                    "confidence": "1.0",
                }
            )
            alignments = {(agent_authority.eid, "https://www.wikidata.org/wiki/Q2054"): record}
            dates = {
                "birthdate": {
                    "timestamp": "0400-01-01",
                    "isbc": True,
                    "isdate": True,
                    "precision": "y",
                    "isiso": True,
                },
                "deathdate": {
                    "timestamp": "0065-04-12",
                    "isbc": False,
                    "isdate": True,
                    "precision": "d",
                    "isiso": True,
                },
            }
            aligner = AgentAligner(cnx)
            aligner.process_alignments(alignments)
            agent_info = cnx.find("AgentInfo").one()
            self.assertEqual(agent_info.dates, dates)

    def test_isdate(self):
        """Test updating database.

        Trying: date is not valid date format
        Expecting: date is not retrieved
        """
        with self.admin_access.cnx() as cnx:
            agent_authority = cnx.create_entity("AgentAuthority")
            cnx.commit()
            record = AgentRecord(
                {
                    "eid": str(agent_authority.eid),
                    "date naissance": "0061",
                    "precision date naissance": "11",
                    "date mort": "0114",
                    "precision date mort": "11",
                    "description": "orateur et homme politique romain",
                    "uri": "https://www.wikidata.org/wiki/Q168707",
                    "source": "wikidata",
                    "confidence": "1.0",
                }
            )
            alignments = {(agent_authority.eid, "https://www.wikidata.org/wiki/Q168707"): record}
            aligner = AgentAligner(cnx)
            aligner.process_alignments(alignments)
            agent_info = cnx.find("AgentInfo").one()
            self.assertEqual(agent_info.dates, {})

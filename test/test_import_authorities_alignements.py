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
import csv
import os

# CubicWeb specific imports
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb_francearchives.testutils import PostgresTextMixin

# library specific imports
from cubicweb_frarchives_edition.alignments import get_externaluri_data
from cubicweb_frarchives_edition.alignments.authorities_align import (
    AgentImportAligner,
    AgentImportRecord,
    SubjectImportAligner,
)

from pgfixtures import setup_module, teardown_module  # noqa


class AgentimportAlignerTC(PostgresTextMixin, CubicWebTC):
    """Import alignment test cases (automatic)."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        super(AgentimportAlignerTC, self).setup_database()
        self.eids_map = {}
        with self.admin_access.cnx() as cnx:
            filepath = os.path.join(self.datapath(), "agents-import-alignments.csv")
            with open(filepath) as stream:
                reader = csv.DictReader(stream, delimiter="\t")
                for i, row in enumerate(reader):
                    record = AgentImportRecord(row)
                    if record.autheid not in self.eids_map:
                        agent = cnx.create_entity(
                            "AgentAuthority",
                            label=record.pnialabel,
                            reverse_authority=cnx.create_entity(
                                "AgentName", type=record.indextype, label=record.agentnamelabel
                            ),
                        )
                        self.eids_map[record.autheid] = agent.eid
                    if record.keep == "no":
                        agent = cnx.find("AgentAuthority", eid=self.eids_map[record.autheid]).one()
                        agent.cw_set(
                            same_as=cnx.create_entity(
                                "ExternalUri",
                                source=get_externaluri_data(record.externaluri)[0],
                                label="",
                                uri=record.externaluri,
                            )
                        )
            cnx.commit()

    def test_process_alignements(self):
        """Test import agents alignments form CSV file.

        Trying: process CSV file and import new alignements
        Expecting: new alignments are set on three Agents and one alignement is removed
        """
        with self.admin_access.cnx() as cnx:
            aligner = AgentImportAligner(cnx)
            existing_alignments = set()
            reverse_eids_map = {str(new): old for old, new in self.eids_map.items()}
            for autheid, uri in aligner.compute_existing_alignment():
                existing_alignments.add((str(reverse_eids_map[autheid]), uri))
            add_alignments, remove_alignments = {}, {}
            brenanos = cnx.find("AgentAuthority", eid=self.eids_map["18704351"]).one()
            self.assertEqual(1, len(brenanos.same_as))
            self.assertEqual("https://www.wikidata.org/wiki/Q31", brenanos.same_as[0].uri)
            file_name = os.path.join(self.datapath(), "agents-import-alignments.csv")
            with open(file_name) as fp:
                new_alignments, to_remove_alignments = aligner.process_csv(
                    fp, existing_alignments, override_alignments=True
                )
                # one conflit for Ludwig van Beethoven (1770-1827)  must be detected
                self.assertCountEqual(
                    list(new_alignments.keys()),
                    [
                        ("18704351", "https://www.wikidata.org/wiki/Q315072"),
                        ("131399075", "https://data.bnf.fr/fr/11894985/albert_camus/"),
                        ("130963047", "https://data.bnf.fr/ark:/12148/cb11907966z"),
                        (
                            "130963047",
                            "https://www.larousse.fr/encyclopedie/personnage/Victor_Hugo/124393",
                        ),
                    ],
                )
                self.assertEqual(
                    list(to_remove_alignments.keys()),
                    [("18704351", "https://www.wikidata.org/wiki/Q31")],
                )
                for key, record in new_alignments.items():
                    add_alignments[(self.eids_map[key[0]], key[1])] = record
                for key, record in to_remove_alignments.items():
                    remove_alignments[(self.eids_map[key[0]], key[1])] = record
            # import new alignements
            aligner.process_alignments(add_alignments, remove_alignments, override_alignments=True)
            # check new alignemnts are added
            # https://www.wikidata.org/wiki/Q31 is removed from 18704351
            for old_eid, expected_uri in (
                ("18704351", "https://www.wikidata.org/wiki/Q315072"),
                ("131399075", "https://data.bnf.fr/fr/11894985/albert_camus/"),
            ):
                agent = cnx.find("AgentAuthority", eid=self.eids_map[old_eid]).one()
                for link in agent.same_as:
                    self.assertEqual(expected_uri, link.uri)
            # https://www.larousse.fr/encyclopedie/personnage/Victor_Hugo/124393
            # is added on 130963047
            agent = cnx.find("AgentAuthority", eid=self.eids_map["130963047"]).one()
            expected = (
                "https://data.bnf.fr/ark:/12148/cb11907966z",
                "https://www.larousse.fr/encyclopedie/personnage/Victor_Hugo/124393",
            )
            self.assertCountEqual(expected, [link.uri for link in agent.same_as])

    def test_process_subject_csv_file(self):
        """Test import subject alignments form CSV file. Subjects and Agents
        share the same import alignments code. Only CSV headers are différents

        Trying: process CSV file
        Expecting: file is correctply formed
        """
        with self.admin_access.cnx() as cnx:
            aligner = SubjectImportAligner(cnx)
            existing_alignments = set()
            file_name = os.path.join(self.datapath(), "subjects-import-alignments.csv")
            with open(file_name) as fp:
                new_alignments, to_remove_alignments = aligner.process_csv(
                    fp, existing_alignments, override_alignments=True
                )
                # one conflit for M1 (char d'assaut)  must be detected
                self.assertCountEqual(
                    list(new_alignments.keys()),
                    [
                        ("78921211", "https://data.bnf.fr/fr/11984784/m1__carabine_/"),
                        ("23827342", "https://www.wikidata.org/wiki/Q1357802"),
                    ],
                )
                self.assertEqual(0, len(to_remove_alignments.keys()))

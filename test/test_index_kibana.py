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
import datetime

from mock import patch

import unittest

from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_francearchives.testutils import EADImportMixin

from cubicweb_frarchives_edition.entities.kibana.sqlutils import create_kibana_authorities_sql

from pgfixtures import setup_module, teardown_module  # noqa


class KibanaIndexerImporterTC(EADImportMixin, CubicWebTC):
    configcls = PostgresApptestConfiguration

    def setup_database(self):
        super(KibanaIndexerImporterTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            cnx.create_entity(
                "Service",
                category="?",
                name="Les Archives Nationales",
                short_name="Les AN",
                code="FRAN",
            )
            scheme = cnx.create_entity("ConceptScheme", title="Title")
            concept0 = cnx.create_entity("Concept", cwuri="https://example.com", in_scheme=scheme)
            concept1 = cnx.create_entity("Concept", cwuri="https://foo.com", in_scheme=scheme)
            cnx.create_entity(
                "Label",
                label="example",
                language_code="en",
                kind="preferred",
                label_of=concept0,
            )
            cnx.create_entity(
                "Label",
                label="exemple",
                language_code="fr-fr",
                kind="preferred",
                label_of=concept0,
            )
            cnx.create_entity(
                "Label",
                label="foo",
                language_code="fr-fr",
                kind="preferred",
                label_of=concept1,
            )
            cnx.create_entity("SubjectAuthority", label="Exemple", same_as=(concept0, concept1))
            cnx.commit()

    def publish_ir(self, cnx):
        # this is done by frarchives_edition.tasks.import_ead.launch_task
        rset = cnx.execute(
            """Any S WHERE S is State, S state_of WF, X default_workflow WF,
            X name "FindingAid", WF initial_state S"""
        )
        cnx.system_sql(
            "INSERT INTO in_state_relation (eid_from, eid_to) "
            "SELECT cw_eid, %(eid_to)s FROM cw_findingaid WHERE "
            "NOT EXISTS (SELECT 1 FROM in_state_relation i "
            "WHERE i.eid_from = cw_eid)",
            {"eid_to": rset[0][0]},
        )
        for fa in cnx.find("FindingAid").entities():
            adapted = fa.cw_adapt_to("IWorkflowable")
            adapted.fire_transition_if_possible("wft_cmsobject_publish")
        cnx.commit()

    def create_authority_record(self, cnx):
        kind_eid = cnx.find("AgentKind", name="person")[0][0]
        name = "Jean Cocotte"
        return cnx.create_entity(
            "AuthorityRecord",
            record_id="FRAN_NP_006883",
            agent_kind=kind_eid,
            reverse_name_entry_for=cnx.create_entity(
                "NameEntry", parts=name, form_variant="authorized"
            ),
            xml_support="foo",
            start_date=datetime.datetime(1940, 1, 1),
            reverse_occupation_agent=cnx.create_entity("Occupation", term="éleveur de poules"),
            reverse_history_agent=cnx.create_entity("History", text="<p>Il aimait les poules</p>"),
        )

    def test_kibana_service_es(self):
        """Test for IKibanaIndexSerializable index"""
        with self.admin_access.cnx() as cnx:
            service = cnx.find("Service", code="FRAN").one()
            bc = cnx.create_entity(
                "BaseContent",
                title="program",
                content="31 juin",
                basecontent_service=service,
                reverse_children=cnx.create_entity(
                    "Section", title="Publication", name="publication"
                ),
            )
            cnx.commit()
            doc = service.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(0, doc["documents_count"])
            self.import_filepath(cnx, "FRAN_IR_050263.xml")
            service = cnx.find("Service", code="FRAN").one()
            s_ift = service.cw_adapt_to("IKibanaIndexSerializable")
            self.assertEqual(s_ift.es_id, service.eid)
            doc = s_ift.serialize()
            self.assertEqual(0, doc["documents_count"])
            self.publish_ir(cnx)
            service = cnx.find("Service", code="FRAN").one()
            s_ift = service.cw_adapt_to("IKibanaIndexSerializable")
            self.assertEqual(s_ift.es_id, service.eid)
            doc = s_ift.serialize()
            self.assertEqual(523, doc["documents_count"])
            self.assertEqual(523, doc["archives"])
            self.assertEqual(0, doc["siteres"])
            self.assertEqual(0, doc["nomina"])
            # publish BaseContent
            bc = cnx.entity_from_eid(bc.eid)
            bc.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            doc = service.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(524, doc["documents_count"])
            self.assertEqual(523, doc["archives"])
            self.assertEqual(1, doc["siteres"])

    def test_kibana_authority_es(self):
        """Test for IKibanaIndexSerializable index"""
        with self.admin_access.cnx() as cnx:
            create_kibana_authorities_sql(cnx)
            self.import_filepath(cnx, "FRAN_IR_050263.xml")
            create_kibana_authorities_sql(cnx)
            agent = cnx.execute(
                "Any X WHERE X is AgentAuthority, X label %(l)s", {"l": "Hugo, Victor"}
            ).one()
            record = self.create_authority_record(cnx)
            agent.cw_set(same_as=record)
            esdoc = agent.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(False, esdoc["quality"])
            agent.cw_set(quality=True)
            cnx.commit()
            self.assertEqual(len(agent.reverse_authority[0].index), 1)
            s_ift = agent.cw_adapt_to("IKibanaIndexSerializable")
            self.assertEqual(s_ift.es_id, agent.eid)
            doc = s_ift.serialize()
            self.assertEqual(0, cnx.system_sql("Select * from published.cw_FindingAid").rowcount)
            self.assertEqual(0, doc["documents_count"])  # AuthorityRecord is not taken account
            self.assertEqual(0, doc["services_count"])
            self.assertEqual([], doc["services"])
            initial_doc = agent.cw_adapt_to("IKibanaInitiaLAuthorityIndexSerializable").serialize()
            self.assertCountEqual(doc, initial_doc)
            # publish FindingAid
            self.publish_ir(cnx)
            create_kibana_authorities_sql(cnx)
            service = cnx.find("Service", code="FRAN").one()
            fa = cnx.find("FindingAid").one()
            self.assertEqual(fa.related_service.eid, service.eid)
            agent.cw_clear_all_caches()
            esdoc = agent.cw_adapt_to("IKibanaIndexSerializable").serialize()
            expected = {
                "creation_date": agent.creation_date,
                "cw_etype": "AgentAuthority",
                "documents_count": 1,
                "eid": agent.eid,
                "grouped_with": [],
                "grouped_with_count": 0,
                "is_grouped": False,
                "label": "Hugo, Victor",
                "location": [],
                "quality": True,
                "reindex_date": datetime.date.today().strftime("%Y-%m-%d"),
                "same_as": [
                    {"label": "FRAN_NP_006883", "source": "EAC-CPF", "uri": "FRAN_NP_006883"}
                ],
                "same_as_count": 1,
                "services": [
                    {
                        "code": "FRAN",
                        "eid": service.eid,
                        "level": "None",
                        "title": "Les Archives Nationales",
                    }
                ],
                "services_count": 1,
                "types": ["persname"],
                "urlpath": f"http://testing.fr/cubicweb/agent/{agent.eid}",
            }
            self.assertDictEqual(expected, esdoc)
            self.assertEqual(service.eid, esdoc["services"][0]["eid"])
            ini_esdoc = agent.cw_adapt_to("IKibanaInitiaLAuthorityIndexSerializable").serialize()
            self.assertCountEqual(doc, ini_esdoc)
            self.assertCountEqual(esdoc, ini_esdoc)

    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_location_authority(self, index, exists):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            loc1 = ce(
                "LocationAuthority",
                label="Paris (Paris, France)",
                longitude=48.85341,
                latitude=2.3488,
            )
            cnx.commit()
            esdoc = loc1.cw_adapt_to("IKibanaIndexSerializable").serialize()
            expected = {
                "creation_date": loc1.creation_date,
                "cw_etype": "LocationAuthority",
                "documents_count": 0,
                "eid": loc1.eid,
                "grouped_with": [],
                "grouped_with_count": 0,
                "is_grouped": False,
                "label": "Paris (Paris, France)",
                "location": [48.85341, 2.3488],
                "quality": False,
                "reindex_date": datetime.date.today().strftime("%Y-%m-%d"),
                "same_as": [],
                "same_as_count": 0,
                "services": [],
                "services_count": 0,
                "types": [],
                "urlpath": f"http://testing.fr/cubicweb/location/{loc1.eid}",
            }
            self.assertDictEqual(expected, esdoc)

    def test_subject_authority_same_as(self):
        """Test SubjectAuthority same_as."""
        with self.admin_access.cnx() as cnx:
            authority = cnx.find("SubjectAuthority").one()
            actual = authority.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(actual["same_as_count"], 2)
            self.assertCountEqual(
                actual["same_as"],
                [
                    {"label": "foo", "uri": "https://foo.com", "source": "Title"},
                    {"label": "exemple", "uri": "https://example.com", "source": "Title"},
                ],
            )

    def test_agent_authority_same_as(self):
        """Test SubjectAuthority same_as."""
        with self.admin_access.cnx() as cnx:
            kind_eid = cnx.find("AgentKind", name="person")[0][0]
            name = "La Toto compagnie"
            auth_rec = cnx.create_entity(
                "AuthorityRecord",
                record_id="FRAN_NP_006883",
                agent_kind=kind_eid,
                reverse_name_entry_for=cnx.create_entity(
                    "NameEntry", parts=name, form_variant="authorized"
                ),
                xml_support="foo",
            )
            authority = cnx.create_entity("AgentAuthority", label="person", same_as=auth_rec)
            cnx.commit()
            json = authority.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertCountEqual(
                json["same_as"],
                [{"label": "FRAN_NP_006883", "uri": "FRAN_NP_006883", "source": "EAC-CPF"}],
            )


if __name__ == "__main__":
    unittest.main

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
import unittest

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_frarchives_edition.entities.kibana.sqlutils import create_kibana_authorities_sql

from cubicweb_francearchives.testutils import PostgresTextMixin, EADImportMixin

from pgfixtures import setup_module, teardown_module  # noqa


class KibanaIndexerImporterTC(EADImportMixin, PostgresTextMixin, CubicWebTC):
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

    def test_kibana_service_es(self):
        """Test for IKibanaIndexSerializable index"""
        with self.admin_access.cnx() as cnx:
            service = cnx.find("Service", code="FRAN").one()
            doc = service.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(0, doc["documents_count"])
            self.import_filepath(cnx, self.datapath("FRAN_IR_050263.xml"))
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

    def test_kibana_authority_es(self):
        """Test for IKibanaIndexSerializable index"""
        with self.admin_access.cnx() as cnx:
            create_kibana_authorities_sql(cnx)
            self.import_filepath(cnx, self.datapath("FRAN_IR_050263.xml"))
        with self.admin_access.cnx() as cnx:
            create_kibana_authorities_sql(cnx)
            agent = cnx.execute(
                "Any X WHERE X is AgentAuthority, X label %(l)s", {"l": "Hugo, Victor"}
            ).one()
            self.assertEqual(len(agent.reverse_authority[0].index), 1)
            s_ift = agent.cw_adapt_to("IKibanaIndexSerializable")
            self.assertEqual(s_ift.es_id, agent.eid)
            doc = s_ift.serialize()
            self.assertEqual(0, cnx.system_sql("Select * from published.cw_FindingAid").rowcount)
            self.assertEqual(0, doc["documents_count"])
            self.assertEqual(0, doc["services_count"])
            self.assertEqual([], doc["services"])
            initial_doc = agent.cw_adapt_to("IKibanaInitiaLAuthorityIndexSerializable").serialize()
            self.assertCountEqual(doc, initial_doc)
            # publish FindingAid
            self.publish_ir(cnx)
        with self.admin_access.cnx() as cnx:
            create_kibana_authorities_sql(cnx)
            service = cnx.find("Service", code="FRAN").one()
            fa = cnx.find("FindingAid").one()
            self.assertEqual(fa.related_service.eid, service.eid)
            agent = cnx.find("AgentAuthority", eid=agent.eid).one()
            doc = agent.cw_adapt_to("IKibanaIndexSerializable").serialize()
            self.assertEqual(1, doc["documents_count"])
            self.assertEqual(1, doc["services_count"])
            self.assertEqual(service.eid, doc["services"][0]["eid"])
            initial_doc = agent.cw_adapt_to("IKibanaInitiaLAuthorityIndexSerializable").serialize()
            self.assertCountEqual(doc, initial_doc)


if __name__ == "__main__":
    unittest.main

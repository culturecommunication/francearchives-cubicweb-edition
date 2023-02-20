# -*- coding: utf-8 -*-
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2021
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
import datetime
import json

# third party imports
# CubicWeb specific imports

# library specific imports
from pgfixtures import setup_module, teardown_module  # noqa
from utils import TaskTC

from cubicweb_francearchives.testutils import PostgresTextMixin, EADImportMixin
from cubicweb_francearchives.utils import merge_dicts

from cubicweb_frarchives_edition.tasks.export_ape import retrieve_ape


class ExportApeEadTC(EADImportMixin, PostgresTextMixin, TaskTC):
    """Export ape-ead files."""

    readerconfig = merge_dicts({}, EADImportMixin.readerconfig, {"nodrop": False})

    def setUp(self):
        super(ExportApeEadTC, self).setUp()
        with self.admin_access.cnx() as cnx:
            self.service = cnx.create_entity("Service", code="FRAD095", category="foo")
            cnx.commit()

    def insert_fa_initial_wfstate(self, cnx):
        #  insert intial state for the FindingAid with no current state after reimport
        rset = cnx.execute(
            "Any S WHERE S is State, S state_of WF, "
            'X default_workflow WF, X name "FindingAid", '
            "WF initial_state S"
        )
        cnx.system_sql(
            "INSERT INTO in_state_relation (eid_from, eid_to) "
            "SELECT cw_eid, %(eid_to)s FROM cw_findingaid WHERE "
            "NOT EXISTS (SELECT 1 FROM in_state_relation i "
            "WHERE i.eid_from = cw_eid)",
            {"eid_to": rset[0][0]},
        )
        cnx.commit()

    def test_export_ape_ead(self):
        with self.admin_access.cnx() as cnx:
            self.get_or_create_imported_filepath("FRAD095_00162.xml")
            task_name = "export_ape"
            self.import_filepath(cnx, "FRAD095_00162.xml")
            self.login()
            fi = cnx.find("FindingAid").one()
            self.insert_fa_initial_wfstate(cnx)
            fi.cw_adapt_to("IWorkflowable").fire_transition_if_possible("wft_cmsobject_publish")
            cnx.commit()
            fi = cnx.find("FindingAid").one()
            self.assertEqual(fi.cw_adapt_to("IWorkflowable").state, "wfs_cmsobject_published")
            data = json.dumps({"name": task_name, "title": task_name, "services": "FRAD095"})

            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                f"/RqTask/?schema_type={task_name}",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, task_name)
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            self.assertEqual(1, len(job.output_file))
            output_file = job.output_file[0]
            date = datetime.datetime.now().strftime("%Y%m%d")
            self.assertTrue(
                output_file.data_name == output_file.title == f"ape_{date}_{job.id}.zip"
            )

    def test_select_fi_for_ape_ead_export(self):
        with self.admin_access.cnx() as cnx:
            self.get_or_create_imported_filepath("FRAD095_00162.xml")
            self.import_filepath(cnx, "FRAD095_00162.xml")
            self.insert_fa_initial_wfstate(cnx)
            fi = cnx.find("FindingAid").one()
            self.assertEqual(fi.cw_adapt_to("IWorkflowable").state, "wfs_cmsobject_draft")
            ape_files = []
            arcnames = []
            retrieve_ape(cnx, "FRAD095", ape_files, arcnames)
            self.assertFalse(arcnames)
            fi.cw_adapt_to("IWorkflowable").fire_transition_if_possible("wft_cmsobject_publish")
            cnx.commit()
            fi = cnx.find("FindingAid").one()
            self.assertEqual(fi.cw_adapt_to("IWorkflowable").state, "wfs_cmsobject_published")
            retrieve_ape(cnx, "FRAD095", ape_files, arcnames)
            self.assertEqual(arcnames, ["FRAD095/ape-FRAD095_00162.xml"])

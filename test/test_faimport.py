# -*- coding: utf-8 -*-
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
"""cubicweb-frarchives_edition tests for FindingAid imports"""

# standard library imports
import base64
import csv
from io import StringIO, TextIOWrapper
import json
import shutil
import zipfile
import os
import os.path as osp
import unittest.mock

# third party imports
import rq

# library specific imports

from cubicweb_francearchives.dataimport.oai_nomina import compute_nomina_stable_id
from cubicweb_francearchives.testutils import OaiSickleMixin, S3BfssStorageTestMixin

from cubicweb_frarchives_edition.tasks.qualify_authorities import KIBANA_FIELDNAMES
from cubicweb_frarchives_edition.rq import work

from utils import create_findingaid, TaskTC

from pgfixtures import setup_module, teardown_module  # noqa


class FAImportArchiveMixIn(object):
    """FindingAid import test cases MixIn.

    The MixIn provides a method to prepare a Zip archive for consumption
    by the 'import_ead' and 'import_csv' tasks.
    """

    def create_archive(self, service, skipfiles=[], add_service_dir=True):
        datadir = osp.join(self.datadir, "ir_data")
        service_dir = osp.join(datadir, service)
        if not add_service_dir:
            datadir = service_dir
        zipf = zipfile.ZipFile(osp.join(service_dir, "%s.zip" % service), "w", zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(service_dir):
            zipf.write(root, osp.relpath(root, datadir))
            for fname in files:
                ext = osp.splitext(fname)[1]
                if ext in (".xml", ".pdf", ".csv", ".txt"):
                    rel_filename = osp.join(osp.relpath(root, datadir), fname)
                    if rel_filename not in skipfiles:
                        zipf.write(osp.join(root, fname), rel_filename)
        return zipf


class FAImportsBaseTC(S3BfssStorageTestMixin, TaskTC):
    """FindingAid import test cases base class."""

    def setUp(self):
        """Set up job queue and configuration."""
        super(FAImportsBaseTC, self).setUp()
        self.config.global_set_option("ead-services-dir", self.datapath("ir_data", "import"))
        self.config.global_set_option("eac-services-dir", self.datapath("ir_data", "import"))
        self.config.global_set_option("nomina-services-dir", self.datapath("ir_data", "import"))
        self.config.global_set_option("nomina-index-name", "dummy_nomina")
        self.config.global_set_option("admin-emails", "toto@logilab.fr, tata@logilab.fr")


class FAImportsOAITC(OaiSickleMixin, FAImportsBaseTC):
    """FindingAid import test cases.

    The test cases assert that the 'import_oai' tasks are
    executed as expected.
    """

    def filepath(self):
        assert self.filename is not None
        return self.datapath(self.filename)

    @classmethod
    def init_config(cls, config):
        super(FAImportsOAITC, cls).init_config(config)
        config.set_option("nomina-services-dir", "/tmp")

    def setup_database(self):
        """Set database up and add services related to the test cases to
        the database.
        """
        super(FAImportsOAITC, self).setup_database()
        url = "file://{}?verb=ListRecords&metadataPrefix={}"
        with self.admin_access.repo_cnx() as cnx:
            service = cnx.create_entity("Service", name="Hérault", code="FRAD034", category="foo")
            cnx.create_entity(
                "OAIRepository",
                name="oai_dc",
                service=service,
                url=url.format(self.datapath("oai_dc_sample.xml"), "oai_dc"),
            )
            service = cnx.create_entity("Service", name="Marne", code="FRAD051", category="bar")
            cnx.create_entity(
                "OAIRepository",
                name="oai_ead",
                service=service,
                url=url.format(self.datapath("oai_ead_sample.xml"), "oai_ead"),
            )
            service = cnx.create_entity("Service", name="Ardennes", code="FRAD0O8", category="bar")
            cnx.create_entity(
                "OAIRepository",
                name="nomina",
                service=service,
                url=url.format(self.datapath("oai_nomina_sample.xml"), "nomina"),
            )
            cnx.commit()

    def test_import_oai(self):
        """Test OAI import.

        Trying: OAI-DC import
        Expecting: 'import_oai' job executes successfully and 1 FindingAid is
        imported
        """
        self.filename = "oai_dc_sample.xml"
        with self.admin_access.cnx() as cnx:
            eid = cnx.execute('Any X WHERE X is OAIRepository, X name "oai_dc"').one().eid
            data = json.dumps(
                {
                    "name": "import_oai",
                    "title": "oai_dc",
                    "oairepository": eid,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.login()
            self.webapp.post(
                "/RqTask/?schema_type=import_oai",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "import_oai (oai_dc)")
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.fatask_findingaid), 1)

    def test_harvest_nomina_oai(self):
        """Test NOMINA OAI-PMH harvesting and import

        Trying: harvest a nomina file
        Expecting: 'import_oai' job for nomina is successfully executed: 3 cwfiles with
                    harvested data are created an import_csv_nomina job is created
        """
        self.filename = "oai_nomina_sample.xml"
        with self.admin_access.cnx() as cnx:
            eid = cnx.execute('Any X WHERE X is OAIRepository, X name "nomina"').one().eid
            data = json.dumps(
                {
                    "name": "import_oai",
                    "title": "nomina",
                    "dry-run": False,
                    "oairepository": eid,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.login()
            self.webapp.post(
                "/RqTask/?schema_type=import_oai",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "import_oai (nomina)")
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.output_file), 1)
            csvfile = task.output_file[0]
            with TextIOWrapper(csvfile.data, encoding="utf-8", newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                rows = [row for row in reader]
            self.assertEqual(len(rows), 8)
            self.assertEqual(1, len(task.subtasks))

    def test_delete_nomina_by_service(self):
        """Test delete nomina records for a servicce

        Trying: delete all NominaRecords for a service
        Expecting: 'delete_nomina_by_service' job for nomina is successfully executed:
                    no NominaRecords is left
        """
        with self.admin_access.cnx() as cnx:
            service = cnx.find("Service", code="FRAD034").one()
            for idx in range(1, 21):
                cnx.create_entity(
                    "NominaRecord",
                    stable_id=compute_nomina_stable_id(service.code, str(idx)),
                    json_data={
                        "e": {
                            "N": {
                                "d": [{"d": "1894-06-24", "y": "1894"}],
                                "l": [
                                    {"c": "France", "d": "Yvelines (ex Seine et Oise)", "dc": "78"}
                                ],
                            },
                        },
                        "p": [{"f": "Georges René", "n": f"BIEUVILLE {idx}"}],
                        "t": "MPF14-18",
                        "u": "https://site.fr/fr/ark:/40699/m005239d4b17{idx}e",  # noqa
                    },
                    service=service,
                )
            cnx.commit()
            self.assertEqual(
                20,
                cnx.execute(
                    "Any COUNT(X) WHERE X service S, X is NominaRecord, S eid %(e)s",
                    {"e": service.eid},
                )[0][0],
            )
            task_name = "delete_nomina_by_service"
            data = json.dumps(
                {
                    "name": task_name,
                    "title": "delete NominaRecords",
                    "service": service.eid,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.login()
            self.webapp.post(
                f"/RqTask/?schema_type={task_name}",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, task_name)
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(
                0,
                cnx.execute(
                    "Any COUNT(X) WHERE X service S, X is NominaRecord, S eid %(e)s",
                    {"e": service.eid},
                )[0][0],
            )

    @unittest.mock.patch("cubicweb.cwconfig.CubicWebConfiguration.sendmails")
    def test_oai_email_notifications(self, mock_sendmail):
        """
        Trying: OAI-DC import
        Expecting: a mail with warnings is send"""
        self.filename = "oai_dc_sample.xml"
        with self.admin_access.cnx() as cnx:
            eid = cnx.execute('Any X WHERE X is OAIRepository, X name "oai_dc"').one().eid
            data = json.dumps(
                {
                    "name": "import_oai",
                    "title": "oai_dc",
                    "oairepository": eid,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.login()
            self.webapp.post(
                "/RqTask/?schema_type=import_oai",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "import_oai (oai_dc)")
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            args, _ = mock_sendmail.call_args
            msg, to_addrs = args[0][0]
            self.assertEqual(to_addrs, "toto@logilab.fr, tata@logilab.fr")
            expected = [
                ("Content-Type", "multipart/alternative"),
                ("MIME-Version", "1.0"),
                (
                    "Subject",
                    "[FranceArchives]: Hérault (FRAD034) - harvesting completed with errors/warnings",  # noqa
                ),
                ("From", "cubicweb-test@logilab.fr"),
                ("To", "toto@logilab.fr, tata@logilab.fr"),
            ]
            got = dict(msg.items())
            for key, value in expected:
                self.assertEqual(got[key], value)
            email_message = {part.get_content_type(): part.get_payload() for part in msg.walk()}
            html_content = str(base64.b64decode(email_message["text/html"]).decode("utf-8"))
            self.assertIn("""</div> EADID value""", html_content)
            self.assertNotIn("""\n""", html_content)
            text_content = str(base64.b64decode(email_message["text/plain"]).decode("utf-8"))
            self.assertNotIn("""</div> EADID value""", text_content)
            self.assertIn(""" EADID value""", text_content)
            self.assertIn("""\n""", text_content)

    def test_import_oai_compute_alignments(self):
        """Test OAI import.

        Trying: OAI-EAD import
        Expecting: 'import_oai' job executes successfully,
        1 'compute_alignments' task executes successfully and
        2 FindingAids are imported
        """
        self.filename = "oai_ead_sample.xml"
        with self.admin_access.cnx() as cnx:
            eid = cnx.execute('Any X WHERE X is OAIRepository, X name "oai_ead"').one().eid
            data = json.dumps(
                {
                    "name": "import_oai",
                    "title": "oai_ead",
                    "oairepository": eid,
                    "should_normalize": True,
                    "context_service": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.login()
            self.webapp.post(
                "/RqTask/?schema_type=import_oai",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.execute("Any X WHERE X is RqTask, X name 'import_oai (oai_ead)'").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.fatask_findingaid), 2)
            subtasks = cnx.execute(
                "Any X WHERE X is RqTask, X name 'compute_alignments'"
            ).entities()
            subtasks = list(subtasks)
            self.assertTrue(len(subtasks) == 1 == len(task.subtasks))
            self.assertEqual(subtasks, list(task.subtasks))
            job = subtasks[0].cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "finished")


class FAImportsTC(FAImportArchiveMixIn, FAImportsBaseTC):
    """FindingAid import test cases.

    The test cases assert that the 'import_ead' and 'import_csv' tasks are
    executed as expected.
    """

    def setUp(self):
        """Set up test cases."""
        super(FAImportsTC, self).setUp()

    def tearDown(self):
        """Remove test-specific files."""
        super(FAImportsTC, self).tearDown()
        import_dir = self.datapath("ir_data", "import")
        if osp.exists(import_dir):
            shutil.rmtree(import_dir)
        zf = osp.join(self.datadir, "ir_data", "FRAD008", "FRAD008.zip")
        if osp.exists(zf):
            os.remove(zf)

    def setup_database(self):
        """Set database up and add services related to the test cases to
        the database.
        """
        super(FAImportsTC, self).setup_database()
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("Service", code="FRAD008", category="foo")
            cnx.create_entity("Service", code="FRMAEE", category="foo")
            cnx.create_entity("Service", code="FRAD051", category="foo")
            cnx.create_entity("Service", code="FRAD092", category="foo")
            cnx.commit()

    def work(self, cnx):
        """Start task."""
        return work(cnx, burst=True, worker_class=rq.worker.SimpleWorker)

    def send_archive(self, service="FRAD008", task_name="import_ead", skipfiles=[]):
        """Prepare POST request arguments."""
        self.create_archive(service, skipfiles=skipfiles)
        basename = "%s.zip" % service
        zip_path = osp.join(self.datadir, "ir_data", service, basename)
        data = {
            "name": task_name,
            "title": "a task",
            "file": basename,
            "should_normalize": False,
            "context_service": True,
            "service": service,
        }
        return dict(
            params=[("data", json.dumps(data))],
            upload_files=[("fileobj", basename, open(zip_path, "rb").read())],
        )

    def test_fazip_import_ead(self):
        """Test EAD import.

        Trying: Zip archive
        Expecting: 'import_ead' job is executes successfully and 3 FindingAids
        are imported
        """
        post_kwargs = self.send_archive("FRAD008", skipfiles=["FRAD008/SER_1110.xml"])
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.assertTrue(
                job.description.startswith(
                    "cubicweb_frarchives_edition.tasks.import_ead.import_ead("
                )
            )
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.fatask_findingaid), 3)

    def test_fazip_import_ead_compute_alignments(self):
        """Test EAD import.

        Trying: Zip archive
        Expecting: 1 'compute_alignments' task executes successfully
        """
        post_kwargs = self.send_archive("FRAD008", skipfiles=["FRAD008/SER_1110.xml"])
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            self.work(cnx)
            task = cnx.execute("Any X WHERE X is RqTask, X name 'import_ead'").one()
            subtasks = cnx.execute(
                "Any X WHERE X is RqTask, X name 'compute_alignments'"
            ).entities()
            subtasks = list(subtasks)
            self.assertTrue(len(subtasks) == 1 == len(task.subtasks))
            self.assertEqual(subtasks, list(task.subtasks))
            job = subtasks[0].cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "finished")

    def test_wrong_files(self):
        """Test EAD import.

        Trying: Zip archive contains malformed file
        Expecting: job does not successfully terminate and error message
        corresponds to cause
        """
        post_kwargs = self.send_archive("FRAD008")
        self.login()
        res = self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=400,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        errors = json.loads(res.text)["errors"]
        self.assertEqual(errors[0]["status"], 422)
        details = 'Following files does not start with "FRAD008" : FRAD008/SER_1110.xml'
        self.assertEqual(errors[0]["details"], details)

    def test_empty_zip(self):
        """Test EAD import.

        Trying: Zip archive is empty
        Expecting: job does not successfully terminate and error message
        corresponds to cause
        """
        post_kwargs = self.send_archive("FRAD051", skipfiles=["FRAD051/text.txt"])
        self.login()
        res = self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=400,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        errors = json.loads(res.text)["errors"]
        self.assertEqual(errors[0]["status"], 422)
        details = "This archive contains no files."
        self.assertEqual(errors[0]["details"], details)

    def test_without_xml(self):
        """Test EAD import.

        Trying: Zip archive does not contain XML files
        Expecting: job does not successfully terminate and error message
        corresponds to cause
        """
        post_kwargs = self.send_archive("FRAD051")
        self.login()
        res = self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=400,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        errors = json.loads(res.text)["errors"]
        self.assertEqual(errors[0]["status"], 422)
        details = "no XML files found in zip"
        self.assertEqual(errors[0]["details"], details)

    def test_without_metadata(self):
        """Test EAD import.

        Trying: Zip archive does not contain CSV file containing metadata
        Expecting: job does not successfully terminate and error message
        corresponds to cause
        """
        post_kwargs = self.send_archive(
            "FRAD008", skipfiles=["FRAD008/SER_1110.xml", "FRAD008/PDF/metadata.csv"]
        )
        self.login()
        res = self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=400,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        errors = json.loads(res.text)["errors"]
        self.assertEqual(errors[0]["status"], 422)
        details = "PDF/metadata.csv is missing from zip"
        self.assertEqual(errors[0]["details"], details)

    def test_fazip_import_csv(self):
        """Test CSV file import.

        Trying: CSV file containing 1 FindingAid
        Expecting: job is executed successfully and 1 FindingAid is imported
        """
        post_kwargs = self.send_archive("FRAD092", task_name="import_csv")
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_csv",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.execute("Any X WHERE X is RqTask, X name 'import_csv'").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.assertTrue(
                job.description.startswith(
                    "cubicweb_frarchives_edition.tasks.import_csv.import_csv("
                )
            )
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.fatask_findingaid), 1)

    def test_fazip_import_csv_compute_alignments(self):
        """Test CSV file import.

        Trying: CSV file
        Expecting: 1 'compute_alignments' task executes successfully
        """
        post_kwargs = self.send_archive("FRAD092", task_name="import_csv")
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_csv",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            self.work(cnx)
            task = cnx.execute("Any X WHERE X is RqTask, X name 'import_csv'").one()
            subtasks = cnx.execute(
                "Any X WHERE X is RqTask, X name 'compute_alignments'"
            ).entities()
            subtasks = list(subtasks)
            self.assertTrue(len(subtasks) == 1 == len(task.subtasks))
            self.assertEqual(subtasks, list(task.subtasks))
            job = subtasks[0].cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "finished")

    def test_fazip_import_relfiles(self):
        """Test import EAD.

        Trying: related files are referenced
        Expecting: job executes successfully and related files are created
        """
        post_kwargs = self.send_archive("FRMAEE")
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_ead",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.assertTrue(
                job.description.startswith(
                    "cubicweb_frarchives_edition.tasks.import_ead.import_ead("
                )
            )
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            expected = [
                "MAROC / Secrétariat particulier du résident général",
                "FRMAEE_1BIP_1919-1994",
                "CANADA / Secrétariat particulier du résident général",
            ]
            self.assertCountEqual([f.dc_title() for f in task.fatask_findingaid], expected)
            self.assertEqual(len(cnx.execute("Any F WHERE X findingaid_support F")), 3)
            self.assertEqual(len(cnx.execute("Any F WHERE X ape_ead_file F")), 2)
            rset = cnx.execute(
                f"""Any S, {self.fkeyfunc}(D) WHERE F data_hash S,
                X fa_referenced_files F, F data D"""
            )
            self.assertEqual(rset.rowcount, 12)
            for data_sha1hex, pdfpath in rset:
                pdfpath = pdfpath.getvalue()
                if self.s3_bucket_name:
                    destpath = self.get_filepath_by_storage(
                        f"{data_sha1hex}_{osp.basename(pdfpath).decode('utf-8')}"
                    )
                else:
                    destpath = self.get_filepath_by_storage(
                        f"{self.config['appfiles-dir']}/{data_sha1hex}_{osp.basename(pdfpath).decode('utf-8')}"  # noqa
                    )
                self.assertNotEqual(destpath, pdfpath)
                self.assertTrue(self.fileExists(pdfpath))
                self.assertTrue(self.fileExists(destpath))
                if not self.s3_bucket_name:
                    self.assertTrue(osp.islink(destpath))

    def test_fazip_import_eac(self):
        """Test EAC import.

        Trying: Zip archive
        Expecting: 'import_eac' job is executes successfully and 1 AuthorityRecord
        is imported
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity("Service", code="FRAN", category="l")
            cnx.commit()
        service = "FRAN"
        basename = "FRAN_NP_150159.xml"
        zip_path = osp.join(self.datadir, "ir_data", service, basename)
        data = {"name": "import_eac", "title": "a task", "file": basename, "service": service}
        post_kwargs = dict(
            params=[("data", json.dumps(data))],
            upload_files=[("fileobj", basename, open(zip_path, "rb").read())],
        )
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_eac",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.assertTrue(
                job.description.startswith(
                    "cubicweb_frarchives_edition.tasks.import_eac.import_eac("
                )
            )
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(len(task.fatask_authorityrecord), 1)

    def test_import_nomina_csv(self):
        """Test NOMINA import.

        Trying: CSV file
        Expecting: 'import_nomina' job is executes successfully and 10 NominaEntries are imported
        """
        with self.admin_access.cnx() as cnx:
            cnx.create_entity("Service", code="FRAD056", category="l")
            cnx.commit()
        service = "FRAD056"
        basename = "morbihan_nomina_exemple.csv"
        fpath = osp.join(self.datadir, "ir_data", service, basename)
        data = {
            "name": "import_csv_nomina",
            "title": "import nomina",
            "filepaths": [basename],
            "service": service,
            "doctype": "RM",
            "delimiter": ";",
        }
        buff = ""
        with open(fpath, "rb") as f:
            buff = f.read()
        post_kwargs = dict(
            params=[("data", json.dumps(data))],
            upload_files=[("fileobj", basename, buff)],
        )
        self.login()
        self.webapp.post(
            "/RqTask/?schema_type=import_csv_nomina",
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.assertTrue(
                job.description.startswith(
                    "cubicweb_frarchives_edition.tasks.import_csv_nomina.import_csv_nomina("
                )
            )
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            self.assertEqual(9, cnx.execute("Any COUNT(X) WHERE X is NominaRecord")[0][0])


class LocAuthorityGroupTC(FAImportsBaseTC):
    """test cases.

    The test cases assert that the 'compute_location_authorities_to_group' and
    'group_location_authorities' tasks are
    executed as expected.
    """

    def setup_database(self):
        geoname_uri = "http://www.geonames.org/2988507"
        super(LocAuthorityGroupTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            paris = cnx.create_entity(
                "ExternalUri", label="Paris (Île-de-france, France)", uri=geoname_uri
            )
            fa1 = create_findingaid(cnx, name="FRAD_XXX1")
            self.loc1 = cnx.create_entity(
                "LocationAuthority", label="Paris (Ile-de-France, France)", same_as=paris
            )
            self.geog1 = cnx.create_entity(
                "Geogname", label="Paris (Ile-de-france, France)", index=fa1, authority=self.loc1
            )
            fa2 = create_findingaid(cnx, name="FRAD_XXX2")
            self.loc2 = cnx.create_entity(
                "LocationAuthority", label="Paris (France)", same_as=paris
            )
            self.geog2 = cnx.create_entity(
                "Geogname", label="Paris (France)", index=fa2, authority=self.loc2
            )
            self.loc3 = cnx.create_entity(
                "LocationAuthority", label="Toulouse (Haute-Garonne, France)"
            )
            # populated place features
            sql = """INSERT INTO geonames (geonameid,name,admin4_code,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','P',%s)"""
            cnx.system_sql(sql, ("2988507", "Paris", "", "PPLC")),
            # cities
            sql = """INSERT INTO geonames (geonameid,name,admin4_code,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','A','ADM4')"""
            cnx.system_sql(sql, ("6455259", "Paris", "75056")),
            # regions
            sql = """INSERT INTO geonames
            (geonameid,admin1_code,name,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','A','ADM1')"""
            cnx.system_sql(sql, ("3012874", "11", "Île-de-France"))
            # countries
            sql = """INSERT INTO geonames (geonameid,country_code,fcode)
            VALUES (%s,%s,'PCLI')"""
            cnx.system_sql(sql, ("3017382", "FR"))
            sql = """INSERT INTO geonames_altnames
            (alternate_name,geonameid,isolanguage,alternatenameid)
            VALUES(%s,%s,'fr',%s)"""
            cnx.system_sql(sql, ("France", "3017382", "1556321"))
            cnx.commit()
            self.expected_candidates = "{label1}###{url1}\t{label2}###{url2}\r\n".format(
                label1=self.loc1.label,
                label2=self.loc2.label,
                url1=self.loc1.absolute_url(),
                url2=self.loc2.absolute_url(),
            ).encode("utf-8")

    def create_task_kwargs(self, content, filename, task_name):
        """Prepare POST request arguments."""
        data = {
            "name": task_name,
            "title": "a task",
            "file": filename,
        }
        return dict(
            params=[("data", json.dumps(data))], upload_files=[("fileobj", filename, content)]
        )

    def test_compute_location_authorities_to_group(self):
        """generate csv with candidates to group"""
        self.login()
        task_name = "compute_location_authorities_to_group"
        data = json.dumps(
            {
                "name": task_name,
                "title": task_name,
            }
        )
        post_kwargs = {"params": [("data", data)]}
        self.webapp.post(
            "/RqTask/?schema_type={}".format(task_name),
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, task_name)
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            candidates = task.output_file[0].read()
            self.assertEqual(self.expected_candidates, candidates)

    def test_group_location_authorities(self):
        """group locations"""
        self.login()
        task_name = "group_location_authorities"
        post_kwargs = self.create_task_kwargs(self.expected_candidates, "group.csv", task_name)
        self.webapp.post(
            "/RqTask/?schema_type={}".format(task_name),
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, task_name)
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            loc1 = cnx.find("LocationAuthority", eid=self.loc1.eid).one()
            loc2 = cnx.find("LocationAuthority", eid=self.loc2.eid).one()
            self.assertEqual((loc1,), loc2.grouped_with)
            self.assertCountEqual(
                [self.geog1.eid, self.geog2.eid], [geog.eid for geog in loc1.reverse_authority]
            )
            self.assertFalse(loc2.reverse_authority)

    def test_quality_authorities_ok(self):
        """test  "import_qualified_authorities" task"""
        self.login()
        with self.admin_access.cnx() as cnx:
            loc3 = cnx.find("LocationAuthority", eid=self.loc3.eid).one()
            self.assertFalse(loc3.quality)
        task_name = "import_qualified_authorities"
        stream = StringIO()
        writer = csv.writer(stream, delimiter="\t")
        writer.writerow(list(KIBANA_FIELDNAMES.keys()))
        writer.writerow((self.loc3.eid, self.loc1.label, "yes"))
        writer.writerow((self.loc2.eid, self.loc2.label, "no"))
        qualified_authorities = stream.getvalue().encode("utf-8")

        post_kwargs = self.create_task_kwargs(
            qualified_authorities, "qualified_authorities.csv", task_name
        )
        self.webapp.post(
            "/RqTask/?schema_type={}".format(task_name),
            status=201,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        with self.admin_access.cnx() as cnx:
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, task_name)
            job = task.cw_adapt_to("IRqJob")
            self.assertEqual(job.status, "queued")
            self.work(cnx)
            job.refresh()
            self.assertEqual(job.status, "finished")
            loc3 = cnx.find("LocationAuthority", eid=self.loc3.eid).one()
            self.assertTrue(loc3.quality)

    def test_quality_authorities_ko(self):
        """test  "import_qualified_authorities" task"""
        self.login()
        with self.admin_access.cnx() as cnx:
            loc3 = cnx.find("LocationAuthority", eid=self.loc3.eid).one()
            self.assertFalse(loc3.quality)
        task_name = "import_qualified_authorities"
        post_kwargs = self.create_task_kwargs(b"test", "qualified_authorities.js", task_name)
        res = self.webapp.post(
            "/RqTask/?schema_type={}".format(task_name),
            status=400,
            headers={"Accept": "application/json"},
            **post_kwargs
        )
        errors = json.loads(res.text)["errors"]
        self.assertEqual(errors[0]["status"], 422)
        details = '"qualified_authorities.js" file contains invalid headers "test"'
        self.assertEqual(errors[0]["details"], details)


if __name__ == "__main__":
    import unittest

    unittest.main()

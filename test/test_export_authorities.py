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
import io
import csv
import json
import zipfile
import glob
import os
import os.path
import datetime

# third party imports
from urllib.parse import urljoin

# CubicWeb specific imports

# library specific imports
from pgfixtures import setup_module, teardown_module  # noqa
from utils import TaskTC


def today():
    return datetime.datetime.now().strftime("%Y%m%d")


class ExportTC(TaskTC):
    """Export authorities task test cases."""

    def setUp(self):
        """Set up job queue and configuration."""
        super(ExportTC, self).setUp()
        self.config.global_set_option("appfiles-dir", str(self.datapath("appfiles")))

    def tearDown(self):
        """Clean up job queue."""
        super(ExportTC, self).tearDown()
        filenames = glob.glob(os.path.join(self.config["appfiles-dir"], "*"))
        for filename in filenames:
            os.remove(filename)

    def setup_database(self):
        """Set up database."""
        super(ExportTC, self).setup_database()
        with self.admin_access.repo_cnx() as cnx:
            cnx.allow_all_hooks_but("align")
            # create Service
            service0 = cnx.create_entity("Service", category="foo", code="FRAD000", name="foo")
            # create FAHeader
            fa_header0 = cnx.create_entity("FAHeader")
            did0 = cnx.create_entity("Did", unittitle="foo")
            # create FindingAid
            finding_aid0 = cnx.create_entity(
                "FindingAid",
                name="foo",
                eadid="1234567890",
                publisher="bar",
                fa_header=fa_header0,
                did=did0,
                stable_id="1234567890",
                service=service0,
            )
            # create ExternalUri (GeoNames)
            external_uri_geoname = cnx.create_entity(
                "ExternalUri",
                uri="https://www.geonames.org/2988507",
                label="Paris",
                source="geoname",
            )
            # create ExternalUri (data.bnf.fr)
            external_uri_databnf = cnx.create_entity(
                "ExternalUri",
                uri="https://data.bnf.fr/12093134",
                label="Orhan Pamuk",
                source="databnf",
            )
            # create ExternalUri (Wikidata)
            external_uri_wikidata = cnx.create_entity(
                "ExternalUri",
                uri="https://www.wikidata.org/wiki/Q241248",
                label="Orhan Pamuk",
                source="wikidata",
            )
            # create Concept
            scheme = cnx.create_entity("ConceptScheme", title="example")
            concept = cnx.create_entity("Concept", cwuri="https://example.com", in_scheme=scheme)
            cnx.create_entity(
                "Label", label="example", language_code="en", kind="preferred", label_of=concept
            )
            # create LocationAuthority
            location_authority0 = cnx.create_entity(
                "LocationAuthority",
                label="Paris",
                same_as=external_uri_geoname,
                longitude=48.85341,
                latitude=2.3488,
            )
            # create other LocationAuthority
            location_authority1 = cnx.create_entity("LocationAuthority", label="Berlin")
            # create AgentAuthority
            agent_authority0 = cnx.create_entity(
                "AgentAuthority",
                label="Orhan Pamuk",
                same_as=(external_uri_databnf, external_uri_wikidata),
            )
            # create other AgentAuthority
            agent_authority1 = cnx.create_entity("AgentAuthority", label="Kazuo Ishiguro")
            # create SubjectAuthority
            subject_authority0 = cnx.create_entity(
                "SubjectAuthority", label="example", same_as=concept
            )
            # create other SubjectAuthority
            subject_authority1 = cnx.create_entity("SubjectAuthority", label="instance")
            # create other Service
            service1 = cnx.create_entity("Service", category="bar", code="FRAD001", name="bar")
            # create FAHeader
            fa_header1 = cnx.create_entity("FAHeader")
            # create Did
            did1 = cnx.create_entity("Did", unittitle="bar")
            # create other FindingAid
            finding_aid1 = cnx.create_entity(
                "FindingAid",
                name="bar",
                eadid="0987654321",
                publisher="baz",
                fa_header=fa_header1,
                did=did1,
                stable_id="0987654321",
                service=service1,
            )
            # create Geogname
            cnx.create_entity(
                "Geogname",
                label="Paris",
                index=(finding_aid0, finding_aid1),
                authority=location_authority0,
            )
            # create other Geogname
            cnx.create_entity(
                "Geogname",
                label="Berlin",
                index=(finding_aid0, finding_aid1),
                authority=location_authority1,
            )
            # create AgentName
            cnx.create_entity(
                "AgentName",
                type="persname",
                label="Orhan Pamuk",
                index=(finding_aid0,),
                authority=agent_authority0,
            )
            # create other AgentName
            cnx.create_entity(
                "AgentName",
                type="persname",
                label="Kazuo Ishiguro",
                index=(finding_aid0,),
                authority=agent_authority1,
            )
            # create Subject
            cnx.create_entity(
                "Subject",
                type="subject",
                label="example",
                index=(finding_aid0,),
                authority=subject_authority0,
            )
            # create other Subject
            cnx.create_entity(
                "Subject",
                type="subject",
                label="instance",
                index=(finding_aid0,),
                authority=subject_authority1,
            )
            cnx.commit()
            cnx.allow_all_hooks_but()

    def _check_zip_archive(self, output_file, content):
        """Check Zip archive.

        :param File output_file: Zip archive
        :param dict content: expected rows per file
        """
        with zipfile.ZipFile(output_file.data) as zip_file:
            self.assertCountEqual(content.keys(), zip_file.namelist())
            for filename, rows in content.items():
                if rows:  # empty list of rows if we do not want to check related output file
                    with zip_file.open(filename) as fp:
                        with io.TextIOWrapper(fp, encoding="utf-8", newline="") as text:
                            reader = csv.reader(text, delimiter="\t")
                            self.assertEqual([row for row in reader], rows)

    def test_export_particular_service_location(self):
        """Test export authorities task.

        Trying: particular service
        Expecting: Zip archive containing CSV
        containing aligned authorities related to given service
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {"name": "export_locationauthorities", "title": "export", "services": "FRAD000",}
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_locationauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_locationauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # Zip archive exists
            self.assertEqual(1, len(job.output_file))
            output_file = job.output_file[0]
            self.assertTrue(output_file.data_name == output_file.title == "authorities.zip")
            # CSV file exists and contains 2 rows
            filename = "geoname/frad000-{today}.csv".format(today=today())
            fieldnames = [
                "identifiant_LocationAuthority",
                "URI_Geogname",
                "libelle_Geogname",
                "URI_LocationAuthority",
                "libelle_LocationAuthority",
                "URI_GeoNames",
                "libelle_GeoNames",
                "longitude",
                "latitude",
                "keep",
                "fiabilite_alignement",
            ]
            external_uri = cnx.execute("Any X WHERE X is ExternalUri, X source 'geoname'").one()
            base_url = cnx.vreg.config.get("consultation-base-url")
            geogname = cnx.execute(
                "Any X WHERE X is Geogname, X authority A, EXISTS(A same_as E)"
            ).one()
            geogname_uri = urljoin(base_url, "geogname/{eid}".format(eid=geogname.eid))
            auth = cnx.execute("Any X WHERE X is LocationAuthority, EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "location/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                str(geogname_uri),
                str(geogname.label),
                str(auth_uri),
                str(auth.label),
                str(external_uri.uri),
                str(external_uri.label),
                str(auth.longitude),
                str(auth.latitude),
                "yes",
                "",
            ]
            rows = [fieldnames, expected]
            content = {filename: rows}
            self._check_zip_archive(output_file, content)

    def test_export_particular_service_agent(self):
        """Test export authorities task.

        Trying: particular service
        Expecting: Zip archive containing CSV
        containing aligned authorities related to given service
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {"name": "export_agentauthorities", "title": "export", "services": "FRAD000",}
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_agentauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_agentauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # Zip archive exists
            self.assertEqual(1, len(job.output_file))
            output_file = job.output_file[0]
            self.assertTrue(output_file.data_name == output_file.title == "authorities.zip")
            # CSV files exist and file related to Wikidata contains 2 rows
            filenames = [
                "databnf/frad000-{today}.csv".format(today=today()),
                "wikidata/frad000-{today}.csv".format(today=today())
            ]
            fieldnames = [
                "identifiant_AgentAuthority",
                "URI_AgentName",
                "libelle_AgentName",
                "URI_AgentAuthority",
                "libelle_AgentAuthority",
                "type_AgentName",
                "URI_wikidata",
                "libelle_wikidata",
                "keep",
            ]
            external_uri = cnx.execute("Any X WHERE X is ExternalUri, X source 'wikidata'").one()
            base_url = cnx.vreg.config.get("consultation-base-url")
            agentname = cnx.execute(
                "Any X WHERE X is AgentName, X authority A, EXISTS (A same_as E)"
            ).one()
            agentname_uri = urljoin(base_url, "agentname/{eid}".format(eid=agentname.eid))
            auth = cnx.execute("Any X WHERE X is AgentAuthority, EXISTS (X same_as E)").one()
            auth_uri = urljoin(base_url, "agent/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                agentname_uri,
                agentname.label,
                auth_uri,
                auth.label,
                agentname.type,
                external_uri.uri,
                external_uri.label,
                "yes",
            ]
            content = {filenames[0]: [], filenames[1]: [fieldnames, expected]}
            self._check_zip_archive(output_file, content)

    def test_export_particular_service_subject(self):
        """Test export authorities task.

        Trying: particular service
        Expecting: Zip archive containing CSV
        containing aligned authorities related to given service
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {"name": "export_subjectauthorities", "title": "export", "services": "FRAD000",}
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_subjectauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_subjectauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # Zip archive exists
            self.assertEqual(1, len(job.output_file))
            output_file = job.output_file[0]
            self.assertTrue(output_file.data_name == output_file.title == "authorities.zip")
            # CSV file exists and contains 2 rows
            filename = "thesaurus/frad000-{today}.csv".format(today=today())
            fieldnames = [
                "identifiant_SubjectAuthority",
                "URI_Subject",
                "libelle_Subject",
                "URI_SubjectAuthority",
                "libelle_SubjectAuthority",
                "type_Subject",
                "URI_thesaurus",
                "libelle_thesaurus",
                "keep",
            ]
            concept = cnx.execute("Any X WHERE X is Concept").one()
            base_url = cnx.vreg.config.get("consultation-base-url")
            subject = cnx.execute(
                "Any X WHERE X is Subject, X authority A, EXISTS (A same_as E)"
            ).one()
            subject_uri = urljoin(base_url, "subjectname/{eid}".format(eid=subject.eid))
            auth = cnx.execute("Any X WHERE X is SubjectAuthority, EXISTS (X same_as E)").one()
            auth_uri = urljoin(base_url, "subject/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                subject_uri,
                subject.label,
                auth_uri,
                auth.label,
                subject.type,
                concept.cwuri,
                concept.dc_title(),
                "yes",
            ]
            content = {filename: [fieldnames, expected]}
            self._check_zip_archive(output_file, content)

    def test_export_no_service(self):
        """Test export authorities task.

        Trying: no service
        Expecting: CSV containing aligned authorities related to service
        for each service
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps({"name": "export_locationauthorities", "title": "export",})
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_locationauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_locationauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            output_file = job.output_file[0]
            # CSV files geoname/frad000.csv and geoname/frad001.csv exist
            self._check_zip_archive(
                output_file, {
                    "geoname/frad000-{today}.csv".format(today=today()): [],
                    "geoname/frad001-{today}.csv".format(today=today()): []
                }
            )

    def test_export_nonaligned_location(self):
        """Test export nonaligned LocationAuthorities.

        Trying: no service and toggle export nonaligned is on (toggle export aligned is off)
        Expecting: CSV containing nonaligned LocationAuthorities related to service
        for each service
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "export_locationauthorities",
                    "title": "export",
                    "aligned": False,
                    "nonaligned": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_locationauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_locationauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            output_file = job.output_file[0]
            # CSV files nonaligned/frad000.csv and nonaligned/frad001.csv exist and contain 2 rows
            filenames = [
                "nonaligned/frad000-{today}.csv".format(today=today()),
                "nonaligned/frad001-{today}.csv".format(today=today())
            ]
            fieldnames = [
                "identifiant_LocationAuthority",
                "URI_Geogname",
                "libelle_Geogname",
                "URI_LocationAuthority",
                "libelle_LocationAuthority",
                "URI_GeoNames",
                "libelle_GeoNames",
                "longitude",
                "latitude",
                "keep",
                "fiabilite_alignement",
            ]
            base_url = cnx.vreg.config.get("consultation-base-url")
            geogname = cnx.execute(
                "Any X WHERE X is Geogname, X authority A, NOT EXISTS(A same_as E)"
            ).one()
            geogname_uri = urljoin(base_url, "geogname/{eid}".format(eid=geogname.eid))
            auth = cnx.execute("Any X WHERE X is LocationAuthority, NOT EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "location/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                str(geogname_uri),
                str(geogname.label),
                str(auth_uri),
                str(auth.label),
                "",
                "",
                "",
                "",
                "yes",
                "",
            ]
            content = {filename: [fieldnames, expected] for filename in filenames}
            self._check_zip_archive(output_file, content)

    def test_export_nonaligned_agent(self):
        """Test export nonaligned AgentAuthorities.

        Trying: no service and toggle export nonaligned is on (toggle export aligned is off)
        Expecting: CSV containing nonaligned LocationAuthorities related to service
        for each service
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "export_agentauthorities",
                    "title": "export",
                    "aligned": False,
                    "nonaligned": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_agentauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_agentauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            output_file = job.output_file[0]
            # CSV files nonaligned/frad000.csv exists and contains 2 rows
            filename = "nonaligned/frad000-{today}.csv".format(today=today())
            fieldnames = [
                "identifiant_AgentAuthority",
                "URI_AgentName",
                "libelle_AgentName",
                "URI_AgentAuthority",
                "libelle_AgentAuthority",
                "type_AgentName",
                "keep",
            ]
            base_url = cnx.vreg.config.get("consultation-base-url")
            agentname = cnx.execute(
                "Any X WHERE X is AgentName, X authority A, NOT EXISTS(A same_as E)"
            ).one()
            agentname_uri = urljoin(base_url, "agentname/{eid}".format(eid=agentname.eid))
            auth = cnx.execute("Any X WHERE X is AgentAuthority, NOT EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "agent/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                agentname_uri,
                agentname.label,
                auth_uri,
                auth.label,
                agentname.type,
                "yes",
            ]
            content = {filename: [fieldnames, expected]}
            self._check_zip_archive(output_file, content)

    def test_export_nonaligned_subject(self):
        """Test export nonaligned SubjectAuthorities.

        Trying: no service and toggle export nonaligned is on (toggle export aligned is off)
        Expecting: CSV containing nonaligned SubjectAuthorities related to service
        for each service
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "export_subjectauthorities",
                    "title": "export",
                    "aligned": False,
                    "nonaligned": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_subjectauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "export_subjectauthorities")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            output_file = job.output_file[0]
            # CSV files nonaligned/frad000.csv exists and contains 2 rows
            filename = "nonaligned/frad000-{today}.csv".format(today=today())
            fieldnames = [
                "identifiant_SubjectAuthority",
                "URI_Subject",
                "libelle_Subject",
                "URI_SubjectAuthority",
                "libelle_SubjectAuthority",
                "type_Subject",
                "keep",
            ]
            base_url = cnx.vreg.config.get("consultation-base-url")
            subject = cnx.execute(
                "Any X WHERE X is Subject, X authority A, NOT EXISTS(A same_as E)"
            ).one()
            subject_uri = urljoin(base_url, "subjectname/{eid}".format(eid=subject.eid))
            auth = cnx.execute("Any X WHERE X is SubjectAuthority, NOT EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "subject/{eid}".format(eid=auth.eid))
            expected = [
                str(auth.eid),
                subject_uri,
                subject.label,
                auth_uri,
                auth.label,
                subject.type,
                "yes",
            ]
            content = {filename: [fieldnames, expected]}
            self._check_zip_archive(output_file, content)

    def test_export_simplified_location(self):
        """Test export aligned and nonaligned LocationAuthorities (simplified CSV file format).

        Trying: particular service and simplified CSV file format toggle is on
        Expecting: simplified CSV file format
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "export_locationauthorities",
                    "title": "export",
                    "services": "FRAD000",
                    "nonaligned": True,
                    "simplified": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=export_locationauthorities",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # execute task
            self.work(cnx)
            task = cnx.find("RqTask").one()
            job = task.cw_adapt_to("IRqJob")
            output_file = job.output_file[0]
            # CSV files geonames/frad000.csv and nonaligned/frad000.csv exist
            filenames = [
                "geoname/frad000-{today}.csv".format(today=today()),
                "nonaligned/frad000-{today}.csv".format(today=today())
            ]
            # simplified CSV file format
            fieldnames = [
                "identifiant_LocationAuthority",
                "URI_LocationAuthority",
                "libelle_LocationAuthority",
                "URI_GeoNames",
                "libelle_GeoNames",
                "longitude",
                "latitude",
                "keep",
                "fiabilite_alignement",
            ]
            # aligned LocationAuthorities
            external_uri = cnx.execute(
                "Any X WHERE X is ExternalUri, X source IN ('geoname', 'bano')"
            ).one()
            base_url = cnx.vreg.config.get("consultation-base-url")
            auth = cnx.execute("Any X WHERE X is LocationAuthority, EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "location/{eid}".format(eid=auth.eid))
            expected_aligned = [
                str(auth.eid),
                str(auth_uri),
                str(auth.label),
                str(external_uri.uri),
                str(external_uri.label),
                str(auth.longitude),
                str(auth.latitude),
                "yes",
                "",
            ]
            # nonaligned LocationAuthorities
            auth = cnx.execute("Any X WHERE X is LocationAuthority, NOT EXISTS(X same_as E)").one()
            auth_uri = urljoin(base_url, "location/{eid}".format(eid=auth.eid))
            expected_nonaligned = [
                str(auth.eid),
                str(auth_uri),
                str(auth.label),
                "",
                "",
                "",
                "",
                "yes",
                "",
            ]
            content = {
                filenames[0]: [fieldnames, expected_aligned],
                filenames[1]: [fieldnames, expected_nonaligned],
            }
            self._check_zip_archive(output_file, content)

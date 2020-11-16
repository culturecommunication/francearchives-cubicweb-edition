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

# third party imports
# CubicWeb specific imports
# library specific imports
from cubicweb_francearchives.testutils import HashMixIn
from utils import create_findingaid, TaskTC
from pgfixtures import setup_module, teardown_module  # noqa


class ComputeAlignmentsAllTC(HashMixIn, TaskTC):
    """Compute alignments (entire database) task test cases."""

    def setup_database(self):
        """Set up database."""
        super(ComputeAlignmentsAllTC, self).setup_database()
        with self.admin_access.repo_cnx() as cnx:
            # set up GeoNames
            # cities
            sql = """INSERT INTO geonames (geonameid,name,admin4_code,country_code,fclass,fcode)
            VALUES ('6455259','Paris','75056','FR','A','ADM4')"""
            cnx.system_sql(sql)
            # departments
            sql = """INSERT INTO geonames
            (geonameid,admin2_code,name,country_code,fclass,fcode)
            VALUES ('2968815','75','Paris','FR','A','ADM2')"""
            cnx.system_sql(sql)
            # set up BANO
            sql = """INSERT INTO bano_whitelisted (banoid,voie,nom_comm)
            VALUES ('751200047W-1','Rue Achille','Paris')"""
            cnx.system_sql(sql)
            # create FindingAids
            service = cnx.create_entity("Service", category="foo", code="FRAD000", name="FOO")
            findingaid0 = create_findingaid(cnx, name="foo", service=service)
            findingaid1 = create_findingaid(cnx, name="bar", service=service)
            # create 2 LocationAuthorities per label to fill output file(s)
            authority0 = cnx.create_entity("LocationAuthority", label="Paris (Paris)")
            authority1 = cnx.create_entity("LocationAuthority", label="Paris (Paris)")
            authority2 = cnx.create_entity("LocationAuthority", label="Paris -- Achille (rue)")
            authority3 = cnx.create_entity("LocationAuthority", label="Paris -- Achille (rue)")
            # create Geognames
            cnx.create_entity("Geogname", label="PARIS", index=(findingaid0,), authority=authority0)
            cnx.create_entity("Geogname", label="paris", index=(findingaid0,), authority=authority1)
            cnx.create_entity(
                "Geogname",
                label="PARIS -- ACHILLE (RUE)",
                index=(findingaid1,),
                authority=authority2,
            )
            cnx.create_entity(
                "Geogname",
                label="paris -- achille (rue)",
                index=(findingaid1,),
                authority=authority3,
            )
            cnx.commit()

    def setUp(self):
        """Set up job queue and configuration."""
        super(ComputeAlignmentsAllTC, self).setUp()
        self.config.global_set_option("appfiles-dir", str(self.datapath("appfiles")))

    def tearDown(self):
        """Clean up job queue."""
        super(ComputeAlignmentsAllTC, self).tearDown()
        filenames = glob.glob(os.path.join(self.config["appfiles-dir"], "*"))
        for filename in filenames:
            os.remove(filename)

    def get_output_file_path(self, output_file):
        """Get output file path.

        :param File output_file: output file

        :returns: output file path
        :rtype: str
        """
        return os.path.join(
            self.config["appfiles-dir"],
            "{}_{}".format(output_file.compute_hash(), output_file.data_name),
        )

    def test_compute_alignments_all_geoname(self):
        """Test computing alignments to target datasets (entire database).

        Trying: aligning to GeoNames and default file size
        Expecting: 0 subtasks and 1 output file
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "compute_alignments_all",
                    "title": "compute_alignments_all",
                    "geoname": True,
                    "bano": False,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=compute_alignments_all",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.execute('Any X WHERE X is RqTask, X name "compute_alignments_all"').one()
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # 1 output file
            self.assertEqual(len(task.output_file), 1)
            output_file = task.output_file[0]
            self.assertEqual(output_file.name, "alignment-geoname.csv")
            expected = self.get_output_file_path(output_file)
            self.assertTrue(os.path.isfile(expected))
            # CSV file contains headers + 2 authorities = 3 rows
            with io.TextIOWrapper(output_file.data, encoding="utf-8", newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                rows = [row for row in reader]
            self.assertEqual(len(rows), 3)
            # 0 subtask(s)
            self.assertEqual(len(task.subtasks), 0)

    def test_compute_alignments_all_geoname_simplified(self):
        """Test computing alignments to target datasets (entire database).

        Trying: aligning to GeoNames and default file size and simplified CSV file format
        Expecting: simplified CSV file format
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "compute_alignments_all",
                    "title": "compute_alignments_all",
                    "geoname": True,
                    "bano": False,
                    "simplified": True,
                }
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=compute_alignments_all",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            task = cnx.execute('Any X WHERE X is RqTask, X name "compute_alignments_all"').one()
            self.work(cnx)
            output_file = task.output_file[0]
            self.assertTrue(os.path.isfile(self.get_output_file_path(output_file)))
            # simplified CSV file format
            # fieldnames
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
            # rows
            eids = cnx.execute('Any X WHERE X is LocationAuthority, X label "Paris (Paris)"').rows
            auth_uri = "{base_url}/location/{{eid}}".format(
                base_url=cnx.vreg.config.get("consultation-base-url")
            )
            expected = [
                [
                    str(eid),
                    auth_uri.format(eid=eid),
                    "Paris (Paris)",
                    "http://www.geonames.org/2968815",
                    "Paris (Paris)",
                    "",
                    "",
                    "yes",
                    "1.000",
                ]
                for eid, in eids
            ]
            with io.TextIOWrapper(output_file.data, encoding="utf-8", newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                # fieldnames
                self.assertEqual(next(reader), fieldnames)
                # rows
                actual = [next(reader), next(reader)]
                self.assertCountEqual(actual, expected)

    def test_compute_alignments_all_bano(self):
        """Test computing alignments to target datasets (entire database).

        Trying: aligning to BANO and default file size
        Expecting: 1 subtask and 1 output file
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "compute_alignments_all",
                    "title": "compute_alignments_all",
                    "geoname": False,
                    "bano": True,
                },
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=compute_alignments_all",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.execute('Any X WHERE X is RqTask, X name "compute_alignments_all"').one()
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # 1 output file
            self.assertEqual(len(task.output_file), 1)
            output_file = task.output_file[0]
            self.assertEqual(output_file.name, "alignment-bano.csv")
            self.assertTrue(os.path.isfile(self.get_output_file_path(output_file)))
            # CSV file contains headers + 2 authorities = 3 rows
            with io.TextIOWrapper(output_file.data, encoding="utf-8", newline="") as fp:
                reader = csv.reader(fp, delimiter="\t")
                rows = [row for row in reader]
                self.assertEqual(len(rows), 3)
            # 1 subtask(s)
            self.assertEqual(len(task.subtasks), 1)
            self.assertEqual(task.subtasks[0].name, "import_alignment")

    def test_compute_alignments_all(self):
        """Test computing alignments to target datasets (entire database).

        Trying: aligning to GeoNames and BANO and file size 2
        Expecting: 2 subtasks (1 per output file) and 4 output files
        """
        # run task
        with self.admin_access.cnx() as cnx:
            self.login()
            data = json.dumps(
                {
                    "name": "compute_alignments_all",
                    "title": "compute_alignments_all",
                    "geoname": True,
                    "bano": True,
                    "file_size": 2,
                },
            )
            post_kwargs = {"params": [("data", data)]}
            self.webapp.post(
                "/RqTask/?schema_type=compute_alignments_all",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            # task is executed successfully
            task = cnx.execute('Any X WHERE X is RqTask, X name "compute_alignments_all"').one()
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # 2 subtask(s)
            self.assertEqual(len(task.subtasks), 2)
            self.assertEqual(task.subtasks[0].name, "import_alignment")
            # 2 output file Zip archives containing 2 output files each
            self.assertEqual(len(task.output_file), 2)
            output_files = sorted(task.output_file, key=lambda x: x.name)
            targets = ["bano", "geoname"]
            self.assertEqual(
                [output_file.name for output_file in output_files],
                ["{}.zip".format(target) for target in targets],
            )
            for target, output_file in zip(targets, output_files):
                self.assertTrue(os.path.isfile(self.get_output_file_path(output_file)))
                with zipfile.ZipFile(output_file.data) as zip_file:
                    expected = [
                        "alignment-{}-01.csv".format(target),
                        "alignment-{}-02.csv".format(target),
                    ]
                    self.assertCountEqual(zip_file.namelist(), expected)
                    for filename in zip_file.namelist():
                        with zip_file.open(filename) as fp:
                            with io.TextIOWrapper(fp, encoding="utf-8", newline="") as text:
                                # CSV files contain headers + 1 authority = 2 rows
                                reader = csv.reader(text, delimiter="\t")
                                rows = [row for row in reader]
                                self.assertEqual(len(rows), 2)

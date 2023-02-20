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
import glob
import logging
import os

# third party imports
# CubicWeb specific imports
from cubicweb import NoResultError
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

# library specific imports
from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa
from cubicweb_frarchives_edition.tasks.import_authorities import update_authorities
from cubicweb_frarchives_edition.tasks.import_alignments import update_alignments
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord, GeonameAligner
from cubicweb_frarchives_edition.alignments.authorities_align import (
    SubjectImportRecord,
    SubjectImportAligner,
)
from cubicweb_francearchives.testutils import S3BfssStorageTestMixin


class ImportTC(S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
    """Import authorities task test cases."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        """Set up database."""
        super(ImportTC, self).setup_database()
        self.import_dir = self.datapath("imp")
        if not os.path.isdir(self.import_dir):
            os.mkdir(self.import_dir)

        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("LocationAuthority", label="Cottbus")
            cnx.create_entity("LocationAuthority", label="Liège")
            cnx.create_entity("LocationAuthority", label="Leningrad")
            cnx.create_entity("LocationAuthority", label="Moscou")
            cnx.create_entity("SubjectAuthority", label="traité de Compiègne")
            cnx.create_entity("SubjectAuthority", label="M1 (carabine)")
            cnx.commit()

    def setUp(self):
        """Set up."""
        super(ImportTC, self).setUp()
        # create LocationAuthorities CSV files
        with self.admin_access.repo_cnx() as cnx:
            eid0 = cnx.find("LocationAuthority", label="Cottbus").one().eid
            eid1 = cnx.find("LocationAuthority", label="Liège").one().eid
            eid2 = cnx.find("LocationAuthority", label="Leningrad").one().eid
            eid3 = cnx.find("LocationAuthority", label="Moscou").one().eid
        with open(os.path.join(self.import_dir, "authorities-geonames.csv"), "w") as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(GeonameRecord.headers.keys()))
            writer.writerow((str(eid0), "", "", "", "Kottbus", "foo", "", "", "", "yes", "", "yes"))
            writer.writerow((str(eid1), "", "", "", "Lüttich", "foo", "", "", "", "yes", "", "no"))
            writer.writerow((str(eid1), "", "", "", "Léck", "foo", "", "", "", "yes", "", "yes"))
            writer.writerow(
                (str(eid2), "", "", "", "Petrogradr", "foo", "", "", "", "no", "", "yes")
            )
            writer.writerow((str(eid2), "", "", "", "Piter", "foo", "", "", "", "yes", "", "yes"))
            writer.writerow((str(eid3), "", "", "", "Moscou", "foo", "", "", "", "yes", "", "aaa"))
            writer.writerow(("", "", "", "", "foo", "bar", "", "", "", "yes", "", "no"))
            writer.writerow(
                ("18338507", "", "", "", "Ludwigshafen am Rhein", "foo", "", "", "", "", "aaa")
            )
        with open(
            os.path.join(self.import_dir, "authorities-geonames-missing-column.csv"), "w"
        ) as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(GeonameRecord.headers.keys())[:-3])
            writer.writerow((str(eid0), "", "", "", "Kottbus", "foo", "", "", "", "", "yes"))

        # create SubjectAuthorities CSV files
        with self.admin_access.repo_cnx() as cnx:
            eid0 = cnx.find("SubjectAuthority", label="traité de Compiègne").one().eid
            eid1 = cnx.find("SubjectAuthority", label="M1 (carabine)").one().eid
        with open(os.path.join(self.import_dir, "authorities-subjects.csv"), "w") as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(SubjectImportRecord.REQUIRED_HEADERS_QUALITY)
            writer.writerow((str(eid0), "Traité de Compiègne", "no"))
            writer.writerow((str(eid1), "M1 (carabine)", "yes"))
            writer.writerow(("1111", "Unknown", "yes"))
        with open(
            os.path.join(self.import_dir, "authorities-subjects-missing-column.csv"), "w"
        ) as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(SubjectImportRecord.REQUIRED_HEADERS_LABELS)
            writer.writerow((str(eid0), "Traité de Compiègne"))
        with open(
            os.path.join(self.import_dir, "authorities-subjects-missing-quality-column.csv"), "w"
        ) as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(
                [
                    "identifiant_SubjectAuthority",
                    "libelle_SubjectAuthority",
                    "URI_ExternalUri",
                    "keep",
                ]
            )
            writer.writerow(
                (str(eid0), "Traité de Compiègne", "https://www.wikidata.org/wiki/Q1357802", "yes")
            )

    def tearDown(self):
        """Clean up."""
        super(ImportTC, self).tearDown()
        filenames = glob.glob(os.path.join(self.import_dir, "*"))
        for filename in filenames:
            os.remove(filename)
        os.removedirs(self.import_dir)

    def test_update_labels_quality_subjects(self):
        """Test update SubjectAuthorities' labels and quality.

        Trying: update SubjectAuthorities' labels
        Expecting: SubjectAuthorities' labels are updated
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            etype = "SubjectAuthority"
            csvpath = self.get_or_create_imported_filepath("imp/authorities-subjects.csv")
            compiegne = cnx.find(etype, label="traité de Compiègne").one()
            self.assertFalse(compiegne.quality)
            carabine = cnx.find(etype, label="M1 (carabine)").one()
            self.assertFalse(carabine.quality)
            update_authorities(cnx, log, csvpath, "authorities-subjects.csv", etype)
            # 1 label has been changed, quality has not been changed
            compiegne.cw_clear_all_caches()
            self.assertEqual("Traité de Compiègne", compiegne.label)
            self.assertFalse(compiegne.quality)
            self.assertFalse(cnx.find(etype, label="traité de Compiègne"))
            # 1 label has not been changed, quality has been changed
            carabine.cw_clear_all_caches()
            self.assertEqual("M1 (carabine)", carabine.label)
            self.assertTrue(carabine.quality)
            # 1 invalid row has been ignored
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="Unknown").one()

    def test_update_labels_locations(self):
        """Test update LocationAuthorities' labels.

        Trying: update LocationAuthorities' labels
        Expecting: LocationAuthorities' labels are updated
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath("imp/authorities-geonames.csv")
            cottbus = cnx.find("LocationAuthority", label="Cottbus").one()
            update_authorities(cnx, log, csvpath, "authorities-geonames.csv", "LocationAuthority")
            # 1 label has been changed
            self.assertEqual(cottbus.eid, cnx.find("LocationAuthority", label="Kottbus")[0][0])
            # 2 conflicting user-defined labels have been ignored
            self.assertTrue(cnx.find("LocationAuthority", label="Liège").one())
            self.assertTrue(cnx.find("LocationAuthority", label="Leningrad").one())
            self.assertFalse(cnx.find("LocationAuthority", label="Piter"))
            self.assertFalse(cnx.find("LocationAuthority", label="Petrogradr"))
            # 1 unknown LocationAuthority has been ignored
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="Ludwigshafen am Rhein").one()
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="Cottbus").one()
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="Lüttich").one()
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="Léck").one()
            # 1 invalid row has been ignored
            with self.assertRaises(NoResultError):
                cnx.find("LocationAuthority", label="foo").one()

    def test_update_quality(self):
        """Test update LocationAuthorities' quality.

        Trying: update LocationAuthorities' quality
        Expecting: LocationAuthorities' quality are updated
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath("imp/authorities-geonames.csv")
            cottbus = cnx.find("LocationAuthority", label="Cottbus").one()
            self.assertFalse(cottbus.quality)
            leningrad = cnx.find("LocationAuthority", label="Leningrad").one()
            self.assertFalse(leningrad.quality)
            liege = cnx.find("LocationAuthority", label="Liège").one()
            self.assertFalse(liege.quality)
            update_authorities(cnx, log, csvpath, "authorities-geonames.csv", "LocationAuthority")
            kottbus = cnx.find("LocationAuthority", label="Kottbus").one()
            # 1 quality has been changed
            self.assertTrue(kottbus.quality)
            liege.cw_clear_all_caches()
            self.assertFalse(leningrad.quality)
            liege.cw_clear_all_caches()
            self.assertFalse(liege.quality)
            # ignore a wrong quality value
            moscou = cnx.find("LocationAuthority", label="Moscou").one()
            self.assertFalse(moscou.quality)

    def test_missing_columns_alignements(self):
        """Test missing column for alignements.

        Trying: missing column for alignement task
        Expecting: CSV is not processed
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath(
                "imp/authorities-geonames-missing-column.csv"
            )
            with self.assertRaises(ValueError):
                update_alignments(cnx, log, csvpath, GeonameAligner)

    def test_missing_columns_update_labels(self):
        """Test missing column for update labels.

        Trying: missing column for labels update tasks
        Expecting: CSV is not processed
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath(
                "imp/authorities-subjects-missing-column.csv"
            )
            with self.assertRaises(ValueError):
                update_authorities(
                    cnx, log, csvpath, "authorities-subjects-missing-column.csv", "SubjectAuthority"
                )

    def test_missing_quality_columns_alignements(self):
        """Test missing column for alignements.

        Trying: missing quality column in alignement task
        Expecting: CSV is process processed
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath(
                "imp/authorities-subjects-missing-quality-column.csv"
            )
            update_alignments(cnx, log, csvpath, SubjectImportAligner)
            compiegne = cnx.find("SubjectAuthority", label="traité de Compiègne").one()
            self.assertEqual("https://www.wikidata.org/wiki/Q1357802", compiegne.same_as[0].uri)

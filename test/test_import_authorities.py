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
import os.path

# third party imports
# CubicWeb specific imports
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.req import FindEntityError

# library specific imports
from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa
from cubicweb_frarchives_edition.tasks.import_authorities import update_labels
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord


class ImportTC(FrACubicConfigMixIn, CubicWebTC):
    """Import authorities task test cases."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        """Set up database."""
        super(ImportTC, self).setup_database()
        with self.admin_access.repo_cnx() as cnx:
            cnx.create_entity("LocationAuthority", label="Cottbus")
            cnx.create_entity("LocationAuthority", label="Liège")
            cnx.commit()

    @property
    def temp_file_path(self):
        temp_file_path = "/tmp/test-import-authorities"
        if not os.path.exists(temp_file_path):
            os.makedirs(temp_file_path)
        return temp_file_path

    def setUp(self):
        """Set up."""
        super(ImportTC, self).setUp()
        with self.admin_access.repo_cnx() as cnx:
            eid0 = cnx.find_one_entity("LocationAuthority", label="Cottbus").eid
            eid1 = cnx.find_one_entity("LocationAuthority", label="Liège").eid
        with open(os.path.join(self.temp_file_path, "authorities-geonames.csv"), "w") as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(GeonameRecord.headers.keys()))
            writer.writerow((str(eid0), "", "", "", "Kottbus", "foo", "", "", "", "yes", ""))
            writer.writerow((str(eid1), "", "", "", "Lüttich", "foo", "", "", "", "yes", ""))
            writer.writerow((str(eid1), "", "", "", "Léck", "foo", "", "", "", "yes", ""))
            writer.writerow(("", "", "", "", "foo", "bar", "", "", "", "yes", ""))
            writer.writerow(
                ("18338507", "", "", "", "Ludwigshafen am Rhein", "foo", "", "", "", "yes", "")
            )
        with open(
            os.path.join(self.temp_file_path, "authorities-geonames-missing-column.csv"), "w"
        ) as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(GeonameRecord.headers.keys())[:-3])
            writer.writerow((str(eid0), "", "", "", "Kottbus", "foo", "", "", "", "yes", ""))

    def tearDown(self):
        """Clean up."""
        super(ImportTC, self).tearDown()
        filenames = glob.glob(os.path.join(self.temp_file_path, "*"))
        for filename in filenames:
            os.remove(filename)
        os.removedirs(self.temp_file_path)

    def test_update_labels(self):
        """Test update LocationAuthorities' labels.

        Trying: update LocationAuthorities' labels
        Expecting: LocationAuthorities' labels are updated
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = os.path.join(self.temp_file_path, "authorities-geonames.csv")
            update_labels(cnx, log, csvpath)
            # 1 label has been changed
            self.assertTrue(cnx.find_one_entity("LocationAuthority", label="Kottbus"))
            # 2 conflicting user-defined labels have been ignored
            self.assertTrue(cnx.find_one_entity("LocationAuthority", label="Liège"))
            # 1 unknown LocationAuthority has been ignored
            with self.assertRaises(FindEntityError):
                cnx.find_one_entity("LocationAuthority", label="Ludwigshafen am Rhein")
            # 1 invalid row has been ignored
            with self.assertRaises(FindEntityError):
                cnx.find_one_entity("LocationAuthority", label="foo")

    def test_missing_column(self):
        """Test missing column.

        Trying: missing column
        Expecting: CSV is not processed
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = os.path.join(self.temp_file_path, "authorities-geonames-missing-column.csv")
            with self.assertRaises(ValueError):
                update_labels(cnx, log, csvpath)

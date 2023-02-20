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
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

# library specific imports
from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa
from cubicweb_frarchives_edition.tasks.qualify_authorities import (
    process_qualification,
    process_quality,
    FIELDNAMES,
    AUTH_URL_PATTERN,
)

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin


class ImportTC(S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
    """Import authorities task test cases."""

    configcls = PostgresApptestConfiguration
    filename = "qualified_authorities.csv"

    def setup_database(self):
        """Set up database."""
        super(ImportTC, self).setup_database()
        self.import_dir = self.datapath("qualif")
        if not os.path.isdir(self.import_dir):
            os.mkdir(self.import_dir)

    def tmp_filepath(self, filepath):
        return self.get_or_create_imported_filepath(f"qualif/{filepath}")

    def setUp(self):
        """Set up."""
        super(ImportTC, self).setUp()
        self.data = [
            ("AgentAuthority", "Juppé, Alain (1945-...)", "yes"),
            ("SubjectAuthority", "Seconde Guerre mondiale (1939-1945)", "oui"),
            ("LocationAuthority", "Leningrad (Russie)", "yes"),
            ("SubjectAuthority", "guerre 1939-1945", ""),
            ("AgentAuthority", "Jacques Martin", "non"),
            ("AgentAuthority", "Jean Martin", "no"),
            ("SubjectAuthority", "guerre quarante", "non"),
            ("SubjectAuthority", "prison", "test"),
            ("LocationAuthority", "Toulouse (Haute-Garonne, France)", "yes"),
        ]
        with self.admin_access.repo_cnx() as cnx:
            with open(os.path.join(self.import_dir, self.filename), "w") as fp:
                writer = csv.writer(fp, delimiter="\t")
                writer.writerow(FIELDNAMES.keys())
                for idx, (etype, label, quality) in enumerate(self.data):
                    entity = cnx.create_entity(etype, label=label)
                    cnx.commit()
                    if idx == 0:
                        identifier = entity.absolute_url()
                    elif idx == 1:
                        identifier = entity.rest_path()
                    elif idx == 2:
                        identifier = str(entity.eid)
                    else:
                        identifier = f"{entity.absolute_url()}?es_escategory=archives&es_escategory=siteres"  # noqa
                    writer.writerow((identifier, entity.label, str(quality)))

    def tearDown(self):
        """Clean up."""
        super(ImportTC, self).tearDown()
        filenames = glob.glob(os.path.join(self.import_dir, "*"))
        for filename in filenames:
            os.remove(filename)
        os.removedirs(self.import_dir)

    def test_process_qualification(self):
        """Test authorities qualification process.

        Trying: update authorities qualification
        Expecting: authorities qualification are updated
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            csvpath = self.get_or_create_imported_filepath(f"qualif/{self.filename}")
            process_qualification(cnx, csvpath, FIELDNAMES, log)
            guerre = cnx.find("SubjectAuthority", label="guerre 1939-1945").one()
            guerre.cw_set(quality=True)
            cnx.commit()
            for etype, label, quality in self.data:
                entity = cnx.find(etype, label=label).one()
                if entity.eid == guerre.eid:
                    self.assertTrue(entity.quality)
                try:
                    qualification = process_quality(quality)
                except Exception:
                    self.assertIn(quality, ("test", ""))
                    continue
                self.assertEqual(qualification, entity.quality)

    def test_auth_url_pattern_re(self):
        """TEST AUTH_URL_PATTERN
        Trying: test autorized value for indentifier
        Expecting: authority eid is matched
        """
        for url in (
            "https://francearchives.fr/fr/agent/18486745",
            "agent/18486745",
            "18486745",
            "https://francearchives.fr/fr/agent/18486745?es_escategory=archives&es_escategory=siteres",  # noqa
        ):
            self.assertEqual("18486745", AUTH_URL_PATTERN.match(url)["eid"])

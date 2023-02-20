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
import logging
import os
import shutil

# third party imports
# CubicWeb specific imports
from cubicweb.devtools.testlib import CubicWebTC

# library specific imports
from pgfixtures import setup_module, teardown_module  # noqa

from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord
from cubicweb_frarchives_edition.tasks.import_authorities import update_authorities

from cubicweb_francearchives import Authkey
from cubicweb_francearchives.utils import merge_dicts
from cubicweb_francearchives.testutils import EADImportMixin, PostgresTextMixin
from cubicweb_francearchives.dataimport.sqlutil import delete_from_filename


class AuthoritiesHistoryTC(EADImportMixin, PostgresTextMixin, CubicWebTC):
    readerconfig = merge_dicts({}, EADImportMixin.readerconfig, {"nodrop": False})

    def setUp(self):
        """Set up database."""
        super(AuthoritiesHistoryTC, self).setUp()
        self.import_dir = self.datapath("imp")
        if not os.path.isdir(self.import_dir):
            os.mkdir(self.import_dir)

    def tearDown(self):
        """Tear down test cases."""
        super(AuthoritiesHistoryTC, self).tearDown()
        if os.path.exists(self.import_dir):
            shutil.rmtree(self.import_dir)

    def write_authorities_geoname(self, data):
        with open(os.path.join(self.import_dir, "authorities-geonames.csv"), "w") as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(GeonameRecord.headers.keys()))
            for autheid, new_label in data:
                writer.writerow(
                    (str(autheid), "", "", "", new_label, "foo", "", "", "", "yes", "", "no")
                )

    def test_update_location_label(self):
        """
        Trying: import an IR and rename a related LocationAuthority.
                Delete and reimport it again.
        Expecting: no new AgentAutority is created as the renamed authority
               is kept in authority_history
        """
        log = logging.getLogger("rq.task")
        with self.admin_access.cnx() as cnx:
            filepath = "ir_data/FRAD008/FRAD008_INV08_07.xml"
            self.import_filepath(cnx, filepath)
            old_locations = [e[0] for e in cnx.find("LocationAuthority")]
            old_label = "Sanciat"
            sanciat = cnx.find("LocationAuthority", label=old_label).one()
            new_label = "Sarlat"
            self.write_authorities_geoname([(sanciat.eid, new_label)])
            csvpath = self.get_or_create_imported_filepath("imp/authorities-geonames.csv")
            update_authorities(cnx, log, csvpath, "authorities-geonames.csv", "LocationAuthority")
            self.assertEqual(new_label, cnx.find("LocationAuthority", eid=sanciat.eid).one().label)
            indexed_irs = [
                e[0]
                for e in cnx.execute(
                    """Any SI ORDERBY SI WHERE I authority X, I index F, I label L,
                   F stable_id SI, X eid %(eid)s""",
                    {"eid": sanciat.eid},
                )
            ]
            # delete the imported IR
            delete_from_filename(cnx, "FRAD008_INV08_07.xml", interactive=False, esonly=False)
            cnx.commit()
            self.import_filepath(cnx, filepath, autodedupe_authorities="service/normalize")
            # assert no new LocationAuthority with old_label has been created
            new_locations = [e[0] for e in cnx.find("LocationAuthority")]
            self.assertCountEqual(old_locations, new_locations)
            self.assertFalse(cnx.find("LocationAuthority", label=old_label))
            self.assertTrue(sanciat.eid, cnx.find("LocationAuthority", label=new_label)[0][0])
            # test FAComponents are correctly attached to the sanciat authority
            new_indexed_irs = [
                e[0]
                for e in cnx.execute(
                    """Any SI ORDERBY SI  WHERE I authority X, I index F, I label L,
                F stable_id SI, X eid %(eid)s""",
                    {"eid": sanciat.eid},
                )
            ]
            self.assertEqual(indexed_irs, new_indexed_irs)
            cu = cnx.system_sql(
                """
                SELECT fa_stable_id, type, label, indexrole, autheid
                FROM authority_history ORDER BY fa_stable_id"""
            )
            auth_history = []
            for fa_stable_id, type, label, indexrole, auth in cu.fetchall():
                key = Authkey(fa_stable_id, type, label, indexrole)
                auth_history.append((key.as_tuple(), auth))
            expected = [
                (
                    ("2b01e9fbd139ccdb7adda4b6a25cf5964b4b9303", "geogname", old_label, "index"),
                    sanciat.eid,
                )
            ]
            self.assertEqual(expected, auth_history)

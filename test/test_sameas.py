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
# library specific imports
from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa

from cubicweb_frarchives_edition.alignments import compute_label_from_url

from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC


class FaapiTC(FrACubicConfigMixIn, CubicWebTC):
    """FranceArchives API utils test cases."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        """Set database up."""
        super(FaapiTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            cursor = cnx.cnxset.cu
            sql = """
            INSERT INTO geonames
            (geonameid, name, country_code, admin1_code, admin2_code, fcode, fclass)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            geonames = [
                ("1234567", "Fontenay-sous-Bois", "FR", "11", "94", "", ""),
                ("2345678", "Fontenay-sous-Bois", "FR", "11", "", "", ""),
                ("3456789", "Fontenay-sous-Bois", "FR", "", "94", "", ""),
                ("4567890", "Fontenay-sous-Bois", "FR", "", "", "", ""),
                ("2935453", "Dossenheim", "DE", "", "", "", ""),
                ("2921044", "Federal Republic of Germany", "DE", "", "", "PCLI", "A"),
                ("524901", "Moscow", "ru", "", "", "", ""),
                ("2971090", "Val-de-Marne", "FR", "11", "94", "ADM2", "A"),
                ("3012874", "Île-de-France", "FR", "11", "", "ADM1", "A"),
            ]
            cursor.executemany(sql, geonames)
            cnx.commit()
            sql = """
            INSERT INTO geonames_altnames
            (alternatenameid, geonameid, isolanguage, alternate_name, ispreferredname,
              rank)
            VALUES (%s,%s,%s,%s,%s,%s)
            """
            altnames = [
                ("311867", "524901", "fr", "Moscou", "f", 1),
                ("1590979", "524901", "en", "Moscow", "t", 1),
                ("1557491", "2921044", "fr", "République allemande", "f", 2),
                ("1557490", "2921044", "fr", "Allemagne", "t", 1),
            ]
            cursor.executemany(sql, altnames)
            cnx.commit()

    def test_compute_label_from_url(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/1234567
        Expecting: 'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/1234567"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Fontenay-sous-Bois (Île-de-France, Val-de-Marne)")

    def test_compute_label_from_url_no_admin2_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/2345678
        Expecting: 'Fontenay-sous-Bois (Île-de-France)'
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/2345678"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Fontenay-sous-Bois (Île-de-France)")

    def test_compute_label_from_url_no_admin1_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/3456789
        Expecting: 'Fontenay-sous-Bois (Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/3456789"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Fontenay-sous-Bois (Val-de-Marne)")

    def test_compute_label_from_url_no_admin_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/4567890
        Expecting: 'Fontenay-sous-Bois'
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/4567890"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Fontenay-sous-Bois")

    def test_create_geonames_foreign_city_with_country(self):
        """Test GeoNames label creation for foreign cities with country.

        Trying: https://www.geonames.org/2935453 (not in France)
        Expecting: 'Dossenheim (Allemagne)'

        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/2935453"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Dossenheim (Allemagne)")

    def test_create_geonames_foreign_city_without_country(self):
        """Test GeoNames label creation for foreign cities without country.

        Trying: https://www.geonames.org/524901
        Expecting: 'Moscou'

        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/524901"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Moscou")

    def test_compute_label_from_url_not_in_table(self):
        """Test GeoNames label creation for inexisting geonameid.

        Trying: not in GeoNames table
        https://www.geonames.org/5678901
        Expecting: empty string
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/5678901"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "")

    def test_compute_label_from_full_url(self):
        """Test GeoNames label creation from the full geonames url

        Trying: https://www.geonames.org/1234567/fontenay-sous-bois.html
        Expecting: 'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = "https://www.geonames.org/1234567/fontenay-sous-bois.html"
            label = compute_label_from_url(cnx, url)
            self.assertEqual(label, "Fontenay-sous-Bois (Île-de-France, Val-de-Marne)")

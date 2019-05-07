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

from cubicweb_frarchives_edition.faapi import create_label

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
            sql = '''
            INSERT INTO geonames
            (geonameid, name, country_code, admin1_code, admin2_code, fcode)
            VALUES (%s,%s,%s,%s,%s,%s)
            '''
            geonames = [
                (u'1234567', u'Fontenay-sous-Bois', u'FR', u'11', u'94', u''),
                (u'2345678', u'Fontenay-sous-Bois', u'FR', u'11', u'', u''),
                (u'3456789', u'Fontenay-sous-Bois', u'FR', u'', u'94', u''),
                (u'4567890', u'Fontenay-sous-Bois', u'FR', u'', u'', u''),
                (u'2935453', u'Dossenheim', u'DE', u'', u'', u''),
                (u'2921044', u'Federal Republic of Germany', u'DE', u'', u'', u'PCLI'),
                (u'524901', u'Moscou', u'ru', u'', u'', u'')
            ]
            cursor.executemany(sql, geonames)
            cnx.commit()

    def test_create_geonames_label(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/1234567
        Expecting: 'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/1234567'
            label = create_label(cnx, url)
            self.assertEqual(
                label, u'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
            )

    def test_create_geonames_label_no_admin2_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/2345678
        Expecting: 'Fontenay-sous-Bois (Île-de-France)'
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/2345678'
            label = create_label(cnx, url)
            self.assertEqual(
                label, u'Fontenay-sous-Bois (Île-de-France)'
            )

    def test_create_geonames_label_no_admin1_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/3456789
        Expecting: 'Fontenay-sous-Bois (Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/3456789'
            label = create_label(cnx, url)
            self.assertEqual(
                label, u'Fontenay-sous-Bois (Val-de-Marne)'
            )

    def test_create_geonames_label_no_admin_code(self):
        """Test GeoNames label creation.

        Trying: https://www.geonames.org/4567890
        Expecting: 'Fontenay-sous-Bois'
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/4567890'
            label = create_label(cnx, url)
            self.assertEqual(
                label, u'Fontenay-sous-Bois'
            )

    def test_create_geonames_foreign_city_with_country(self):
        """Test GeoNames label creation for foreign cities with country.

        Trying: https://www.geonames.org/2935453 (not in France)
        Expecting: 'Dossenheim (Allemagne)'

        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/2935453'
            label = create_label(cnx, url)
            self.assertEqual(label, 'Dossenheim (Allemagne)')

    def test_create_geonames_foreign_city_without_country(self):
        """Test GeoNames label creation for foreign cities without country.

        Trying: https://www.geonames.org/524901
        Expecting: 'Moscou'

        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/524901'
            label = create_label(cnx, url)
            self.assertEqual(label, u'Moscou')

    def test_create_geonames_label_not_in_table(self):
        """Test GeoNames label creation for inexisting geonameid.

        Trying: not in GeoNames table
        https://www.geonames.org/5678901
        Expecting: empty string
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/5678901'
            label = create_label(cnx, url)
            self.assertEqual(label, u'')

    def test_create_geonames_label_full_url(self):
        """Test GeoNames label creation from the full geonames url

        Trying: https://www.geonames.org/1234567/fontenay-sous-bois.html
        Expecting: 'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
        """
        with self.admin_access.cnx() as cnx:
            url = u'https://www.geonames.org/1234567/fontenay-sous-bois.html'
            label = create_label(cnx, url)
            self.assertEqual(
                label, u'Fontenay-sous-Bois (Île-de-France, Val-de-Marne)'
            )

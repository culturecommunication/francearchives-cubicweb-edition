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

from pgfixtures import setup_module, teardown_module  # noqa
from utils import FrACubicConfigMixIn

from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_frarchives_edition.alignments.group_locations import (
    process_candidates,
    Label,
    compute_location_authorities_to_group,
)


class GroupLocationsTC(FrACubicConfigMixIn, CubicWebTC):
    """Group locations task test cases."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        super(GroupLocationsTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            sqlcursor = cnx.cnxset.cu
            # populated place features
            sql = """INSERT INTO geonames (geonameid,name,admin4_code,country_code,fclass,fcode)
            VALUES (%s,%s,%s,%s,%s,%s)"""
            ppl = [
                ("2802361", "Belgium", "", "BE", "A", "PCLI"),
                ("3017382", "France", "", "FR", "A", "PCLI"),
                ("2972315", "Toulouse", "31555", "FR", "P", "PPLA"),
            ]
            sqlcursor.executemany(sql, ppl)
            cnx.commit()
            # populate alternate_names
            sql = """INSERT INTO geonames_altnames
            (geonameid,alternatenameid, alternate_name, isolanguage)
            VALUES (%s,%s,%s,%s)"""
            countries = [
                ("2802361", "28023611", "Belgique", "fr"),
                ("3017382", "30173822", "France'", "fr"),
            ]
            sqlcursor.executemany(sql, countries)
            cnx.commit()

    def create_candidates(self, cnx, quality=None):
        candidates = [
            Label(
                cnx,
                "Rochefort (Charente-Maritime)",
                18354267,
                "false",
                1,
                0,
                0,
                "rochefort",
                "charente maritime",
                "",
                "",
            ),
            Label(
                cnx,
                "Rochefort-sur-Mer (Charente-Maritime, France)",
                30326760,
                "false",
                1,
                0,
                2,
                "rochefort sur mer",
                "charente maritime",
                "",
                "france",
            ),
            Label(
                cnx,
                "Rochefort (Charente-Maritime, France)",
                130944149,
                "false",
                1,
                0,
                2,
                "rochefort",
                "charente maritime",
                "",
                "france",
            ),
            Label(
                cnx,
                "Rochefort (France)",
                130944150,
                "false",
                0,
                0,
                2,
                "rochefort",
                "charente maritime",
                "",
                "france",
            ),
        ]
        if quality:
            for candidate in candidates:
                if candidate.eid == quality:
                    candidate.quality = "true"
                    candidate.update_quality("true")
        return candidates

    def test_no_quality_candidates(self):
        """Test candidates are sorted by label, and score calculated as below:
        1/ quality
        2/ department, country
        3/ country, department
        4/ department, region, country
        5/ country, region, department
        6/ other combinaisons avec country, region, department
        7/ department
        8/ region, country
        9/ country, region
        """
        with self.admin_access.cnx() as cnx:
            candidates = self.create_candidates(cnx)
            to_be_grouped, not_to_be_grouped = process_candidates(candidates)
            for label_to, other_labels in list(to_be_grouped.items()):
                self.assertEqual("rochefort", label_to)
                self.assertEqual(
                    [(18354267, "false"), (130944149, "false"), (130944150, "false")],
                    [(o.eid, o.quality) for o in other_labels],
                )

            for label_to, other_labels in list(not_to_be_grouped.items()):
                self.assertEqual("rochefort sur mer", label_to)
                self.assertEqual(30326760, other_labels[0].eid)
                self.assertEqual(
                    [18354267, 130944149, 130944150], sorted([o.eid for o in other_labels[1:]])
                )

    def test_quality_candidates(self):
        """Test candidates are sorted by label, and score calculated as below:
        1/ quality
        2/ department, country
        3/ country, department
        4/ department, region, country
        5/ country, region, department
        6/ other combinaisons avec country, region, department
        7/ department
        8/ region, country
        9/ country, region
        """
        with self.admin_access.cnx() as cnx:
            candidates = self.create_candidates(cnx, quality=18354267)
            to_be_grouped, not_to_be_grouped = process_candidates(candidates)
            for label_to, other_labels in list(to_be_grouped.items()):
                self.assertEqual("rochefort", label_to)
                self.assertEqual(
                    [(18354267, "true"), (130944149, "false"), (130944150, "false")],
                    [(o.eid, o.quality) for o in other_labels],
                )

            for label_to, other_labels in list(not_to_be_grouped.items()):
                self.assertEqual("rochefort sur mer", label_to)
                self.assertEqual(30326760, other_labels[0].eid)
                self.assertEqual(
                    [18354267, 130944149, 130944150], sorted([o.eid for o in other_labels[1:]])
                )

    def test_foreigncountries_candidates(self):
        """Candidates for foreign countries can be grouped"""
        with self.admin_access.cnx() as cnx:
            bruxelles = cnx.create_entity(
                "ExternalUri",
                source="geoname",
                label="Bruxelles (Belgique)",
                uri="https://www.geonames.org/2802361",
            )
            cnx.create_entity(
                "LocationAuthority",
                label="Bruxelles (Belgique)",
                same_as=bruxelles,
                reverse_authority=cnx.create_entity("Geogname", label="Bruxelles (Belgique)"),
            )
            cnx.create_entity(
                "LocationAuthority",
                label="Bruxelles (Belgique)",
                same_as=bruxelles,
                reverse_authority=cnx.create_entity("Geogname", label="Bruxelles (Belgique)"),
            )
            # Metropole de lyon" is found as departement in geodata
            cnx.commit()
            to_be_grouped, not_to_be_grouped = compute_location_authorities_to_group(cnx)
            self.assertEqual(1, len(to_be_grouped.items()))

    def test_toulouse_complete_candidates(self):
        """Test toulouse with "Metropole de lyon" is department as it is found as
        departement in geodata"""
        with self.admin_access.cnx() as cnx:
            toulouse = cnx.create_entity(
                "ExternalUri",
                source="geoname",
                label="Toulouse (Metropole de lyon, France)",
                uri="https://www.geonames.org/2972315",
            )
            cnx.create_entity(
                "LocationAuthority",
                label="Toulouse (Metropole de lyon, France)",
                same_as=toulouse,
                reverse_authority=cnx.create_entity("Geogname", label="Toulouse"),
            )
            t2 = cnx.create_entity(
                "LocationAuthority",
                label="Toulouse (Metropole de lyon, France)",
                same_as=toulouse,
                reverse_authority=cnx.create_entity("Geogname", label="Toulouse"),
            )
            cnx.commit()
            to_be_grouped, not_to_be_grouped = compute_location_authorities_to_group(cnx)
            self.assertEqual(1, len(to_be_grouped.items()))
            for label_to, other_labels in list(to_be_grouped.items()):
                self.assertEqual("Toulouse (Metropole de lyon, France)", label_to.label)
                self.assertEqual([str(t2.eid)], [o.eid for o in other_labels])

    def test_toulouse_incomplete_candidates(self):
        with self.admin_access.cnx() as cnx:
            toulouse = cnx.create_entity(
                "ExternalUri",
                source="geoname",
                label="Toulouse (Metropole de lyon, France)",
                uri="https://www.geonames.org/2972315",
            )
            cnx.create_entity(
                "LocationAuthority",
                label="Toulouse (France)",
                same_as=toulouse,
                reverse_authority=cnx.create_entity("Geogname", label="Toulouse"),
            )
            t2 = cnx.create_entity(
                "LocationAuthority",
                label="Toulouse (France)",
                same_as=toulouse,
                reverse_authority=cnx.create_entity("Geogname", label="Toulouse"),
            )
            cnx.commit()
            to_be_grouped, not_to_be_grouped = compute_location_authorities_to_group(cnx)
            self.assertEqual(1, len(to_be_grouped.items()))
            for label_to, other_labels in list(to_be_grouped.items()):
                self.assertEqual("Toulouse (France)", label_to.label)
                self.assertEqual([str(t2.eid)], [o.eid for o in other_labels])

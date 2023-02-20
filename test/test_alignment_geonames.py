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
from pytest import xfail
from pgfixtures import setup_module, teardown_module  # noqa

from io import StringIO
from mock import patch

from utils import FrACubicConfigMixIn, create_findingaid
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from nazca.rl.aligner import BaseAligner, PipelineAligner
from nazca.rl.blocking import MinHashingBlocking, PipelineBlocking
from nazca.utils.distances import BaseProcessing
from nazca.utils.normalize import NormalizerPipeline, simplify, SimplifyNormalizer
from nazca.utils.minhashing import Minlsh

from cubicweb_frarchives_edition import get_samesas_history
from cubicweb_frarchives_edition.alignments import geonames_align, location


class GeonamesAlignTaskBaseTC(FrACubicConfigMixIn, CubicWebTC):
    """GeoNames test cases base class."""

    configcls = PostgresApptestConfiguration

    def setup_database(self):
        super(GeonamesAlignTaskBaseTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            sqlcursor = cnx.cnxset.cu
            # populated place features
            sql = """INSERT INTO geonames (geonameid,name,country_code,admin4_code,fclass,fcode)
            VALUES (%s,%s,%s,%s,%s,%s)"""
            ppl = [
                ("2988507", "Paris", "FR", "", "P", "PPLC"),
                ("2972328", "Toulon", "FR", "83137", "P", "PPL"),
                ("3000378", "Les Marrons", "FR", "05153", "P", "PPL"),
                ("2977998", "Saint-Michel-de-Chaillol", "FR", "05153", "P", "PPL"),
                ("3004994", "Le Breuil", "FR", "78265", "P", "PPL"),
                ("3004993", "Le Breuil", "FR", "51085", "P", "PPL"),
                ("3005001", "Le Breuil", "FR", "71059", "P", "PPL"),
                ("3005010", "Le Breuil", "FR", "69026", "P", "PPL"),
                ("2994172", "Mesvres", "FR", "71297", "P", "PPL"),
                ("2970761", "Vanves", "FR", "92075", "P", "PPL"),
                ("3015894", "Givry", "FR", "71221", "P", "PPL"),
                ("3037394", "Anzy-le-Duc", "FR", "71011", "P", "PPL"),
                ("3031090", "Bourbon-Lancy", "FR", "71047", "P", "PPL"),
                ("2986468", "Poligny", "FR", "05104", "P", "PPL"),
                ("3173435", "Milan", "IT", "", "P", "PPL"),
                ("2800866", "Brussels", "BE", "21004", "P", "PPLC"),
                ("702550", "Lviv", "UA", "", "P", "PPLA"),
                ("702548", "Lviv", "UA", "", "P", "PPL"),
                ("1172451", "Lahore", "PK", "", "P", "PPLA"),
            ]
            sqlcursor.executemany(sql, ppl)
            # populated place features
            sql = """INSERT INTO geonames (
                                 geonameid,name,country_code,admin1_code,
                                 admin2_code,admin3_code,admin4_code,fclass,fcode)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            ppl = [
                ("2972315", "Toulouse", "FR", "76", "31", "313", "31555", "P", "PPLA"),
                ("6453974", "Toulouse", "FR", "76", "31", "313", "31555", "A", "ADM4"),
                ("6332218", "Toulouse", "US", "KY", "131", None, None, "P", "PPL"),
                ("6428080", "Saint-Jean-d'Angély", "FR", "75", "17", "175", "17347", "A", "ADM4"),
                ("2979363", "Saint-Jean-d'Angély", "FR", "75", "17", "175", "17347", "P", "PPLA3"),
                ("3013767", "Upper Garonne", "FR", "76", "31", None, None, "A", "ADM2"),  # dpt
            ]
            sqlcursor.executemany(sql, ppl)
            # cities
            sql = """INSERT INTO geonames (geonameid,name,admin4_code,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','A','ADM4')"""
            cities = [
                ("6442320", "Mesvres", "71297"),
                ("6450842", "Anzy-le-Duc", "71011"),
                ("6455259", "Paris", "75056"),
                ("6450870", "Bourbon-Lancy", "71047"),
                ("6612862", "Toulon", "83137"),
                ("6457024", "Le Breuil", "69026"),
                ("6613135", "Vanves", "92075"),
                ("6616930", "Le Breuil", "51085"),
                ("6614558", "Le Breuil", "71059"),
                ("6617524", "Givry", "71221"),
                ("6425641", "Saint-Michel-de-Chaillol", "05153"),
                ("6425608", "Poligny", "05104"),
                ("6446924", "Bar-sur-Seine", "10034"),
                ("6449898", "Aube", "57037"),
            ]
            sqlcursor.executemany(sql, cities)
            # departments
            sql = """INSERT INTO geonames
            (geonameid,admin2_code,name,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','A','ADM2')"""
            departments = [
                ("2968815", "75", "Paris"),
                ("2970749", "83", "Var"),
                ("3013738", "05", "Hautes-Alpes"),
                ("2967196", "78", "Yvelines"),
                ("2976082", "71", "Département de Saône-et-Loire"),
                ("2995603", "51", "Marne"),
                ("2987410", "69", "Département du Rhône"),
                ("3013657", "92", "Hauts-de-Seine"),
                ("2971071", "95", "Département du Val-d’Oise"),
                ("3028791", "15", "Cantal"),
                ("3013793", "2B", "Département de la Haute-Corse"),
                ("3031359", "13", "Département des Bouches-du-Rhône"),
                ("3020781", "26", "Département de la Drôme"),
                ("3036420", "10", "Département de l'Aube"),
                ("3026644 ", "17", "Charente-Maritime"),
            ]
            sqlcursor.executemany(sql, departments)
            # regions
            regions = [
                ("2985244", "93", "Provence-Alpes-Côte d'Azur"),
                ("3012874", "11", "Île-de-France"),
                ("11071619", "27", "Bourgogne-Franche-Comté"),
                ("11071622", "44", "Grand Est"),
                ("11071623", "76", "Occitanie"),
                ("11071620", "75", "Nouvelle-Aquitaine"),
            ]
            sql = """INSERT INTO geonames
            (geonameid,admin1_code,name,country_code,fclass,fcode)
            VALUES (%s,%s,%s,'FR','A','ADM1')"""
            sqlcursor.executemany(sql, regions)
            # countries
            countries = [
                ("France", "3017382", "FR", "1556321", None),
                ("Algérie", "2589581", "DZ", "1557027", None),
                ("Belgique", "2802361", "BE", "1559635", None),
                ("Allemagne", "2921044", "DE", "1557490", None),
                ("Île Maurice", "934292", "MU", "2256656", None),
                ("Ukraine", "690791", "UA", "1564432", None),
                ("Pakistan", "1168579", "PK", "1557611", None),
            ]
            alt_names = [
                ("République Française", "3017382", "FR", "1291074", None),
                ("Île de France", "934292", "MU", "518473", True),
            ]
            sql = """INSERT INTO geonames_altnames
            (alternate_name,geonameid,isolanguage,alternatenameid,ishistoric)
            VALUES(%s,%s,'fr',%s,%s)"""
            sqlcursor.executemany(
                sql,
                [
                    (altname, geonameid, alternatenameid, ishistoric)
                    for altname, geonameid, _, alternatenameid, ishistoric in countries + alt_names
                ],
            )
            # altnames
            cities = [
                ("Bruxelles", "2800866", "fr", "1260587", None, None, 1),
                ("Lviv", "702550", "fr", "1649879", None, None, 1),
                ("Lviv", "702548", "fr", "16606915", None, None, 1),
                ("Lahore", "1172451", "fr", "1899630", None, None, 1),
                ("Lâhore", "1172451", "fr", "1596968", None, None, 2),
                ("Toulouse", "2972314", "fr", "16254299", None, None, 1),
                ("Toulouse", "6453974", "fr", "9495607", None, None, 1),
                ("Toulouse", "2972315", "fr", "1601977", None, None, 1),
                ("Saint-Jean-d'Angély", "2979363", "fr", "16314570", None, None, 1),
                ("Haute-Garonne", "3013767", "fr", "2187116", True, None, 1),  # dpt
                (
                    "Département de la Haute-Garonne",
                    "3013767",
                    "fr",
                    "2080345",
                    None,
                    None,
                    2,
                ),  # dpt
                ("Occitanie", "11071623", "fr", "11839623", None, None, 1),
                ("Languedoc-Roussillon-Midi-Pyrénées", "11071623", "fr", "11712764", True, None, 2),
                ("Région Occitanie", "11071623", "fr", "11839625", None, None, 3),
                (
                    "Région Occitanie (Pyrénées-Méditerranée)",
                    "11071623",
                    "fr",
                    "11839626",
                    None,
                    None,
                    4,
                ),
                ("Charente-Maritime", "3026644", "fr", "2187099", True, None, 1),
                ("Département de la Charente-Maritime", "3026644", "fr", "2080374", True, None, 2),
                ("Nouvelle-Aquitaine", "11071620", "fr", "12791523", None, None, 1),
            ]
            sql = """INSERT INTO geonames_altnames
            (alternate_name,geonameid,isolanguage,alternatenameid,ispreferredname,ishistoric,rank)
            VALUES(%s,%s,%s,%s,%s,%s,%s)"""
            sqlcursor.executemany(
                sql,
                [
                    (
                        alternate_name,
                        geonameid,
                        isolanguage,
                        alternatenameid,
                        ispreferredname,
                        ishistoric,
                        rank,
                    )
                    for alternate_name, geonameid, isolanguage, alternatenameid, ispreferredname, ishistoric, rank in cities  # noqa
                ],
            )
            sql = """INSERT INTO geonames (geonameid,country_code,fcode)
            VALUES (%s,%s,'PCLI')"""
            sqlcursor.executemany(
                sql, [(geonameid, country_code) for _, geonameid, country_code, _, _ in countries]
            )
            # update latitude(s) and longitude(s)
            cnx.system_sql(
                """UPDATE geonames SET latitude=0.0,longitude=0.0
                WHERE geonameid='2988507'"""
            )
            cnx.commit()


class GeonamesAlignTC(GeonamesAlignTaskBaseTC):
    """The test cases showcase the expected alignment given the manually
    inserted data.

    They do not in any way represent the alignment given a complete dataset
    and should be used for debugging purposes only.
    """

    def test_paris(self):
        """Test label that does not contain context.

        Trying: Paris
        Expecting: is aligned to  Paris (Île-de-France, Paris)
        """
        rows = [["1234567890", "", "", "", "Paris", "", "", "75", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, records_dptonly, _, _ = geonames_align.build_record(rows, geodata)
        geonameid = 2988507
        geonames = [
            [geonameid, "Paris", ("paris", "paris", None, None), "", "", "Paris (paris)", "", ""]
        ]  # populated place
        pairs = location.alignment_geo_data(records_dptonly, geonames)
        cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_dptonly))
        self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_toulon(self):
        """Test label that contains dpt as context.

        Trying: Toulon (Var)
        Expecting: is aligned to Toulon (Provence-Alpes-Côte d'Azur, Var)
        """
        rows = [["1234567890", "", "", "", "Toulon (Var)", "", "Var", "83", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
        geonameid = 2972328
        geonames = [
            [
                geonameid,
                "Toulon",
                ("var", "toulon", None, None),
                "",
                "",
                "Toulon (var, toulon)",
                "",
                "",
            ]
        ]
        pairs = location.alignment_geo_data(records, geonames)
        cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
        self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_marrons(self):
        """Test label that contains dpt and city as context.

        Trying: Les Marrons (Saint-Michel-de-Chaillol, Hautes-Alpes; hameau)
        Expecting: is aligned to Saint-Michel-de-Chaillol
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Les Marrons (Saint-Michel-de-Chaillol, Hautes-Alpes; hameau)",
                "",
                "Hautes-Alpes",
                "05",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2977998"
            geonames = [
                [
                    geonameid,
                    "Saint-Michel-de-Chaillol",
                    ("hautes alpes", "saint michel de chaillol", None, None),
                    "",
                    "",
                    "Saint-Michel-de-Chaillol (hautes alpes)",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
        self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")
        pairs = location.alignment_geo_data(records, geonames)
        cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
        self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_breuil(self):
        """Test label that contains context that is neither dpt nor city
        and is associated with more than one service.

        Trying: Breuil (Le) associated with Yvelines, Marne, Saône-et-Loire,
        Haute-Saône, Rhône
        Expecting: is aligned to Le Breuil in Yvelines, Le Breuil
        in Marne, Le Breuil in Saône-et-Loire et Le Breuil in Rhône
        """
        finding_aids = [
            [["1234567890", "", "", "", "Breuil (Le)", "", "Yvelines", "78", "no"]],
            [["2345678901", "", "", "", "Breuil (Le)", "", "Marne", "51", "no"]],
            [["3456789012", "", "", "", "Breuil (Le)", "", "Saône-et-Loire", "71", "no"]],
            [["4567890123", "", "", "", "Breuil (Le)", "", "Rhône", "69", "no"]],
        ]
        urls = []
        geonameids = (3004994, 3004993, 3005001, 3005010)
        geonames = [
            [
                geonameids[0],
                "Le Breuil",
                ["yvelines", "le breuil", None, None],
                "",
                "",
                "Le Breuil (yvelines)",
                "",
                "",
            ],
            [
                geonameids[1],
                "Le Breuil",
                ["marne", "le breuil", None, None],
                "",
                "",
                "Le Breuil (marne)",
                "",
                "",
            ],
            [
                geonameids[2],
                "Le Breuil",
                ["saone et loire", "le breuil", None, None],
                "",
                "",
                "Le Breuil (saone-et-loire)",
                "",
                "",
            ],
            [
                geonameids[3],
                "Le Breuil",
                ["rhone", "le breuil", None, None],
                "",
                "",
                "Le Breuil (rhone)",
                "",
                "",
            ],
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            for rows in finding_aids:
                _, records_dptonly, _, _ = geonames_align.build_record(rows, geodata)
                pairs = location.alignment_geo_data(records_dptonly, geonames)
                cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_dptonly))
                if cells:
                    urls.append(cells[0][5])
        self.assertCountEqual(
            [f"https://www.geonames.org/{geonameid}" for geonameid in geonameids], urls
        )

    def test_an_minutiers(self):
        """Test special case AN-Minutier.

        Trying: rue
        Expecting: is aligned to Paris
        """
        rows = [["1234567890", "", "", "", "Truffaut (rue)", "MC/E", "FRAN", "93", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
        expected = [
            [
                "Paris",
                ["paris", "paris", None, None],
                "1234567890",
                "",
                "",
                "",
                "Paris (paris, paris)",
                "Truffaut (rue)",
                "no",
            ]
        ]
        self.assertEqual(expected, records)
        for geonameid in (2988507, 6455259, 2968815):
            geonames = [
                [
                    geonameid,
                    "Paris",
                    ("paris", "paris", None, None),
                    "",
                    "",
                    "Paris (paris)",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_algerie(self):
        """Test aligning to country if no context is given.

        Trying: Algérie
        Expecting: is aligned to Algeria
        """
        rows = [["1234567890", "", "", "", "Algérie", "", "Paris", "13", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = 2589581
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")
            expected = f"https://www.geonames.org/{geonameid}"
            self.assertEqual(expected, cells[0][5])

    def test_foreign_country_in_context(self):
        """Test aligning to foreign country if foreign country is in context.

        Trying: Maison du Peuple (Bruxelles, Belgique)
        Expecting: is aligned to Belgique (instead of not at all)
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Maison du Peuple (Bruxelles, Belgique)",
                "",
                "FR075FMSH",
                "75",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2802361"
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_foreign_city(self):
        """Test aligning to foreign country if foreign city and country are given.

        Trying: Bruxelles (Belgique)
        Expecting: is aligned to Bruxelles (Belgique)
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Bruxelles (Belgique)",
                "",
                "FR075FMSH",
                "75",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2800866"
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_ambiguous_foreign_city(self):
        """Test aligning to foreign country if foreign city and country are given.

        Trying: Lviv (Ukraine)
        Expecting: is aligned to Ukraine as there is two distinct Lviv, Ukraine in geonames
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Lviv (Ukraine)",
                "",
                "FR075FMSH",
                "75",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = "690791"
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_duplicated_foreign_city(self):
        """Test aligning to foreign country if foreign city and country are given.

        Trying: Lâhore (Pakistan)
        Expecting: is aligned to Lâhore (Pakistan) although two different versions
                   of the city name exist in geonames_altnames
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Lâhore (Pakistan)",
                "",
                "FRAD64",
                "65",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = "1172451"
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_city_in_context(self):
        """Test aligning to city if city is in context.

        Trying: Villeneuve (Poligny, Hautes-Alpes; hameau)
        Expecting: is aligned to Poligny (instead of Villeneuve)
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Villeneuve (Poligny, Hautes-Alpes; hameau)",
                "",
                "FRAD005",
                "05",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2986468"
            geonames = [
                [
                    geonameid,
                    "Poligny",
                    ("hautes alpes", "poligny", None, None),
                    "",
                    "",
                    "Poligny (hautes alpes)",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_foreign_country_only_in_context(self):
        """Test aligning to foreign country if foreign country only is in context.

        Trying: Heidelberg (Allemagne)
        Expecting: is aligned to Allemagne (instead of not at all)
        """
        rows = [["1234567890", "", "", "", "Heidelberg (Allemagne)", "", "FRAD015", "015", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, records_countryonly, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2921044"
            pairs = location.alignment_geo_foreign_countries_cities(
                cnx, geodata, records_countryonly
            )
            cells = list(geonames_align.cells_from_aligned_pairs(pairs))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_topographic_before_context(self):
        """Test aligning to feature classes S, H, T, V and L if topographic key
        word is before context.

        Trying: Versailles, château de (Yvelines, France)
        Expecting: is aligned to Château de Versailles
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Versailles, château de (Yvelines, France)",
                "",
                "",
                "",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, _, records_topographic = geonames_align.build_record(rows, geodata)
            geonameid = "6284980"
            geonames = [
                [
                    geonameid,
                    "Château de Versailles",
                    ("yvelines", None, None, "S"),
                    "",
                    "",
                    "Château de Versailles",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data_topographic(records_topographic, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_topographic))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_topographic_in_context(self):
        """Test aligning to feature classes S, H, T, V and L if topographic key
        word is in context.

        Trying: Saint-Cloud (Hauts-de-Seine , château de)
        Expecting: is aligned to Château de Saint-Cloud
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Saint-Cloud (Hauts-de-Seine , château de)",
                "",
                "",
                "",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, _, records_topographic = geonames_align.build_record(rows, geodata)
            geonameid = "8658867"
            geonames = [
                [
                    geonameid,
                    "Château de Saint-Cloud",
                    ("hauts de seine", None, None, "S"),
                    "",
                    "",
                    "Château de Saint-Cloud",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data_topographic(records_topographic, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_topographic))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_topographic_department(self):
        """Test aligning to feature classes S, H, T, V and L if department is in context.

        Trying: Melo, lac de (Haute-Corse, France)
        Expecting: is aligned to Lac de Melo
        """
        rows = [["1234567890", "", "", "", "Melo, lac de (Haute-Corse, France)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, _, records_topographic = geonames_align.build_record(rows, geodata)
            geonameid = "2994657"
            geonames = [
                [
                    geonameid,
                    "Lac de Melo",
                    ("haute corse", None, None, "H"),
                    "",
                    "",
                    "Lac de Melo (haute corse)",
                    "",
                    "",
                    "no",
                ]
            ]
            pairs = location.alignment_geo_data_topographic(records_topographic, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_topographic))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")
            self.assertEqual(cells[0][6], "Lac de Melo (haute corse)")

    def test_topographic_no_department(self):
        """Test aligning to feature classes S, H, T, V and L if
        there isn't a department specified.

        Trying: rhône (cours d'eau)
        Expecting: is not aligned to Rhône
        """
        rows = [["1234567890", "", "", "", "rhône (cours d'eau)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, _, _, records_topographic = geonames_align.build_record(rows, geodata)
            geonameid = "2983752"
            geonames = [
                [
                    geonameid,
                    "Rhône",
                    ("bouches du rhone", None, None, "H"),
                    "",
                    "",
                    "Rhône (bouches du rhone)",
                    "",
                    "",
                ],
                [
                    "2969183",
                    "Vieux Rhône",
                    ("bouches du rhone", None, None, "H"),
                    "",
                    "",
                    "Vieux Rhône (bouches du rhone)",
                    "",
                    "",
                ],
            ]
            pairs = location.alignment_geo_data_topographic(records_topographic, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records_topographic))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_admin2(self):
        """Test aligning to administrative class A if administrative key word
        is in context.

        Trying: Drôme (département)
        Expecting: is aligned to Département de la Drôme
        """
        xfail(reason="not always aligned to given GeoNames record")
        rows = [["1234567890", "", "", "", "Drôme (département)", "", "", ""]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "3020781"
            geonames = [
                [
                    geonameid,
                    "Département de la Drôme",
                    ("drome", None, None, None),
                    "",
                    "",
                    "Département de la Drôme (drome)",
                    "",
                    "",
                ],
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_admin3(self):
        """Test aligning to administrative class A if administrative key word
        is in context.

        Trying: Reims (Marne , arrondissement)
        Expecting: is aligned to Arrondissement de Reims
        """
        rows = [["1234567890", "", "", "", "Reims (Marne , arrondissement)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "2984113"
            geonames = [
                [
                    geonameid,
                    "Arrondissement de Reims",
                    ("marne", None, None, None),
                    "",
                    "",
                    "Arrondissement de Reims (marne)",
                    "",
                    "",
                ]
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_dom(self):
        """Test aligning to département d'outre-mer.

        Trying: Réunion (France ; département d'outre-mer)
        Expecting: is aligned to Réunion
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Réunion (France ; département d'outre-mer)",
                "",
                "",
                "",
                "no",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "6690284"
            geonames = [
                [geonameid, "Réunion", (None, None, "reunion", None), "", "", "Réunion", "", ""]
            ]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")

    def test_tom(self):
        """Test aligning to territoire d'outre-mer.

        Trying: Mayotte (territoire d'outre-mer)
        Expecting: is aligned to Mayotte
        """
        rows = [["1234567890", "", "", "", "Mayotte (territoire d'outre-mer)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = "1024031"
            geonames = [[geonameid, "Mayotte", (None, None, None, None), "", "", "Mayotte", "", ""]]
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(geonames_align.cells_from_pairs(pairs, geonames, records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")


class GeonamesPipelineComponentsTC(GeonamesAlignTaskBaseTC):
    """Geonames alignment task general test cases."""

    def setup_database(self):
        super(GeonamesPipelineComponentsTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            create_findingaid(cnx, name="FRAD_XXX")
            cnx.system_sql(
                """INSERT INTO geonames (geonameid,name)
                VALUES ('1234567890','foo')"""
            )
            cnx.commit()

    def build_record(self, geogname, loc, geoname_uri, keep):
        m = geonames_align.CONTEXT_RE.search(loc.label)
        label = m.group(1) if m else loc.label
        return geonames_align.GeonameRecord(
            dict(
                [
                    ("identifiant_LocationAuthority", str(loc.eid)),
                    ("URI_Geogname", ""),
                    ("libelle_Geogname", geogname.label),
                    ("URI_LocationAuthority", ""),
                    ("libelle_LocationAuthority", loc.label),
                    ("URI_GeoNames", geoname_uri),
                    ("libelle_GeoNames", label),
                    ("longitude", 0.0),
                    ("latitude", 0.0),
                    ("keep", keep),
                    ("fiabilite_alignement", 1),
                    ("quality", "yes" if loc.quality else "no"),
                ]
            )
        )

    def test_process_empty_csv(self):
        existing_alignment = set()
        headers = list(geonames_align.GeonameRecord.headers.keys())
        fp = StringIO()
        fp.write("\t".join(headers))
        fp.seek(0)
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            result = aligner.process_csv(fp, existing_alignment)
        self.assertEqual(result, ({}, {}))

    def test_process_csv_conflicts(self):
        """Test processing CSV file.

        Trying: CSV file contains conflicting alignments
        Expecting: alignments to add does not contain more than one alignment per authority
        """
        with self.admin_access.cnx() as cnx:
            existing_alignment = {("18296037", "https://www.geonames.org/3582490")}
            aligner = geonames_align.GeonameAligner(cnx)
            with open(self.datapath("alignment-geonames-conflict.csv")) as fp:
                new_alignment, to_remove_alignment = aligner.process_csv(
                    fp, existing_alignment, override_alignments=True
                )
            expected = [("18296037", "https://www.geonames.org/3036015")]
            self.assertEqual(list(new_alignment.keys()), expected)
            expected = [("18296037", "https://www.geonames.org/3582490")]
            self.assertEqual(list(to_remove_alignment.keys()), expected)

    def test_process_csv_missing_column(self):
        """Test processing CSV file.

        Trying: missing column
        Expecting: CSV is not processed
        """
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            with self.assertRaises(ValueError):
                with open(self.datapath("alignment-geonames-missing-column.csv")) as fp:
                    aligner.process_csv(fp, {}, override_alignments=True)

    def test_build_records(self):
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Mesvres",
                "U1",
                "Département de Saône-et-Loire",
                "71",
                "no",
            ],
            [
                "1234567890",
                "",
                "",
                "",
                "Vanves (Hauts-de-Seine, France)",
                "U1",
                "Département de Saône-et-Loire",
                "71",
                "no",
            ],
        ]
        expected = (
            [
                [
                    "Mesvres",
                    ["saone et loire", "Mesvres", None, None],
                    "1234567890",
                    "",
                    "",
                    "",
                    "Mesvres saone et loire",
                    "Mesvres",
                    "no",
                ]
            ],
            [
                [
                    "Vanves",
                    ["hauts de seine", "Vanves", "france", None],
                    "1234567890",
                    "",
                    "",
                    "",
                    "Vanves (Hauts-de-Seine, France)",
                    "Vanves (Hauts-de-Seine, France)",
                    "no",
                ]
            ],
            [],
            [],
        )
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            self.assertCountEqual(expected, geonames_align.build_record(rows, geodata))

    def test_build_record_dptonly(self):
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Cortiambles (Givry)",
                "U1",
                "Département de Saône-et-Loire",
                "71",
                "no",
            ],
            [
                "1234567890",
                "",
                "",
                "",
                "Tours (Anzy-le-Duc)",
                "U2",
                "Département de Saône-et-Loire",
                "71",
                "no",
            ],
            [
                "1234567890",
                "",
                "",
                "",
                "Bourbon-Lancy, Bailliage de (France)",
                "U3",
                "Département de Saône-et-Loire",
                "71",
                "no",
            ],
        ]
        expected = (
            [],
            [
                [
                    "Cortiambles",
                    ["saone et loire", "givry", None, None],
                    "1234567890",
                    "",
                    "",
                    "",
                    "Cortiambles (Givry) saone et loire",
                    "Cortiambles (Givry)",
                    "no",
                ],
                [
                    "Tours",
                    ["saone et loire", "anzy le duc", None, None],
                    "1234567890",
                    "",
                    "",
                    "",
                    "Tours (Anzy-le-Duc) saone et loire",
                    "Tours (Anzy-le-Duc)",
                    "no",
                ],
                [
                    "Bourbon-Lancy, Bailliage de",
                    ["saone et loire", None, "france", None],
                    "1234567890",
                    "",
                    "",
                    "",
                    "Bourbon-Lancy, Bailliage de (France) saone et loire",
                    "Bourbon-Lancy, Bailliage de (France)",
                    "no",
                ],
            ],
            [],
            [],
        )
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            self.assertEqual(expected, geonames_align.build_record(rows, geodata))

    def test_build_record_topographic(self):
        """Test building records.

        Trying: Cormeilles en Vexin, aéroport and
        Le lac de Menet, 1783 (aveu au roi par Gabrielle de la Croix)
        and Aisne (Marne ; cours d'eau) -- Sainte-Menehould
        Expecting: 1st and 3rd are in list of topographic records and 2nd is
        in list of departmental records
        """
        rows = [
            [
                "1234567890",
                "",
                "",
                "",
                "Cormeilles en Vexin, aéroport",
                "",
                "Val-d'Oise",
                "95",
                "no",
            ],
            [
                "1234567890",
                "",
                "",
                "",
                "Le lac de Menet, 1783 (aveu au roi par Gabrielle de la Croix)",
                "",
                "Cantal",
                "15",
                "no",
            ],
            [
                "1234567890",
                "",
                "",
                "",
                "Aisne (Marne ; cours d'eau) -- Sainte-Menehould",
                "",
                "Marne",
                "51",
                "no",
            ],
        ]
        expected_dptonly = [
            [
                "Le lac de Menet, 1783",
                ["cantal", None, None, None],
                "1234567890",
                "",
                "",
                "",
                "Le lac de Menet, 1783 (aveu au roi par Gabrielle de la Croix) cantal",
                "Le lac de Menet, 1783 (aveu au roi par Gabrielle de la Croix)",
                "no",
            ]
        ]
        expected_topographic = [
            [
                "Aisne",
                ["marne", None, None, "H"],
                "1234567890",
                "",
                "",
                "",
                "cours d eau Aisne",
                "Aisne (Marne ; cours d'eau) -- Sainte-Menehould",
                "no",
            ],
            [
                "Cormeilles en Vexin, aéroport",
                [None, None, None, "S"],
                "1234567890",
                "",
                "",
                "",
                "aeroport Cormeilles en Vexin",
                "Cormeilles en Vexin, aéroport",
                "no",
            ],
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            _, records_dptonly, _, records_topographic = geonames_align.build_record(rows, geodata)
            self.assertCountEqual(expected_dptonly, records_dptonly)
            self.assertCountEqual(expected_topographic, records_topographic)

    def test_build_record_blacklisted(self):
        """Test bulding records.

        Trying: Crocq (Creuse, France ; canton)
        Expecting: is skipped
        """
        rows = [["1234567890", "", "", "", " Crocq (Creuse, France ; canton)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, dptonly, countryonly, topographic = geonames_align.build_record(rows, geodata)
            self.assertEqual(records, [])
            self.assertEqual(dptonly, [])
            self.assertEqual(countryonly, [])
            self.assertEqual(topographic, [])

    def test_build_record_blacklist_departments(self):
        """Test building record. Do not treat a token as both department and city if
        it could be either.

        Trying: token is name of department and name of city
        Expecting: token is treated as department
        """
        rows = [["1234567890", "", "", "", "Bar-sur-Seine (Aube, France)", "", "", "", "no"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            records, _, _, _ = geonames_align.build_record(rows, geodata)
            record = records[0]
            self.assertEqual(record[1][0], "aube")
            self.assertNotEqual(record[1][1], "aube")

    def test_toulouse_pp(self):
        """Test that cities are aligned to P fclass if P and A exist.

        Trying: Toulouse (Haute-Garonne, France)
        Expecting: is aligned to Toulouse (Haute-Garonne, France)
                   https://www.geonames.org/2972315 (fclass P)

        """
        rows = [["167886031", "", "", "", "Toulouse (Haute-Garonne, France)", "", "", "", "yes"]]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            pnia_records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = 2972315
            geoname = location.build_geoname_set(cnx, geodata)
            self.assertIn("toulouse", geodata.simplified_cities.values())
            pairs = location.alignment_geo_data(pnia_records, geoname)
            cells = list(geonames_align.cells_from_pairs(pairs, geoname, pnia_records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")
            self.assertEqual(cells[0][6], "Toulouse (Occitanie, Haute-Garonne)")

    def test_saint_jean_pp(self):
        """Test that cities are aligned to P fclass if P and A exist.

        Trying: Saint-Jean-d'Angély (Charente-Maritime, France, commune)
        Expecting: is aligned to Saint-Jean-d'Angély (Nouvelle-Aquitaine,Charente-Maritime)
                   to https://www.geonames.org/2979363 (fclasse P)

        """
        rows = [
            [
                "167886031",
                "",
                "",
                "",
                "Saint-Jean-d'Angély (Charente-Maritime, France, commune)",
                "",
                "",
                "",
                "yes",
            ]
        ]
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            pnia_records, _, _, _ = geonames_align.build_record(rows, geodata)
            geonameid = 2979363
            geoname = location.build_geoname_set(cnx, geodata)
            self.assertIn("saint jean d angely", geodata.simplified_cities.values())
            pairs = location.alignment_geo_data(pnia_records, geoname)
            cells = list(geonames_align.cells_from_pairs(pairs, geoname, pnia_records))
            self.assertEqual(cells[0][5], f"https://www.geonames.org/{geonameid}")
            self.assertEqual(
                cells[0][6], "Saint-Jean-d'Angély (Nouvelle-Aquitaine, Charente-Maritime)"
            )

    def _test_minhashing_pipline(self, refset, targetset):
        """MinHash pipeline.

        :param set refset: reference set
        :param set targetset: target set

        :returns: list of aligned pairs
        :rtype: list
        """
        processing = BaseProcessing(
            ref_attr_index=0, target_attr_index=0, distance_callback=location.approx_match
        )
        place_normalizer = NormalizerPipeline((SimplifyNormalizer(attr_index=0),))
        dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
        dpt_aligner.register_ref_normalizer(place_normalizer)
        dpt_aligner.register_target_normalizer(place_normalizer)
        blocking_2 = MinHashingBlocking(0, 0, threshold=0.4)
        dpt_blocking = PipelineBlocking((blocking_2,), collect_stats=True)
        dpt_aligner.register_blocking(dpt_blocking)
        return list(PipelineAligner((dpt_aligner,)).get_aligned_pairs(refset, targetset))

    def test_minhashing_bagnolet(self):
        """
        Test a PipelineAligner minhashing
        """
        self.skipTest("This test serves as documentation")
        refset = [["bagnolet"]]
        targetset = [["baignolet"]]
        pairs = self._test_minhashing_pipline(refset, targetset)
        self.assertFalse(pairs)
        with patch("random.randint", lambda a, b: (a + b) // 2):
            minlsh = Minlsh()
            sentences = (refset[0][0], targetset[0][0])
            minlsh.train(
                (simplify(s, remove_stopwords=True) for s in sentences),
            )
            self.assertEqual(set([]), minlsh.predict(0.1))
            self.assertEqual(set([]), minlsh.predict(0.4))

    def test_minhashing_gond(self):
        """
        Test a PipelineAligner minhashing with two sentences
        """
        self.skipTest("This test serves as documentation")
        refset = [["Saint-Gond, Marais de"]]
        targetset = [["Marais de Saint-Gond"]]
        pairs = self._test_minhashing_pipline(refset, targetset)
        self.assertTrue(pairs)
        with patch("random.randint", lambda a, b: (a + b) // 2):
            minlsh = Minlsh()
            sentences = (refset[0][0], targetset[0][0])
            minlsh.train(
                (simplify(s, remove_stopwords=True) for s in sentences),
            )
            self.assertEqual(set([(0, 1)]), minlsh.predict(0.4))

    def test_suppression_remove_localization(self):
        """
        The alignment has been launched automatically.

        Trying: delete the existing alignment
        Expecting : localisation info is delete on the corresponding Location
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2972328"
            cnx.system_sql(
                """UPDATE geonames SET latitude=43.12,longitude=5.93
                WHERE geonameid=2972328"""
            )
            cnx.commit()
            toulon = cnx.create_entity(
                "ExternalUri", label="Toulon (Var, France)", uri=geoname_uri, extid="2972328"
            )
            loc = cnx.create_entity("LocationAuthority", label="Toulon", same_as=toulon)
            fa = cnx.find("FindingAid", name="FRAD_XXX").one()
            geogname = cnx.create_entity("Geogname", label="Toulon", index=fa, authority=loc)
            cnx.commit()
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            self.assertEqual(43.12, loc.latitude)
            self.assertEqual(5.93, loc.longitude)
            # remove the alignement
            # delete the existing same_as
            aligner = geonames_align.GeonameAligner(cnx)
            key = (loc.eid, geoname_uri)
            record = self.build_record(geogname, loc, geoname_uri, "n")
            to_remove_alignment = {key: record}
            aligner.process_alignments({}, to_remove_alignment, override_alignments=True)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            self.assertFalse(loc.same_as)
            self.assertFalse(loc.latitude)
            self.assertFalse(loc.longitude)

    def setup_process_alignments(self, cnx, geoname_uri):
        paris = cnx.create_entity(
            "ExternalUri", label="Paris (France)", uri=geoname_uri, extid="2988507"
        )
        loc = cnx.create_entity(
            "LocationAuthority", label="Paris (France)", latitude=0.0, longitude=0.0, same_as=paris
        )
        fa = cnx.find("FindingAid", name="FRAD_XXX").one()
        geogname = cnx.create_entity("Geogname", label="index location 1", index=fa, authority=loc)
        cnx.commit()
        loc = cnx.find("LocationAuthority", eid=loc.eid).one()
        self.assertTrue(loc.same_as)
        paris = cnx.find("ExternalUri", eid=paris.eid).one()
        self.assertEqual("geoname", paris.source)
        # update latitude(s) and longitude(s)
        cnx.system_sql(
            """UPDATE geonames SET latitude=48.85,longitude=2.34
            WHERE geonameid='2988507'"""
        )
        cnx.commit()
        return fa, loc, geogname

    def test_suppression_dont_override_user_alignments(self):
        """
        A same_as relation has been set by user,
        The alignment has been launched automatically.
        User alignments are not overriden (override_alignments=False)

        Trying: delete the existing alignment
        Expecting : same_as relation still exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            fa, loc, geogname = self.setup_process_alignments(cnx, geoname_uri)
            self.assertEqual(
                [
                    (geoname_uri, loc.eid, True),
                ],
                get_samesas_history(cnx, complete=True),
            )
            record = self.build_record(geogname, loc, geoname_uri, "n")
            key = (loc.eid, geoname_uri)
            to_remove_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # do not delete the existing same_as
            aligner.process_alignments({}, to_remove_alignment, override_alignments=False)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            # delete alignment to same GeoNames URI than user-defined same_as relation
            findingaid = create_findingaid(cnx, "eadid2")
            locationauthority = cnx.create_entity("LocationAuthority", label="Paris")
            _ = cnx.create_entity(  # noqa
                "Geogname", index=[findingaid], authority=locationauthority
            )
            cnx.commit()
            record = self.build_record(geogname, locationauthority, geoname_uri, "y")
            key = (locationauthority.eid, geoname_uri)
            new_alignment = {key: record}
            record = self.build_record(geogname, locationauthority, geoname_uri, "n")
            to_remove_alignment = {key: record}
            aligner.process_alignments(new_alignment, {})
            aligner.process_alignments({}, to_remove_alignment)
            locationauthority = cnx.find("LocationAuthority", eid=locationauthority.eid).one()
            self.assertFalse(locationauthority.same_as)

    def test_suppression_override_user_alignments(self):
        """
        A same_as relation has been set by user.
        Process_alignments is launched from the file.
        User alignments are overriden (override_alignments=True)

        Trying: delete the existing alignment
        Expecting : same_as relation do not exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            fa, loc, geogname = self.setup_process_alignments(cnx, geoname_uri)
            cnx.commit()
            self.assertEqual(
                [
                    (geoname_uri, loc.eid, True),
                ],
                get_samesas_history(cnx, complete=True),
            )
            record = self.build_record(geogname, loc, geoname_uri, "n")
            key = (loc.eid, geoname_uri)
            to_remove_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # do not delete the existing same_as
            aligner.process_alignments({}, to_remove_alignment)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            # delete the existing same_as
            aligner.process_alignments({}, to_remove_alignment, override_alignments=True)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertFalse(loc.same_as)

    def test_creation_override_deleted_alignments(self):
        """
        A same_as relation has been removed by user.
        Process_alignments is launched from the file.
        User alignments are not overriden (override_alignments=False)

        Trying: add a removed alignment
        Expecting : same_as relation must not exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            _, loc, geogname = self.setup_process_alignments(cnx, geoname_uri)
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual(
                [
                    (geoname_uri, loc.eid, False),
                ],
                get_samesas_history(cnx, complete=True),
            )
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            record = self.build_record(geogname, loc, geoname_uri, "y")
            key = (loc.eid, geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # the deleted alignment must not be recreated
            aligner.process_alignments(new_alignment, {})
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            self.assertEqual(1, cnx.execute("Any COUNT(X) WHERE X is ExternalUri")[0][0])

    def test_supression_override_deleted_alignments(self):
        """
        A same_as relation has been removed by user.
        Process_alignments is launched from the file.
        User alignments are overriden (override_alignments=True)

        Trying: add a removed alignment
        Expecting : same_as relation must exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            _, loc, geogname = self.setup_process_alignments(cnx, geoname_uri)
            loc.cw_set(same_as=None)
            cnx.commit()
            self.assertEqual(
                [
                    (geoname_uri, loc.eid, False),
                ],
                get_samesas_history(cnx, complete=True),
            )
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            record = self.build_record(geogname, loc, geoname_uri, "y")
            key = (loc.eid, geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # the deleted alignemnt must not be recreated
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            loc = cnx.find("LocationAuthority", eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            self.assertEqual(1, cnx.execute("Any COUNT(X) WHERE X is ExternalUri")[0][0])

    def test_do_not_update_locationauthority_if_user_defined(self):
        """
        Test GeoNames alignment.

        Trying: update alignments when there is user-defined same_as relation
        and override_alignments toggle is off
        Expecting: user-defined latitude/longitude values are not changed
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            _, locationauthority, geogname = self.setup_process_alignments(cnx, geoname_uri)
            self.assertEqual(locationauthority.latitude, 0.0)
            self.assertEqual(locationauthority.longitude, 0.0)

            self.assertEqual(
                [
                    (geoname_uri, locationauthority.eid, True),
                ],
                get_samesas_history(cnx, complete=True),
            )
            record = self.build_record(geogname, locationauthority, geoname_uri, "y")
            key = (locationauthority.eid, geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(new_alignment, {})
            locationauthority = cnx.find("LocationAuthority", eid=locationauthority.eid).one()
            self.assertEqual(locationauthority.latitude, 0.0)
            self.assertEqual(locationauthority.longitude, 0.0)

    def test_update_locationauthority_if_user_defined(self):
        """Test GeoNames alignment.

        Trying: update alignments when there is user-defined same_as relation
        and override_alignments toggle is on
        Expecting: user-defined latitude/longitude values are changed
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            _, locationauthority, geogname = self.setup_process_alignments(cnx, geoname_uri)
            self.assertEqual(locationauthority.latitude, 0.0)
            self.assertEqual(locationauthority.longitude, 0.0)

            self.assertEqual(
                [
                    (geoname_uri, locationauthority.eid, True),
                ],
                get_samesas_history(cnx, complete=True),
            )
            record = self.build_record(geogname, locationauthority, geoname_uri, "y")
            key = (locationauthority.eid, geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            locationauthority = cnx.find("LocationAuthority", eid=locationauthority.eid).one()
            self.assertEqual(locationauthority.latitude, 48.85)
            self.assertEqual(locationauthority.longitude, 2.34)

    def test_update_same_as_relation(self):
        """Test GeoNames alignment.

        Trying: update alignments when there is (user-defined) same-as
        relation and override_alignments toggle is on
        Expecting: there is only newest alignment in same-as relation
        """
        with self.admin_access.cnx() as cnx:
            # insert user-defined
            _, locationauthority, geogname = self.setup_process_alignments(
                cnx, "https://www.geonames.org/2988507"
            )
            geonameid = "1234567890"
            geoname_uri = f"https://www.geonames.org/{geonameid}"
            cnx.create_entity(
                "ExternalUri", uri=geoname_uri, label="foo", extid=geonameid, source="geoname"
            ).eid
            new_alignment = {(locationauthority.eid, geoname_uri): tuple()}
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            rows = cnx.execute(
                '''Any A, U WHERE X is ExternalUri, X uri U,
                A same_as X, X source "geoname"'''
            ).rows
            self.assertEqual(rows, [[locationauthority.eid, geoname_uri]])

    def test_compute_existing_alignment(self):
        """Test fetching existing alignments from database.

        Trying: fetching existing alignments
        Expecting: existing alignments
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = "https://www.geonames.org/2988507"
            _, locationauthority, geogname = self.setup_process_alignments(cnx, geoname_uri)
            expected = {(str(locationauthority.eid), geoname_uri)}
            aligner = geonames_align.GeonameAligner(cnx)
            self.assertEqual(aligner.compute_existing_alignment(), expected)

    def test_update_sameas_history_new_alignment(self):
        """Test updating same-as relation history.

        Trying: adding / modifying alignment
        Expecting: same-as relation history is updated
        """
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            findingaid = create_findingaid(cnx, name="foo")
            authority = cnx.create_entity("LocationAuthority", label="bar")
            geogname = cnx.create_entity("Geogname", index=findingaid, authority=authority)
            cnx.commit()
            geoname_uri = "https://www.geonames.org/1234567890"
            record = self.build_record(geogname, authority, geoname_uri, "y")
            key = (authority.eid, geoname_uri)
            new_alignment = {key: record}
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            sql = """SELECT * FROM sameas_history
            WHERE autheid=%(autheid)s AND sameas_uri=%(geonameuri)s
            AND action=true"""
            rows = cnx.system_sql(
                sql, {"autheid": authority.eid, "geonameuri": geoname_uri}
            ).fetchall()
            self.assertTrue(rows)

    def test_update_sameas_history_to_remove_alignment(self):
        """Test updating same-as relation history.

        Trying: removing alignment
        Expecting: same-as relation history is updated
        """
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            findingaid = create_findingaid(cnx, name="foo")
            authority = cnx.create_entity("LocationAuthority", label="bar")
            geogname = cnx.create_entity("Geogname", index=findingaid, authority=authority)
            cnx.commit()
            geoname_uri = "https://www.geonames.org/1234567890"
            record = self.build_record(geogname, authority, geoname_uri, "y")
            key = (authority.eid, geoname_uri)
            new_alignment = {key: record}
            record = self.build_record(geogname, authority, geoname_uri, "n")
            to_remove_alignment = {key: record}
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            aligner.process_alignments({}, to_remove_alignment, override_alignments=True)
            sql = """SELECT * FROM sameas_history
            WHERE autheid=%(autheid)s AND sameas_uri=%(geonameuri)s
            AND action=false"""
            rows = cnx.system_sql(
                sql, {"autheid": authority.eid, "geonameuri": geoname_uri}
            ).fetchall()
            self.assertTrue(rows)

    def test_new_alignment_geonames_label(self):
        """Test adding new alignment.

        Trying: add new alignment pre-defined GeoNames label
        Expecting: ExternalUri label is pre-defined GeoNames label
        """
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            findingaid = create_findingaid(cnx, name="foo")
            authority = cnx.create_entity("LocationAuthority", label="bar")
            geogname = cnx.create_entity("Geogname", index=findingaid, authority=authority)
            cnx.commit()
            geoname_uri = "https://www.geonames.org/1234567890"
            record = self.build_record(geogname, authority, geoname_uri, "y")
            record.geonamealignlabel = "foobar"
            key = (authority.eid, geoname_uri)
            new_alignment = {key: record}
            aligner.process_alignments(new_alignment, {})
            externaluri = cnx.find("ExternalUri").one()
            self.assertEqual(record.geonamealignlabel, externaluri.label)

    def test_new_alignment_no_geonames_label(self):
        """Test adding new alignment.

        Trying: add new alignment without pre-defined GeoNames label
        Expecting: ExternalUri label is fetched from GeoNames database
        """
        with self.admin_access.cnx() as cnx:
            aligner = geonames_align.GeonameAligner(cnx)
            findingaid = create_findingaid(cnx, name="foo")
            authority = cnx.create_entity("LocationAuthority", label="bar")
            geogname = cnx.create_entity("Geogname", index=findingaid, authority=authority)
            cnx.commit()
            geonameid = "1234567890"
            geoname_uri = f"https://www.geonames.org/{geonameid}"
            record = self.build_record(geogname, authority, geoname_uri, "y")
            record.geonamealignlabel = None
            key = (authority.eid, geoname_uri)
            new_alignment = {key: record}
            aligner.process_alignments(new_alignment, {})
            (name,) = cnx.system_sql(
                "SELECT name FROM geonames WHERE geonameid=%(geonameid)s", {"geonameid": geonameid}
            ).fetchone()
            externaluri = cnx.find("ExternalUri").one()
            self.assertEqual(externaluri.label, name)

    def test_missing_required_column(self):
        """Test initializing new GeoNames record.

        Trying: required column is missing
        Expecting: ValueError
        """
        with self.assertRaises(ValueError):
            geonames_align.GeonameRecord({})

    def test_only_required_column(self):
        """Test initializing new GeoNames record.

        Trying: only required columns are present
        Expecting: new GeoNames record is initialized
        """
        dictrow = {
            "identifiant_LocationAuthority": "foo",
            "libelle_LocationAuthority": "foobar",
            "URI_GeoNames": "bar",
            "keep": "baz",
            "quality": "no",
        }
        record = geonames_align.GeonameRecord(dictrow)
        self.assertEqual(record.autheid, dictrow["identifiant_LocationAuthority"])
        self.assertEqual(record.sourceid, dictrow["URI_GeoNames"])
        self.assertEqual(record.keep, dictrow["keep"])

    def test_do_not_overwrite_bano(self):
        """Test processing alignments.

        Trying: importing GeoNames alignment if BANO alignment exists
        Expecting: is not overwritten
        """
        with self.admin_access.cnx() as cnx:
            # insert LocationAuthority aligned to BANO
            externalid = cnx.create_entity(
                "ExternalId", extid="1234567890", label="foo", source="bano"
            )
            authority = cnx.create_entity(
                "LocationAuthority", label="bar", same_as=externalid, latitude=-1.0, longitude=-1.0
            )
            # insert non-aligned LocationAuthority
            authority_overwrite = cnx.create_entity("LocationAuthority", label="baz")
            # insert ExternalUri to align to
            geonameid = "2988507"
            geonameuri = "https://geonames.org/{}".format(geonameid)
            cnx.create_entity("ExternalUri", uri=geonameuri, extid=geonameid, source="geoname")
            cnx.commit()
            self.assertFalse(authority_overwrite.quality)
            record = geonames_align.GeonameRecord(
                {
                    "identifiant_LocationAuthority": authority.eid,
                    "libelle_LocationAuthority": authority.label,
                    "URI_GeoNames": geonameuri,
                    "keep": "yes",
                    "quality": "no",
                }
            )
            record_overwrite = geonames_align.GeonameRecord(
                {
                    "identifiant_LocationAuthority": authority_overwrite.eid,
                    "libelle_LocationAuthority": authority_overwrite.label,
                    "URI_GeoNames": geonameuri,
                    "keep": "yes",
                    "quality": "no",
                }
            )
            key = (authority.eid, geonameuri)
            key_overwrite = (authority_overwrite.eid, geonameuri)
            new_alignment = {key: record, key_overwrite: record_overwrite}
            new_alignment = {
                (authority.eid, geonameuri): record,
                (authority_overwrite.eid, geonameuri): record_overwrite,
            }
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(new_alignment, {}, override_alignments=True)
            # LocationAuthority aligned to BANO is not updated
            authority = cnx.find("LocationAuthority", eid=authority.eid).one()
            self.assertEqual(authority.longitude, -1.0)
            self.assertEqual(authority.latitude, -1.0)
            # previously non-aligned LocationAuthority is updated
            authority_overwrite = cnx.find("LocationAuthority", eid=authority_overwrite.eid).one()
            self.assertEqual(authority_overwrite.longitude, 0.0)
            self.assertEqual(authority_overwrite.latitude, 0.0)
            self.assertFalse(authority_overwrite.quality)

    def test_do_not_use_historic_names(self):
        """Testing not using historic names.

        Trying: listing country names (French)
        Expecting: historic names are not in list
        """
        # ishistoric is True
        code, name = "MU", "Île de France"
        with self.admin_access.cnx() as cnx:
            geodata = location.Geodata(cnx)
            # assert that country is still in list of French country names
            actual = geodata.countries[code]
            # assert that French country name is not historical name
            self.assertNotEqual(actual, name)

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

import os.path as osp
import difflib

from nazca.utils.normalize import NormalizerPipeline, SimplifyNormalizer
from nazca.utils.distances import BaseProcessing
from nazca.rl.blocking import KeyBlocking, PipelineBlocking, NGramBlocking, MinHashingBlocking
from nazca.rl.aligner import BaseAligner, PipelineAligner

from cubicweb_frarchives_edition.alignments.utils import simplify

HERE = osp.dirname(osp.abspath(__file__))


class Geodata(object):
    """Geodata manager.

    :cvar dict OVERSEAS_DEPARTMENTS: map of overseas
    departments (admin2_code-country_code)
    :ivar dict cities: map of normalized cities
    :ivar dict departments: map of normalized departments
    :ivar dict countries: map of normalized countries
    """

    OVERSEAS_DEPARTMENTS = {
        "971": "GP",  # Guadeloupe
        "972": "MQ",  # Martinique
        "973": "GF",  # Guyane
        "974": "RE",  # Réunion
        "975": "PM",  # Saint Pierre and Miquelon
        "976": "YT",  # Mayotte
        "977": "BL",  # Saint-Barthélemy
        "978": "MF",  # Saint-Martin
        "986": "WF",  # Wallis-et-Futuna
        "987": "PF",  # Polynésie française
        "988": "NC",  # Nouvelle-Calédonie
    }

    METROPOLIS = {"69M": "Métropole de Lyon"}

    HISTORIC_REGIONS = {
        "Alsace": "44",
        "Aquitaine": "75",
        "Auvergne": "84",
        "Basse-Normandie": "28",
        "Bourgogne": "27",
        "Champagne-Ardenne": "44",
        "Centre": "24",
        "Franche-Comté": "27",
        "Haute-Normandie": "28",
        "Limousin": "75",
        "Lorraine": "44",
        "Midi-Pyrénées": "76",
        "Picardie": "32",
        "Rhône-Alpes": "84",
    }

    def __init__(self, cnx, country_code="FR", isolanguage="fr", force=False):
        """Initialize geodata manager.

        :param Connection cnx: CubicWeb database connection
        """
        self.cnx = cnx
        self.init_table(country_code=country_code, isolanguage=isolanguage, force=force)

    def init_table(self, country_code="FR", isolanguage="fr", force=False):
        """Initialize table.

        :param str country_code: country code
        """
        args = [country_code]
        if country_code == "FR":
            placeholder = ",".join("%s" for _ in range(len(self.OVERSEAS_DEPARTMENTS) + 1))
            args += list(self.OVERSEAS_DEPARTMENTS.values())
        else:
            placeholder = "%s"
        if force:
            self.cnx.system_sql("DROP TABLE IF EXISTS geodata")
        sql = """CREATE TABLE IF NOT EXISTS geodata AS (
            SELECT geonameid,name,fclass,fcode,country_code,
            admin1_code,admin2_code,admin4_code FROM geonames
            WHERE country_code IN ({placeholder})
            AND fclass IN ('A','P')
            )""".format(
            placeholder=placeholder
        )
        self.cnx.system_sql(sql, args)
        self.cnx.system_sql("""CREATE INDEX IF NOT EXISTS geodata_fclass_idx ON geodata(fclass)""")
        self.cnx.system_sql("""CREATE INDEX IF NOT EXISTS geodata_fcode_idx ON geodata(fcode)""")
        self.cnx.commit()
        self._cities = {}
        self._simplified_cities = {}
        self._departments = {}
        self._blacklist = {}
        self._simplified_departments = {}
        self._simplified_blacklist = {}
        self._regions = {}
        self._simplified_regions = {}
        self._simplified_historic_regions = {}
        self._countries = {}
        self._simplified_countries = {}

    @property
    def cities(self):
        """Map of cities."""
        if not self._cities:
            # admin4_code is not unique if historic towns are included as well
            # in our case, only Val-Couesnon (ADM4) and
            # Saint-Ouen-la-Rouërie (ADM4H) share admin4_code
            rows = self.cnx.system_sql(
                """SELECT name,admin4_code FROM geodata
                WHERE fclass='A' AND fcode IN ('ADM4','ADM4H')
                """
            )
            self._cities = {code: name for (name, code) in rows}
        return self._cities

    @property
    def simplified_cities(self):
        """Map of simplified cities."""
        if not self._simplified_cities:
            self._simplified_cities = {
                code: simplify(name) for (code, name) in list(self.cities.items())
            }
        return self._simplified_cities

    @property
    def departments(self):
        """Map of departments."""
        if not self._departments:
            rows = self.cnx.system_sql(
                r"""SELECT admin2_code,
                REGEXP_REPLACE(name, E'Département (de l\'|de la |du |des |d\'|de )', '')
                FROM geodata WHERE fclass='A' AND fcode='ADM2'"""
            )
            self._departments = {code: name for (code, name) in rows}
            self._departments.update(self.METROPOLIS)
        return self._departments

    @property
    def blacklist(self):
        """Map of departments
        (related to https://extranet.logilab.fr/ticket/67923708)."""
        if not self._blacklist:
            self._blacklist = {
                code: name
                for code, name in self.departments.items()
                if code in ("03", "10", "19", "23", "25", "36", "40", "53", "75", "84", "86")
            }
        return self._blacklist

    @property
    def simplified_departments(self):
        """Map of simplified departments."""
        if not self._simplified_departments:
            self._simplified_departments = {
                code: simplify(name) for (code, name) in list(self.departments.items())
            }
        return self._simplified_departments

    @property
    def simplified_blacklist(self):
        """Map of simplified departments
        (related to https://extranet.logilab.fr/ticket/67923708)."""
        if not self._simplified_blacklist:
            self._simplified_blacklist = {
                code: name
                for code, name in self.simplified_departments.items()
                if code in ("03", "10", "19", "23", "25", "36", "40", "53", "75", "84", "86")
            }
        return self._simplified_blacklist

    @property
    def regions(self):
        """Map of regions (not including overseas departments)."""
        if not self._regions:
            rows = self.cnx.system_sql(
                """SELECT admin1_code,name FROM geodata WHERE fclass='A'
                AND fcode='ADM1'"""
            )
            self._regions = {code: name for (code, name) in rows}
        return self._regions

    @property
    def simplified_regions(self):
        """Map of simplified regions."""
        if not self._simplified_regions:
            self._simplified_regions = {
                code: simplify(name) for (code, name) in list(self.regions.items())
            }
        return self._simplified_regions

    @property
    def simplified_historic_regions(self):
        """Map of simplified historic regions."""
        if not self._simplified_historic_regions:
            self._simplified_historic_regions = {
                code: simplify(name) for (name, code) in list(self.HISTORIC_REGIONS.items())
            }
        return self._simplified_historic_regions

    @property
    def countries(self):
        """Map of countries in French.

        ishistoric is True if historic name NULL else
        """
        if not self._countries:
            rows = self.cnx.system_sql(
                """SELECT temp.alternate_name,geonames.country_code
                FROM (
                    SELECT DISTINCT ON(geonameid) geonameid,alternate_name
                    FROM geonames_altnames WHERE isolanguage='fr' AND
                    ishistoric IS NULL
                    ORDER BY geonameid,alternate_name
                ) AS temp
                JOIN geonames ON temp.geonameid=geonames.geonameid
                WHERE (fcode='PCL' OR fcode='PCLI')"""
            )
            self._countries = {code: alternate_name for (alternate_name, code) in rows}
        return self._countries

    @property
    def simplified_countries(self):
        """Map of simplified countries in French."""
        if not self._simplified_countries:
            self._simplified_countries = {
                code: simplify(name) for (code, name) in list(self.countries.items())
            }
        return self._simplified_countries


def create_geonames_label(name, admin1, admin2, geodata):
    """Create GeoNames label.

    :param str name: name
    :param str admin1: administrative code
    :param str admin2: administrative code
    :param Geodata geodata: Geodata manager

    :returns: GeoNames label
    :rtype: str
    """
    info = (geodata.regions.get(admin1), geodata.departments.get(admin2))
    if any(info):
        return "{name} ({info})".format(name=name, info=",".join(item for item in info if item))
    else:
        return name


def build_topographic_geoname_set(cnx, geodata):
    """Build GeoNames set containing topopgraphic feature classes.

    :param Connection cnx: CubicWeb database connection
    :param Geodata geodata: Geodata manager

    :returns: GeoNames set
    :rtype: list
    """
    geoname_set = []
    rows = cnx.system_sql(
        """SELECT geonameid,name,admin1_code,admin2_code,latitude,longitude,fclass
        FROM geonames WHERE country_code='FR' AND fcode IN (
        'CSTL','PAL','HSEC','AIRP','DAM','PK','MT','MTS',
        'PASS','ISL','PEN','PLN','UPLD','FRST','PRK','PRT','STM','LK','CNL')"""
    )
    for geonameid, name, admin1_code, admin2_code, latitude, longitude, fclass in rows:
        dpt = geodata.simplified_departments.get(admin2_code)
        label = create_geonames_label(name, admin1_code, admin2_code, geodata)
        geoname_set.append(
            [geonameid, name, (dpt, None, None, fclass), "", label, "", latitude, longitude]
        )
    return geoname_set


def build_countries_geoname_set(cnx, isolanguage="fr"):
    """Build GeoNames set containing countries in given language.

    :param Connection cnx: CubicWeb database connection
    :param str isolanguage: language

    :returns: GeoNames set
    :rtype: list
    """
    rows = cnx.system_sql(
        """SELECT temp.alternate_name,geonames.geonameid,
        geonames.name,geonames.latitude,geonames.longitude
        FROM (
            SELECT DISTINCT ON(geonameid) geonameid,alternate_name
            FROM geonames_altnames WHERE isolanguage=%(isolanguage)s
            ORDER BY geonameid,alternate_name
        ) AS temp JOIN geonames ON temp.geonameid=geonames.geonameid
        WHERE (fcode='PCL' OR fcode='PCLI')""",
        {"isolanguage": isolanguage},
    )
    return [
        [
            geonameid,
            name,
            (None, None, simplify(altname), None),
            "",
            altname,
            altname,
            latitude,
            longitude,
        ]
        for altname, geonameid, name, latitude, longitude in rows
    ]


def build_geoname_set(cnx, geodata, dpt_code=None):
    """Buld GeoNames set.

    :param Connection cnx: CubicWeb database connection
    :param Geodata geodata: Geodata manager
    :param str dpt_code: department

    :returns: GeoNames set
    :rtype: list
    """
    q = """
        SELECT tmp.geonameid, tmp.name, tmp.admin1_code,
               tmp.admin2_code, tmp.admin4_code, tmp.fclass, tmp.latitude, tmp.longitude
        FROM (SELECT geonameid, name, admin1_code,
        admin2_code, admin4_code, fclass, latitude, longitude,
              ROW_NUMBER() OVER (PARTITION BY name, admin1_code, admin2_code, admin4_code
              ORDER BY fclass desc) rank
        FROM geonames
        WHERE country_code IN ({placeholder}) AND fclass IN ('A', 'P')
        """
    dpt = dpt_code
    if dpt is not None:
        placeholder = "%s"
        args = [geodata.OVERSEAS_DEPARTMENTS.get(dpt, "FR"), dpt]
        q += " AND admin2_code = %s"
        q = q.format(placeholder=placeholder)
    else:
        placeholder = ",".join("%s" for _ in range(len(geodata.OVERSEAS_DEPARTMENTS) + 1))
        args = ["FR"] + list(geodata.OVERSEAS_DEPARTMENTS.values())
        q = q.format(placeholder=placeholder)
    q += ") as tmp WHERE tmp.rank = 1"
    rows = cnx.system_sql(q, args).fetchall()
    # TODO replace list of cities by list of localized cities
    geoname_set = []
    for id, name, ad1, ad2, ad4, fclass, latitude, longitude in rows:
        dpt, city, country = None, None, None
        comp_string = name
        dpt = geodata.simplified_departments.get(ad2)
        city = geodata.simplified_cities.get(ad4)
        if dpt:
            if dpt_code is None:
                info = ", ".join(e for e in [dpt, city] if e)
            else:
                # do not add the city as we dont have it for the
                # matching pnia records_dptonly
                info = dpt
            comp_string = "{} ({})".format(name, info)
        label = create_geonames_label(name, ad1, ad2, geodata)
        geoname_set.append(
            [id, name, (dpt, city, country, None), "", label, comp_string, latitude, longitude]
        )
    return geoname_set


def approx_match(x, y):
    return 1.0 - difflib.SequenceMatcher(None, x, y).ratio() if x != y else 0.0


def dpt_block_cb(record):
    dpt, city, _, _ = record
    return dpt


def dpt_city_block_cb(record):
    dpt, city, _, _ = record
    return dpt, city


def country_block_cb(record):
    _, _, country, _ = record
    return country


def fclass_block_cb(record):
    _, _, _, fclass = record
    return fclass


def alignment_geo_data(refset, targetset=None):
    """Align two sets of records.

    :param refset: set of reference data (based on imported FindingAid(s))
    :param targetset: set of target data (GeoNames)

    An element in refset is a 8-tuple
    (part of auth_label before parenthesis, [name of dpt, name of city, geonamid country],
    autheid, geogname_uri, geogname_label, auth_uri, auth_label (mod.), auth_label (orig.)).

    An element in targetset is a 8-tuple
    (geonameid, geoname_label, [name of dpt, name of city, geonameid country],
    '', geoname_label (UI display), geoname_label (mod.), latitude, longitude).

    The labels auth_label (mod.) and geoname_label (mod.) are used in NGram comparison.
    """
    processing = BaseProcessing(
        ref_attr_index=0, target_attr_index=1, distance_callback=approx_match
    )
    # attribute index is not the same in set of reference data and targetset
    # given that the FindingAid entity ID has been removed from the former
    place_normalizer_ref = NormalizerPipeline((SimplifyNormalizer(attr_index=0),))
    place_normalizer_target = NormalizerPipeline((SimplifyNormalizer(attr_index=1),))
    # Define aligner 1 - Align using france departements and city
    city_dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    city_dpt_aligner.register_ref_normalizer(place_normalizer_ref)
    city_dpt_aligner.register_target_normalizer(place_normalizer_target)
    blocking_1 = KeyBlocking(1, 2, ignore_none=True, callback=dpt_city_block_cb)
    blocking_2 = MinHashingBlocking(0, 1, threshold=0.4)
    city_dpt_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    city_dpt_aligner.register_blocking(city_dpt_blocking)
    # Define aligner 2 - Align using france departements
    dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    dpt_aligner.register_ref_normalizer(place_normalizer_ref)
    dpt_aligner.register_target_normalizer(place_normalizer_target)
    blocking_1 = KeyBlocking(1, 2, ignore_none=True, callback=dpt_block_cb)
    blocking_2 = MinHashingBlocking(0, 1, threshold=0.4)
    dpt_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    dpt_aligner.register_blocking(dpt_blocking)
    # Define aligner 4 - Align remaining data
    ngram_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    ngram_aligner.register_ref_normalizer(place_normalizer_ref)
    ngram_aligner.register_target_normalizer(place_normalizer_target)
    blocking_1 = NGramBlocking(6, 5, ngram_size=3, depth=1)
    blocking_2 = MinHashingBlocking(6, 5, threshold=0.4)
    no_countries_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    ngram_aligner.register_blocking(no_countries_blocking)
    # Launch the pipeline
    return list(
        PipelineAligner((city_dpt_aligner, dpt_aligner, ngram_aligner)).get_aligned_pairs(
            refset, targetset
        )
    )


def alignment_geo_data_countryonly(refset, targetset):
    """Align two sets of records."""
    place_normalizer_ref = NormalizerPipeline((SimplifyNormalizer(attr_index=0),))
    place_normalizer_target = NormalizerPipeline((SimplifyNormalizer(attr_index=1),))
    # Define aligner 3 - Align using countries
    country_processing = BaseProcessing(distance_callback=lambda x, y: 0.0)
    country_aligner = BaseAligner(threshold=0.2, processings=(country_processing,))
    country_aligner.register_ref_normalizer(place_normalizer_ref)
    country_aligner.register_target_normalizer(place_normalizer_target)
    blocking_1 = KeyBlocking(1, 2, ignore_none=True, callback=country_block_cb)
    country_aligner.register_blocking(blocking_1)
    # Launch the pipeline
    return list(PipelineAligner((country_aligner,)).get_aligned_pairs(refset, targetset))


def alignment_geo_data_topographic(refset, targetset):
    """Align two sets of records."""
    place_normalizer_ref = NormalizerPipeline((SimplifyNormalizer(attr_index=(0, 6)),))
    place_normalizer_target = NormalizerPipeline((SimplifyNormalizer(attr_index=1),))
    # Define aligner 5 - Align using topographic features (prefixed) and department
    processing = BaseProcessing(
        ref_attr_index=6, target_attr_index=1, distance_callback=approx_match
    )
    aligner = BaseAligner(threshold=0.2, processings=(processing,))
    aligner.register_ref_normalizer(place_normalizer_ref)
    aligner.register_target_normalizer(place_normalizer_target)
    blocking1 = KeyBlocking(1, 2, ignore_none=True, callback=dpt_block_cb)
    blocking2 = KeyBlocking(1, 2, ignore_none=False, callback=fclass_block_cb)
    blocking = PipelineBlocking((blocking1, blocking2), collect_stats=True)
    aligner.register_blocking(blocking)
    # Define aligner 6 - Align using topographic features (unmodified) and department
    unmodified_processing = BaseProcessing(
        ref_attr_index=0, target_attr_index=1, distance_callback=approx_match
    )
    unmodified_aligner = BaseAligner(threshold=0.2, processings=(unmodified_processing,))
    unmodified_aligner.register_ref_normalizer(place_normalizer_ref)
    unmodified_aligner.register_target_normalizer(place_normalizer_target)
    blocking1 = KeyBlocking(1, 2, ignore_none=True, callback=dpt_block_cb)
    blocking2 = KeyBlocking(1, 2, ignore_none=False, callback=fclass_block_cb)
    unmodified_blocking = PipelineBlocking((blocking1, blocking2), collect_stats=True)
    unmodified_aligner.register_blocking(unmodified_blocking)
    # Define aligner 7 - Align using topographic features (prefixed) without department
    aligner_no_dpt = BaseAligner(threshold=0.1, processings=(processing,))
    aligner_no_dpt.register_ref_normalizer(place_normalizer_ref)
    aligner_no_dpt.register_target_normalizer(place_normalizer_target)
    blocking1 = KeyBlocking(1, 2, ignore_none=False, callback=fclass_block_cb)
    blocking2 = MinHashingBlocking(6, 1, threshold=0.4)
    blocking_no_dpt = PipelineBlocking((blocking1, blocking2), collect_stats=True)
    aligner_no_dpt.register_blocking(blocking_no_dpt)
    # Define aligner 8 - Align using topographic features (unmodified) without department
    unmod_aligner_no_dpt = BaseAligner(threshold=0.1, processings=(unmodified_processing,))
    unmod_aligner_no_dpt.register_ref_normalizer(place_normalizer_ref)
    unmod_aligner_no_dpt.register_target_normalizer(place_normalizer_target)
    blocking1 = KeyBlocking(1, 2, ignore_none=False, callback=fclass_block_cb)
    blocking2 = MinHashingBlocking(0, 1, threshold=0.4)
    blocking_unmod_no_dpt = PipelineBlocking((blocking1, blocking2), collect_stats=True)
    unmod_aligner_no_dpt.register_blocking(blocking_unmod_no_dpt)
    return list(
        PipelineAligner(
            (aligner, unmodified_aligner, aligner_no_dpt, unmod_aligner_no_dpt)
        ).get_aligned_pairs(refset, targetset)
    )

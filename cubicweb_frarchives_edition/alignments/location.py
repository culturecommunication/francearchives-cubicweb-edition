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
from __future__ import print_function
import json
import os.path as osp
import difflib

from nazca.utils.normalize import NormalizerPipeline, SimplifyNormalizer
from nazca.utils.distances import BaseProcessing
from nazca.rl.blocking import KeyBlocking, PipelineBlocking, NGramBlocking, MinHashingBlocking
from nazca.rl.aligner import BaseAligner, PipelineAligner

from cubicweb_frarchives_edition.alignments.utils import simplify


HERE = osp.dirname(osp.abspath(__file__))


DPT_COUNTRY_MAP = {
    '971': 'GP',
    '972': 'MQ',
    '973': 'GY',
    '974': 'RE',
    '976': 'YT',
    '975': 'PM',
    '977': 'BL',
    '978': 'MF',
    '986': 'WF',
    '987': 'PF',
    '988': 'NC',
}


with open(osp.join(HERE, 'city_code.json')) as f:
    city_code = json.load(f)
city_code_to_name = {code: simplify(name)for name, code in city_code.items()}
city_name = set(city_code_to_name.values())

with open(osp.join(HERE, 'dpt_code.json')) as f:
    dpt_code = json.load(f)
    code_dpt = dict([(name, code) for code, name in dpt_code.items()])

with open(osp.join(HERE, 'regions_code.json')) as f:
    regions_code = json.load(f)
    code_regions = dict([(name, code) for code, name in regions_code.items()])

dpt_code_to_name = {code: simplify(name) for name, code in dpt_code.items()}
dpt_name = set(dpt_code_to_name.values())

with open(osp.join(HERE, 'alternatenames.json')) as fp:
    alternatenames = json.load(fp)

country_name_to_code = {
    simplify(name): code for name, code in alternatenames.items()
}
country_code_to_name = {code: name for name, code in alternatenames.items()}


def build_countries_geoname_set(cnx, geonameids):
    """build a set only with countries records"""
    geoname_set = []
    if not geonameids:
        return geoname_set
    q = '''
        SELECT geonameid, name
        FROM geonames WHERE geonameid IN (%(geonameids)s)
    '''
    geonameids = ", ".join(str(i) for i in geonameids)
    crs = cnx.system_sql(q % {"geonameids": geonameids})
    for id, name in crs.fetchall():
        label = country_code_to_name.get(id).encode('utf-8')
        # id, label, (department label, city label, country geonameid), empty string,
        # label for display in UI, string for ngram comparaison
        geoname_set.append([id, name, (None, None, id), '', label, label])
    return geoname_set


def build_geoname_set(cnx, dpt_code=None):
    q = '''
        SELECT tmp.geonameid, tmp.name, tmp.admin1_code,
               tmp.admin2_code, tmp.admin4_code, tmp.fclass
        FROM (SELECT geonameid, name, admin1_code, admin2_code, admin4_code, fclass,
              ROW_NUMBER() OVER (PARTITION BY name, admin1_code, admin2_code, admin4_code
              ORDER BY fclass desc) rank
        FROM geonames
        WHERE country_code=%(country)s AND fclass IN ('A', 'P')
        '''
    dpt = dpt_code
    if dpt is not None:
        q += ' AND admin2_code = %(dpt)s'
    q += ') as tmp WHERE tmp.rank = 1'
    crs = cnx.system_sql(
        q, {'dpt': dpt, 'country': DPT_COUNTRY_MAP.get(dpt, 'FR')}
    )
    geoname_set = []
    for id, name, ad1, ad2, ad4, fclass in crs.fetchall():
        dpt, city, country = None, None, None
        comp_string = name
        if ad2 and dpt_code_to_name.get(ad2):
            dpt = dpt_code_to_name.get(ad2)
        city = city_code_to_name.get(ad4)
        if dpt:
            if dpt_code is None:
                info = u', '.join(e for e in [dpt, city] if e)
            else:
                # do not add the city as we dont have it for the
                # matching pnia records_dptonly
                info = dpt
            comp_string = u'{} ({})'.format(name, info.encode('utf-8'))
        label = u'{} ({})'.format(
            name,
            u', '.join(
                part
                for part in (
                    code_regions.get(ad1),
                    code_dpt.get(ad2),
                )
                if part
            )
        ).encode('utf-8')
        geoname_set.append([id, name, (dpt, city, country), '', label, comp_string])
    return geoname_set


def approx_match(x, y):
    return 1.0 - difflib.SequenceMatcher(None, x, y).ratio() if x != y else 0.0


def dpt_block_cb(record):
    dpt, city, _ = record
    return dpt


def dpt_city_block_cb(record):
    dpt, city, _ = record
    return dpt, city


def country_block_cb(record):
    _, _, country = record
    return country


def alignment_geo_data(refset, targetset=None):
    """Align two sets of records.

    each record in refset is expected to be a 7-tuple:
      id,
      label,
      (department label, city label, country geonameid),
      origlabel,
      facomponent id,
      authority id,
      string for ngram comparaison
    each record in targetset is expected to be a 6-tuple:
      id,
      label,
      (department label, city label, country geonameid),
      empty string,
      label for display in UI,
      string for ngram comparaison
    """
    processing = BaseProcessing(
        ref_attr_index=1, target_attr_index=1, distance_callback=approx_match
    )
    place_normalizer = NormalizerPipeline((SimplifyNormalizer(attr_index=1),))
    # Define aligner 1 - Align using france departements and city
    city_dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    city_dpt_aligner.register_ref_normalizer(place_normalizer)
    city_dpt_aligner.register_target_normalizer(place_normalizer)
    blocking_1 = KeyBlocking(2, 2, ignore_none=True, callback=dpt_city_block_cb)
    blocking_2 = MinHashingBlocking(1, 1, threshold=0.4)
    city_dpt_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    city_dpt_aligner.register_blocking(city_dpt_blocking)
    # Define aligner 2 - Align using france departements
    dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    dpt_aligner.register_ref_normalizer(place_normalizer)
    dpt_aligner.register_target_normalizer(place_normalizer)
    blocking_1 = KeyBlocking(2, 2, ignore_none=True, callback=dpt_block_cb)
    blocking_2 = MinHashingBlocking(1, 1, threshold=0.4)
    dpt_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    dpt_aligner.register_blocking(dpt_blocking)
    # Define aligner 4 - Align remaining data
    ngram_aligner = BaseAligner(threshold=0.2, processings=(processing,))
    ngram_aligner.register_ref_normalizer(place_normalizer)
    ngram_aligner.register_target_normalizer(place_normalizer)
    blocking_1 = NGramBlocking(6, 5, ngram_size=3, depth=1)
    blocking_2 = MinHashingBlocking(6, 5, threshold=0.4)
    no_countries_blocking = PipelineBlocking((blocking_1, blocking_2), collect_stats=True)
    ngram_aligner.register_blocking(no_countries_blocking)
    # Launch the pipeline
    return list(PipelineAligner((
        city_dpt_aligner,
        dpt_aligner,
        ngram_aligner
    )).get_aligned_pairs(refset, targetset))


def alignment_geo_data_countryonly(refset, targetset):
    """Align two sets of records."""
    place_normalizer = NormalizerPipeline((SimplifyNormalizer(attr_index=1),))
    # Define aligner 3 - Align using countries
    country_processing = BaseProcessing(distance_callback=lambda x, y: 0.0)
    country_aligner = BaseAligner(threshold=0.2, processings=(country_processing,))
    country_aligner.register_ref_normalizer(place_normalizer)
    country_aligner.register_target_normalizer(place_normalizer)
    blocking_1 = KeyBlocking(2, 2, ignore_none=True, callback=country_block_cb)
    country_aligner.register_blocking(blocking_1)
    # Launch the pipeline
    return list(
        PipelineAligner((country_aligner,)).get_aligned_pairs(refset, targetset)
    )

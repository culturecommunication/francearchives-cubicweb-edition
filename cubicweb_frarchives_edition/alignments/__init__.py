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
"""cubicweb-frarchives_edition geo-alignments data"""
from collections import namedtuple

import logging

import re
import requests

import SPARQLWrapper

import time
import urllib.parse

from cubicweb_frarchives_edition import GEONAMES_RE
from cubicweb_frarchives_edition.alignments.location import Geodata


# https://data.bnf.fr/11907966/victor_hugo
DATABNF_RE = re.compile(r"data.bnf.fr/.{0,3}(?P<notice_id>\d{8})/.+")
# .{0,3} is here for language infix (2 chars) followed by '/'
# databnf.fr ark domains is ARK 12148: ark:/12148/\w+?(?P<notice_id>\d{8})\w

DATABNF_ARK_RE = re.compile(r"data.bnf.fr/.{0,3}ark:/12148/\w{2}(?P<notice_id>\d{8})\w")
DATABNF_SOURCE = "databnf"

WIKIDATA_RE = re.compile(r"wikidata.org/wiki/(?P<wikiid>Q\d+)")
WIKIDATA_SOURCE = "wikidata"


def get_externaluri_data(uri):
    for source, regx in (
        (DATABNF_SOURCE, DATABNF_RE),
        (DATABNF_SOURCE, DATABNF_ARK_RE),
        (WIKIDATA_SOURCE, WIKIDATA_RE),
        ("geoname", GEONAMES_RE),
    ):
        m = regx.search(uri)
        if m:
            return source, m.group(1)
    source = urllib.parse.urlparse(uri).netloc
    if source == "data.bnf.fr":
        source = DATABNF_SOURCE
    elif source == "wikidata.org":
        source = WIKIDATA_SOURCE
    return source, None


class Literal(str):
    """wrap unicode literal resutls to hold additional metadata (e.g. lang)"""

    def __new__(cls, value, lang=None):
        inst = super(Literal, cls).__new__(cls, value)
        inst.lang = lang
        return inst


class SparqlRset(object):
    """tiny wrapper around SPARQLWrapper result structure

    ``SparqlRset`` allows to iterate on results either based on the selected
    variable index or the selected variable name.
    """

    def __init__(self, results):
        self._results = results
        self.fieldnames = results["head"]["vars"]
        self.row_cls = namedtuple("SparqlRsetRow", self.fieldnames)

    def _build_row(self, rowdef):
        # XXX use rdflib.term objects ?
        data = []
        for field in self.fieldnames:
            if field not in rowdef:
                fieldvalue = None
            else:
                fieldvalue = rowdef[field]["value"]
                if rowdef[field]["type"] == "literal":
                    fieldvalue = Literal(fieldvalue, lang=rowdef[field].get("xml:lang"))
                if rowdef[field]["type"] == "typed-literal":
                    if rowdef[field]["datatype"] == "http://www.w3.org/2001/XMLSchema#integer":
                        fieldvalue = int(fieldvalue)
            data.append(fieldvalue)
        return self.row_cls(*data)

    def __iter__(self):
        """iterates on results of a SPARQL query saved as json by the endpoint.

        yields:
          A namedtuple where each attribute maps one of the sparql variable.
          Values are not postprocessed.
        """
        for rowdef in self._results["results"]["bindings"]:
            yield self._build_row(rowdef)

    def __len__(self):
        return len(self._results["results"]["bindings"])

    def __getitem__(self, idx):
        return self._build_row(self._results["results"]["bindings"][idx])


class SPARQLDatabase(object):
    def __init__(self, endpoint, cache_dir=None, agent=SPARQLWrapper.__agent__):
        self.querier = SPARQLWrapper.SPARQLWrapper(endpoint, agent=agent)
        self.querier.setReturnFormat(SPARQLWrapper.JSON)
        self.logger = logging.getLogger("francearchives.alignment")
        # 15 sec must be suffisant, but we could find a better value
        self.querier.setTimeout(15)

    def execute(self, query):
        """perform `query` on the database endpoint"""
        self.logger.info('try query "%s"', query)
        try:
            self.querier.setQuery(query)
            self.querier.query().convert()
            raw_results = self.querier.query().convert()
            return SparqlRset(raw_results)
        except Exception:
            logging.exception("failed to execute SPARQL query %r", query)
            return {}


def compute_label_from_url(cnx, url):
    """Try to compute ExternalUri label.

    :param Connection cnx: CubicWeb database connection
    :param str url: GeoNames URL

    :return: label
    :rtype: str
    """
    if "geonames" in url.lower():
        return compute_geonames_label(cnx, url)
    else:
        return ""


def compute_geonames_label(cnx, url):
    """Create GeoNames label ('ville (region, departement)').

    :param Connection cnx: CubicWeb database connection
    :param str url: GeoNames URL

    :returns: label
    :rtype: str
    """
    # GeoNames URL is either e.g. https://www.geonames.org/2988507/paris.html
    # or e.g. https://www.geonames.org/2988507
    match = GEONAMES_RE.search(url)
    if not match:
        return ""
    geonameid = match.group(1)
    res = cnx.system_sql(
        """
        SELECT name, country_code, admin1_code, admin2_code
        FROM geonames WHERE geonameid = %(gid)s
        """,
        {"gid": geonameid},
    ).fetchall()
    if not res:
        return ""
    label, country_code, admin1_code, admin2_code = res[0]
    geodata = Geodata(cnx)
    if country_code == "FR":
        admin1_name = geodata.regions.get(admin1_code, "")
        admin2_name = geodata.departments.get(admin2_code, "")
        if admin1_name or admin2_name:
            label = "{} ({})".format(label, ", ".join(v for v in (admin1_name, admin2_name) if v))
    else:
        # for other countries
        # try to retrieve the french name
        res = cnx.system_sql(
            """
            SELECT alternate_name, MIN(rank)
            FROM geonames_altnames
            WHERE geonameid = %(gid)s
            AND isolanguage = 'fr'
            GROUP BY alternate_name, rank
            LIMIT 1;
            """,
            {"gid": geonameid},
        ).fetchall()
        if res:
            label = res[0][0]
        # only retrieve the country name
        res = geodata.countries.get(country_code, "")
        if res:
            label = "{} ({})".format(label, res)
    return label


class DataGouvQuerier(object):
    """This class contains the geo_query function which allows to interrogate
    the "Base Adresse Nationale" API from the French Government
    """

    endpoint = "https://api-adresse.data.gouv.fr/search"

    def __init__(self, **kwargs):
        self.logger = logging.getLogger("francearchives.data.gouv.alignment")

    def execute(self, **kwargs):
        """perform `query` on the database endpoint"""
        time.sleep(0.1)  # limit is 10 requests by second https://geo.api.gouv.fr/faq
        self.logger.info(f'try query "{self.endpoint}", {kwargs}')
        try:
            r = requests.get(self.endpoint, params=kwargs)
            return r.json()
        except Exception:
            logging.exception("failed to execute")

    def geo_query(self, address, city, postcode=None, citycode=None, limit=30):
        """
        :param address string: postal address
        :param city string: city
        :param postalcode string: code postal
        :param citycode string: code commune INSEE

        :returns: [longitude, latitude]

        Known issues:
        - City name comparison with or without accent (e.g., Epinal, Épinal)
        - Shortened city name (e.g., Saint-Ouen, Saint-Ouen-sur-Seine)
        """
        if not address:
            return None
        kwargs = {"q": address}
        if limit:
            kwargs["limit"] = limit
        if citycode:
            kwargs["citycode"] = citycode
        elif postcode:
            kwargs["postcode"] = postcode
        results = self.execute(**kwargs)
        if results:
            for res in results.get("features", []):
                if citycode:
                    if res["geometry"]["type"] == "Point":
                        return res["geometry"]["coordinates"]
                if res["properties"]["city"].lower() == city.lower():
                    if res["geometry"]["type"] == "Point":
                        return res["geometry"]["coordinates"]

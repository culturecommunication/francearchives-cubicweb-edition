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
"""cubicweb-frarchives-edition specific databnf utils"""

import json
from cubicweb_francearchives.views import STRING_SEP

from cubicweb_frarchives_edition.alignments.utils import strptime
from cubicweb_frarchives_edition.alignments import SPARQLDatabase


def literal2date(datestr):
    supported_formats = {
        "%Y": "y",
        "-%Y": "y",
        "%Y-%m": "m",
        "-%Y-%m": "m",
        "%m-%Y": "m",
        "-%m-%Y": "m",
        "%Y-%m-%d": "d",
        "-%Y-%m-%d": "d",
        "%d-%m-%Y": "d",
        "-%d-%m-%Y": "d",
    }
    isbc = False
    try:
        datestr = "".join(datestr.split())
        dateobj, format_ = strptime(datestr, *list(supported_formats.keys()))
        if format_.startswith("-"):
            isbc = True
        return dateobj.date(), supported_formats[format_], isbc
    except (ValueError,):
        pass
    try:
        dateint = int(datestr)
        if dateint < 0:
            isbc = True
        return strptime("{0:04d}".format(abs(dateint)), "%Y")[0].date(), "y", isbc
    except ValueError:
        return None, "d", isbc


def compute_dates(datestr, year):
    date = None
    if year is not None:
        # if year would skip year = 0 because bool(0) evaluates to False
        year = str(year)
    if datestr:
        date, precision, isbc = literal2date(datestr)
    if not date and year:
        date, precision, isbc = literal2date(year)
    if date:
        timestamp = date.strftime("%4Y-%m-%d")
        isdate = True
    else:
        timestamp = datestr or year
        isdate = False
    # data.bnf.fr does not adhere to ISO8601 standard (BCE dates)
    return {
        "timestamp": timestamp,
        "isdate": isdate,
        "isbc": isbc,
        "isiso": False,
        "precision": precision,
    }


class DataBnfDatabase(SPARQLDatabase):
    def __init__(self, *args, **kwargs):
        super(DataBnfDatabase, self).__init__("http://data.bnf.fr/sparql", *args, **kwargs)

    def author_query(self, extid):
        query = """
   PREFIX bnf-onto: <http://data.bnf.fr/ontology/bnf-onto/>
   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
   PREFIX foaf: <http://xmlns.com/foaf/0.1/>
   PREFIX bio: <http://vocab.org/bio/0.1/>

   SELECT ?label ?birthyear ?birthdate ?deathyear ?deathdate ?description WHERE {

        ?concept bnf-onto:FRBNF "%(extid)s"^^xsd:integer;
        skos:prefLabel ?label ;
        foaf:focus ?person.

        OPTIONAL {
        ?person bnf-onto:firstYear ?birthyear.
        }

        OPTIONAL {
        ?person bio:birth ?birthdate.
        }

        OPTIONAL {
        ?person bnf-onto:lastYear ?deathyear.
        }

        OPTIONAL {
        ?person bio:death ?deathdate.
        }

        OPTIONAL {
        ?concept skos:note ?description
        FILTER(lang(?description)="fr")
        }
   }""" % {
            "extid": extid
        }  # noqa
        return self.execute(query)

    def agent_infos(self, extid):
        rset = self.author_query(extid)
        data_infos = {}
        descriptions = set()
        if not rset:
            return data_infos
        for label, birthyear, birthdate, deathyear, deathdate, description in rset:
            data_infos["label"] = label
            dates = {}
            if birthdate or birthyear:
                dates["birthdate"] = compute_dates(birthdate, birthyear)
            if deathdate or deathyear:
                dates["deathdate"] = compute_dates(deathdate, deathyear)
            if description:
                descriptions.add(description)
            data_infos["dates"] = json.dumps(dates)
        if descriptions:
            data_infos["description"] = STRING_SEP.join(descriptions)
        return data_infos

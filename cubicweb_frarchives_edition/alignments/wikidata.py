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
"""cubicweb-frarchives-edition specific wikidata utils"""

import json
from cubicweb_francearchives import get_user_agent
from cubicweb_frarchives_edition.alignments import SPARQLDatabase
from cubicweb_frarchives_edition.alignments.utils import strptime

WIKIDATA_PRECISION = {"9": "y", "10": "m", "11": "d"}


def compute_dates(datestr, precision):
    precision = WIKIDATA_PRECISION.get(precision)
    supported_formats = {
        "%Y-%m-%dT%H:%M:%SZ": False,
        "+%Y-%m-%dT%H:%M:%SZ": False,
        "-%Y-%m-%dT%H:%M:%SZ": True,
    }
    if not precision:
        return {}
    try:
        dateobj, format_ = strptime(datestr, *list(supported_formats.keys()))
        return {
            "timestamp": dateobj.strftime("%4Y-%m-%d"),
            "isbc": supported_formats[format_],
            "isdate": True,
            "precision": precision,
            "isiso": True,
        }
    except ValueError:
        return {}


class WikidataDatabase(SPARQLDatabase):
    def __init__(self, *args, **kwargs):
        super(WikidataDatabase, self).__init__(
            "https://query.wikidata.org/sparql", agent=get_user_agent(), **kwargs
        )

    def agent_query(self, extid):
        # in case of multiple dates of birth and dates of death take
        # dates having lowest possible precision
        query = """
  PREFIX wikidata: <http://www.wikidata.org/entity/>

  SELECT DISTINCT ?personLabel ?birthdate ?deathdate ?birthprecision ?deathprecision ?personDesc WHERE {
  wikidata:%(extid)s rdfs:label ?personLabel.
  FILTER langMatches( lang(?personLabel), "FR" ).

  OPTIONAL{ wikidata:%(extid)s schema:description ?personDesc.
            FILTER langMatches( lang(?personDesc), "FR" ).}
  OPTIONAL { wikidata:%(extid)s p:P569/psv:P569 ?birth_date_node.  # birth date
            ?birth_date_node wikibase:timePrecision ?birthprecision ; # birth date has specific day
            wikibase:timeValue ?birthdate .
  }
  OPTIONAL { wikidata:%(extid)s p:P570/psv:P570 ?death_date_node.  # death date
            ?death_date_node wikibase:timePrecision ?deathprecision ; # death date has specific day
            wikibase:timeValue ?deathdate.
  }

} ORDER BY ?birthprecision ?deathprecision LIMIT 1""" % {  # noqa
            "extid": extid
        }
        return self.execute(query)

    def agent_infos(self, extid):
        rset = self.agent_query(extid)
        data_infos = {}
        for (label, birthdate, deathdate, birthprecision, deathprecision, description) in rset:
            data_infos["label"] = label
            dates = {}
            birthdate = compute_dates(birthdate, birthprecision)
            if birthdate:
                dates["birthdate"] = birthdate
            deathdate = compute_dates(deathdate, deathprecision)
            if deathdate:
                dates["deathdate"] = deathdate
            if description:
                data_infos["description"] = description
            data_infos["dates"] = json.dumps(dates)
        return data_infos

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
"""
this script groups LocationAutority which have been aligned on the same
geonames uri and:

 - are not aligned on any other geonames uri;

 - only have department|region|country in their label between brackets
   ex. Ytrac // Ytrac (Cantal) // Ytrac (France, Cantal) : ok
   ex. Bex, le (Ytrac, Cantal, France) : not ok

 - do not have any thing after brackets
   ex. Bex, le (Cantal, France) -- Chapelle du Pont-Neuf : not ok
   ex. Bex, le (Cantal, France) Chapelle du Pont-Neuf : not ok

cf.: https://extranet.logilab.fr/ticket/63948400

run  :
cubicweb-ctl index-es-suggest $active_instance
cubicweb-ctl index-es-suggest frarchives_edition

"""
from collections import defaultdict
import csv
from datetime import datetime
from itertools import chain
import re

from logilab.common.decorators import timed

from cubicweb_frarchives_edition import CANDIDATE_SEP, update_suggest_es
from cubicweb_frarchives_edition.alignments.utils import simplify
from cubicweb_frarchives_edition.alignments.location import Geodata


NOW = datetime.now()

CONTEXT_RE = re.compile(r"([^(]+)\(([^)]+)\)(\s*.*)")

query = """
SELECT array_agg(ARRAY[tmp.cw_label, tmp.cw_eid::text]), ext.cw_uri
FROM (
    SELECT
        loc.cw_label,
        loc.cw_eid
    FROM cw_locationauthority loc
    JOIN same_as_relation sar ON loc.cw_eid = sar.eid_from
    JOIN cw_externaluri ext ON sar.eid_to = ext.cw_eid
    WHERE ext.cw_source = 'geoname'
    GROUP BY 1, 2
    HAVING COUNT(loc.cw_eid) = 1
) AS tmp
JOIN same_as_relation sar ON tmp.cw_eid = sar.eid_from
JOIN cw_externaluri ext ON sar.eid_to = ext.cw_eid
WHERE ext.cw_source = 'geoname'
AND EXISTS (SELECT 1 from cw_geogname g WHERE g.cw_authority = tmp.cw_eid)
GROUP BY ext.cw_uri
HAVING COUNT(ext.cw_uri) > 1;
"""


countries = [
    "france",
]
articles = ["la ", "le ", "les "]


def art_simplify(label):
    label = simplify(label)
    for article in articles:
        if label.startswith(article):
            return label.split(article, 1)[1]
    return label


def write_log(msg, log=None):
    if log:
        log.info(msg)
    else:
        print(msg)


class Label(object):
    def __init__(
        self,
        cnx,
        label,
        eid,
        dpt,
        region,
        country,
        simplified_label,
        dpt_name,
        region_name,
        country_name,
    ):
        self.eid = eid
        self.label = label
        self.score = self.compute_score(dpt, region, country)
        self.encoded_label = label.encode("utf-8")
        self.simplified_label = simplified_label
        self.dpt_name = dpt_name
        self.region_name = region_name
        self.country_name = country_name
        url = "{}location".format(cnx.base_url())
        self.candidate_info = "{}{}{}".format(
            self.label, CANDIDATE_SEP, "{}/{}".format(url, self.eid)
        )

    def compute_score(self, dpt, region, country):
        """
        Lables order
        1/ department, country
        2/ country, department
        3/ department, region, country
        4/ country, region, department
        5/ other combinaisons avec country, region, department
        6/ department
        7/ region, country
        8/ country, region
        """
        if dpt == 1 and country == 2:
            return 9
        if country == 1 and dpt == 2:
            return 8
        if dpt == 1 and region == 2 and country == 3:
            return 7
        if country == 1 and region == 2 and dpt == 3:
            return 6
        if any((country, region, dpt)):
            return 5
        if dpt:
            return 4
        if region == 1 and country == 2:
            return 3
        if country == 1 and region == 2:
            return 2
        return 1


@timed
def group_location_authorities(cnx, dry_run=True, log=None):
    """
    group location authorities

    :param Connection cnx: CubicWeb database connection
    :param boolean dry_run: is True do not group entities, juste write the result
    :param Logging log
    """
    do_group, do_not_group = compute_location_authorities_to_group(cnx)
    filepath = "locations_not_to_group_{}{}{:02d}.csv".format(NOW.year, NOW.day, NOW.month)
    with open(filepath, "w") as fp:
        writer = csv.writer(fp)
        for idx, (label_to, other_labels) in enumerate(do_not_group.items()):
            writer.writerow([label_to.encoded_label] + [ol.encoded_label for ol in other_labels])
    to_group_filepath = "locations_to_group_{}{}{:02d}.csv".format(NOW.year, NOW.day, NOW.month)
    group_elts = 0
    with open(to_group_filepath, "w") as fp:
        writer = csv.writer(fp)
        for idx, (label_to, other_labels) in enumerate(do_group.items()):
            writer.writerow([label_to.encoded_label] + [ol.encoded_label for ol in other_labels])
            group_elts += len(other_labels) + 1
    if not dry_run:
        group_candidates(cnx, do_group, group_elts, log)


def group_candidates(cnx, do_group, group_elts, log):
    group_filepath = "grouped_geonamed_locationauthority_{}{}{:02d}.csv".format(
        NOW.year, NOW.day, NOW.month
    )
    write_log("group {} records, {} entities".format(len(do_group), group_elts), log)
    with cnx.allow_all_hooks_but(
        "reindex-suggest-es",
    ):
        with open(group_filepath, "w") as fp:
            writer = csv.writer(fp)
            for idx, (label_to, other_labels) in enumerate(do_group.items()):
                target = cnx.entity_from_eid(label_to.eid)
                sources = [cnx.entity_from_eid(ol.eid) for ol in other_labels]
                sources_info = [ol.encoded_label for ol in other_labels]
                line = [label_to.encoded_label] + sources_info
                writer.writerow(line)
                write_log(
                    "idx: {}, target: {}, source: {}".format(
                        idx, label_to.encoded_label, ", ".join(sources_info)
                    ),
                    log,
                )
                target.group([ol.eid for ol in other_labels])
                cnx.commit()
                update_suggest_es(cnx, [target] + sources)


def process_candidates(all_candidates):
    do_group, do_not_group = defaultdict(list), defaultdict(list)
    sorted_candidates = defaultdict(list)
    for candidate in all_candidates:
        # first sort on labels
        sorted_candidates[candidate.simplified_label].append(candidate)
    for label, candidates in list(sorted_candidates.items()):
        if len(candidates) == 1:
            do_not_group[label] = sorted(
                all_candidates, key=lambda x: x.simplified_label == label, reverse=True
            )
            continue
        candidates = sorted(candidates, key=lambda x: x.score, reverse=True)
        standard = candidates.pop(0)
        do_group[label].append(standard)
        dpt_name = simplify(standard.dpt_name)
        region_name = simplify(standard.region_name)
        country_name = simplify(standard.country_name)
        for candidate in candidates:
            if dpt_name and dpt_name == simplify(candidate.dpt_name):
                do_group[label].append(candidate)
                continue
            if region_name and region_name == simplify(candidate.region_name):
                do_group[label].append(candidate)
                continue
            if country_name and country_name == simplify(candidate.country_name):
                do_group[label].append(candidate)
                continue
            do_not_group[label].append(candidate)
    return do_group, do_not_group


class CountryLabel(object):
    def __init__(self, cnx, entity, label):
        self.eid = entity.eid
        self.label = label
        self.score = self.compute_score(entity)
        self.encoded_label = label.encode("utf-8")
        url = "{}location".format(cnx.base_url())
        self.candidate_info = "{}{}{}".format(
            self.encoded_label, CANDIDATE_SEP, "{}/{}".format(url, self.eid)
        )

    def compute_score(self, entity):
        serializer = entity.cw_adapt_to("ISuggestIndexSerializable")
        if serializer:
            json = serializer.serialize()
            return json["count"]
        return 0


def process_countries(cnx, auth_label, auth_eid, countries_to_group):
    """to be grouped, country labels must be identical"""
    geodata = Geodata(cnx)
    try:
        label = simplify(auth_label)
    except ValueError:
        # non ascii characters
        label = auth_label
    if label in list(geodata.simplified_countries.values()):
        countries_to_group[label].append(
            CountryLabel(cnx, cnx.entity_from_eid(auth_eid), auth_label)
        )  # noqa
        return True
    return False


def compute_location_authorities_to_group(cnx, log=None):
    geodata = Geodata(cnx)
    do_group = defaultdict(list)
    do_not_group = defaultdict(list)
    countries_to_group = defaultdict(list)
    rset = cnx.system_sql(query).fetchall()  # noqa
    write_log("found {} LocationAuthorities candidates to group".format(len(rset)), log)
    for items, geonames_url in rset:
        to_be_grouped = []
        candidates = []
        for auth_label, auth_eid in items:
            m = CONTEXT_RE.search(auth_label)
            if not m:
                process_countries(cnx, auth_label, auth_eid, countries_to_group)
                continue
            elif m.group(3):
                # got something after brackets
                continue
            label = m.group(1)
            if "," in label:
                continue
            context = m.group(2)
            tokens = set(chain(*[c.split(";") for c in context.split(",")]))
            try:
                simplified_label = art_simplify(label)
            except ValueError:
                # non ascii characters
                simplified_label = label
            dpt, region, country = (0,) * 3
            other = False
            data = {"dpt_name": "", "region_name": "", "country_name": ""}
            for ind, token in enumerate(tokens, 1):
                token = simplify(token)
                dpt_name = (
                    token
                    if token in list(geodata.simplified_departments.values())
                    else geodata.simplified_departments.get(token)
                )
                if dpt_name:
                    data["dpt_name"] = dpt_name
                    dpt = ind
                    continue
                simplified_regions = list(geodata.simplified_regions.values()) + list(
                    geodata.simplified_historic_regions.values()
                )
                region_name = (
                    token if token in simplified_regions else geodata.simplified_regions.get(token)
                )
                if region_name:
                    data["region_name"] = region_name
                    region = ind
                    continue
                country_name = token if token in countries else None
                if country_name:
                    data["country_name"] = country_name
                    country = ind
                else:
                    other = True
            if not other:
                candidates.append(
                    Label(
                        cnx,
                        auth_label,
                        auth_eid,
                        dpt,
                        region,
                        country,
                        simplified_label,
                        data["dpt_name"],
                        data["region_name"],
                        data["country_name"],
                    )
                )
        if len(candidates) > 1:
            to_be_grouped, not_to_be_grouped = process_candidates(candidates)
            for validated in list(to_be_grouped.values()):
                if len(validated) > 1:
                    do_group[validated[0]] = validated[1:]
            for rejected in list(not_to_be_grouped.values()):
                do_not_group[rejected[0]] = list(set(rejected[1:]))
    # process countries
    for country, candidates in list(countries_to_group.items()):
        if len(candidates) > 1:
            candidates = sorted(candidates, key=lambda x: x.score, reverse=True)
            do_group[candidates[0]] = candidates[1:]
    final_count = len(do_group)
    all_count = final_count + sum([len(i) for i in list(do_group.values())])
    write_log(
        "found {} LocationAuthorities to group into {} LocationAuthorities".format(
            all_count, final_count
        ),
        log,
    )
    return do_group, do_not_group


if __name__ == "__main__":
    group_location_authorities(cnx)  # noqa

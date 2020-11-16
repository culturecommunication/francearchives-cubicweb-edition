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

from collections import OrderedDict, defaultdict
from itertools import chain
import logging
import re

from nazca.utils.normalize import lunormalize, tokenize

from cubicweb_frarchives_edition import geonames_id_from_url
from cubicweb_frarchives_edition.alignments import location
from cubicweb_frarchives_edition.alignments.align import LocationRecord, LocationAligner
from cubicweb_frarchives_edition.alignments.utils import simplify


CONTEXT_RE = re.compile(r"([^(]+)\((.+)\)+")
BLACKLISTED = set(
    (
        "canton",
        "volcan",
        "tunnel",
        "region naturelle",
        "region historique",
        "province",
        "principaute",
        "estuaire",
        "etang",
        "mer",
        "diocese",
        "paroisse",
    )
)
ADMINISTRATIVE = set(
    (
        "region",
        "departement",
        "arrondissement",
        "departement d outre mer",
        "territoire d outre mer",
    )
)
TOPOGRAPHIC_FCLASS = {
    "chateau": "S",
    "aeroport": "S",
    "barrage": "S",
    "canal": "H",
    "lac": "H",
    "cours d eau": "H",
    "pic": "T",
    "chaine de montagnes": "T",
    "col": "T",
    "ile": "T",
    "archipel": "T",
    "presqu ile": "T",
    "plateau": "T",
    "foret": "V",
    "parc national": "L",
    "parc naturel": "L",
    "parc regional": "L",
    "port": "L",
}
TOPOGRAPHIC_FCLASS_RE = re.compile(
    r"({})(?:\s(?:de|du|de l|de la|d|des|a|a l|a la|au|aux))?\Z".format(
        r"|".join(TOPOGRAPHIC_FCLASS.keys())
    ),
    re.UNICODE,
)
CITY_WORDS = set(
    (
        "rue",
        "impasse",
        "passage",
        "place",
        "avenue",
        "boulevard",
        "cite",
        "quai",
        "pont",
        "espanade",
        "chemin",
        "villa",
        "ile",
        "ruelle",
        "allee",
        "parc",
        "jardin",
        "square",
    )
)


def _get_tokens(label):
    """Get list of tokens.

    :param str label: (part of) LocationAuthority label

    :returns: tokens
    :rtype: list
    """
    tokens = list(token.strip() for token in re.split("[;,()]", label) if token.strip())
    return tokens


def get_blacklisted(tokens):
    """Check if LocationAuthority is blacklisted feature.

    :param list tokens: list of tokens

    :returns: whether LocationAuthority is blacklisted feature
    :rtype: bool
    """
    # NOTE if preposition is included it is not matched
    # blacklist is more permissive than feature class check
    # 1/ regular expression is more expensive and 2/ avoid false positives
    return bool(BLACKLISTED.intersection(tokens))


def get_administrative(tokens):
    """Check if LocationAuthority is administrative feature.

    :param list tokens: list of tokens

    :returns: name of administrative feature if is topographic feature
    else empty string
    :rtype: str
    """
    common_tokens = ADMINISTRATIVE.intersection(tokens)
    if common_tokens:  # len(common_tokens) should be 1
        return next(iter(common_tokens))
    return ""


def get_topographic(tokens):
    """Check if LocationAuthority is topographic feature.

    :param list tokens: list of tokens

    :returns: name and fclass of topographic feature if is topographic feature
    else empty string
    :rtype: str
    """
    for token in tokens:
        match = TOPOGRAPHIC_FCLASS_RE.match(token)
        if match:
            return TOPOGRAPHIC_FCLASS.get(match.group(1), ""), token
    else:
        return "", ""


def process_tokens(name, tokens):
    """Check if LocationAuthority is either administrative or
    topographic feature.

    :param str name: name
    :param list tokens: list of tokens

    :returns: feature class and name of topographic feature or
    name of administrative feature (empty strings if neither),
    whether to skip LocationAuthority
    :rtype: tuple, str, bool
    """
    # check if any of tokens is blacklisted
    if get_blacklisted(tokens):
        return ("", ""), "", True
    topographic_name = ""
    administrative_name = ""
    # topographic key words take precedence which is an arbitrary decision
    fclass, topographic = get_topographic(tokens)
    if topographic:
        topographic_name = "{} {}".format(topographic, name)
    else:
        administrative = get_administrative(tokens)
        if administrative:
            if administrative in ["departement d outre mer", "territoire d outre mer"]:
                # DOM/TOM are countries in GeoNames: do not add 'departement' prefix for them
                administrative_name = name
            else:
                administrative_name = "{} {}".format(administrative, name)
    topographic_info = (fclass, topographic_name)
    return topographic_info, administrative_name, False


def process_before_parentheses(part):
    """Process part of LocationAuthority label before parentheses
    (or entire label if none).

    :param str part: (part of) label

    :returns: feature class and name of topographic feature or
    name of administrative feature (empty strings if neither), whether to
    skip LocationAuthority
    :rtype: tuple, str, bool
    """
    # do not take part after '--' into account
    # (see list of examples https://extranet.logilab.fr/ticket/65749407)
    tokens = _get_tokens(part.split("--", 1)[0])
    topographic_info, administrative_name = ("", ""), ""
    skip = False
    if len(tokens) > 1:
        # 1st token is name of place
        simplified_tokens = [simplify(token) for token in tokens[1:]]
        topographic_info, administrative_name, skip = process_tokens(tokens[0], simplified_tokens)
    return topographic_info, administrative_name, skip


def process_label(label):
    """Process LocationAuthority label.

    :param str label: LocationAuthority label

    :returns: name of location, context, tokens, feature class and name of topographic feature,
    name of administrative feature, whether to skip LocationAuthority
    :rtype: tuple
    """
    match = CONTEXT_RE.search(label)
    if not match:
        name = label
        topographic_info, administrative_name, skip = process_before_parentheses(label)
        context = ""
        tokens = []
    else:
        name, context = match.group(1).strip(), match.group(2).strip()
        # check if part before parentheses contains topographic or
        # administrative feature
        topographic_info, administrative_name, skip = process_before_parentheses(name)
        tokens = set(simplify(token) for token in _get_tokens(context)) - CITY_WORDS
        if not any((topographic_info[0], administrative_name)):
            topographic_info, administrative_name, skip = process_tokens(name, tokens)
    return name, context, tokens, topographic_info, administrative_name, skip


def an_minutiers(service_code, faunitid, label, context=None):
    # unitid is not required
    if not faunitid:
        return False
    if service_code == "FRAN" and faunitid.startswith("MC/"):
        if context:
            if CITY_WORDS.intersection(tokenize(lunormalize(context))):
                return True
        try:
            label, info = label.split("--")
            if CITY_WORDS.intersection(tokenize(lunormalize(info))):
                return True
        except Exception:
            pass
    return False


def build_record(rows, geodata):
    """Build record based on LocationAuthority label.

    There are 3 record sets
    * records_fr is aligned to locations in France
    * records_dpt is aligned to locations in given department (including overseas territories)
    * records_countries is aligned to countries
    * records_topographic is aligned to topographic feature classes H, T, V
    and L, including S

    In general applies to the corresponding target set sizes
    targets_countries < targets_dpt < targets_fr

    :param list rows: location(s) related to the given FindingAid entities
    :param Geodata geodata: Geodata manager

    :returns: records_fr, records_dpt, records_countries
    :rtype: list, list, list
    """
    records_fr = []
    records_dpt = []
    records_countries = []
    records_topographic = []
    for (
        autheid,
        geogname_uri,
        geogname_label,
        auth_uri,
        auth_label,
        unitid,
        service_code,
        service_dptcode,
    ) in rows:
        name, context, tokens, topographic_info, administrative_name, skip = process_label(
            auth_label
        )
        if skip:
            continue
        record = [
            # is composed of:
            # part of LocationAuthority label before parenthesis,
            # (name of dpt, name of city, name of country, feature class if any),
            # LocationAuthority eid,
            # Geogname URI,
            # Geogname label,
            # LocationAuthority uri,
            # LocationAuthority label (mod., used for alignment),
            # LocationAuthority label (for user),
            None,
            [None, None, None, None],
            autheid,
            geogname_uri,
            geogname_label,
            auth_uri,
            auth_label,
            auth_label,
        ]
        record[0] = name
        topographic_fclass, topographic_name = topographic_info
        if topographic_name:
            record[1][3] = topographic_fclass
            record[6] = topographic_name
        if administrative_name:
            record[0] = administrative_name
        if not context:
            simplified_name = simplify(name)
            if administrative_name:
                if simplified_name in geodata.simplified_departments.values():
                    record[1][0] = simplified_name
                records_fr.append(record)
                continue
            if topographic_name:
                records_topographic.append(record)
                continue
            if simplified_name in geodata.simplified_countries.values():
                record[1][2] = simplified_name
                records_countries.append(record)
                continue
        else:
            for token in tokens:
                if token in geodata.simplified_blacklist.values():
                    # https://extranet.logilab.fr/ticket/67923708
                    if record[1][0] is None:
                        record[1][0] = token
                    continue
                if token in geodata.blacklist:
                    # https://extranet.logilab.fr/ticket/67923708
                    if record[1][0] is None:
                        record[1][0] = geodata.simplified_blacklist[token]
                    continue
                if token in geodata.simplified_departments.values():
                    record[1][0] = token
                if token in geodata.departments and record[1][0] is None:
                    record[1][0] = geodata.simplified_departments[token]
                if token in geodata.simplified_cities.values():
                    record[1][1] = token
                if token in geodata.simplified_countries.values():
                    record[1][2] = token
        if administrative_name:
            if not record[1][0]:
                simplified_name = simplify(name)
                if simplified_name in geodata.simplified_departments.values():
                    record[1][0] = simplified_name
                records_fr.append(record)
                continue
        if topographic_name:
            records_topographic.append(record)
            continue
        if record[1][2] and record[1][2] != "france":
            # context contains foreign country
            # Maison du Peuple (Bruxelles, Belgique)
            records_countries.append(record)
            continue
        if record[1][0]:
            # dpt is found
            if record[1][1]:
                record[0] = record[1][1]
            records_fr.append(record)
        else:
            if an_minutiers(service_code, unitid, auth_label, context=context):
                # a special rule for AN to be aligned on Paris
                # cf. https://extranet.logilab.fr/ticket/63909184
                record[0] = "Paris"
                record[1][0] = "paris"
                record[1][1] = "paris"
                record[6] = "Paris (paris, paris)"
                records_fr.append(record)
                continue
            if service_dptcode:
                # no dpt information was found between parentheses. The records_dpt
                # set will be later aligned with the service department data.
                # Here we juste add the service dpt label to the record for display
                # purpose
                record[1][0] = geodata.simplified_departments.get(service_dptcode)
                if record[1][0]:
                    record[6] = "{} {}".format(auth_label, record[1][0])
            records_dpt.append(record)
    return records_fr, records_dpt, records_countries, records_topographic


def cells_from_pairs(pairs, geonamerecords, pniarecords, simplified=False):
    for (_, pniaidx), (_, geonameidx), distance in pairs:
        pniarecord = pniarecords[pniaidx]
        geonamerecord = geonamerecords[geonameidx]
        row = [
            str(pniarecord[2]),  # LocationAuthority entity ID
            pniarecord[3],  # Geogname URI
            pniarecord[4],  # Geogname label
            pniarecord[5],  # LocationAuthority URI
            pniarecord[7],  # LocationAuthority label
            "http://www.geonames.org/{}".format(geonamerecord[0]),  # GeoNames URI
            geonamerecord[-4],  # GeoNames label (UI display)
            geonamerecord[7],  # longitude
            geonamerecord[6],  # latitude
            "yes",  # keep
            f"{1-distance:.3f}",  # confidence (default value)
        ]
        if simplified:
            # Geogname URI at index 1
            row.pop(1)
            # Geogname label at index 1 after removing Geogname URI
            row.pop(1)
        yield (row)


class GeonameRecord(LocationRecord):
    headers = OrderedDict(
        [
            ("identifiant_LocationAuthority", "autheid"),
            ("URI_Geogname", "geognameuri"),
            ("libelle_Geogname", "geognamelabel"),
            ("URI_LocationAuthority", "authuri"),
            ("libelle_LocationAuthority", "pnialabel"),
            ("URI_GeoNames", "geonameuri"),
            ("libelle_GeoNames", "geonamealignlabel"),
            ("longitude", "longitude"),
            ("latitude", "latitude"),
            ("keep", "keep"),
            ("fiabilite_alignement", "confidence"),
        ]
    )
    simplified_headers = OrderedDict(
        header for i, header in enumerate(headers.items()) if i not in (1, 2)
    )
    REQUIRED_HEADERS = (
        "identifiant_LocationAuthority",
        "libelle_LocationAuthority",
        "URI_GeoNames",
        "keep",
    )

    @property
    def sourceid(self):
        return self.geonameuri


class GeonameAligner(LocationAligner):
    """GeoNames Aligner."""

    record_type = GeonameRecord

    def __init__(self, cnx, log=None):
        super(GeonameAligner, self).__init__(cnx, log=log)
        self._geoname_set = None
        self.geodata = location.Geodata(cnx)

    def geoname_set(self):
        if self._geoname_set is None:
            self._geoname_set = location.build_geoname_set(self.cnx, self.geodata)
        return self._geoname_set

    def find_conflicts(self, to_modify, existing_alignment):
        """Find conflicting alignment(s).

        :param defaultdict to_modify: alignment(s) to modify

        :returns: authority entity IDs having conflicting alignment(s)
        :rtype: list
        """
        log = logging.getLogger("rq.task")
        conflicts = []
        for autheid, entries in to_modify.items():
            if len(entries) > 1:
                # new alignment(s)
                new = defaultdict(list)
                for key, record, keep in entries:
                    if keep:
                        new[key].append(record)
                # alignment(s) tagged to be added and removed
                remove_conflicts = tuple(key for key, _, keep in entries if not keep and key in new)
                if len(remove_conflicts):
                    log.warning(
                        (
                            "%d new alignments column "
                            "'identifiant_LocationAuthority' %s are also tagged "
                            "to be removed"
                        ),
                        len(remove_conflicts),
                        autheid,
                    )
                # check for conflicts (new alignments)
                new_conflicts = False
                # more than one new alignment
                if len(new) > 1:
                    log.warning(
                        ("%d new alignments column " "'identifiant_LocationAuthority' %s"),
                        len(new),
                        autheid,
                    )
                    new_conflicts = True
                # more than one row per alignment
                rows = [key for key in new if len(new[key]) > 1]
                for key in rows:
                    labels = set(record.geonamealignlabel for record in new[key])
                    if len(labels) > 1:
                        log.warning(
                            (
                                "%d different 'libelle_GeoNames' columns "
                                "found for combination of "
                                "'identifiant_LocationAuthority' %s "
                                "and 'URI_GeoNames' %s"
                            ),
                            len(labels),
                            *key
                        )
                        new_conflicts = True
                if any((remove_conflicts, new_conflicts)):
                    conflicts.append(autheid)
        return conflicts

    def process_csv(self, fp, existing_alignment, override_alignments=False):
        """Process CSV file.

        :param file fp: CSV file
        :param set existing_alignment: list of existing alignments
        :param bool override_alignments: toggle overwriting user-defined
        alignments on/off

        :returns: list of new alignments, list of alignments to remove
        :rtype: dict, dict
        """
        invalid = []
        alignments = []
        if override_alignments:
            to_modify = defaultdict(list)
            for autheid, sourceeid, record, keep, err in self._process_csv(fp):
                if err:
                    invalid.append(err)
                    continue
                to_modify[autheid].append(((autheid, sourceeid), record, keep))
            conflicts = self.find_conflicts(to_modify, existing_alignment)
            alignments = []
            for autheid, entries in to_modify.items():
                if autheid not in conflicts:
                    alignments += entries
        else:
            for autheid, sourceeid, record, keep, err in self._process_csv(fp):
                if err:
                    invalid.append(err)
                    continue
                alignments.append(((autheid, sourceeid), record, keep))
        if invalid:
            self.log.warning(
                "found missing value in required column(s): {}".format(";".join(invalid))
            )
        new_alignment, to_remove_alignment = self._fill_alignments(existing_alignment, alignments)
        return new_alignment, to_remove_alignment

    def compute_findingaid_alignments(self, findingaid_eids, simplified=False):
        pnia = self.fetch_locations(findingaid_eids)
        if not pnia:
            self.log.info("no location found in findingids skip alignment")
            return []
        geoname = self.geoname_set()
        if not geoname:
            self.log.info("no geonames data found, skip alignment")
            return []
        (
            pnia_records,
            pnia_records_dptonly,
            pnia_records_countryonly,
            pnia_records_topographic,
        ) = build_record(pnia, self.geodata)
        # first align location name with department mention
        self.log.info(
            "aligne les lieux avec contexte (%s lieux) vers %s lieux geoname",
            len(pnia_records),
            len(geoname),
        )
        pairs = location.alignment_geo_data(pnia_records, geoname)
        lines_iter = []
        lines_iter.append(cells_from_pairs(pairs, geoname, pnia_records, simplified=simplified))
        # then align location name without department mention
        # in this case we assume location refer to some place in departement
        # so geoname set is filter to match current department
        geoname = location.build_geoname_set(self.cnx, self.geodata, dpt_code=pnia[0][4])
        self.log.info(
            "aligne les lieux sans contexte (%s lieux) vers %s lieux "
            "geoname du département (%s)",
            len(pnia_records_dptonly),
            len(geoname),
            pnia[0][3],
        )
        pairs = location.alignment_geo_data(pnia_records_dptonly, geoname)
        lines_iter.append(
            cells_from_pairs(pairs, geoname, pnia_records_dptonly, simplified=simplified)
        )
        # then align to country
        geoname = location.build_countries_geoname_set(self.cnx)
        self.log.info(
            "aligne les pays sans contexte (%s lieux) vers %s pays geoname",
            len(pnia_records_countryonly),
            len(geoname),
        )
        pairs = location.alignment_geo_data_countryonly(pnia_records_countryonly, geoname)
        lines_iter.append(
            cells_from_pairs(pairs, geoname, pnia_records_countryonly, simplified=simplified)
        )
        # then align to topopgraphic feature classes
        geoname = location.build_topographic_geoname_set(self.cnx, self.geodata)
        self.log.info(
            "aligne les lieux topographiques (%s lieux) vers %s lieux geoname",
            len(pnia_records_topographic),
            len(geoname),
        )
        pairs = location.alignment_geo_data_topographic(pnia_records_topographic, geoname)
        lines_iter.append(
            cells_from_pairs(pairs, geoname, pnia_records_topographic, simplified=simplified)
        )
        lines = set(tuple(row) for row in chain(*lines_iter))
        self.log.info("found {} lines".format(len(lines) if lines else "0"))
        return lines

    def compute_existing_alignment(self):
        """Fetch existing alignment(s) from database.

        :returns: exiting alignment(s)
        :rtype: set
        """
        alignment_query = """DISTINCT Any X, E
        WHERE X is LocationAuthority, X same_as EX,
        EX uri E, EX source 'geoname'
        """
        return {(str(auth), exturi) for auth, exturi in self.cnx.execute(alignment_query)}

    def process_alignments(self, new_alignment, to_remove_alignment, override_alignments=False):
        """
        Add or remove alignements

        :param new_alignment dict: alignments to add
        :param to_remove_alignment dict: alignments to remove
        :param override_alignments bool: user action must or not be overridden
        """
        # first add new alignment
        failed = 0
        self.log.info("will create %s new alignments", len(new_alignment))
        if not (new_alignment or to_remove_alignment):
            return
        # XXX update existing ExternalUri
        existing_exturi = {
            uri: eid
            for eid, uri in self.cnx.execute(
                'Any X, U WHERE X is ExternalUri, X uri U, X source "geoname"'
            )
        }
        for (autheid, geonameuri), record in new_alignment.items():
            try:
                if not override_alignments:
                    is_in_sameas_history = self.cnx.system_sql(
                        """SELECT 1 FROM sameas_history
                        WHERE autheid=%(autheid)s AND sameas_uri=%(geonameuri)s
                        AND action=false""",
                        {"autheid": autheid, "geonameuri": geonameuri},
                    ).fetchall()
                    # user removed alignment, do not re-insert it
                    if is_in_sameas_history:
                        continue
                if geonameuri in existing_exturi:
                    ext = existing_exturi[geonameuri]
                else:
                    geonameid = geonames_id_from_url(geonameuri)
                    if not geonameid:
                        self.log.error(
                            "invalid GeoNames Uri %s for authority %s", geonameuri, autheid
                        )
                        continue
                    # (possibly mod.) GeoNames label in CSV takes precedence
                    # sameas-label hook will create it if GeoNames label is not
                    # given
                    label = record.geonamealignlabel or ""
                    ext = self.cnx.create_entity(
                        "ExternalUri",
                        uri=geonameuri,
                        label=label,
                        extid=geonameid,
                        source="geoname",
                    ).eid
                    existing_exturi[geonameuri] = ext
                query = """
                INSERT INTO same_as_relation (eid_from, eid_to)
                VALUES (%(l)s, %(ext)s)
                ON CONFLICT (eid_from, eid_to) DO NOTHING
                """
                self.cnx.system_sql(query, {"l": int(autheid), "ext": int(ext)})
                if override_alignments:
                    # user-defined alignment takes precedence over any
                    # existing alignment(s), therefore
                    # add other existing alignment(s) to list of alignments
                    # to be removed
                    result_set = self.cnx.execute(
                        """Any U WHERE X is ExternalUri, X uri U, A same_as X,
                        A eid %(autheid)s, X eid != %(ext)s,
                        X source 'geoname'""",
                        {"autheid": autheid, "ext": ext},
                    ).rows
                    to_remove_alignment.update(
                        {(autheid, geonameuri): tuple() for geonameuri, in result_set}
                    )
                    # update same-as relation history
                    query = """INSERT INTO sameas_history (sameas_uri, autheid, action)
                    VALUES (%(geonameuri)s, %(autheid)s, true)
                    ON CONFLICT (sameas_uri,autheid)
                    DO UPDATE SET action=true"""
                    self.cnx.system_sql(query, {"geonameuri": geonameuri, "autheid": autheid})
            except Exception:
                failed += 1
        if failed > 0:
            self.log.error(
                "failed to add all new alignments : %d/%d alignments could not be added",
                failed,
                len(new_alignment),
            )
        # then remove unwanted alignment
        failed = 0
        self.log.info("will remove %s alignments", len(to_remove_alignment))
        for autheid, geonameuri in to_remove_alignment:
            geonameid = existing_exturi[geonameuri]
            try:
                query = """DELETE FROM same_as_relation
                WHERE eid_from=%(autheid)s AND eid_to=%(ext)s"""
                if not override_alignments:
                    query += """
                    AND
                    NOT EXISTS(SELECT 1 FROM sameas_history sh
                    WHERE sh.sameas_uri = %(geonameuri)s
                    AND sh.autheid = %(autheid)s
                    AND sh.action=true
                    )
                    """
                self.cnx.system_sql(
                    query,
                    {
                        "autheid": autheid,
                        "ext": existing_exturi[geonameuri],
                        "geonameuri": geonameuri,
                    },
                )
                if override_alignments:
                    query = """INSERT INTO sameas_history (sameas_uri, autheid, action)
                    VALUES (%(geonameuri)s, %(autheid)s, false)
                    ON CONFLICT (sameas_uri,autheid)
                    DO UPDATE SET action=false"""
                    self.cnx.system_sql(query, {"autheid": autheid, "geonameuri": geonameuri})
                # remove the longitude/latitude
                geonameid = geonames_id_from_url(geonameuri)
                # reset longitude/latitude values related to the GeoNames alignment being removed
                # if any other alignment to GeoNames exists, the longitude/latitude values will
                # be updated in the subsequent step
                # alignments to BANO are not affected; if any BANO alignment exists, its
                # longitude/latitude values take precedence over any possible GeoNames alignment
                if geonameid:
                    query = """
                    UPDATE
                    cw_locationauthority as l SET cw_latitude=Null, cw_longitude=Null
                    FROM(
                        SELECT latitude, longitude FROM geonames
                        WHERE geonameid::int=%(geonameid)s
                    ) as geo
                    WHERE l.cw_eid = %(autheid)s
                    AND l.cw_latitude=geo.latitude
                    AND l.cw_longitude=geo.longitude
                    """
                    self.cnx.system_sql(
                        query,
                        {
                            "autheid": autheid,
                            "geonameid": geonameid,
                        },
                    )
                else:
                    # this should not happen
                    query = """
                    UPDATE
                    cw_locationauthority as l SET cw_latitude=Null, cw_longitude=Null
                    WHERE l.cw_eid = %(autheid)s"""
                    self.cnx.system_sql(query, {"autheid": autheid})
            except Exception:
                self.log.error("error will trying to remove alignment")
                failed += 1
        if failed > 0:
            self.log.error(
                "failed to remove all deprecated alignments : %d/%d could not be removed",
                failed,
                len(to_remove_alignment),
            )
        # update longitude/latitude
        self.log.info("update localisation")
        sql = """
            UPDATE
            cw_locationauthority as l SET cw_latitude=g.latitude, cw_longitude=g.longitude
            FROM
            (
               SELECT tmp.cw_extid, tmp.cw_eid
               FROM cw_externaluri tmp
               WHERE tmp.cw_source = 'geoname'
            ) eu
            JOIN same_as_relation sa ON (sa.eid_to = eu.cw_eid)
            JOIN geonames g ON g.geonameid = eu.cw_extid::int
            WHERE
            l.cw_eid = sa.eid_from
            AND NOT EXISTS (
                SELECT eid_from FROM same_as_relation tmp
                WHERE tmp.eid_to IN (SELECT cw_eid FROM cw_externalid WHERE cw_source='bano')
                AND eid_from=l.cw_eid
            )
            """
        if not override_alignments:
            # do not update if has been aligned previously
            sql += " AND l.cw_latitude IS NULL"
        self.cnx.system_sql(sql)
        try:
            self.cnx.commit()
        except Exception:
            self.log.error("failed to update database, all changes have been lost")

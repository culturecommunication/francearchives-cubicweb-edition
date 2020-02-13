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


# standard library imports
import re
from collections import defaultdict, OrderedDict

# third party imports

# library specific imports
from cubicweb_frarchives_edition.alignments import align
from cubicweb_frarchives_edition.alignments.utils import simplify


def cells_from_pairs(pairs, bano_set, records, simplified=False):
    """Build CubicWeb database row from aligned pairs."""
    for auth_index, bano_index in pairs:
        auth_record = records[auth_index]
        bano_record = bano_set[bano_index]
        ui_display = "{}, {}".format(bano_record[2], bano_record[1])
        row = [
            str(auth_record[0]),  # LocationAuthority entity ID
            auth_record[1],  # Geogname URI
            auth_record[2],  # Geogname label
            auth_record[3],  # LocationAuthority URI
            auth_record[4],  # LocationAuthority label
            bano_record[0],  # BANO ID
            ui_display,  # BANO label used for
            # UI display ('city, street name')
            str(bano_record[4]),  # longitude
            str(bano_record[3]),  # latitude
            "yes",  # keep
            "",  # confidence (default value)
        ]
        if simplified:
            # Geoname URI at index 1
            row.pop(1)
            # Geogname label at index 1 after removing Geogname URI
            row.pop(1)
        yield row


def _process_label(label, city=""):
    """Extract city and street name from LocationAuthority label.

    :param str label: label
    :param str city: city

    :returns: city and street name
    :rtype: dict
    """
    city_pattern = re.compile(r"(.+?)\s*(\(.+?\)\s*)?--\s*(.+)")
    # ' -- ' is in label
    city_match = city_pattern.match(label)
    if city_match:
        label = city_match.group(3).strip()
        if not city:
            city = city_match.group(1).strip()
    if not city:
        # do not process if no city is found
        return
    voie_pattern = re.compile(r"(?P<voie>.+?)\s*\(\s*(?P<type>.+?)(\s*;\s*.+?)?\)")
    voie_match = voie_pattern.match(label)
    if not voie_match:
        return {"voie": label, "city": city}
    else:
        voie_groupdict = voie_match.groupdict()
        return {"voie": "{type} {voie}".format(**voie_groupdict).strip(), "city": city}


def _normalize_voie(voie):
    """Normalize street name.

    :param str voie: street name

    :returns: normalized street name
    :rtype: str
    """
    voie = simplify(voie)
    prepositions = [
        simplify(preposition) for preposition in ("à", "au", "aux", "de", "d'", "du", "des")
    ]
    voie = sorted(voie.split())
    voie = " ".join(item for item in voie if item not in prepositions)
    return voie


def build_record(locations):
    """Build records to align.

    :param list locations: location(s) related to FindingAid/FAComponent(s)

    :param str label: label

    :return: record
    :rtype: list
    """
    for location in locations:
        auth_label, unitid, code, _ = location[4:]
        processed_label = {}
        # Minutiers des AN
        # unitid is not required
        if code == "FRAN" and unitid and unitid.startswith("MC/"):
            processed_label = _process_label(auth_label, city="Paris")
        elif "--" in auth_label:
            processed_label = _process_label(auth_label)
        if processed_label:
            yield location + (processed_label,)


def build_bano_set(cnx, city):
    """Build BANO data set."""
    sql = """SELECT banoid, voie, nom_comm, lat, lon
             FROM bano_whitelisted
             WHERE nom_comm=%(city)s"""
    return cnx.system_sql(sql, {"city": city}).fetchall()


class BanoRecord(align.LocationRecord):
    """BANO alignment record."""

    headers = OrderedDict(
        [
            ("identifiant_LocationAuthority", "autheid"),
            ("URI_Geogname", "geognameuri"),
            ("libelle_Geogname", "geognamelabel"),
            ("URI_LocationAuthority", "authuri"),
            ("libelle_LocationAuthority", "pnialabel"),
            ("bano id", "banoid"),
            ("bano label (used for UI display)", "banolabel"),
            ("longitude", "longitude"),
            ("latitude", "latitude"),
            ("keep", "keep"),
            ("fiabilite_alignement", "confidence"),
        ]
    )
    simplified_headers = OrderedDict(
        header for i, header in enumerate(headers.items()) if i not in (1, 2)
    )

    @property
    def sourceid(self):
        return self.banoid


class BanoAligner(align.LocationAligner):
    """BANO aligner."""

    record_type = BanoRecord

    def __init__(self, cnx, log=None):
        """Initialize BANO aligner.

        :param Connection cnx: CubicWeb database connection
        :param log: logger
        :type: Logger or None
        """
        super(BanoAligner, self).__init__(cnx, log=log)
        self._bano_sets = {}

    def bano_sets(self, cities):
        res = []
        for city in cities:
            if city not in self._bano_sets:
                self._bano_sets[city] = build_bano_set(self.cnx, city)
            res.extend(self._bano_sets[city])
        return res

    def compute_findingaid_alignments(self, findingaids, simplified=False):
        """Compute FindingAid alignment.

        :param list findingaids: list of imported FindingAids

        :returns: list of aligned pairs
        :rtype: list
        """
        locations = self.fetch_locations(findingaids)
        if not locations:
            self.log.info("no location found, skip BANO alignment")
            return []
        records = list(build_record(locations))
        cities = list(set(record[-1]["city"] for record in records))
        bano_set = self.bano_sets(cities)
        if bano_set is None:
            self.log.info("no BANO data found, skip BANO alignment")
            return []
        pairs = self.align(bano_set, records)
        rows = list(cells_from_pairs(pairs, bano_set, records, simplified=simplified))
        rows = set(tuple(row) for row in rows)
        return rows

    def compute_existing_alignment(self):
        """Fetch existing alignment(s) from database.

        :returns: exiting alignment(s)
        :rtype: set
        """
        alignment_query = """DISTINCT Any X, E WHERE
        X is LocationAuthority, X same_as EX,
        EX extid E, EX source 'bano'
        """
        return {(str(auth), extid) for auth, extid in self.cnx.execute(alignment_query)}

    def process_csv(self, fp, existing_alignment, override_alignments=False):
        """Process CSV file.

        :param file fp: CSV file
        :param set existing_alignment: list of existing alignments
        :param bool override_alignments: toggle overwriting user-defined
        alignments on/off

        :returns: list of new alignments, list of alignements to remove
        :rtype: set, set
        """
        alignments = [
            ((autheid, sourceeid), record, keep)
            for autheid, sourceeid, record, keep, err in self._process_csv(fp)
            if not err
        ]
        new_alignment, to_remove_alignment = self._fill_alignments(existing_alignment, alignments)
        return new_alignment, to_remove_alignment

    def align(self, bano_set, records):
        """Align FindingAid and related FAComponent(s) to BANO.

        :param list bano_set: BANO data set
        :param list records: pre-processed LocationAuthority entities related
        to FindingAid entities/FAComponent(s)

        :returns: list of index pairs
        :rtype: list
        """
        auth_labels = defaultdict(lambda: defaultdict(list))
        bano_labels = defaultdict(lambda: defaultdict(list))
        for i, record in enumerate(records):
            processed_label = record[-1]
            normalized_voie = _normalize_voie(processed_label["voie"])
            normalized_city = simplify(processed_label["city"])
            auth_labels[normalized_city][normalized_voie].append(i)
        for i, (_, voie, city, _, _) in enumerate(bano_set):
            # force UTF-8 encoding
            voie = voie.encode("utf-8", "ignore").decode("utf-8")
            city = city.encode("utf-8", "ignore").decode("utf-8")
            normalized_voie = _normalize_voie(voie)
            normalized_city = simplify(city)
            bano_labels[normalized_city][normalized_voie] = i
        pairs = []
        for normalized_city in auth_labels:
            if normalized_city in bano_labels:
                for normalized_voie in auth_labels[normalized_city]:
                    if normalized_voie in bano_labels[normalized_city]:
                        auth_indices = auth_labels[normalized_city][normalized_voie]
                        bano_index = bano_labels[normalized_city][normalized_voie]
                        pairs += [(auth_index, bano_index) for auth_index in auth_indices]
        return pairs

    def process_alignments(self, new_alignment, to_remove_alignment, override_alignments):
        """Update database.

        :param dict new_alignment: alignment(s) to add to database
        :param dict to_remove_alignment: alignment(s) to remove from database
        :param bool override_alignments: toggle overwriting user-defined alignments on/off
        """
        self.log.info("will create %s new alignments", len(new_alignment))
        if override_alignments:
            self.log.info("user actions will be overridden")
        else:
            self.log.info("user actions will not be overriden")
        existing_extid = {
            extid: eid
            for eid, extid in self.cnx.execute(
                'Any X, I WHERE X is ExternalId, X extid I, X source "bano"'
            )
        }
        # update ExternalId entities
        try:
            for (autheid, banoid), record in list(new_alignment.items()):
                if not override_alignments:
                    # user modified or removed alignment, do not update it
                    is_in_sameas_history = self.cnx.system_sql(
                        """
                        SELECT 1 FROM sameas_history
                        WHERE sameas_uri = %(banoid)s
                        AND autheid = %(autheid)s
                        """,
                        {"banoid": banoid, "autheid": record.autheid},
                    ).fetchall()
                    if is_in_sameas_history:
                        continue
                if banoid in existing_extid:
                    ext = existing_extid[banoid]
                else:
                    ext = self.cnx.create_entity(
                        "ExternalId", extid=banoid, label=record.banolabel, source="bano"
                    ).eid
                    existing_extid[banoid] = ext
                self.cnx.system_sql(
                    """
                    INSERT INTO same_as_relation (eid_from, eid_to)
                    VALUES (%(autheid)s, %(ext)s)
                    ON CONFLICT (eid_from, eid_to) DO NOTHING
                    """,
                    {"autheid": int(autheid), "ext": int(ext)},
                )
                if override_alignments:
                    self.cnx.system_sql(
                        """INSERT INTO sameas_history (sameas_uri,autheid,action)
                        VALUES (%(extid)s,%(autheid)s,true)
                        ON CONFLICT (sameas_uri,autheid) DO UPDATE SET action=true
                        """,
                        {"extid": banoid, "autheid": autheid},
                    )
        except Exception:
            self.log.exception("failed to add new alignments")
        self.log.info("will remove %s alignments", len(to_remove_alignment))
        try:
            sql = """DELETE FROM same_as_relation
            WHERE eid_from=%(autheid)s AND eid_to=%(ext)s"""
            for autheid, banoid in to_remove_alignment:
                self.cnx.system_sql(sql, {"autheid": autheid, "ext": existing_extid[banoid]})
                if override_alignments:
                    self.cnx.system_sql(
                        """INSERT INTO sameas_history (sameas_uri,autheid,action)
                        VALUES (%(extid)s,%(autheid)s,false)
                        ON CONFLICT (sameas_uri,autheid) DO UPDATE SET action=false
                        """,
                        {"extid": banoid, "autheid": autheid},
                    )
        except Exception:
            self.log.exception("failed to remove deprecated alignments")
        # alignment to BANO takes precedence over alignment to GeoNames
        # update in any case
        self.cnx.system_sql(
            """
            UPDATE cw_locationauthority as l
            SET cw_latitude=b.lat, cw_longitude=b.lon
            FROM (
                SELECT tmp.cw_eid, tmp.cw_extid FROM cw_externalid tmp
                WHERE tmp.cw_source = 'bano'
            ) eu
            JOIN same_as_relation sa ON sa.eid_to = eu.cw_eid
            JOIN bano_whitelisted b ON b.banoid = eu.cw_extid
            WHERE l.cw_eid = sa.eid_from
            """
        )
        try:
            self.cnx.commit()
        except Exception:
            self.log.exception("failed to update database, all changes have been lost")

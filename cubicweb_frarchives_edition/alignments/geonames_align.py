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

from collections import OrderedDict, Counter
import csv
from itertools import chain
import re
from six import text_type as unicode
import os

import logging

from nazca.utils.normalize import lunormalize, tokenize

from cubicweb_frarchives_edition import get_samesas_history
from cubicweb_frarchives_edition.alignments import location
from cubicweb_frarchives_edition.alignments.utils import simplify


CONTEXT_RE = re.compile(r'([^(]+)\(([^)]+)\)')

CITY_WORDS = ('rue', 'impasse', 'passage', 'place', 'avenue', 'boulevard',
              'cite', 'quai', 'pont', 'espanade', 'chemin', 'villa',
              'ile', 'ruelle', 'allee', 'parc', 'jardin', 'square')

CITY_WORDS_COUNTER = Counter(CITY_WORDS)


def an_minutiers(service_code, faunitid, label, context=None):
    if service_code == 'FRAN' and faunitid.startswith('MC/'):
        if context:
            if any(CITY_WORDS_COUNTER & Counter(tokenize(lunormalize(context)))):
                return True
        try:
            label, info = label.split('--')
            if any(CITY_WORDS_COUNTER & Counter(tokenize(lunormalize(info)))):
                return True
        except Exception:
            pass
    return False


def build_record(rows):
    records = []
    records_dptonly = []
    records_countryonly = []
    for faid, faunitid, label, service_code, service_dptcode, fac, autheid in rows:
        record = [faid, None, [None, None, None], label, fac, autheid, label]
        # a record will contain: findingaid eid, label (part before parantheses), [dpt name,
        # city name, country geonameid], original label, label for comparaison with geonames
        context = None
        m = CONTEXT_RE.search(label)
        if not m:
            record[1] = label
            # no context, could be country
            record[2][2] = location.country_name_to_code.get(simplify(label))
            if record[2][2]:
                records_countryonly.append(record)
                continue
        else:
            context = m.group(2)
            tokens = set(chain(*[
                c.split(';') for c in context.split(',')
            ]))
            record[1] = m.group(1).strip()
            for token in tokens:
                token = simplify(token)
                if token in location.dpt_name:
                    record[2][0] = token
                if token in location.dpt_code_to_name:
                    record[2][0] = location.dpt_code_to_name[token]
                if token in location.city_name:
                    record[2][1] = token
        if record[2][0] is not None:
            # dpt is found
            records.append(record)
        else:
            # no dpt information was found between parentheses so we will try to
            # align this record on geonames record restricted to current service
            # dpt
            if an_minutiers(service_code, faunitid, label, context):
                # a special rule for AN to be aligned on Paris
                # cf. https://extranet.logilab.fr/ticket/63909184
                record[1] = u'Paris'
                record[2][0] = u'paris'
                record[2][1] = u'paris'
                record[6] = u'Paris (paris, paris)'
                records.append(record)
            elif service_dptcode:
                dpt = location.dpt_code_to_name.get(service_dptcode)
                if dpt:
                    record[2][0] = dpt
                    record[6] = u'{} {}'.format(label, dpt)
            records_dptonly.append(record)
    return records, records_dptonly, records_countryonly


def cells_from_pairs(pairs, geonamerecords, pniarecords):
    for (_, pniaidx), (_, geonameidx) in pairs:
        pniarecord = pniarecords[pniaidx]
        geonamerecord = geonamerecords[geonameidx]
        yield [
            pniarecord[3].encode('utf-8'),  # orig auth label
            pniarecord[1].encode('utf-8'),  # auth label before parentheses
            geonamerecord[1].encode('utf-8'),  # geoname label
            'http://www.geonames.org/{}'.format(geonamerecord[0]),
            'yes',  # should we keep this alignement
            str(pniarecord[0]),  # findingaid eid
            str(pniarecord[-3] or ''),  # facomponent eid (or None)
            str(pniarecord[-2]),  # authority eid
            str(geonamerecord[0]),  # geoname id
            geonamerecord[-2],  # geoname label + admin codes for UI display
        ]


def process_csv(csvfile, existing_alignment, sameas_history=(),
                override_alignments=False):
    new_alignment = {}
    to_remove_alignment = {}
    reader = csv.DictReader(csvfile, delimiter='\t')
    for row in reader:
        if not row:
            continue
        row = {k.decode('utf-8'): v.decode('utf-8') for k, v in row.items()}
        r = Record(row)
        # while automatically aligning geonames (override_alignments=False)
        # first check if the alignment is already present in sameas_history in
        # which case do nothing
        if not override_alignments and (r.geonameuri, r.autheid) in sameas_history:
            continue
        remove = r.keep.lower() in ('n', 'no', 'non')
        keep = r.keep.lower() in ('y', 'yes', 'oui', 'o')
        key = (r.faeid, r.pnialabel, r.geonameuri)
        if keep and key not in existing_alignment:
            new_alignment[key] = r
        elif remove and key in existing_alignment:
            to_remove_alignment[key] = r
    return new_alignment, to_remove_alignment


class Record(object):
    headers = OrderedDict([
        ('pnia original label', 'pnialabel'),
        ('pnia name before parentheses (used for alignment)', 'pniaalignlabel'),
        ('geoname label (used for alignment)', 'geonamealignlabel'),
        ('geoname uri', 'geonameuri'),
        ('keep', 'keep'),
        ('findingaid eid', 'faeid'),
        ('facomponent eid', 'faceid'),
        ('pnia authority eid', 'autheid'),
        ('geoname id', 'geonameid'),
        ('geoname label with admin code (used for UI display)', 'geonamelabel'),
    ])

    def __init__(self, dictrow):
        for k, v in self.headers.items():
            self.__dict__[v] = dictrow.get(k)


class GeonameAligner(object):
    location_query = '''(
        Any F, FU, L, SN, SC, NULL, X WHERE X is LocationAuthority, X label L,
        F is FindingAid, F eid %(e)s, F did D, D unitid  FU,
        F service S, S code SN, S dpt_code SC,
        G index F, G authority X
        ) UNION (
        Any F, FU, L, SN, SC, FA, X WHERE X is LocationAuthority, X label L,
        F is FindingAid, F eid %(e)s,
        FA finding_aid F,  F did D, D unitid  FU,
        F service S, S code SN, S dpt_code SC,
        G index FA, G authority X
        )'''

    def __init__(self, cnx, log=None):
        if log is None:
            log = logging.getLogger()
        self.log = log
        self.log.info('initialize GeonameAligner')
        self.cnx = cnx
        self._geoname_set = None
        self._sameas_history = None

    def geoname_set(self):
        if self._geoname_set is None:
            self._geoname_set = location.build_geoname_set(self.cnx)
        return self._geoname_set

    def sameas_history(self):
        if self._sameas_history is None:
            self._sameas_history = get_samesas_history(self.cnx)
        return self._sameas_history

    def compute_findingaid_alignment(self, findingaid_eid):
        pnia = self.cnx.execute(self.location_query, {'e': findingaid_eid}).rows
        if not pnia:
            self.log.info('no location found in findingid %s skip alignment', findingaid_eid)
            return []
        geoname = self.geoname_set()
        if not geoname:
            self.log.info('no geonames data found, skip alignment')
            return []
        pnia_records, pnia_records_dptonly, pnia_records_countryonly = build_record(pnia)
        # first align location name with department mention
        self.log.info(u'aligne les lieux avec contexte (%s lieux) vers %s lieux geoname',
                      len(pnia_records), len(geoname))
        pairs = location.alignment_geo_data(pnia_records, geoname)
        lines = list(cells_from_pairs(pairs, geoname, pnia_records))
        # then align location name without department mention
        # in this case we assume location refer to some place in departement
        # so geoname set is filter to match current department
        geoname = location.build_geoname_set(self.cnx, dpt_code=pnia[0][4])
        self.log.info(u'aligne les lieux sans contexte (%s lieux) vers %s lieux '
                      u'geoname du département (%s)',
                      len(pnia_records_dptonly), len(geoname), pnia[0][3])
        pairs = location.alignment_geo_data(pnia_records_dptonly, geoname)
        lines += list(cells_from_pairs(pairs, geoname, pnia_records_dptonly))
        # then align to country
        geoname = location.build_countries_geoname_set(
            self.cnx,
            [record[2][2] for record in pnia_records_countryonly]
        )
        self.log.info(
            u'aligne les pays sans contexte (%s lieux) vers %s pays geoname',
            len(pnia_records_countryonly), len(geoname)
        )
        pairs = location.alignment_geo_data_countryonly(
            pnia_records_countryonly, geoname
        )
        lines += list(cells_from_pairs(pairs, geoname, pnia_records_countryonly))
        self.log.info('findingaid {}: found {} lines'.format(
            findingaid_eid, len(lines) if lines else '0'))
        return lines

    def compute_existing_alignment(self):
        query = '''(
            DISTINCT Any F, L, E WHERE X is LocationAuthority, X label L,
            F is FindingAid,
            G index F, G authority X,
            X same_as EX, EX uri E
            ) UNION (
            DISTINCT Any F, L, E WHERE X is LocationAuthority, X label L,
            F is FindingAid,
            FA finding_aid F,
            G index FA, G authority X,
            X same_as EX, EX uri E
        )'''
        return {(unicode(f), l, e) for f, l, e in self.cnx.execute(query)}

    def process_csvpath(self, csvpath, override_alignments=False):
        """
        Process csvfile to determine alignments to add and to delete

        :param csvpath string: csv file path
        :param override_alignments bool: user action must or not be overridden
        """
        existing_alignment = self.compute_existing_alignment()
        sameas_history = self.sameas_history() if not override_alignments else ()
        try:
            with open(csvpath) as f:
                new_alignment, to_remove_alignment = process_csv(
                    f, existing_alignment,
                    sameas_history=sameas_history,
                    override_alignments=override_alignments)
        finally:
            os.unlink(csvpath)
        self.process_alignments(new_alignment, to_remove_alignment,
                                override_alignments=override_alignments)

    def process_alignments(self, new_alignment, to_remove_alignment,
                           override_alignments=False):
        """
        Add or remove alignements

        :param new_alignment dict: alignments to add
        :param to_remove_alignment dict: alignments to remove
        :param override_alignments bool: user action must or not be overridden
        """
        # first add new alignment
        self.log.info('will create %s new alignments', len(new_alignment))
        if override_alignments:
            self.log.info('user actions will be overridden')
        else:
            self.log.info('user actions will not be overridden')
        if not (new_alignment or to_remove_alignment):
            return
        # XXX update existing ExternalUri
        existing_exturi = {
            uri: eid for eid, uri in self.cnx.execute(
                'Any X, U WHERE X is ExternalUri, X uri U, X source "geoname"')
        }
        try:
            for (faeid, pnialabel, geonameuri), record in new_alignment.items():
                if geonameuri in existing_exturi:
                    ext = existing_exturi[geonameuri]
                else:
                    ext = self.cnx.create_entity(
                        'ExternalUri',
                        uri=geonameuri,
                        label=record.geonamelabel,
                        extid=record.geonameid,
                        source=u'geoname',
                    ).eid
                    existing_exturi[geonameuri] = ext
                if not override_alignments:
                    # TODO try to make single request with INSERT
                    is_in_sameas_history = (self.cnx.system_sql('''
                    SELECT 1 FROM sameas_history sh
                    WHERE sh.sameas_uri = %(geonameuri)s AND sh.action=false''', {
                        'geonameuri': geonameuri}).fetchall())
                    # user removed alignment, do not re-insert it
                    if is_in_sameas_history:
                        continue
                query = '''
                INSERT INTO same_as_relation (eid_from, eid_to)
                VALUES (%(l)s, %(ext)s)
                ON CONFLICT (eid_from, eid_to) DO NOTHING
                '''
                self.cnx.system_sql(
                    query,
                    {'l': int(record.autheid), 'ext': int(ext)}
                )
            sql = (
                '''
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
                '''
            )
            if not override_alignments:
                # do not update if has been aligned previously
                sql += ' AND l.cw_latitude IS NULL'
            self.cnx.system_sql(sql)
        except Exception:
            self.log.exception('error will trying to add alignment')
        # then remove unwanted alignment
        self.log.info('will remove %s alignments', len(to_remove_alignment))
        try:
            for faeid, pnialabel, geonameuri in to_remove_alignment:
                query = ('''
                DELETE FROM same_as_relation
                USING cw_externaluri e,
                      cw_locationauthority l,
                      cw_geogname g,
                      index_relation ir
                WHERE
                e.cw_uri = %(geonameuri)s AND
                l.cw_label = %(pnialabel)s AND
                g.cw_authority = l.cw_eid AND
                ir.eid_from = g.cw_eid AND
                same_as_relation.eid_from = l.cw_eid AND
                same_as_relation.eid_to = e.cw_eid AND
                (
                ir.eid_to = %(faeid)s
                OR
                EXISTS(SELECT 1 FROM cw_facomponent fa
                JOIN cw_findingaid f ON fa.cw_finding_aid = f.cw_eid
                WHERE f.cw_eid = %(faeid)s AND ir.eid_to = fa.cw_eid)
                )''')
                if not override_alignments:
                    query += '''
                    AND
                    NOT EXISTS(SELECT 1 FROM sameas_history sh
                    WHERE sh.sameas_uri = %(geonameuri)s AND sh.action=true
                    )
                    '''
                self.cnx.system_sql(query, {
                    'pnialabel': pnialabel,
                    'faeid': faeid,
                    'geonameuri': geonameuri})
        except Exception:
            self.log.exception('error will trying to remove alignment')
        self.cnx.commit()

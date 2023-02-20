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
import csv
from collections import OrderedDict, defaultdict
import json
import logging

# third party imports
from urllib.parse import urljoin

# library specific imports
from cubicweb_francearchives.storage import S3BfssStorageMixIn

from cubicweb_frarchives_edition import get_samesas_history, IncompatibleFile
from cubicweb_frarchives_edition.alignments.databnf import compute_dates as databnf_dates
from cubicweb_frarchives_edition.alignments.wikidata import compute_dates as wikidata_dates


class Record(object):
    """Alignment record base class.

    :cvar OrderedDict headers: CSV headers
    :cvar tuple REQUIRED_HEADERS_ALIGN: required CSV headers used for alignements
    :cvar tuple REQUIRED_HEADERS_LABELS: required CSV headers used for labels changes
    :cvar tuple REQUIRED_HEADERS_QUALITY: required CSV headers used for quality changes
    """

    headers = OrderedDict([])
    REQUIRED_HEADERS_ALIGN = ()
    REQUIRED_HEADERS_LABELS = ()
    REQUIRED_HEADERS_QUALITY = ()
    BOOLEAN_HEADERS = ()
    BOOLEAN_VALUES = {False: ("no", "non"), True: ("yes", "oui")}

    def check_boolean(self, value):
        if value.lower() not in (self.BOOLEAN_VALUES[True] + self.BOOLEAN_VALUES[False]):
            return value
        return value.lower() in self.BOOLEAN_VALUES[True]

    def __init__(self, dictrow, align=True, labels=False, quality=False):
        """Initialize alignment record.

        :param dict dictrow: row
        :param Bool align: flag to check REQUIRED_HEADERS_LIGN
        :param Bool align: labels to check REQUIRED_HEADERS_LABELS
        :param Bool align: quality to check REQUIRED_HEADERS_QUALITY
        """
        self._validate(dictrow, align=align, labels=labels, quality=quality)
        for k, v in list(self.headers.items()):
            value = dictrow.get(k)
            if value and k in self.BOOLEAN_HEADERS:
                value = self.check_boolean(value)
            self.__dict__[v] = value

    def _validate(self, dictrow, align=True, labels=False, quality=False):
        """Validate row.

        :param dict dictrow: row
        :param Bool align: flag to check REQUIRED_HEADERS_LIGN
        :param Bool align: labels to check REQUIRED_HEADERS_LABELS
        :param Bool align: quality to check REQUIRED_HEADERS_QUALITY

        :raises: ValueError if row is invalid
        """
        required_headers = set()
        if align:
            required_headers |= set(self.REQUIRED_HEADERS_ALIGN)
        if labels:
            required_headers |= set(self.REQUIRED_HEADERS_LABELS)
        if quality:
            required_headers |= set(self.REQUIRED_HEADERS_QUALITY)
        set_columns = [k for k in dictrow if dictrow.get(k)]
        non_set_required = [k for k in list(required_headers) if k not in set_columns]
        if non_set_required:
            raise ValueError("{columns}".format(columns=",".join(non_set_required)))

    @classmethod
    def validate_csv(cls, fieldnames, align=True, labels=False, quality=False):
        """Validate CSV.

        :param list fieldnames: CSV headers to check again required headers
        :param Bool align: flag to check REQUIRED_HEADERS_LIGN
        :param Bool align: labels to check REQUIRED_HEADERS_LABELS
        :param Bool align: quality to check REQUIRED_HEADERS_QUALITY
        :raises: ValueError if CSV is invalid
        """
        required_headers = set()
        if align:
            required_headers |= set(cls.REQUIRED_HEADERS_ALIGN)
        if labels:
            required_headers |= set(cls.REQUIRED_HEADERS_LABELS)
        if quality:
            required_headers |= set(cls.REQUIRED_HEADERS_QUALITY)
        required_headers = list(required_headers)
        if not fieldnames:
            missing_required = required_headers
        else:
            missing_required = [k for k in required_headers if k not in fieldnames]
        if missing_required:
            raise ValueError(
                "following required columns are missing: {columns}".format(
                    columns=",".join(missing_required)
                )
            )


class ImportAligner(object):
    """ImportAligner base class.

    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    modified_alignments = False

    def __init__(self, cnx, log=None):
        """Initialize aligner.

        :param Connection cnx: CubicWeb database connection
        :param str cw_etype: Authority cw_etype

        :param log: logger
        :type: Logger or None
        """
        if log is None:
            self.log = logging.getLogger()
        else:
            self.log = log
        self.cnx = cnx
        self._sameas_history = None

    def sameas_history(self):
        if self._sameas_history is None:
            self._sameas_history = get_samesas_history(self.cnx)
        return self._sameas_history

    def _process_csv(self, fp):
        """Process CSV file (generator).

        :param file fp: CSV file

        :returns: alignment
        :rtype: list
        """
        log = logging.getLogger("rq.task")
        reader = csv.DictReader(fp, delimiter="\t")
        try:
            self.record_type.validate_csv(reader.fieldnames)
        except ValueError as exception:
            log.error(exception)
            raise exception
        for i, row in enumerate(reader):
            if row:
                try:
                    record = self.record_type(row)
                except ValueError as exception:
                    yield ("", "", "", "", "{} ({})".format(i + 1, exception))
                    continue
                keep = record.keep.lower() in record.BOOLEAN_VALUES[True]
                remove = record.keep.lower() in record.BOOLEAN_VALUES[False]
                if not any((keep, remove)):
                    log.warning(
                        "row %d contains invalid value '%s' in column 'keep' (skip)",
                        i + 1,
                        record.keep,
                    )
                yield (record.autheid, record.sourceid, record, keep, "")

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
            conflicts = self.find_conflicts(to_modify)
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

    def _fill_alignments(self, existing_alignment, alignments):
        """Fill lists of new alignments and alignments to remove.

        :param set existing_alignment: list of existing alignments
        :param list alignments: list of read-in alignments

        :returns: list of new alignments, list of alignments to remove
        :rtype: dict, dict
        """
        new_alignment = {}
        to_remove_alignment = {}
        for key, record, keep in alignments:
            if keep and key not in existing_alignment:
                new_alignment[key] = record
            elif not keep and key in existing_alignment:
                to_remove_alignment[key] = record
        return new_alignment, to_remove_alignment

    def process_csvpath(self, csvpath, override_alignments=False):
        """Process CSV file.

        :param str csvpath: CSV file path
        :param bool override_alignments: toggle overwriting user-defined alignments on/off
        """
        existing_alignment = self.compute_existing_alignment()
        st = S3BfssStorageMixIn()
        try:
            with st.storage_read_file(csvpath) as fp:
                new_alignment, to_remove_alignment = self.process_csv(
                    fp, existing_alignment, override_alignments=override_alignments
                )
        except UnicodeDecodeError:
            raise IncompatibleFile("File encoding is not UTF-8")
        self.process_alignments(
            new_alignment,
            to_remove_alignment,
            override_alignments=override_alignments,
        )
        self.modified_alignments = bool(new_alignment or to_remove_alignment)

    def process_alignments(self, new_alignment, to_remove_alignment, override_alignments=False):
        """Update database.

        :param dict new_alignment: alignment(s) to add to database
        :param dict to_remove_alignment: alignment(s) to remove from database
        :param bool override_alignments: toggle overwriting user-defined alignments on/off
        """
        raise NotImplementedError


class AgentRecord(Record):
    """AgentAuthority alignment record base class."""

    headers = OrderedDict(
        [
            ("eid", "autheid"),
            ("date naissance", "date_birth"),
            ("precision date naissance", "date_birth_precision"),
            ("date mort", "date_death"),
            ("precision date mort", "date_death_precision"),
            ("description", "description"),
            ("uri", "exturi"),
            ("source", "source"),
            ("confidence", "confidence"),
            ("external label", "extlabel"),
        ]
    )


class AgentAligner(object):
    """AgentAuthority alignment base class.

    :cvar obj record_cls: record class
    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    record_cls = AgentRecord

    def __init__(self, cnx, log=None):
        """Initialize AugentAuthority aligner.

        :param Connection cnx: CubicWeb database connection
        :param Logger log: logger
        """
        self.cnx = cnx
        self.log = log or logging.getLogger()

    def fetch_external_uris(self, source):
        """Fetch ExternalUris.

        :param str source: source

        :returns: external URIs
        :rtype: dict
        """
        external_uris = {
            uri: eid
            for eid, uri in self.cnx.execute(
                "Any X, U WHERE X is ExternalUri, X source %(source)s, X uri U", {"source": source}
            ).rows
        }
        return external_uris

    def process_csv(self, fp):
        """Process CSV file.

        :param file fp: CSV file

        :returns: alignments
        :rtype: dict
        """
        reader = csv.DictReader(fp, delimiter="\t")
        try:
            self.record_cls.validate_csv(reader.fieldnames)
        except ValueError as exception:
            self.log.error(exception)
            raise exception
        alignments = {}
        invalid = []
        for i, row in enumerate(reader, 1):
            if row:
                try:
                    record = self.record_cls(row)
                except ValueError as exception:
                    invalid.append("{} ({})".format(i, exception))
                    continue
                if float(record.confidence) < 0.7:
                    continue
                alignments[(record.autheid, record.exturi)] = record
        if invalid:
            self.log.warning(
                "found missing value in required column(s): {}".format(";".join(invalid))
            )
        return alignments

    def _process_date(self, date, precision, source):
        """Process date.

        :param str date: date
        :param str precision: precision
        :param str source: source

        :returns: processed date
        :rtype: dict
        """
        if source == "wikidata":
            return wikidata_dates(date, precision)
        else:
            return databnf_dates(date, None)

    def _process_alignment(self, alignment, external_uris):
        """Process alignment.

        :param tuple alignment: alignment
        :param dict external_uris: existing ExternalUri entities

        :returns: args needed to update AgentInfo
        :rtype: tuple
        """
        (agent_authority, exturi), record = alignment
        external_uri = external_uris.get(exturi)
        dates = {}
        if record.date_birth:
            birthdate = self._process_date(
                record.date_birth, record.date_birth_precision, record.source
            )
            if birthdate:
                dates["birthdate"] = birthdate
        if record.date_death:
            deathdate = self._process_date(
                record.date_death, record.date_death_precision, record.source
            )
            if deathdate:
                dates["deathdate"] = deathdate
        description = record.description or None
        dates = json.dumps(dates)
        if not external_uri:
            # ExternalUri does not exist, create ExternalUri
            external_uri = self.cnx.create_entity(
                "ExternalUri", uri=exturi, source=record.source, label=record.extlabel
            ).eid
            external_uris[exturi] = external_uri
        # delete related AgentInfo if any
        self.cnx.execute("DELETE AgentInfo X WHERE X agent_info_of %(eid)s", {"eid": external_uri})
        # create new AgentInfo
        self.cnx.create_entity(
            "AgentInfo", dates=dates, description=description, agent_info_of=external_uri
        )
        external_uris[exturi] = external_uri
        # insert same-as relation
        self.cnx.system_sql(
            """INSERT INTO same_as_relation (eid_from,eid_to)
            VALUES (%(eid_from)s,%(eid_to)s) ON CONFLICT (eid_from,eid_to) DO NOTHING""",
            {"eid_from": int(agent_authority), "eid_to": int(external_uri)},
        )

    def process_alignments(self, alignments):
        """Update database.

        :param dict alignments: alignments
        """
        if not alignments:
            self.log.warning("empty list of alignments")
            return
        source = list(alignments.values())[0].source
        exturis = self.fetch_external_uris(source)
        for alignment in list(alignments.items()):
            self._process_alignment(alignment, exturis)
        self.cnx.commit()


class LocationRecord(Record):
    """LocationAuthority alignment record base class."""

    @property
    def sourceid(self):
        """Map identifying attribute specific to data source to CSV column,
        e.g. exturi (GeoNames) or extid (BANO).
        """
        raise NotImplementedError


class LocationAligner(ImportAligner):
    """Aligner base class.

    :cvar str location_query: RQL query FindingAid and related FAComponent(s)
    :cvan type record_type: LocationRecord subclass
    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    location_query = """
    (
    Any FU, SN, SC, G, GL, X, XL, Q WHERE
    X is LocationAuthority,
    X label XL,
    F is FindingAid,
    F eid IN (%(e)s),
    F did D,
    D unitid FU,
    F service S,
    S code SN,
    S dpt_code SC,
    G index F,
    G authority X,
    G label GL,
    X quality Q {restrict}
    ) UNION (
    Any FU, SN, SC, G, GL, X, XL, Q WHERE
    X is LocationAuthority,
    X label XL,
    F is FindingAid,
    F eid IN (%(e)s),
    FA finding_aid F,
    F did D,
    D unitid FU,
    F service S,
    S code SN,
    S dpt_code SC,
    G index FA,
    G authority X,
    G label GL,
    X quality Q {restrict}
    )
    """
    record_type = LocationRecord
    cw_etype = "LocationAuthority"
    source = ""

    def compte_location_query(self, force=False):
        """Fetch location(s) related to the given FindingAid entities.

        :param boolean force: if True: recalculate existing alignements, if False: ignore them

        :returns: url
        :rtype: string
        """
        raise NotImplementedError

    def fetch_locations(self, findingaid_eids, simplified=False, force=False):
        """Fetch location(s) related to the given FindingAid entities.

        :param list findingaid_eids: FindingAid entity IDs
        :param boolean simplified: use simplified header for genarated csv file
        :param boolean force: if True: recalculate existing alignements, if False: ignore them

        :returns: location(s)
        :rtype: list
        """
        base_url = self.cnx.vreg.config.get("consultation-base-url")
        locations = []
        location_query = self.compte_location_query(force=force)
        resultset = self.cnx.execute(
            location_query % {"e": ",".join('"%s"' % eid for eid in findingaid_eids)}
        )
        for row in resultset:
            geogname_eid, geogname_label = str(row[3]), row[4]
            auth_eid, auth_label = str(row[5]), row[6]
            unitid, code, dpt = row[:3]
            quality = "yes" if row[7] else "no"
            location = (
                auth_eid,
                urljoin(base_url, "geogname/{eid}".format(eid=geogname_eid)),
                geogname_label,
                urljoin(base_url, "location/{eid}".format(eid=auth_eid)),
                auth_label,
                unitid,
                code,
                dpt,
                quality,
            )
            locations.append(location)
        return locations

    def compute_findingaid_alignments(self, findingaids, simplified=False):
        """Compute FindingAid alignments.

        :param list findingaids: list of imported FindingAids
        :param bool simplified: toggle simplified CSV file format on/off
        """
        raise NotImplementedError

    def compute_existing_alignment(self):
        """Fetch existing alignment(s) from database.

        :returns: exiting alignment(s)
        :rtype: set
        """
        alignment_query = f"""DISTINCT Any X, E
        WHERE X is LocationAuthority, X same_as EX,
        EX {'extid' if self.source == 'bano' else 'uri'} E, EX source '{self.source}'
        """
        return {(str(auth), attr) for auth, attr in self.cnx.execute(alignment_query)}

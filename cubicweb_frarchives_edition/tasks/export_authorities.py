# -*- coding: utf-8 -*-
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

# standard library imports
import logging
import os.path
import datetime

# third party imports
import rq
from urllib.parse import urljoin

# CubicWeb specific imports

# library specific imports
from cubicweb_frarchives_edition.rq import rqjob, update_progress
from cubicweb_frarchives_edition.alignments.bano_align import BanoRecord
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord
from cubicweb_frarchives_edition.tasks.utils import write_csv, zip_files, serve_zip


class AuthorityExporter:
    """Authority exporting base class.

    :ivar Connection cnx: CubicWeb database connection
    :ivar str consultation_url: consultation instance base URL
    :ivar Logger log: Rq task logger
    """

    NONALIGNED_QUERY = """
    DISTINCT Any I, IL, A, AL, NULL, NULL, NULL, NULL, IT ORDERBY IT, AL, A
    WITH I, IL, A, AL, IT BEING (
        (DISTINCT Any {var} A, AL, IT WHERE F is FindingAid, F service %(eid)s, I is {index},
        I label IL, I index F, I authority A, A label AL, NOT EXISTS (A same_as E,
        {condition}), I type IT)
        UNION
        (DISTINCT Any {var} A, AL, IT WHERE F is FindingAid, F service %(eid)s, C is FAComponent,
        C finding_aid F, I is {index}, I index C, I label IL, I authority A, A label AL,
        NOT EXISTS (A same_as E, {condition}), I type IT)
    )"""

    def __init__(self, cnx, simplified):
        """Initialize authority exporting.

        :param Connection cnx: CubicWeb database connection
        :param bool simplified: toggle simplified CSV file format on/off
        """
        self.cnx = cnx
        self.simplified = simplified
        self.consultation_url = self.cnx.vreg.config.get("consultation-base-url")
        self.log = logging.getLogger("rq.task")

    def create_csv(self, service, rows, headers, source=""):
        """Create CSV containing exported authorities.

        :param Service service: service
        :param tuple headers: column headers
        :param list rows: rows
        :param str source: target or empty string if nonaligned

        :returns: filename, arcname
        :rtype: tuple
        """
        filename = write_csv(rows, headers=headers, delimiter="\t")
        arcname = "{service}-{date}.csv".format(
            service=service.code,
            date=datetime.datetime.now().strftime("%Y%m%d")
        ).lower()
        arcname = os.path.join(source if source else "nonaligned", arcname)
        return filename, arcname

    def format_rows(self, rset):
        """Format rows.

        :param ResultSet rset: result set

        :returns: formatted row
        :rtype: list
        """
        raise NotImplementedError

    def export_aligned(self, service):
        """Export aligned authorities.

        :param Service service: service
        """
        raise NotImplementedError

    def export_nonaligned(self, service):
        """Export nonaligned authorities.

        :param Service service: service
        """
        raise NotImplementedError


class LocationAuthorityExporter(AuthorityExporter):
    """LocationAuthority exporting."""

    def __init__(self, cnx, simplified):
        """Intialize LocationAuthority exporting.

        :param Connection cnx: CubicWeb database connection
        :param bool simplified: toggle simplified CSV file format on/off
        """
        super().__init__(cnx, simplified)
        self.pretty_sources = {"geoname": "GeoNames", "bano": "BANO"}
        self.headers = {
            "geoname": list(GeonameRecord.headers.keys())
            if not self.simplified
            else list(GeonameRecord.simplified_headers.keys()),
            "bano": list(BanoRecord.headers.keys())
            if not self.simplified
            else list(BanoRecord.simplified_headers.keys()),
        }

    def format_rows(self, rset):
        """Format rows.

        :param ResultSet rset: result set

        :returns: formatted row
        :rtype: list
        """
        for row in rset:
            (
                geogname_eid,
                geogname_label,
                authority_eid,
                authority_label,
                longitude,
                latitude,
                external_id,
                external_label,
                _,
            ) = row
            authority_uri = urljoin(self.consultation_url, "location/{}".format(authority_eid))
            row = [
                authority_eid,
                authority_uri,
                authority_label,
                external_id,
                external_label,
                longitude,
                latitude,
                "yes",
                "",
            ]
            if not self.simplified:
                geogname_uri = urljoin(self.consultation_url, "geogname/{}".format(geogname_eid))
                row.insert(1, geogname_uri)
                row.insert(2, geogname_label)
            yield (row)

    def export_nonaligned(self, service):
        """Export nonaligned authorities. Nonaligned authorities
        are exported in GeoNames format to allow for re-importing
        manually added alignments.

        :param Service service: service

        :returns: filename, arcname
        :rtype: tuple
        """
        self.log.info("export nonaligned")
        var = "NULL, NULL," if self.simplified else "I, IL,"
        condition = "E source IN ('geoname', 'bano')"
        rset = self.cnx.execute(
            self.NONALIGNED_QUERY.format(var=var, index="Geogname", condition=condition),
            {"eid": service.eid},
        )
        if not rset:
            self.log.info("0 nonaligned LocationAuthorities")
            return "", ""
        return self.create_csv(service, list(self.format_rows(rset)), self.headers["geoname"])

    def export_aligned(self, service, source):
        """Export authorities aligned to given target.

        :param str source: target

        :returns: filename, arcname
        :rtype: tuple
        """
        self.log.info("export aligned to {}".format(self.pretty_sources[source]))
        external = "E is {}, E source %(source)s, E {} EXID,".format(
            *(("ExternalUri", "uri") if source == "geoname" else ("ExternalId", "extid"))
        )
        var = "NULL, NULL," if self.simplified else "I, IL,"
        aligned_query = """DISTINCT Any I, IL, A, AL, LONG, LAT, EXID, EL, NULL ORDERBY AL, A
        WITH I, IL, A, AL, LONG, LAT, EXID, EL BEING (
        (DISTINCT Any {var} A, AL, LONG, LAT, EXID, EL WHERE
        F is FindingAid, F service %(eid)s, I is Geogname, I label IL,
        I index F, I authority A, A label AL, A longitude LONG, A latitude LAT,
        A same_as E, {external} E label EL)
        UNION
        (DISTINCT Any {var} A, AL, LONG, LAT, EXID, EL WHERE
        F is FindingAid, F service %(eid)s, C is FAComponent, C finding_aid F,
        I is Geogname, I label IL, I index C, I authority A, A label AL,
        A longitude LONG, A latitude LAT, A same_as E, {external} E label EL)
        )""".format(
            var=var, external=external
        )
        rset = self.cnx.execute(aligned_query, {"eid": service.eid, "source": source})
        if not rset:
            self.log.info("%s : found 0 aligned LocationAuthorities", self.pretty_sources[source])
            return "", ""
        return self.create_csv(
            service, list(self.format_rows(rset)), self.headers[source], source=source
        )


class AgentAuthorityExporter(AuthorityExporter):
    """AgentAuthority exporting."""

    def __init__(self, cnx, simplified):
        """Initialize AgentAuthority exporting.

        :param Connection cnx: CubicWeb database connection
        :param bool simplified: toggle simplified CSV file format on/off
        """
        super().__init__(cnx, simplified)
        self.pretty_sources = {"databnf": "data.bnf.fr", "wikidata": "Wikidata"}

    @property
    def nonaligned_headers(self):
        headers = [
            "identifiant_AgentAuthority",
            "URI_AgentAuthority",
            "libelle_AgentAuthority",
            "type_AgentName",
            "keep",
        ]
        if self.simplified:
            return headers
        return [headers[0], "URI_AgentName", "libelle_AgentName", *headers[1:]]

    @property
    def headers(self):
        headers = self.nonaligned_headers
        databnf_headers = headers[:-1] + ["URI_databnf", "libelle_databnf", headers[-1]]
        wikidata_headers = headers[:-1] + ["URI_wikidata", "libelle_wikidata", headers[-1]]
        return {"databnf": databnf_headers, "wikidata": wikidata_headers}

    def format_rows(self, rset):
        """Format rows.

        :param ResultSet rset: result set

        :returns: formatted row
        :rtype: list
        """
        for row in rset:
            (
                agentname_eid,
                agentname_label,
                authority_eid,
                authority_label,
                _,
                _,
                external_id,
                external_label,
                agentname_type,
            ) = row
            authority_uri = urljoin(self.consultation_url, "agent/{}".format(authority_eid))
            row = [authority_eid, authority_uri, authority_label, agentname_type, "yes"]
            if all((external_id, external_label)):
                row.insert(4, external_id)
                row.insert(5, external_label)
            if not self.simplified:
                agentname_uri = urljoin(self.consultation_url, "agentname/{}".format(agentname_eid))
                row.insert(1, agentname_uri)
                row.insert(2, agentname_label)
            yield (row)

    def export_nonaligned(self, service):
        """Export nonaligned authorities.

        :param Service service: service

        :returns: filename, arcname
        :rtype: tuple
        """
        self.log.info("export nonaligned")
        var = "NULL, NULL," if self.simplified else "I, IL,"
        condition = "E source IN ('databnf', 'wikidata')"
        rset = self.cnx.execute(
            self.NONALIGNED_QUERY.format(var=var, index="AgentName", condition=condition),
            {"eid": service.eid},
        )
        if not rset:
            self.log.info("0 nonaligned AgentAuthorities")
            return "", ""
        return self.create_csv(service, list(self.format_rows(rset)), self.nonaligned_headers)

    def export_aligned(self, service, source):
        """Export authorities aligned to given target.

        :param Service service: service
        :param str source: target

        :returns: filename, arcname
        :rtype: tuple
        """
        self.log.info("export aligned to {}".format(self.pretty_sources[source]))
        var = "NULL, NULL," if self.simplified else "I, IL,"
        aligned_query = """DISTINCT Any I, IL, A, AL, NULL, NULL, EXID, EL, IT ORDERBY AL, A
        WITH I, IL, A, AL, EXID, EL, IT BEING (
        (DISTINCT Any {var} A, AL, EXID, EL, IT WHERE
        F is FindingAid, F service %(eid)s, I is AgentName, I label IL, I type IT, I index F,
        I authority A, A label AL, A same_as E, E is ExternalUri, E source %(source)s, E uri EXID,
        E label EL)
        UNION
        (DISTINCT Any {var} A, AL, EXID, EL, IT WHERE
        F is FindingAid, F service %(eid)s, C is FAComponent, C finding_aid F, I is AgentName,
        I label IL, I type IT, I index C, I authority A, A label AL, A same_as E, E is ExternalUri,
        E source %(source)s, E uri EXID, E label EL)
        )""".format(
            var=var
        )
        rset = self.cnx.execute(aligned_query, {"eid": service.eid, "source": source})
        if not rset:
            self.log.info("%s : found 0 aligned AgentAuthorities", self.pretty_sources[source])
            return "", ""
        return self.create_csv(
            service, list(self.format_rows(rset)), self.headers[source], source=source
        )


class SubjectAuthorityExporter(AuthorityExporter):
    """SubjectAuthority exporting."""

    @property
    def nonaligned_headers(self):
        headers = [
            "identifiant_SubjectAuthority",
            "URI_SubjectAuthority",
            "libelle_SubjectAuthority",
            "type_Subject",
            "keep",
        ]
        if self.simplified:
            return headers
        return [headers[0], "URI_Subject", "libelle_Subject", *headers[1:]]

    @property
    def headers(self):
        headers = self.nonaligned_headers
        return headers[:-1] + ["URI_thesaurus", "libelle_thesaurus", headers[-1]]

    def format_rows(self, rset):
        """Format rows.

        :param ResultSet rset: result set

        :returns: formatted row
        :rtype: list
        """
        for row in rset:
            (
                subject_eid,
                subject_label,
                authority_eid,
                authority_label,
                _,
                _,
                concept_eid,
                concept_cwuri,
                subject_type,
            ) = row
            authority_uri = urljoin(self.consultation_url, "subject/{}".format(authority_eid))
            row = [authority_eid, authority_uri, authority_label, subject_type, "yes"]
            if all((concept_eid, concept_cwuri)):
                row.insert(4, concept_cwuri)
                row.insert(5, self.cnx.entity_from_eid(int(concept_eid)).dc_title())
            if not self.simplified:
                subject_uri = urljoin(self.consultation_url, "subjectname/{}".format(subject_eid))
                row.insert(1, subject_uri)
                row.insert(2, subject_label)
            yield (row)

    def export_nonaligned(self, service):
        """Export nonaligned authorities.

        :param Service service: service

        :returns: filename, arcname
        :rtype: tuple
        """
        self.log.info("export nonaligned")
        var = "NULL, NULL," if self.simplified else "I, IL,"
        condition = "E is Concept"
        rset = self.cnx.execute(
            self.NONALIGNED_QUERY.format(var=var, index="Subject", condition=condition),
            {"eid": service.eid},
        )
        if not rset:
            self.log.info("0 nonaligned SubjectAuthorities")
            return "", ""
        return self.create_csv(service, list(self.format_rows(rset)), self.nonaligned_headers)

    def export_aligned(self, service):
        """Export authorities aligned to thesaurus.

        :param Service service: service
        """
        self.log.info("export aligned to thesaurus")
        var = "NULL, NULL," if self.simplified else "I, IL,"
        aligned_query = """DISTINCT Any I, IL, A, AL, NULL, NULL, C, CC, IT ORDERBY AL, A
        WITH I, IL, A, AL, C, CC, IT BEING(
        (DISTINCT Any {var} A, AL, C, CC, IT WHERE
        F is FindingAid, F service %(eid)s, I is Subject, I label IL, I type IT, I index F,
        I authority A, A label AL, A same_as C, C is Concept, C cwuri CC)
        UNION
        (DISTINCT Any {var} A, AL, C, CC, IT WHERE
        F is FindingAid, F service %(eid)s, FC is FAComponent, FC finding_aid F, I is Subject,
        I label IL, I type IT, I index FC, I authority A, A label AL, A same_as C,
        C is Concept, C cwuri CC)
        )""".format(
            var=var
        )
        rset = self.cnx.execute(aligned_query, {"eid": service.eid})
        if not rset:
            self.log.info("found 0 aligned SubjectAuthorities")
            return "", ""
        return self.create_csv(
            service, list(self.format_rows(rset)), self.headers, source="thesaurus"
        )


@rqjob
def export_authorities(
    cnx, services, authority_type, aligned=True, nonaligned=False, simplified=False
):
    """Export LocationAuthorities.

    :param Connection cnx: CubicWeb database connection
    :param list services: list of services
    :param bool aligned: toggle exporting aligned LocationAuthorities on/off
    :param bool nonaligned: toggle exporting nonaligned LocationAuthorities on/off
    :param bool simplified: toggle simplified CSV file format on/off
    """
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    if not services:
        services = cnx.execute("""Any X, C WHERE EXISTS (F service X), X code C""")
    else:
        services = cnx.execute(
            "Any X, C WHERE X is SERVICE, X code IN ({}), X code C".format(
                ",".join('"{}"'.format(service) for service in services)
            )
        )
    rowcount = services.rowcount
    services = [services.get_entity(i, 0) for i in range(rowcount)]
    progress_value = 1.0 / (rowcount + 1)
    filenames = []
    if authority_type == "location":
        exporter = LocationAuthorityExporter(cnx, simplified)
        sources = ("geoname", "bano")
    elif authority_type == "agent":
        exporter = AgentAuthorityExporter(cnx, simplified)
        sources = ("databnf", "wikidata")
    elif authority_type == "subject":
        exporter = SubjectAuthorityExporter(cnx, simplified)
        sources = ()
    for service in services:
        log.info("start exporting service %s", service.code)
        # export aligned
        if aligned:
            if sources:
                for source in sources:
                    filenames.append(exporter.export_aligned(service, source))
            else:
                filenames.append(exporter.export_aligned(service))
        # export nonaligned
        if nonaligned:
            filenames.append(exporter.export_nonaligned(service))
        progress = update_progress(job, progress + progress_value)
    filenames = [(filename, arcname) for (filename, arcname) in filenames if filename]
    if not filenames:
        log.info("archive would be empty, skipping creating archive")
    else:
        archive = zip_files(filenames)
        serve_zip(cnx, int(job.id), "authorities.zip", archive)
        # clean-up
        for filename, _ in filenames:
            os.remove(filename)
        os.remove(archive)

# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2022
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
import logging

from cubicweb_frarchives_edition.alignments import get_externaluri_data
from cubicweb_frarchives_edition.alignments.align import Record, ImportAligner


class ImportRecord(Record):
    headers = OrderedDict()
    simplified_headers = OrderedDict(
        header for i, header in enumerate(headers.items()) if i not in (1, 2, 5)
    )
    BOOLEAN_HEADERS = ("quality",)

    @property
    def sourceid(self):
        """return URI_External"""
        return self.externaluri


class AgentImportRecord(ImportRecord):
    headers = OrderedDict(
        [
            ("identifiant_AgentAuthority", "autheid"),
            ("URI_AgentName", "agentnameuri"),
            ("libelle_AgentName", "agentnamelabel"),
            ("URI_AgentAuthority", "authuri"),
            ("libelle_AgentAuthority", "pnialabel"),
            ("type_AgentName", "indextype"),
            ("URI_ExternalUri", "externaluri"),
            ("libelle_ExternalUri", "externallabel"),
            ("keep", "keep"),
            ("quality", "quality"),
        ]
    )

    REQUIRED_HEADERS_ALIGN = (
        "identifiant_AgentAuthority",
        "libelle_AgentAuthority",
        "URI_ExternalUri",
        "keep",
    )

    REQUIRED_HEADERS_LABELS = (
        "identifiant_AgentAuthority",
        "libelle_AgentAuthority",
    )

    REQUIRED_HEADERS_QUALITY = (
        "identifiant_AgentAuthority",
        "libelle_AgentAuthority",
        "quality",
    )


class SubjectImportRecord(ImportRecord):
    headers = OrderedDict(
        [
            ("identifiant_SubjectAuthority", "autheid"),
            ("URI_Subject", "agentnameuri"),
            ("libelle_Subject", "agentnamelabel"),
            ("URI_SubjectAuthority", "authuri"),
            ("libelle_SubjectAuthority", "pnialabel"),
            ("type_Subject", "indextype"),
            ("URI_ExternalUri", "externaluri"),
            ("libelle_ExternalUri", "externallabel"),
            ("keep", "keep"),
            ("quality", "quality"),
        ]
    )

    REQUIRED_HEADERS_ALIGN = (
        "identifiant_SubjectAuthority",
        "libelle_SubjectAuthority",
        "URI_ExternalUri",
        "keep",
    )

    REQUIRED_HEADERS_LABELS = (
        "identifiant_SubjectAuthority",
        "libelle_SubjectAuthority",
    )

    REQUIRED_HEADERS_QUALITY = (
        "identifiant_SubjectAuthority",
        "libelle_SubjectAuthority",
        "quality",
    )


class AgentSubjectImportAligner(ImportAligner):
    """Aligner base class.

    :cvan type record_type: AgentImportRecord subclass
    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    record_type = AgentImportRecord

    def find_conflicts(self, to_modify):
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
                            "'identifiant_Authority' %s are also tagged "
                            "to be removed. Skip them all."
                        ),
                        len(remove_conflicts),
                        autheid,
                    )
                # check for conflicts (new alignments)
                new_conflicts = False
                # more than one row per alignment
                rows = [key for key in new if len(new[key]) > 1]
                for key in rows:
                    labels = set(record.externallabel for record in new[key])
                    if len(labels) > 1:
                        log.warning(
                            (
                                "%d different 'libelle_ExternalUri' columns "
                                "found for combination of "
                                "'identifiant_Authority' %s "
                                "and 'URI_ExternalUri' %s. Skip them all."
                            ),
                            len(labels),
                            *key
                        )
                        new_conflicts = True
                if any((remove_conflicts, new_conflicts)):
                    conflicts.append(autheid)
        return conflicts

    def compute_existing_alignment(self):
        """Fetch existing alignment(s) from database.

        :returns: exiting alignment(s)
        :rtype: set
        """
        alignment_query = f"""DISTINCT Any X, E
        WHERE X is {self.cw_etype}, X same_as EX,
        EX uri E
        """
        return {(str(auth), exturi) for auth, exturi in self.cnx.execute(alignment_query)}

    def _get_eid_from_externaluri_uri(self, externuri):
        """
        Return the eid of an ExternalUri having the given externuri as uri.
        If the ExternalUri does not exist, return None.

        :param externuri: the uri
        """
        cursor = self.cnx.cnxset.cu
        cursor.execute("SELECT cw_eid FROM cw_externaluri WHERE cw_uri = %s LIMIT 1", (externuri,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def process_alignments(self, new_alignment, to_remove_alignment, override_alignments=False):
        """
        Add or remove alignements

        :param new_alignment dict: alignments to add
        :param to_remove_alignment dict: alignments to remove
        :param override_alignments bool: user action must or not be overridden
        """
        # first add new alignments
        failed = 0
        self.log.info("will create %s new alignments", len(new_alignment))
        if not (new_alignment or to_remove_alignment):
            return
        for (autheid, externuri), record in new_alignment.items():
            # check Authority exists
            authority = self.cnx.system_sql(
                f"""SELECT 1 FROM cw_{self.cw_etype}
                WHERE cw_eid=%(autheid)s""",
                {"autheid": autheid},
            ).fetchall()
            if not authority:
                self.log.error("%s %s doesn't exist", self.cw_etype, autheid)
                continue
            try:
                if not override_alignments:
                    is_in_sameas_history = self.cnx.system_sql(
                        """SELECT 1 FROM sameas_history
                        WHERE autheid=%(autheid)s AND sameas_uri=%(externuri)s
                        AND action=false""",
                        {"autheid": autheid, "externuri": externuri},
                    ).fetchall()
                    # user removed alignment, do not re-insert it
                    if is_in_sameas_history:
                        continue
                source, externeid = get_externaluri_data(externuri)
                ext = self._get_eid_from_externaluri_uri(externuri)
                if not ext:
                    # (possibly mod.) ExternalUri label in CSV takes precedence
                    # sameas-label hook will create it if ExternalUries label is not
                    # given
                    label = record.externallabel or ""
                    ext = self.cnx.create_entity(
                        "ExternalUri",
                        uri=externuri,
                        label=label,
                        extid=externeid,
                        source=source,
                    ).eid
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
                        X source '%(source)s'""",
                        {"autheid": autheid, "ext": ext, "source": source},
                    ).rows
                    to_remove_alignment.update(
                        {(autheid, externuri): tuple() for externuri, in result_set}
                    )
                    # update same-as relation history
                    query = """INSERT INTO sameas_history (sameas_uri, autheid, action)
                    VALUES (%(externuri)s, %(autheid)s, true)
                    ON CONFLICT (sameas_uri,autheid)
                    DO UPDATE SET action=true"""
                    self.cnx.system_sql(query, {"externuri": externuri, "autheid": autheid})
            except Exception as exception:
                self.log.error(exception)
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
        for autheid, externuri in to_remove_alignment:
            externeid = self._get_eid_from_externaluri_uri(externuri)
            if not externeid:
                continue
            try:
                query = """DELETE FROM same_as_relation
                WHERE eid_from=%(autheid)s AND eid_to=%(ext)s"""
                if not override_alignments:
                    query += """
                    AND
                    NOT EXISTS(SELECT 1 FROM sameas_history sh
                    WHERE sh.sameas_uri = %(externuri)s
                    AND sh.autheid = %(autheid)s
                    AND sh.action=true
                    )
                    """
                self.cnx.system_sql(
                    query,
                    {
                        "autheid": autheid,
                        "ext": externeid,
                        "externuri": externuri,
                    },
                )
                if override_alignments:
                    query = """INSERT INTO sameas_history (sameas_uri, autheid, action)
                    VALUES (%(externuri)s, %(autheid)s, false)
                    ON CONFLICT (sameas_uri,autheid)
                    DO UPDATE SET action=false"""
                    self.cnx.system_sql(query, {"autheid": autheid, "externuri": externuri})
            except Exception:
                self.log.error("error will trying to remove alignment")
                failed += 1
        if failed > 0:
            self.log.error(
                "failed to remove all deprecated alignments : %d/%d could not be removed",
                failed,
                len(to_remove_alignment),
            )
        try:
            self.cnx.commit()
        except Exception:
            self.log.error("failed to update database, all changes have been lost")


class AgentImportAligner(AgentSubjectImportAligner):
    """Aligner base class.

    :cvan type record_type: AgentImportRecord subclass
    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    record_type = AgentImportRecord
    cw_etype = "AgentAuthority"


class SubjectImportAligner(AgentSubjectImportAligner):
    """Aligner base class.

    :cvan type record_type: SubjectImportRecord subclass
    :ivar Connection cnx: CubicWeb database connection
    :ivar Logger log: logger
    """

    record_type = SubjectImportRecord
    cw_etype = "SubjectAuthority"

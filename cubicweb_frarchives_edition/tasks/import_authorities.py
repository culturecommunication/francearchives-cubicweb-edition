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
import csv
import logging
from collections import defaultdict
from psycopg2.extras import execute_batch

# third party imports
import rq

# CubicWeb specific imports

# library specific imports
from cubicweb_francearchives.storage import S3BfssStorageMixIn

from cubicweb_frarchives_edition.rq import rqjob, update_progress
from cubicweb_frarchives_edition.tasks.import_alignments import update_alignments
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord, GeonameAligner
from cubicweb_frarchives_edition.alignments.authorities_align import (
    AgentImportRecord,
    AgentImportAligner,
    SubjectImportRecord,
    SubjectImportAligner,
)


CW_ETYPES_ALIGNERS = {
    "LocationAuthority": {"record": GeonameRecord, "aligner": GeonameAligner},
    "AgentAuthority": {"record": AgentImportRecord, "aligner": AgentImportAligner},
    "SubjectAuthority": {"record": SubjectImportRecord, "aligner": SubjectImportAligner},
}


def update_authorities(
    cnx, log, csvpath, csvfilename, cw_etype, update_labels=True, update_quality=True
):
    """Update Authorities labels.

    :param Connection cnx: CubicWeb database connection
    :param Logger log: RqTask logger
    :param str csvpath: path to CSV file (can be a hash)
    :param str csvfilename: CSV file name
    :param str cw_etype: authorities cw_etype
    :param bool update_labels: toggle updating labels on/off
    :param bool update_quality: toggle updating quality on/off

    """
    if update_labels:
        log.info(f"update labels from {csvfilename}")
    if update_quality:
        log.info(f"update quality from {csvfilename}")

    user_defined_labels = defaultdict(set)
    user_defined_qualities = defaultdict(set)
    st = S3BfssStorageMixIn(log=log)
    record_cls = CW_ETYPES_ALIGNERS[cw_etype]["record"]
    with st.storage_read_file(csvpath) as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        try:
            record_cls.validate_csv(
                reader.fieldnames, align=False, labels=update_labels, quality=update_quality
            )
        except ValueError as exception:
            raise exception

        labels, qualities = {}, {}
        for eid, label, quality in cnx.execute(
            f"""Any A, L, Q WHERE A is {cw_etype}, A label L, A quality Q"""
        ):
            labels[str(eid)] = label
            qualities[str(eid)] = quality
        if update_quality and "quality" not in reader.fieldnames:
            raise (ValueError("column 'quality' is missing for the csv file"))
        invalid = []
        for i, row in enumerate(reader, 1):
            try:
                record = record_cls(row, align=False, labels=update_labels, quality=update_quality)
            except ValueError as exception:
                invalid.append("{} {}".format(i, exception))
                continue
            label = labels.get(record.autheid, "")
            if not label:
                # make certain that the Authority is not known
                # instead of empty label
                if record.autheid not in labels:
                    log.warning(
                        f"unknown {cw_etype} %s (row %d, column 1) (skip)", record.autheid, i
                    )
                    continue
            if record.pnialabel != label:
                if update_labels:
                    user_defined_labels[record.autheid].add(record.pnialabel)
            if update_quality:
                if not isinstance(record.quality, bool):
                    log.warning(
                        f"""quality ignored: found a wrong quality value '{record.quality}' for {cw_etype} {record.autheid} (row {i}, column 1)"""  # noqa
                    )
                    continue
                if record.quality != qualities[record.autheid]:
                    user_defined_qualities[record.autheid].add(record.quality)
        if invalid:
            log.warning("found missing value in required column(s): {}".format(";".join(invalid)))
    skipped, confirmed = process_data(log, user_defined_labels, "label", [])
    # update labels
    if update_labels:
        if confirmed:
            log.info(f"update {len(confirmed)} user-defined {cw_etype} label")
            execute_batch(
                cnx.cnxset.cu, f"UPDATE cw_{cw_etype} SET cw_label=%s WHERE cw_eid=%s", confirmed
            )
            cnx.commit()
            for label, eid in confirmed:
                cnx.entity_from_eid(eid).add_to_auth_history()
        else:
            log.info(f"No user-defined {cw_etype} labels found to be changed")

    # process and update quality
    if update_quality:
        if user_defined_qualities:
            _, confirmed = process_data(log, user_defined_qualities, "quality", skipped)
            if confirmed:
                log.info(f"update {len(confirmed)} user-defined {cw_etype} quality")
                cnx.cnxset.cu.executemany(
                    f"UPDATE cw_{cw_etype} SET cw_quality=%s WHERE cw_eid=%s", confirmed
                )
                cnx.commit()
            else:
                log.info(f"No user-defined {cw_etype} quality found to be changed")


def process_data(log, user_defined, column, to_skip):
    """process data for Authorities's update.

    :param Logger log: RqTask logger
    :param dict user_defined: autorities to update
    :param str column: Authority column to update
    :param list to_skip: autorities to skip
    """
    args = []
    skipped = []
    for autheid, values in list(user_defined.items()):
        if autheid in to_skip:
            continue
        if len(values) > 1:
            log.warning(
                f"authority %s : found %d conflicting user-defined {column} : %s. Skip all",
                autheid,
                len(values),
                " ; ".join('"{}"'.format(value) for value in values),
            )
            skipped.append(autheid)
            continue
        args.append((values.pop(), autheid))
    return skipped, args


@rqjob
def import_authorities(
    cnx, csvpath, csvfilename, cw_etype, labels=True, alignments=True, quality=True
):
    """Import authorities.

    :param Connection cnx: CubicWeb database connection
    :param str csvpath: path to CSV file
    :param str csvfilename: CSV file name
    :param str cw_etype: authorities cw_etype
    :param bool labels: toggle updating labels on/off
    :param bool alignments: toggle updating alignments on/off
    :param bool quality: toggle updating quality on/off
    """
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    if labels and alignments:
        progress_value = 0.5
    else:
        progress_value = 1
    log.info(f'Start processing "{csvfilename}" ({cw_etype})')
    if labels or quality:
        try:
            update_authorities(cnx, log, csvpath, csvfilename, cw_etype, labels, quality)
        except Exception as error:
            log.error(f"failed to update labels or quality: {error}")
        progress = update_progress(job, progress + progress_value)
    if alignments:
        log.info(f"update alignments from {csvfilename}")
        try:
            aligner_cls = CW_ETYPES_ALIGNERS[cw_etype]["aligner"]
            update_alignments(cnx, log, csvpath, aligner_cls, override_alignments=True)
        except Exception as error:
            log.error(f"failed to update alignments : {error}")
        progress = update_progress(job, progress + progress_value)
    # delete the temporary file
    S3BfssStorageMixIn(log=log).storage_delete_file(csvpath)

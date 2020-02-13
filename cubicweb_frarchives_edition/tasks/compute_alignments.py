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

from uuid import uuid4

# third party imports
import rq

# CubicWeb specific imports
# library specific imports
from cubicweb_frarchives_edition.rq import rqjob, update_progress
from cubicweb_frarchives_edition.tasks.utils import (
    serve_csv,
    serve_zip,
    write_binary_csv,
    write_csv,
    zip_files,
)
from cubicweb_frarchives_edition.tasks.import_alignments import auto_run_import
from cubicweb_frarchives_edition.alignments.utils import split_up
from cubicweb_frarchives_edition.alignments.bano_align import BanoAligner, BanoRecord
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameAligner, GeonameRecord

TARGETS = {
    "geoname": (GeonameAligner, GeonameRecord, "GeoNames"),
    "bano": (BanoAligner, BanoRecord, "BANO"),
}


def _get_findingaids(cnx, findingaids):
    """Get list of FindingAid entity IDs.

    :param list findingaids: list of either FindingAid entity IDs or stable IDs

    :returns: list of FindingAid entity IDs
    :rtype: list
    """
    eids = []
    for eid in findingaids:
        if type(eid) is int or eid.isdigit():
            rset = cnx.execute("Any T WHERE X is FindingAid, X eid %(eid)s, X is T", {"eid": eid})
        else:
            rset = cnx.execute(
                "Any X, T WHERE X is FindingAid, X stable_id %(eid)s, X is T", {"eid": eid}
            )
            eid = rset[0][0]
        eids.append(eid)
    return eids


def _get_temp_csv_output_file(cnx, rows, title):
    """Create temporary CSV output file.

    :param Connection cnx: CubicWeb database connection
    :param rows: rows
    :param str title: title

    :returns: output_file and filename
    :rtype: tuple
    """
    # create temporary file
    filename = write_csv(rows, delimiter="\t")
    # create output file (needed for automatically importing alignments)
    output_file = cnx.create_entity(
        "File",
        data=write_binary_csv(rows, delimiter="\t"),
        data_format="text/csv",
        data_name=title,
        title=title,
        uuid=str(uuid4().hex),
    )
    return output_file, filename


def update_rqtask(cnx, rows, target, auto_import=False, simplified=False, file_size=0):
    """Create output file and subtask(s).

    :param Connection cnx: CubicWeb database connection
    :param int eid: RqTask entity ID
    :param list rows: rows
    :param str target: target dataset
    :param bool auto_import: toggle automatically importing alignments on/off
    :param bool simplified: toggle simplified CSV file format on/off
    :param int file_size: file size (if 0 unlimited)
    """
    log = logging.getLogger("rq.task")
    aligner_cls, record_cls, dbname = TARGETS[target]
    job = rq.get_current_job()
    eid = int(job.id)
    rqtask = cnx.entity_from_eid(eid)
    # BANO alignments are always imported automatically
    auto_import = auto_import or target == "bano"
    if auto_import:
        log.info("automatically import %s alignments", dbname)
    # add headers and sort by LocationAuthority label and entity ID
    if simplified:
        headers = list(record_cls.simplified_headers.keys())
        rows.sort(key=lambda x: (x[2], x[0]))
    else:
        headers = list(record_cls.headers.keys())
        rows.sort(key=lambda x: (x[4], x[0]))
    # if unlimited file size or number of rows less than file size
    # output file is CSV file
    if not file_size or len(rows) < file_size:
        rows.insert(0, headers)
        output_file = serve_csv(
            cnx, eid, "alignment-{target}.csv".format(target=target), rows, delimiter="\t"
        )
        if auto_import:
            rqtask.cw_set(subtasks=auto_run_import(cnx, rqtask, aligner_cls, output_file))
    # if limited file size and number of rows is greater than file size
    # output file is Zip archive
    else:
        temp_files = []
        for i, chunk in enumerate(split_up(rows, file_size - 1), 1):
            title = "alignment-{target}-{i}.csv".format(target=target, i=str(i).zfill(2))
            chunk.insert(0, headers)
            temp_csv, file_name = _get_temp_csv_output_file(cnx, chunk, title)
            temp_files.append((temp_csv, file_name, title))
        serve_zip(
            cnx,
            eid,
            "{target}.zip".format(target=target),
            zip_files([(t_file, t_title) for _, t_file, t_title in temp_files]),
        )
        if auto_import:
            rqtask.cw_set(
                subtasks=[
                    auto_run_import(cnx, rqtask, aligner_cls, t_csv) for t_csv, _, _ in temp_files
                ]
            )
    cnx.commit()


def compute_alignment_target(cnx, findingaids, target, simplified=False):
    """Compute alignment to target dataset.

    :param Connection cnx: CubicWeb database connection
    :param list findingaids: list of FindingAids to align
    :param str target: target dataset
    :param bool simplified: toggle simplified CSV file format on/off
    """
    # do not spam interface
    log = logging.getLogger()
    aligner_cls, record_cls, dbname = TARGETS[target]
    aligner = aligner_cls(cnx, log)
    rows = list(aligner.compute_findingaid_alignments(findingaids, simplified=simplified))
    return rows


@rqjob
def compute_alignments(cnx, findingaids, auto_import=False, targets=("geoname", "bano")):
    """Compute alignments to target datasets.

    :param Connection cnx: CubicWeb database connection
    :param list findingaids: list of FindingAids to align
    :param bool auto_import: toggle automatically importing alignments on/off
    :param tuple targets: target datasets
    """
    log = logging.getLogger("rq.task")
    findingaids = _get_findingaids(cnx, findingaids)
    if not findingaids:
        log.warning("no FindingAids found")
        return
    log.info("found %d FindingAids", len(findingaids))
    for target in targets:
        _, _, dbname = TARGETS[target]
        try:
            log.info("align to %s", dbname)
            rows = compute_alignment_target(cnx, findingaids, target)
            if rows:
                log.info("found %d alignments to %s", len(rows), dbname)
                update_rqtask(cnx, rows, target, auto_import=auto_import)
            else:
                log.info("no alignments to %s found", dbname)
        except Exception as exception:
            log.error("failed to align to %s (%s)", dbname, exception)
            continue


@rqjob
def compute_alignments_all(cnx, simplified=False, targets=("geoname", "bano"), file_size=0):
    """Compute alignments to target datasets (entire database).

    :param Connection cnx: CubicWeb database connection
    :param bool simplified: toggle simplified CSV file format on/off
    :param tuple targets: target datasets
    :param int file_size: file size (if 0 unlimited)
    """
    log = logging.getLogger("rq.task")
    services = [
        eid for eid, in cnx.execute("Any X WHERE EXISTS (F service X, F is FindingAid)").rows
    ]
    if not services:
        log.warning("no FindingAids found")
        return
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    progress_value = 1.0 / (len(targets) * len(services))
    # 1/ fetch FindingAids
    for target in targets:
        _, _, dbname = TARGETS[target]
        try:
            rows = []
            num_services = len(services)
            for i, eid in enumerate(services, 1):
                log.info("aligning %d/%d FindingAid batches to %s", i, num_services, dbname)
                findingaids = [
                    eid
                    for eid, in cnx.execute(
                        "Any X WHERE X is FindingAid, X service %(eid)s", {"eid": eid}
                    )
                ]
                rows += compute_alignment_target(cnx, findingaids, target, simplified=simplified)
                progress = update_progress(job, progress + progress_value)
            if rows:
                rows = list(set(rows))
                log.info("found %d alignments to %s", len(rows), dbname)
                update_rqtask(cnx, rows, target, simplified=simplified, file_size=file_size)
            else:
                log.info("no alignments to %s found", dbname)
        except rq.timeouts.JobTimeoutException as exception:
            log.error(
                "failed to align database (%s) while trying to align to %s", exception, dbname
            )
            return
        except Exception as exception:
            log.error("failed to align to %s (%s)", dbname, exception)
            continue

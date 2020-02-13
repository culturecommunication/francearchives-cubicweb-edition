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

# third party imports
import rq

# CubicWeb specific imports

# library specific imports
from cubicweb_frarchives_edition.rq import rqjob, update_progress
from cubicweb_frarchives_edition.tasks.import_alignments import update_alignments
from cubicweb_frarchives_edition.alignments.geonames_align import GeonameRecord, GeonameAligner


def update_labels(cnx, log, csvpath):
    """Update LocationAuthorities' labels.

    :param Connection cnx: CubicWeb database connection
    :param Logger log: RqTask logger
    :param str csvpath: path to CSV file
    """
    labels = {
        str(eid): label
        for (eid, label) in cnx.execute("Any A, L WHERE A is LocationAuthority, A label L").rows
    }
    user_defined_labels = defaultdict(set)
    with open(csvpath) as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        try:
            GeonameRecord.validate_csv(reader.fieldnames)
        except ValueError as exception:
            raise exception
        invalid = []
        for i, row in enumerate(reader, 1):
            try:
                record = GeonameRecord(row)
            except ValueError as exception:
                invalid.append("{} {}".format(i, exception))
                continue
            label = labels.get(record.autheid, "")
            if not label:
                # make certain that LocationAuthority is not known
                # instead of empty label
                if record.autheid not in labels:
                    log.warning(
                        "unknown LocationAuthority %s (row %d, column 1) (skip)", record.autheid, i
                    )
                    continue
            if record.pnialabel != label:
                user_defined_labels[record.autheid].add(record.pnialabel)
        if invalid:
            log.warning("found missing value in required column(s): {}".format(";".join(invalid)))
    args = []
    for autheid, labels in list(user_defined_labels.items()):
        if len(labels) > 1:
            log.warning(
                "%d conflicting user-defined labels LocationAuthority %s (skip all)",
                len(labels),
                autheid,
            )
            continue
        args.append((labels.pop(), autheid))
    log.info("update %d user-defined LocationAuthority label(s)", len(args))
    cursor = cnx.cnxset.cu
    cursor.executemany("UPDATE cw_locationauthority SET cw_label=%s WHERE cw_eid=%s", args)
    cnx.commit()


@rqjob
def import_authorities(cnx, csvpath, labels=True, alignments=True):
    """Import LocationAuthorities.

    :param Connection cnx: CubicWeb database connection
    :param str csvpath: path to CSV file
    :param bool labels: toggle updating labels on/off
    :param bool alignments: toggle updating alignments on/off
    """
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    if labels and alignments:
        progress_value = 0.5
    else:
        progress_value = 1
    if labels:
        log.info("update LocationAuthority labels")
        try:
            update_labels(cnx, log, csvpath)
        except Exception:
            log.error("failed to update LocationAuthority labels")
        progress = update_progress(job, progress + progress_value)
    if alignments:
        log.info("update GeoNames alignments")
        try:
            update_alignments(cnx, log, csvpath, GeonameAligner, override_alignments=True)
        except Exception:
            log.error("failed to update GeoNames alignments")
        progress = update_progress(job, progress + progress_value)

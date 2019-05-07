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
from __future__ import print_function
import csv
from datetime import datetime

import logging

import rq
from six import text_type as unicode
from uuid import uuid4

from cubicweb import Binary
from cubicweb_frarchives_edition import CANDIDATE_SEP

from cubicweb_frarchives_edition.rq import update_progress, rqjob
from cubicweb_frarchives_edition.alignments.group_geonamed_locationauthorities import (
    compute_location_authorities_to_group as compute_candidates,
    update_suggest_es)

LOGGER = logging.getLogger(__name__)
NOW = datetime.now()


def location_authorities_to_group(cnx, log=None):
    """Compute location authorities candidates to group based on they geonameid

    """
    if log is None:
        log = LOGGER
    job = rq.get_current_job()
    rqtask = cnx.entity_from_eid(int(job.id))
    do_group, do_not_group = compute_candidates(cnx, log)
    if not do_group:
        return
    write_and_save_candidates(cnx, do_group, rqtask)


def write_and_save_csv(cnx, candidates, rqtask):
    """
    save resulting csv with:
    - first column: authorities to group with
    - other columns: all authorities to group
    each column contains authority's label and url separated by '###' (CANDIDATE_SEP)

    """
    b = Binary()
    writer = csv.writer(b, delimiter='\t')
    for label_to, other_labels in candidates.items():
        writer.writerow([label_to.candidate_info] + [
            l.candidate_info for l in other_labels])
    uuid = unicode(uuid4().hex)
    filename = u'location_authorities_to_group_{}{:02d}{:02d}_{}.csv'.format(
        NOW.year, NOW.day, NOW.month, uuid)
    return add_file_to_rtqsk(cnx, rqtask, b, filename, uuid)


def add_file_to_rtqsk(cnx, rqtask, data, filename, uuid):
    cwfile = cnx.create_entity(
        'File',
        data=data,
        data_format='text/csv',
        data_name=filename,
        title=filename,
        uuid=uuid,
    )
    if rqtask is not None:
        rqtask.cw_set(output_file=cwfile.eid)
    cnx.commit()
    return cwfile


def write_and_save_candidates(cnx, candidates, rqtask):
    """
    save resulting csv with:
    - first column: authorities to group with
    - other columns: all authorities to group
    each column may contain:
     - the authority label and url separated by '###' (CANDIDATE_SEP)
     - the authority url
     - the authority eid
    """
    b = Binary()
    writer = csv.writer(b, delimiter='\t')
    for label_to, other_labels in candidates.items():
        writer.writerow([label_to.candidate_info] + [
            l.candidate_info for l in other_labels])
    uuid = unicode(uuid4().hex)
    filename = u'location_authorities_to_group_{}{:02d}{:02d}_{}.csv'.format(
        NOW.year, NOW.day, NOW.month, uuid)
    return add_file_to_rtqsk(cnx, rqtask, b, filename, uuid)


def get_locationautorithy(cnx, column):
    """each column may contain :
       - authority's label and url separated by '###' (CANDIDATE_SEP)
       - the authority's url
       - authority's eid

    """
    if CANDIDATE_SEP in column:
        label, uri = column.split(CANDIDATE_SEP)
    else:
        uri = column
    eid = uri.split('/')[-1].strip()
    try:
        return cnx.entity_from_eid(eid)
    except Exception:
        return None


def get_log_info(cnx, column):
    if CANDIDATE_SEP in column:
        label, url = column.split(CANDIDATE_SEP)
        return '{} : {}'.format(label, url)
    return column


def group_location_authorities_candidates(cnx, csvpath, log=None):
    if log is None:
        log = LOGGER
    job = rq.get_current_job()
    rqtask = cnx.entity_from_eid(int(job.id))
    failed = []
    current_progress = update_progress(job, 0.)
    with cnx.allow_all_hooks_but('reindex-suggest-es',):
        with open(csvpath, "r") as f:
            reader = list(csv.reader(f, delimiter='\t'))
            progress_step = 1. / (len(reader) + 1)
            for idx, row in enumerate(reader, 1):
                log.info('processing row %s', idx)
                if not row:
                    log.info('skip an empty row %s', idx)
                    continue
                # remove empty columns
                row = [col for col in row if col.strip()]
                if len(row) < 2:
                    log.warning('skip the row %s: this row only contains one column'
                                ' (make sure colums are separated by a tabulation)', idx)
                    failed.append(row)
                    # there is no authority sources to group this the target
                    continue
                entities = [get_locationautorithy(cnx, col) for col in row]
                if not all(entities):
                    log.warning('skip the row %s: one of columns '
                                'contains an invalid authority url or eid', idx)
                    failed.append(row)
                    continue
                log_info = [get_log_info(cnx, r) for r in row]
                log.info('target: {}, sources: {}'.format(
                    log_info[0], '; '.join(log_info[1:])))
                target = entities[0]
                sources = entities[1:]
                for eidx, e in enumerate(sources, 1):
                    grouped = e.grouped_with
                    if grouped and grouped[0].eid == target.eid:
                        log.info('%s is already grouped with %s', row[eidx], row[0])
                try:
                    target.group([s.eid for s in sources])
                    cnx.commit()
                except Exception as ex:
                    log.exception('could not group authorities of row %s: %s', idx, ex)
                    failed.append(row)
                    continue
                update_suggest_es(cnx, entities)
                current_progress = update_progress(job, current_progress + progress_step)
    if failed:
        log.warning('%s records could not be grouped', len(failed))
        write_and_save_failed_candidates(cnx, failed, rqtask)


def write_and_save_failed_candidates(cnx, failed, rqtask):
    """
    save resulting csv with:
    - first column: authorities to group with
    - other columns: all authorities to group
    """
    b = Binary()
    writer = csv.writer(b, delimiter='\t')
    writer.writerows(failed)

    uuid = unicode(uuid4().hex)
    filename = u'failed_authorities_to_group_{}{:02d}{:02d}_{}.csv'.format(
        NOW.year, NOW.day, NOW.month, uuid)
    return add_file_to_rtqsk(cnx, rqtask, b, filename, uuid)


@rqjob
def compute_location_authorities_to_group(cnx):
    """Compute location authorities candidates to group based on they geonameid

    """
    log = logging.getLogger('rq.task')
    log.info('start the task')
    location_authorities_to_group(cnx, log=log)


@rqjob
def group_location_authorities(cnx, csvpath):
    """Group location authorities candidates to group based on they geonameid

    """
    log = logging.getLogger('rq.task')
    log.info('start the task')
    group_location_authorities_candidates(cnx, csvpath, log)

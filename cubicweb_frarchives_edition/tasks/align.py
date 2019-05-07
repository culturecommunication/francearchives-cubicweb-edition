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
import logging
import tempfile
import os

import rq
from uuid import uuid4
import csv
from six import text_type as unicode

from cubicweb import Binary

from cubicweb_frarchives_edition.rq import rqjob
from cubicweb_frarchives_edition.alignments.geonames_align import (
    GeonameAligner, Record)


@rqjob
def compute_alignment(cnx, findingaid_eid, auto_import=False):
    """build csv containing alignment of LocationAuthority and Geoname.
    """
    log = logging.getLogger('rq.task')
    if type(findingaid_eid) is int or findingaid_eid.isdigit():
        rset = cnx.execute(
            'Any T WHERE X is FindingAid, X eid %(e)s, X is T',
            {'e': findingaid_eid}
        )
    else:
        rset = cnx.execute(
            'Any X, T WHERE X is FindingAid, X stable_id %(e)s, X is T',
            {'e': findingaid_eid}
        )
        findingaid_eid = rset[0][0]
    if not rset:
        log.warning('compute_alignment: no task/findingaid with this eid "%s"',
                    findingaid_eid)
        return
    job = rq.get_current_job()
    rqtask = cnx.entity_from_eid(int(job.id))
    aligner = GeonameAligner(cnx, log)
    lines = aligner.compute_findingaid_alignment(findingaid_eid)
    if lines:
        cwfile = write_and_save_csv(cnx, lines, rqtask)
        log.info('compute_alignment: prepare to launch auto_run_import')
        auto_run_import(cnx, auto_import, rqtask, cwfile)
    else:
        log.info('compute_alignment: no geo alignements found for %s', findingaid_eid)


def write_and_save_csv(cnx, lines, rqtask):
    # save resulting csv
    b = Binary()
    writer = csv.writer(b, delimiter='\t')
    writer.writerow(Record.headers.keys())
    writer.writerows(lines)
    uuid = unicode(uuid4().hex)
    cwfile = cnx.create_entity(
        'File',
        data=b,
        data_format='text/csv',
        data_name='alignment-%s.csv' % uuid,
        title='alignment-%s.csv' % uuid,
        uuid=uuid,
    )
    if rqtask is not None:
        rqtask.cw_set(output_file=cwfile.eid)
    cnx.commit()
    return cwfile


def auto_run_import(cnx, auto_import, rqtask, cwfile):
    log = logging.getLogger('rq.task')
    if not auto_import:
        log.warning('auto_run_import: the task %s is not automatically launchable'
                    ' ("auto_import option was not selected',
                    rqtask.dc_title())
        return
    if rqtask is None:
        log.warning('auto_run_import: no rqtask found, do not launch import')
        return
    importtask = cnx.create_entity(
        'RqTask',
        name=u'import_alignment',
        title=u'automatic import_alignment for {}'.format(rqtask.eid)
    )
    tmp_fd, tmp_filepath = tempfile.mkstemp()
    os.write(tmp_fd, cwfile.data.getvalue())
    os.close(tmp_fd)
    importtask.cw_adapt_to('IRqJob').enqueue(import_alignment, tmp_filepath)
    log.info('auto_run_import: launch task %s', rqtask.dc_title())
    rqtask.cw_set(subtasks=importtask)
    cnx.commit()


@rqjob
def import_alignment(cnx, csvpath, override_alignments=False):
    """create same_as relation based on csv

    parameters
    ----------
    csvpath: filepath to csv which contains the columns defined in Recorde.headers
    """
    log = logging.getLogger('rq.task')
    log.info('auto_run_import: start the task')
    aligner = GeonameAligner(cnx, log)
    aligner.process_csvpath(csvpath, override_alignments=override_alignments)

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

"""
Align findingaids wirh geonames.org
"""
from __future__ import print_function
from itertools import chain
import csv
import os.path
import logging
import multiprocessing as mp
from glob import glob

from cubicweb_francearchives import admincnx
from cubicweb_francearchives.dataimport import sqlutil

from cubicweb_frarchives_edition.alignments.geonames_align import (
    GeonameAligner, Record, process_csv)

LOGGER = logging.getLogger()


def geonames_align_findingaid(aligner, findingaid_info, log, csv_dir):
    findingaid_eid, findingaid_stable_id = findingaid_info
    lines = aligner.compute_findingaid_alignment(findingaid_eid)
    if lines:
        csv_path = '{}.csv'.format(findingaid_stable_id)
        with open(os.path.join(csv_dir, csv_path), 'w') as fout:
            writer = csv.writer(fout, delimiter='\t')
            writer.writerow(Record.headers.keys())
            writer.writerows(lines)


def findingaid_geonames_aligner(appid, alignement_queue, csv_dir):
    log = logging.getLogger('aligne geonames')
    with admincnx(appid) as cnx:
        _findingaid_geonames_aligner(cnx, alignement_queue, log, csv_dir)


def _findingaid_geonames_aligner(cnx, alignement_queue, log, csv_dir):
    aligner = GeonameAligner(cnx, log)
    while True:
        next_job = alignement_queue.get()
        # worker got None in the queue, job is finished
        if next_job is None:
            break
        findingaid_info = next_job
        try:
            geonames_align_findingaid(aligner, findingaid_info, log, csv_dir)
        except Exception:
            import traceback
            traceback.print_exc()
            print('failed to import', repr(findingaid_info))
            LOGGER.exception('failed to import %r', findingaid_info)
            continue


class FakeQueue(list):

    def get(self, *args):
        return self.pop(*args)


def import_alignments(cnx, log, csv_dir):
    aligner = GeonameAligner(cnx, log)
    existing_alignment = aligner.compute_existing_alignment()
    new_align_global, to_remove_global = {}, {}
    sameas_history = aligner.sameas_history()
    for csvpath in glob(os.path.join(csv_dir, '*')):
        with open(csvpath) as f:
            new_alignment, to_remove_alignment = process_csv(
                f, existing_alignment, sameas_history)
            new_align_global.update(new_alignment)
            to_remove_global.update(to_remove_alignment)
        log.info('%s done: %s items in `new_align_global`', csvpath, len(new_align_global))
    aligner.process_alignments(new_align_global, to_remove_global)


def align_findingaids(cnx, appid, csv_dir, services, nodrop):
    log = logging.getLogger('aligne geonames')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    query = 'Any X, S WHERE X is FindingAid, X stable_id S'
    if services:
        query += ', X service SV, SV code IN ({})'.format(
            ','.join('"%s"' % s.upper() for s in services)
        )
    findingaids = cnx.execute(query).rows
    if not findingaids:
        log.info('no findingaids found')
        return
    if not nodrop:
        if not services:
            ext_uris = cnx.system_sql('''
            SELECT ext.cw_eid FROM cw_externaluri ext
            LEFT JOIN sameas_history sh ON ext.cw_uri=sh.sameas_uri
            WHERE ext.cw_source='geoname'
            ''').fetchall()
            if ext_uris:
                with sqlutil.no_trigger(
                        cnx, interactive=False,
                        tables=('entities',
                                'created_by_relation',
                                'owned_by_relation',
                                'cw_source_relation',
                                'is_relation',
                                'is_instance_of_relation',
                                'cw_externaluri',
                                'same_as_relation')):
                    cursor = cnx.cnxset.cu
                    log.info('Start deleting %r ExternalUris', len(ext_uris))
                    # delete ExternalUri and same_as relation
                    cursor.execute('DROP TABLE IF EXISTS exturl_to_remove')
                    cursor.execute('CREATE TABLE exturl_to_remove (eid integer)')
                    cursor.executemany(
                        'INSERT INTO exturl_to_remove (eid) VALUES (%s)', ext_uris)
                    cursor.execute('''
                    WITH tmp as (
                        SELECT cw_eid FROM cw_locationauthority l
                        JOIN same_as_relation sa ON sa.eid_from = l.cw_eid
                        JOIN exturl_to_remove eu ON sa.eid_to = eu.eid
                    )
                    UPDATE
                     cw_locationauthority as l
                    SET cw_latitude=NULL, cw_longitude=NULL
                    FROM tmp
                    WHERE tmp.cw_eid = l.cw_eid
                    ''')
                    cursor.executemany(
                        'DELETE FROM same_as_relation '
                        'WHERE eid_to = %s', ext_uris)
                    cursor.execute(
                        "SELECT delete_entities('cw_externaluri', 'exturl_to_remove')"
                    )
                    cnx.commit()
                    cursor.execute('DROP TABLE IF EXISTS exturl_to_remove')
                    cnx.commit()
                log.info('Deleted %r ExternalUris', len(ext_uris))
    nb_processes = max(mp.cpu_count() - 1, 1)
    if nb_processes == 1:
        fake_queue = FakeQueue([None] + findingaids)
        findingaid_geonames_aligner(appid, fake_queue, csv_dir)
    else:
        queue = mp.Queue(2 * nb_processes)
        workers = []
        for i in range(nb_processes):
            workers.append(mp.Process(target=findingaid_geonames_aligner,
                                      args=(appid, queue, csv_dir)))
        for w in workers:
            w.start()
        nb_findingaids = len(findingaids)
        for idx, job in enumerate(chain(findingaids, (None,) * nb_processes)):
            if job is not None:
                print('pushing {}/{} job in queue - findingaid stable_id {}'.format(
                    idx + 1, nb_findingaids, job[1]))
            queue.put(job)
        for w in workers:
            w.join()
    import_alignments(cnx, log, csv_dir)

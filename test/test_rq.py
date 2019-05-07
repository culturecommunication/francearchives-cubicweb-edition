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
"""cubicweb-frarchives_edition tests for RQ tasks"""
import logging

import rq
import rq.job
import fakeredis
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb_frarchives_edition.rq import work, rqjob

from utils import FrACubicConfigMixIn


@rqjob
def rq_task(cnx, success=False):
    logger = logging.getLogger('rq.task')
    for level in ('debug', 'info', 'warning', 'error', 'critical'):
        getattr(logger, level)(level)
    try:
        raise RuntimeError('catched')
    except Exception:
        logger.exception('unexpected')
    if success:
        return 42
    else:
        raise ValueError('uncatched')


class RQTaskTC(FrACubicConfigMixIn, CubicWebTC):

    def setUp(self):
        super(RQTaskTC, self).setUp()
        self.fakeredis = fakeredis.FakeStrictRedis()

    def assertDateAlmostEqual(self, d1, d2, epsilon=0.1):
        dt = d1 - d2
        self.assertLessEqual(
            abs(dt.total_seconds()), epsilon, '%s and %s are not almost equal' % (d1, d2)
        )

    def test_success(self):
        with self.admin_access.cnx() as cnx, rq.Connection(self.fakeredis):
            task = cnx.create_entity('RqTask', name=u'import_ead')
            job = task.cw_adapt_to('IRqJob')
            job.enqueue(rq_task, success=True)
            cnx.commit()
            self.assertEqual(job.status, 'queued')
            work(cnx, burst=True, worker_class=rq.worker.SimpleWorker)
            job.refresh()
            self.assertEqual(job.status, 'finished')
            self.assertEqual(job.result, 42)
            log = job.log
            for expected in (
                'debug', 'info', 'warning', 'error', 'critical',
                'unexpected', 'RuntimeError: catched'
            ):
                self.assertIn(expected, log)
            task = cnx.entity_from_eid(task.eid)
            self.assertEqual(task.status, job.status)
            for attr in ('enqueued_at', 'started_at'):
                self.assertDateAlmostEqual(getattr(task, attr), getattr(job, attr))
            self.assertEqual(task.log.read(), log)

    def test_failure(self):
        with self.admin_access.cnx() as cnx, rq.Connection(self.fakeredis):
            task = cnx.create_entity('RqTask', name=u'import_ead')
            job = task.cw_adapt_to('IRqJob')
            job.enqueue(rq_task, success=False)
            cnx.commit()
            self.assertEqual(job.status, 'queued')
            work(cnx, burst=True, worker_class=rq.worker.SimpleWorker)
            job.refresh()
            self.assertEqual(job.status, 'failed')
            self.assertEqual(job.result, None)
            log = job.log
            for expected in (
                'debug', 'info', 'warning', 'error', 'critical',
                'unexpected', 'RuntimeError: catched', 'ValueError: uncatched',
            ):
                self.assertIn(expected, log)
            task = cnx.entity_from_eid(task.eid)
            self.assertEqual(task.status, job.status)
            for attr in ('enqueued_at', 'started_at'):
                self.assertDateAlmostEqual(getattr(job, attr), getattr(task, attr))
            self.assertEqual(task.log.read(), log)

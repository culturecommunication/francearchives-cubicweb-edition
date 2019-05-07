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
from __future__ import absolute_import
from functools import wraps
from contextlib import contextmanager
import sys
import logging

import rq
import rq.job
from rq.utils import ColorizingStreamHandler
from rq.contrib.sentry import register_sentry
import six

from cubicweb.cwconfig import CubicWebConfiguration

from cubicweb_francearchives import admincnx as orig_admincnx, init_bfss
from cubicweb_francearchives.dataimport.ead import init_sentry_client, RAVEN_CLIENT


@contextmanager
def admincnx(appid_or_cnx, loglevel=None):
    if isinstance(appid_or_cnx, six.string_types):
        with orig_admincnx(appid_or_cnx, loglevel) as cnx:
            yield cnx
    else:
        assert appid_or_cnx.vreg.config.mode == 'test', \
            'expected to be a connection only in test mode'
        yield appid_or_cnx


def rqjob(task):
    @wraps(task)
    def task_wrapper(appid, *args, **kwargs):
        job = rq.get_current_job()
        with admincnx(appid, loglevel='warning') as cnx:
            init_bfss(cnx.repo)
            rqtask = cnx.find('RqTask', eid=int(job.id)).one()
            irqjob = rqtask.cw_adapt_to('IRqJob')
            try:
                result = task(cnx, *args, **kwargs)
            except Exception:
                logging.getLogger('rq.task').exception('yo !')
                cnx.rollback()
                irqjob.handle_failure(*sys.exc_info())
                cnx.commit()
                raise
            else:
                irqjob.handle_finished()
                cnx.commit()
        return result
    return task_wrapper


class RedisHandler(logging.Handler):

    def emit(self, record):
        job = rq.get_current_job()
        if job is not None:
            key = 'rq:job:{0}:log'.format(job.id)
            pipe = job.connection.pipeline()
            msg = self.format(record)
            if isinstance(msg, six.text_type):
                msg = msg.encode('utf-8')
            pipe.append(key, msg)
            # XXX: find a better value than '7260'
            #      7200 is our default timeout on task
            #      (cf. .entities.adapters.StartRqTaskOp.postcommit_event)
            pipe.expire(key, 7260)
            pipe.execute()


def update_progress(job, progress_value):
    job.meta['progress'] = progress_value
    job.save_meta()
    return progress_value


def config_from_appid(appid_or_cnx):
    if isinstance(appid_or_cnx, six.string_types):
        return CubicWebConfiguration.config_for(appid_or_cnx)
    assert appid_or_cnx.vreg.config.mode == 'test', \
        'expected to be a connection only in test mode'
    return appid_or_cnx.vreg.config


def work(appid, burst=False, worker_class=rq.Worker):
    logger = logging.getLogger('rq.worker')
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                      datefmt='%H:%M:%S')
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    task_logger = logging.getLogger('rq.task')
    task_logger.setLevel(logging.DEBUG)
    handler = RedisHandler()
    handler.setFormatter(logging.Formatter(
        fmt="%(levelname)s %(asctime)s %(module)s %(process)d %(message)s\n"))
    task_logger.addHandler(handler)

    class Job(rq.job.Job):

        @property
        def args(self):
            return (appid,) + super(Job, self).args

    cwconfig = config_from_appid(appid)
    init_sentry_client(cwconfig)
    worker = worker_class('default', job_class=Job)
    if RAVEN_CLIENT.get('default'):
        register_sentry(worker, RAVEN_CLIENT['default'])
    worker.work(burst=burst)

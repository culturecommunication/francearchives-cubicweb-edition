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

import rq

from cubicweb_frarchives_edition.rq import update_progress, rqjob


@rqjob
def publish_findingaid(cnx, imported_task_eid, taskeid=None):
    log = logging.getLogger("rq.task")
    rset = cnx.find("RqTask", eid=imported_task_eid)
    if not rset:
        log.warning('no task with this eid "%s"', imported_task_eid)
        return
    importead_task = rset.one()
    job = rq.get_current_job()
    current_progress = update_progress(job, 0.0)
    imported_findingaids = importead_task.fatask_findingaid
    log.info("Importead_findingaid number : %r", len(importead_task.fatask_findingaid))
    if importead_task.fatask_findingaid:
        progress_step = 1.0 / len(importead_task.fatask_findingaid)
        for fa in importead_task.fatask_findingaid:
            adapted = fa.cw_adapt_to("IWorkflowable")
            adapted.fire_transition_if_possible("wft_cmsobject_publish")
            current_progress = update_progress(job, current_progress + progress_step)
        # link published IR to the current task
        job = rq.get_current_job()
        if job is not None and taskeid is None:
            taskeid = int(job.id)
        log.info("taskeid : %r", taskeid)
        if taskeid is not None:
            entity = cnx.entity_from_eid(taskeid)
            entity.cw_set(fatask_findingaid=imported_findingaids)
            log.info("set %r fatask_findingaid", taskeid)
    cnx.commit()

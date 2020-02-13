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

from cubicweb_francearchives.dataimport.oai import import_delta

from cubicweb_frarchives_edition.rq import rqjob
from cubicweb_frarchives_edition.tasks.compute_alignments import compute_alignments
from cubicweb_frarchives_edition.tasks.publish import publish_findingaid


@rqjob
def import_oai(cnx, repo_eid, ignore_last_import=False, auto_import=False, publish=False):
    log = logging.getLogger("rq.task")
    oairepo = cnx.entity_from_eid(repo_eid)
    job = rq.get_current_job()
    taskeid = int(job.id)
    import_delta(cnx, repo_eid, ignore_last_import, log=log, reraise=True, rqtask_eid=taskeid)
    oaitask = oairepo.tasks[-1]
    wf = oaitask.cw_adapt_to("IWorkflowable")
    start = oaitask.creation_date
    stop = wf.latest_trinfo().creation_date
    rset = cnx.execute(
        "Any X WHERE X is FindingAid, X creation_date >= %(start)s, X creation_date <= %(stop)s",
        {"start": start, "stop": stop},
    )
    imported_findingaids = [fa for fa, in rset]
    log.info("%s imported findingaid(s)", len(imported_findingaids))
    if imported_findingaids:
        rqtask = cnx.entity_from_eid(taskeid)
        rqtask.cw_set(fatask_findingaid=imported_findingaids)
        cnx.commit()
    # remove published findingaid that was deleted in current task
    cnx.system_sql(
        "SELECT published.unpublish_findingaid(fa.cw_eid) "
        "FROM published.cw_findingaid fa LEFT OUTER JOIN entities e ON "
        "(e.eid=fa.cw_eid) WHERE e.eid IS NULL"
    )
    # insert intial state for all FindingAid with no current state
    rset = cnx.execute(
        'Any S WHERE S is State, S state_of WF, X default_workflow WF, X name "FindingAid", '
        "WF initial_state S"
    )
    cnx.system_sql(
        "INSERT INTO in_state_relation (eid_from, eid_to) "
        "SELECT cw_eid, %(eid_to)s FROM cw_findingaid WHERE "
        "NOT EXISTS (SELECT 1 FROM in_state_relation i "
        "WHERE i.eid_from = cw_eid)",
        {"eid_to": rset[0][0]},
    )
    if not imported_findingaids:
        return
    # publish harvested findingaids
    if publish:
        publish_task = cnx.create_entity(
            "RqTask", name="publish_findingaid", title="publish IR harvested in {}".format(job.id)
        )
        publish_task.cw_adapt_to("IRqJob").enqueue(publish_findingaid, rqtask.eid)
        rqtask.cw_set(subtasks=publish_task.eid)
        cnx.commit()
    # launch compute alignment
    aligntask = cnx.create_entity(
        "RqTask",
        name="compute_alignments",
        title="automatic compute_alignments for {}".format(job.id),
    )
    aligntask.cw_adapt_to("IRqJob").enqueue(compute_alignments, imported_findingaids, auto_import)
    rqtask.cw_set(subtasks=aligntask.eid)
    cnx.commit()

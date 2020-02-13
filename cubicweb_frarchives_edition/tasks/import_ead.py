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

from cubicweb_francearchives import init_bfss
from cubicweb_francearchives.dataimport import (
    ead,
    sqlutil,
    es_bulk_index,
    load_services_map,
    service_infos_from_filepath,
)
from cubicweb_francearchives.dataimport.stores import create_massive_store

from cubicweb_frarchives_edition.rq import update_progress, rqjob
from cubicweb_frarchives_edition.tasks.compute_alignments import compute_alignments


def service_code_from_faeid(cnx, faeids):
    eids = ",".join(str(e) for e in faeids)
    rset = cnx.execute(
        "DISTINCT Any C WHERE X is FindingAid, X eid IN ({}), X service S, S code C".format(eids)
    )
    return [c for c, in rset]


def process_import_ead(reader, filepath, services_map, log):
    service_infos = service_infos_from_filepath(filepath, services_map)
    return reader.import_filepath(filepath, service_infos)


@rqjob
def import_ead(cnx, filepaths, force_delete=False, auto_align=True, auto_dedupe=True, taskeid=None):
    launch_task(
        cnx,
        ead.Reader,
        process_import_ead,
        filepaths,
        auto_dedupe=auto_dedupe,
        force_delete=force_delete,
        taskeid=taskeid,
        auto_align=auto_align,
    )


def launch_task(
    cnx,
    readercls,
    process_func,
    filepaths,
    metadata_filepath=None,
    force_delete=False,
    auto_align=False,
    auto_dedupe=True,
    taskeid=None,
):
    config = ead.readerconfig(
        cnx.vreg.config, cnx.vreg.config.appid, esonly=False, nodrop=True, force_delete=force_delete
    )
    log = config["log"] = logging.getLogger("rq.task")
    log.info("Start the task.")
    job = rq.get_current_job()
    current_progress = update_progress(job, 0.0)
    progress_step = 1.0 / (len(filepaths) + 1)
    config["reimport"] = True
    config["nb_processes"] = 1
    config["autodedupe_authorities"] = "service/strict" if auto_dedupe else None
    foreign_key_tables = sqlutil.ead_foreign_key_tables(cnx.vreg.schema)
    store = create_massive_store(cnx, nodrop=config["nodrop"])
    with sqlutil.no_trigger(cnx, foreign_key_tables, interactive=False):
        services_map = load_services_map(cnx)
        init_bfss(cnx.repo)
        indexer = cnx.vreg["es"].select("indexer", cnx)
        es = indexer.get_connection()
        log.info("Getting readercls...")
        r = readercls(config, store)
        for filepath in filepaths:
            log.info("Start importing %r", filepath)
            try:
                if metadata_filepath:
                    es_docs = process_func(r, filepath, metadata_filepath, services_map, log)
                else:
                    es_docs = process_func(r, filepath, services_map, log)
            except Exception:
                es_docs = []
                log.exception("failed to import %r in import_ead task", filepath)
            if es_docs:
                log.info("Start reindexing elasticsearch for %r", filepath)
                es_bulk_index(es, es_docs)
            store.flush()
            current_progress = update_progress(job, current_progress + progress_step)
        store.finish()
    # remove published findingaid that was deleted in current task
    cnx.system_sql(
        "SELECT published.unpublish_findingaid(fa.cw_eid) "
        "FROM published.cw_findingaid fa LEFT OUTER JOIN entities e ON "
        "(e.eid=fa.cw_eid) WHERE e.eid IS NULL"
    )
    # insert intial state for all FindingAid with no current state
    log.info("insert intial state for all FindingAid with no current state")
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
    log.info("Imported findingaids number : %r", len(r.imported_findingaids))
    if r.imported_findingaids:
        job = rq.get_current_job()
        if job is not None and taskeid is None:
            taskeid = int(job.id)
        if taskeid is not None:
            entity = cnx.entity_from_eid(taskeid)
            entity.cw_set(fatask_findingaid=r.imported_findingaids)
            log.info("set %r fatask_findingaid", taskeid)
    cnx.commit()
    if not r.imported_findingaids or taskeid is None:
        return
    aligntask = cnx.create_entity(
        "RqTask",
        name="compute_alignments",
        title="automatic compute_alignments for job {}".format(job.id),
    )
    aligntask.cw_adapt_to("IRqJob").enqueue(compute_alignments, r.imported_findingaids, auto_align)
    entity.cw_set(subtasks=aligntask.eid)
    cnx.commit()

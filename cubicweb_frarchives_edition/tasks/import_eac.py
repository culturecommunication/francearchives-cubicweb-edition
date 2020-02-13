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
from collections import defaultdict

import logging

import rq

from cubicweb_francearchives.dataimport import eac, sqlutil
from cubicweb_francearchives.dataimport.stores import create_massive_store

from cubicweb_frarchives_edition.rq import update_progress, rqjob


@rqjob
def import_eac(cnx, filepaths, nodrop=True, taskeid=None):
    log = logging.getLogger("rq.task")
    log.info("Start the task.")
    job = rq.get_current_job()
    current_progress = update_progress(job, 0.0)
    progress_step = 1.0 / (len(filepaths) + 1)
    store = create_massive_store(cnx, nodrop=nodrop, eids_seq_range=1000)
    foreign_key_tables = sqlutil.ead_foreign_key_tables(cnx.vreg.schema)
    sameas_authorityrecords = defaultdict(set)
    query = """
    DISTINCT Any R, A WITH R, A BEING (
    (
        DISTINCT Any R, A WHERE A same_as Y, Y is AuthorityRecord, Y record_id R
    ) UNION
    (   DISTINCT Any R, A WHERE I authority A,
        I authfilenumber R, NOT I authfilenumber NULL)
    )"""
    for record_id, autheid in store.rql(query):
        sameas_authorityrecords[record_id].add(autheid)
    log.info("Start initializing data")
    extid2eid = eac.init_extid2eid_index(cnx, cnx.repo.system_source)
    log.info("Finish initializing data")
    foreign_key_tables = eac.eac_foreign_key_tables(cnx.vreg.schema)
    service = cnx.vreg["services"].select("eac.import", cnx)
    imported_authrecords = set()
    imported = created = updated = 0
    with sqlutil.no_trigger(cnx, foreign_key_tables, interactive=False):
        for fpath in filepaths:
            log.info("Start importing %r", fpath)
            _created, _updated, record, not_visited = eac.eac_import_file(
                service, store, fpath, extid2eid, log
            )
            if _created or _updated:
                imported += 1
                created += len(_created)
                updated += len(_updated)
                store.flush()
                store.commit()
                imported_authrecords.add((record["eid"], record["record_id"]))
                current_progress = update_progress(job, current_progress + progress_step)
        if imported:
            store.finish()
        output_str = (
            "\nImported {imported}/{total} files ({created} entities + " "{updated} updates)"
        )
        log.info(
            output_str.format(
                imported=imported, created=created, total=len(filepaths), updated=updated
            )
        )
    if imported:
        log.info("Start postprocess")
        eac.postprocess_import_eac(cnx, imported_authrecords, sameas_authorityrecords, log)
        job = rq.get_current_job()
        if job is not None and taskeid is None:
            taskeid = int(job.id)
        if taskeid is not None:
            entity = cnx.entity_from_eid(taskeid)
            print("imported_authrecords", imported_authrecords)
            entity.cw_set(fatask_authorityrecord=[r[0] for r in imported_authrecords])
    cnx.commit()

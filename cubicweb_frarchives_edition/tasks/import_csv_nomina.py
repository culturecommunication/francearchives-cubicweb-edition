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

from cubicweb_francearchives.dataimport import sqlutil, es_bulk_index

from cubicweb_francearchives.dataimport.stores import create_massive_store
from cubicweb_francearchives.dataimport.csv_nomina import CSVNominaReader, readerconfig

from cubicweb_frarchives_edition.rq import rqjob


@rqjob
def import_csv_nomina(
    cnx,
    filepath,
    service_code,
    doctype,
    delimiter=";",
    taskeid=None,
):
    log = logging.getLogger("rq.task")
    store = create_massive_store(cnx, nodrop=True)
    config = readerconfig(cnx.vreg.config)
    reader = CSVNominaReader(config, store, service_code, log=log)
    log.debug('Start importing filepath="%s"', filepath)
    notrigger_tables = sqlutil.nomina_foreign_key_tables(cnx.vreg.schema)
    with sqlutil.no_trigger(cnx, notrigger_tables, interactive=False):
        try:
            es_docs = reader.import_records(filepath, doctype, delimiter)
        except Exception as error:
            es_docs = []
            log.exception(
                """
                failed to import {fpath} in import_csv_nomina task.
                <div class="alert alert-danger">{error}</div>""".format(
                    fpath=filepath, error=error
                )
            )
        store.flush()
        store.finish()
        log.info(
            "Imported %s new and %s updated nomina records in Postgres.",
            reader.created_records,
            reader.updated_records,
        )
        if es_docs:
            log.info("Start ES indexing of %s nomina records", len(es_docs))
            indexer = cnx.vreg["es"].select("nomina-indexer", cnx)
            es = indexer.get_connection()
            try:
                es_bulk_index(es, es_docs)
            except Exception as error:
                log.error("[es] error: %s" % error)
            log.info("End ES indexing nomina records")
        if not reader.created_records + reader.updated_records:
            log.info("No valid nomina records found. No nomina records has been imported.")

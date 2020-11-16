# -*- coding: utf-8 -*-
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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


# standard library imports
import logging

import rq

# third party imports

# CubicWeb specific imports
# library specific imports
from elasticsearch.helpers import parallel_bulk

from cubicweb_elasticsearch.es import indexable_entities


from cubicweb_frarchives_edition.entities.kibana.sqlutils import create_kibana_authorities_sql

from cubicweb_frarchives_edition.rq import update_progress, rqjob


def update_sql_data(cnx, log):
    log.info("creating sql temporary tables for authorities")
    create_kibana_authorities_sql(cnx)


def bulk_actions(cnx, indexer, adapter, etype, log, job, current_progress, progress_step):
    indexed = 0
    for idx, entity in enumerate(indexable_entities(cnx, etype, chunksize=100000), 1):
        serializer = entity.cw_adapt_to(adapter)
        if not serializer:
            log.error("Adaptor {} not found for {}".format(adapter, etype))
            raise
        json = serializer.serialize(complete=False)
        if json:
            data = {
                "_op_type": "index",
                "_index": indexer.index_name,
                "_id": serializer.es_id,
                "_source": json,
            }
            yield data
        indexed += 1
        current_progress = update_progress(job, current_progress + progress_step)
    log.info("[{}] indexed {} {} entities".format(indexer.index_name, indexed, etype))


@rqjob
def index_kibana(cnx, index_authorities=True, index_services=False):
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    current_progress = update_progress(job, 0.0)
    indexers = {}
    if index_authorities:
        indexers["authority"] = "kibana-auth-indexer"
    if index_services:
        indexers["service"] = "kibana-service-indexer"
    for indexer_name in indexers.values():
        indexer = cnx.vreg["es"].select(indexer_name, cnx)
        log.info("""start reindexing "{}" index""".format(indexer.index_name))
        es = indexer.get_connection()
        if not es:
            log.error("no elasticsearch configuration found, skipping")
            return
        indexer.create_index()
        if indexer_name == "kibana-auth-indexer":
            update_sql_data(cnx, log)
            adapter = "IKibanaInitiaLAuthorityIndexSerializable"
        else:
            adapter = "IKibanaIndexSerializable"
        for etype in indexer.etypes:
            nb_entities = cnx.execute("Any COUNT(X) WHERE X is %s" % etype)[0][0]
            progress_step = 1.0 / (nb_entities + 1)
            log.info("start indexing {} {}".format(nb_entities, etype))
            for _ in parallel_bulk(
                es,
                bulk_actions(
                    cnx, indexer, adapter, etype, log, job, current_progress, progress_step
                ),
            ):
                pass
        log.info("""finished reindexing "{}" index """.format(indexer.index_name))

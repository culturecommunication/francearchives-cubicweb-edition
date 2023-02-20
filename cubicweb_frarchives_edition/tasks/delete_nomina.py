# -*- coding: utf-8 -*-
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2022
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

# third party imports
from elasticsearch_dsl import Search, query as dsl_query

# CubicWeb specific imports
from logilab.common.decorators import timed


from cubicweb_elasticsearch.es import get_connection

from cubicweb_francearchives.scripts.index_nomina import index_nomina_in_es
from cubicweb_frarchives_edition.rq import rqjob
from cubicweb_frarchives_edition.scripts.fast_drop_entities import fast_drop_entities


@timed
def delete_nomina_records_from_pg(cnx, service, logger, chunksize=50000):
    nb_entities = cnx.execute(
        f"Any COUNT(X) WHERE X service S, X is NominaRecord, S eid {service.eid}"
    )[0][0]
    logger.info(f"[postgres]: start deleting {nb_entities} NominaRecords for {service.code}")
    rql = """Any X LIMIT {limit}
             WHERE X service S, S eid {eid}, X is NominaRecord, X stable_id ST"""
    deleted = 0
    rset = cnx.execute(rql.format(limit=chunksize, eid=service.eid))
    while rset:
        # test permisions for one entity
        if not rset.get_entity(0, 0).cw_has_perm("delete"):
            logger.exception(
                "[postgres]: Abort deletion: you are not allowed to delete NominaRecords"
            )
            return deleted
        try:
            fast_drop_entities(rset)
            cnx.commit()
        except Exception as ex:
            logger.exception("[es]: Abort deletion: %s", ex)
            return deleted
        deleted += rset.rowcount
        logger.info(f"[postgres]: deleted {deleted} NominaRecords out of {nb_entities}")
        rset = cnx.execute(rql.format(limit=chunksize, eid=service.eid))
    return deleted


def number_of_nomina_records(cnx, index_name, service):
    search = Search(index=index_name)
    must = [
        {"match": {"service": service.eid}},
        {"match": {"cw_etype": "NominaRecord"}},
    ]
    search.query = dsl_query.Bool(must=must)
    return search.count()


def delete_nomina_records_from_es(cnx, es_cnx, index_name, service, logger):
    nb_entities = number_of_nomina_records(cnx, index_name, service)
    logger.info(f"[es]: expect {nb_entities} NominaRecords to be deleted for {service.code}")
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"service": service.eid}},
                    {"match": {"cw_etype": "NominaRecord"}},
                ],
            }
        }
    }
    logger.info(f"[es]: start deleting NominaRecords for {service.code}")
    res = es_cnx.delete_by_query(index_name, body=query)
    es_total = res["total"]
    es_deleted = res["deleted"]
    if es_total != nb_entities or es_deleted != nb_entities:
        func = logger.error
    else:
        func = logger.info
    func(f"[es]: {es_deleted}/{es_total} NominaRecords have been deleted.")


def index_es_nomina_records(cnx, es, index_name, service, logger):
    index_nomina_in_es(cnx, es, index_name, logger, services=[service], dry_run=False)


@rqjob
def delete_nomina_by_service(cnx, service_eid):
    """Delete NominaRecord by service

    :param Connection cnx: CubicWeb database connection
    :param service_eid: eid of the service which NominaRecord will be deleted
    """
    logger = logging.getLogger("rq.task")
    service = cnx.find("Service", eid=service_eid).one()
    # delete all NominaRecords of the service from Postgres
    deleted = delete_nomina_records_from_pg(cnx, service, logger)
    if not deleted:
        return
    cwconfig = cnx.vreg.config
    es_cnx = get_connection(cwconfig)
    if not es_cnx:
        logger.error(
            "[es]: could not delete NominaRecords from es: " "no elastisearch connection available."
        )
        return
    index_name = cwconfig["nomina-index-name"]
    if not index_name:
        logger.error("[es]: could not delete NominaRecords from es: no index name found.")
        return
    # delete all NominaRecords of the service from ES
    delete_nomina_records_from_es(cnx, es_cnx, index_name, service, logger)
    nb_entities = cnx.execute(
        f"Any COUNT(X) WHERE X service S, X is NominaRecord, S eid {service.eid}"
    )[0][0]
    if nb_entities:
        # in case something got wrong, reindex NominaRecords for the service
        logger.info(f"[es]: reindex {nb_entities} remaining NominaRecords")
        index_es_nomina_records(cnx, es_cnx, index_name, service.code, logger)

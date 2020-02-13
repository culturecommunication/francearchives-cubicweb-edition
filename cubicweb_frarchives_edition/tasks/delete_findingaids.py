# -*- coding: utf-8 -*-
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


# standard library imports
import csv
import logging

# third party imports
from elasticsearch.exceptions import ConnectionError, NotFoundError
from urllib3.exceptions import ProtocolError

# CubicWeb specific imports
# library specific imports
from cubicweb_francearchives.dataimport.sqlutil import delete_from_filename
from cubicweb_frarchives_edition.rq import rqjob


def log_results(log, deleted, ids, not_found, forbidden):
    log.info(
        "processed %s identifiers", len(ids),
    )
    if deleted:
        log.info("deleted %s FindingAids: %s", len(deleted), "; ".join(deleted))
    if not_found:
        log.error("could not find %d FindingAids with stable_ids/eadids: %s",
                  len(not_found), "; ".join(not_found))
    if forbidden:
        log.error(
            "you have no permission to delete %d FindingAids: %s",
            len(forbidden),
            ", ".join(forbidden),
        )


@rqjob
def delete_findingaids(cnx, filename):
    """Delete FindingAids.

    :param Connection cnx: CubicWeb database connection
    :param str filename: filename with 1 column: stable_id_or_eadid
    """
    log = logging.getLogger("rq.task")
    try:
        fp = open(filename)
        ids = [row[0] for row in csv.reader(fp)]
    except Exception:
        log.error("unable to read stable ids from file, please check file contents")
        return
    finally:
        fp.close()
    if len(ids) <= 1:
        log.warning("empty list of stable ids / eadids")
        return
    # drop header
    ids = ids[1:]
    log.info("delete %d FindingAids", len(ids))
    not_found = []
    deleted = []
    forbidden = []
    for irid in ids:
        rset = cnx.execute(
            """Any X, S, N WHERE X is FindingAid, X stable_id S,
               X findingaid_support FS?, FS data_name N,
               X stable_id %(id)s OR X eadid %(id)s""", {
                "id": irid}
        )
        if not rset:
            not_found.append(irid)
            continue
        for entity, stable_id, filename in rset.iter_rows_with_entities():
            if not entity.cw_has_perm("delete"):
                forbidden.append("csv_id: {}, stable id: {}, filename: {})".format(
                    irid, stable_id, filename or ''))
            try:
                delete_from_filename(cnx, stable_id, is_filename=False,
                                     interactive=False, esonly=False)
                # no commit here because it is already done in sqlutil.delete_from_filename
                cnx.vreg["services"].select("sync", cnx).sync([("delete", entity)])
            except (ConnectionError, ProtocolError, NotFoundError):
                log.error("elasticsearch indexation failed for FindingAid %s",
                          entity.absolute_url())
            except Exception:
                log.error("unable to delete FindingAid %s ", entity.absolute_url())
                log.error("abort deletion")
                log_results(log, deleted, ids, not_found, forbidden)
                return
            deleted.append("csv_id: {}, stable id: {}, filename: {})".format(
                irid, stable_id, filename or ''))
    log_results(log, deleted, ids, not_found, forbidden)

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
import logging

# third party imports
from elasticsearch.exceptions import ConnectionError, NotFoundError
from urllib3.exceptions import ProtocolError

# CubicWeb specific imports
# library specific imports

from cubicweb_francearchives.utils import delete_from_es_by_eid
from cubicweb_frarchives_edition.rq import rqjob


def log_results(log, removed, ids, not_found, forbidden):
    if removed:
        log.info(f'removed {len(removed)} authorities: {"; ".join(removed)}')
    if not_found:
        log.error(
            f"""could not find {len(not_found)} authorities with eid: {"; ".join(not_found)}"""
        )
    if forbidden:
        log.error(
            f'you have no permission to remove {len(forbidden)} authorities: {", ".join(forbidden)}"'  # noqa
        )


@rqjob
def remove_authorities(cnx, eids, etypes=("SubjectAuthority",)):
    """Remove Authorities. By default do it for SubjectAuthorities

    :param Connection cnx: CubicWeb database connection
    :param eids list: list of eids to remove
    """
    log = logging.getLogger("rq.task")
    if len(eids) <= 1:
        log.warning("empty list of eid")
        return
    not_found = []
    removed = []
    removed_eids = []
    forbidden = []
    if not isinstance(eids, (list, tuple)):
        eids = (eids,)
    log.info("prepared to remove {} authorities: {}".format(len(eids), "; ".join(eids)))
    for eid in eids:
        rset = cnx.execute(
            """Any X WHERE X eid %(eid)s, X is IN (%(etypes)s)"""
            % {"eid": eid, "etypes": ", ".join(etypes)},
        )
        if not rset:
            not_found.append(eid)
            continue
        authority = rset.one()
        if authority.cw_etype not in etypes:
            log.warning(
                'do not remove "{}" ({}) as its etype is not in {}'.format(
                    authority.absolute_url(), eid, etypes
                )
            )
            continue
        if authority.quality:
            log.warning(
                'do not remove "{}" ({}) as it is a qualified authority'.format(
                    authority.absolute_url(), eid
                )
            )
            continue
        log.info(f'deleting "{authority.absolute_url()}"')
        if not authority.cw_has_perm("delete"):
            forbidden.append(f"{authority.absolute_url()}")
            continue
        indexes_count = cnx.execute(
            """Any COUNT(I) WHERE I authority E, E eid %(eid)s""", {"eid": authority.eid}
        )[0][0]
        log.info(f"Start reindex ES for {indexes_count} indexes")
        try:
            authority.delete_blacklisted()
        except (ConnectionError, ProtocolError, NotFoundError) as err:
            log.error(
                f"abort deletion of {authority.absolute_url()}: elasticsearch indexation: {err}"
            )
        except Exception as err:
            log.error(f"abort deletion of {authority.absolute_url()}: {err}")
            log_results(log, removed, eids, not_found, forbidden)
            continue
        log.info(f"deletion done for {authority.absolute_url()}")
        removed.append(f"{authority.absolute_url()}")
        removed_eids.append(eid)
    log.info("Delete removed authorities from ES")
    indexes = [
        cnx.vreg.config["index-name"] + "_suggest",
        cnx.vreg.config["published-index-name"] + "_suggest",
    ]
    if cnx.vreg.config["enable-kibana-indexes"]:
        indexes.append(cnx.vreg.config["kibana-authorities-index-name"])
    delete_from_es_by_eid(cnx, removed_eids, indexes)
    log_results(log, removed, eids, not_found, forbidden)

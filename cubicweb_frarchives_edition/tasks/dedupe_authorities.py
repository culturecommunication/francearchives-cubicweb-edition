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


from cubicweb_francearchives.dataimport import normalize_entry

from cubicweb_frarchives_edition.rq import rqjob

LOGGER = logging.getLogger(__name__)


def filter_authority(authorities):
    """
    remove entries of authories which have only one authority for a particular label
    """
    result = {}
    for label, auth in authorities.items():
        if len(auth) <= 1:
            continue
        # sort authorities according to their count of same_as
        auth.sort(key=lambda a: a[1])
        result[label] = auth
    return result


def dedupe_one_type(cnx, authtype, log=None, strict=True, service=None):
    """
    strict means do we compare label authority strictly or using normalize_entry function
    authtype is expected to be one of 'LocationAuthority', 'SubjectAuthority' or 'AgentAuthority'
    service is expected to be Service code (like 'FRAD054')
    """
    if log is None:
        log = LOGGER
    if service is None:
        rset = cnx.execute(
            "Any X, L, COUNT(SA) GROUPBY X, L WHERE X is {}, X label L, "
            "X same_as SA?".format(authtype)
        )
    else:
        rset = cnx.execute(
            """
            (
                Any X, L, COUNT(SA) GROUPBY X, L WHERE X is {0}, X label L, X same_as SA?,
                I authority X, I index FAC, FAC finding_aid FA, FA service S, S code %(s)s
            )
            UNION
            (
                Any X, L, COUNT(SA) GROUPBY X, L WHERE X is {0}, X label L, X same_as SA?,
                I authority X, I index FA, FA service S, S code %(s)s
            )
            UNION
            (
                Any X, L, COUNT(SA) GROUPBY X, L WHERE X is {0}, X label L, X same_as SA?,
                NOT EXISTS(I authority X)
            )
            """.format(
                authtype
            ),
            {"s": service},
        )
    # dict of label as key and list of authority eid with its count of same_as as value
    d = {}
    for x, label, s in rset:
        if not strict:
            label = normalize_entry(label)
        d.setdefault(label, []).append((x, s))
    log.debug("before filter %s %s", len(d), authtype)
    d = filter_authority(d)
    log.info("will dedupe %s %s", len(d), authtype)
    for label, authorities in d.items():
        if any(a[1] > 0 for a in authorities[:-1]):
            # at least 2 authorities with more than 0 same_as link => we can't decide
            # which authority should be kept
            continue
        auth_to_keep, _ = authorities[-1]
        for autheid, _ in authorities[:-1]:
            auth = cnx.entity_from_eid(autheid)
            # rewrite `index_entries` in related es docs
            for index in auth.reverse_authority:
                index.update_es_docs(oldauth=auth.eid, newauth=auth_to_keep)
            # redirect index entities from old authority to new authority
            cnx.execute(
                "SET I authority A WHERE A eid %(a)s, I authority OLD, OLD eid %(old)s",
                {"a": auth_to_keep, "old": autheid},
            )
            # delete old authority
            auth.cw_delete()


def dedupe(cnx, log=None, strict=True, service=None):
    for authtype in ("LocationAuthority", "SubjectAuthority", "AgentAuthority"):
        dedupe_one_type(cnx, authtype, log=log, strict=strict, service=service)
    cnx.commit()


@rqjob
def dedupe_authorities(cnx, strict=True, service=None):
    log = logging.getLogger("rq.task")
    dedupe(cnx, log=log, strict=strict, service=service)

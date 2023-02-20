# -*- coding: utf-8 -*-
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2021
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
from collections import OrderedDict, defaultdict
from psycopg2.extras import execute_batch
import rq

# CubicWeb specific imports

# library specific imports
from cubicweb_francearchives.storage import S3BfssStorageMixIn

from cubicweb_frarchives_edition.rq import rqjob, update_progress

from cubicweb_frarchives_edition import AUTH_URL_PATTERN

KIBANA_FIELDNAMES = OrderedDict(
    [
        ("urlpath", "urlpath"),
        ("label", "label"),
        ("quality", "quality"),
    ]
)


FIELDNAMES = OrderedDict(
    [
        ("identifiant", "urlpath"),
        ("libelle", "label"),
        ("qualite", "quality"),
    ]
)


def process_quality(quality):
    if quality in ("oui", "yes"):
        return True
    if quality in ("non", "no"):
        return False
    raise Exception(f"Invalid value for qualify '{quality}'")


def load_data(cnx, csvpath, fieldnames, log):
    st = S3BfssStorageMixIn(log=log)
    data = defaultdict(list)
    with st.storage_read_file(csvpath) as fp:
        reader = csv.DictReader(fp, delimiter="\t", fieldnames=fieldnames)
        next(reader, None)  # skip the headers
        for idx, line in enumerate(reader):
            entry = {
                fieldnames[key.lower()]: value if value else None for key, value in line.items()
            }
            try:
                quality = process_quality(entry["quality"])
            except Exception as err:
                log.error(
                    f"""line {idx}: found a wrong quality value "{entry["quality"]}": ({err}). Skip the row"""  # noqa
                )
                continue
            url = entry["urlpath"]
            match = AUTH_URL_PATTERN.match(url)
            if not match:
                log.error(
                    f"""line {idx}: found a wrong  identifiant "{url}" found. Skip the row"""
                )  # noqa
                continue
            eid = match["eid"]
            try:
                etype = cnx.system_sql(f"select type from entities where eid={eid}").fetchone()[0]
            except Exception:
                log.error(
                    f"""line {idx}: no entity with identifiant "{url}" found. Skip the row"""
                )  # noqa
                continue
            if isinstance(quality, float) and quality > 1:
                log.error(f"""line {idx}: quality score must be <= 1. Skip the row""")
                continue
            data[etype].append({"eid": eid, "quality": quality})
        return data


def process_qualification(cnx, csvpath, headers, log):
    """Process authority qualification

    :param Connection cnx: CubicWeb database connection
    :param Logger log: RqTask logger
    :param str csvpath: path to CSV file
    """
    data = load_data(cnx, csvpath, headers, log)
    query = "UPDATE cw_{} SET cw_quality=%s WHERE cw_eid=%s"
    for etype, values in data.items():
        args = [(row["quality"], row["eid"]) for row in values]
        execute_batch(cnx.cnxset.cu, query.format(etype), args)
        cnx.commit()
    log.info(f"""Updated {len(values)} {etype}""")


@rqjob
def import_qualified_authorities(cnx, csvpath, headers):
    """Qualify Authorities.

    :param Connection cnx: CubicWeb database connection
    :param str csvpath: path to CSV file
    """
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    log.info(f"process authority qualification from {csvpath}")
    try:
        process_qualification(cnx, csvpath, headers, log)
    except Exception as error:
        log.error(f"failed to update authority qualification : {error}")
    progress = update_progress(job, progress + 1)
    # delete the temporary file
    S3BfssStorageMixIn().storage_delete_file(csvpath)

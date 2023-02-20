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
import csv
import logging
import datetime
import os
import os.path

# third party imports
import rq

# CubicWeb specific imports
# library specific imports
from cubicweb_francearchives.scripts.dead_links import clean_up_linkchecker, run_linkchecker
from cubicweb_francearchives.storage import S3BfssStorageMixIn

from cubicweb_frarchives_edition.rq import rqjob
from cubicweb_frarchives_edition.tasks.utils import serve_csv


@rqjob
def run_dead_links(cnx):
    eid = int(rq.get_current_job().id)
    log = logging.getLogger("rq.task")
    linkchecker_output = cnx.vreg.config["linkchecker-output"]
    dead_links_output = cnx.vreg.config["dead-links-output"]
    log.info("start checking dead links. This will take several hours.")
    try:
        run_linkchecker(
            cnx.repo.vreg.config["consultation-base-url"],
            linkchecker_output,
            cnx.vreg.config["linkchecker-maxmem"],
            config=cnx.vreg.config["linkchecker-config"],
        )
    except RuntimeError as exception:
        log.warning("incomplete results:%s", exception)
    except Exception as exception:
        log.error(exception)
        return
    st = S3BfssStorageMixIn(log=log)
    try:
        clean_up_linkchecker(linkchecker_output, dead_links_output)
    except Exception as exception:
        log.error(exception)
        return
    output_file = os.path.join(dead_links_output, "liens_morts.csv")
    with st.storage_read_file(output_file) as fp:
        serve_csv(
            cnx,
            eid,
            f"dead-links-{datetime.datetime.now().strftime('%Y%m%d')}.csv",
            [row for row in csv.reader(fp, delimiter=";")],
            delimiter=";",
        )
    log.info("Stop checking dead links.")

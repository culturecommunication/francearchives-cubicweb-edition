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
import os.path as osp

import rq


from cubicweb_francearchives.dataimport.scripts.generate_ape_ead import (
    generate_ape_ead_xml_from_eids,
    generate_ape_ead_other_sources_from_eids,
)
from cubicweb_frarchives_edition.rq import update_progress, rqjob
from cubicweb_frarchives_edition.tasks.utils import zip_files, serve_zip


def retrieve_ape(cnx, service_code, ape_files, arcnames):
    """Retrieve APE files.

    :param Connection cnx: CubicWeb database connection
    :param str service: service
    :param list ape_files: APE files
    :param list arcnames: archive files

    :returns: list of APE files
    :rtype: list
    """
    log = logging.getLogger("rq.task")
    rset = cnx.find("Service", code=service_code)
    xml_rset = cnx.execute(
        "Any X, N, SI, FSPATH(D), NULL, CS "
        "WHERE X findingaid_support F, "
        "X stable_id SI, X name N,  F data D, "
        "X service S, S code CS, "
        'NOT EXISTS(X ape_ead_file AF), F data_format "application/xml", '
        "X service S, S code %(c)s",
        {"c": service_code},
    )
    other_fas = cnx.execute(
        """
      (Any X, XID WHERE X eadid XID,
       X service S, S code %(c)s,
       NOT EXISTS(X findingaid_support F), NOT EXISTS(X ape_ead_file AF)
    )
    UNION
      (Any X, XID WHERE X eadid XID,
       X service S, S code %(c)s,
       NOT EXISTS(X ape_ead_file AF),
       X findingaid_support F, NOT F data_format "application/xml")
    """,
        {"c": service_code},
    )
    if xml_rset:
        for (fa_eid, fa_name, fa_stable_id, fspath, ape_ead_fspath, service_code) in xml_rset:
            try:
                generate_ape_ead_xml_from_eids(cnx, [str(fa_eid)])
            except Exception:
                log.exception("failed to export %r (#%s)", fspath, fa_eid)
    if other_fas:
        # now generate ape-XML for PDF, dc_based and OAI based finding aids
        for fa in other_fas.entities():
            try:
                generate_ape_ead_other_sources_from_eids(cnx, [str(fa.eid)])
            except Exception:
                log.exception("failed to export ape dump for fa #%s", fa.eid)
                continue
    rset = cnx.execute(
        "Any X, SI, FSPATH(D) WHERE X ape_ead_file F, X stable_id SI, F data D, "
        "X service S, S code %(c)s",
        {"c": service_code},
    )
    if rset:
        for fa_eid, fa_stable_id, fspath in rset:
            fspath = fspath.getvalue().decode("utf-8")
            if not (osp.exists(fspath) and osp.isfile(fspath)):
                log.warning("file does not exists %r (#%s)", fspath, fa_eid)
                continue
            ape_files.append(fspath)
            arcnames.append(osp.join(service_code, osp.basename(fspath)))
    else:
        log.info("No files found for {service_code}".format(service_code=service_code))


@rqjob
def export_ape(cnx, service_codes):
    log = logging.getLogger("rq.task")
    job = rq.get_current_job()
    progress = update_progress(job, 0.0)
    if not service_codes:
        service_codes = [
            row[0]
            for row in cnx.execute(
                """DISTINCT Any C WHERE X is Service, X code C, NOT X code NULL,
                F service X, F is FindingAid"""
            )
        ]
    step = 1.0 / len(service_codes)
    ape_files = []
    arcnames = []
    for service_code in service_codes:
        log.info("export APE files for {service_code}".format(service_code=service_code))
        retrieve_ape(cnx, service_code, ape_files, arcnames)
        progress = update_progress(job, progress + step)
    # group all ape files in zip archive
    appfiles_dir = cnx.vreg.config["appfiles-dir"]
    zippath = osp.join(appfiles_dir, "ape_%s.zip" % job.id)
    if not arcnames:
        log.info(
            "No files found for {service_codes}".format(service_codes=", ".join(service_codes))
        )
        return
    files = list(zip(ape_files, arcnames))
    zip_files(files, archive=zippath)
    # compute url and move archive so that nginx can serve it
    serve_zip(cnx, int(job.id), osp.basename(zippath), zippath)
    cnx.commit()

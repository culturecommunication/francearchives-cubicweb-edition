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
import zipfile
import shutil

import rq


from cubicweb_francearchives.dataimport import usha1
from cubicweb_francearchives.dataimport.scripts.generate_ape_ead import (
    generate_ape_ead_xml_from_eids,
    generate_ape_ead_other_sources_from_eids)
from cubicweb_frarchives_edition.rq import update_progress, rqjob


@rqjob
def export_ape(cnx, service_code):
    job = rq.get_current_job()
    taskeid = int(job.id)
    log = logging.getLogger('rq.task')
    rset = cnx.find('Service', code=service_code)
    if not rset:
        log.warning('no service with this code "%s"', service_code)
        return
    allape = []
    current_progress = update_progress(job, 0.)
    xml_rset = cnx.execute(
        'Any X, N, SI, FSPATH(D), NULL, CS '
        'WHERE X findingaid_support F, '
        'X stable_id SI, X name N,  F data D, '
        'X service S, S code CS, '
        'NOT EXISTS(X ape_ead_file AF), F data_format "application/xml", '
        'X service S, S code %(c)s',
        {'c': service_code})
    other_fas = cnx.execute('''
      (Any X, XID WHERE X eadid XID,
       X service S, S code %(c)s,
       NOT EXISTS(X findingaid_support F), NOT EXISTS(X ape_ead_file AF)
    )
    UNION
      (Any X, XID WHERE X eadid XID,
       X service S, S code %(c)s,
       NOT EXISTS(X ape_ead_file AF),
       X findingaid_support F, NOT F data_format "application/xml")
    ''', {'c': service_code})
    all_irs = cnx.execute(
        'Any COUNT(X) WHERE X is FindingAid, X service S, S code %(c)s',
        {'c': service_code})[0][0]
    progress_step = 1. / (all_irs + len(xml_rset) + len(other_fas) + 1)
    print('xml_rset', xml_rset.rowcount)
    if xml_rset:
        for (fa_eid, fa_name, fa_stable_id, fspath,
             ape_ead_fspath, service_code) in xml_rset:
            current_progress = update_progress(job, current_progress + progress_step)
            try:
                generate_ape_ead_xml_from_eids(cnx, [str(fa_eid)])
            except Exception:
                log.exception('failed to export %r (#%s)', fspath, fa_eid)
    print('oai_rset', other_fas.rowcount)
    if other_fas:
        # now generate ape-XML for PDF, dc_based and OAI based finding aids
        for fa in other_fas.entities():
            current_progress = update_progress(job, current_progress + progress_step)
            try:
                generate_ape_ead_other_sources_from_eids(cnx, [str(fa.eid)])
            except Exception:
                log.exception('failed to export ape dump for fa #%s', fa.eid)
                continue
    rset = cnx.execute(
        'Any X, SI, FSPATH(D) WHERE X ape_ead_file F, X stable_id SI, F data D, '
        'X service S, S code %(c)s',
        {'c': service_code})
    print('all rset', rset.rowcount)
    if rset:
        for fa_eid, fa_stable_id, fspath in rset:
            current_progress = update_progress(job, current_progress + progress_step)
            fspath = fspath.getvalue()
            if not(osp.exists(fspath) and osp.isfile(fspath)):
                log.warning('file does not exists %r (#%s)', fspath, fa_eid)
                continue
            allape.append(fspath)
    # group all ape files in zip archive
    appfiles_dir = cnx.vreg.config['appfiles-dir']
    zippath = osp.join(appfiles_dir, 'ape_%s_%s.zip' % (service_code.lower(), taskeid))
    with zipfile.ZipFile(
            zippath, 'w', compression=zipfile.ZIP_DEFLATED,
            allowZip64=True) as z:
        for apepath in allape:
            try:
                z.write(apepath, osp.join(service_code, osp.basename(apepath)))
            except Exception as ex:
                log.warning('%s', ex)
    # compute url and move archive so that nginx can serve it
    sha1 = usha1(open(zippath, 'rb').read())
    basename = osp.basename(zippath)
    finalpath = osp.join(appfiles_dir, '%s_%s' % (sha1, basename))
    shutil.move(zippath, finalpath)
    rqtask = cnx.entity_from_eid(int(job.id))
    rqtask.cw_set(output_descr=(u'Téléchargement du résultat : '
                                u'<a href="{url}">{url}</a>').format(
                                    url=cnx.build_url('file/%s/%s' % (sha1, basename))))
    cnx.commit()

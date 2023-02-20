# flake8: noqa
# -*- coding: utf-8 -*-
#
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
#

import os
import os.path as osp

from cubicweb_francearchives.dataimport import usha1
from cubicweb_frarchives_edition.entities.adapters import copy

print("-> update CSV files, create symlinks")

appdir = cnx.vreg.config["appfiles-dir"]
pub_appfiles_dir = cnx.vreg.config.get("published-appfiles-dir")

for fpath, data_hash, eid in rql(
    """Any FSPATH(FD), DH, FA WHERE FA findingaid_support F, F data FD, F data_hash DH, F data_format ILIKE '%csv'"""
):
    fpath = fpath.getvalue().decode("utf-8")
    if not data_hash:
        with open(fpath, "rb") as f:
            data_hash = usha1(f.read()).encode("utf-8")
    basename = osp.basename(fpath)
    if not basename.startswith(data_hash):
        basename = "{}_{}".format(data_hash, basename)
    destpath = osp.join(appdir, basename)
    if osp.lexists(destpath):
        os.unlink(destpath)
    os.symlink(fpath, destpath)
    entity = cnx.entity_from_eid(eid)
    wf = entity.cw_adapt_to("IWorkflowable")
    if wf and wf.state == "wfs_cmsobject_published":
        published_destpath = osp.join(pub_appfiles_dir, basename)
        copy(fpath, published_destpath)

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

"""cubicweb-frarchives-edition workflow definitions."""

from cubicweb import _


def dataimport_workflow(add_workflow, etype):
    wf = add_workflow("{0} data import workflow".format(etype), etype)
    pending = wf.add_state(_("wfs_faimport_pending"), initial=True)
    running = wf.add_state(_("wfs_faimport_running"))
    completed = wf.add_state(_("wfs_faimport_completed"))
    failed = wf.add_state(_("wfs_faimport_failed"))
    wf.add_transition(_("wft_faimport_run"), (pending,), running)
    wf.add_transition(_("wft_faimport_complete"), (running,), completed)
    wf.add_transition(_("wft_faimport_fail"), (running,), failed)
    return wf


def cmsobject_workflow(add_workflow, etype):
    wf = add_workflow("CMS Object workflow", etype)
    draft = wf.add_state(_("wfs_cmsobject_draft"), initial=True)
    public = wf.add_state(_("wfs_cmsobject_published"))
    wf.add_transition(
        _("wft_cmsobject_publish"), (draft,), public, requiredgroups=("users", "managers")
    )
    wf.add_transition(
        _("wft_cmsobject_unpublish"), (public,), draft, requiredgroups=("users", "managers")
    )
    return wf


def section_workflow(add_workflow, etype):
    wf = add_workflow("Section Object workflow", etype)
    draft = wf.add_state(_("wfs_cmsobject_draft"), initial=True)
    public = wf.add_state(_("wfs_cmsobject_published"))
    wf.add_transition(_("wft_cmsobject_publish"), (draft,), public, requiredgroups=("managers",))
    wf.add_transition(_("wft_cmsobject_unpublish"), (public,), draft, requiredgroups=("managers",))
    return wf

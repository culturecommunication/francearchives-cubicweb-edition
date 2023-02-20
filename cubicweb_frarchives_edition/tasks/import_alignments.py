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

# CubicWeb specific imports

# library specific imports
from cubicweb_frarchives_edition.rq import rqjob
from cubicweb_frarchives_edition import load_leaflet_json
from cubicweb_frarchives_edition.alignments.align import LocationAligner


def auto_run_import(cnx, rqtask, aligner_cls, cw_file):
    """Automatically import alignments.

    XXX : to FIX it seems that  cw_file is the same file as rqtask.output_file, there is no need
    to pass it as agrument

    :param Connection cnx: CubicWeb database connection
    :param RQTask rqtask: current task
    :param classobj aligner_cls: aligner class
    :param File cw_file: output file containing alignments
    """
    subtask = cnx.create_entity(
        "RqTask",
        name="import_alignment",
        title="automatic import-alignment {eid}".format(eid=rqtask.eid),
    )
    # for some raison we can not pass an object in enqueue params, so we pass the filepath
    temp_path = cw_file.get_filepath("data")
    subtask.cw_adapt_to("IRqJob").enqueue(
        import_alignment, temp_path, aligner_cls, override_alignments=False
    )
    return subtask.eid


def update_alignments(cnx, log, temp_path, aligner_cls, override_alignments=False):
    """Update alignments.

    :param Connection cnx: CubicWeb database connection
    :param Logger log: RQTask logger
    :param string temp_path: path to a file
    :param classobj aligner_cls: aligner class
    :param bool override_alignments: toggle overriding user-defined alignments on/off
    """
    aligner = aligner_cls(cnx, log)
    # read-in CSV file and import alignment(s)
    log.info(f"[import alignements]: process '{temp_path}'")
    aligner.process_csvpath(temp_path, override_alignments=override_alignments)
    # reload the map only if modification is found in alignements
    # we should probably make it a daily cron
    if isinstance(aligner, LocationAligner):
        if aligner.modified_alignments:
            log.info("[import alignements]: update IR map")
            load_leaflet_json(cnx)
        else:
            log.info(
                "[import alignements]: do not update IR map as no alignments have been modified."
            )


@rqjob
def import_alignment(cnx, temp_path, aligner_cls, override_alignments=False):
    """Import alignment.

    :param cnx Connection: CubicWeb database connection
    :param string temp_path: file
    :param classobj aligner_cls: aligner class
    :param bool override_alignments: toggle overriding user-defined alignments on/off
    """
    log = logging.getLogger("rq.task")
    update_alignments(cnx, log, temp_path, aligner_cls, override_alignments=override_alignments)

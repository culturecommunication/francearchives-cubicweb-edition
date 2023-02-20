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
# knowledge of the CeCILL license and that you accept its terms.

from .import_ead import import_ead  # noqa
from .export_ape import export_ape  # noqa
from .publish import publish_findingaid  # noqa
from .import_oai import import_oai  # noqa
from .import_csv import import_csv  # noqa
from .import_csv_nomina import import_csv_nomina  # noqa
from .import_eac import import_eac  # noqa
from .compute_alignments import compute_alignments, compute_alignments_all  # noqa
from .import_alignments import import_alignment  # noqa
from .export_authorities import export_authorities  # noqa
from .dedupe_authorities import dedupe_authorities  # noqa
from .group_authorities import compute_location_authorities_to_group  # noqa
from .group_authorities import group_location_authorities  # noqa
from .import_authorities import import_authorities  # noqa
from .delete_findingaids import delete_findingaids  # noqa
from .run_dead_links import run_dead_links  # noqa
from .index_kibana import index_kibana  # noqa
from .qualify_authorities import import_qualified_authorities  # noqa
from .remove_authorities import remove_authorities  # noqa
from .delete_nomina import delete_nomina_by_service  # noqa

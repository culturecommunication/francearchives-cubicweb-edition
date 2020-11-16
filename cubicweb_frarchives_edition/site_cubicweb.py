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
from logilab.common.configuration import REQUIRED
from logilab.database import FunctionDescr

from rql.utils import register_function

options = (
    (
        "consultation-sync-url",
        {
            "type": "string",
            "default": REQUIRED,
            "help": "base to url to post synchronization orders on",
            "group": "sync",
            "level": 2,
        },
    ),
    (
        "published-appfiles-dir",
        {
            "type": "string",
            "default": REQUIRED,
            "help": "directory of appfiles for public instance",
            "group": "sync",
            "level": 2,
        },
    ),
    (
        "published-staticdir-path",
        {
            "type": "string",
            "default": REQUIRED,
            "help": "hero images directory for public instance",
            "group": "sync",
            "level": 2,
        },
    ),
    (
        "published-index-name",
        {
            "type": "string",
            "default": REQUIRED,
            "help": "Name of public Elastic Search index",
            "group": "elasticsearch",
            "level": 2,
        },
    ),
    (
        "tinymce-key",
        {
            "type": "string",
            "default": "z1su7cyfbql76bw5u2un462erbimfg3wasuec8lf5ebo5fmm",
            "help": "https://www.tiny.cloud APIKEY",
            "group": "UI",
            "level": 2,
        },
    ),
    (
        "linkchecker-output",
        {
            "type": "string",
            "default": "/tmp",
            "help": "linkchecker output file name",
            "group": "linkchecker",
            "level": 2,
        },
    ),
    (
        "linkchecker-config",
        {
            "type": "string",
            "default": "",
            "help": "linkchecker configuration file",
            "group": "linkchecker",
            "level": 2,
        },
    ),
    (
        "dead-links-output",
        {
            "type": "string",
            "default": "",
            "help": "dead links output directory",
            "group": "linkchecker",
            "level": 2,
        },
    ),
)


class UNACCENT(FunctionDescr):
    minargs = 1
    maxargs = 1
    supported_backends = ("postgres",)
    rtype = "String"


register_function(UNACCENT)

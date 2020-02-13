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
"""
helpers for alignment
"""

import datetime
from string import punctuation

from nazca.utils.normalize import lunormalize


def simplify(sentence, substitute=None, log=None, debug=False):
    """
    Simply the given sentence
        1/ Normalize a sentence (ie remove accents, set to lower, etc)
        2/ Remove punctuation
    """
    sentence = sentence.strip()
    try:
        sentence = lunormalize(sentence, substitute=substitute)
    except Exception as exception:
        msg = "could not normalize %s : %s"
        if debug:
            if log:
                log.error(msg, repr(sentence), exception)
            else:
                print(msg % (repr(sentence), exception))
    cleansent = "".join([s if s not in punctuation else " " for s in sentence]).strip()
    #  comma followed by a space is replaced by two spaces, keep only one
    cleansent = cleansent.replace("  ", " ")
    return cleansent


def split_up(array, n):
    """Split array into n-sized chunks (generator).

    :param list array: array
    :param int n: chunk size

    :returns: n-sized chunk
    :rtype: list
    """
    for i in range(0, len(array), n):
        yield array[i : i + n]


def strptime(datestr, *formats):
    """New date parsed from string. If more than one format are
    given the first matching format is used, e.g. if '2019-12' and
    '%Y-%d' and '%Y-%m' are given in that order, the resulting
    datetime object is datetime.datetime(2019, 1, 12, 0, 0).

    :param str datestr: string
    :param tuple formats: formats (e.g. '%Y-%m-%d')

    :raises: ValueError if string does not match any of formats

    :returns: datetime object, matching format
    :rtype: datetime, str
    """
    for format_ in formats:
        try:
            return datetime.datetime.strptime(datestr, format_), format_
        except ValueError:
            continue
    raise ValueError(
        "'{datestr}' does not match any of {formats}".format(
            datestr=datestr, formats=", ".join("'{}'".format(format_) for format_ in formats)
        )
    )

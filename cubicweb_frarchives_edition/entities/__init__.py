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

"""cubicweb-frarchives-edition entity's classes"""

import base64

from six import string_types
from six.moves.urllib import parse as urllib_parse

from logilab.common.registry import yes

from cubicweb.entities import AnyEntity, fetch_config

from cubicweb_jsonschema.mappers import CollectionItemMapper


class FACollectionItemMapper(CollectionItemMapper):
    __select__ = CollectionItemMapper.__select__ & yes()

    @staticmethod
    def serialize(entity):
        """Return a dictionary with entity represented as a collection item."""
        return entity.cw_adapt_to('IJSONSchema').serialize()


def parse_dataurl(url):
    """Parse a `Data URL`_ and return (`data`, `mediatype`, `parameters`).

    .. _`Data URL`: http://dataurl.net/
    """
    scheme, data = url.split(':', 1)
    if scheme != 'data':
        raise ValueError('invalid scheme {}'.format(scheme))
    mediatype, data = data.rsplit(',', 1)
    data = urllib_parse.unquote(data)
    if mediatype.endswith(';base64'):
        data = base64.b64decode(data)
        mediatype = mediatype[:-len(';base64')]
    assert isinstance(data, string_types)
    if not mediatype:
        # default media type, according to RFC
        mediatype = 'text/plain;charset=US-ASCII'
    try:
        mediatype, params = mediatype.split(';', 1)
    except ValueError:
        parameters = {}
    else:
        parameters = dict(p.split('=') for p in params.split(';'))
    return data, mediatype, parameters


class RqTask(AnyEntity):
    __regid__ = 'RqTask'
    fetch_attrs, cw_fetch_order = fetch_config(['title',
                                                'name', ])

    def dc_title(self):
        return u'{} ({})'.format(self.title, self.name)

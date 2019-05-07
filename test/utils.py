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

import os
import os.path as osp

from cubicweb import Binary
from cubicweb.devtools import (
    DEFAULT_SOURCES,
    DEFAULT_PSQL_SOURCES,
    ApptestConfiguration,
    PostgresApptestConfiguration,
)
from cubicweb_francearchives.cssimages import static_css_dir


HERE = osp.dirname(osp.abspath(__file__))


DEFAULT_SOURCES['admin']['password'] = \
    DEFAULT_PSQL_SOURCES['admin']['password'] = \
    'pas650RDw$rd'


ApptestConfiguration.anonymous_credential = \
    PostgresApptestConfiguration.anonymous_credential = \
    ('anon', 'a%o8ps650RDw')


class FrACubicConfigMixIn(object):

    @classmethod  # XXX could be turned into a regular method
    def init_config(cls, config):
        config.default_admin_config['password'] = DEFAULT_SOURCES['admin']['password']
        config.anonymous_credential = ApptestConfiguration.anonymous_credential
        super(FrACubicConfigMixIn, cls).init_config(config)


def create_findingaid(cnx, with_file=False):
    faheader = cnx.create_entity('FAHeader')
    did = cnx.create_entity('Did', unittitle=u'unittitle',
                            unitid=u'unitid')
    findingaid = cnx.create_entity('FindingAid', name=u'f',
                                   stable_id=u'f{}'.format(did.eid),
                                   publisher=u'the-publisher',
                                   eadid=u'eadid',
                                   fa_header=faheader,
                                   did=did)
    if with_file:
        cnx.create_entity('File', data_name=u'data',
                          data=Binary('data'),
                          reverse_findingaid_support=findingaid)
    return findingaid


def fafileimport_setdefaults(attrs):
    for name, value in [
        ('dc_title', u'test'),
        ('dc_identifier', u'test-id'),
        ('origination', u'publisher-test'),
    ]:
        attrs.setdefault(name, value)


def create_fafileimport(cnx,  **attrs):
    fafileimport_setdefaults(attrs)
    return cnx.create_entity('FAFileImport', **attrs)


def create_default_commemoitem(cnx, authority=None):
    ce = cnx.create_entity
    return ce('CommemorationItem', title=u'tmp',
              commemoration_year=2019,
              alphatitle=u'alphatitle',
              content=u'content',
              collection_top=ce('CommemoCollection',
                                title=u'collection',
                                year=2016))


def create_default_agent_authority(cnx):
    authority = cnx.create_entity('AgentAuthority', label=u'the-preflabel')
    create_default_commemoitem(cnx, authority=authority)
    return authority


def test_datapath(*path):
    return osp.join(HERE, 'data', *path)


class EsSerializableMixIn(object):
    def setUp(self):
        super(EsSerializableMixIn, self).setUp()
        if 'PIFPAF_ES_ELASTICSEARCH_URL' in os.environ:
            self.config.global_set_option(
                'elasticsearch-locations',
                os.environ['PIFPAF_ES_ELASTICSEARCH_URL']
            )
        else:
            self.config.global_set_option(
                'elasticsearch-locations',
                'http://nonexistant.elastic.search:9200'
            )
        self.config.global_set_option('varnishcli-hosts', '127.0.0.1:6082')
        self.config.global_set_option('varnish-version', 4)
        self.index_name = 'unittest_index_name'
        self.published_index_name = 'unittest_published_index_name'
        self.config.global_set_option('compute-sha1hex', 'yes')
        self.config.global_set_option('index-name', self.index_name)
        self.config.global_set_option('published-index-name',
                                      self.published_index_name)
        self.config.global_set_option('published-appfiles-dir',
                                      self.datapath('published-appfiles-dir'))
        for name in ('published-staticdir-path', 'staticdir-path'):
            path = self.datapath(name)
            self.config.global_set_option(name, path)
            csspath = static_css_dir(path)
            if not osp.exists(csspath):
                os.makedirs(csspath)

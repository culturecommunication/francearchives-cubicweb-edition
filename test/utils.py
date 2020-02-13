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
import sys

import rq
import fakeredis

from cubicweb import Binary
from cubicweb.devtools import (
    DEFAULT_SOURCES,
    DEFAULT_PSQL_SOURCES,
    ApptestConfiguration,
    PostgresApptestConfiguration,
)
from cubicweb.pyramid.test import PyramidCWTest
from cubicweb_francearchives.cssimages import static_css_dir
from cubicweb_francearchives.dataimport import usha1
from cubicweb_frarchives_edition.rq import work


HERE = osp.dirname(osp.abspath(__file__))


DEFAULT_SOURCES["admin"]["password"] = DEFAULT_PSQL_SOURCES["admin"]["password"] = "pas650RDw$rd"


ApptestConfiguration.anonymous_credential = PostgresApptestConfiguration.anonymous_credential = (
    "anon",
    "a%o8ps650RDw",
)


class FrACubicConfigMixIn(object):
    @classmethod  # XXX could be turned into a regular method
    def init_config(cls, config):
        config.default_admin_config["password"] = DEFAULT_SOURCES["admin"]["password"]
        config.anonymous_credential = ApptestConfiguration.anonymous_credential
        super(FrACubicConfigMixIn, cls).init_config(config)


def create_findingaid(cnx, name=None, with_file=False, service=None):
    faheader = cnx.create_entity("FAHeader")
    did = cnx.create_entity("Did", unittitle="unittitle", unitid="unitid")
    name = name or "_FRAD_XXX_"
    findingaid = cnx.create_entity(
        "FindingAid",
        name=name,
        stable_id=usha1(name),
        publisher="the-publisher",
        eadid="eadid",
        fa_header=faheader,
        did=did,
    )
    if service:
        findingaid.cw_set(service=service)
    if with_file:
        cnx.create_entity(
            "File", data_name="data", data=Binary(b"data"), reverse_findingaid_support=findingaid
        )
    return findingaid


def fafileimport_setdefaults(attrs):
    for name, value in [
        ("dc_title", "test"),
        ("dc_identifier", "test-id"),
        ("origination", "publisher-test"),
    ]:
        attrs.setdefault(name, value)


def create_fafileimport(cnx, **attrs):
    fafileimport_setdefaults(attrs)
    return cnx.create_entity("FAFileImport", **attrs)


def create_default_commemoitem(cnx, authority=None):
    ce = cnx.create_entity
    return ce(
        "CommemorationItem",
        title="tmp",
        commemoration_year=2019,
        alphatitle="alphatitle",
        content="content",
        collection_top=ce("CommemoCollection", title="collection", year=2016),
    )


def create_default_agent_authority(cnx):
    authority = cnx.create_entity("AgentAuthority", label="the-preflabel")
    create_default_commemoitem(cnx, authority=authority)
    return authority


def test_datapath(*path):
    return osp.join(HERE, "data", *path)


class EsSerializableMixIn(object):
    def setUp(self):
        super(EsSerializableMixIn, self).setUp()
        if "PIFPAF_ES_ELASTICSEARCH_URL" in os.environ:
            self.config.global_set_option(
                "elasticsearch-locations", os.environ["PIFPAF_ES_ELASTICSEARCH_URL"]
            )
        else:
            self.config.global_set_option(
                "elasticsearch-locations", "http://nonexistant.elastic.search:9200"
            )
        self.config.global_set_option("varnishcli-hosts", "127.0.0.1:6082")
        self.config.global_set_option("varnish-version", 4)
        self.index_name = "unittest_index_name"
        self.published_index_name = "unittest_published_index_name"
        self.config.global_set_option("index-name", self.index_name)
        self.config.global_set_option("published-index-name", self.published_index_name)
        self.config.global_set_option(
            "published-appfiles-dir", self.datapath("published-appfiles-dir")
        )
        for name in ("published-staticdir-path", "staticdir-path"):
            path = self.datapath(name)
            self.config.global_set_option(name, path)
            csspath = static_css_dir(path)
            if not osp.exists(csspath):
                os.makedirs(csspath)


class TaskTC(FrACubicConfigMixIn, PyramidCWTest):
    """Task test cases base class."""

    configcls = PostgresApptestConfiguration
    settings = {
        "cubicweb.bwcompat": False,
        "pyramid.debug_notfound": True,
        "cubicweb.session.secret": "stuff",
        "cubicweb.auth.authtkt.session.secret": "stuff",
        "cubicweb.auth.authtkt.persistent.secret": "stuff",
        "francearchives.autoinclude": "no",
    }

    def setUp(self):
        """Set up job queue and configuration."""
        super(TaskTC, self).setUp()
        self._rq_connection = rq.Connection(fakeredis.FakeStrictRedis())
        self._rq_connection.__enter__()

    def tearDown(self):
        """Clean up job queue."""
        super(TaskTC, self).tearDown()
        self._rq_connection.__exit__(*sys.exc_info())

    def includeme(self, config):
        config.include("cubicweb_frarchives_edition.api")
        config.include("cubicweb_francearchives.pviews")

    def work(self, cnx):
        """Start task.

        :param Connection cnx: CubicWeb database connection
        """
        return work(cnx, burst=True, worker_class=rq.worker.SimpleWorker)

    def _is_executed_successfully(self, cnx, job):
        """Check if task is executed successfully.

        :param Connection cnx: CubicWeb database connection
        :param IRqJob job: job
        """
        self.assertEqual(job.status, "queued")
        self.work(cnx)
        job.refresh()
        self.assertEqual(job.status, "finished")

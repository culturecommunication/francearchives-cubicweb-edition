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
"""cubicweb-frarchives_edition unit tests for entities"""
import fakeredis

from cubicweb import Binary

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin

import utils
import urllib.request
import urllib.parse
import urllib.error


class JSONSchemaAdapterTC(S3BfssStorageTestMixin, utils.FranceArchivesCMSTC):
    def includeme(self, config):
        config.registry.settings["frarchives_edition.rq.redis"] = fakeredis.FakeStrictRedis()
        config.include("cubicweb_frarchives_edition.cms")
        # config.include('cubicweb_frarchives_edition.api')
        config.include("cubicweb_francearchives.pviews.renderer")
        config.include("cubicweb_francearchives.pviews.cwroutes")

    def test_downloadable(self):
        """Ensure we get the download_url member in JSON document."""
        with self.admin_access.cnx() as cnx:
            f = cnx.create_entity("File", data_name="data", data=Binary(b"data"))
            cnx.commit()
            eid = f.eid
            data_sha1hex = f.data_hash
        self.webapp.extra_environ["debug_routematch"] = "true"
        res = self.webapp.get(
            "/file/{0}/".format(eid), status=200, headers={"accept": "application/json"}
        )
        self.assertEqual(
            res.json["download_url"], "/file/{}/data".format(urllib.parse.quote(data_sha1hex))
        )

    def test_state(self):
        with self.admin_access.cnx() as cnx:
            findingaid = utils.create_findingaid(cnx)
            cnx.create_entity(
                "File",
                data_name="data",
                data=Binary(b"data"),
                reverse_findingaid_support=findingaid,
            )
            cnx.commit()
            data = findingaid.cw_adapt_to("IJSONSchema").serialize()
            self.assertEqual(data["workflow_state"], cnx._("wfs_cmsobject_draft"))

    def test_trinfo(self):
        with self.admin_access.cnx() as cnx:
            findingaid = utils.create_findingaid(cnx)
            cnx.create_entity(
                "File",
                data_name="data",
                data=Binary(b"data"),
                reverse_findingaid_support=findingaid,
            )
            cnx.commit()
            iwf = findingaid.cw_adapt_to("IWorkflowable")
            iwf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
            findingaid.cw_clear_all_caches()
            iwf = findingaid.cw_adapt_to("IWorkflowable")
            trinfo = iwf.latest_trinfo()
            data = trinfo.cw_adapt_to("IJSONSchema").serialize()
        self.assertEqual(data["from_state"], "wfs_cmsobject_draft")
        self.assertEqual(data["to_state"], "wfs_cmsobject_published")


if __name__ == "__main__":
    import unittest

    unittest.main()

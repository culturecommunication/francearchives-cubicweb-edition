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
import unittest

from cubicweb.pyramid.test import PyramidCWTest

from utils import FrACubicConfigMixIn


class CommemoLinkTests(FrACubicConfigMixIn, PyramidCWTest):

    settings = {
        "cubicweb.bwcompat": False,
        "cubicweb.auth.authtkt.session.secret": "top secret",
        "pyramid.debug_notfound": True,
        "cubicweb.session.secret": "stuff",
        "cubicweb.auth.authtkt.persistent.secret": "stuff",
        "francearchives.autoinclude": "no",
    }

    def includeme(self, config):
        config.include("cubicweb_jsonschema.api.schema")
        config.include("cubicweb_jsonschema.api.entities")

    def setup_database(self):
        with self.admin_access.cnx() as cnx:
            coll = cnx.create_entity("CommemoCollection", title="the-commemo", year=2016)
            sect1 = cnx.create_entity("Section", title="sect-1", reverse_children=coll)
            sect2 = cnx.create_entity("Section", title="sect-2")
            cnx.commit()
        self.coll = coll.eid
        self.sect_coll = sect1.eid
        self.sect_nocoll = sect2.eid

    def addrel_links(self, path, status=200, **kwargs):
        response = self.webapp.get(
            path, status=status, headers={"accept": "application/schema+json"}, **kwargs
        )
        if status != 200:
            return None
        schema = response.json
        return [l["href"] for l in schema["links"] if l["rel"] == "related.children"]

    def test_commemocollection_addlink(self):
        coll = self.coll
        self.login()
        links = self.addrel_links("/commemocollection/{}/schema".format(coll))
        self.assertIn(
            "/commemocollection/{}/relationships/"
            "children?target_type=CommemorationItem".format(coll),
            links,
        )

    def test_section_in_commemocollection_addlink(self):
        sect = self.sect_coll
        self.login()
        links = self.addrel_links("/section/{}/schema".format(sect))
        self.assertIn(
            "/section/{}/relationships/" "children?target_type=CommemorationItem".format(sect),
            links,
        )

    def test_section_no_commemocollection_addlink(self):
        sect = self.sect_nocoll
        self.login()
        links = self.addrel_links("/section/{}/schema".format(sect))
        self.assertNotIn(
            "/section/{}/relationships/" "children?target_type=CommemorationItem".format(sect),
            links,
        )

    def test_section_add_basecontent(self):
        sect = self.sect_coll
        self.login()
        links = self.addrel_links("/section/{}/schema".format(sect))
        self.assertIn(
            "/section/{}/relationships/" "children?target_type=BaseContent".format(sect), links
        )


if __name__ == "__main__":
    unittest.main()

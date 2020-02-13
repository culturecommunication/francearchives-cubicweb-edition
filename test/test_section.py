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

from cubicweb.devtools import testlib

from cubicweb_frarchives_edition.entities.section import move_child

from utils import FrACubicConfigMixIn


class SectionTC(FrACubicConfigMixIn, testlib.CubicWebTC):
    def test_update_order(self):
        with self.admin_access.cnx() as cnx:
            children = []
            for i in range(10):
                children.append(cnx.create_entity("BaseContent", order=i, title="base"))
            s = cnx.create_entity("Section", title="section")
            s.cw_set(children=children)
            cnx.commit()
            child = children[5]
            move_child(cnx, s.eid, child.eid, 3)
            cnx.commit()
        children.remove(child)
        children.insert(3, child)
        children = [[c.eid, idx] for idx, c in enumerate(children)]
        with self.admin_access.cnx() as cnx:
            rset = cnx.execute(
                "Any B, O ORDERBY O WHERE S is Section, S eid %(s)s, " "S children B, B order O",
                {"s": s.eid},
            )
            self.assertEqual(rset.rows, children)


if __name__ == "__main__":
    from logilab.common.testlib import unittest_main

    unittest_main()

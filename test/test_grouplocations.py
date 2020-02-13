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

from utils import FrACubicConfigMixIn

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_frarchives_edition.alignments.group_locations import process_candidates, Label


class GroupLocationsTC(FrACubicConfigMixIn, CubicWebTC):
    """Group locations task test cases."""

    def test_candidates(self):
        """Test candidates are sorted by label, and score calculated as below:

        1/ department, country
        2/ country, department
        3/ department, region, country
        4/ country, region, department
        5/ other combinaisons avec country, region, department
        6/ department
        7/ region, country
        8/ country, region
        """
        with self.admin_access.cnx() as cnx:
            candidates = [
                Label(
                    cnx,
                    "Rochefort (Charente-Maritime)",
                    18354267,
                    1,
                    0,
                    0,
                    "rochefort",
                    "charente maritime",
                    "",
                    "",
                ),
                Label(
                    cnx,
                    "Rochefort-sur-Mer (Charente-Maritime, France)",
                    30326760,
                    1,
                    0,
                    2,
                    "rochefort sur mer",
                    "charente maritime",
                    "",
                    "france",
                ),
                Label(
                    cnx,
                    "Rochefort (Charente-Maritime, France)",
                    130944149,
                    1,
                    0,
                    2,
                    "rochefort",
                    "charente maritime",
                    "",
                    "france",
                ),
                Label(
                    cnx,
                    "Rochefort (France)",
                    130944150,
                    0,
                    0,
                    2,
                    "rochefort",
                    "charente maritime",
                    "",
                    "france",
                ),
            ]
            to_be_grouped, not_to_be_grouped = process_candidates(candidates)
            for label_to, other_labels in list(to_be_grouped.items()):
                self.assertEqual("rochefort", label_to)
                self.assertEqual([130944149, 18354267, 130944150], [o.eid for o in other_labels])

            for label_to, other_labels in list(not_to_be_grouped.items()):
                self.assertEqual("rochefort sur mer", label_to)
                self.assertEqual(30326760, other_labels[0].eid)
                self.assertEqual(
                    [18354267, 130944149, 130944150], sorted([o.eid for o in other_labels[1:]])
                )

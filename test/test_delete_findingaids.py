# -*- coding: utf-8 -*-
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


# standard library imports
import json

# third party imports
# CubicWeb specific imports
# library specific imports
from pgfixtures import setup_module, teardown_module  # noqa
from utils import TaskTC


class DeleteFindingAidsTC(TaskTC):
    """Delete finding aids test cases."""

    def setup_database(self):
        """Set up database."""
        super().setup_database()
        with self.admin_access.repo_cnx() as cnx:
            publisher = "foobar"
            cnx.create_entity(
                "FindingAid",
                name="foo",
                eadid="0123",
                publisher=publisher,
                fa_header=cnx.create_entity("FAHeader"),
                did=cnx.create_entity("Did", unittitle="foo"),
                stable_id="foo0123",
            )
            cnx.create_entity(
                "FindingAid",
                name="bar",
                eadid="1234",
                publisher=publisher,
                fa_header=cnx.create_entity("FAHeader"),
                did=cnx.create_entity("Did", unittitle="bar"),
                stable_id="bar1234",
            )
            cnx.create_entity(
                "FindingAid",
                name="baz",
                eadid="1234",
                publisher=publisher,
                fa_header=cnx.create_entity("FAHeader"),
                did=cnx.create_entity("Did", unittitle="baz"),
                stable_id="baz2345",
            )
            cnx.commit()

    def test_delete_finding_aids(self):
        """Test deleting finding aids.

        Trying: deleting existing finding aids
        Expecting: finding aids are deleted
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            filename = "delete-finding-aids.csv"
            data = json.dumps(
                {"name": "delete_finding_aids", "file": filename, "title": "delete_finding_aids"}
            )
            upload_files = [
                ("fileobj", filename, b"\n".join([b"identifier", b"foo0123", b"bar1234"]))
            ]
            post_kwargs = {"params": [("data", data)], "upload_files": upload_files}
            self.webapp.post(
                "/RqTask/?schema_type=delete_finding_aids",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            self.assertEqual(cnx.execute("Any X WHERE X is FindingAid").rowcount, 3)
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "delete_finding_aids")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # finding aids have been deleted
            self.assertTrue(cnx.execute("Any X WHERE X is FindingAid").one())

    def test_delete_nonexisting(self):
        """Test deleting nonexisting finding aids.

        Trying: deleting nonexisting finding aid
        Expecting: existing finding aid has been deleted
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            filename = "delete-finding-aids.csv"
            data = json.dumps(
                {"name": "delete_finding_aids", "file": filename, "title": "delete_finding_aids"}
            )
            upload_files = [
                (
                    "fileobj",
                    filename,
                    b"\n".join([b"stable_id", b"foobar3456", b"baz2345"])
                )
            ]
            post_kwargs = {"params": [("data", data)], "upload_files": upload_files}
            self.webapp.post(
                "/RqTask/?schema_type=delete_finding_aids",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            self.assertEqual(cnx.execute("Any X WHERE X is FindingAid").rowcount, 3)
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "delete_finding_aids")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # finding aids have been deleted
            stable_ids = ["foo0123", "bar1234"]
            self.assertCountEqual(
                [row[0] for row in cnx.execute("Any S WHERE X is FindingAid, X stable_id S")],
                stable_ids,
            )

    def test_delete_eadid(self):
        """Test deleting finding aid.

        Trying: deleting finding aid
        Expecting: all finding aids having same eadid have been deleted
        """
        with self.admin_access.cnx() as cnx:
            self.login()
            filename = "delete-finding-aids.csv"
            data = json.dumps(
                {"name": "delete_finding_aids", "file": filename, "title": "delete_finding_aids"}
            )
            upload_files = [
                ("fileobj", filename, b"\n".join([b"eadid", b"1234"]))
            ]
            post_kwargs = {"params": [("data", data)], "upload_files": upload_files}
            self.webapp.post(
                "/RqTask/?schema_type=delete_finding_aids",
                status=201,
                headers={"Accept": "application/json"},
                **post_kwargs
            )
            self.assertEqual(cnx.execute("Any X WHERE X is FindingAid").rowcount, 3)
            # task is executed successfully
            task = cnx.find("RqTask").one()
            self.assertEqual(task.name, "delete_finding_aids")
            job = task.cw_adapt_to("IRqJob")
            self._is_executed_successfully(cnx, job)
            # finding aids have been deleted
            self.assertEqual(cnx.execute("Any X WHERE X is FindingAid").rowcount, 1)

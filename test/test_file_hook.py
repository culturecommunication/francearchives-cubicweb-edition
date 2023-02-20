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
"""cubicweb-frarchives_edition unit tests for files hooks"""

from io import BytesIO
import os.path as osp
import shutil
from PIL import Image
import unittest

from cubicweb import Binary
from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration

from cubicweb_francearchives import S3_ACTIVE
from cubicweb_francearchives.testutils import PostgresTextMixin, S3BfssStorageTestMixin

from cubicweb_frarchives_edition import SUBJECT_IMAGE_SIZE

from utils import FrACubicConfigMixIn, create_findingaid
from pgfixtures import setup_module, teardown_module  # noqa


class FileHooksTC(S3BfssStorageTestMixin, PostgresTextMixin, FrACubicConfigMixIn, CubicWebTC):
    configcls = PostgresApptestConfiguration

    @classmethod
    def init_config(cls, config):
        super(FileHooksTC, cls).init_config(config)
        config.set_option("published-appfiles-dir", cls.datapath("tmp/published"))

    def tearDown(self):
        """Tear down test cases."""
        super(FileHooksTC, self).tearDown()
        appdir = self.config["published-appfiles-dir"]
        if osp.exists(appdir):
            shutil.rmtree(appdir)

    def get_published_fkey(self, cnx, fobj, fkey):
        if self.s3_bucket_name:
            return fkey
        else:
            pub_appfiles_dir = fobj.cw_adapt_to("IFileSync").pub_appfiles_dir
            return osp.join(pub_appfiles_dir.encode("utf-8"), osp.basename(fkey))

    def _create_data(self, cnx):
        fobj = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="Disparus#_Algérie_27 mars.pdf",
            data_format="application/pdf",
        )
        bc1 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
<p>
<h1>bc</h1>
<a href="%s">file.pdf</a>
</p>"""
            % fobj.cw_adapt_to("IDownloadable").download_url(),
        )
        bc2 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
            <a href="%s">file.pdf</a>
            """
            % fobj.cw_adapt_to("IDownloadable").download_url(),
        )
        cnx.commit()
        return fobj, bc1, bc2

    def _create_same_data_different_paths(self, cnx):
        fobj1 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="file1.pdf",
            data_format="application/pdf",
        )
        bc1 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
<p>
<h1>bc</h1>
<a href="%s">file.pdf</a>
</p>"""
            % fobj1.cw_adapt_to("IDownloadable").download_url(),
        )
        fobj2 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="file2.pdf",
            data_format="application/pdf",
        )
        bc2 = cnx.create_entity(
            "BaseContent",
            title="bc",
            content="""\
            <a href="%s">file.pdf</a>
            """
            % fobj2.cw_adapt_to("IDownloadable").download_url(),
        )
        cnx.commit()
        return fobj1, bc1, fobj2, bc2

    def _create_circular(self, cnx):
        fobj = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="Disparus#_Algérie_27 mars.pdf",
            data_format="application/pdf",
        )
        circular = cnx.create_entity(
            "Circular",
            circ_id="c1_AB_EC",
            status="in-effect",
            title="c1",
            attachment=fobj,
        )
        cnx.commit()
        return circular

    def _create_card(self, cnx):
        fobj = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="Disparus#_Algérie_27 mars.pdf",
            data_format="application/pdf",
        )
        card = cnx.create_entity(
            "Card",
            title="test",
            wikiid="test",
            content="""\
            <a href="%s">file.pdf</a>
            """
            % fobj.cw_adapt_to("IDownloadable").download_url(),
        )
        cnx.commit()
        return fobj, card

    @unittest.skipIf(S3_ACTIVE, "BFSS specific tests, skipping when S3 activated")
    def test_file_storage_paths(self):
        """
        Trying: create a new file
        Expecting: check various paths and download urls
        """
        with self.admin_access.cnx() as cnx:
            circular = self._create_circular(cnx)
            fobj = circular.attachment[0]
            self.assertEqual(cnx.vreg.config["appfiles-dir"], "/tmp")
            expected_bfss_relpath = (
                "87b2b0e8c632bfbace95bf693f8e99f4acc92eca_Disparus#_Algérie_27 mars.pdf"  # noqa
            )
            self.assertEqual(expected_bfss_relpath, fobj.bfss_storage_relpath("data"))
            # check download_path
            fpath = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][
                0
            ].getvalue()
            # code of cwroutes.bfss_download_view
            bfss_download_path = osp.join(
                cnx.vreg.config["appfiles-dir"], f"{fobj.data_hash}_{fobj.name}"
            )
            expected_fbfss_path = b"/tmp/87b2b0e8c632bfbace95bf693f8e99f4acc92eca_Disparus#_Alg\xc3\xa9rie_27 mars.pdf"  # noqa
            self.assertEqual(expected_fbfss_path, fpath)
            self.assertEqual(expected_fbfss_path.decode(), bfss_download_path)

    def test_file_download_paths(self):
        """
        Trying: create a new file
        Expecting: check download_path
        """
        with self.admin_access.cnx() as cnx:
            circular = self._create_circular(cnx)
            fobj = circular.attachment[0]
            self.assertEqual(cnx.vreg.config["appfiles-dir"], "/tmp")
            expected_url = "http://testing.fr/cubicweb/file/87b2b0e8c632bfbace95bf693f8e99f4acc92eca/Disparus%23_Alg%C3%A9rie_27%20mars.pdf"  # noqa
            url = fobj.cw_adapt_to("IDownloadable").download_url()
            self.assertEqual(expected_url, url)

    def test_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File
        Expecting: File references the both BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            self.assertCountEqual(
                [f.eid for f in fobj.reverse_referenced_files], [bc1.eid, bc2.eid]
            )
            self.assertTrue(self.fileExists(fkey))

    def test_referenced_files_empty(self):
        """
        Trying: create a Card referencing a File and remove the content
        Expecting: referenced File is removed
        """
        with self.admin_access.cnx() as cnx:
            fobj, card = self._create_card(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            self.assertTrue(cnx.find("File", eid=fobj.eid))
            self.assertTrue(self.fileExists(fkey))
            card.cw_set(content=None)
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(self.fileExists(fkey))

    def test_delete_one_referenced_file(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove one of the reference
        Expecting: File references only one BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            bc2.cw_set(content="remove file")
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            self.assertCountEqual([f.eid for f in fobj.reverse_referenced_files], [bc1.eid])
            self.assertTrue(self.fileExists(fkey))

    def test_delete_all_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove both references
        Expecting: file no longer exists
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            bc1.cw_set(content="remove file")
            bc2.cw_set(content="remove file")
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(self.fileExists(fkey))

    def test_delete_one_entity_with_referenced_file(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove one of BaseContent
        Expecting: file references only one BaseContent
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            bc2.cw_delete()
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            self.assertCountEqual([f.eid for f in fobj.reverse_referenced_files], [bc1.eid])
            self.assertTrue(self.fileExists(fkey))

    def test_delete_all_entities_with_referenced_files(self):
        """
        Trying: create two BaseContents referencing the same File and then
                remove both references
        Expecting: file no longer exists
        """
        with self.admin_access.cnx() as cnx:
            fobj, bc1, bc2 = self._create_data(cnx)
            fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X is File")[0][0].getvalue()
            bc1.cw_delete()
            bc2.cw_delete()
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(self.fileExists(fkey))

    def test_delete_one_file_different_paths(self):
        """
        Trying: create two BaseContents referencing two Files with the
                same content but different names. Remove one of files.
        Expecting: the other file is still present
        """
        with self.admin_access.cnx() as cnx:
            fobj1, bc1, fobj2, bc2 = self._create_same_data_different_paths(cnx)
            fkey1 = cnx.execute(
                f"""Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s""", {"e": fobj1.eid}
            )[0][0].getvalue()
            fkey2 = cnx.execute(
                f"""Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s""", {"e": fobj2.eid}
            )[0][0].getvalue()
            self.assertNotEqual(fkey1, fkey2)
            self.assertEqual(fobj1.data_hash, fobj2.data_hash)
            fobj1.cw_delete()
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj1.eid))
            self.assertFalse(self.fileExists(fkey1))
            cnx.find("File", eid=fobj2.eid).one()
            self.assertTrue(self.fileExists(fkey2))

    def test_publish_entity_with_referenced_files(self):
        """
        Trying: create a BaseContent referencing a file then publish and depublish it
        Expecting : only one file is created
        """
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            bc = cnx.create_entity(
                "BaseContent",
                title="bc",
                content="""\
                <p>
<h1>bc</h1>
                <a href="%s">file.pdf</a>
                </p>"""
                % fobj.cw_adapt_to("IDownloadable").download_url(),
            )
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)
            bc = cnx.entity_from_eid(bc.eid)
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)
        with self.admin_access.cnx() as cnx:
            bc = cnx.entity_from_eid(bc.eid)
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            files = cnx.execute("Any X WHERE X is File")
            self.assertEqual(1, len(files))
            self.assertEqual(fobj.eid, files.one().eid)

    def test_delete_referenced_file_in_published_circular(self):
        """
        Trying: create a Circular referencing a file then publish it and delete the file
        Expecting : the fkey is no more referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            circular = cnx.create_entity(
                "Circular",
                circ_id="c1_AB_EC",
                status="in-effect",
                title="c1",
                attachment=fobj,
            )
            cnx.commit()
            fkey = cnx.execute(
                f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid}
            )[0][0].getvalue()
            self.assertTrue(self.fileExists(fkey))
            cnx.entity_from_eid(circular.eid).cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_publish"
            )
            cnx.commit()
            self.assertTrue(self.fileExists(fkey))
            published_fkey = self.get_published_fkey(cnx, fobj, fkey)
            self.assertTrue(self.fileExists(published_fkey))
            cnx.find("File", eid=fobj.eid).one().cw_delete()
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj.eid))
            self.assertFalse(self.fileExists(fkey))
            self.assertFalse(self.fileExists(published_fkey))

    def test_file_in_published_entities_fa_referenced_files(self):
        """
        Trying: reference a same file in a BaseContent's content
                and as fa_referenced_files of an FindingAid
                publish both, then unpublish the FindingAid
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            fobj1 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            cnx.commit()
            bc = cnx.create_entity(
                "BaseContent",
                title="bc1",
                content="""\
                <p><h1>bc</h1>
                <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
                </p>"""
                % fobj1.cw_adapt_to("IDownloadable").download_url(),
            )
            fa = create_findingaid(cnx, name=None)
            cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
                reverse_fa_referenced_files=fa,
            )
            cnx.commit()
            for eid in (bc.eid, fa.eid):
                cnx.entity_from_eid(eid).cw_adapt_to("IWorkflowable").fire_transition(
                    "wft_cmsobject_publish"
                )
            cnx.commit()
            cnx.find("FindingAid", eid=fa.eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            fkey = cnx.execute(
                f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid}
            )[0][0].getvalue()
            self.assertTrue(self.fileExists(fkey))
            published_fkey = self.get_published_fkey(cnx, fobj1, fkey)
            self.assertTrue(self.fileExists(published_fkey))

    def test_file_in_published_entities_image_file(self):
        """
        Trying: reference a same file in a BaseContent's content
                and as image_file of an other BaseContent
                publish both, then unpublish the first BaseContent
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            fobj1 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            # TODO - with S3, is it a bug that creating two files
            # in single transaction fails ?
            cnx.commit()
            fobj2 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            cnx.commit()
            bc1 = cnx.create_entity(
                "BaseContent",
                title="bc1",
                content="""\
                <p><h1>bc</h1>
                <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
                </p>"""
                % fobj1.cw_adapt_to("IDownloadable").download_url(),
            )
            bc2 = cnx.create_entity(
                "BaseContent",
                title="bc1",
                content="toto",
                basecontent_image=cnx.create_entity(
                    "Image", caption="image-caption", image_file=fobj2
                ),
            )
            cnx.commit()
            for eid in (bc1.eid, bc2.eid):
                cnx.entity_from_eid(eid).cw_adapt_to("IWorkflowable").fire_transition(
                    "wft_cmsobject_publish"
                )
            cnx.commit()
            cnx.find("BaseContent", eid=bc1.eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            fkey = cnx.execute("Any fspath(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[0][
                0
            ].getvalue()
            self.assertTrue(self.fileExists(fkey))
            published_fkey = self.get_published_fkey(cnx, fobj1, fkey)
            self.assertTrue(self.fileExists(published_fkey))

    def create_and_test_fkey_in_published_entities(self, cnx):
        fobj1 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="Disparus#_Algérie_27 mars.pdf",
            data_format="application/pdf",
        )
        # TODO s3 bug when adding twice a file in same transaction ?
        cnx.commit()
        fobj2 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="Disparus#_Algérie_27 mars.pdf",
            data_format="application/pdf",
        )
        cnx.commit()
        bc = cnx.create_entity(
            "BaseContent",
            title="bc1",
            content="""\
            <p><h1>bc</h1>
            <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
            </p>"""
            % fobj1.cw_adapt_to("IDownloadable").download_url(),
        )
        circular = cnx.create_entity(
            "Circular",
            circ_id="c1_AB_EC",
            status="in-effect",
            title="c1",
            attachment=fobj2,
        )
        cnx.commit()
        fkey1 = cnx.execute(
            f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid}
        )[0][0].getvalue()
        fkey2 = cnx.execute(
            f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj2.eid}
        )[0][0].getvalue()
        self.assertEqual(fkey1, fkey2)
        self.assertTrue(self.fileExists(fkey1))
        fobj1 = cnx.find("File", eid=fobj1.eid).one()
        fobj2 = cnx.find("File", eid=fobj2.eid).one()
        # publish content
        cnx.entity_from_eid(bc.eid).cw_adapt_to("IWorkflowable").fire_transition(
            "wft_cmsobject_publish"
        )
        cnx.entity_from_eid(circular.eid).cw_adapt_to("IWorkflowable").fire_transition(
            "wft_cmsobject_publish"
        )
        cnx.commit()
        self.assertTrue(self.fileExists(fkey1))
        # assert fkey and published_fkey exist
        published_fkey = self.get_published_fkey(cnx, fobj1, fkey1)
        self.assertTrue(self.fileExists(published_fkey))
        return (fobj1.eid, fobj2.eid, bc.eid, circular.eid, fkey1, published_fkey)

    def test_file_in_published_entities_remove_one_fpath(self):
        """
        Trying: reference a same file in a BaseContent and a Circular,
                publish both delete the referenced fkey from BaseContent
        Expecting: the fkey is still referenced by a published Circular
                   and exists in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc_eid,
                circular_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_entities(cnx)
            # remove fobj1 reference from the BaseContent
            cnx.entity_from_eid(bc_eid).cw_set(content="remove file")
            cnx.commit()
            # fobj1 is deleted
            self.assertFalse(cnx.find("File", eid=fobj1_eid))
            self.assertTrue(cnx.find("File", eid=fobj2_eid))
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_file_in_published_entities_unpublish_base_content(self):
        """
        Trying: reference a same file in a BaseContent and a Circular,
                publish both, then unpublish the BaseContent
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc_eid,
                circular_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_entities(cnx)
            # unpublish the BaseContent
            cnx.find("BaseContent", eid=bc_eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            cnx.find("File", eid=fobj1_eid).one()
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_file_in_published_entities_unpublish_cicular(self):
        """
        Trying: reference a same file in a BaseContent and a Circular,
                publish both, then unpublish the Circular
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc_eid,
                circular_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_entities(cnx)
            # unpublish the Circular
            cnx.find("Circular", eid=circular_eid).one().cw_adapt_to(
                "IWorkflowable"
            ).fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            cnx.find("File", eid=fobj1_eid).one()
            cnx.find("File", eid=fobj2_eid).one()
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_file_in_published_entities_unpublish_both(self):
        """
        Trying: reference a same file in a BaseContent and a Circular,
                publish both, then unpublish both the BaseContent & Circular
        Expecting: the fkey is no more referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc_eid,
                circular_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_entities(cnx)
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))
            # unpublish the BaseContent
            cnx.find("BaseContent", eid=bc_eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.find("Circular", eid=circular_eid).one().cw_adapt_to(
                "IWorkflowable"
            ).fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            cnx.find("File", eid=fobj1_eid).one()
            cnx.find("File", eid=fobj2_eid).one()
            if S3_ACTIVE:
                fkey = ".hidden/" + fkey.decode("utf-8")
            self.assertTrue(self.fileExists(fkey))
            self.assertFalse(self.fileExists(published_fkey))

    def test_link_referenced_files(self):
        """
        Trying: reference a same file in two BaseContents the way they are created in UI,
                e.g by creating two distinct CWFiles. Also create a CWFiles referencing
                the same file and link it to a Circular
        Expecting : only two CWFiles are kept: one CWfile referencing both BaseContents and
                    the other linked to the Circular. The fkey still exists
        """
        with self.admin_access.cnx() as cnx:
            file_name = "Disparus#_Algérie_27 mars.pdf"
            fobj1 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name=file_name,
                data_format="application/pdf",
            )
            cnx.commit()
            fobj2 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name=file_name,
                data_format="application/pdf",
            )
            cnx.commit()
            fkey = cnx.execute(
                f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid}
            )[0][0].getvalue()
            fobj3 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name=file_name,
                data_format="application/pdf",
                reverse_attachment=cnx.create_entity(
                    "Circular", circ_id="c1", status="revoked", title="c1"
                ),
            )
            cnx.commit()
            for fobj in (fobj1, fobj2, fobj3):
                _fkey = cnx.execute(
                    f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid}
                )[0][0].getvalue()
                self.assertEqual(_fkey, fkey)
            self.assertTrue(self.fileExists(fkey))
            cnx.find("File", eid=fobj1.eid)
            cnx.find("File", eid=fobj2.eid)
            cnx.find("File", eid=fobj3.eid)
            bc1 = cnx.create_entity(
                "BaseContent",
                title="bc1",
                content="""\
                <p><h1>bc</h1>
                <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
                </p>"""
                % fobj1.cw_adapt_to("IDownloadable").download_url(),
            )
            bc2 = cnx.create_entity(
                "BaseContent",
                title="bc2",
                content="""\
                <p><h1>bc</h1>
                <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
                </p>"""
                % fobj2.cw_adapt_to("IDownloadable").download_url(),
            )

            cnx.commit()
            self.assertTrue(self.fileExists(fkey))
            # the file must still exists
            fobj1 = cnx.entity_from_eid(fobj1.eid)
            self.assertCountEqual(fobj1.reverse_referenced_files, (bc1, bc2))
            self.assertFalse(cnx.find("File", eid=fobj2.eid))
            self.assertTrue(cnx.find("File", eid=fobj3.eid))

    def create_and_test_fkey_in_published_basecontents(self, cnx):
        fobj1 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="23oi@~{É LO[à.pdf",
            data_format="application/pdf",
        )
        cnx.commit()
        fobj2 = cnx.create_entity(
            "File",
            data=Binary(b"some-file-data"),
            data_name="23oi@~{É LO[à.pdf",
            data_format="application/pdf",
        )
        cnx.commit()
        bc1 = cnx.create_entity(
            "BaseContent",
            title="bc1",
            content="""\
            <p><h1>bc</h1>
            <a href="%s">23oi@~{É LO[à.pdf</a>
            </p>"""
            % fobj1.cw_adapt_to("IDownloadable").download_url(),
        )
        bc2 = cnx.create_entity(
            "BaseContent",
            title="bc1",
            content="""\
            <p><h1>bc</h1>
            <a href="%s">23oi@~{É LO[à.pdf</a>
            </p>"""
            % fobj1.cw_adapt_to("IDownloadable").download_url(),
        )
        cnx.commit()
        # check file doesn't exist since it is deduplicated using data_hash
        self.assertFalse(cnx.find("File", eid=fobj2.eid))
        fobj1 = cnx.find("File", eid=fobj1.eid).one()
        self.assertCountEqual(fobj1.reverse_referenced_files, (bc1, bc2))
        fkey = cnx.execute(f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj1.eid})[
            0
        ][0].getvalue()
        self.assertTrue(self.fileExists(fkey))
        # publish content
        cnx.entity_from_eid(bc1.eid).cw_adapt_to("IWorkflowable").fire_transition(
            "wft_cmsobject_publish"
        )
        cnx.entity_from_eid(bc2.eid).cw_adapt_to("IWorkflowable").fire_transition(
            "wft_cmsobject_publish"
        )
        cnx.commit()
        # assert fkey and published_fkey exist
        self.assertTrue(self.fileExists(fkey))
        published_fkey = self.get_published_fkey(cnx, fobj1, fkey)
        self.assertTrue(self.fileExists(published_fkey))
        return (fobj1.eid, fobj2.eid, bc1.eid, bc2.eid, fkey, published_fkey)

    def test_referenced_file_in_published_contents_remove_one_fpath(self):
        """
        Trying: reference a same file in two BaseContent,
                publish both and remove the fkey from one of BaseContents's content
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            # remove the filepath from one of BaseContent
            cnx.find("BaseContent", eid=bc1_eid).one().cw_set(content=None)
            cnx.commit()
            # fkey and published_fkey still exist
            fobj1 = cnx.find("File", eid=fobj1_eid).one()
            self.assertCountEqual(
                [e.eid for e in fobj1.reverse_referenced_files],
                [
                    bc2_eid,
                ],
            )
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))
            # remove the filepath from one of BaseContent
            cnx.find("BaseContent", eid=bc2_eid).one().cw_set(content=None)
            cnx.commit()

    def test_referenced_file_in_published_contents_remove_all_fpath(self):
        """
        Trying: reference a same file in two BaseContent,
                publish both and remove the fkey from both BaseContents's content
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            # remove the filepath from one of BaseContent
            cnx.find("BaseContent", eid=bc1_eid).one().cw_set(content=None)
            cnx.commit()
            # remove the filepath from the other BaseContent
            cnx.find("BaseContent", eid=bc2_eid).one().cw_set(content=None)
            cnx.commit()
            self.assertFalse(cnx.find("File", eid=fobj1_eid))
            self.assertFalse(cnx.entity_from_eid(bc1_eid).referenced_files)
            self.assertFalse(cnx.entity_from_eid(bc2_eid).referenced_files)
            # fkey and published_fkey no more exist
            self.assertFalse(self.fileExists(fkey))
            self.assertFalse(self.fileExists(published_fkey))

    def test_referenced_file_in_published_contents_delete_one_content(self):
        """
        Trying: reference a same file in two BaseContent,
                publish both and delete one of BaseContent
        Expecting: the fkey is not more referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            # remove bc1
            cnx.find("BaseContent", eid=bc1_eid).one().cw_delete()
            cnx.commit()
            # fkey and published_fkey still exist
            fobj1 = cnx.find("File", eid=fobj1_eid).one()
            self.assertCountEqual(
                [e.eid for e in fobj1.reverse_referenced_files],
                [
                    bc2_eid,
                ],
            )
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_referenced_file_in_published_contents_delete_both_content(self):
        """
        Trying: reference a same file in two BaseContent,
                publish both and delete both  BaseContent
        Expecting: the fkey is no more referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            # remove bc1
            cnx.find("BaseContent", eid=bc1_eid).one().cw_delete()
            cnx.commit()
            cnx.find("BaseContent", eid=bc2_eid).one().cw_delete()
            cnx.commit()
            # fkey and published_fkey no more exist
            self.assertFalse(cnx.find("File", eid=fobj1_eid))
            self.assertFalse(self.fileExists(fkey))
            self.assertFalse(self.fileExists(published_fkey))

    def test_referenced_file_in_published_contents_unpublish_one(self):
        """
        Trying: reference a same file in tow BaseContent,
                publish both and unpublish one of BaseContent
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                fobj2_eid,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            # remove bc1
            cnx.find("BaseContent", eid=bc1_eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            # fkey and published_fkey still exist
            fobj1 = cnx.find("File", eid=fobj1_eid).one()
            self.assertCountEqual(
                [e.eid for e in fobj1.reverse_referenced_files],
                [
                    bc1_eid,
                    bc2_eid,
                ],
            )
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_referenced_file_in_published_contents_unpublish_both(self):
        """
        Trying: reference a same file in two BaseContent,
                publish both and unpublish both of BaseContent
        Expecting: the fkey is still referenced in the published directory
        """
        with self.admin_access.cnx() as cnx:
            (
                fobj1_eid,
                _,
                bc1_eid,
                bc2_eid,
                fkey,
                published_fkey,
            ) = self.create_and_test_fkey_in_published_basecontents(cnx)
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))
            # remove bc1
            cnx.find("BaseContent", eid=bc1_eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            cnx.find("BaseContent", eid=bc2_eid).one().cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            # fkey and published_fkey no more exist
            fobj1 = cnx.find("File", eid=fobj1_eid).one()
            self.assertCountEqual(
                [e.eid for e in fobj1.reverse_referenced_files],
                [
                    bc1_eid,
                    bc2_eid,
                ],
            )
            self.assertFalse(self.fileExists(published_fkey))
            if S3_ACTIVE:
                fkey = ".hidden/" + fkey.decode("utf-8")
            self.assertTrue(self.fileExists(fkey))

    def test_publish_unpublish_twice(self):
        """
        Trying: reference a file in a BaseContent's content
                and check several publish/unpublish states
        Expecting: everything is all right
        """
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            bc = cnx.create_entity(
                "BaseContent",
                title="bc1",
                content="""\
                <p><h1>bc</h1>
                <a href="%s">Disparus#  Algérie_27 mars.pdf</a>
                </p>"""
                % fobj.cw_adapt_to("IDownloadable").download_url(),
            )
            cnx.commit()
            fkey = cnx.execute(
                f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid}
            )[0][0].getvalue()
            self.assertTrue(self.fileExists(fkey))
            # publish content
            cnx.entity_from_eid(bc.eid).cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_publish"
            )
            cnx.commit()
            self.assertTrue(self.fileExists(fkey))
            published_fkey = self.get_published_fkey(cnx, fobj, fkey)
            self.assertTrue(self.fileExists(published_fkey))
            if S3_ACTIVE:
                self.assertEqual(fkey, published_fkey)
            # unpublish content
            cnx.entity_from_eid(bc.eid).cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_unpublish"
            )
            cnx.commit()
            self.assertFalse(self.fileExists(published_fkey))
            if S3_ACTIVE:
                hidden_fkey = ".hidden/" + fkey.decode("utf-8")
                self.assertTrue(self.fileExists(hidden_fkey))
            else:
                self.assertTrue(self.fileExists(fkey))
            # re-publish content
            cnx.entity_from_eid(bc.eid).cw_adapt_to("IWorkflowable").fire_transition(
                "wft_cmsobject_publish"
            )
            cnx.commit()
            if S3_ACTIVE:
                self.assertFalse(self.fileExists(hidden_fkey))
                self.assertEqual(fkey, published_fkey)
            self.assertTrue(self.fileExists(fkey))
            self.assertTrue(self.fileExists(published_fkey))

    def test_update_file(self):
        """
        This test aims to reproduce a case of Image update
        with jsonschema : when an entity is updated, all related data are removed and
        put back
        Trying: in the same transaction create a new file and delete an old file with the same
                data_hash/data_path
        Expecting: the file still extists
        """
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            cnx.commit()
            fkey = cnx.execute(
                f"Any {self.fkeyfunc}(D) WHERE X data D, X eid %(e)s", {"e": fobj.eid}
            )[0][0].getvalue()
            self.assertTrue(self.fileExists(fkey))
            fobj.cw_delete()
            cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="Disparus#_Algérie_27 mars.pdf",
                data_format="application/pdf",
            )
            cnx.commit()
            self.assertTrue(self.fileExists(fkey))
            self.assertEqual(1, cnx.execute("Any COUNT(X) WHERE X is File")[0][0])


class SubjectImagesHookTC(
    S3BfssStorageTestMixin, PostgresTextMixin, FrACubicConfigMixIn, CubicWebTC
):
    """Tests for Subject hooks."""

    configcls = PostgresApptestConfiguration

    @classmethod
    def init_config(cls, config):
        super(SubjectImagesHookTC, cls).init_config(config)
        config.set_option("consultation-base-url", "https://francearchives.fr")

    def setup_database(self):
        super(SubjectImagesHookTC, self).setup_database()
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir, "cat_narrow.png"), "rb") as stream:
                fobj = cnx.create_entity(
                    "File",
                    data=Binary(stream.read()),
                    data_name="cat.png",
                    data_format="image/png",
                )
                self.image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
                cnx.commit()

    def test_create_subject_image_file(self):
        with self.admin_access.cnx() as cnx:
            image_path = cnx.execute(
                "Any FSPATH(D) WHERE X eid %(e)s, X image_file F, F data D", {"e": self.image.eid}
            )[0][0].getvalue()
            stream = BytesIO(self.getFileContent(image_path))
            image = Image.open(stream)
            self.assertEqual((150, 275), image.size)
            # use image in as SubjectAuthority
            cnx.create_entity("SubjectAuthority", label="subject", subject_image=self.image)
            cnx.commit()
            image_path = cnx.execute(
                "Any FSPATH(D) WHERE X eid %(e)s, X image_file F, F data D", {"e": self.image.eid}
            )[0][0].getvalue()
            stream = BytesIO(self.getFileContent(image_path))
            image = Image.open(stream)
            self.assertEqual(image.size, SUBJECT_IMAGE_SIZE)


if __name__ == "__main__":
    import unittest

    unittest.main()

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
import datetime
import unittest
import json
import os.path as osp
from pgfixtures import setup_module, teardown_module  # noqa

from mock import patch

from logilab.common.date import ustrftime

from cubicweb import Binary
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_francearchives.dataimport.oai_nomina import compute_nomina_stable_id
from cubicweb_francearchives.pviews.edit import load_json_value
from cubicweb_francearchives.testutils import S3BfssStorageTestMixin

from utils import FrACubicConfigMixIn, EsSerializableMixIn

from esfixtures import teardown_module as teardown_module  # noqa

import utils


def json_date(date):
    return ustrftime(date, "%Y/%m/%d %H:%M:%S")


def loads(data, eschema):
    data = json.loads(data)
    for rschema in eschema.ordered_relations():
        if rschema.final and rschema in data:
            ttype = rschema.targets(eschema.type)[0].type
            data[rschema] = load_json_value(data[rschema], ttype)
    return data


def naive_dt(dt):
    return dt.replace(microsecond=0, tzinfo=None)


class SyncServiceTC(S3BfssStorageTestMixin, EsSerializableMixIn, FrACubicConfigMixIn, CubicWebTC):
    """test Sync Service"""

    configcls = PostgresApptestConfiguration

    def assertCalledWith(self, call, *args, **kwargs):
        calledargs, calledkwargs = call
        if args:
            self.assertEqual(args, calledargs)
        for arg_name, arg_value in list(kwargs.items()):
            self.assertIn(arg_name, calledkwargs)
            if arg_value is not None:
                self.assertEqual(arg_value, calledkwargs[arg_name], "%s should be equal" % arg_name)

    def get_published_fpath(self, fpath):
        return osp.join(self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fpath))

    @patch("elasticsearch.Elasticsearch.delete")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_publish_and_unpublish_basecontent(self, index, exists, create, reindex, delete):
        with self.admin_access.cnx() as cnx:
            bc = cnx.create_entity("BaseContent", title="bc")
            cnx.commit()
            wf = bc.cw_adapt_to("IWorkflowable")
            self.assertEqual(wf.state, "wfs_cmsobject_draft")
            wf.fire_transition("wft_cmsobject_publish")
            self.assertFalse(reindex.called)
            cnx.commit()
        self.assertCalledWith(
            reindex.call_args_list[0],
            source_index=self.index_name + "_all",
            target_index=self.published_index_name + "_all",
            query={"query": {"match": {"eid": bc.eid}}},
        )
        with self.admin_access.cnx() as cnx:
            bc = cnx.entity_from_eid(bc.eid)
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            self.assertFalse(delete.called)
            cnx.commit()
        self.assertCalledWith(
            delete.call_args_list[0], self.published_index_name + "_all", id=bc.eid
        )

    @patch("elasticsearch.helpers.scan")
    @patch("elasticsearch.Elasticsearch.delete")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_move_section_with_news(self, index, exists, create, bulk, reindex, delete, scan):
        with self.admin_access.cnx() as cnx:
            section1 = cnx.create_entity("Section", title="titre", content="section1")
            section2 = cnx.create_entity("Section", title="titre", content="section2")
            news = cnx.create_entity("NewsContent", title="news", reverse_children=section2)
            cnx.commit()
            self.assertFalse(reindex.called)
            scan.return_value = [
                {
                    "_type": news.cw_etype,
                    "_id": news.eid,
                    "_source": {
                        "cw_etype": news.cw_etype,
                        "eid": news.eid,
                        "ancestors": [news.reverse_children[0].eid],
                    },
                }
            ]
            cnx.commit()
            section2.cw_set(reverse_children=section1.eid)
            cnx.commit()
            actions = list(bulk.call_args_list[0][0][1])
            action = actions[0]
            source = action["_source"]
            self.assertEqual(source["eid"], news.eid)
            self.assertEqual(source["ancestors"], [section1.eid, section2.eid])

    @patch("elasticsearch.helpers.scan")
    @patch("elasticsearch.Elasticsearch.delete")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_move_section_with_externref(self, index, exists, create, bulk, reindex, delete, scan):
        with self.admin_access.cnx() as cnx:
            section1 = cnx.create_entity("Section", title="titre", content="section1")
            section2 = cnx.create_entity("Section", title="titre", content="section2")
            externref = cnx.create_entity(
                "ExternRef",
                title="virtual exhibit",
                reftype="Virtual_exhibit",
                reverse_children=section2,
            )
            cnx.commit()
            self.assertFalse(reindex.called)
            scan.return_value = [
                {
                    "_type": externref.cw_etype,
                    "_id": externref.eid,
                    "_source": {
                        "cw_etype": externref.cw_etype,
                        "eid": externref.eid,
                        "ancestors": [externref.reverse_children[0].eid],
                    },
                }
            ]
            cnx.commit()
            section2.cw_set(reverse_children=section1.eid)
            cnx.commit()
            actions = list(bulk.call_args_list[0][0][1])
            action = actions[0]
            source = action["_source"]
            self.assertEqual(source["eid"], externref.eid)
            self.assertEqual(source["ancestors"], [section1.eid, section2.eid])

    @patch("elasticsearch.Elasticsearch.delete")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_publish_and_unpublish_sectiontranslation(
        self, index, exists, create, bulk, reindex, delete
    ):
        with self.admin_access.cnx() as cnx:
            section = cnx.create_entity("Section", title="titre", content="contenu")
            cnx.commit()
            wf = section.cw_adapt_to("IWorkflowable")
            self.assertEqual(wf.state, "wfs_cmsobject_draft")
            wf.fire_transition("wft_cmsobject_publish")
            self.assertFalse(reindex.called)
            cnx.commit()
            en = cnx.create_entity(
                "SectionTranslation",
                language="en",
                title="title",
                translation_of=section,
            )
            cnx.commit()
            wf = en.cw_adapt_to("IWorkflowable")
            self.assertEqual(wf.state, "wfs_cmsobject_draft")
            wf.fire_transition("wft_cmsobject_publish")
            self.assertTrue(reindex.called)
            cnx.commit()
        actions = list(bulk.call_args_list[0][0][1])
        self.assertEqual(len(actions), 1)
        action = actions[0]
        for got, expected in (
            (action["_id"], section.eid),
            (action["_op_type"], "index"),
            (action["_index"], self.published_index_name + "_all"),
            (action["_source"]["title_en"], en.title),
        ):
            self.assertEqual(expected, got)
        bulk.reset_mock()
        self.assertCalledWith(
            reindex.call_args_list[0],
            source_index=self.index_name + "_all",
            target_index=self.published_index_name + "_all",
            query={"query": {"match": {"eid": section.eid}}},
        )
        with self.admin_access.cnx() as cnx:
            en = cnx.entity_from_eid(en.eid)
            wf = en.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            self.assertFalse(delete.called)
            cnx.commit()
        self.assertEqual(len(bulk.call_args_list), 1)
        actions = list(bulk.call_args_list[0][0][1])
        self.assertEqual(len(actions), 1)
        action = actions[0]
        for got, expected in (
            (action["_id"], section.eid),
            (action["_op_type"], "index"),
            (action["_index"], self.published_index_name + "_all"),
        ):
            self.assertEqual(expected, got)
        self.assertNotIn("title_en", action["_source"])

    @patch("elasticsearch.helpers.scan", return_value=[{"_type": "FindingAid", "_id": 0}])
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_published_findingaid(self, index, exists, create, bulk, reindex, scan):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            service = ce("Service", category="s1")
            fa = utils.create_findingaid(cnx, service=service)
            ce("EsDocument", entity=fa, doc={"stable_id": fa.stable_id, "eid": fa.eid})
            fac = ce(
                "FAComponent",
                did=ce("Did", unittitle="unittitle", unitid="unitid"),
                stable_id="facomponent_stable_id",
                finding_aid=fa,
            )
            ce("EsDocument", entity=fac, doc={"stable_id": fac.stable_id, "eid": fac.eid})
            cnx.commit()
            wf = fa.cw_adapt_to("IWorkflowable")
            self.assertEqual(wf.state, "wfs_cmsobject_draft")
            wf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
        self.assertEqual(len(reindex.call_args_list), 2)
        calls = {}
        for call in reindex.call_args_list:
            calledargs, calledkwargs = call
            calls[calledkwargs["target_index"]] = call
        published_index_name = self.published_index_name + "_all"
        for index_name in (published_index_name,):
            self.assertCalledWith(
                calls[index_name],
                source_index=self.index_name + "_all",
                target_index=index_name,
                query={"query": {"match": {"fa_stable_id": fa.stable_id}}},
            )
        self.assertEqual(6, len(index.call_args_list))
        self.assertCalledWith(
            index.call_args_list[0],
            index=f"{self.index_name}_all",
            id=fa.stable_id,
        )
        self.assertCalledWith(
            index.call_args_list[1],
            index=f"{self.index_name}_all",
            id=fac.stable_id,
        )
        self.assertCalledWith(
            index.call_args_list[2],
            index=f"{self.index_name}_all",
            id=service.eid,
        )
        self.assertCalledWith(
            index.call_args_list[3],
            index="unittest_service_siaf",
            id=service.eid,
        )
        self.assertCalledWith(
            index.call_args_list[4],
            index=f"{self.index_name}_all",
            id=fa.stable_id,
        )
        self.assertCalledWith(
            index.call_args_list[5],
            index="unittest_service_siaf",
            id=service.eid,
        )
        with self.admin_access.cnx() as cnx:
            fa = cnx.entity_from_eid(fa.eid)
            wf = fa.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            bulk.reset_mock()
            cnx.commit()
        self.assertEqual(len(bulk.call_args_list), 1)
        actions = {}
        for action in (list(bulk.call_args_list[0][0][1])[0],):
            actions[action["_index"]] = action
        for index_name in (published_index_name,):
            self.assertEqual(
                actions[index_name],
                {
                    "_id": 0,
                    "_index": index_name,
                    "_op_type": "delete",
                    "_type": "_doc",
                },
            )

    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_reindex_modified_entity(self, index, exists, create, bulk, reindex):
        """ensure index is called with {'refresh': 'true'}.  If this is not done,
        elsasticsearch.scan method retrieves the old index values which
        causes the wrong values to be copied while synchronizing different
        indexes.
        """
        with self.admin_access.cnx() as cnx:
            news = cnx.create_entity(
                "NewsContent", title="news", order=0, start_date=datetime.datetime(2016, 1, 1)
            )
            cnx.commit()
            news.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            self.assertTrue(index.called)
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["title"], "news")
            news = cnx.find("NewsContent", eid=news.eid).one()
            news.cw_set(title="news title")
            cnx.commit()
            self.assertCalledWith(
                reindex.call_args_list[0],
                source_index=self.index_name + "_all",
                target_index=self.published_index_name + "_all",
                query={"query": {"match": {"eid": news.eid}}},
            )
            self.assertTrue(index.called)
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["title"], "news title")
            self.assertEqual(kwargs["params"]["refresh"], "true")

    @patch("cubicweb_frarchives_edition.entities.adapters.CircularFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_pdf_on_publish(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir, "pdf1.pdf"), "rb") as pdf:
                fobj1 = cnx.create_entity(
                    "File",
                    data=Binary(pdf.read()),
                    data_name="pdf1.pdf",
                    data_format="application/pdf",
                )
            circular = cnx.create_entity(
                "Circular", circ_id="c1", status="revoked", title="c1", attachment=fobj1
            )
            cnx.commit()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            cnx.commit()
            query = "Any FSPATH(D) WHERE F data D, F eid %(f)s"
            fobj1_path = cnx.execute(query, {"f": fobj1.eid})[0][0].getvalue()
            expected = [fobj1_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj1_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.FindingAidIFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_csv_on_publish(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            fa = utils.create_findingaid(cnx)
            fobj1 = cnx.create_entity(
                "File",
                data=Binary(b"toto"),
                data_name="csv1.csv",
                data_format="application/csv",
                reverse_findingaid_support=fa,
            )
            cnx.commit()
            wf = fa.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            cnx.commit()
            query = "Any FSPATH(D) WHERE F data D, F eid %(f)s"
            fobj1_path = cnx.execute(query, {"f": fobj1.eid})[0][0].getvalue()
            expected = [fobj1_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj1_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.BaseContentFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_in_richstring(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="file.pdf",
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
            wf = bc.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            cnx.commit()
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.CardFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_for_card(self, index, exists, create, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="file.pdf",
                data_format="application/pdf",
            )
            cnx.create_entity(
                "Card",
                title="card",
                wikiid="test-card",
                content="""\
<p>
<h1>bc</h1>
<a href="%s">file.pdf</a>
</p>"""
                % fobj.cw_adapt_to("IDownloadable").download_url(),
            )
            cnx.commit()
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.ServiceFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_service_logo(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-image-data"),
                data_name="image-name.png",
                data_format="image/png",
            )
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            cnx.create_entity("Service", category="s1", service_image=image)
            cnx.commit()
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.ImageFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_newscontent_image(
        self, index, exists, reindex, copy, s3_copy_fpath
    ):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-image-data"),
                data_name="image-name.png",
                data_format="image/png",
            )
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            news = cnx.create_entity(
                "NewsContent",
                title="news",
                start_date=datetime.datetime(2016, 1, 1),
                news_image=image,
            )
            cnx.commit()
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertFalse(copy_func.called)
            # publish NewsContent
            news.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"other-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.CommemorationItemFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_commemo_image(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            fobj_c = cnx.create_entity(
                "File",
                data=Binary(b"some-item-image-data"),
                data_name="item-image-name.png",
                data_format="image/png",
            )
            image_c = cnx.create_entity("Image", caption="image-caption", image_file=fobj_c)
            commemo = cnx.create_entity(
                "CommemorationItem",
                title="item1",
                alphatitle="item1",
                commemoration_year=2010,
                commemoration_image=image_c,
            )
            copy_func.reset_mock()
            cnx.commit()
            self.assertFalse(copy_func.called)
            fobj_c = cnx.find("File", eid=fobj_c.eid).one()
            fobj_c.cw_set(data=Binary(b"new-item-image-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertFalse(copy_func.called)
            # publish CommemorationItem
            commemo.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj_c.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)
            fobj_c = cnx.find("File", eid=fobj_c.eid).one()
            fobj_c.cw_set(data=Binary(b"other-item-image-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj_c.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.ImageFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_externref_image(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-image-data"),
                data_name="image-name.png",
                data_format="image/png",
            )
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            externref = cnx.create_entity(
                "ExternRef",
                title="virtual exhibit",
                reftype="Virtual_exhibit",
                externref_image=image,
            )
            cnx.commit()
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertFalse(copy_func.called)
            # publish ExternrefContent
            externref.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"other-data"))
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            expected = [fobj_path]
            if not self.s3_bucket_name:
                expected.append(self.get_published_fpath(fobj_path))
            self.assertCalledWith(copy_func.call_args_list[0], *expected)

    @patch("cubicweb_frarchives_edition.entities.adapters.SectionFileSync.s3_copy_fpath")
    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfiles_on_modify_css_image(self, index, exists, reindex, copy, s3_copy_fpath):
        with self.admin_access.cnx() as cnx:
            static_css_dir = osp.join(self.datadir, "static", "css")
            section = cnx.create_entity("Section", title="Gérer", name="gerer", order=1)
            with open(osp.join(static_css_dir, "hero-comprendre.jpg"), "rb") as stream:
                fobj = cnx.create_entity(
                    "File",
                    data=Binary(stream.read()),
                    data_name="hero-comprendre.jpg",
                    data_format="image/jpg",
                )
            css_image = cnx.create_entity(
                "CssImage",
                cssid="gerer",
                order=1,
                caption="<p>image-caption</p>",
                image_file=fobj,
                cssimage_of=section,
            )
            cnx.commit()
            copy_func = s3_copy_fpath if self.s3_bucket_name else copy
            self.assertFalse(copy_func.called)
            # publish Section
            section.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy_func.reset_mock()
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            # update CssImage
            css_image = cnx.find("CssImage", eid=css_image.eid).one()
            css_image.cw_set(description="description")
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            adapted = section.cw_adapt_to("IFileSync")
            heroimages = adapted.heroimages_to_sync()
            # 6 static images + css image_file
            self.assertEqual(len(copy_func.call_args_list), 7)
            for i in range(6):
                fobj_path = heroimages[i]
                if self.s3_bucket_name:
                    self.assertCalledWith(copy_func.call_args_list[i], fobj_path)
                else:
                    self.assertCalledWith(
                        copy_func.call_args_list[i],
                        fobj_path,
                        osp.join(adapted.published_static_css_dir, osp.basename(fobj_path)),
                    )
            # change the image file
            with open(osp.join(static_css_dir, "hero-gerer.jpg"), "rb") as stream:
                fobj.cw_set(data=Binary(stream.read()))
            copy_func.reset_mock()
            cnx.commit()
            self.assertTrue(copy_func.called)
            adapted = cnx.find("Section", eid=section.eid).one().cw_adapt_to("IFileSync")
            new_heroimages = adapted.heroimages_to_sync()
            self.assertEqual(heroimages, new_heroimages)
            for i in range(6):
                fobj_path = new_heroimages[i]
                if self.s3_bucket_name:
                    self.assertCalledWith(
                        copy_func.call_args_list[i],
                        fobj_path,
                    )
                else:
                    self.assertCalledWith(
                        copy_func.call_args_list[i],
                        fobj_path,
                        osp.join(adapted.published_static_css_dir, osp.basename(fobj_path)),
                    )

    @patch("cubicweb_frarchives_edition.entities.adapters.CircularFileSync.s3_delete_fpath")
    @patch("os.remove")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_unpublish(self, index, exists, reindex, remove, s3_delete_fpath):
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir, "pdf1.pdf"), "rb") as pdf:
                fobj1 = cnx.create_entity(
                    "File",
                    data=Binary(pdf.read()),
                    data_name="pdf1.pdf",
                    data_format="application/pdf",
                )
            circular = cnx.create_entity(
                "Circular", circ_id="c1", status="revoked", title="c1", attachment=fobj1
            )
            cnx.commit()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
            remove_func = s3_delete_fpath if self.s3_bucket_name else remove
            remove_func.reset_mock()
        with self.admin_access.cnx() as cnx:
            circular = cnx.find("Circular", eid=circular.eid).one()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            fobj1_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj1.eid})[
                0
            ][0].getvalue()
        if self.s3_bucket_name:
            expected = [fobj1_path, fobj1.eid]
        else:
            expected = [self.get_published_fpath(fobj1_path)]
        self.assertCalledWith(remove_func.call_args_list[0], *expected)

    @patch("os.remove")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_delete(self, index, exists, reindex, remove):
        self.skipTest(
            "DELETE file synchronization is not handled yet, " "possibly requires lot of changes"
        )
        with self.admin_access.cnx() as cnx:
            with open(osp.join(self.datadir, "pdf1.pdf"), "rb") as pdf:
                fobj1 = cnx.create_entity(
                    "File",
                    data=Binary(pdf.read()),
                    data_name="pdf11.pdf",
                    data_format="application/pdf",
                )
            circular = cnx.create_entity(
                "Circular", circ_id="c1", status="revoked", title="c1", attachment=fobj1
            )
            cnx.commit()
            fobj1_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj1.eid})[
                0
            ][0].getvalue()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            cnx.commit()
            circular.cw_delete()
            remove.reset_mock()
            cnx.commit()
            published_path = osp.join(
                self.config["published-appfiles-dir"], osp.basename(fobj1_path)
            )
        self.assertCalledWith(remove.call_args_list[0], published_path)

    @patch("elasticsearch.Elasticsearch.delete")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_on_delete_service(self, index, exists, reindex, delete):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD092", short_name="AD 92", level="level-D", category="foo"
            )
            cnx.commit()
            service.cw_delete()
            self.assertFalse(delete.called)
            cnx.commit()
            self.assertEqual(3, len(delete.call_args_list))
            self.assertCalledWith(
                delete.call_args_list[0],
                index=f"{self.index_name}_all",
                doc_type="_doc",
                id=service.eid,
            )
            self.assertCalledWith(
                delete.call_args_list[1],
                f"{self.published_index_name}_all",
                doc_type="_doc",
                id=service.eid,
            )
            self.assertCalledWith(
                delete.call_args_list[2],
                index="unittest_service_siaf",
                doc_type="_doc",
                id=service.eid,
            )

    @patch("elasticsearch.Elasticsearch.delete_by_query")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_on_delete_authorityrecord(self, index, exists, reindex, delete):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD092", short_name="AD 92", level="level-D", category="foo"
            )
            kind_eid = cnx.find("AgentKind", name="person")[0][0]
            record = cnx.create_entity(
                "AuthorityRecord",
                record_id="FRAN_NP_006883",
                agent_kind=kind_eid,
                maintainer=service.eid,
                reverse_name_entry_for=cnx.create_entity(
                    "NameEntry", parts="Jean Cocotte", form_variant="authorized"
                ),
                xml_support="foo",
            )
            cnx.commit()
            record.cw_delete()
            self.assertFalse(delete.called)
            cnx.commit()
            self.assertEqual(2, len(delete.call_args_list))
            self.assertCalledWith(
                delete.call_args_list[0],
                self.index_name + "_all",
                doc_type="_doc",
                body={"query": {"match": {"eid": record.eid}}},
            )
            self.assertCalledWith(
                delete.call_args_list[1],
                self.published_index_name + "_all",
                doc_type="_doc",
                body={"query": {"match": {"eid": record.eid}}},
            )

    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_reindex_modify_nominarecord(self, index, exists, create, bulk, reindex):
        """ensure index is called with {'refresh': 'true'}.  If this is not done,
        elsasticsearch.scan method retrieves the old index values which
        causes the wrong values to be copied while synchronizing different
        indexes.
        """
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD008", short_name="AD 92", level="level-D", category="foo"
            )
            record = cnx.create_entity(
                "NominaRecord",
                stable_id=compute_nomina_stable_id(service.code, "42"),
                json_data={"p": [{"n": "Valjean"}], "t": "RM"},
                service=service.eid,
            )
            cnx.commit()
            self.assertTrue(index.called)
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["names"], ["Valjean"])
            self.assertNotIn("forenames", kwargs["body"])
            news = cnx.find("NominaRecord", eid=record.eid).one()
            news.cw_set(json_data={"p": [{"n": "Valjean", "f": "Jean"}], "t": "RM"})
            cnx.commit()
            self.assertEqual(1, len(reindex.call_args_list))
            self.assertCalledWith(
                reindex.call_args_list[0],
                source_index=f"{self.index_name}_all",
                target_index=f"{self.published_index_name}_all",
                query={"query": {"match": {"eid": service.eid}}},
            )
            self.assertEqual(4, len(index.call_args_list))
            self.assertCalledWith(
                index.call_args_list[0],
                index=f"{self.index_name}_all",
                id=service.eid,
            )
            self.assertCalledWith(
                index.call_args_list[1],
                index="unittest_service_siaf",
                id=service.eid,
            )
            self.assertCalledWith(
                index.call_args_list[2],
                index="unittest_index_name_nomina",
                id=record.stable_id,
            )
            self.assertCalledWith(
                index.call_args_list[3],
                index="unittest_index_name_nomina",
                id=record.stable_id,
            )
            self.assertTrue(index.called)
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["names"], ["Valjean"])
            self.assertEqual(kwargs["body"]["forenames"], ["Jean"])
            self.assertEqual(kwargs["params"]["refresh"], "true")

    @patch("elasticsearch.Elasticsearch.delete_by_query")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_on_delete_nominarecord(self, index, exists, reindex, delete):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD008", short_name="AD 92", level="level-D", category="foo"
            )
            record = cnx.create_entity(
                "NominaRecord",
                stable_id=compute_nomina_stable_id(service.code, "42"),
                json_data={"p": [{"n": "Valjean", "f": "Jean"}], "t": "RM"},
                service=service.eid,
            )
            cnx.commit()
            record.cw_delete()
            self.assertFalse(delete.called)
            cnx.commit()
            self.assertEqual(1, len(delete.call_args_list))
            self.assertEqual(delete.call_args[0], ("unittest_index_name_nomina",))
            self.assertCalledWith(
                delete.call_args_list[0],
                doc_type="_doc",
                body={"query": {"match": {"eid": record.eid}}},
            )

    @patch("elasticsearch.Elasticsearch.delete_by_query")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_autority_on_nominarecord(self, index, exists, reindex, delete):
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD008", short_name="AD 92", level="level-D", category="foo"
            )
            record = cnx.create_entity(
                "NominaRecord",
                stable_id=compute_nomina_stable_id(service.code, "42"),
                json_data={"p": [{"n": "Valjean", "f": "Jean"}], "t": "RM"},
                service=service.eid,
            )
            cnx.commit()
            jean = cnx.create_entity("AgentAuthority", label="jean")
            record.cw_set(same_as=jean)
            cnx.commit()
            self.assertEqual(1, len(reindex.call_args_list))
            self.assertCalledWith(
                reindex.call_args_list[0],
                source_index=f"{self.index_name}_all",
                target_index=f"{self.published_index_name}_all",
                query={"query": {"match": {"eid": service.eid}}},
            )
            self.assertEqual(4, len(index.call_args_list))
            self.assertCalledWith(
                index.call_args_list[0],
                index=f"{self.index_name}_all",
                id=service.eid,
            )
            self.assertCalledWith(
                index.call_args_list[1],
                index="unittest_service_siaf",
                id=service.eid,
            )
            self.assertCalledWith(
                index.call_args_list[2],
                index="unittest_index_name_nomina",
                id=record.stable_id,
            )
            self.assertCalledWith(
                index.call_args_list[3],
                index="unittest_index_name_nomina",
                id=record.stable_id,
            )
            self.assertTrue(index.called)
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["authority"], [jean.eid])

    @patch("elasticsearch.client.Elasticsearch.index")
    def test_update_authority_label_sync_to_nomina(self, index):
        """
        Check that renaming an authority sends reindexing a linked NominaRecord
        with the new label
        """
        with self.admin_access.cnx() as cnx:
            service = cnx.create_entity(
                "Service", code="FRAD008", short_name="AD 92", level="level-D", category="foo"
            )
            jean = cnx.create_entity("AgentAuthority", label="jeanot")
            cnx.create_entity(
                "NominaRecord",
                stable_id=compute_nomina_stable_id(service.code, "42"),
                json_data={"p": [{"n": "Valjean", "f": "Jean"}], "t": "RM"},
                service=service.eid,
                same_as=jean,
            )
            cnx.commit()
            self.assertTrue(index.called)
            args, kwargs = index.call_args

            self.assertEqual(kwargs["body"]["authority"], [jean.eid])
            self.assertIn("jeanot", kwargs["body"]["alltext"])
            index.reset_mock()

            jean.cw_set(label="lapin")
            cnx.commit()

            args, kwargs = index.call_args
            self.assertIn("lapin", kwargs["body"]["alltext"])


if __name__ == "__main__":
    unittest.main()

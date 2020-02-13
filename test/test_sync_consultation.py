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
import json
import os.path as osp
from pgfixtures import setup_module, teardown_module  # noqa

from mock import patch

from logilab.common.date import ustrftime

from cubicweb import Binary
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_francearchives.pviews.edit import load_json_value
from cubicweb_francearchives.testutils import HashMixIn

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


class SyncServiceTC(HashMixIn, EsSerializableMixIn, FrACubicConfigMixIn, CubicWebTC):
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

    @patch("elasticsearch.helpers.scan", return_value=[{"_type": "FindingAid", "_id": 0}])
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.helpers.bulk")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_published_findingaid(self, index, exists, create, bulk, reindex, scan):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            fa = utils.create_findingaid(cnx)
            ce("EsDocument", entity=fa, doc={"stable_id": fa.stable_id, "eid": fa.eid})
            fac = ce(
                "FAComponent",
                did=ce("Did", unittitle="unittitle", unitid="unitid"),
                stable_id="stable",
                finding_aid=fa,
            )
            ce("EsDocument", entity=fac, doc={"stable_id": fac.stable_id, "eid": fac.eid})
            cnx.commit()
            wf = fa.cw_adapt_to("IWorkflowable")
            self.assertEqual(wf.state, "wfs_cmsobject_draft")
            wf.fire_transition("wft_cmsobject_publish")
            self.assertFalse(reindex.called)
            cnx.commit()
        self.assertEqual(len(reindex.call_args_list), 1)
        self.assertCalledWith(
            reindex.call_args_list[0],
            source_index=self.index_name + "_all",
            target_index=self.published_index_name + "_all",
            query={"query": {"match": {"fa_stable_id": fa.stable_id}}},
        )
        with self.admin_access.cnx() as cnx:
            fa = cnx.entity_from_eid(fa.eid)
            wf = fa.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            bulk.reset_mock()
            cnx.commit()
        self.assertEqual(len(bulk.call_args_list), 1)
        actions = list(bulk.call_args_list[0][0][1])
        self.assertEqual(
            [
                {
                    "_id": 0,
                    "_index": self.published_index_name + "_all",
                    "_op_type": "delete",
                    "_type": "_doc",
                }
            ],
            actions,
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
            news = cnx.create_entity("NewsContent", title="news", order=0, start_date="2016-01-01")
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
            print(dir(index))
            args, kwargs = index.call_args
            self.assertEqual(kwargs["body"]["title"], "news title")
            self.assertEqual(kwargs["params"]["refresh"], "true")

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_publish(self, index, exists, reindex, copy):
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
            self.assertFalse(copy.called)
            cnx.commit()
            fobj1_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj1.eid})[
                0
            ][0].getvalue()
        self.assertCalledWith(
            copy.call_args_list[0],
            fobj1_path,
            osp.join(
                self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj1_path)
            ),
        )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_in_richstring(self, index, exists, reindex, copy):
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
            self.assertFalse(copy.called)
            cnx.commit()
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.create")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_for_card(self, index, exists, create, reindex, copy):
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
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_service_logo(self, index, exists, reindex, copy):
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
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_newscontent_image(self, index, exists, reindex, copy):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-image-data"),
                data_name="image-name.png",
                data_format="image/png",
            )
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            news = cnx.create_entity(
                "NewsContent", title="news", start_date="2016-01-01", news_image=image
            )
            cnx.commit()
            self.assertFalse(copy.called)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertFalse(copy.called)
            # publish NewsContent
            news.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"other-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_commemo_image(self, index, exists, reindex, copy):
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-coll-image-data"),
                data_name="coll-image-name.png",
                data_format="image/png",
            )
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            coll = cnx.create_entity(
                "CommemoCollection", title="recueil 2010", year=201, section_image=image
            )
            cnx.commit()
            self.assertFalse(copy.called)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-coll-image-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertFalse(copy.called)
            # publish CommemoCollection
            coll.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"other-coll-image-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            # add CommemorationItem
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
                collection_top=coll,
            )
            copy.reset_mock()
            cnx.commit()
            # here copy is called on CommemoCollection Image
            # ('coll-image-name.png')
            self.assertTrue(copy.called)
            fobj_c = cnx.find("File", eid=fobj_c.eid).one()
            fobj_c.cw_set(data=Binary(b"new-item-image-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertFalse(copy.called)
            # publish CommemorationItem
            commemo.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj_c.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            fobj_c = cnx.find("File", eid=fobj_c.eid).one()
            fobj_c.cw_set(data=Binary(b"other-item-image-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj_c.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_modify_externref_image(self, index, exists, reindex, copy):
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
            self.assertFalse(copy.called)
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"new-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertFalse(copy.called)
            # publish ExternrefContent
            externref.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )
            fobj = cnx.find("File", eid=fobj.eid).one()
            fobj.cw_set(data=Binary(b"other-data"))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            fobj_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj.eid})[
                0
            ][0].getvalue()
            self.assertCalledWith(
                copy.call_args_list[0],
                fobj_path,
                osp.join(
                    self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj_path)
                ),
            )

    @patch("shutil.copy")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfiles_on_modify_css_image(self, index, exists, reindex, copy):
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
            self.assertFalse(copy.called)
            # publish Section
            section.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            copy.reset_mock()
            cnx.commit()
            fobj = cnx.find("File", eid=fobj.eid).one()
            # update CssImage
            css_image = cnx.find("CssImage", eid=css_image.eid).one()
            css_image.cw_set(description="description")
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            adapted = section.cw_adapt_to("IFileSync")
            heroimages = adapted.heroimages_to_sync()
            # 6 static images + css image_file
            self.assertEqual(len(copy.call_args_list), 7)
            for i in range(6):
                fobj_path = heroimages[i]
                self.assertCalledWith(
                    copy.call_args_list[i],
                    fobj_path,
                    osp.join(adapted.published_static_css_dir, osp.basename(fobj_path)),
                )
            # change the image file
            with open(osp.join(static_css_dir, "hero-gerer.jpg"), "rb") as stream:
                fobj.cw_set(data=Binary(stream.read()))
            copy.reset_mock()
            cnx.commit()
            self.assertTrue(copy.called)
            adapted = cnx.find("Section", eid=section.eid).one().cw_adapt_to("IFileSync")
            new_heroimages = adapted.heroimages_to_sync()
            self.assertEqual(heroimages, new_heroimages)
            for i in range(6):
                fobj_path = new_heroimages[i]
                self.assertCalledWith(
                    copy.call_args_list[i],
                    fobj_path,
                    osp.join(adapted.published_static_css_dir, osp.basename(fobj_path)),
                )

    @patch("os.remove")
    @patch("elasticsearch.helpers.reindex")
    @patch("elasticsearch.client.indices.IndicesClient.exists")
    @patch("elasticsearch.client.Elasticsearch.index")
    def test_syncfile_on_unpublish(self, index, exists, reindex, remove):
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
            remove.reset_mock()
        with self.admin_access.cnx() as cnx:
            circular = cnx.find("Circular", eid=circular.eid).one()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            fobj1_path = cnx.execute("Any FSPATH(D) WHERE F data D, F eid %(f)s", {"f": fobj1.eid})[
                0
            ][0].getvalue()
            published_path = osp.join(
                self.config["published-appfiles-dir"].encode("utf-8"), osp.basename(fobj1_path)
            )
        self.assertCalledWith(remove.call_args_list[0], published_path)

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


if __name__ == "__main__":
    unittest.main()

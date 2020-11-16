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
import shutil
import logging

import rq
import redis
import rq.exceptions

import traceback

from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl.search import Search
from elasticsearch_dsl import query as dsl_query

from logilab.common.decorators import cachedproperty

from cubicweb import Binary
from cubicweb.predicates import match_kwargs, is_instance
from cubicweb.server.hook import DataOperationMixIn, Operation
from cubicweb.view import EntityAdapter, Adapter

from cubicweb_francearchives.dataimport import usha1, normalize_entry
from cubicweb_francearchives.cssimages import HERO_SIZES, thumbnail_name, static_css_dir

from cubicweb_frarchives_edition import UnpublishFilesOp


class EsAuthorityCandidatesMixin(object):
    def candidates_response(self, query_string, cw_etype):
        """
        select candidates authorities which can be grouped :
        1/ candidate authority must be of the same cw_etype as the main authority
        2/ candidate authority must not be already grouped
        """
        if not query_string:
            return []
        indexer = self._cw.vreg["es"].select("indexer", self._cw)
        indexer.get_connection()
        index_name = self._cw.vreg.config["index-name"]
        search = Search(
            doc_type="_doc", extra={"size": 1000}, index="{}_suggest".format(index_name)
        ).sort("-count")
        must = [
            {"match": {"text": {"query": query_string, "operator": "and"}}},
            # only display authorities of the same cw_etype
            {"match": {"cw_etype": cw_etype}},
            # do not display already grouped authorities
            {"match": {"grouped": False}},
        ]
        search.query = dsl_query.Bool(must=must)
        try:
            response = search.execute()
        except NotFoundError:
            return []
        except Exception as ex:
            self.error("%r, %r", self, ex)
            return []
        if not (response and response.hits.total.value):
            return []
        return response


class IAvailableMixin(object):
    __regid__ = "IAvailable"

    def candidates(self):
        raise NotImplementedError()


class IEntityAvailable(IAvailableMixin, EntityAdapter):
    __select__ = is_instance("Any")

    def candidates(self):
        return {"eid": self.entity.eid, "title": self.entity.dc_title()}


class IEtypeAvailable(IAvailableMixin, Adapter):
    __select__ = match_kwargs("etype")

    def __init__(self, _cw, **kwargs):
        super(IEtypeAvailable, self).__init__(_cw, **kwargs)
        self.etype = kwargs.pop("etype")


class PostgresMixin(object):
    """search data in postgres"""

    def candidates(self, **params):
        rset = self.candidates_rset(**params)
        return [{"title": title, "eid": eid} for eid, title in rset]

    def candidates_rset(self, **params):
        raise NotImplementedError()


class IConceptAvailable(PostgresMixin, IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({"etype": "Concept"})

    def candidates_rset(self, **params):
        return self._cw.execute(
            """
        Any X, LL WHERE X is Concept, X preferred_label L,
        L label LL HAVING NORMALIZE_ENTRY(LL) ILIKE %(q)s
        """,
            {"q": "%{}%".format(normalize_entry(params["q"]))},
        )


class IServiceAvailable(PostgresMixin, IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({"etype": "Service"})

    def candidates_rset(self, **params):
        return self._cw.execute(
            """
        DISTINCT Any X, XN, XN2, C WITH X, XN, XN2, C BEING (
          (Any X, XN, XN2, C WHERE X is Service,
           X name2 XN2, X code C,
           X name XN HAVING NORMALIZE_ENTRY(XN) ILIKE %(q)s)
          UNION
          (Any X, XN, XN2, C WHERE X is Service,
           X name XN, X code C,
           X name2 XN2 HAVING NORMALIZE_ENTRY(XN2) ILIKE %(q)s)
          UNION
          (Any X, XN, XN2, C WHERE X is Service,
           X name XN, X name2 XN2,
           X code C HAVING NORMALIZE_ENTRY(C) ILIKE %(q)s)
        )        """,
            {"q": "%{}%".format(normalize_entry(params["q"]))},
        )

    def candidates(self, **params):
        rset = self.candidates_rset(**params)
        return [{"title": name or name2 or code, "eid": eid} for eid, name, name2, code in rset]


class IAuthorityAvailable(EsAuthorityCandidatesMixin, IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & (
        match_kwargs({"etype": "AgentAuthority"})
        | match_kwargs({"etype": "LocationAuthority"})
        | match_kwargs({"etype": "SubjectAuthority"})
    )

    def candidates(self, **params):
        response = self.candidates_response(params["q"], params["target_type"])
        results = []
        if not (response and response.hits.total.value):
            return []
        _ = self._cw._
        countlabel_templates = (_("0 document"), _("1 document"), _("{count} documents"))
        eid = params.get("eid")
        related = []
        if eid:
            entity = self._cw.entity_from_eid(eid)
            related = [e[0] for e in entity.related(params["rtype"])]
        for result in response:
            if related and result.eid in related:
                continue
            count = result.count if hasattr(result, "count") else 0
            countlabel = countlabel_templates[min(count, 2)].format(count=count)
            indextype = result.type if "type" in result else result.cw_etype
            title = "{title}, {etype} - {countlabel}".format(
                title=result.text, etype=_(indextype), countlabel=countlabel
            )
            results.append(
                {"url": self._cw.build_url(result.urlpath), "title": title, "eid": result.eid}
            )
        return results


class IEntityToGroup(EsAuthorityCandidatesMixin, EntityAdapter):
    __regid__ = "IToGroup"
    __select__ = is_instance("AgentAuthority", "LocationAuthority", "SubjectAuthority")

    def serialize(self):
        return {"eid": self.entity.eid, "title": self.entity.dc_title()}

    def candidates(self, query_string):
        """
        dispaly candidates authorities which can be grouped :

        1/ candidate authority must be of the same cw_etype as the main authority
        2/ candidate authority must not be already grouped
        """
        response = self.candidates_response(query_string, self.entity.cw_etype)
        results = []
        if not (response and response.hits.total.value):
            return results
        _ = self._cw._
        countlabel_templates = (_("0 document"), _("1 document"), _("{count} documents"))
        for result in response:
            if result.eid == self.entity.eid:
                continue
            count = result.count if hasattr(result, "count") else 0
            countlabel = countlabel_templates[min(count, 2)].format(count=count)
            indextype = result.type if "type" in result else result.cw_etype
            title = "{title}, {etype} - {countlabel}".format(
                title=result.text, etype=_(indextype), countlabel=countlabel
            )
            results.append(
                {"url": self._cw.build_url(result.urlpath), "title": title, "eid": result.eid}
            )
        return results


class StartRqTaskOp(DataOperationMixIn, Operation):
    def postcommit_event(self):
        # use connection if it exists (e.g. one created by FakeRedis), else create new connection
        # the demo does not seem to connect automatically to the redis instance, so we need
        # to manually create it
        queue = rq.Queue(connection=rq.connections.get_current_connection() or redis.Redis())
        for args, kwargs in self.cnx.transaction_data.get("rq_tasks", []):
            kwargs.setdefault("job_timeout", "2h")
            queue.enqueue(*args, **kwargs)


class IRqJob(EntityAdapter):
    """provide a proxy from an entity to rq Job"""

    __regid__ = "IRqJob"
    END_STATUSES = (rq.job.JobStatus.FINISHED, rq.job.JobStatus.FAILED)

    def __init__(self, *args, **kwargs):
        super(IRqJob, self).__init__(*args, **kwargs)
        self._job = None

    @property
    def id(self):
        return str(self.entity.eid)

    def enqueue(self, *args, **kwargs):
        assert "job_id" not in kwargs, "job_id is a reserved kwarg"
        kwargs["job_id"] = self.id
        self._cw.transaction_data.setdefault("rq_tasks", []).append((args, kwargs))
        # Operation want a cnx not a request
        cnx = getattr(self._cw, "cnx", self._cw)
        StartRqTaskOp.get_instance(cnx).add_data(self.entity.eid)

    def get_job(self):
        if self._job is None:
            try:
                self._job = rq.job.Job.fetch(self.id)
            except rq.job.NoSuchJobError:
                self.warning("failed to get job #%s from redis, mocking one", self.id)
                return rq.job.Job.create(self.id)
        return self._job

    def refresh(self):
        self._job = None

    @property
    def progress(self):
        if self.status in self.END_STATUSES:
            return 1.0
        meta = self.get_job().meta
        return meta.get("progress", 0.0)

    @property
    def log(self):
        key = "rq:job:{0}:log".format(self.id)
        connection = self.get_job().connection
        content = connection.get(key) or b""
        content = content.decode("utf-8")
        return content

    def handle_finished(self):
        pass

    def __getattr__(self, attr):
        return getattr(self.get_job(), attr)


class RqTaskJob(IRqJob):
    __select__ = IRqJob.__select__ & is_instance("RqTask")

    def handle_failure(self, *exc_info):
        update = dict(
            log=Binary(self.log.encode("utf-8")),
            status=rq.job.JobStatus.FAILED,
        )
        for attr in ("enqueued_at", "started_at"):
            update[attr] = getattr(self, attr)
        self.entity.cw_set(**update)

    def handle_finished(self):
        # save relevant metadata in persistent storage
        update = {"log": Binary(self.log.encode("utf-8"))}
        for attr in ("enqueued_at", "started_at"):
            update[attr] = getattr(self, attr)
        update["status"] = rq.job.JobStatus.FINISHED
        self.entity.cw_set(**update)

    def is_finished(self):
        return self.entity.status in self.END_STATUSES

    def get_job(self):
        if self.is_finished():
            return self.entity
        return super(RqTaskJob, self).get_job()

    @property
    def status(self):
        if self.is_finished():
            return self.entity.status
        return self.get_status()

    @property
    def log(self):
        if self.is_finished():
            return self.entity.log.read().decode("utf-8")
        return super(RqTaskJob, self).log


def copy(src, dest, logger=None):
    try:
        shutil.copy(src, dest)
    except Exception:
        if logger is None:
            logger = logging.getLogger("cubicweb_francearchives.sync")
        logger.exception("failed to sync %r -> %r", src, dest)
        traceback.print_exc()


class IFileSync(EntityAdapter):
    __regid__ = "IFileSync"
    __select__ = is_instance("Any")

    @property
    def pub_appfiles_dir(self):
        return self._cw.vreg.config.get("published-appfiles-dir")

    @staticmethod
    def queries():
        return ()

    def files_to_sync(self):
        if not self.pub_appfiles_dir:
            return []
        queries = self.queries()
        if not queries:
            return []
        if len(queries) > 1:
            query = " UNION ".join("(%s)" % q for q in queries)
        else:
            query = queries[0]
        rset = self._cw.execute(query, {"e": self.entity.eid})
        return [(fpath.getvalue(), eid) for fpath, eid in rset]

    def delete(self):
        for fpath, feid in self.files_to_sync():
            fullpath = osp.join(self.pub_appfiles_dir.encode("utf-8"), osp.basename(fpath))
            if osp.exists(fullpath):
                UnpublishFilesOp.get_instance(self._cw).add_data(
                    (self.entity, feid, fpath, fullpath)
                )

    def copy(self):
        if not self.pub_appfiles_dir:
            return
        if not osp.exists(self.pub_appfiles_dir):
            os.makedirs(self.pub_appfiles_dir)
        for fpath, feid in self.files_to_sync():
            fullpath = osp.join(self.pub_appfiles_dir.encode("utf-8"), osp.basename(fpath))
            copy(fpath, fullpath)


class FindingAidIFileSync(IFileSync):
    __select__ = IFileSync.__select__ & is_instance("FindingAid")

    @staticmethod
    def queries():
        return (
            "Any FSPATH(FD), F WHERE FA findingaid_support F, F data FD, "
            'FA eid %(e)s, F data_name ILIKE "%.pdf"',
            "Any FSPATH(FD), F WHERE FA ape_ead_file F, F data FD, " "FA eid %(e)s",
            "Any FSPATH(FD), F WHERE FA fa_referenced_files F, F data FD, " "FA eid %(e)s",
            "Any FSPATH(FD), F WHERE FAC finding_aid FA, FAC fa_referenced_files F, F data FD, "
            "FA eid %(e)s",
        )

    def get_destpath(self, filepath):
        with open(filepath, "rb") as f:
            sha1 = usha1(f.read())
        basename = osp.basename(filepath)
        if not basename.startswith(sha1):
            basename = "{}_{}".format(sha1, basename)
        return osp.join(self.pub_appfiles_dir, basename)

    def get_fullpath(self, fpath):
        if isinstance(fpath, bytes):
            fpath = fpath.decode("utf-8")
        if fpath.endswith(".pdf"):
            return self.get_destpath(fpath)
        basepath = osp.basename(fpath)
        if fpath.endswith(".xml") and basepath.startswith("ape-"):
            ape_ead_service_dir = osp.join(
                self.pub_appfiles_dir, "ape-ead", self.entity.service_code
            )
            if not osp.exists(ape_ead_service_dir):
                os.makedirs(ape_ead_service_dir)
            return osp.join(ape_ead_service_dir, basepath)
        return ""

    def delete(self):
        for fpath, feid in self.files_to_sync():
            fullpath = self.get_fullpath(fpath)
            if osp.exists(fullpath):
                UnpublishFilesOp.get_instance(self._cw).add_data(
                    (self.entity, feid, fpath, fullpath)
                )

    def copy(self):
        for fpath, feid in self.files_to_sync():
            fullpath = self.get_fullpath(fpath)
            copy(fpath, fullpath)


class CircularFileSync(IFileSync):
    __select__ = IFileSync.__select__ & is_instance("Circular")

    @staticmethod
    def queries():
        return (
            "Any FSPATH(FD), F WHERE X attachment F, F data FD, X eid %(e)s",
            "Any FSPATH(FD), F WHERE X additional_attachment F, F data FD, X eid %(e)s",
        )


class RichContentFileSyncMixin(object):
    def queries(self):
        q = super(RichContentFileSyncMixin, self).queries()
        if not self.entity.e_schema.has_relation("referenced_files", "subject"):
            return q
        return q + (
            "Any FSPATH(FD), F WHERE F is File, F data FD, X eid %(e)s, X referenced_files F",
        )


class ImageFileSync(IFileSync):
    __abstract__ = True
    rtype = None

    def queries(self):
        return (
            "Any FSPATH(FD), F WHERE X {} I, I image_file F, F data FD, "
            "X eid %(e)s".format(self.rtype),
        )


class CommemoFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("CommemoCollection")
    rtype = "section_image"


class SectionFileSync(RichContentFileSyncMixin, CommemoFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("Section")

    def queries(self):
        q = super(SectionFileSync, self).queries()
        return q + (
            "Any FSPATH(FD), F WHERE X eid %(e)s, I cssimage_of X, I image_file F, F data FD",
        )

    @cachedproperty
    def published_static_css_dir(self):
        return static_css_dir(self._cw.vreg.config.get("published-staticdir-path"))

    def heroimages_to_sync(self):
        files = []
        rset = self._cw.execute(
            "Any I WHERE X cssimage_of S, S eid %(e)s, X cssid I", {"e": self.entity.eid}
        )
        if rset:
            static_dir = static_css_dir(self._cw.vreg.config.static_directory)
            cssid = rset[0][0]
            image_path = "%s.jpg" % cssid
            basename, ext = osp.splitext(image_path)
            for size, suffix in HERO_SIZES:
                thumb_name = thumbnail_name(basename, suffix, ext)
                thumbpath = osp.join(static_dir, thumb_name)
                files.append(thumbpath)
        return files

    def copy(self):
        if self.published_static_css_dir and osp.exists(self.published_static_css_dir):
            for srcpath in self.heroimages_to_sync():
                destpath = osp.join(self.published_static_css_dir, osp.basename(srcpath))
                copy(srcpath, destpath)
        super(SectionFileSync, self).copy()


class SectionTranslationFileSync(RichContentFileSyncMixin, IFileSync):
    __select__ = IFileSync.__select__ & is_instance("SectionTranslation")


class ServiceFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("Service")
    rtype = "service_image"


class NewsFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("NewsContent")
    rtype = "news_image"


class BaseContentFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("BaseContent")
    rtype = "basecontent_image"


class BaseContentTranslationFileSync(RichContentFileSyncMixin, IFileSync):
    __select__ = IFileSync.__select__ & is_instance("BaseContentTranslation")


class CommemorationItemFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("CommemorationItem")
    rtype = "commemoration_image"


class MapFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("Map")
    rtype = "map_image"


class CardFileSync(RichContentFileSyncMixin, IFileSync):
    __select__ = IFileSync.__select__ & is_instance("Card")


class ExternRefFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance("ExternRef")
    rtype = "externref_image"


class IGeoExtrenalDBAdapter(EntityAdapter):
    __regid__ = "IGeoDB"
    osm_uri = "http://www.openstreetmap.org/?mlat={latitude}&mlon={longitude}&zoom=18"

    def __init__(self, _cw, **kwargs):
        super(IGeoExtrenalDBAdapter, self).__init__(_cw, **kwargs)
        self._latitude = None
        self._longitude = None
        self.init_coordinates()

    @property
    def sql(self):
        if hasattr(self._cw, "cnx"):
            return self._cw.cnx.system_sql
        elif hasattr(self._cw, "system_sql"):
            return self._cw.system_sql

    @property
    def latitude(self):
        return self._latitude

    @property
    def longitude(self):
        return self._longitude

    def init_coordinates(self):
        raise NotImplementedError()

    @cachedproperty
    def openstreetmap_uri(self):
        if self.latitude and self.longitude:
            return self.osm_uri.format(latitude=self.latitude, longitude=self.longitude)


class ExternalUriGeoExtrenalDBAdapter(IGeoExtrenalDBAdapter):
    __select__ = IGeoExtrenalDBAdapter.__select__ & is_instance("ExternalUri")

    def init_coordinates(self):
        if self.entity.source == "geoname":
            crs = self.sql(
                """
                SELECT latitude, longitude
                FROM geonames WHERE geonameid=%(gid)s
            """,
                {"gid": self.entity.extid},
            )
            res = crs.fetchall()
            if res:
                self._latitude, self._longitude = res[0]


class ExternalIdGeoExtrenalDBAdapter(IGeoExtrenalDBAdapter):
    __select__ = IGeoExtrenalDBAdapter.__select__ & is_instance("ExternalId")

    def init_coordinates(self):
        if self.entity.source == "bano":
            crs = self.sql(
                """
            SELECT lat, lon
            FROM  bano_whitelisted WHERE banoid=%(bid)s
            """,
                {"bid": self.entity.extid},
            )
            res = crs.fetchall()
            if res:
                self._latitude, self._longitude = res[0]


class LeafletJson(EntityAdapter):
    __regid__ = "ILeaflet"
    __select__ = EntityAdapter.__select__ & is_instance("LocationAuthority")

    def get_baseurl(self, instance_type):
        if instance_type == "cms":
            return self._cw.base_url()
        elif instance_type == "consultation":
            return "{}/".format(self._cw.vreg.config.get("consultation-base-url"))
        raise Exception("Unknown instance type {}".format(instance_type))

    def json(self, instance_type):
        base_url = self.get_baseurl(instance_type)
        rset = self._cw.execute(
            "Any P, PL, PLAT, PLNG, COUNT(F) GROUPBY P, PL, PLAT, PLNG "
            "WHERE I authority P, I index F, P eid %(eid)s, "
            "P is LocationAuthority, P latitude PLAT, P longitude PLNG, "
            "P label PL, NOT P latitude NULL",
            {"eid": self.entity.eid},
        )
        if rset:
            eid, label, latitude, longitude, count = rset[0]
            return [
                {
                    "eid": eid,
                    "label": label,
                    "lat": latitude,
                    "lng": longitude,
                    "dashLabel": "--" in label,
                    "count": count,
                    "url": "{}location/{}".format(base_url, eid),
                }
            ]
        return []

    def csv_row(self):
        props = self.properties()
        return [(props[h] or "") for h in self.headers]

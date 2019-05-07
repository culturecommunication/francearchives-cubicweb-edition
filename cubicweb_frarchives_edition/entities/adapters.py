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
import rq.exceptions

import traceback

from logilab.common.decorators import cachedproperty

from cubicweb import Binary
from cubicweb.predicates import match_kwargs, is_instance
from cubicweb.server.hook import DataOperationMixIn, Operation
from cubicweb.view import EntityAdapter, Adapter

from cubicweb_francearchives.dataimport import usha1
from cubicweb_francearchives.cssimages import (HERO_SIZES,
                                               thumbnail_name,
                                               static_css_dir)


class IAvailableMixin(object):
    __regid__ = 'IAvailable'


class IEntityAvailable(IAvailableMixin, EntityAdapter):
    __select__ = is_instance('Any')

    def serialize(self):
        return {'eid': self.entity.eid,
                'title': self.entity.dc_title()}


class IEtypeAvailable(IAvailableMixin, Adapter):
    __select__ = match_kwargs('etype')

    def rql(self):
        raise NotImplementedError()


class IConceptAvailable(IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({'etype': 'Concept'})

    def rql(self):
        return 'Any X WHERE X is Concept, X preferred_label L, L label ILIKE %(q)s'


class IServiceAvailable(IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({'etype': 'Service'})

    def rql(self):
        return ('Any X, XN, XN2, XL WHERE X is Service, '
                'X name XN, X name2 XN2, X level XL, '
                '(X name ILIKE %(q)s OR X category ILIKE %(q)s)')


class IAgentAuthority(IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({'etype': 'AgentAuthority'})

    def rql(self):
        return 'Any X WHERE X is AgentAuthority, X label ILIKE %(q)s'


class ILocationAuthority(IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({'etype': 'LocationAuthority'})

    def rql(self):
        return 'Any X WHERE X is LocationAuthority, X label ILIKE %(q)s'


class ISubjectAuthority(IEtypeAvailable):
    __select__ = IEtypeAvailable.__select__ & match_kwargs({'etype': 'SubjectAuthority'})

    def rql(self):
        return 'Any X WHERE X is SubjectAuthority, X label ILIKE %(q)s'


class StartRqTaskOp(DataOperationMixIn, Operation):

    def postcommit_event(self):
        queue = rq.Queue()
        for args, kwargs in self.cnx.transaction_data.get('rq_tasks', []):
            kwargs.setdefault('timeout', '2h')
            queue.enqueue(*args, **kwargs)


class IRqJob(EntityAdapter):
    """provide a proxy from an entity to rq Job"""
    __regid__ = 'IRqJob'
    END_STATUSES = (rq.job.JobStatus.FINISHED, rq.job.JobStatus.FAILED)

    def __init__(self, *args, **kwargs):
        super(IRqJob, self).__init__(*args, **kwargs)
        self._job = None

    @property
    def id(self):
        return str(self.entity.eid)

    def enqueue(self, *args, **kwargs):
        assert 'job_id' not in kwargs, 'job_id is a reserved kwarg'
        kwargs['job_id'] = self.id
        self._cw.transaction_data.setdefault('rq_tasks', []).append((args, kwargs))
        # Operation want a cnx not a request
        cnx = getattr(self._cw, 'cnx', self._cw)
        StartRqTaskOp.get_instance(cnx).add_data(self.entity.eid)

    def get_job(self):
        if self._job is None:
            try:
                self._job = rq.job.Job.fetch(self.id)
            except rq.job.NoSuchJobError:
                self.warning('failed to get job #%s from redis, mocking one',
                             self.id)
                return rq.job.Job.create(self.id)
        return self._job

    def refresh(self):
        self._job = None

    @property
    def progress(self):
        if self.status in self.END_STATUSES:
            return 1.
        meta = self.get_job().meta
        return meta.get('progress', 0.)

    @property
    def log(self):
        key = 'rq:job:{0}:log'.format(self.id)
        connection = self.get_job().connection
        content = connection.get(key) or b''
        content = content.decode('utf-8')
        return content

    def handle_finished(self):
        pass

    def __getattr__(self, attr):
        return getattr(self.get_job(), attr)


class RqTaskJob(IRqJob):
    __select__ = IRqJob.__select__ & is_instance('RqTask')

    def handle_failure(self, *exc_info):
        update = dict(
            log=Binary(self.log.encode('utf-8')),
            status=rq.job.JobStatus.FAILED,
        )
        for attr in ('enqueued_at', 'started_at'):
            update[attr] = getattr(self, attr)
        self.entity.cw_set(**update)

    def handle_finished(self):
        # save relevant metadata in persistent storage
        update = {'log': Binary(self.log.encode('utf-8'))}
        for attr in ('enqueued_at', 'started_at'):
            update[attr] = getattr(self, attr)
        update['status'] = rq.job.JobStatus.FINISHED
        self.entity.cw_set(**update)

    def is_finished(self):
        return self.entity.status in self.END_STATUSES

    def get_job(self):
        if self.is_finished():
            return self.entity
        return super(RqTaskJob, self).get_job()

    @property
    def log(self):
        if self.is_finished():
            return self.entity.log.read().decode('utf-8')
        return super(RqTaskJob, self).log


def copy(src, dest, logger=None):
    try:
        shutil.copy(src, dest)
    except Exception:
        if logger is None:
            logger = logging.getLogger('cubicweb_francearchives.sync')
        logger.exception('failed to sync %r -> %r', src, dest)
        traceback.print_exc()


class IFileSync(EntityAdapter):
    __regid__ = 'IFileSync'
    __select__ = is_instance('Any')

    @property
    def pub_appfiles_dir(self):
        return self._cw.vreg.config.get('published-appfiles-dir')

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
            query = ' UNION '.join('(%s)' % q for q in queries)
        else:
            query = queries[0]
        rset = self._cw.execute(query, {'e': self.entity.eid})
        return [fpath.getvalue() for fpath, in rset]

    def delete(self):
        for fpath in self.files_to_sync():
            fullpath = osp.join(self.pub_appfiles_dir, osp.basename(fpath))
            if osp.exists(fullpath):
                os.remove(fullpath)

    def copy(self):
        if not self.pub_appfiles_dir:
            return
        if not osp.exists(self.pub_appfiles_dir):
            os.makedirs(self.pub_appfiles_dir)
        for fpath in self.files_to_sync():
            fullpath = osp.join(self.pub_appfiles_dir, osp.basename(fpath))
            copy(fpath, fullpath)


class FindingAidIFileSync(IFileSync):
    __select__ = IFileSync.__select__ & is_instance('FindingAid')

    @staticmethod
    def queries():
        return (
            'Any FSPATH(FD) WHERE FA findingaid_support F, F data FD, '
            'FA eid %(e)s, F data_name ILIKE "%.pdf"',
            'Any FSPATH(FD) WHERE FA ape_ead_file F, F data FD, '
            'FA eid %(e)s',
            'Any FSPATH(FD) WHERE FA fa_referenced_files F, F data FD, '
            'FA eid %(e)s',
            'Any FSPATH(FD) WHERE FAC finding_aid FA, FAC fa_referenced_files F, F data FD, '
            'FA eid %(e)s',
        )

    def get_destpath(self, filepath):
        sha1 = usha1(open(filepath).read())
        basename = osp.basename(filepath)
        if not basename.startswith(sha1):
            basename = '{}_{}'.format(sha1, basename)
        return osp.join(self.pub_appfiles_dir, basename)

    def get_fullpath(self, fpath):
        if fpath.endswith('.pdf'):
            return self.get_destpath(fpath)
        basepath = osp.basename(fpath)
        if fpath.endswith('.xml') and basepath.startswith('ape-'):
            ape_ead_service_dir = osp.join(self.pub_appfiles_dir, 'ape-ead',
                                           self.entity.service_code)
            if not osp.exists(ape_ead_service_dir):
                os.makedirs(ape_ead_service_dir)
            return osp.join(ape_ead_service_dir, basepath)
        return ''

    def delete(self):
        for fpath in self.files_to_sync():
            fullpath = self.get_fullpath(fpath)
            if osp.exists(fullpath):
                os.remove(fullpath)

    def copy(self):
        for fpath in self.files_to_sync():
            fullpath = self.get_fullpath(fpath)
            copy(fpath, fullpath)


class CircularFileSync(IFileSync):
    __select__ = IFileSync.__select__ & is_instance('Circular')

    @staticmethod
    def queries():
        return (
            'Any FSPATH(FD) WHERE X attachment F, F data FD, X eid %(e)s',
            'Any FSPATH(FD) WHERE X additional_attachment F, F data FD, X eid %(e)s',
        )


class RichContentFileSyncMixin(object):

    def queries(self):
        q = super(RichContentFileSyncMixin, self).queries()
        if not self.entity.e_schema.has_relation('referenced_files', 'subject'):
            return q
        return q + (
            'Any FSPATH(FD) WHERE F is File, F data FD, X eid %(e)s, X referenced_files F',
        )


class ImageFileSync(IFileSync):
    __abstract__ = True
    rtype = None

    def queries(self):
        return (
            'Any FSPATH(FD) WHERE X {} I, I image_file F, F data FD, '
            'X eid %(e)s'.format(self.rtype),
        )


class CommemoFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('CommemoCollection')
    rtype = 'section_image'


class SectionFileSync(RichContentFileSyncMixin, CommemoFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('Section')

    def queries(self):
        q = super(SectionFileSync, self).queries()
        return q + ('Any FSPATH(FD) WHERE X eid %(e)s, I cssimage_of X, I image_file F, F data FD',)

    @cachedproperty
    def published_static_css_dir(self):
        return static_css_dir(self._cw.vreg.config.get('published-staticdir-path'))

    def heroimages_to_sync(self):
        files = []
        rset = self._cw.execute(
            'Any I WHERE X cssimage_of S, S eid %(e)s, X cssid I',
            {'e': self.entity.eid}
        )
        if rset:
            static_dir = static_css_dir(self._cw.vreg.config.static_directory)
            cssid = rset[0][0]
            image_path = '%s.jpg' % cssid
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


class ServiceFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('Service')
    rtype = 'service_image'


class NewsFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('NewsContent')
    rtype = 'news_image'


class BaseContentFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('BaseContent')
    rtype = 'basecontent_image'


class CommemorationItemFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('CommemorationItem')
    rtype = 'commemoration_image'


class MapFileSync(RichContentFileSyncMixin, ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('Map')
    rtype = 'map_image'


class CardFileSync(RichContentFileSyncMixin, IFileSync):
    __select__ = IFileSync.__select__ & is_instance('Card')


class ExternRefFileSync(ImageFileSync):
    __select__ = ImageFileSync.__select__ & is_instance('ExternRef')
    rtype = 'externref_image'

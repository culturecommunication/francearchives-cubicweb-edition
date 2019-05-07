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
"""cubicweb-ctl plugin providing additional commands

:organization: Logilab
:copyright: 2017 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
:contact: http://www.logilab.fr/ -- mailto:contact@logilab.fr
"""
from __future__ import print_function
from __future__ import absolute_import

import os
import os.path as osp
import sys

import redis
import requests
import rq

import time

import urlparse
import zipfile

from logilab.common.shellutils import ProgressBar


from cubicweb.cwctl import CWCTL, init_cmdline_log_threshold
from cubicweb.cwconfig import CubicWebConfiguration as cwcfg
from cubicweb.pyramid import settings_from_cwconfig
from cubicweb.toolsutils import Command

from cubicweb_francearchives import CMS_OBJECTS
from cubicweb_francearchives import admincnx
from cubicweb_francearchives.ccplugin import ImportEAD

from cubicweb_frarchives_edition.rq import work
from cubes.frarchives_edition.slony import create_master, add_slave, start_slave
from cubicweb_frarchives_edition.alignments import setup
from cubicweb_frarchives_edition.alignments.geonames_importer import (
    align_findingaids, )

HERE = osp.dirname(osp.abspath(__file__))


@CWCTL.register
class SetupInitialState(Command):
    """build initial state for all workflowable entities imported with massive objectstore"""
    arguments = '<instance>'
    name = 'fa-initial-state'
    min_args = max_args = 1

    def run(self, args):
        appid = args.pop()
        with admincnx(appid) as cnx:
            for etype in ({'FindingAid'}
                          | set(CMS_OBJECTS) - {'ExternRef', 'Map'}):
                print('migrating', etype)
                rset = cnx.execute('Any S WHERE S is State, S state_of WF, X default_workflow WF, '
                                   'X name %(etype)s, WF initial_state S', {'etype': etype})
                if len(rset) != 1:
                    print(rset)
                    continue
                cnx.system_sql('INSERT INTO in_state_relation (eid_from, eid_to) '
                               'SELECT cw_eid, %(eid_to)s FROM cw_{} WHERE '
                               'NOT EXISTS (SELECT 1 FROM in_state_relation i '
                               'WHERE i.eid_from = cw_eid)'.format(etype.lower()),
                               {'eid_to': rset[0][0]})
            cnx.commit()


@CWCTL.register
class PeriodicOaiImport(Command):
    """run ``cubicweb_francearchives.dataimport.oai.import_delta`` in RqTask"""
    arguments = '<instance>'
    name = 'fa-rq-import-oai'
    max_args = None
    min_args = 1

    def run(self, args):
        from cubicweb_frarchives_edition.tasks import import_oai
        appid = args.pop()
        settings = settings_from_cwconfig(cwcfg.config_for(appid))
        connection = redis.StrictRedis.from_url(settings.get('rq.redis_url', ''))
        with admincnx(appid) as cnx, rq.Connection(connection):
            for oairepoeid, in cnx.execute('Any X WHERE X is OAIRepository'):
                rqtask = cnx.create_entity('RqTask',
                                           name=u'import_oai',
                                           title=u'import-oai today')
                rqtask.cw_adapt_to('IRqJob').enqueue(import_oai, oairepoeid)
                cnx.commit()


@CWCTL.register
class RqWorker(Command):
    """run a python-rq worker for instance"""
    arguments = '<instance>'
    name = 'rq-worker'
    max_args = None
    min_args = 1

    def run(self, args):
        appid = args.pop()
        settings = settings_from_cwconfig(cwcfg.config_for(appid))
        connection = redis.StrictRedis.from_url(settings.get('rq.redis_url', ''))
        with rq.Connection(connection):
            work(appid)


@CWCTL.register
class CMSImportEAD(ImportEAD):
    """override default import-ead command to set initial state"""
    options = ImportEAD.options + [
        ('initial-state', {
            'type': 'string',
            'default': None,
            'help': 'expected state of all imported findingaids. Default is '
                    "the workflow's initial state."
        }),
    ]

    def run(self, args):
        appid = args[0]
        super(CMSImportEAD, self).run(args)
        with admincnx(appid) as cnx:
            if self['initial-state']:
                state_rset = cnx.execute(
                    'Any S WHERE S is State, S state_of WF, '
                    'X default_workflow WF, X name "FindingAid", '
                    'S name %(sn)s', {'sn': self['initial-state']})
                if not state_rset:
                    print("Error: the initial state %r doesn't exist"
                          % self['initial-state'])
                    sys.exit(1)
                state_eid = state_rset[0][0]
            else:
                state_eid = cnx.execute(
                    'Any S WHERE S is State, S state_of WF, '
                    'X default_workflow WF, X name "FindingAid", '
                    'WF initial_state S')[0][0]
            cnx.system_sql('INSERT INTO in_state_relation (eid_from, eid_to) '
                           'SELECT cw_eid, %(state_eid)s '
                           'FROM cw_FindingAid fa '
                           '     LEFT OUTER JOIN in_state_relation isr '
                           '                  ON (fa.cw_eid=isr.eid_from) '
                           'WHERE isr.eid_from IS NULL',
                           {'state_eid': state_eid})
            cnx.commit()


class GenerateSlonyMaster(Command):
    """generate Slony command file for the master node"""
    arguments = '<instance>'
    name = 'gen-slony-master'
    min_args = max_args = 1
    options = [
        ('output', {
            'short': 'o',
            'type': 'string',
            'default': 'setup_master.slonik',
            'help': 'output file',
        }),
        ('debug', {
            'short': 'D', 'action': 'store_true',
            'help': 'start server in debug mode.'
        }),
        ('loglevel', {
            'short': 'l', 'type': 'choice', 'metavar': '<log level>',
            'default': None, 'choices': ('debug', 'info', 'warning', 'error'),
            'help': 'debug if -D is set, error otherwise',
        }),
    ]

    def run(self, args):
        appid = args.pop(0)
        config = cwcfg.config_for(appid, debugmode=self['debug'])
        init_cmdline_log_threshold(config, self['loglevel'])

        skip_entities = ('FAFileImport', 'FAOAIPMHImport', )
        skip_relations = ('imported_findingaid', )
        mih = config.migration_handler()
        try:
            with mih.cnx:
                setup_cmd = create_master(
                    mih.cnx,
                    skip_entities=skip_entities,
                    skip_relations=skip_relations)
        finally:
            mih.shutdown()

        outfile = osp.abspath(self['output'])
        with open(outfile, 'w') as outf:
            outf.write(setup_cmd)

        print('Slony command file for creating the master node configuration '
              'generated in\n   {file}\n\n'
              'You must run it:\n\n  slonik {file}\n'.format(
                  file=outfile))


class GenerateSlonySlave(Command):
    """generate Slony command file to add a slave to the replication cluster"""
    arguments = '<instance> <slaveid>'
    name = 'gen-slony-slave'
    min_args = max_args = 2
    options = [
        ('slave-db-host', {
            'short': 'H', 'type': 'string',
            'default': None,
            'help': ('database host of the slave; if unset, '
                     'use the same host as the instance.'),
        }),
        ('slave-db-port', {
            'short': 'p', 'type': 'string',
            'default': None,
            'help': ('database host port of the slave; if unset, '
                     'use the same host as the instance.'),
        }),
        ('slave-db-name', {
            'type': 'string',
            'short': 'n',
            'default': None,
            'help': 'slave database name',
        }),
        ('slave-db-user', {
            'type': 'string',
            'short': 'U',
            'default': None,
            'help': 'slave database user',
        }),
        ('slave-db-password', {
            'type': 'password',
            'short': 'P',
            'default': '',
            'help': 'slave database password',
        }),
        ("debug", {
            'short': 'D', 'action': 'store_true',
            'help': 'start server in debug mode.'
        }),
        ('loglevel', {
            'short': 'l', 'type': 'choice', 'metavar': '<log level>',
            'default': None, 'choices': ('debug', 'info', 'warning', 'error'),
            'help': 'debug if -D is set, error otherwise',
        }),
    ]

    def run(self, args):
        appid, slaveid = args
        slaveid = int(slaveid)
        config = cwcfg.config_for(appid, debugmode=self['debug'])
        init_cmdline_log_threshold(config, self['loglevel'])
        slave_dbcfg = config.system_source_config.copy()
        slave_dbcfg['db-name'] = self['slave-db-name'] or slave_dbcfg['db-name'] + '_slave'
        for cfg in ('host', 'port', 'user', 'password'):
            if self['slave-db-' + cfg]:
                slave_dbcfg['db-' + cfg] = self['slave-db-' + cfg]
        repo = config.migration_handler().repo

        cmd = add_slave(repo, slaveid, slave_dbcfg)
        outfile = osp.abspath('add_slave_%s.slonik' % slaveid)
        with open(outfile, 'w') as outf:
            outf.write(cmd)
        print('Slony command file to add a new slave has been generated in\n'
              '{file}\n\n'
              'You must run it:\n\n  slonik {file}\n'.format(
                  file=outfile))

        startcmd, sloncmd = start_slave(repo, slaveid, slave_dbcfg)
        outfile = osp.abspath('start_slave_%s.slonik' % slaveid)
        with open(outfile, 'w') as outf:
            outf.write(startcmd)
        print('A Slony command file to start the replication on that slave '
              'has also been generated in\n  {file}\n\n'
              'You can run it:\n\n  slonik {file}\n'.format(
                  file=outfile))
        print('NOTE: the replicated database MUST exists, \n'
              '      schemas and tables MUST exists \n'
              '      BEFORE starting the replication.\n')
        print('NOTE: slon replication daemons must be started for the '
              'replication to happen:\n\n  %s\n' % sloncmd)


@CWCTL.register
class AlignGeonames(Command):
    """align the whole base with geonames.org"""
    arguments = '<instance>'
    name = 'align-geonames'
    max_args = None
    min_args = 1
    options = [
        ('csv-dir', {
            'type': 'string',
            'default': '/tmp/csv',
            'help': 'filepath to directory where csv will be written',
        }),
        ('nodrop', {
            'action': 'store_true',
            'help': (u"Do not delete existing geonames alignements")
        }),
        ('services', {
            'type': 'csv',
            'default': (),
            'help': (u"List of service codes to be aligned "
                     u"separated by comma (f.e: FRAD051,FRAD052"),
        }),
    ]

    def run(self, args):
        """call align_findingaids from alignements.import"""
        appid = args.pop()
        with admincnx(appid) as cnx:
            align_findingaids(cnx, appid,
                              csv_dir=self['csv-dir'],
                              services=self['services'],
                              nodrop=self['nodrop'])


class SetuDatabaseMixin(object):
    download_chunk_size = 8192

    def download_file(self, zip_fname, datadir, download):
        zip_fpath = osp.join(datadir, zip_fname)
        unzip_fpath = osp.join(datadir, '{}.txt'.format(zip_fname.split('.zip')[0]))
        if not osp.isfile(zip_fpath) or download:
            uri = urlparse.urljoin(self.database_dump_uri, zip_fname)
            print(u'\n-> Downloading {0} \n   to {1}'.format(uri, zip_fpath))
            r = requests.get(urlparse.urljoin(self.database_dump_uri, zip_fname), stream=True)
            try:
                fsize = int(r.headers.get('Content-Length'))
            except (ValueError, TypeError):
                pb = None
            else:
                pb = ProgressBar(fsize, 50)
            with open(zip_fpath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=self.download_chunk_size):
                    if chunk:
                        f.write(chunk)
                        if pb:
                            pb.update(self.download_chunk_size)
            print(u'\nUsing downloaded file {0}'.format(zip_fpath))
            # unzip the file
            if not osp.isfile(unzip_fpath) or download:
                print(u'\n-> Extracting {0} \n  to {1}'.format(
                    zip_fpath, unzip_fpath))
                with zipfile.ZipFile(zip_fpath) as geonames_zip:
                    geonames_zip.extractall(datadir)
            file_info = '\nUsing extracted file {0}: \n    size: {1}, last modified: {2}'
        else:
            file_info = '\nUsing existing file {0}: \n    size: {1}, last modified: {2}'
        (mode, ino, dev, nlink, uid, gid,
         size, atime, mtime, ctime) = os.stat(unzip_fpath)
        print(file_info.format(unzip_fpath, size, time.ctime(mtime)))
        return unzip_fpath

    def table_exists(self, cnx, tablename):
        crs = cnx.system_sql(
            'SELECT relname FROM pg_class WHERE relname = %(t)s',
            {'t': tablename})
        return crs.fetchall()


@CWCTL.register
class SetupGeonamesDatabase(SetuDatabaseMixin, Command):
    """Setup required initial data (geonames, etc.)."""
    name = 'setup-geonames'
    arguments = '<instance id>'
    options = (
        ('folder', {
            'type': 'yn',
            'help': ('If yes, print the folder where Geonames files are downloaded '
                     'and exits.'),
        }),
        ('update', {
            'short': 'u', 'type': 'yn', 'default': False,
            'help': 'Force Geonames table removed, even if tables already exists'
        }),
        ('download', {
            'type': 'yn', 'default': False,
            'help': 'Force Geonames data download, even file already exists'
        }),
        ('dry-run', {
            'type': 'yn', 'default': False,
            'help': 'Set to True if you want to skip table update'
        }),
    )
    database_dump_uri = 'http://download.geonames.org/export/dump/'

    def run(self, args):
        appid = args[0]
        with admincnx(appid) as cnx:
            datadir = osp.join(cnx.repo.config.appdatahome, 'geonames')
        if not osp.isdir(datadir):
            os.mkdir(datadir)
        if self.get('folder'):
            print(u'Geonames files are downloaded into '
                  'the following folder:\n{0}'.format(datadir))
            return
        with admincnx(appid) as cnx:
            if self.table_exists(cnx, 'geonames') and not self.get('update'):
                print(u'Geonames database already exists. Use `--update=y` to update.'.format(
                    datadir))
                return
            download = self.get('download')
            allcountries_path = self.download_file('allCountries.zip', datadir, download)
            altnames_path = self.download_file('alternateNames.zip', datadir, download)
            allcountries_path = osp.join(datadir,
                                         'allCountries.txt')
            altnames_path = osp.join(datadir,
                                     'alternateNames.txt')
            if not self.get('dry-run'):
                print('\n-> Create and populate geonames tables')
                setup.load_geonames_tables(cnx, allcountries_path, altnames_path)


CWCTL.register(GenerateSlonyMaster)
CWCTL.register(GenerateSlonySlave)

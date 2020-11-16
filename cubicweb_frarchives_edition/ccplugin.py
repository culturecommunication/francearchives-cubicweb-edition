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


from datetime import datetime

import gzip

import os
import os.path as osp

import sys

import redis
import requests
import rq

import time

import shutil
import urllib.parse
import zipfile

from logilab.common.shellutils import ProgressBar
from cubicweb import ConfigurationError

from cubicweb.cwctl import CWCTL, init_cmdline_log_threshold
from cubicweb.cwconfig import CubicWebConfiguration as cwcfg
from cubicweb.cwconfig import instance_configuration
from cubicweb.pyramid import settings_from_cwconfig
from cubicweb.toolsutils import Command

from cubicweb_francearchives import CMS_OBJECTS
from cubicweb_francearchives import CMS_I18N_OBJECTS
from cubicweb_francearchives import admincnx
from cubicweb_francearchives.ccplugin import ImportEAD


from cubicweb_frarchives_edition import load_leaflet_json
from cubicweb_frarchives_edition import ALIGN_IMPORT_DIR
from cubicweb_frarchives_edition.entities.kibana import (
    IndexESIRKibana,
    IndexESServicesKibana,
    IndexESAuthoritiesKibana,
    IndexEsKibanaLauncher,
)
from cubicweb_frarchives_edition.rq import work
from cubicweb_frarchives_edition.slony import create_master, add_slave, start_slave
from cubicweb_frarchives_edition.alignments import setup
from cubicweb_frarchives_edition.alignments.importers import (
    GeonamesAlignImporter,
    BanoAlignImporter,
)
from cubicweb_frarchives_edition.alignments.align import AgentAligner
from cubicweb_frarchives_edition.alignments.group_subjects import group_subject_authorities

HERE = osp.dirname(osp.abspath(__file__))


def get_rq_redis_connection(appid):
    settings = settings_from_cwconfig(cwcfg.config_for(appid))
    redis_url = settings.get("rq.redis_url")
    if redis_url is None:
        raise ConfigurationError(
            "could not start rq: `rq.redis_url` is missing from " "pyramid.ini file"
        )
    return redis.StrictRedis.from_url(redis_url)


@CWCTL.register
class SetupInitialState(Command):
    """build initial state for all workflowable entities imported with massive objectstore"""

    arguments = "<instance>"
    name = "fa-initial-state"
    min_args = max_args = 1

    def run(self, args):
        appid = args.pop()
        with admincnx(appid) as cnx:
            for etype in (
                {"FindingAid"} | set(CMS_OBJECTS) - {"ExternRef", "Map"} | set(CMS_I18N_OBJECTS)
            ):
                print("migrating", etype)
                rset = cnx.execute(
                    "Any S WHERE S is State, S state_of WF, X default_workflow WF, "
                    "X name %(etype)s, WF initial_state S",
                    {"etype": etype},
                )
                if len(rset) != 1:
                    print(rset)
                    continue
                cnx.system_sql(
                    "INSERT INTO in_state_relation (eid_from, eid_to) "
                    "SELECT cw_eid, %(eid_to)s FROM cw_{} WHERE "
                    "NOT EXISTS (SELECT 1 FROM in_state_relation i "
                    "WHERE i.eid_from = cw_eid)".format(etype.lower()),
                    {"eid_to": rset[0][0]},
                )
            cnx.commit()


@CWCTL.register
class PeriodicOaiImport(Command):
    """run ``cubicweb_francearchives.dataimport.oai.import_delta`` in RqTask"""

    arguments = "<instance>"
    name = "fa-rq-import-oai"
    max_args = None
    min_args = 1

    def run(self, args):
        from cubicweb_frarchives_edition.tasks import import_oai

        appid = args.pop()
        connection = get_rq_redis_connection(appid)
        with admincnx(appid) as cnx, rq.Connection(connection):
            for (oairepoeid, auto_dedupe, context_service) in cnx.execute(
                """Any X, A, S WHERE X is OAIRepository,
                    X should_normalize A, X context_service S"""
            ):
                service = cnx.entity_from_eid(oairepoeid).service[0]
                task_title = "import-oai delta {code} ({date})".format(
                    code=service.code, date=datetime.utcnow().strftime("%Y-%m-%d")
                )
                rqtask = cnx.create_entity("RqTask", name="import_oai", title=task_title)
                rqtask.cw_adapt_to("IRqJob").enqueue(
                    import_oai, oairepoeid, auto_dedupe, context_service, publish=True
                )
                cnx.commit()


@CWCTL.register
class RqWorker(Command):
    """run a python-rq worker for instance"""

    arguments = "<instance>"
    name = "rq-worker"
    max_args = None
    min_args = 1

    def run(self, args):
        appid = args.pop()
        connection = get_rq_redis_connection(appid)
        with rq.Connection(connection):
            work(appid)


@CWCTL.register
class CMSImportEAD(ImportEAD):
    """override default import-ead command to set initial state"""

    options = ImportEAD.options + [
        (
            "initial-state",
            {
                "type": "string",
                "default": None,
                "help": "expected state of all imported findingaids. Default is "
                "the workflow's initial state.",
            },
        ),
    ]

    def run(self, args):
        appid = args[0]
        super(CMSImportEAD, self).run(args)
        with admincnx(appid) as cnx:
            if self["initial-state"]:
                state_rset = cnx.execute(
                    "Any S WHERE S is State, S state_of WF, "
                    'X default_workflow WF, X name "FindingAid", '
                    "S name %(sn)s",
                    {"sn": self["initial-state"]},
                )
                if not state_rset:
                    print("Error: the initial state %r doesn't exist" % self["initial-state"])
                    sys.exit(1)
                state_eid = state_rset[0][0]
            else:
                state_eid = cnx.execute(
                    "Any S WHERE S is State, S state_of WF, "
                    'X default_workflow WF, X name "FindingAid", '
                    "WF initial_state S"
                )[0][0]
            cnx.system_sql(
                "INSERT INTO in_state_relation (eid_from, eid_to) "
                "SELECT cw_eid, %(state_eid)s "
                "FROM cw_FindingAid fa "
                "     LEFT OUTER JOIN in_state_relation isr "
                "                  ON (fa.cw_eid=isr.eid_from) "
                "WHERE isr.eid_from IS NULL",
                {"state_eid": state_eid},
            )
            cnx.commit()


class GenerateSlonyMaster(Command):
    """generate Slony command file for the master node"""

    arguments = "<instance>"
    name = "gen-slony-master"
    min_args = max_args = 1
    options = [
        (
            "output",
            {
                "short": "o",
                "type": "string",
                "default": "setup_master.slonik",
                "help": "output file",
            },
        ),
        ("debug", {"short": "D", "action": "store_true", "help": "start server in debug mode."}),
        (
            "loglevel",
            {
                "short": "l",
                "type": "choice",
                "metavar": "<log level>",
                "default": None,
                "choices": ("debug", "info", "warning", "error"),
                "help": "debug if -D is set, error otherwise",
            },
        ),
    ]

    def run(self, args):
        appid = args.pop(0)
        config = cwcfg.config_for(appid, debugmode=self["debug"])
        init_cmdline_log_threshold(config, self["loglevel"])

        skip_entities = ()
        skip_relations = ("imported_findingaid",)
        mih = config.migration_handler()
        try:
            with mih.cnx:
                setup_cmd = create_master(
                    mih.cnx, skip_entities=skip_entities, skip_relations=skip_relations
                )
        finally:
            mih.shutdown()

        outfile = osp.abspath(self["output"])
        with open(outfile, "w") as outf:
            outf.write(setup_cmd)

        print(
            "Slony command file for creating the master node configuration "
            "generated in\n   {file}\n\n"
            "You must run it:\n\n  slonik {file}\n".format(file=outfile)
        )


class GenerateSlonySlave(Command):
    """generate Slony command file to add a slave to the replication cluster"""

    arguments = "<instance> <slaveid>"
    name = "gen-slony-slave"
    min_args = max_args = 2
    options = [
        (
            "slave-db-host",
            {
                "short": "H",
                "type": "string",
                "default": None,
                "help": (
                    "database host of the slave; if unset, " "use the same host as the instance."
                ),
            },
        ),
        (
            "slave-db-port",
            {
                "short": "p",
                "type": "string",
                "default": None,
                "help": (
                    "database host port of the slave; if unset, "
                    "use the same host as the instance."
                ),
            },
        ),
        (
            "slave-db-name",
            {
                "type": "string",
                "short": "n",
                "default": None,
                "help": "slave database name",
            },
        ),
        (
            "slave-db-user",
            {
                "type": "string",
                "short": "U",
                "default": None,
                "help": "slave database user",
            },
        ),
        (
            "slave-db-password",
            {
                "type": "password",
                "short": "P",
                "default": "",
                "help": "slave database password",
            },
        ),
        ("debug", {"short": "D", "action": "store_true", "help": "start server in debug mode."}),
        (
            "loglevel",
            {
                "short": "l",
                "type": "choice",
                "metavar": "<log level>",
                "default": None,
                "choices": ("debug", "info", "warning", "error"),
                "help": "debug if -D is set, error otherwise",
            },
        ),
    ]

    def run(self, args):
        appid, slaveid = args
        slaveid = int(slaveid)
        config = cwcfg.config_for(appid, debugmode=self["debug"])
        init_cmdline_log_threshold(config, self["loglevel"])
        slave_dbcfg = config.system_source_config.copy()
        slave_dbcfg["db-name"] = self["slave-db-name"] or slave_dbcfg["db-name"] + "_slave"
        for cfg in ("host", "port", "user", "password"):
            if self["slave-db-" + cfg]:
                slave_dbcfg["db-" + cfg] = self["slave-db-" + cfg]
        repo = config.migration_handler().repo

        cmd = add_slave(repo, slaveid, slave_dbcfg)
        outfile = osp.abspath("add_slave_%s.slonik" % slaveid)
        with open(outfile, "w") as outf:
            outf.write(cmd)
        print(
            "Slony command file to add a new slave has been generated in\n"
            "{file}\n\n"
            "You must run it:\n\n  slonik {file}\n".format(file=outfile)
        )

        startcmd, sloncmd = start_slave(repo, slaveid, slave_dbcfg)
        outfile = osp.abspath("start_slave_%s.slonik" % slaveid)
        with open(outfile, "w") as outf:
            outf.write(startcmd)
        print(
            "A Slony command file to start the replication on that slave "
            "has also been generated in\n  {file}\n\n"
            "You can run it:\n\n  slonik {file}\n".format(file=outfile)
        )
        print(
            "NOTE: the replicated database MUST exists, \n"
            "      schemas and tables MUST exists \n"
            "      BEFORE starting the replication.\n"
        )
        print(
            "NOTE: slon replication daemons must be started for the "
            "replication to happen:\n\n  %s\n" % sloncmd
        )


class ImportAlignments(Command):
    arguments = "<instance>"
    max_args = None
    min_args = 1
    options = [
        (
            "csv-dir",
            {
                "type": "string",
                "default": ALIGN_IMPORT_DIR,
                "help": "filepath to directory where csv will be written",
            },
        ),
        (
            "nodrop",
            {"action": "store_true", "help": ("Do not delete existing geonames alignements")},
        ),
        (
            "services",
            {
                "type": "csv",
                "default": (),
                "help": (
                    "List of service codes to be aligned "
                    "separated by comma (f.e: FRAD051,FRAD052"
                ),
            },
        ),
        (
            "force",
            {
                "action": "store_true",
                "default": False,
                "help": ("Override user alignments"),
            },
        ),
    ]

    def run(self, args):
        """call align_findingaids from alignements.import"""
        appid = args.pop()
        with admincnx(appid) as cnx:
            config = {
                "appid": appid,
                "csv_dir": self["csv-dir"],
                "services": self["services"],
                "nodrop": self["nodrop"],
                "force": self["force"],
            }
            self.importer(cnx, config).align()
            # update json for leafleat
            load_leaflet_json(cnx)


@CWCTL.register
class AlignGeonames(ImportAlignments):
    """align the whole base with geonames.org"""

    name = "align-geonames"
    importer = GeonamesAlignImporter


@CWCTL.register
class ComputeGeonamesAlignment(Command):
    """run ``cubicweb_frarchives_edition.tasks.compute_alignments``
    in RqTask for all services"""

    arguments = "<instance>"
    name = "compute-geonames-alignment"
    max_args = None
    min_args = 1

    def run(self, args):
        from cubicweb_frarchives_edition.tasks import compute_alignments

        appid = args.pop()
        connection = get_rq_redis_connection(appid)
        with admincnx(appid) as cnx, rq.Connection(connection):
            # get FindingAid/FAComponent entities by service
            for service, code in cnx.execute(
                """Any X, C WHERE X is Service,
            X code C, EXISTS (F service X)"""
            ):
                findingaids = cnx.execute(
                    """Any X WHERE X is FindingAid,
                    X service S, S eid %(service)s""",
                    {"service": service},
                )
                task_title = "align-geonames-services {service}".format(service=code)
                rqtask = cnx.create_entity("RqTask", name="compute_alignments", title=task_title)
                rqtask.cw_adapt_to("IRqJob").enqueue(
                    compute_alignments, [eid for eid, in findingaids.rows], targets=("geonames",)
                )
                cnx.commit()


@CWCTL.register
class AlignBano(ImportAlignments):
    """align the whole base with BANO (prototype)."""

    name = "align-bano"
    importer = BanoAlignImporter


class SetupDatabase(Command):
    """Setup required initial data (geonames, bano, etc.)."""

    arguments = "<instance id>"
    download_chunk_size = 8192
    datadir_basename = None
    witness_table = None

    options = (
        (
            "datadir",
            {
                "type": "string",
                "help": ("folder where database files are downloaded"),
            },
        ),
        (
            "update",
            {
                "short": "u",
                "type": "yn",
                "default": False,
                "help": "Force table removal, even if tables already exists",
            },
        ),
        (
            "download",
            {
                "type": "yn",
                "default": False,
                "help": "Force data download, even file already exists",
            },
        ),
        (
            "dry-run",
            {
                "type": "yn",
                "default": False,
                "help": "Set to True if you want to skip table update",
            },
        ),
        (
            "db-dbname",
            {
                "type": "string",
                "help": ("database name, default is the " "same as the cubicweb instance."),
            },
        ),
        (
            "db-dbhost",
            {
                "type": "string",
                "help": ("database host, default is the " "same as the cubicweb instance."),
            },
        ),
        (
            "db-dbport",
            {
                "type": "string",
                "help": ("database port, default is the " "same as the cubicweb instance."),
            },
        ),
        (
            "db-dbuser",
            {
                "type": "string",
                "help": ("database user, default is the " "same as the cubicweb instance."),
            },
        ),
        (
            "db-dbpassword",
            {
                "type": "string",
                "help": ("database password, default is the " "same as the cubicweb instance."),
            },
        ),
    )
    database_dump_uri = "http://download.geonames.org/export/dump/"

    def run(self, args):
        appid = args[0]
        inst_config = instance_configuration(appid)
        sconf = inst_config.system_source_config
        dbparams = {
            "database": self.config.db_dbname or sconf["db-name"],
            "host": self.config.db_dbhost or sconf.get("db-host", ""),
            "user": self.config.db_dbuser or sconf["db-user"],
            "password": self.config.db_dbpassword or sconf.get("db-password", ""),
            "port": self.config.db_dbport or sconf.get("db-port", ""),
        }
        if self.config.db_dbuser and not self.config.db_dbpassword:
            from getpass import getpass

            dbparams["password"] = getpass("password: ")
        datadir = self.get("datadir")
        if datadir and not osp.isdir(datadir):
            print('\n-> The specified directory "{}" does not exist. ' "Exit.\n".format(datadir))
            return
        with admincnx(appid) as cnx:
            if not datadir:
                datadir = cnx.repo.config.appdatahome
            datadir = osp.join(datadir, self.datadir_basename)
        if not osp.isdir(datadir):
            os.mkdir(datadir)
        if self.get("download"):
            print(
                "\n-> Database files are downloaded into "
                "the following folder:\n    {0}".format(datadir)
            )
        else:
            print("\n-> Use database files from " "the following folder:\n    {0}".format(datadir))
        if setup.table_exists(dbparams, self.witness_table) and not self.get("update"):
            print("\n-> Database already exists. Use `--update=y` to update.".format())
            return
        table_owner = sconf["db-user"] if dbparams["user"] != sconf["db-user"] else None
        self.load_data(appid, dbparams, datadir, table_owner=table_owner)

    def load_data(self, appid, dbparams, datadir, table_owner=None):
        """"create and populate sql tables"""
        raise NotImplementedError()

    def unzip_file(self, zip_fpath, datadir):
        with zipfile.ZipFile(zip_fpath) as data_zip:
            print(
                "\n-> Extracting {0} \n    to {1}".format(zip_fpath, ", ".join(data_zip.namelist()))
            )
            data_zip.extractall(datadir)

    def gunzip_file(self, zip_fpath, unzip_fpath, datadir):
        print("\n-> Extracting {0} \n    to {1}".format(zip_fpath, unzip_fpath))
        with gzip.GzipFile(zip_fpath) as data_zip:
            with open(unzip_fpath, "wb") as out:
                shutil.copyfileobj(data_zip, out)

    def download_file(self, zip_fname, unzip_fname, datadir):
        zip_fpath = osp.join(datadir, zip_fname)
        unzip_fpath = osp.join(datadir, unzip_fname)
        download = self.get("download")
        if not osp.isfile(zip_fpath) or download:
            uri = urllib.parse.urljoin(self.database_dump_uri, zip_fname)
            print("\n-> Downloading {0} \n    to {1}".format(uri, zip_fpath))
            r = requests.get(urllib.parse.urljoin(self.database_dump_uri, zip_fname), stream=True)
            try:
                fsize = int(r.headers.get("Content-Length"))
            except (ValueError, TypeError):
                pb = None
            else:
                pb = ProgressBar(fsize, 50)
            if fsize:
                print("    file size: expected {0}".format(fsize))
            with open(zip_fpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=self.download_chunk_size):
                    if chunk:
                        f.write(chunk)
                        if pb:
                            pb.update(self.download_chunk_size)
            print("\nUsing downloaded file {0}".format(zip_fpath))
            # unzip the file
            if not osp.isfile(unzip_fpath) or download:
                try:
                    self.unzip_file(zip_fpath, datadir)
                except zipfile.BadZipfile:
                    self.gunzip_file(zip_fpath, unzip_fpath, datadir)
            file_info = "\nUsing extracted file {0}: \n    size: {1}, last modified: {2}"
        else:
            file_info = "\nUsing existing file {0}: \n    size: {1}, last modified: {2}"
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(unzip_fpath)
        print(file_info.format(unzip_fpath, size, time.ctime(mtime)))
        return unzip_fpath


@CWCTL.register
class SetupGeonamesDatabase(SetupDatabase):
    """Setup required initial data for Geonames"""

    name = "setup-geonames"
    datadir_basename = "geonames"
    witness_table = "geonames"
    database_dump_uri = "http://download.geonames.org/export/dump/"

    def load_data(self, appid, dbparams, datadir, table_owner=None):
        allcountries_path = self.download_file("allCountries.zip", "allCountries.txt", datadir)
        altnames_path = self.download_file("alternateNames.zip", "alternateNames.txt", datadir)
        if not self.get("dry-run"):
            print("\n-> Create and populate geonames tables")
            setup.load_geonames_tables(
                appid, dbparams, allcountries_path, altnames_path, table_owner=table_owner
            )


@CWCTL.register
class SetupBanoDatabase(SetupDatabase):
    """Setup required initial data for BANO"""

    name = "setup-bano"
    datadir_basename = "bano"
    witness_table = "bano_whitelisted"
    database_dump_uri = "https://bano.openstreetmap.fr/data/full.csv.gz"

    def load_data(self, appid, dbparams, datadir, table_owner=None):
        full_path = self.download_file("full.csv.gz", "full.csv", datadir)
        if not self.get("dry-run"):
            print("\n-> Create and populate BANO tables")
            setup.load_bano_tables(appid, dbparams, full_path, table_owner=table_owner)


@CWCTL.register
class ImportAlignmentAgent(Command):
    """import AgentAuthority alignments (wikidata, data.bnf)"""

    name = "import-alignment-agent"
    arguments = "<instance id> <file>"
    min_args = max_args = 2

    def run(self, args):
        appid, file_name = args
        with admincnx(appid) as cnx:
            aligner = AgentAligner(cnx)
            try:
                fp = open(file_name)
            except IOError as exception:
                print("failed to open file {0} ({1})(abort)".format(file_name, exception))
                return
            alignments = aligner.process_csv(fp)
            print("found {} alignments".format(len(alignments)))
            aligner.process_alignments(alignments)
            fp.close()


@CWCTL.register
class ReloadJsonMap(Command):
    """reload IR map data"""

    name = "reload-map"
    arguments = "<instance>"
    max_args = min_args = 1

    def run(self, args):
        """reload IR map data"""
        appid = args.pop()
        with admincnx(appid) as cnx:
            # update json for leafleat
            load_leaflet_json(cnx)


@CWCTL.register
class GroupSimilarSubjects(Command):
    """group similar subjects

    <instance id>
      identifier of the instance
    """

    name = "group-subjects"
    arguments = "<instance>"
    max_args = min_args = 1
    options = [
        (
            "dry-run",
            {
                "type": "yn",
                "default": True,
                "help": "set to False if you want to perform the grouping",
            },
        ),
        (
            "directory",
            {
                "type": "string",
                "default": "/tmp",
                "help": "directory to directory where subjects_togroup_DATE.csv will be written",
            },
        ),
        (
            "limit",
            {
                "type": "int",
                "default": "110000",
                "help": "limit on documents number linked to an autority",
            },
        ),
    ]

    def run(self, args):
        """reload IR map data"""
        appid = args.pop()
        directory = self.config.directory
        if directory and not osp.isdir(directory):
            print('\n-> the specified directory "{}" does not exist. \n'.format(directory))
            return
        with admincnx(appid) as cnx:
            dry_run = self.config.dry_run
            if dry_run:
                print("\n-> no grouping will be performed.")
            group_subject_authorities(
                cnx, dry_run=dry_run, directory=directory, limitdoc=self.config.limit
            )


for cmdclass in (
    GenerateSlonyMaster,
    GenerateSlonySlave,
    IndexESIRKibana,
    IndexESServicesKibana,
    IndexESAuthoritiesKibana,
    IndexEsKibanaLauncher,
):
    CWCTL.register(cmdclass)

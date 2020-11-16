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
from functools import partial

import logging

import tqdm

from elasticsearch.helpers import parallel_bulk

from cubicweb.toolsutils import Command

from cubicweb.view import EntityAdapter

from cubicweb_elasticsearch.entities import Indexer
from cubicweb_elasticsearch.es import indexable_entities

from cubicweb_francearchives import admincnx

from cubicweb_frarchives_edition.entities.kibana.sqlutils import create_kibana_authorities_sql


_tqdm = partial(tqdm.tqdm, disable=None)


class AbstractKibanaSerializable(EntityAdapter):
    __abstract__ = True
    __regid__ = "IKibanaIndexSerializable"

    @property
    def es_id(self):
        return self.entity.eid


class AbstractKibanaIndexer(Indexer):
    __regid__ = "IKibanaIndexSerializable"
    analyser_settings = {
        "analysis": {
            "filter": {
                "elision": {
                    "type": "elision",
                    "articles": [
                        "l",
                        "m",
                        "t",
                        "qu",
                        "n",
                        "s",
                        "j",
                        "d",
                        "c",
                        "jusqu",
                        "quoiqu",
                        "lorsqu",
                        "puisqu",
                    ],
                },
                "my_ascii_folding": {
                    "type": "asciifolding",
                    "preserve_original": True,
                },
                "french_stop": {
                    "type": "stop",
                    "stopwords": "_french_",
                },
            },
            "analyzer": {
                "default": {
                    "filter": ["my_ascii_folding", "lowercase", "elision", "french_stop"],
                    "tokenizer": "standard",
                    "char_filter": ["html_strip"],
                }
            },
        }
    }

    @property
    def settings(self):
        settings = Indexer.settings.copy()
        settings.update(
            {
                "settings": self.analyser_settings,
                "mappings": self.mapping,
            }
        )
        return settings


class IndexESIRKibana(Command):
    """Create indexes and index FindingAids and FAComponents for data monitoring in Kibana.

    <instance id>
      identifier of the instance

    """

    name = "index-es-ir-kibana"
    min_args = max_args = 1
    arguments = "<instance id>"
    options = [
        (
            "kibana-ir-index-name",
            {
                "type": "string",
                "default": "siaf",
                "help": "use a custom index name rather than the one "
                "specified in the all-in-one.conf file. ",
            },
        ),
    ]

    def run(self, args):
        """run the command with its specific arguments"""
        appid = args.pop(0)
        with admincnx(appid) as cnx:
            kibana_ir_indexer = cnx.vreg["es"].select("kibana-ir-indexer", cnx)
            print(
                """creating "{}" kibana index for FindingAid/FAComponents""".format(
                    kibana_ir_indexer.index_name
                )
            )
            es = kibana_ir_indexer.get_connection()
            if not es and self.config.debug:
                print("no elasticsearch configuration found, skipping")
                return
            kibana_ir_indexer.create_index()
            kibana_ir_indexer.populate_index()


class IndexESKibana(Command):
    """Create indexes and index data monitoring in Kibana.

    <instance id>
      identifier of the instance

    """

    min_args = max_args = 1
    arguments = "<instance id>"
    indexer_name = None
    serializer = "IKibanaIndexSerializable"
    options = [
        (
            "no-index",
            {
                "type": "yn",
                "default": False,
                "help": "set to True if you only want to create views",
            },
        ),
        (
            "etypes",
            {
                "type": "csv",
                "default": "",
                "help": "only index given etypes [default:all indexable types]",
            },
        ),
        (
            "index-name",
            {
                "type": "string",
                "default": "",
                "help": "use a custom index name rather than the one "
                "specified in the all-in-one.conf file. ",
            },
        ),
        (
            "chunksize",
            {
                "type": "int",
                "default": 100000,
                "help": "max number of entities to fetch at once (deafult: 100000)",
            },
        ),
    ]

    def run(self, args):
        """run the command with its specific arguments"""
        appid = args[0]
        with admincnx(appid) as cnx:
            indexer = cnx.vreg["es"].select(self.indexer_name, cnx)
            index_name = self.config.index_name or indexer.index_name
            print(""""{}" kibana index for {}""".format(index_name, ", ".join(indexer.etypes)))
            es = indexer.get_connection()
            if not es and self.config.debug:
                print("no elasticsearch configuration found, skipping")
                return
            indexer.create_index(index_name)
            self.update_sql_data(cnx)
            if self.config.no_index:
                # do not reindex
                print("do not index es")
                return
            for _ in parallel_bulk(
                es,
                self.bulk_actions(cnx, es, indexer),
                raise_on_error=False,
                raise_on_exception=False,
            ):
                pass

    def bulk_actions(self, cnx, es, indexer):
        index_name = self.config.index_name or indexer.index_name
        etypes = self.config.etypes or indexer.etypes
        for etype in etypes:
            nb_entities = cnx.execute("Any COUNT(X) WHERE X is %s" % etype)[0][0]
            print("\n-> indexing {} {}".format(nb_entities, etype))
            progress_bar = _tqdm(total=nb_entities)
            for idx, entity in enumerate(
                indexable_entities(cnx, etype, chunksize=self.config.chunksize), 1
            ):
                serializer = entity.cw_adapt_to(self.serializer)
                json = serializer.serialize(complete=False)
                if json:
                    data = {
                        "_op_type": "index",
                        "_index": indexer.index_name,
                        "_id": serializer.es_id,
                        "_source": json,
                    }
                    yield data
                try:
                    progress_bar.update()
                except Exception:
                    pass
            cnx.info("[{}] indexed {} {} entities".format(index_name, idx, etype))

    def update_sql_data(self, cnx):
        raise NotImplementedError


class IndexESAuthoritiesKibana(IndexESKibana):
    """Create indexes and index Authorities data monitoring in Kibana"""

    name = "index-es-auth-kibana"
    indexer_name = "kibana-auth-indexer"
    serializer = "IKibanaInitiaLAuthorityIndexSerializable"

    def update_sql_data(self, cnx):
        create_kibana_authorities_sql(cnx)


class IndexESServicesKibana(IndexESKibana):
    """Create indexes and index Services data monitoring in Kibana"""

    name = "index-es-service-kibana"
    indexer_name = "kibana-service-indexer"

    def update_sql_data(self, cnx):
        pass


class IndexEsKibanaLauncher(IndexESKibana):
    """Launch IndexESAuthoritiesKibana and IndexESServicesKibana cmd"""

    name = "index-es-kibana"
    indexer_name = "kibana-service-indexer"

    def run(self, args):
        """run the command with its specific arguments"""
        logger = logging.getLogger(self.name)
        for cmd in (IndexESAuthoritiesKibana(logger), IndexESServicesKibana(logger)):
            cmd.run(args)

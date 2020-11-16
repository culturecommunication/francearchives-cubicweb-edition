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
"""cubicweb-frarchives_edition application package

Edition components for FranceArchives
"""
import os
import os.path as osp

import re
import urllib.parse

from logilab.common.decorators import monkeypatch

from cubicweb import NoResultError

from cubicweb_varnish.varnishadm import varnish_cli_connect_from_config

from cubicweb.server.hook import DataOperationMixIn, LateOperation
from cubicweb.server.sources import storages

GEONAMES_RE = re.compile(r"geonames\.org/(\d+)(?:/.+?\.html)?")

CANDIDATE_SEP = "###"

ALIGN_IMPORT_DIR = "/tmp/csv"


def geonames_id_from_url(geonameuri):
    geonameid = GEONAMES_RE.search(geonameuri)
    if not geonameid:
        return None
    else:
        return geonameid.group(1)


class IncompatibleFile(Exception):
    """Raised when CSV file is incompatible."""


def fspath_for(cnx, eid, attrname="data"):
    """Return the current file-system path for attribute `attrname` of
    entity with `eid`.
    """
    entity = cnx.entity_from_eid(eid)
    storage = cnx.repo.system_source.storage(entity.cw_etype, attrname)
    return storage.current_fs_path(entity, attrname)


@monkeypatch(storages.AddFileOp)
def postcommit_event(self):
    for filepath in self.get_data():
        FinalDeleteFileOperation.get_instance(self.cnx).add_data(("added", filepath))


@monkeypatch(storages.DeleteFileOp)  # noqa
def postcommit_event(self):  # noqa
    for filepath in self.get_data():
        FinalDeleteFileOperation.get_instance(self.cnx).add_data(("deleted", filepath))


class UnpublishFilesOp(DataOperationMixIn, LateOperation):
    """after a parent entity has been unpublished the fpath of related files
    must be removed from the published directory if the path
    is not referenced by any other published entity
    """

    def postcommit_event(self):
        sys_source = self.cnx.repo.system_source
        removed = []
        for parent, cwfile_eid, filepath, published_filepath in self.get_data():
            if self.cnx.deleted_in_transaction(cwfile_eid):
                continue
            if published_filepath in removed:
                continue
            # collect all other CWFiles referencing the same filepath (e.g cw_data attribute)
            sql_query = """
            SELECT f.cw_eid FROM cw_file f
            JOIN cw_file f1 ON f.cw_data=f1.cw_data
            WHERE f1.cw_eid =%(eid)s;
            """
            cu = sys_source.doexec(self.cnx, sql_query, {"eid": cwfile_eid})
            cwfiles_eids = [str(f[0]) for f in cu.fetchall()]
            eschema = self.cnx.vreg.schema.eschema("File")
            assert len([e for e in eschema.subjrels if not e.meta and not e.final]) == 0
            # skip 'service_image' relation as irrelevent
            # process 'image_file' relation bellow
            file_objrels = [
                e.type
                for e in eschema.objrels
                if not e.meta and e.type not in ("image_file", "output_file")
            ]
            published_parents_queries = [
                """
                (
                   DISTINCT Any X{i} WHERE X{i} {rel} F,
                   NOT X{i} identity X, X eid %(x)s,
                   X{i} in_state S{i}, S{i} name "%(st)s",
                   F eid IN (%(f)s)
                )
                """.format(
                    i=i, rel=rel
                )
                for i, rel in enumerate(file_objrels)
            ]
            # process 'image_file' relation
            image_eschema = self.cnx.vreg.schema.eschema("Image")
            # Service entity is not IWorkflowable, skip 'service_image' relation
            image_objrels = [
                e.type
                for e in image_eschema.objrels
                if not e.meta and e.type not in ("service_image")
            ]
            published_parents_queries.extend(
                [
                    """
                (
                    DISTINCT Any C{i} WHERE
                    I{i} image_file F, C{i} {rel} I{i},
                    NOT C{i} identity X, X eid %(x)s,
                    C{i} in_state CS{i}, CS name "%(st)s",
                    F eid IN (%(f)s)
                )""".format(
                        i=i, rel=rel
                    )
                    for i, rel in enumerate(image_objrels)
                ]
            )
            published_parents_query = "DISTINCT Any X WITH X BEING ({q})".format(
                q=" UNION ".join(published_parents_queries)
            )
            published_parents = self.cnx.execute(
                published_parents_query
                % {"x": parent.eid, "f": ", ".join(cwfiles_eids), "st": "wfs_cmsobject_published"}
            )
            if not published_parents:
                os.remove(published_filepath)
                removed.append(published_filepath)


class FinalDeleteFileOperation(DataOperationMixIn, LateOperation):
    def postcommit_event(self):
        fpaths = {"deleted": set(), "added": set()}
        for data in self.get_data():
            fpaths[data[0]].add(data[1])
        deleted = fpaths["deleted"].difference(fpaths["added"])
        pub_appfiles_dir = self.cnx.vreg.config.get("published-appfiles-dir")
        # thoses CWFiles have been deleted. Try to remove the fs stored files
        for filepath in deleted:
            assert isinstance(filepath, str)
            try:
                os.unlink(filepath)
            except Exception as ex:
                self.error("can't remove %s: %s" % (filepath, ex))
            # try to remove the published file even if the previous deletion fails
            if pub_appfiles_dir:
                pub_fpath = osp.join(pub_appfiles_dir, osp.basename(filepath))
                if osp.exists(pub_fpath):
                    try:
                        os.unlink(pub_fpath)
                    except Exception as ex:
                        self.error("can't remove published %s: %s" % (filepath, ex))


def get_samesas_history(cnx, complete=False):
    if complete:
        query = "SELECT * FROM sameas_history"
    else:
        query = "SELECT sameas_uri, autheid FROM sameas_history"
    return cnx.system_sql(query).fetchall()


def update_samesas_history(cnx, records):
    """
    Update `samesas_history` sql table with entites created ou deleted by users

    Args:

    - `records` is a list with a 3-tuple list:
      - sameas_uri: uri of related ExternalUri
      - autheid: eid of related Authority
      - action: 0 or 1:
         - 1 : created entity
         - 0 : deleted entity
    """
    records = [
        {"sameas_uri": sauri, "autheid": autheid, "action": act} for sauri, autheid, act in records
    ]
    cnx.cnxset.cu.executemany(
        """
        INSERT INTO sameas_history (sameas_uri, autheid, action)
        VALUES (%(sameas_uri)s, %(autheid)s, %(action)s::boolean)
        ON CONFLICT (sameas_uri, autheid) DO UPDATE SET action = EXCLUDED.action
    """,
        records,
    )


def compute_leaflet_json(cnx):
    """compute json for leaflet"""
    json_data = {"cms": [], "consultation": []}
    rset = cnx.execute(
        "Any P, PL, PLAT, PLNG, COUNT(F) GROUPBY P, PL, PLAT, PLNG "
        "WHERE I authority P, I index F, "
        "P is LocationAuthority, P latitude PLAT, P longitude PLNG, "
        "P label PL, NOT P latitude NULL"
    )
    consult_base_url = cnx.vreg.config.get("consultation-base-url")
    for eid, label, lat, lng, count in rset:
        json_data["cms"].append(
            {
                "eid": eid,
                "label": label,
                "lat": lat,
                "lng": lng,
                "dashLabel": "--" in label,
                "count": count,
                "url": cnx.build_url("location/{}".format(eid)),
            }
        )
        json_data["consultation"].append(
            {
                "eid": eid,
                "label": label,
                "lat": lat,
                "lng": lng,
                "dashLabel": "--" in label,
                "count": count,
                "url": "{}/location/{}".format(consult_base_url, eid),
            }
        )
    return json_data


def get_leaflet_cache_entities(cnx):
    """access to leaflet json stored in database"""
    try:
        return cnx.execute(
            "Any X, V WHERE X is Caches, X name %(name)s, X values V", {"name": "geomap"}
        )
    except NoResultError:
        return None


def load_leaflet_json(cnx):
    """load json data for leaflet map"""
    json_data = compute_leaflet_json(cnx)
    cache_name = "geomap"
    for instance_type in ("cms", "consultation"):
        try:
            cache = cnx.execute(
                """Any X, V WHERE X is Caches,
                   X instance_type %(instance_type)s,
                   X name %(name)s, X values V""",
                {"name": cache_name, "instance_type": instance_type},
            ).one()
        except NoResultError:
            cache = cnx.create_entity("Caches", name=cache_name, instance_type=instance_type)
        cache.cw_set(values=json_data[instance_type])


class ForbiddenPublishedTransition(Exception):
    """Asked Transaction can not be fired"""

    def __init__(self, cnx, msg):
        super(ForbiddenPublishedTransition, self).__init__({cnx._("Impossible to publish"): msg})


class VarnishPurgeMixin(object):
    def purge_varnish(self, urls):
        config = self._cw.vreg.config
        if not config.get("varnishcli-hosts"):
            return
        purge_cmd = "ban req.url ~"
        cnxs = varnish_cli_connect_from_config(config)
        for url in urls:
            for varnish_cli in cnxs:
                varnish_cli.execute(purge_cmd, "^%s" % urllib.parse.urlparse(url).path)
        for varnish_cli in cnxs:
            varnish_cli.close()


def update_suggest_es(cnx, entities):
    if cnx.vreg.config.mode == "test":
        return
    service = cnx.vreg["services"].select("reindex-suggest", cnx)
    try:
        service.index_authorities(entities)
    except Exception:
        import traceback

        traceback.print_exc()

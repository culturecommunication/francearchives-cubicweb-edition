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
import re

from logilab.common.decorators import monkeypatch

from cubicweb import NoResultError

from cubicweb.server.hook import DataOperationMixIn, LateOperation
from cubicweb.server.sources import storages

GEONAMES_RE = re.compile(r"geonames\.org/(\d+)(?:/.+?\.html)?")

CANDIDATE_SEP = "###"

ALIGN_IMPORT_DIR = "/tmp/csv"


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
def postcommit_event(self):
    for filepath in self.get_data():
        FinalDeleteFileOperation.get_instance(self.cnx).add_data(("deleted", filepath))


class FinalDeleteFileOperation(DataOperationMixIn, LateOperation):
    def postcommit_event(self):
        fpaths = {"deleted": set(), "added": set()}
        for data in self.get_data():
            fpaths[data[0]].add(data[1])
        deleted = fpaths["deleted"].difference(fpaths["added"])
        for filepath in deleted:
            assert isinstance(filepath, str)  # bytes on py2, unicode on py3
            try:
                os.unlink(filepath)
            except Exception as ex:
                self.error("can't remove %s: %s" % (filepath, ex))


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

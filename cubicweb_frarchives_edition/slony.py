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
"""Utility functions to generate slony replication files"""

from cubicweb.server.schema2sql import rschema_has_table
from cubicweb.schema import PURE_VIRTUAL_RTYPES
from cubicweb.server.sqlutils import SQL_PREFIX


def create_master(cnx, sqlschema="published", skip_entities=(), skip_relations=()):
    """
    Generate slonik configuration/command file to setup the master node
    """
    repo = cnx.repo
    schema = repo.schema
    appid = repo.config.appid
    dbcfg = repo.system_source.config
    output = []

    tablenames = [
        x[0]
        for x in cnx.system_sql(
            "select tablename from pg_tables " "where schemaname='%s'" % sqlschema
        )
    ]

    clustername = "cw_%s_cluster" % appid
    output.append("cluster name = %s;" % clustername)

    master = "dbname=%(db-name)s host=%(db-host)s " "port=%(db-port)s user=%(db-user)s" % dbcfg
    output.append("node 1 admin conninfo='%s'" % master)

    output.append("init cluster (id=1, comment='Master Node');")
    output.append("create set (id=1, origin=1, " "comment='tables for cw app %s');" % appid)

    idx = 1
    output.append(
        "set add table (set id=1, origin=1, id=%(idx)s, "
        "fully qualified name='public.entities');" % {"idx": idx}
    )
    idx += 1
    for etype in sorted(schema.entities()):
        eschema = schema.eschema(etype)
        if eschema.final or eschema.type in skip_entities:
            continue
        table = SQL_PREFIX + eschema.type.lower()
        schemaname = table in tablenames and sqlschema or "public"
        output.append(
            "set add table (set id=1, origin=1, id=%(idx)s, "
            "fully qualified name='%(schema)s.%(table)s', "
            "comment='table for entity %(entity)s');"
            % {
                "idx": idx,
                "table": table,
                "entity": eschema.type,
                "schema": schemaname,
            }
        )
        idx += 1

    for rtype in sorted(schema.relations()):
        rschema = schema.rschema(rtype)
        if rschema_has_table(rschema, set(skip_relations) | PURE_VIRTUAL_RTYPES):
            table = "%s_relation" % rschema.type
            schemaname = table in tablenames and sqlschema or "public"
            output.append(
                "set add table (set id=1, origin=1, id=%(idx)s, "
                "fully qualified name='%(schema)s.%(table)s', "
                "comment='table for relation %(rel)s');"
                % {
                    "idx": idx,
                    "table": table,
                    "rel": rschema.type,
                    "schema": schemaname,
                }
            )
        idx += 1
    return "\n".join(output)


def start_slave(repo, slaveid, slavedbcfg):
    """
    Generate slonik configuration/command to start a replication node

    Also return the slon cmd line to start the replication daemon on the node
    """
    appid = repo.config.appid
    dbcfg = repo.system_source.config.copy()
    master = "dbname=%(db-name)s host=%(db-host)s " "port=%(db-port)s user=%(db-user)s" % dbcfg
    clustername = "cw_%s_cluster" % appid
    dbcfg.update(slavedbcfg)
    slave = "dbname=%(db-name)s host=%(db-host)s " "port=%(db-port)s user=%(db-user)s" % dbcfg

    output = []
    output.append("cluster name = %s" % clustername)
    output.append("node 1 admin conninfo='%s';" % (master))
    output.append("node %d admin conninfo='%s';" % (slaveid, slave))
    output.append("subscribe set (id=1, provider=1, receiver=%d, forward=no);" % (slaveid))

    sloncmdline = 'slon %s "%s"' % (clustername, slave)

    return "\n".join(output), sloncmdline


def add_slave(repo, slaveid, slavedbcfg):
    """
    Generate slonik configuration/command to add a node to the replication cluster
    """
    appid = repo.config.appid
    dbcfg = repo.system_source.config.copy()
    output = []
    master = "dbname=%(db-name)s host=%(db-host)s " "port=%(db-port)s user=%(db-user)s" % dbcfg
    clustername = "cw_%s_cluster" % appid

    # create the slave nodes
    dbcfg.update(slavedbcfg)
    slave = "dbname=%(db-name)s host=%(db-host)s " "port=%(db-port)s user=%(db-user)s" % dbcfg
    output.append("cluster name = %s" % clustername)
    output.append("node 1 admin conninfo='%s';" % (master))
    output.append("node %d admin conninfo='%s';" % (slaveid, slave))

    output.append("store node (id=%d, comment = 'Slave node', event node=1);" % (slaveid))
    output.append("store path (server=1, client=%d, conninfo='%s');" % (slaveid, master))
    output.append("store path (server=%d, client=1, conninfo='%s');" % (slaveid, slave))

    return "\n".join(output)

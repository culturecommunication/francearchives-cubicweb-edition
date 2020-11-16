# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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
# migration utils


from jinja2 import Environment, PackageLoader

from cubicweb_frarchives_edition.mviews import formatted_ignored_cwproperties


def get_published_tables(cnx, etypes, skipped_etypes=(), skipped_relations=()):
    rtypes = {}
    rnames = set()
    skipped_relations = ("in_state",)  # in_state is handled separately
    for etype in etypes:
        if etype in skipped_etypes:
            continue
        eschema = cnx.repo.schema[etype]
        rtypes[etype] = {}
        for rschema, targetschemas, role in eschema.relation_definitions():
            # r.rule is not None for computed relations
            if (
                rschema.final
                or rschema.inlined
                or rschema.meta
                or rschema.rule is not None
                or rschema.type in skipped_relations
            ):
                continue
            rtypes[etype].setdefault(rschema.type, []).append(role)
            rnames.add(rschema.type)
    return etypes, rtypes, rnames


def update_published_triggers(cnx, etypes, sql=None, sqlschema="published", bootstrap=True):
    """Create (or replace) SQL triggers to handle filtered copied of CMS
    entities postgresql tables (and the resuired relations) that are
    in the wfs_cmsobject_published WF state.
    """
    if sql is None:
        sql = cnx.system_sql
    skipped_etypes = ("CWProperty", "CWUser")  # idem
    skipped_relations = ("in_state",)  # in_state is handled separately
    etypes, rtypes, rnames = get_published_tables(cnx, etypes, skipped_etypes, skipped_relations)
    env = Environment(
        loader=PackageLoader("cubicweb_frarchives_edition", "templates"),
    )
    env.filters["sqlstr"] = lambda x: "'{}'".format(x)
    template = env.get_template("published.sql")
    sqlcode = template.render(
        schema=sqlschema,
        etypes=etypes,
        rtypes=rtypes,
        rnames=rnames,
        ignored_cwproperties=formatted_ignored_cwproperties(cnx),
    )
    if sql:
        print(sqlcode)
        sql(sqlcode)

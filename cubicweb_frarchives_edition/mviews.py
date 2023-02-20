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
import os.path as osp
from yams.constraints import UniqueConstraint

from jinja2 import Environment, PackageLoader

from cubicweb.schema import RQLExpression
from cubicweb.server.ssplanner import prepare_plan
from cubicweb.server.sqlutils import SQL_PREFIX

from cubicweb_francearchives import CMS_OBJECTS
from cubicweb_francearchives import CMS_I18N_OBJECTS
from cubicweb_francearchives.utils import setup_published_schema


class MVRQLExpression(RQLExpression):
    predefined_variables = "X"

    def __init__(self, etype, expression):
        self.etype = etype
        expression = "X is %s, %s" % (etype, expression)
        super(MVRQLExpression, self).__init__(expression, "X", None)


exprs = [
    MVRQLExpression(etype, 'X in_state S, S name "wfs_cmsobject_published"')
    for etype in CMS_OBJECTS + ("FindingAid", "GlossaryTerm", "FaqItem") + CMS_I18N_OBJECTS
]
exprs.append(MVRQLExpression("CWUser", 'X login IN ("admin", "anon")'))


def formatted_ignored_cwproperties(cnx):
    deps = lambda cube: set(cnx.vreg.config.expand_cubes((cube,)))  # noqa
    filtered_out = list(deps("frarchives_edition") - deps("francearchives"))
    return ",".join("'system.version.{}'".format(prop) for prop in filtered_out)


def filter_cwproperty_expr(cnx):
    cube_restr = formatted_ignored_cwproperties(cnx)
    return MVRQLExpression("CWProperty", "NOT X pkey IN ({})".format(cube_restr))


def bootstrap_view(cnx, expression, sqlschema="published"):
    """Initialize views (tables) for an entity type from a rql query.

    The given rql must generate a rset of one column of a single
    CWEtype.

    Returns tuple of string: SQL statements to fill the
    view in the given sql schema.

    """
    rql = expression.minimal_rql
    rqlst = cnx.vreg.parse(cnx, rql)
    assert len(expression.mainvars) == 1
    var = list(expression.mainvars)[0]

    # get the SQL for this RQL query
    # XXX too bad no simpler solution seems to exist
    querier = cnx.repo.querier
    source = cnx.repo.sources_by_uri["system"]
    cnx.vreg.compute_var_types(cnx, rqlst, {})
    cnx.vreg.rqlhelper.annotate(rqlst)
    plan = querier.plan_factory(rqlst, {}, cnx)
    plan.cache_key = None
    prepare_plan(plan, querier.schema, cnx.vreg.rqlhelper)
    if len(plan.steps) != 1:
        raise ValueError("Invalid RQL query")
    sql, args, _ = source._rql_sqlgen.generate(rqlst, {})

    table = "%s%s" % (SQL_PREFIX, expression.etype.lower())
    matsql = [
        "insert into %(schema)s.%(table)s (SELECT _%(var)s.* FROM %(stm)s);"
        % {"schema": sqlschema, "table": table, "var": var, "stm": sql.split("FROM", 1)[1]},
    ]
    matsql += build_indexes(cnx, expression.etype, sqlschema)
    return matsql, args


def build_indexes(cnx, etype, sqlschema="published"):
    # build indexes
    eschema = cnx.vreg.schema.eschema(etype)
    dbhelper = cnx.repo.system_source.dbhelper
    attrs = list(eschema.attribute_definitions())
    attrs += [
        (rschema, None)
        for rschema in eschema.subject_relations()
        if not rschema.final and rschema.inlined
    ]
    matsql = []
    table = "%s%s" % (SQL_PREFIX, etype.lower())
    for rschema, attrschema in attrs:
        if attrschema is None or eschema.rdef(rschema).indexed:
            matsql.append(
                dbhelper.sql_create_index(sqlschema + "." + table, SQL_PREFIX + rschema.type) + ";"
            )
        if attrschema and any(
            isinstance(cstr, UniqueConstraint) for cstr in eschema.rdef(rschema).constraints
        ):
            matsql.append(
                dbhelper.sql_create_index(
                    sqlschema + "." + table, SQL_PREFIX + rschema.type, unique=True
                )
                + ";"
            )
    matsql.append(dbhelper.sql_create_index(sqlschema + "." + table, SQL_PREFIX + "eid"))
    return matsql


def get_published_tables(cnx, skipped_etypes=(), skipped_relations=()):
    """Return the lists of entiies and relations to be used to manage the
    dedicated Postgresql namespace implementing the "materialiazed
    views" mecanisme for published entities
    """
    if not any(e.etype == "CWProperty" for e in exprs):
        exprs.append(filter_cwproperty_expr(cnx))
    etypes = [eexp.etype for eexp in exprs if eexp.etype not in skipped_etypes]
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


def setup_published_triggers(cnx, sql=None, sqlschema="published", dumpfiles=None, bootstrap=True):
    """Create (or replace) SQL triggers to handle filtered copied of CMS
    entities postgresql tables (and the resuired relations) that are
    in the wfs_cmsobject_published WF state.
    """
    # ensure that Section required for the main index view are published
    for s in cnx.execute(
        "Any S WHERE X is CssImage, " 'X cssid LIKE "hero-%%", ' "X cssimage_of S"
    ).entities():
        s.cw_adapt_to("IWorkflowable").fire_transition_if_possible("wft_cmsobject_publish")

    # create tables in the dedicated namespace (schema) for entities
    # that have a publication workflow and their relations
    if sql is None:
        sql = cnx.system_sql

    skipped_etypes = ("CWProperty", "CWUser")  # idem
    skipped_relations = ("in_state",)  # in_state is handled separately
    etypes, rtypes, rnames = get_published_tables(cnx, skipped_etypes, skipped_relations)

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
    if dumpfiles:
        with open(osp.join(dumpfiles, "published.sql"), "w") as fobj:
            fobj.write(sqlcode)
    if sql:
        sql(sqlcode)
    if bootstrap:
        # bootstrap already published stuff
        sqlcode = []
        for rqlexpr in exprs:
            sqls, args = bootstrap_view(cnx, rqlexpr)
            sqlcode.append("\n".join(sqls) % args + "\n")
            if sql:
                sql("\n".join(sqls), args)
        if dumpfiles:
            with open(osp.join(dumpfiles, "bootstrap.sql"), "w") as fobj:
                fobj.write("\n".join(sqlcode))


def migration_update_trigger(cnx):
    skipped_relations = ("in_state",)  # in_state is useless there
    etypes, _, rnames = get_published_tables(
        cnx, skipped_relations=skipped_relations, skipped_etypes=("CWUser",)
    )
    etypes = set(etypes) | {"FAComponent", "CWUser"}
    setup_published_schema(cnx.system_sql, etypes, rnames, dumpfiles="/tmp")
    setup_published_triggers(cnx, dumpfiles="/tmp", bootstrap=False)
    cnx.commit()

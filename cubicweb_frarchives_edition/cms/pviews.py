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

import logging

from pyramid import httpexceptions, security
from pyramid.view import view_config
from pyramid.response import Response
import redis
import rq

from cubicweb import ConfigurationError

from cubicweb_jsonschema.resources.entities import EntityResource, ETypeResource
from cubicweb_jsonschema.api import jsonapi_error, JSONBadRequest

from cubicweb_francearchives.pviews.faroutes import card_view

from cubicweb_frarchives_edition.entities import section as section_edition
from cubicweb_frarchives_edition.api import json_config


LOG = logging.getLogger(__name__)


@json_config(
    route_name="cmssection",
    request_method="POST",
)
def move_section(request):
    req = request.cw_request
    target = request.json_body["target"]
    child = request.json_body["child"]
    if child == target:
        raise JSONBadRequest(jsonapi_error(status=401, details=req._("not allowed")))
    new_order = request.json_body["newOrder"]
    already_a_child = req.execute(
        "Any C WHERE S children C, " "S eid %(s)s, C eid %(c)s", {"s": target, "c": child}
    )
    if not already_a_child:
        req.execute(
            "SET S children S2 WHERE S eid %(s)s, S2 eid %(s2)s", {"s": target, "s2": child}
        )
    section_edition.move_child(req, target, child, new_order)
    return None


@json_config(
    route_name="cwentities",
    context=ETypeResource,
    request_method="GET",
    request_param="attrs",
)
def get_entities_json(context, request):
    """Render multiple entities in JSON format."""
    entities = []
    for entity in context.rset.entities():
        serializer = entity.cw_adapt_to("IJSONSchema")
        entities.append(serializer.serialize(attrs=request.params["attrs"].split(",")))
    return {"data": entities}


@json_config(route_name="service")
def service(request):
    code = request.matchdict["code"]
    vreg = request.registry["cubicweb.registry"]
    req = request.cw_request
    Service = vreg["etypes"].etype_class("Service")
    st = Service.fetch_rqlst(request.cw_cnx.user, ordermethod=None)
    if code == "departements":
        dpt = request.params.get("dpt")
        if not dpt:
            raise httpexceptions.HTTPNotFound()
        st.add_constant_restriction(st.get_variable("X"), "dpt_code", "d", "Substitute")
        st.add_constant_restriction(st.get_variable("X"), "level", ("level-C", "level-D"), "String")
        rset = req.execute(st.as_string(), {"d": dpt.upper()})
    else:
        st.add_constant_restriction(st.get_variable("X"), "eid", "c", "Substitute")
        rset = req.execute(st.as_string(), {"c": code})
    if not rset:
        raise httpexceptions.HTTPNotFound()
    entities = []
    done = set()
    for entity in rset.entities():
        if entity.eid not in done:
            entities.append(entity.cw_adapt_to("IJSONSchema").serialize())
            done.add(entity.eid)
        for annex in entity.reverse_annex_of:
            if annex.eid not in done:
                entities.append(annex.cw_adapt_to("IJSONSchema").serialize())
                done.add(annex.eid)
    return {"data": entities}


@view_config(
    route_name="downloadable",
    context=EntityResource,
)
def download(context, request):
    """Download view for entities with BFSS managed "data" attribute."""
    entity = context.rset.one()  # May raise HTTPNotFound.
    adapted = entity.cw_adapt_to("IDownloadable")
    return Response(
        adapted.download_data(),
        content_type=adapted.download_content_type().encode("utf-8"),
        content_disposition='attachment; filename="%s"' % adapted.download_file_name(),
    )


@view_config(route_name="logout", effective_principals=security.Authenticated)
def logout(request):
    headers = security.forget(request)
    new_path = request.params.get("postlogin_path", "")
    url = request.cw_request.build_url(new_path)
    raise httpexceptions.HTTPSeeOther(url, headers=headers)


@view_config(
    route_name="non-repris",
)
def unclassified_section(request):
    rset = request.cw_request.execute("Any X WHERE X is Section, X name %(n)s", {"n": "non-repris"})
    return httpexceptions.HTTPFound(location=rset.one().absolute_url())


@view_config(route_name="siteinfo")
def siteinfo(request):
    props = request.cw_request.execute(
        "Any K,V WHERE X is CWProperty, X pkey K, " 'X value V, X pkey LIKE "system.version%"'
    ).rows
    response = "\n".join("{}: {}".format(*row) for row in props)
    return Response(response, content_type="text/plain")


@json_config(route_name="cwusers")
def cwusers(request):
    req = request.cw_request
    rset = req.execute(
        "DISTINCT Any X, L, F, S ORDERBY 2 "
        "WHERE X is CWUser, "
        'X login L, NOT X login IN ("anon", "admin"), '
        "X firstname F, X surname S"
    )
    if not rset:
        raise httpexceptions.HTTPNotFound()
    entities = []
    for e in rset.entities():
        data = e.cw_adapt_to("IJSONSchema").serialize()
        data["in_group_name"] = [req._(g) for g in e.groups]
        entities.append(data)
    return entities


@json_config(route_name="faservices")
def faservices(request):
    req = request.cw_request
    rset = req.execute(
        """DISTINCT Any X, N, N2, SSN, CODE
        GROUPBY X, N, N2, SSN, CODE ORDERBY CODE ASC, N, N2, SSN
        WHERE X is Service, X code CODE,
        X name N, X name2 N2, X short_name SSN,
        EXISTS(F service X, F is FindingAid)
        """
    )
    entities = []
    for eid, name, name2, short_name, code in rset:
        entities.append({"code": code, "name": "{} ({})".format(name or name2, code)})
    return entities


@json_config(route_name="faforservice")
def faforservice(request):
    req = request.cw_request
    service_code = request.params.get("service")
    if not service_code:
        # XXX add an explicite error
        raise httpexceptions.HTTPNotFound()
    rset = req.execute(
        """
        Any F, EADID, SID, TITLEPROPER, UNITTITLE, UNITID, FNAME, CDATE, SNAME, OAI
        ORDERBY CDATE DESC WHERE F is FindingAid,
        F service S, S code %(code)s,
        F eadid EADID,
        F fa_header FA, FA titleproper TITLEPROPER,
        F did D, D unittitle UNITTITLE, D unitid UNITID,
        F stable_id SID,
        F findingaid_support FS?, FS data_name FNAME,
        F creation_date CDATE,
        F oai_id OAI,
        F in_state ST?, ST name SNAME
        """,
        {"code": service_code},
    )
    entities = []
    _ = req._
    for (
        eid,
        eadid,
        stable_id,
        titleproper,
        unittitle,
        unitid,
        filename,
        creation_date,
        status,
        oai,
    ) in rset:
        name = titleproper or unittitle or unitid or "???"
        entities.append(
            {
                "eid": eid,
                "eadid": eadid,
                "stable_id": stable_id,
                "name": name,
                "filename": filename,
                "import": "OAI" if bool(oai) else "ZIP",
                "creation_date": creation_date,
                "url": [eadid, req.build_url("findingaid/{}".format(stable_id))],
                "status": _(status),
            }
        )

    return entities


@json_config(route_name="rqtasks")
def rqtasks(request):
    req = request.cw_request
    rset = req.execute(
        "DISTINCT Any X, SN, D ORDERBY D desc "
        "WHERE X is RqTask, "
        "X creation_date D, "
        "X status SN"
    )
    if not rset:
        raise httpexceptions.HTTPNotFound()
    entities = [e.cw_adapt_to("IJSONSchema").serialize() for e in rset.entities()]
    return {"data": entities}


def rq_tween_factory(handler, registry):
    def rq_tween(request):
        with rq.Connection(registry.settings["rq.redis"]):
            return handler(request)

    return rq_tween


def includeme(config):
    config.add_route("logout", "/logout")
    config.add_route("siteinfo", "/siteinfo")
    config.add_route("non-repris", "/non-repris")
    config.add_route("entrypoint-card-alert", "/{wiki:alert}")
    config.add_view(card_view, route_name="entrypoint-card-alert", request_method=("GET", "HEAD"))
    config.add_route("cmssection", "/section", strict_accept="application/json")
    config.add_route("service", "/annuaire/{code}", strict_accept="application/json")
    config.add_route("cwusers", "/cwusers", strict_accept="application/json")
    config.add_route("rqtasks", "/rqtasks", strict_accept="application/json")
    config.add_route("faservices", "/faservices", strict_accept="application/json")
    config.add_route("faforservice", "/faforservice", strict_accept="application/json")
    config.scan(__name__)
    cwconfig = config.registry["cubicweb.config"]
    if cwconfig.mode == "test" and "frarchives_edition.rq.redis" in config.registry.settings:
        config.registry.settings["rq.redis"] = config.registry.settings[
            "frarchives_edition.rq.redis"
        ]
    else:
        redis_url = config.registry.settings.get("rq.redis_url")
        if redis_url is None:
            raise ConfigurationError(
                "could not start rq: `rq.redis_url` is missing from " "pyramid.ini file"
            )
        config.registry.settings["rq.redis"] = redis.StrictRedis.from_url(redis_url)
    config.add_tween("cubicweb_frarchives_edition.cms.pviews.rq_tween_factory")

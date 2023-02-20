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

import datetime

import logging
import os
import os.path as osp


from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search, Q, query as dsl_query

from pyramid import httpexceptions, security
from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound
import redis
import rq

from cubicweb import ConfigurationError

from cubicweb_jsonschema.resources.entities import EntityResource, ETypeResource
from cubicweb_jsonschema.api import jsonapi_error, JSONBadRequest
from cubicweb_elasticsearch.es import get_connection

from cubicweb_francearchives.dataimport.oai import parse_oai_url
from cubicweb_francearchives.pviews.faroutes import card_view
from cubicweb_francearchives.pviews.cwroutes import download_s3_view
from cubicweb_francearchives.utils import (
    get_autorities_by_label,
    get_autorities_by_eid,
    register_blacklisted_authorities,
)
from cubicweb_frarchives_edition import AUTH_URL_PATTERN
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
        Any F, EADID, SID, TITLEPROPER, UNITTITLE, UNITID, FNAME,
            CDATE, MDATE, SNAME, OAI, APE_FNAME, APE_HASH
        ORDERBY CDATE DESC WHERE F is FindingAid,
        F service S, S code %(code)s,
        F eadid EADID,
        F fa_header FA, FA titleproper TITLEPROPER,
        F did D, D unittitle UNITTITLE, D unitid UNITID,
        F stable_id SID,
        F findingaid_support FS?,
        FS data_name FNAME,
        F creation_date CDATE,
        F modification_date MDATE,
        F oai_id OAI,
        F ape_ead_file APS?,
        APS data_name APE_FNAME,
        APS data_hash APE_HASH,
        F in_state ST?, ST name SNAME
        """,
        {"code": service_code},
    )
    last_harvest_rset = req.execute(
        """
        Any OIT, URL ORDERBY OIT DESC LIMIT 1 WHERE
        S code %(code)s,
        R service S, R is OAIRepository, OIT oai_repository R, R url URL,
        OIT in_state ST, ST name "wfs_oaiimport_completed"
        """,
        {"code": service_code},
    )
    if last_harvest_rset:
        oai_eid, oai_url = last_harvest_rset[0]
        oai_import = req.entity_from_eid(oai_eid)
        wf = oai_import.cw_adapt_to("IWorkflowable")
        last_harvest = wf.latest_trinfo().creation_date
        base_url, params = parse_oai_url(oai_url.strip())
        prefix = params.get("metadataPrefix")
        prefix = "ead" if prefix != "oai_dc" else "dc"
    else:
        last_harvest = None
        prefix = None
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
        modification_date,
        status,
        oai,
        ape_fname,
        ape_hash,
    ) in rset:
        name = titleproper or unittitle or unitid or "???"
        if oai:
            # for now only consider IR  from metaPrefix=ead
            key = f"file/s3/{service_code}/oaipmh/ead/{filename}"
        else:
            # do not process csv files, only imported by ead
            key = f"file/s3/{service_code}/{filename}"
        if ape_hash:
            ape_key = req.build_url(f"file/{ape_hash}/ape-ead/{service_code}/{ape_fname}")
        else:
            ape_key = ""
        entities.append(
            {
                "eid": eid,
                "eadid": eadid,
                "stable_id": stable_id,
                "name": name,
                "filename": [filename, req.build_url(key)] if filename else ["", ""],
                "import": "OAI" if bool(oai) else "ZIP",
                "creation_date": creation_date,
                "modification_date": modification_date,
                "harvest_date": last_harvest if bool(oai) else None,
                "url": [eadid, req.build_url("findingaid/{}".format(stable_id))],
                "ape_ead": [ape_fname, ape_key] if ape_key else ["", ""],
                "status": _(status),
            }
        )

    return entities


@json_config(route_name="rqtasks")
def rqtasks(request):
    req = request.cw_request
    today = datetime.datetime.today()
    rset = req.execute(
        """DISTINCT Any X, SN, D ORDERBY D desc
           WHERE X is RqTask, X status SN,
           X creation_date D, X creation_date >= %(last_year)s
        """,
        {"last_year": today - datetime.timedelta(356)},
    )
    if not rset:
        return {"data": []}
        # FIXME handle httpexceptions in the task code
        # raise httpexceptions.HTTPNotFound()
    entities = [e.cw_adapt_to("IJSONSchema").serialize() for e in rset.entities()]
    return {"data": entities}


def rq_tween_factory(handler, registry):
    def rq_tween(request):
        with rq.Connection(registry.settings["rq.redis"]):
            return handler(request)

    return rq_tween


@view_config(route_name="s3-oai", request_method=("GET", "HEAD"))
def oai_s3_download_view(request):
    cwconfig = request.registry["cubicweb.config"]
    filepath = osp.join(
        cwconfig["ead-services-dir"],
        request.matchdict["servicecode"],
        "oaipmh",
        request.matchdict["prefix"],
        request.matchdict["basename"],
    )
    return download_s3_view(filepath)


@view_config(route_name="s3-xml", request_method=("GET", "HEAD"))
def xml_s3_download_view(request):
    cwconfig = request.registry["cubicweb.config"]
    filepath = osp.join(
        cwconfig["ead-services-dir"],
        request.matchdict["servicecode"],
        request.matchdict["basename"],
    )
    return download_s3_view(filepath)


@view_config(route_name="s3-csv", request_method=("GET", "HEAD"))
def csv_s3_download_view(request):
    """
    On CMS display unpublished images/files by fallback to .hidden
    """
    filepath = "{hash}_{basename}".format(**request.matchdict)
    return download_s3_view(filepath)


@view_config(route_name="s3-cms", request_method=("GET", "HEAD"))
def s3_download_view(request):
    """
    On CMS display unpublished images/files by fallback to .hidden
    """
    filepath = "{hash}_{basename}".format(**request.matchdict)
    basename = request.matchdict["basename"]
    if "_nomina_" in basename and basename.endswith("csv"):
        # XXX find a better way
        data_hash = request.matchdict["hash"]
        cwreq = request.cw_request
        rset = cwreq.execute(f"Any FSPATH(D) WHERE F data_hash '{data_hash}', F data D")
        if rset:
            filepath = rset[0][0].getvalue().decode("utf-8")
    try:
        response = download_s3_view(filepath)
        # cubicweb_francearchives.pviews.cwroutes.3_download_view don't raise, but
        # return an HTTPNotFound
        if isinstance(response, HTTPNotFound):
            raise HTTPNotFound()
        return response
    except HTTPNotFound:
        # TODO use secret key to improve security of .hidden
        return download_s3_view(".hidden/" + filepath)


@json_config(route_name="get-blacklisted")
def get_blacklisted_authorities(request):
    req = request.cw_request
    query = """SELECT label FROM blacklisted_authorities ORDER BY label DESC"""
    data = [{"label": label} for label, in req.cnx.system_sql(query).fetchall()]
    return data


@json_config(route_name="add-blacklisted")
def add_blacklisted_authority(request):
    req = request.cw_request
    data = request.json_body
    register_blacklisted_authorities(req.cnx, data["label"])


@json_config(route_name="remove-blacklisted")
def remove_blacklisted_authority(request):
    req = request.cw_request
    data = request.json_body
    req.cnx.system_sql(
        """DELETE FROM blacklisted_authorities
           WHERE label=%(label)s""",
        data,
    )
    req.cnx.commit()


@json_config(route_name="show-blacklisted-candidates")
def show_blacklisted_candidates(request):
    req = request.cw_request
    data = request.json_body
    label = data["label"]
    match = AUTH_URL_PATTERN.match(label)
    if match:
        res = get_autorities_by_eid(req, match["eid"])
        if res:
            return res
    res = get_autorities_by_label(req, label, auth_etypes="SubjectAuthority")
    return res


@json_config(route_name="sectionthemes")
def sectionthemes(request):
    req = request.cw_request
    section_eid = request.matchdict["eid"]
    entities = {"available": [], "selected": []}
    # retrieve all children sections
    es = get_connection(req.vreg.config)
    if not es:
        req.error("no elastisearch connection available")
        return entities
    index_name = req.vreg.config["index-name"]
    content_eids = []
    # retrive all children of the sections with a related authority
    # unfortunately we can't filter on the authority etype
    search = Search(
        index="{}_all".format(index_name),
        extra={"size": 10000},
    )
    must = [{"match": {"ancestors": section_eid}}, Q("exists", field="index_entries")]
    search.query = dsl_query.Bool(must=must)
    try:
        response = search.execute()
    except NotFoundError:
        return entities
    content_eids = [str(r.eid) for r in response]
    if not content_eids:
        return entities
    # retrieve all related subject authorities
    rset = req.execute(
        """DISTINCT Any A, L WHERE Y eid IN (%(e)s),
        Y related_authority A,
        A is SubjectAuthority, A label L"""
        % {"e": ", ".join(content_eids)},
    )
    if not rset:
        return entities
    index_name = req.vreg.config["index-name"]
    eids = [row[0] for row in rset]
    search = Search(
        index="{}_suggest".format(index_name),
        extra={"size": 10000},
    ).sort("-siteres")
    must = [
        {"terms": {"eid": eids}},
    ]
    search.query = dsl_query.Bool(must=must)
    try:
        response = search.execute()
    except NotFoundError:
        return []
    selected_themes = {
        row[0]: [row[1], row[2]]
        for row in req.execute(
            """Any A, OA, O WHERE X eid %(e)s, X section_themes OA,
               OA subject_entity A, OA order O""",
            {"e": section_eid},
        )
    }
    for result in response:
        ordered_data = selected_themes.get(result.eid)
        if ordered_data:
            entities["selected"].append(
                {
                    "count": result.siteres,
                    "label": [result.text, req.build_url(f"subject/{result.eid}")],
                    "order": ordered_data[1],
                    "eid": ordered_data[0],
                }
            )
        else:
            entities["available"].append(
                {
                    "count": result.siteres,
                    "label": [result.text, req.build_url(f"subject/{result.eid}")],
                    "eid": result.eid,
                }
            )
    return entities


@json_config(route_name="add-sectiontheme")
def add_sectiontheme(request):
    req = request.cw_request
    req.create_entity(
        "OrderedSubjectAuthority",
        order=0,
        subject_entity=request.json_body["eid"],
        reverse_section_themes=request.matchdict["eid"],
    )


@json_config(route_name="modify-subjecttheme")
def modify_subjecttheme(request):
    req = request.cw_request
    # should we recalculate all orders ?
    req.execute(
        """SET X order %(o)s WHERE X eid %(eid)s""",
        {"eid": request.matchdict["eid"], "o": request.json_body["order"]},
    )


@json_config(route_name="delete-sectiontheme")
def delete_sectiontheme(request):
    req = request.cw_request
    req.execute(
        """DELETE OrderedSubjectAuthority OA WHERE OA eid %(subj)s""",
        {"subj": request.json_body["eid"]},
    )


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
    config.add_route("get-blacklisted", "/get-blacklisted", strict_accept="application/json")
    config.add_route("add-blacklisted", "/add-blacklisted", strict_accept="application/json")
    config.add_route("remove-blacklisted", "/remove-blacklisted", strict_accept="application/json")
    config.add_route(
        "show-blacklisted-candidates",
        "/show-blacklisted-candidates",
        strict_accept="application/json",
    )
    config.add_route(
        "sectionthemes",
        "/sectionthemes/{eid}",
        strict_accept="application/json",
    )
    config.add_route(
        "add-sectiontheme",
        "/add-sectiontheme/{eid}",
        strict_accept="application/json",
    )
    config.add_route(
        "delete-sectiontheme",
        "/delete-sectiontheme/{eid}",
        strict_accept="application/json",
    )
    config.add_route(
        "modify-subjecttheme",
        "/modify-subjecttheme/{eid}",
        strict_accept="application/json",
    )
    config.scan(__name__)
    cwconfig = config.registry["cubicweb.config"]
    if os.getenv("AWS_S3_BUCKET_NAME"):
        config.add_route("s3-cms", "/file/{hash}/{basename}")
        config.add_route("s3-oai", "/file/s3/{servicecode}/oaipmh/{prefix}/{basename}")
        config.add_route("s3-xml", "/file/s3/{servicecode}/{basename}")
        config.add_route("s3-csv", "/file/s3/{hash}/{basename}")
    else:
        # required because pyramid needs to find routes declared in @view_config - unused
        config.add_route("s3-cms", "/next/file/{hash}/{basename}")
        config.add_route("s3-oai", "/file/s3/{servicecode}/oaipmh/{prefix}/{basename}")
        config.add_route("s3-nomina", "/next/file/s3/{servicecode}/oaipmh/{prefix}/{basename}")
        config.add_route("s3-xml", "/next/file/s3/{servicecode}/{basename}")
        config.add_route("s3-csv", "/next/file/s3/{hash}/{basename}")
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

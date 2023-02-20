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


from pyramid import httpexceptions
from pyramid.renderers import render
from pyramid.response import Response

from pyramid.view import view_config

from cubicweb import ValidationError
from cubicweb_jsonschema.api import (
    LOG,
    JSONBadRequest,
    jsonapi_error,
)

from cubicweb_jsonschema.api.entities import allow_for_entity_relation
from cubicweb_frarchives_edition import update_suggest_es
from cubicweb_frarchives_edition.api import json_config
from cubicweb_frarchives_edition.faapi.resources import (
    FAComponentResource,
    FindingaidResource,
    RelatedAuthorityResource,
    IndexResources,
    SameAsResource,
    AuthorityResource,
)


@json_config(
    route_name="frarchives_edition.faapi",
    context=AuthorityResource,
    request_method=("POST"),
    name="_group",
)
def group_authorities(context, request):
    body = request.json_body
    try:
        all_grouped_auths = context.entity.group(body)
    except Exception as error:
        raise httpexceptions.HTTPBadRequest(error)
    # update suggest for context.entity
    cnx = request.cw_cnx
    update_suggest_es(cnx, all_grouped_auths)
    # remove all cnx.transaction_data cache
    request.cw_cnx.drop_entity_cache()
    url = context.entity.absolute_url()
    raise httpexceptions.HTTPCreated(
        location=url,
        content_type="application/json; charset=UTF-8",
        body=render("json", {"location": url}),
    )


@json_config(
    route_name="frarchives_edition.faapi",
    context=IndexResources,
    request_method=("GET", "HEAD"),
)
def get_fa_indexes(context, request):
    return [
        {
            "label": e.label,
            "type": e.type,
            "eid": e.eid,
            # 'alignments': [ext.uri for ext in e.authority[0].same_as],
            "authorityUrl": e.authority_url,
        }
        for e in context.rset.entities()
    ]


@json_config(
    route_name="frarchives_edition.faapi",
    context=RelatedAuthorityResource,
    request_method="POST",
)
def add_new_authority(context, request):
    oldauth = context.index.authority
    auth = context.index.new_authority()
    # update suggest for old and new authorties
    # as SuggestIndexEsOperation is called too early
    # and the new authority has not all indexed documents yet
    if oldauth:
        cnx = request.cw_cnx
        update_suggest_es(cnx, [oldauth[0], auth])
    url = auth.absolute_url()
    raise httpexceptions.HTTPCreated(
        location=url,
        content_type="application/json; charset=UTF-8",
        body=render("json", {"location": url}),
    )


@json_config(
    route_name="frarchives_edition.faapi",
    context=SameAsResource,
    request_method="GET",
)
def get_same_as(context, request):
    cnx = request.cw_cnx
    entity = context.authority
    rset = cnx.execute(
        """Any X,AA, L, U, S ORDERBY AA DESC WHERE E eid %(eid)s,
           E same_as X, X creation_date AA,
           X source S, X label L, X uri U,
           X is ET, ET name IN ("ExternalUri", "ExternalId")""",
        {"eid": entity.eid},
    )
    vreg = request.registry["cubicweb.registry"]
    rtype, role = ("same_as", "subject")
    mapper = vreg["mappers"].select(
        "jsonschema.collection",
        request.cw_request,
        rtype=rtype,
        role=role,
    )
    request.response.allow = allow_for_entity_relation(entity, rtype, role)
    return mapper.serialize(rset.entities())


@json_config(
    route_name="frarchives_edition.faapi",
    context=SameAsResource,
    request_method="PUT",
)
def edit_same_as(context, request):
    cnx = request.cw_cnx
    body = request.json_body
    auth = context.authority
    same_as_of_auth = dict([(e.eid, e) for e in auth.same_as])
    for sameas in body:
        action_delete = sameas.get("toDelete")
        if "eid" not in sameas and action_delete:
            # user has added item and immediately after deleted it
            # so we ignore this item
            continue
        sameas_cw_etype = sameas.get("cw_etype")
        if sameas_cw_etype and sameas_cw_etype != "ExternalUri":
            # we only can delete the existing relation to Externalid
            if action_delete:
                cnx.execute(
                    "DELETE A same_as E WHERE A eid %(a)s, E eid %(e)s",
                    {"a": auth.eid, "e": sameas["eid"]},
                )
            continue
        sameas_uri = sameas.get("uri", "").strip()
        if not sameas_uri:
            label = sameas.get("label")
            source = sameas.get("source")
            if source:
                label = "{} ({})".format(label, source)
            msg = "{}: {}".format(label, cnx._("uri field is mandatory"))
            return JSONBadRequest(jsonapi_error(status=422, details=msg, pointer="uri"))
        sameas_uri = sameas.get("uri").strip()
        # only accept https for geonames
        if "geonames.org" in sameas_uri:
            sameas_uri = sameas_uri.replace("http://", "https://")
        # an existing ExternalUri whith the wanted uri value
        # ExternalUri exists
        existing_rset = cnx.execute(
            "Any X WHERE X is ExternalUri, X uri %(uri)s", {"uri": sameas_uri}
        )
        existing_exturi = existing_rset.one() if existing_rset else None
        # if eid exists, this means that the user is editing an element in the
        # list
        if sameas.get("eid"):
            # update or delete entity
            exturi = same_as_of_auth.get(sameas["eid"])
            if not exturi:
                # user error: anything can happen in a form : user click on a
                # submit without doing nothing
                raise httpexceptions.HTTPNoContent()
            if exturi.label != sameas["label"]:
                exturi.cw_set(label=sameas["label"])
            if exturi.uri != sameas_uri:
                if existing_exturi:
                    # should we change the label ???
                    auth.cw_set(same_as=existing_exturi.eid)
                else:
                    # in this case we should not call `cw_set` with new uri, maybe
                    # this ExternalUri is also linked to other authority, so prefer
                    # creating new ExternalUri so that other authority are unchanged
                    try:
                        newexturi = cnx.create_entity(
                            "ExternalUri", uri=sameas_uri, label=sameas["label"]
                        )
                    except ValidationError as err:
                        msg = "; ".join(err.errors.values())
                        return JSONBadRequest(jsonapi_error(status=422, details=msg, pointer="uri"))
                    except Exception:
                        msg = cnx._("Encountered an error while creating en entity")
                        return JSONBadRequest(jsonapi_error(status=422, details=msg, pointer="uri"))
                    cnx.execute(
                        "DELETE A same_as E WHERE A eid %(a)s, E eid %(e)s",
                        {"a": auth.eid, "e": exturi.eid},
                    )
                    auth.cw_set(same_as=newexturi.eid)
                    LOG.info("created %s", newexturi)
            # delete same_as relation
            if action_delete:
                cnx.execute(
                    "DELETE A same_as E WHERE A eid %(a)s, E eid %(e)s",
                    {"a": auth.eid, "e": exturi.eid},
                )
        else:
            label = sameas["label"]
            # user created a new element in the list
            # create a new ExternalUri or add a same_as on the existing
            # ExternalUri
            if existing_exturi:
                # should we change the label ???
                if not label:
                    if not same_as_of_auth.get(existing_exturi.eid):
                        auth.cw_set(same_as=existing_exturi.eid)
            else:
                try:
                    newexturi = cnx.create_entity("ExternalUri", uri=sameas_uri, label=label)
                except ValidationError as err:
                    msg = "; ".join(err.errors.values())
                    return JSONBadRequest(jsonapi_error(status=422, details=msg, pointer="uri"))
                except Exception:
                    msg = cnx._("Encountered an error while creating en entity")
                    return JSONBadRequest(jsonapi_error(status=422, details=msg, pointer="uri"))

                auth.cw_set(same_as=newexturi.eid)
                LOG.info("created %s", newexturi)
    url = auth.absolute_url()
    raise httpexceptions.HTTPCreated(
        location=url,
        content_type="application/json; charset=UTF-8",
        body=render("json", {"location": url}),
    )


@json_config(
    route_name="frarchives_edition.faapi",
    context=FAComponentResource,
    name="indexes",
)
def fac_index_view(context, request):
    cnx = request.cw_cnx
    rset = cnx.execute(
        """
Any
  X, L, JSON_AGG(U)
GROUPBY X, L
WHERE
  X is PniaLocation, X preflabel L, F index_location X, X same_as E?, E uri U,
  F eid %(e)s
        """,
        {"e": context.eid},
    )
    result = []
    for idx, (eid, label, exturls) in enumerate(rset):
        result.append(
            {
                "eid": eid,
                "preflabel": label,
                "type": "geogname",
                "alignments": [u for u in exturls if u is not None],
                "url": "location/{}".format(eid),
            }
        )


@json_config(
    route_name="frarchives_edition.faapi",
    context=FindingaidResource,
    name="indexes",
)
def fa_index_view(context, request):
    cnx = request.cw_cnx
    cu = cnx.system_sql(
        """
(
    SELECT
        p.cw_preflabel,
        p.cw_eid,
        COUNT(fac.cw_eid),
        ARRAY_AGG(DISTINCT ext.cw_uri)
    FROM
        cw_findingaid fa
        JOIN cw_facomponent fac ON fac.cw_finding_aid = fa.cw_eid
        JOIN cw_index i ON i.cw_target = fac.cw_eid
        JOIN cw_pnialocation p ON p.cw_eid = i.cw_authority
        LEFT OUTER JOIN same_as_relation sar ON sar.eid_from = p.cw_eid
        LEFT OUTER JOIN cw_externaluri ext ON ext.cw_eid = sar.eid_to
    WHERE
        fa.cw_stable_id = %(stableid)s
    GROUP BY
        p.cw_preflabel, p.cw_eid
)
UNION ALL
(
    SELECT
        p.cw_preflabel,
        p.cw_eid,
        COUNT(fa.cw_eid),
        ARRAY_AGG(DISTINCT ext.cw_uri)
    FROM
        cw_findingaid fa
        JOIN cw_index i ON i.cw_target = fa.cw_eid
        JOIN cw_pnialocation p ON p.cw_eid = i.cw_authority
        LEFT OUTER JOIN same_as_relation sar ON sar.eid_from = p.cw_eid
        LEFT OUTER JOIN cw_externaluri ext ON ext.cw_eid = sar.eid_to
    WHERE
        fa.cw_stable_id = %(stableid)s
    GROUP BY
        p.cw_preflabel, p.cw_eid
)
        """,
        {"stableid": context.entity.stable_id},
    )
    result = []
    for idx, (label, eid, targets, exturls) in enumerate(cu.fetchall()):
        result.append(
            {
                "eid": eid,
                "preflabel": label,
                "type": "geogname",
                "targets": targets,
                "alignments": [u for u in exturls if u is not None],
                "url": "location/{}".format(eid),
            }
        )

    return result


@json_config(
    route_name="frarchives_edition.faapi",
    context=AuthorityResource,
    request_method=("GET"),
    name="group_candidates",
    request_param=("q",),
)
def group_candidates(context, request):
    query_string = "{}".format(request.params["q"])
    results = context.entity.cw_adapt_to("IToGroup").candidates(query_string)
    return {"data": results}


@view_config(route_name="sitelinks", request_method=("GET", "HEAD"))
def sitelinks_view(request):
    cwreq = request.cw_request
    viewsreg = cwreq.vreg["views"]
    view = viewsreg.select("sitelinks", cwreq)
    return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))


def includeme(config):
    config.scan(__name__)

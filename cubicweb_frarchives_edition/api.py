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
from functools import wraps
import logging
import json

from urllib3.exceptions import ProtocolError

from cubicweb import NoResultError, ValidationError, Unauthorized

from pyramid import httpexceptions
from pyramid.view import view_config
from pyramid.renderers import render

from elasticsearch.exceptions import ConnectionError, NotFoundError

from cubicweb_jsonschema.resources.entities import ETypeResource, EntityResource
from cubicweb_jsonschema.resources import RelationshipResource
from cubicweb_jsonschema import VIEW_ROLE, CREATION_ROLE, EDITION_ROLE
from cubicweb_jsonschema.api.schema import jsonschema_config
from cubicweb_jsonschema.api import (
    entities as api_entities,
    schema as api_schema,
    JSONBadRequest,
    jsonapi_error,
)
from cubicweb_jsonschema.resources.schema import ETypeSchema

from cubicweb_francearchives.dataimport import sqlutil

from cubicweb_frarchives_edition.resources import WorkflowTransitionResource


LOG = logging.getLogger(__name__)


def json_config(**settings):
    """Wraps view_config for JSON rendering."""
    settings.setdefault("accept", "application/json")
    settings.setdefault("renderer", "json")
    return view_config(**settings)


def entity_from_context(func):
    """View decorator binding a CubicWeb `entity` to the `context`.

    May raise HTTPNotFound if no entity can be found.
    Will walk through parents of the `context` until an `EntityResource` gets
    found before fetching the `entity`.
    """

    @wraps(func)
    def wrapper(context, request):
        entity_context = context
        while True:
            try:
                rset = entity_context.rset
            except AttributeError:
                try:
                    entity_context = entity_context.__parent__
                except AttributeError:
                    raise httpexceptions.HTTPNotFound()
            else:
                break
        try:
            entity = rset.one()  # May raise HTTPNotFound.
        except NoResultError:
            raise httpexceptions.HTTPNotFound()
        context.entity = entity
        return func(context, request)

    return wrapper


def jsonschema_adapter(cnx, **context):
    return cnx.vreg["adapters"].select("IJSONSchema", cnx, **context)


@json_config(
    name="uischema", route_name="cwentities", request_method="GET", context=ETypeResource,
)
def etype_json_uischema(context, request):
    """Return the uischema for the entity type bound to `context`."""
    vreg = request.registry["cubicweb.registry"]
    null_entity = vreg["etypes"].etype_class(context.etype)(request.cw_request)
    kwargs = dict()
    if "schema_type" in request.params:
        kwargs["schema_type"] = request.params["schema_type"]
    adapter = vreg["adapters"].select(
        "IJsonFormEditable", request.cw_request, entity=null_entity, **kwargs
    )
    return adapter.ui_schema()


@view_config(
    route_name="delete_entity", context=Unauthorized,
)
def deletion_unauthorized(exc, request):
    """Exception view for Unauthorized error on JSON request."""
    LOG.info("%s encountered during processing of %s", exc, request)
    _ = request.cw_request._
    request.cw_cnx.rollback()
    return JSONBadRequest(jsonapi_error(status=401, details=_("not authorized")))


@json_config(
    route_name="delete_entity", context=ValidationError,
)
def deletion_failed(exc, request):
    """Exception view for ValidationError on JSON request."""
    LOG.info("%s encountered during processing of %s", exc, request)
    _ = request.cw_request._
    request.cw_cnx.rollback()
    exc.translate(_)
    errors = [
        jsonapi_error(status=422, details=value, pointer=rolename)
        for rolename, value in list(exc.errors.items())
    ]
    return JSONBadRequest(*errors)


@jsonschema_config(context=ETypeSchema, request_param="role")
def etype_role_schema(context, request):
    """Schema view for an entity type with specified role."""
    req = request.cw_request
    kwargs = dict(etype=context.etype)
    if "schema_type" in request.params:
        kwargs["schema_type"] = request.params["schema_type"]
    adapted = req.vreg["adapters"].select("IJSONSchema", req, **kwargs)
    role = request.params["role"].lower()
    if role == VIEW_ROLE:
        return adapted.view_schema(ordered=True)
    elif role == CREATION_ROLE:
        return adapted.creation_schema(ordered=True)
    else:
        raise httpexceptions.HTTPBadRequest("invalid role: {0}".format(role))


@json_config(
    route_name="cwentities", context=ETypeResource, request_method="POST",
)
def create_entity(context, request):
    """Create a new entity from JSON data."""
    # TODO In case of validation errors, it'd be better to give a JSON Schema
    # entry as a "pointer", would require selection context to be an
    # ETypeSchemaResource.
    etype = context.etype
    kwargs = dict(etype=context.etype)
    if "schema_type" in request.params:
        kwargs["schema_type"] = request.params["schema_type"]
    adapter = jsonschema_adapter(request.cw_request, **kwargs)
    if request.headers.get("content-type") == "application/json":
        instance = request.json_body
    else:
        # assumed it is a multipart request
        data = request.POST.get("data")
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        instance = json.loads(request.POST.get("data"))
        instance["fileobj"] = request.POST.get("fileobj")
    entity = adapter.create_entity(instance)
    request.cw_cnx.commit()
    LOG.info("created %s", entity)

    value = entity.cw_adapt_to("IJSONSchema").serialize()
    location = request.route_url("cwentities", etype=etype, traverse=str(entity.eid))
    raise httpexceptions.HTTPCreated(
        location=location,
        content_type="application/json; charset=UTF-8",
        body=render("json", value),
    )


@json_config(
    name="uischema", route_name="cwentities", request_method="GET", context=RelationshipResource,
)
def relationship_uischema(context, request):
    vreg = request.registry["cubicweb.registry"]
    entity = vreg["etypes"].etype_class(context.target_type)(request.cw_request)
    return entity.cw_adapt_to("IJsonFormEditable").ui_schema()


@json_config(
    name="available-targets",
    route_name="cwentities",
    request_method="GET",
    context=RelationshipResource,
    request_param=("q",),
)
def list_available_entities(context, request):
    params = request.params.copy()
    params["q"] = "%{}%".format(request.params["q"])
    entities = []
    cwreq = request.cw_request
    for tschema in context.target_schemas:
        ttype = tschema.type
        adapter = cwreq.vreg["adapters"].select("IAvailable", cwreq, etype=ttype, **params)
        rql = adapter.rql()
        rset = request.cw_request.execute(rql, params)
        for entity in rset.entities():
            entities.append(entity.cw_adapt_to("IAvailable").serialize())
    return {"data": entities}


@json_config(
    name="targets",
    route_name="cwentities",
    request_method="POST",
    context=RelationshipResource,
    decorator=[entity_from_context],
)
def relate_targets(context, request):
    cnx = request.cw_request
    entity, rtype = context.entity, context.rtype
    adapter = jsonschema_adapter(cnx, entity=entity, rtype=rtype)
    if any(request.json_body):
        values = {value["value"] for value in request.json_body}
        adapter.add_relation(values)
    else:
        adapter.add_relation(set([]))
    return httpexceptions.HTTPCreated(content_type="application/json", body=b"null")


@json_config(
    route_name="cwentities",
    request_method="DELETE",
    context=RelationshipResource,
    decorator=[entity_from_context],
)
def delete_relation(context, request):
    rtype = context.rtype
    context.entity.cw_set(**{rtype: None})
    return httpexceptions.HTTPOk()


@json_config(
    route_name="cwentities",
    context=WorkflowTransitionResource,
    request_method="GET",
    decorator=[entity_from_context],
)
def get_workflow_transitions(context, request):
    wfentity = context.entity.cw_adapt_to("IWorkflowable")
    data = [trinfo.cw_adapt_to("IJSONSchema").serialize() for trinfo in wfentity.workflow_history]
    return data


@json_config(
    route_name="cwentities",
    context=WorkflowTransitionResource,
    request_method="POST",
    decorator=[entity_from_context],
)
def add_workflow_transition(context, request):
    cnx = request.cw_cnx
    data = request.json_body
    entity = cnx.entity_from_eid(context.entity.eid)
    wfentity = entity.cw_adapt_to("IWorkflowable")
    trinfo = wfentity.fire_transition(data["name"], comment=data.get("comment"))
    cnx.commit()
    return trinfo.cw_adapt_to("IJSONSchema").serialize()


@jsonschema_config(
    context=WorkflowTransitionResource, decorator=[entity_from_context],
)
def workflow_transition_schema(context, request):
    """Return the JSON schema of TrInfo entity type restricted to possible
    transitions for entity bound to `context`.
    """
    adapter = jsonschema_adapter(request.cw_request, etype="TrInfo", for_entity=context.entity)
    if "role" not in request.params:
        raise httpexceptions.HTTPBadRequest('missing "role" parameter')
    try:
        method = {
            VIEW_ROLE: "view_schema",
            CREATION_ROLE: "creation_schema",
            EDITION_ROLE: "edition_schema",
        }[request.params["role"].lower()]
    except KeyError:
        raise httpexceptions.HTTPBadRequest('invalid "role" parameter')
    return getattr(adapter, method)(ordered=True)


@view_config(
    route_name="delete_entity",
    context=EntityResource,
    request_method="DELETE",
    decorator=[entity_from_context],
)
def delete_entity(context, request):
    """Delete an entity."""
    entity = context.entity
    cnx = request.cw_cnx
    if entity.cw_etype == "FindingAid":
        if not entity.cw_has_perm("delete"):
            raise httpexceptions.HTTPUnauthorized()
        # XXX add cnx.rollback() ?
        sqlutil.delete_from_filename(
            cnx, entity.stable_id, interactive=False, esonly=False, is_filename=False
        )
        # no commit here because it is already done in sqlutil.delete_from_filename
        sync_service = cnx.vreg["services"].select("sync", cnx)
        try:
            sync_service.sync([("delete", entity)])
        except (ConnectionError, ProtocolError, NotFoundError):
            op_str = "delete #{}".format(entity.eid)
            cnx.warning("[ES] Failed sync operations %s", op_str)
    else:
        entity.cw_delete()
        cnx.commit()
    LOG.info("deleted %s", entity)
    raise httpexceptions.HTTPNoContent()


def includeme(config):
    config.include("cubicweb.pyramid.predicates")
    config.include("cubicweb_jsonschema.predicates")
    config.include(".routes")
    config.scan(api_entities, ignore=(".delete_entity", ".create_entity"))
    config.scan(api_schema, ignore=(".etype_role_schema"))
    config.scan(__name__)

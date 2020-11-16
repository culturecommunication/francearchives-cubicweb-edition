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
import base64
from collections import OrderedDict
import mimetypes
import os.path as osp
import os
from datetime import datetime
import tempfile


import jsl


from yams.constraints import StaticVocabularyConstraint

from logilab.common.date import ustrftime
from logilab.common.registry import yes
from logilab.common.decorators import monkeypatch

from rql import TypeResolverException

from cubicweb import Binary, ValidationError, _
from cubicweb.predicates import adaptable, is_instance, match_kwargs

from cubicweb_jsonschema.__init__ import orm_rtype

from cubicweb_jsonschema.entities import ijsonschema
from cubicweb_jsonschema import mappers
from cubicweb_jsonschema import CREATION_ROLE, EDITION_ROLE


from . import parse_dataurl
from cubicweb_frarchives_edition.cms.faimport import (
    process_faimport_zip,
    process_faimport_xml,
    process_csvimport_zip,
    process_authorityrecords_zip,
    process_authorityrecord_xml,
)
from cubicweb_frarchives_edition import tasks
from cubicweb_frarchives_edition.api import jsonapi_error, JSONBadRequest


class CommemoCollectionIJSONSchemaRelationTargetETypeAdapter(
    ijsonschema.IJSONSchemaRelationTargetETypeAdapter
):  # noqa
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {"etype": "CommemoCollection"}
    )

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(
            CommemoCollectionIJSONSchemaRelationTargetETypeAdapter, self
        ).creation_schema(**kwargs)
        commemocollection_attrs = schema["properties"]
        commemocollection_attrs["sections"] = {
            "type": "array",
            "title": req._("Section"),
            "items": {
                "type": "object",
                "properties": {"title": {"type": "string", "title": req._("title")}},
            },
        }
        commemocollection_attrs["commemorationItems"] = {
            "type": "array",
            "title": req._("Intitules de la rubrique presentation"),
            "items": {
                "type": "object",
                "properties": {"title": {"type": "string", "title": req._("title")}},
            },
        }
        return schema

    def create_entity(self, instance, target):
        entity = super(CommemoCollectionIJSONSchemaRelationTargetETypeAdapter, self).create_entity(
            instance, target
        )
        req = self._cw
        for order, i in enumerate(instance.get("commemorationItems", ())):
            req.create_entity(
                "CommemorationItem",
                reverse_children=entity,
                title=i["title"],
                order=order + 1,
                alphatitle=i["title"],
                commemoration_year=entity.year,
                collection_top=entity,
            )
        for s in instance.get("sections", ()):
            req.create_entity("Section", reverse_children=entity, title=s["title"])
        return entity


class CommemorationIJSONSchemaRelationTargetETypeAdapter(
    ijsonschema.IJSONSchemaRelationTargetETypeAdapter
):  # noqa
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {"etype": "CommemorationItem"}
    )

    def create_entity(self, instance, target):
        entity = super(CommemorationIJSONSchemaRelationTargetETypeAdapter, self).create_entity(
            instance, target
        )
        if target.cw_etype == "CommemoCollection":
            collection_top = target
        else:
            assert target.cw_etype == "Section" and target.is_commemo_section()
            collection_top = target.commemo_section
        # XXX add 'collection_top' to "Additional properties"
        entity.cw_set(collection_top=collection_top)
        return entity


class RqTaskIJSONSchemaAdapter(ijsonschema.IJSONSchemaETypeAdapter):
    TASK_MAP = OrderedDict(
        [
            (_("import_ead"), tasks.import_ead),
            (_("import_csv"), tasks.import_csv),
            (_("import_authorities"), tasks.import_authorities),
            (_("publish_findingaid"), tasks.publish_findingaid),
            (_("delete_finding_aids"), tasks.delete_findingaids),
            (_("export_ape"), tasks.export_ape),
            (_("export_locationauthorities"), tasks.export_authorities),
            (_("export_agentauthorities"), tasks.export_authorities),
            (_("export_subjectauthorities"), tasks.export_authorities),
            (
                _("compute_location_authorities_to_group"),
                tasks.compute_location_authorities_to_group,
            ),
            (_("group_location_authorities"), tasks.group_location_authorities),
            (_("compute_alignments_all"), tasks.compute_alignments_all),
            (_("import_eac"), tasks.import_eac),
            (_("run_dead_links"), tasks.run_dead_links),
            (_("run_index_kibana"), tasks.index_kibana),
        ]
    )
    __select__ = ijsonschema.IJSONSchemaETypeAdapter.__select__ & match_kwargs({"etype": "RqTask"})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["name"] = {
            "title": req._("type of task"),
            "type": "string",
            "enum": list(self.TASK_MAP.keys()),
            "enumNames": [req._(t) for t in self.TASK_MAP],
        }
        if "title" not in schema["required"]:
            schema["required"].append("title")
        return schema

    def create_entity(self, instance):
        req = self._cw
        entity = req.create_entity("RqTask", name=instance["name"], title=instance["title"])
        return entity

    def retrieve_and_validate_services(self, services, valid_services):
        """Retrieve and validate services provided by user.

        :param str services: comma-separated list of services
        :param set valid_services: list of valid services

        :raises JSONBadRequest: if services provided by user contain invalid service

        :returns: list of validated services
        :rtype: list
        """
        services = {service.strip() for service in services.split(",") if service.strip()}
        badcodes = services.difference(valid_services)
        if badcodes:
            if len(badcodes) == 1:
                msg = self._cw._("Found non-existent service code: ")
            else:
                msg = self._cw._("Found non-existent service codes: ")
            raise JSONBadRequest(
                jsonapi_error(
                    status=422, pointer="services", details="{}{}".format(msg, ",".join(badcodes))
                )
            )
        return list(services)

    def write_tempfile(self, content):
        """Write content of input file to temporary file.

        :param bytes content: content
        :param instance: RQ task

        :returns: filename of temporary file
        :rtype: str
        """
        fd, filepath = tempfile.mkstemp()
        os.write(fd, content)
        os.close(fd)
        return filepath


class RqTaskDedupeAuthJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "dedupe_authorities"}
    )
    TASK_MAP = OrderedDict(
        [
            (_("dedupe_authorities"), tasks.dedupe_authorities),
        ]
    )

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskDedupeAuthJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["service"] = {
            "type": "string",
            "title": req._("service code"),
            "enum": [
                c
                for c, in req.execute(
                    "Any C ORDERBY C WHERE X is Service, X code C, NOT X code NULL"
                )
            ],
        }
        props["strict"] = {
            "type": "boolean",
            "title": req._("compare strictly authority label"),
            "default": True,
        }
        return schema

    def create_entity(self, instance):
        entity = super(RqTaskDedupeAuthJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            instance.get("strict", True),
            instance.get("service"),
        )
        return entity


class ImportIndexPolicyMinix(object):
    @property
    def index_policy_props(self):
        req = self._cw
        props = {}
        props["should_normalize"] = {
            "type": "boolean",
            "title": req._("Normalize indexes labels"),
            "enum": [True, False],
            "enumNames": [
                req._("yes"),
                req._("no"),
            ],
            "default": True,
        }
        props["context_service"] = {
            "type": "boolean",
            "title": req._("Context of import"),
            "enum": [True, False],
            "enumNames": [
                req._("service"),
                req._("all services"),
            ],
            "default": True,
        }
        return props


class RqTaskImportEadIJSONSchemaAdapter(ImportIndexPolicyMinix, RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({"schema_type": "import_ead"})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportEadIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {
            "type": "string",
            "title": req._("zip file"),
        }
        props["service"] = {
            "type": "string",
            "title": req._("service code (required only for xml, optional for zip)"),
            "enum": [
                c
                for c, in req.execute(
                    "Any C ORDERBY C WHERE X is Service, X code C, NOT X code NULL"
                )
            ],
        }
        props["force-delete"] = {
            "type": "boolean",
            "title": req._("force-delete-old-findingaids"),
            "default": True,
        }
        props.update(self.index_policy_props)
        schema["required"] = list(set(schema["required"]) | {"file"})
        return schema

    def create_entity(self, instance):
        req = self._cw
        fileobj = instance["fileobj"]
        code, ext = osp.splitext(fileobj.filename)
        if ext == ".zip":
            filepaths = process_faimport_zip(req, fileobj)
        elif ext == ".xml":
            filepaths = process_faimport_xml(req, fileobj, instance["service"])
        force_delete = instance.get("force-delete", False)
        auto_import = False
        entity = super(RqTaskImportEadIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        auto_dedupe = instance["should_normalize"]
        context_service = instance["context_service"]
        kwargs = {}
        if instance["service"] == "FRAN":
            kwargs = {"job_timeout": "18h"}
        entity.cw_adapt_to("IRqJob").enqueue(
            func, filepaths, auto_dedupe, context_service, force_delete, auto_import, **kwargs
        )
        return entity


class RqTaskImportEacIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({"schema_type": "import_eac"})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportEacIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {"type": "string", "title": req._("ZIP or XML file")}
        props["service"] = {
            "type": "string",
            "title": req._(
                "service code of the productor " "(required only for xml, optional for zip)"
            ),
            "enum": [
                c
                for c, in req.execute(
                    "Any C ORDERBY C WHERE X is Service, X code C, NOT X code NULL"
                )
            ],
        }
        schema["required"] = list(set(schema["required"]) | {"file", "title"})
        return schema

    def create_entity(self, instance):
        req = self._cw
        fileobj = instance["fileobj"]
        f, ext = osp.splitext(fileobj.filename)
        if ext == ".zip":
            filepaths = process_authorityrecords_zip(req, fileobj)
        elif ext == ".xml":
            filepaths = [process_authorityrecord_xml(req, fileobj, instance["service"])]
        else:
            raise JSONBadRequest(
                jsonapi_error(
                    status=422,
                    pointer="file",
                    details=req._("input file must be a ZIP or a XML file"),
                )
            )
        entity = super(RqTaskImportEacIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            filepaths,
        )
        return entity


class RqTaskImportOaiIJSONSchemaAdapter(ImportIndexPolicyMinix, RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({"schema_type": "import_oai"})
    TASK_MAP = OrderedDict(
        [
            (_("import_oai"), tasks.import_oai),
        ]
    )

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportOaiIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["oairepository"] = {
            "type": "integer",
            "title": req._("oai repository identifier"),
        }
        props["force-refresh"] = {
            "type": "boolean",
            "title": req._("force-import-oai"),
        }
        props.update(self.index_policy_props)
        schema["required"].append("oairepository")
        return schema

    def create_entity(self, instance):
        force_refresh = instance.get("force-refresh", True)
        auto_import = False
        oairepo = self._cw.entity_from_eid(instance["oairepository"])
        meta_prefix = oairepo.oai_params.get("metadataPrefix", [""])[0]
        name = instance["name"]
        if meta_prefix:
            name = "{} ({})".format(name, meta_prefix)
        entity = self._cw.create_entity("RqTask", name=name, title=instance["title"])
        func = self.TASK_MAP[instance["name"]]
        auto_dedupe = instance["should_normalize"]
        context_service = instance["context_service"]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            instance["oairepository"],
            auto_dedupe,
            context_service,
            force_refresh,
            auto_import,
            job_timeout="24h",
        )
        return entity


class RqTaskExportApeIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({"schema_type": "export_ape"})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskExportApeIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["services"] = {
            "type": "string",
            "title": req._("list_services"),
            "description": req._("list_services_descr"),
        }
        return schema

    def create_entity(self, instance):
        req = self._cw
        rset = req.execute(
            "DISTINCT Any C WHERE X is Service, X code C, NOT X code NULL, "
            "F service X, F is FindingAid"
        )
        allcodes = {c for c, in rset}
        services = self.retrieve_and_validate_services(instance.get("services", ""), allcodes)
        entity = super(RqTaskExportApeIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func, services)
        return entity


class AbstractRqTaskImportCSVAdapter(RqTaskIJSONSchemaAdapter):
    __abstract__ = True

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(AbstractRqTaskImportCSVAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {
            "type": "string",
            "title": req._("csv file to import"),
        }
        schema["required"] = list(set(schema["required"]) | {"file", "title"})
        return schema

    def create_entity(self, instance):
        filepath = self.write_tempfile(instance["fileobj"].value)
        entity = super(AbstractRqTaskImportCSVAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func, filepath)
        return entity


class RqTaskDeleteFindingAidsIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "delete_finding_aids"}
    )

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super().creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {"type": "string", "title": req._("csv-finding-aids")}
        schema["required"] = list(set(schema["required"]) | {"file", "title"})
        return schema

    def create_entity(self, instance):
        entity = super().create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        filepath = self.write_tempfile(instance["fileobj"].value)
        entity.cw_adapt_to("IRqJob").enqueue(func, filepath)
        return entity


class RqTaskImportAuthoritiesIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "import_authorities"}
    )

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportAuthoritiesIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {"type": "string", "title": req._("csv-authorities")}
        props["labels"] = {"type": "boolean", "title": req._("update-labels"), "default": True}
        props["alignments"] = {
            "type": "boolean",
            "title": req._("update-alignments"),
            "default": True,
        }
        schema["required"] = list(set(schema["required"]) | {"file", "title"})
        return schema

    def create_entity(self, instance):
        entity = super(RqTaskImportAuthoritiesIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        filepath = self.write_tempfile(instance["fileobj"].value)
        entity.cw_adapt_to("IRqJob").enqueue(
            func, filepath, instance.get("labels", True), instance.get("alignments", True)
        )
        return entity


class RqTaskComputeAlignementsJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "compute_alignments"}
    )
    TASK_MAP = OrderedDict(
        [
            (_("compute_alignments"), tasks.compute_alignments),
        ]
    )

    def creation_schema(self, **kwargs):
        schema = super(RqTaskComputeAlignementsJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["findingaid_eid"] = {
            "type": "string",
            "title": self._cw._("findingaid eid"),
        }
        schema["required"].append("findingaid_eid")
        return schema

    def create_entity(self, instance):
        req = self._cw
        try:
            if instance["findingaid_eid"].isdigit():
                rset = req.execute(
                    "Any X WHERE X is FindingAid, X eid %(e)s", {"e": instance["findingaid_eid"]}
                )
            else:
                rset = req.execute(
                    "Any X WHERE X is FindingAid, X stable_id %(e)s",
                    {"e": instance["findingaid_eid"]},
                )
        except Exception:
            # e.g. a bad eid might cause a TypeResolverException
            self.exception("failed to fetch FindingAid witg id %r", instance["findingaid_eid"])
            raise JSONBadRequest(
                jsonapi_error(
                    status=422,
                    pointer="findingaid_eid",
                    details=req._("no findingaid with this id"),
                )
            )
        if not rset:
            raise JSONBadRequest(
                jsonapi_error(
                    status=422,
                    pointer="findingaid_eid",
                    details=req._("no findingaid with this id"),
                )
            )
        entity = super(RqTaskComputeAlignementsJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func, instance["findingaid_eid"])
        return entity


class RqTaskComputeAlignmentsAllJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "compute_alignments_all"}
    )

    def creation_schema(self, **kwargs):
        schema = super(RqTaskComputeAlignmentsAllJSONSchemaAdapter, self).creation_schema(**kwargs)
        req = self._cw
        props = schema["properties"]
        props["geoname"] = {"type": "boolean", "title": req._("align-geoname"), "default": True}
        props["bano"] = {"type": "boolean", "title": req._("align-bano"), "default": True}
        props["simplified"] = {
            "type": "boolean",
            "title": req._("simplified-csv"),
            "default": False,
        }
        return schema

    def create_entity(self, instance):
        entity = super(RqTaskComputeAlignmentsAllJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        targets = ()
        if instance.get("geoname", True):
            targets = targets + ("geoname",)
        if instance.get("bano", True):
            targets = targets + ("bano",)
        if not targets:
            msg = self._cw._("no-targets")
            raise JSONBadRequest(jsonapi_error(status=422, pointer="geoname", details=msg))
        # file size 200 000 fixed in https://extranet.logilab.fr/ticket/66914807
        # use non-default timeout 12h to make sure that task can be completed
        # longest observed runtime at this point is +/-3h
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            instance.get("simplified", False),
            targets,
            instance.get("file_size", 200000),
            job_timeout="12h",
        )
        return entity


class RqTaskPublishFAIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "publish_findingaid"}
    )

    def creation_schema(self, **kwargs):
        schema = super(RqTaskPublishFAIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["importead_task_eid"] = {
            "type": "integer",
            "title": self._cw._("task eid"),
        }
        schema["required"].append("importead_task_eid")
        return schema

    def create_entity(self, instance):
        req = self._cw
        try:
            rset = req.find("RqTask", eid=instance["importead_task_eid"])
        except TypeResolverException:
            rset = None
        if not rset:
            raise JSONBadRequest(
                jsonapi_error(
                    status=422, pointer="importead_task_eid", details=req._("bad task eid")
                )
            )
        entity = super(RqTaskPublishFAIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func, instance["importead_task_eid"])
        return entity


class RqTaskImportCSVIJSONSchemaAdapter(ImportIndexPolicyMinix, RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({"schema_type": "import_csv"})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportCSVIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["file"] = {
            "type": "string",
            "title": req._("csv file to import"),
        }
        props["force-delete"] = {
            "type": "boolean",
            "title": req._("force-delete-old-findingaids"),
            "default": True,
        }
        props.update(self.index_policy_props)
        schema["required"] = list(set(schema["required"]) | {"file", "title"})
        return schema

    def create_entity(self, instance):
        req = self._cw
        force_delete = instance.get("force-delete", True)
        auto_import = False
        entity = super(RqTaskImportCSVIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        filepaths = process_csvimport_zip(req, instance["fileobj"])
        auto_dedupe = instance["should_normalize"]
        context_service = instance["context_service"]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            filepaths,
            auto_dedupe,
            context_service,
            force_delete,
            auto_import,
        )
        return entity


class RqTaskComputeLocAuthoritesToGroupJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "compute_location_authorities_to_group"}
    )

    def create_entity(self, instance):
        entity = super(RqTaskComputeLocAuthoritesToGroupJSONSchemaAdapter, self).create_entity(
            instance
        )
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func)
        return entity


class RqTaskGroupLocAuthoritesSONSchemaAdapter(AbstractRqTaskImportCSVAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "group_location_authorities"}
    )


class AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __abstract__ = True

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter, self).creation_schema(
            **kwargs
        )
        props = schema["properties"]
        props["services"] = {
            "type": "string",
            "title": req._("list_services"),
            "description": req._("list_services_descr"),
        }
        props["aligned"] = {"type": "boolean", "title": req._("export-aligned"), "default": True}
        props["nonaligned"] = {
            "type": "boolean",
            "title": req._("export-nonaligned"),
            "default": False,
        }
        props["simplified"] = {
            "type": "boolean",
            "title": req._("simplified-csv"),
            "default": False,
        }
        return schema

    @property
    def authority_type(self):
        raise NotImplementedError

    def create_entity(self, instance):
        services = instance.get("services", "")
        req = self._cw
        rset = req.execute(
            """
            DISTINCT Any C WHERE X is Service,
            X code C, NOT X code NULL"""
        )
        allcodes = {c for c, in rset}
        services = self.retrieve_and_validate_services(services, allcodes)
        entity = super(AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter, self).create_entity(
            instance
        )
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            services,
            self.authority_type,
            instance.get("aligned", True),
            instance.get("nonaligned", False),
            instance.get("simplified", False),
        )
        return entity


class RqTaskExportLocationAuthoritiesIJSONSchemaAdapter(
    AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter
):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "export_locationauthorities"}
    )

    @property
    def authority_type(self):
        return "location"


class RqTaskExportAgentAuthoritiesIJSONSchemaAdapter(
    AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter
):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "export_agentauthorities"}
    )

    @property
    def authority_type(self):
        return "agent"


class RqTaskExportSubjectAuthoritiesIJSONSchemaAdapter(
    AbstractRqTaskExportAuthoritiesIJSONSchemaAdapter
):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "export_subjectauthorities"}
    )

    @property
    def authority_type(self):
        return "subject"


class RqTaskRunDeadLinks(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "run_dead_links"}
    )
    TASK_MAP = OrderedDict(
        [
            (_("run_dead_links"), tasks.run_dead_links),
        ]
    )

    def create_entity(self, instance):
        entity = super(RqTaskRunDeadLinks, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(func, job_timeout="12h")
        return entity


class RqTaskIndexKibana(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {"schema_type": "run_index_kibana"}
    )
    TASK_MAP = OrderedDict(
        [
            (_("run_index_kibana"), tasks.index_kibana),
        ]
    )

    def creation_schema(self, **kwargs):
        schema = super(RqTaskIndexKibana, self).creation_schema(**kwargs)
        props = schema["properties"]
        props["index_authorities"] = {
            "type": "boolean",
            "default": True,
            "title": self._cw._("index authorities"),
        }
        props["index_services"] = {
            "type": "boolean",
            "default": False,
            "title": self._cw._("index services"),
        }
        return schema

    def create_entity(self, instance):
        entity = super(RqTaskIndexKibana, self).create_entity(instance)
        func = self.TASK_MAP[instance["name"]]
        entity.cw_adapt_to("IRqJob").enqueue(
            func,
            instance["index_authorities"],
            instance["index_services"],
            job_timeout="10h",
        )
        return entity


class TargetIJSONSchemaRelationTargetETypeAdapterMixIn(object):
    @property
    def authority_etype(self):
        """The entity type bound to this adapter."""
        return str(self.cw_extra_kwargs["etarget_role"])

    def creation_schema(self, **kwargs):
        authority_schema = self.authority_schema()
        authority_schema["required"] = ["label"]
        return authority_schema

    def create_authority(self, values):
        label = values["label"].strip()
        if not label:
            msg = self._cw._('"label" is required')
            raise ValidationError(None, {"label": msg})
        req = self._cw
        authority = (
            req.vreg["adapters"]
            .select("IJSONSchema", req, etype=self.authority_etype)
            .create_entity(values)
        )
        return authority

    def authority_schema(self):
        req = self._cw
        authority_adaptor = req.vreg["adapters"].select(
            "IJSONSchema",
            req,
            **{"etype": self.authority_etype, "rtype": "authority", "role": "object"}
        )
        return authority_adaptor.creation_schema()

    def create_entity(self, instance, target):
        req = self._cw
        values = {}
        # XXX get rid of iter_relation_mappers
        for adapter, relation_mapper in self.iter_relation_mappers(CREATION_ROLE):
            values.update(relation_mapper.values(None, instance))
        ce = req.create_entity
        authority = self.create_authority(values)
        pnia_role = req.find("IndexRole", label="subject")
        if pnia_role:
            pnia_role = pnia_role[0]
        else:
            pnia_role = ce("IndexRole", label="subject")
        entity = ce(self.etype, authority=authority, pniarole=pnia_role)
        entity.cw_set(**{orm_rtype(self.rtype, self.role): target})
        return entity


class AgentAuthorityTargetIJSONSchemaRelationTargetETypeAdapter(
    TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
    ijsonschema.IJSONSchemaRelationTargetETypeAdapter,
):
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {
            "etype": "AgentName",
            "rtype": "auhtority",
            "role": "subject",
            "etarget_role": "AgentAuthority",
        }
    )

    def creation_schema(self, **kwargs):
        authority_schema = self.authority_schema()
        authority_schema["required"] = ["label"]
        return authority_schema


class LocationAuthorityTargetIJSONSchemaRelationTargetETypeAdapter(
    TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
    ijsonschema.IJSONSchemaRelationTargetETypeAdapter,
):
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {
            "etype": "Index",
            "rtype": "target",
            "role": "subject",
            "etarget_role": "LocationAuthority",
        }
    )


class SubjectAuhtorityFormTargetIJSONSchemaRelationTargetETypeAdapter(
    TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
    ijsonschema.IJSONSchemaRelationTargetETypeAdapter,
):
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {"etype": "Index", "rtype": "target", "role": "subject", "etarget_role": "SubjectAuhtority"}
    )


class FrarchivesStringField(jsl.fields.StringField):
    """react-jsonschema-form waits for enumNames key instead of

    options.enum_titles in schema
    """

    def __init__(self, **kwargs):
        super(FrarchivesStringField, self).__init__(**kwargs)
        if "enum" in kwargs:
            self.enum_titles = kwargs["enum_titles"]

    def get_definitions_and_schema(self, **kwargs):
        definitions, schema = super(jsl.fields.StringField, self).get_definitions_and_schema(
            **kwargs
        )
        if "enum" not in schema:
            return definitions, schema
        schema["enumNames"] = self.enum_titles
        return definitions, schema


class TrInfoIJSONSchemaAdapter(ijsonschema.IJSONSchemaETypeAdapter):
    __select__ = (
        ijsonschema.IJSONSchemaETypeAdapter.__select__
        & match_kwargs({"etype": "TrInfo"})
        & match_kwargs("for_entity")
    )

    def creation_schema(self, **kwargs):
        # By-pass IJSONSchemaETypeAdapter's method which does not support
        # TrInfo entity type for now.
        entity = self.cw_extra_kwargs["for_entity"]
        wfentity = entity.cw_adapt_to("IWorkflowable")
        builder = self._cw.vreg["components"].select("jsonschema.map.builder", self._cw)
        enum = [trinfo.name for trinfo in wfentity.possible_transitions()]
        enum_titles = list(map(self._cw._, enum))
        attrs = {
            "name": FrarchivesStringField(enum=enum, enum_titles=enum_titles, required=True),
            "Options": builder.set_default_options("TrInfo"),
        }
        doc = type("TrInfo", (jsl.Document,), attrs)
        return jsl.DocumentField(doc, as_ref=True).get_schema(**kwargs)


class FileDataAttributeMapper(mappers.BytesMapper):
    __select__ = mappers.BytesMapper.__select__ & mappers.yams_match(
        etype="File", rtype="data", role="subject", target_types="Bytes"
    )
    jsl_field_class = jsl.fields.StringField

    def jsl_field(self, *args, **kwargs):
        kwargs.setdefault("format", "data-url")
        return super(FileDataAttributeMapper, self).jsl_field(*args, **kwargs)

    def values(self, entity, instance):
        if self.rtype not in instance:
            return super(FileDataAttributeMapper, self).values(entity, instance)
        value = instance[self.rtype]
        try:
            filedata, mediatype, parameters = parse_dataurl(value)
        except ValueError as exc:
            raise ValidationError(entity, {self.rtype: str(exc)})
        if "name" not in parameters:
            self.warning('uploaded data-url field for %s has no "name" parameter', entity)
        data_name = parameters.get("name")
        return {
            "data_name": data_name,
            "data_format": mediatype,
            "data_encoding": parameters.get("charset"),
            "data": Binary(filedata),
        }

    def serialize(self, entity):
        if entity.data is None:
            return None
        value = entity.data.read()
        if value is None:
            return None
        parts = ["data:"]
        mimetype = entity.data_format
        if mimetype:
            parts.append(mimetype + ";")
        name = entity.data_name
        if name:
            parts.append("name=" + name + ";")
        parts.append("base64," + base64.b64encode(value).decode("utf8"))
        return "".join(parts)


class FileETypeMixin(object):
    def values(self, entity, instance):
        values = super(FileETypeMixin, self).values(entity, instance)
        if values["data_name"] is None:
            values["data_name"] = values.get("title", "<unspecified file name>")
        _, ext = osp.splitext(values["data_name"])
        if ext:
            values["data_format"] = mimetypes.guess_type(values["data_name"])[0]
        else:
            extension = mimetypes.guess_extension(values["data_format"])
            if extension:
                values["data_name"] += extension
        return values


class FileETypeMapper(FileETypeMixin, mappers.ETypeMapper):
    __select__ = mappers.ETypeMapper.__select__ & match_kwargs({"etype": "File"})


class FileTargetETypeMapper(FileETypeMixin, mappers.TargetETypeMapper):
    __select__ = mappers.TargetETypeMapper.__select__ & match_kwargs({"etype": "File"})


class FrarchivesBytesAttributeMapper(mappers.AttributeMapper):
    __select__ = mappers.yams_match(target_types="Bytes")
    jsl_field_class = jsl.fields.StringField

    def jsl_field(self, *args, **kwargs):
        kwargs.setdefault("format", "data-url")
        return super(mappers.AttributeMapper, self).jsl_field(*args, **kwargs)

    def values(self, entity, instance):
        if self.rtype not in instance:
            return super(FrarchivesBytesAttributeMapper, self).values(entity, instance)
        value = instance[self.rtype]
        try:
            filedata, mediatype, parameters = parse_dataurl(value)
        except ValueError as exc:
            raise ValidationError(entity, {self.rtype: str(exc)})
        return {self.rtype: Binary(filedata)}

    def serialize(self, entity):
        attr = getattr(entity, self.rtype)
        value = attr.read() if attr is not None else None
        if value is None:
            return None
        parts = ("data:;", "base64," + base64.b64encode(value).decode("utf8"))
        return "".join(parts)


@monkeypatch(ijsonschema.IJSONSchemaEntityAdapter)
def add_relation(self, values):
    """relate current entity to eid listed in values through ``rtype``"""
    req = self._cw
    entity = self.entity
    rtype = self.cw_extra_kwargs["rtype"]
    if values:
        eids = ",".join(str(value) for value in values)
        req.execute(
            "SET X {rtype} Y WHERE X eid %(e)s, Y eid IN ({eids}), "
            "NOT X {rtype} Y".format(rtype=rtype, eids=eids),
            {"e": entity.eid},
        )
    rset = req.execute(
        "Any Y WHERE X {rtype} Y, X eid %(e)s".format(rtype=rtype), {"e": entity.eid}
    )
    to_delete = {y for y, in rset} - values
    if to_delete:
        req.execute(
            "DELETE X {rtype} Y WHERE X eid %(e)s, Y eid IN ({eids})".format(
                rtype=rtype, eids=",".join(str(e) for e in to_delete)
            ),
            {"e": entity.eid},
        )


class TrInfoJSONSchemaEntityAdapter(ijsonschema.IJSONSchemaEntityAdapter):

    __select__ = ijsonschema.IJSONSchemaEntityAdapter.__select__ & is_instance("TrInfo")

    def serialize(self):
        data = super(TrInfoJSONSchemaEntityAdapter, self).serialize()
        for rtype in ("from_state", "to_state"):
            data[rtype] = getattr(self.entity, rtype)[0].name
        return data


class FrarchivesIJSONSchemaRelatedEntityAdapter(ijsonschema.IJSONSchemaRelatedEntityAdapter):
    """override default serialize method to add absoluteURL"""

    def serialize(self):
        data = super(FrarchivesIJSONSchemaRelatedEntityAdapter, self).serialize()
        data["absoluteUrl"] = self.entity.absolute_url()
        data["cw_etype"] = self.entity.cw_etype
        return data


class FrarchivesIJSONSchemaEntityAdapter(ijsonschema.IJSONSchemaEntityAdapter):

    __select__ = ijsonschema.IJSONSchemaEntityAdapter.__select__ & yes()

    def serialize(self, attrs=None):
        data = super(FrarchivesIJSONSchemaEntityAdapter, self).serialize()
        entity = self.entity
        if "dc_title" not in data:
            data["dc_title"] = entity.dc_title()
        # XXX we must be able to do without eid and cw_etype
        data.update(
            {"absoluteUrl": entity.absolute_url(), "cw_etype": entity.cw_etype, "eid": entity.eid}
        )
        return data


class IndexEntityAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = (
        FrarchivesIJSONSchemaEntityAdapter.__select__
        & is_instance("BaseContent", "ExternRef", "CommemorationItem")
        & match_kwargs({"rtype": "related_authority"})
    )

    def add_relation(self, values):
        req = self._cw
        entity = self.entity
        already_linked = {
            e
            for e, in req.execute(
                "Any X WHERE E related_authority X, E eid %(e)s", {"e": entity.eid}
            )
        }
        to_add = values - already_linked
        if to_add:
            req.execute(
                "SET X related_authority A WHERE A eid IN ({}), X eid %(e)s".format(
                    ",".join(str(e) for e in values - already_linked)
                ),
                {"e": entity.eid},
            )
        to_delete = already_linked - values
        if to_delete:
            req.execute(
                "DELETE X related_authority A WHERE A eid IN ({}), X eid %(e)s".format(
                    ",".join(str(e) for e in to_delete)
                ),
                {"e": entity.eid},
            )


class RqTaskIJSONSchemaEntityAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & is_instance("RqTask")

    def serialize(self):
        entity = self.entity
        data = super(RqTaskIJSONSchemaEntityAdapter, self).serialize()
        job = entity.cw_adapt_to("IRqJob")
        data["status"] = job.status
        for attr in ("enqueued_at", "started_at", "ended_at"):
            value = getattr(job, attr)
            if value is not None:
                data[attr] = ustrftime(value, "%Y/%m/%d %H:%M:%S")
            else:
                data[attr] = None
        return data


class IndexIJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """This adapter manage edition if `authority` object entities"""

    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & is_instance("Index")

    @property
    def authority(self):
        return self.entity.authority[0]

    def edit_entity(self, instance):
        """Return a CubicWeb entity built from `instance` data matching this
        JSON schema.
        """
        authority = self.authority
        authority.cw_adapt_to("IJSONSchema").edit_entity(instance)
        return self.entity

    def serialize(self):
        """Return a dictionary of entity's data suitable for JSON
        serialization.
        """
        authority_data = self.authority.cw_adapt_to("IJSONSchema").serialize()
        for attr in (
            "absoluteUrl",
            "creation_date",
            "cw_etype",
            "cwuri",
            "dc_title",
            "eid",
            "modification_date",
        ):
            if attr in authority_data:
                del authority_data[attr]
        data = super(IndexIJSONSchemaAdapter, self).serialize()
        del data["pniarole"]
        del data["authority"]
        data.update(authority_data)
        return data


class WorkflowableJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """IJSONSchema adapter for workflowable entity types."""

    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & adaptable("IWorkflowable")

    def serialize(self):
        data = super(WorkflowableJSONSchemaAdapter, self).serialize()
        wfentity = self.entity.cw_adapt_to("IWorkflowable")
        data["workflow_state"] = self._cw._(wfentity.state)
        return data


class DownloadablableJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """IJSONSchema adapter for downloadable entity types."""

    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & adaptable("IDownloadable")

    def serialize(self):
        data = super(DownloadablableJSONSchemaAdapter, self).serialize()
        adapted = self.entity.cw_adapt_to("IDownloadable")
        data["content_type"] = adapted.download_content_type()
        try:
            # XXX Using CubicWebPyramidRequest (i.e. _cw here)'s _request
            # attribute.
            pyramid_request = self._cw._request
        except AttributeError:
            pass
        else:
            data["download_url"] = pyramid_request.route_path(
                "bfss", hash=self.entity.data_hash, basename=self.entity.data_name
            )

        return data


class VocabularyFieldMixIn(object):
    """AttributeMapper jsl_field
    react-jsonschema-from does not support oneOf Field yet"""

    def jsl_field(self, schema_role, **kwargs):
        kwargs.setdefault("format", self.format)
        field_factory = super(mappers.AttributeMapper, self).jsl_field
        if schema_role in (CREATION_ROLE, EDITION_ROLE):
            if "required" not in kwargs and self.attr.cardinality[0] == "1":
                kwargs["required"] = True
            if "default" not in kwargs and self.attr.default is not None:
                kwargs["default"] = self.attr.default
            vocabulary_constraint = next(
                (
                    cstr
                    for cstr in self.attr.constraints
                    if isinstance(cstr, StaticVocabularyConstraint)
                ),
                None,
            )
            if vocabulary_constraint:
                # we dont use oneOf field because of the
                # react-jsonschema-for oneOf field support lack,
                # but we still ignore other constraints.
                voc = vocabulary_constraint.vocabulary()
                kwargs.update({"enum": voc, "enum_titles": [self._cw._(v) for v in voc]})
                return field_factory(schema_role, **kwargs)
            for constraint in self.attr.constraints:
                self.add_constraint(constraint, kwargs)
        return field_factory(schema_role, **kwargs)


class FrarchivesStringMapper(VocabularyFieldMixIn, mappers.StringMapper):
    jsl_field_class = FrarchivesStringField


@staticmethod
def _type(json_value):
    return datetime.strptime(json_value, "%Y-%m-%d").date()


mappers.DateMapper._type = _type


class InGroupsRelationMapper(mappers.BaseRelationMapper):
    __select__ = mappers.BaseRelationMapper.__select__ & mappers.yams_match(
        etype="CWUser", rtype="in_group", role="subject"
    )
    jsl_field_class = FrarchivesStringField
    _type = str

    def jsl_field(self, schema_role, **kwargs):
        req = self._cw
        groups = list(
            req.execute(
                "Any X, N WHERE X is CWGroup, " 'X name N, NOT X name IN ("owners", "guests")'
            )
        )
        kwargs.update(
            {
                "enum": [self._type(e[0]) for e in groups],
                "enum_titles": [req._(e[1]) for e in groups],
                "required": True,
            }
        )
        return super(InGroupsRelationMapper, self).jsl_field(schema_role, **kwargs)

    def serialize(self, entity):
        rset = entity.related(self.rtype, self.role, targettypes=tuple(self.target_types))
        if not rset:
            return None
        return self._type(rset[0][0])

    def values(self, entity, instance):
        if self.rtype in instance:
            if entity is not None:
                # delete existing values
                entity.cw_set(**{self.rtype: None})
            return {self.orm_rtype: self._type(instance[self.rtype])}
        return {}


class AutocompleteRelationMapperMixIn(object):
    def jsl_field_targets(self, schema_role):
        yield jsl.fields.StringField()


class AutocompleteETypeExternRefRelationMapper(
    AutocompleteRelationMapperMixIn, mappers.ETypeRelationMapper
):
    __select__ = mappers.ETypeRelationMapper.__select__ & mappers.yams_match(
        etype="ExternRef", rtype="exref_service", role="subject"
    )


class AutocompleteEntityExternRefRelationMapper(
    AutocompleteRelationMapperMixIn, mappers.EntityRelationMapper
):
    __select__ = mappers.EntityRelationMapper.__select__ & mappers.yams_match(
        etype="ExternRef", rtype="exref_service", role="subject"
    )


class AutocompleteETypeBaseContentRelationMapper(
    AutocompleteRelationMapperMixIn, mappers.ETypeRelationMapper
):
    __select__ = mappers.ETypeRelationMapper.__select__ & mappers.yams_match(
        etype="BaseContent", rtype="basecontent_service", role="subject"
    )


class AutocompleteEntityBaseContentRelationMapper(
    AutocompleteRelationMapperMixIn, mappers.EntityRelationMapper
):
    __select__ = mappers.EntityRelationMapper.__select__ & mappers.yams_match(
        etype="BaseContent", rtype="basecontent_service", role="subject"
    )


# class AutocompleteEntityPniaAgentRelationMapper(AutocompleteRelationMapperMixIn,
#                                                 mappers.ETypeRelationMapper):
#     __select__ = (mappers.ETypeRelationMapper.__select__
#                   & mappers.yams_match(etype='PniaAgent', rtype='preferred_form',
#                                        role='subject'))


# class AutocompleteEntityPniaLocationRelationMapper(AutocompleteRelationMapperMixIn,
#                                                    mappers.ETypeRelationMapper):
#     __select__ = (mappers.ETypeRelationMapper.__select__
#                   & mappers.yams_match(etype='PniaLocation', rtype='preferred_form',
#                                        role='subject'))


# class AutocompleteEntityPniaSubjectRelationMapper(AutocompleteRelationMapperMixIn,
#                                                   mappers.ETypeRelationMapper):
#     __select__ = (mappers.ETypeRelationMapper.__select__
#                   & mappers.yams_match(etype='PniaSubject', rtype='preferred_form',
#                                        role='subject'))


class ExternalUrlSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & is_instance("ExternalUri")

    def serialize(self):
        data = super(ExternalUrlSchemaAdapter, self).serialize()
        entity = self.entity
        data.update(
            {
                "source": entity.source,
            }
        )
        igeo = entity.cw_adapt_to("IGeoDB")
        if igeo:
            data.update({"latitude": igeo.latitude, "longitude": igeo.longitude})
        return data


class ExternalIdSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & is_instance("ExternalId")

    def serialize(self):
        data = super(ExternalIdSchemaAdapter, self).serialize()
        entity = self.entity
        data.update(
            {
                "source": entity.source,
            }
        )
        igeo = entity.cw_adapt_to("IGeoDB")
        if igeo:
            data.update(
                {
                    "latitude": igeo.latitude,
                    "longitude": igeo.longitude,
                    "link": igeo.openstreetmap_uri,
                }
            )
        return data


class OAIRepositoryIJSONSchemaAdapter(
    ImportIndexPolicyMinix, ijsonschema.IJSONSchemaRelationTargetETypeAdapter
):
    __select__ = ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__ & match_kwargs(
        {"etype": "OAIRepository"}
    )

    def creation_schema(self, **kwargs):
        schema = super(OAIRepositoryIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema["properties"]
        for prop, defs in self.index_policy_props.items():
            props[prop] = defs
        return schema


def registration_callback(vreg):
    vreg.register_all(
        list(globals().values()),
        __name__,
        (
            FrarchivesIJSONSchemaRelatedEntityAdapter,
            FrarchivesBytesAttributeMapper,
            FrarchivesStringMapper,
        ),
    )

    vreg.register_and_replace(
        FrarchivesIJSONSchemaRelatedEntityAdapter, ijsonschema.IJSONSchemaRelatedEntityAdapter
    )
    vreg.register_and_replace(FrarchivesBytesAttributeMapper, mappers.BytesMapper)
    vreg.register_and_replace(FrarchivesStringMapper, mappers.StringMapper)

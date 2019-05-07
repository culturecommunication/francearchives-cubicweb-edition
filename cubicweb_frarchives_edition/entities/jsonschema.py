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

from six import text_type

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
from cubicweb_frarchives_edition.cms.faimport import (process_faimport_zip,
                                                      process_faimport_xml,
                                                      process_csvimport_zip)
from cubicweb_frarchives_edition import tasks
from cubicweb_frarchives_edition.api import jsonapi_error, JSONBadRequest


class CommemoCollectionIJSONSchemaRelationTargetETypeAdapter(ijsonschema.IJSONSchemaRelationTargetETypeAdapter):  # noqa
    __select__ = (ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__
                  & match_kwargs({'etype': 'CommemoCollection'}))

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(
            CommemoCollectionIJSONSchemaRelationTargetETypeAdapter, self).creation_schema(**kwargs)
        commemocollection_attrs = schema['properties']
        commemocollection_attrs['sections'] = {
            'type': 'array',
            'title': req._('Section'),
            'items': {
                'type': 'object',
                'properties': {
                    'title': {
                        'type': 'string',
                        'title': req._('title')
                    }
                }
            }
        }
        commemocollection_attrs['commemorationItems'] = {
            'type': 'array',
            'title': req._('Intitules de la rubrique presentation'),
            'items': {
                'type': 'object',
                'properties': {
                    'title': {
                        'type': 'string',
                        'title': req._('title')
                    }
                }
            }
        }
        return schema

    def create_entity(self, instance, target):
        entity = super(CommemoCollectionIJSONSchemaRelationTargetETypeAdapter,
                       self).create_entity(instance, target)
        req = self._cw
        for order, i in enumerate(instance.get('commemorationItems', ())):
            req.create_entity('CommemorationItem',
                              reverse_children=entity,
                              title=i['title'],
                              order=order + 1,
                              alphatitle=i['title'],
                              commemoration_year=entity.year,
                              collection_top=entity)
        for s in instance.get('sections', ()):
            req.create_entity('Section',
                              reverse_children=entity,
                              title=s['title'])
        return entity


class CommemorationIJSONSchemaRelationTargetETypeAdapter(ijsonschema.IJSONSchemaRelationTargetETypeAdapter):  # noqa
    __select__ = (ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__
                  & match_kwargs({'etype': 'CommemorationItem'}))

    def create_entity(self, instance, target):
        entity = super(CommemorationIJSONSchemaRelationTargetETypeAdapter,
                       self).create_entity(instance, target)
        if target.cw_etype == 'CommemoCollection':
            collection_top = target
        else:
            assert target.cw_etype == 'Section' and target.is_commemo_section()
            collection_top = target.commemo_section
        # XXX add 'collection_top' to "Additional properties"
        entity.cw_set(collection_top=collection_top)
        return entity


class RqTaskIJSONSchemaAdapter(ijsonschema.IJSONSchemaETypeAdapter):
    TASK_MAP = OrderedDict([
        (_('import_ead'), tasks.import_ead),
        (_('import_csv'), tasks.import_csv),
        (_('publish_findingaid'), tasks.publish_findingaid),
        (_('export_ape'), tasks.export_ape),
        (_('compute_location_authorities_to_group'),
         tasks.compute_location_authorities_to_group),
        (_('group_location_authorities'),
         tasks.group_location_authorities),
    ])
    __select__ = (ijsonschema.IJSONSchemaETypeAdapter.__select__
                  & match_kwargs({'etype': 'RqTask'}))

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['name'] = {
            'title': req._('type of task'),
            'type': 'string',
            'enum': self.TASK_MAP.keys(),
            'enumNames': [req._(t) for t in self.TASK_MAP],
        }
        if 'title' not in schema['required']:
            schema['required'].append('title')
        return schema

    def create_entity(self, instance):
        req = self._cw
        entity = req.create_entity('RqTask', name=instance['name'], title=instance['title'])
        return entity


class RqTaskDedupeAuthJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({'schema_type':
                                                                     'dedupe_authorities'})
    TASK_MAP = OrderedDict([
        (_('dedupe_authorities'), tasks.dedupe_authorities),
    ])

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskDedupeAuthJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['service'] = {
            'type': 'string',
            'title': req._('service code'),
            'enum': [
                c for c, in req.execute(
                    'Any C ORDERBY C WHERE X is Service, X code C, NOT X code NULL'
                )
            ],
        }
        props['strict'] = {
            'type': 'boolean',
            'title': req._('compare strictly authority label'),
            'default': True,
        }
        return schema

    def create_entity(self, instance):
        entity = super(RqTaskDedupeAuthJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(
            func,
            instance.get('strict', True),
            instance.get('service'),
        )
        return entity


class RqTaskImportEadIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({'schema_type': 'import_ead'})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportEadIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['file'] = {
            'type': 'string',
            'title': req._('zip file'),
        }
        props['service'] = {
            'type': 'string',
            'title': req._('service code (required only for xml, optional for zip)'),
            'enum': [
                c for c, in req.execute(
                    'Any C ORDERBY C WHERE X is Service, X code C, NOT X code NULL'
                )
            ],
        }
        props['force-delete'] = {
            'type': 'boolean',
            'title': req._('force-delete-old-findingaids'),
            'default': True
        }
        return schema

    def create_entity(self, instance):
        req = self._cw
        fileobj = instance['fileobj']
        code, ext = osp.splitext(fileobj.filename)
        if ext == '.zip':
            filepaths = process_faimport_zip(req, fileobj)
        elif ext == '.xml':
            filepaths = process_faimport_xml(req, fileobj, instance['service'])
        force_delete = instance.get('force-delete', False)
        auto_align = instance.get('auto-import-alignment', True)
        auto_dedupe = instance.get('auto-dedupe-authorities', True)
        entity = super(RqTaskImportEadIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(
            func,
            filepaths,
            force_delete,
            auto_align,
            auto_dedupe,
        )
        return entity


class RqTaskImportOaiIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({'schema_type': 'import_oai'})
    TASK_MAP = OrderedDict([
        (_('import_oai'), tasks.import_oai),
    ])

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportOaiIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['oairepository'] = {
            'type': 'integer',
            'title': req._('oai repository identifier'),
        }
        props['force-refresh'] = {
            'type': 'boolean',
            'title': req._('force-import-oai'),
        }
        schema['required'].append('oairepository')
        return schema

    def create_entity(self, instance):
        force_refresh = instance.get('force-refresh', True)
        entity = super(RqTaskImportOaiIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func, instance['oairepository'], force_refresh)
        return entity


class RqTaskExportApeIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({'schema_type': 'export_ape'})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskExportApeIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['service_code'] = {
            'type': 'string',
            'title': req._('service code from which you want to generate APE files'),
        }
        return schema

    def create_entity(self, instance):
        req = self._cw
        rset = req.execute('DISTINCT Any C WHERE X is Service, X code C, NOT X code NULL, '
                           'F service X, F is FindingAid')
        allcodes = {c for c, in rset}
        if instance['service_code'] not in allcodes:
            raise JSONBadRequest(jsonapi_error(
                status=422, pointer='service_code',
                details=req._('bad service code should be one of: ') + ','.join(allcodes)))
        entity = super(RqTaskExportApeIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func, instance['service_code'])
        return entity


class AbstractRqTaskImportCSVAdapter(RqTaskIJSONSchemaAdapter):
    __abstract__ = True

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(AbstractRqTaskImportCSVAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['file'] = {
            'type': 'string',
            'title': req._('csv file to import'),
        }
        schema['required'] = list(set(schema['required']) | {'file', 'title'})
        return schema

    def create_entity(self, instance):
        fd, filepath = tempfile.mkstemp()
        os.write(fd, instance['fileobj'].value)
        os.close(fd)
        entity = super(AbstractRqTaskImportCSVAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func, filepath)
        return entity


class RqTaskImportAlignmentIJSONSchemaAdapter(AbstractRqTaskImportCSVAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({
        'schema_type': 'import_alignment'})
    TASK_MAP = OrderedDict([
        (_('import_alignment'), tasks.import_alignment),
    ])


class RqTaskComputeAlignementJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {'schema_type': 'compute_alignment'})
    TASK_MAP = OrderedDict([
        (_('compute_alignment'), tasks.compute_alignment),
    ])

    def creation_schema(self, **kwargs):
        schema = super(RqTaskComputeAlignementJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['findingaid_eid'] = {
            'type': 'string',
            'title': self._cw._('findingaid eid'),
        }
        schema['required'].append('findingaid_eid')
        return schema

    def create_entity(self, instance):
        req = self._cw
        try:
            if instance['findingaid_eid'].isdigit():
                rset = req.execute(
                    'Any X WHERE X is FindingAid, X eid %(e)s',
                    {'e': instance['findingaid_eid']}
                )
            else:
                rset = req.execute(
                    'Any X WHERE X is FindingAid, X stable_id %(e)s',
                    {'e': instance['findingaid_eid']}
                )
        except Exception:
            # e.g. a bad eid might cause a TypeResolverException
            self.exception('failed to fetch FindingAid witg id %r', instance['findingaid_eid'])
            raise JSONBadRequest(jsonapi_error(
                status=422, pointer='findingaid_eid',
                details=req._('no findingaid with this id')))
        if not rset:
            raise JSONBadRequest(jsonapi_error(
                status=422, pointer='findingaid_eid',
                details=req._('no findingaid with this id')))
        entity = super(RqTaskComputeAlignementJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func, instance['findingaid_eid'])
        return entity


class RqTaskPublishFAIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {'schema_type': 'publish_findingaid'})

    def creation_schema(self, **kwargs):
        schema = super(RqTaskPublishFAIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['importead_task_eid'] = {
            'type': 'integer',
            'title': self._cw._('task eid'),
        }
        schema['required'].append('importead_task_eid')
        return schema

    def create_entity(self, instance):
        req = self._cw
        try:
            rset = req.find('RqTask', eid=instance['importead_task_eid'])
        except TypeResolverException:
            rset = None
        if not rset:
            raise JSONBadRequest(jsonapi_error(
                status=422, pointer='importead_task_eid',
                details=req._('bad task eid')))
        entity = super(RqTaskPublishFAIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func, instance['importead_task_eid'])
        return entity


class RqTaskImportCSVIJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs({'schema_type': 'import_csv'})

    def creation_schema(self, **kwargs):
        req = self._cw
        schema = super(RqTaskImportCSVIJSONSchemaAdapter, self).creation_schema(**kwargs)
        props = schema['properties']
        props['file'] = {
            'type': 'string',
            'title': req._('csv file to import'),
        }
        props['force-delete'] = {
            'type': 'boolean',
            'title': req._('force-delete-old-findingaids'),
        }
        schema['required'] = list(set(schema['required']) | {'file', 'title'})
        return schema

    def create_entity(self, instance):
        req = self._cw
        force_delete = instance.get('force-delete', True)
        auto_align = instance.get('auto-import-alignment', True)
        auto_dedupe = instance.get('auto-dedupe-authorities', True)
        entity = super(RqTaskImportCSVIJSONSchemaAdapter, self).create_entity(instance)
        func = self.TASK_MAP[instance['name']]
        filepaths = process_csvimport_zip(req, instance['fileobj'])
        entity.cw_adapt_to('IRqJob').enqueue(
            func,
            filepaths,
            force_delete,
            auto_align,
            auto_dedupe)
        return entity


class RqTaskComputeLocAuthoritesToGroupJSONSchemaAdapter(RqTaskIJSONSchemaAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {'schema_type': 'compute_location_authorities_to_group'})

    def create_entity(self, instance):
        entity = super(RqTaskComputeLocAuthoritesToGroupJSONSchemaAdapter, self).create_entity(
            instance)
        func = self.TASK_MAP[instance['name']]
        entity.cw_adapt_to('IRqJob').enqueue(func)
        return entity


class RqTaskGroupLocAuthoritesSONSchemaAdapter(AbstractRqTaskImportCSVAdapter):
    __select__ = RqTaskIJSONSchemaAdapter.__select__ & match_kwargs(
        {'schema_type': 'group_location_authorities'})


class TargetIJSONSchemaRelationTargetETypeAdapterMixIn(object):

    @property
    def authority_etype(self):
        """The entity type bound to this adapter."""
        return str(self.cw_extra_kwargs['etarget_role'])

    def creation_schema(self, **kwargs):
        authority_schema = self.authority_schema()
        authority_schema['required'] = ['label']
        return authority_schema

    def create_authority(self, values):
        label = values['label'].strip()
        if not label:
            msg = self._cw._('"label" is required')
            raise ValidationError(None, {'label': msg})
        req = self._cw
        authority = req.vreg['adapters'].select(
            'IJSONSchema', req, etype=self.authority_etype).create_entity(values)
        return authority

    def authority_schema(self):
        req = self._cw
        authority_adaptor = req.vreg['adapters'].select(
            'IJSONSchema', req, **{'etype': self.authority_etype,
                                   'rtype': 'authority',
                                   'role': 'object'})
        return authority_adaptor.creation_schema()

    def create_entity(self, instance, target):
        req = self._cw
        values = {}
        # XXX get rid of iter_relation_mappers
        for adapter, relation_mapper in self.iter_relation_mappers(CREATION_ROLE):
            values.update(relation_mapper.values(None, instance))
        ce = req.create_entity
        authority = self.create_authority(values)
        pnia_role = req.find('IndexRole', label=u'subject')
        if pnia_role:
            pnia_role = pnia_role[0]
        else:
            pnia_role = ce('IndexRole',
                           label=u'subject')
        entity = ce(self.etype, authority=authority,
                    pniarole=pnia_role)
        entity.cw_set(**{orm_rtype(self.rtype, self.role): target})
        return entity


class AgentAuthorityTargetIJSONSchemaRelationTargetETypeAdapter(
        TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
        ijsonschema.IJSONSchemaRelationTargetETypeAdapter):
    __select__ = (ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__
                  & match_kwargs({'etype': 'AgentName', 'rtype': 'auhtority',
                                  'role': 'subject',
                                  'etarget_role': 'AgentAuthority'}))

    def creation_schema(self, **kwargs):
        authority_schema = self.authority_schema()
        authority_schema['required'] = ['label']
        return authority_schema


class LocationAuthorityTargetIJSONSchemaRelationTargetETypeAdapter(
        TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
        ijsonschema.IJSONSchemaRelationTargetETypeAdapter):
    __select__ = (ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__
                  & match_kwargs({'etype': 'Index', 'rtype': 'target',
                                  'role': 'subject',
                                  'etarget_role': 'LocationAuthority'}))


class SubjectAuhtorityFormTargetIJSONSchemaRelationTargetETypeAdapter(
        TargetIJSONSchemaRelationTargetETypeAdapterMixIn,
        ijsonschema.IJSONSchemaRelationTargetETypeAdapter):
    __select__ = (ijsonschema.IJSONSchemaRelationTargetETypeAdapter.__select__
                  & match_kwargs({'etype': 'Index', 'rtype': 'target',
                                  'role': 'subject',
                                  'etarget_role': 'SubjectAuhtority'}))


class FrarchivesStringField(jsl.fields.StringField):
    """react-jsonschema-form waits for enumNames key instead of

    options.enum_titles in schema
    """

    def __init__(self, **kwargs):
        super(FrarchivesStringField, self).__init__(**kwargs)
        if 'enum' in kwargs:
            self.enum_titles = kwargs['enum_titles']

    def get_definitions_and_schema(self, **kwargs):
        definitions, schema = super(
            jsl.fields.StringField, self).get_definitions_and_schema(**kwargs)
        if 'enum' not in schema:
            return definitions, schema
        schema['enumNames'] = self.enum_titles
        return definitions, schema


class TrInfoIJSONSchemaAdapter(ijsonschema.IJSONSchemaETypeAdapter):
    __select__ = (ijsonschema.IJSONSchemaETypeAdapter.__select__
                  & match_kwargs({'etype': 'TrInfo'})
                  & match_kwargs('for_entity'))

    def creation_schema(self, **kwargs):
        # By-pass IJSONSchemaETypeAdapter's method which does not support
        # TrInfo entity type for now.
        entity = self.cw_extra_kwargs['for_entity']
        wfentity = entity.cw_adapt_to('IWorkflowable')
        builder = self._cw.vreg['components'].select(
            'jsonschema.map.builder', self._cw)
        enum = [trinfo.name for trinfo in wfentity.possible_transitions()]
        enum_titles = map(self._cw._, enum)
        attrs = {
            'name': FrarchivesStringField(
                enum=enum, enum_titles=enum_titles, required=True),
            'Options': builder.set_default_options('TrInfo'),
        }
        doc = type('TrInfo', (jsl.Document, ), attrs)
        return jsl.DocumentField(doc, as_ref=True).get_schema(**kwargs)


class FileDataAttributeMapper(mappers.BytesMapper):
    __select__ = (mappers.BytesMapper.__select__
                  & mappers.yams_match(etype='File', rtype='data',
                                       role='subject', target_types='Bytes'))
    jsl_field_class = jsl.fields.StringField

    def jsl_field(self, *args, **kwargs):
        kwargs.setdefault('format', 'data-url')
        return super(FileDataAttributeMapper, self).jsl_field(*args, **kwargs)

    def values(self, entity, instance):
        if self.rtype not in instance:
            return super(FileDataAttributeMapper, self).values(entity, instance)
        value = instance[self.rtype]
        try:
            filedata, mediatype, parameters = parse_dataurl(value)
        except ValueError as exc:
            raise ValidationError(entity, {self.rtype: str(exc)})
        if 'name' not in parameters:
            self.warning(
                'uploaded data-url field for %s has no "name" parameter',
                entity)
        data_name = parameters.get(u'name')
        return {
            'data_name': data_name,
            'data_format': mediatype,
            'data_encoding': parameters.get('charset'),
            'data': Binary(filedata)
        }

    def serialize(self, entity):
        if entity.data is None:
            return None
        value = entity.data.read()
        if value is None:
            return None
        parts = [b'data:']
        mimetype = entity.data_format
        if mimetype:
            parts.append(mimetype.encode('utf-8') + b';')
        name = entity.data_name
        if name:
            parts.append(b'name=' + name.encode('utf-8') + b';')
        parts.append(b'base64,' + base64.b64encode(value))
        return b''.join(parts)


class FileETypeMixin(object):

    def values(self, entity, instance):
        values = super(FileETypeMixin, self).values(entity, instance)
        if values['data_name'] is None:
            values['data_name'] = values.get('title', u'<unspecified file name>')
        _, ext = osp.splitext(values['data_name'])
        if ext:
            values['data_format'] = mimetypes.guess_type(values['data_name'])[0]
        else:
            extension = mimetypes.guess_extension(values['data_format'])
            if extension:
                values['data_name'] += extension
        return values


class FileETypeMapper(FileETypeMixin, mappers.ETypeMapper):
    __select__ = mappers.ETypeMapper.__select__ & match_kwargs({'etype': 'File'})


class FileTargetETypeMapper(FileETypeMixin, mappers.TargetETypeMapper):
    __select__ = mappers.TargetETypeMapper.__select__ & match_kwargs({'etype': 'File'})


class FrarchivesBytesAttributeMapper(mappers.AttributeMapper):
    __select__ = mappers.yams_match(target_types='Bytes')
    jsl_field_class = jsl.fields.StringField

    def jsl_field(self, *args, **kwargs):
        kwargs.setdefault('format', 'data-url')
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
        parts = (b'data:;', b'base64,' + base64.b64encode(value))
        return b''.join(parts)


@monkeypatch(ijsonschema.IJSONSchemaEntityAdapter)
def add_relation(self, values):
    """relate current entity to eid listed in values through ``rtype``"""
    req = self._cw
    entity = self.entity
    rtype = self.cw_extra_kwargs['rtype']
    if values:
        eids = ','.join(str(value) for value in values)
        req.execute('SET X {rtype} Y WHERE X eid %(e)s, Y eid IN ({eids}), '
                    'NOT X {rtype} Y'.format(rtype=rtype, eids=eids),
                    {'e': entity.eid})
    rset = req.execute('Any Y WHERE X {rtype} Y, X eid %(e)s'.format(rtype=rtype),
                       {'e': entity.eid})
    to_delete = {y for y, in rset} - values
    if to_delete:
        req.execute('DELETE X {rtype} Y WHERE X eid %(e)s, Y eid IN ({eids})'.format(
            rtype=rtype, eids=','.join(str(e) for e in to_delete)), {'e': entity.eid})


class TrInfoJSONSchemaEntityAdapter(ijsonschema.IJSONSchemaEntityAdapter):

    __select__ = (ijsonschema.IJSONSchemaEntityAdapter.__select__
                  & is_instance('TrInfo'))

    def serialize(self):
        data = super(TrInfoJSONSchemaEntityAdapter, self).serialize()
        for rtype in ('from_state', 'to_state'):
            data[rtype] = getattr(self.entity, rtype)[0].name
        return data


class FrarchivesIJSONSchemaRelatedEntityAdapter(ijsonschema.IJSONSchemaRelatedEntityAdapter):
    """override default serialize method to add absoluteURL"""

    def serialize(self):
        data = super(FrarchivesIJSONSchemaRelatedEntityAdapter, self).serialize()
        data['absoluteUrl'] = self.entity.absolute_url()
        data['cw_etype'] = self.entity.cw_etype
        return data


class FrarchivesIJSONSchemaEntityAdapter(ijsonschema.IJSONSchemaEntityAdapter):

    __select__ = ijsonschema.IJSONSchemaEntityAdapter.__select__ & yes()

    def serialize(self, attrs=None):
        data = super(FrarchivesIJSONSchemaEntityAdapter, self).serialize()
        entity = self.entity
        if 'dc_title' not in data:
            data['dc_title'] = entity.dc_title()
        # XXX we must be able to do without eid and cw_etype
        data.update({
            'absoluteUrl': entity.absolute_url(),
            'cw_etype': entity.cw_etype,
            'eid': entity.eid})
        return data


class IndexEntityAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = (FrarchivesIJSONSchemaEntityAdapter.__select__
                  & is_instance('ExternRef', 'CommemorationItem')
                  & match_kwargs({'rtype': 'related_authority'}))

    def add_relation(self, values):
        req = self._cw
        entity = self.entity
        already_linked = {e for e, in req.execute(
            'Any X WHERE E related_authority X, E eid %(e)s', {'e': entity.eid})}
        req.execute('SET X related_authority A WHERE A eid IN ({}), X eid %(e)s'.format(
            ','.join(str(e) for e in values - already_linked)), {'e': entity.eid})
        to_delete = already_linked - values
        if to_delete:
            req.execute('DELETE X related_authority A WHERE A eid IN ({}), X eid %(e)s'.format(
                ','.join(str(e) for e in to_delete)), {'e': entity.eid})


class RqTaskIJSONSchemaEntityAdapter(FrarchivesIJSONSchemaEntityAdapter):
    __select__ = FrarchivesIJSONSchemaEntityAdapter.__select__ & is_instance('RqTask')

    def serialize(self):
        entity = self.entity
        data = super(RqTaskIJSONSchemaEntityAdapter, self).serialize()
        job = entity.cw_adapt_to('IRqJob')
        data['status'] = job.status
        for attr in ('enqueued_at', 'started_at', 'ended_at'):
            value = getattr(job, attr)
            if value is not None:
                data[attr] = ustrftime(value, '%Y/%m/%d %H:%M:%S')
            else:
                data[attr] = None
        return data


class IndexIJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """This adapter manage edition if `authority` object entities"""
    __select__ = (FrarchivesIJSONSchemaEntityAdapter.__select__
                  & is_instance('Index'))

    @property
    def authority(self):
        return self.entity.authority[0]

    def edit_entity(self, instance):
        """Return a CubicWeb entity built from `instance` data matching this
        JSON schema.
        """
        authority = self.authority
        authority.cw_adapt_to('IJSONSchema').edit_entity(instance)
        return self.entity

    def serialize(self):
        """Return a dictionary of entity's data suitable for JSON
        serialization.
        """
        authority_data = self.authority.cw_adapt_to('IJSONSchema').serialize()
        for attr in ('absoluteUrl', 'creation_date', 'cw_etype',
                     'cwuri', 'dc_title', 'eid', 'modification_date'):
            if attr in authority_data:
                del authority_data[attr]
        data = super(IndexIJSONSchemaAdapter, self).serialize()
        del data['pniarole']
        del data['authority']
        data.update(authority_data)
        return data


class WorkflowableJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """IJSONSchema adapter for workflowable entity types."""

    __select__ = (FrarchivesIJSONSchemaEntityAdapter.__select__
                  & adaptable('IWorkflowable'))

    def serialize(self):
        data = super(WorkflowableJSONSchemaAdapter, self).serialize()
        wfentity = self.entity.cw_adapt_to('IWorkflowable')
        data['workflow_state'] = self._cw._(wfentity.state)
        return data


class DownloadablableJSONSchemaAdapter(FrarchivesIJSONSchemaEntityAdapter):
    """IJSONSchema adapter for downloadable entity types."""

    __select__ = (FrarchivesIJSONSchemaEntityAdapter.__select__
                  & adaptable('IDownloadable'))

    def serialize(self):
        data = super(DownloadablableJSONSchemaAdapter, self).serialize()
        adapted = self.entity.cw_adapt_to('IDownloadable')
        data['content_type'] = adapted.download_content_type()
        try:
            # XXX Using CubicWebPyramidRequest (i.e. _cw here)'s _request
            # attribute.
            pyramid_request = self._cw._request
        except AttributeError:
            pass
        else:
            data['download_url'] = pyramid_request.route_path(
                'bfss',
                hash=self.entity.data_sha1hex,
                basename=self.entity.data_name)

        return data


class VocabularyFieldMixIn(object):
    """ AttributeMapper jsl_field
    react-jsonschema-from does not support oneOf Field yet"""

    def jsl_field(self, schema_role, **kwargs):
        kwargs.setdefault('format', self.format)
        field_factory = super(mappers.AttributeMapper, self).jsl_field
        if schema_role in (CREATION_ROLE, EDITION_ROLE):
            if 'required' not in kwargs and self.attr.cardinality[0] == '1':
                kwargs['required'] = True
            if 'default' not in kwargs and self.attr.default is not None:
                kwargs['default'] = self.attr.default
            vocabulary_constraint = next(
                (cstr for cstr in self.attr.constraints
                 if isinstance(cstr, StaticVocabularyConstraint)), None)
            if vocabulary_constraint:
                # we dont use oneOf field because of the
                # react-jsonschema-for oneOf field support lack,
                # but we still ignore other constraints.
                voc = vocabulary_constraint.vocabulary()
                kwargs.update({
                    'enum': voc,
                    'enum_titles': [self._cw._(v) for v in voc]})
                return field_factory(schema_role, **kwargs)
            for constraint in self.attr.constraints:
                self.add_constraint(constraint, kwargs)
        return field_factory(schema_role, **kwargs)


class FrarchivesStringMapper(VocabularyFieldMixIn, mappers.StringMapper):
    jsl_field_class = FrarchivesStringField


@staticmethod
def _type(json_value):
    return datetime.strptime(json_value, '%Y-%m-%d').date()


mappers.DateMapper._type = _type


class InGroupsRelationMapper(mappers.BaseRelationMapper):
    __select__ = (mappers.BaseRelationMapper.__select__
                  & mappers.yams_match(etype='CWUser', rtype='in_group',
                                       role='subject'))
    jsl_field_class = FrarchivesStringField
    _type = text_type

    def jsl_field(self, schema_role, **kwargs):
        req = self._cw
        groups = list(req.execute('Any X, N WHERE X is CWGroup, '
                                  'X name N, NOT X name IN ("owners", "guests")'))
        kwargs.update({
            'enum': [self._type(e[0]) for e in groups],
            'enum_titles': [req._(e[1]) for e in groups],
            'required': True,
        })
        return super(InGroupsRelationMapper, self).jsl_field(schema_role, **kwargs)

    def serialize(self, entity):
        rset = entity.related(
            self.rtype, self.role, targettypes=tuple(self.target_types))
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


class AutocompleteETypeExternRefRelationMapper(AutocompleteRelationMapperMixIn,
                                               mappers.ETypeRelationMapper):
    __select__ = (mappers.ETypeRelationMapper.__select__
                  & mappers.yams_match(etype='ExternRef', rtype='exref_service',
                                       role='subject'))


class AutocompleteEntityExternRefRelationMapper(AutocompleteRelationMapperMixIn,
                                                mappers.EntityRelationMapper):
    __select__ = (mappers.EntityRelationMapper.__select__
                  & mappers.yams_match(etype='ExternRef', rtype='exref_service',
                                       role='subject'))


class AutocompleteETypeBaseContentRelationMapper(AutocompleteRelationMapperMixIn,
                                                 mappers.ETypeRelationMapper):
    __select__ = (mappers.ETypeRelationMapper.__select__
                  & mappers.yams_match(etype='BaseContent',
                                       rtype='basecontent_service',
                                       role='subject'))


class AutocompleteEntityBaseContentRelationMapper(AutocompleteRelationMapperMixIn,
                                                  mappers.EntityRelationMapper):
    __select__ = (mappers.EntityRelationMapper.__select__
                  & mappers.yams_match(etype='BaseContent',
                                       rtype='basecontent_service',
                                       role='subject'))


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


def registration_callback(vreg):
    vreg.register_all(globals().values(), __name__,
                      (FrarchivesIJSONSchemaRelatedEntityAdapter,
                       FrarchivesBytesAttributeMapper,
                       FrarchivesStringMapper))

    vreg.register_and_replace(FrarchivesIJSONSchemaRelatedEntityAdapter,
                              ijsonschema.IJSONSchemaRelatedEntityAdapter)
    vreg.register_and_replace(FrarchivesBytesAttributeMapper,
                              mappers.BytesMapper)
    vreg.register_and_replace(FrarchivesStringMapper,
                              mappers.StringMapper)

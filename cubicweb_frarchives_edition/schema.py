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

"""cubicweb-frachives_edition schema"""

from yams.buildobjs import (
    Bytes,
    Datetime,
    String,
    RelationDefinition,
    EntityType,
    RichString,
    ComputedRelation,
)
from cubicweb.schema import make_workflowable


from cubicweb_francearchives import CMS_I18N_OBJECTS
from cubicweb_francearchives.schema.ead import FindingAid
from cubicweb_francearchives.schema import cms as schema_cms

for etype in (
    [FindingAid]
    + [getattr(schema_cms, cms_type) for cms_type in schema_cms.CMS_OBJECTS]
    + [getattr(schema_cms, cms_type) for cms_type in CMS_I18N_OBJECTS]
    + [schema_cms.GlossaryTerm]
    + [schema_cms.FaqItem]
):
    make_workflowable(etype)


class output_file(RelationDefinition):
    subject = "RqTask"
    object = "File"
    cardinality = "*?"
    composite = "subject"


class subtasks(RelationDefinition):
    subject = "RqTask"
    object = "RqTask"
    cardinality = "*?"
    composite = "subject"


class oaiimport_task(RelationDefinition):
    subject = "RqTask"
    object = "OAIImportTask"
    cardinality = "??"


class fatask_findingaid(RelationDefinition):
    subject = "RqTask"
    object = "FindingAid"
    cardinality = "**"


class fatask_authorityrecord(RelationDefinition):
    subject = "RqTask"
    object = "AuthorityRecord"
    cardinality = "**"


def manager_permissions(cls):
    """Set __permissions__ of `cls` entity type class preventing modification
    when user is not in managers group"""
    cls.__permissions__ = cls.__permissions__.copy()
    cls.__permissions__["add"] = ("managers",)
    cls.__permissions__["update"] = ("managers",)
    cls.__permissions__["delete"] = ("managers",)
    return cls


# Customization of francearchives cms schema
manager_permissions(schema_cms.Section)


def post_build_callback(schema):
    set_users_permissions(schema)


def set_users_permissions(schema):
    etypes = [
        "Metadata",
        "ExternRef",
        "Did",
        "FindingAid",
        "CommemorationItem",
        "CommemoDate",
        "Service",
        "Label",
        "FAComponent",
        "BaseContent",
        "OfficialText",
        "Link",  # 'ConceptScheme',
        "Concept",
        "Image",
        "Category",
        "Circular",
        "Card",
        "SocialNetwork",  # 'EmailAddress',
        "DigitizedVersion",
        "Map",
        "NewsContent",  # 'PostalAddress',
        "FAHeader",
        "ExternalUri",
        "File",  # 'IndexRole'
        "SectionTranslation",
        "BaseContentTranslation",
        "CommemorationItemTranslation",
        "FaqItemTranslation",
        "NominaRecord",
    ]
    for etype in etypes:
        schema[etype].permissions.update(
            {"update": ("managers", "users"), "delete": ("managers", "users")}
        )


class RqTask(EntityType):
    title = String()
    name = String(required=True)
    status = String()
    log = Bytes()
    enqueued_at = Datetime()
    started_at = Datetime()
    ended_at = Datetime()
    output_descr = RichString(default_format="text/html")


class referenced_files(RelationDefinition):
    subject = (
        "Card",
        "NewsContent",
        "Map",
        "Section",
        "CommemorationItem",
        "ExternRef",
        "BaseContent",
    ) + CMS_I18N_OBJECTS
    object = "File"
    cardinality = "**"


class cssimage(ComputedRelation):
    rule = "O cssimage_of S"
    cardinality = "??"

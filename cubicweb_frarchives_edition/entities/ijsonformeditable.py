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

from cubicweb.predicates import is_instance, match_kwargs, score_entity
from cubicweb.schema import display_name
from cubicweb.view import EntityAdapter


class JsonFormEditableAdapter(EntityAdapter):
    __regid__ = "IJsonFormEditable"
    __select__ = is_instance("Any")
    fetch_possible_targets = set()

    def ui_schema(self):
        return {}

    def defs_config(self, rtype, role, rdef):
        return {
            "title": display_name(self._cw, rtype, role, context=self.entity.cw_etype),
            "rtype": rtype,
            "multiple": rdef.cardinality[0] in "*+",
            "fetchPossibleTargets": rtype in self.fetch_possible_targets,
        }

    def related(self):
        """send a dictionnary with formData, schema, uiSchema"""
        entity = self.entity
        defs = {}
        rsection = self._cw.vreg["uicfg"].select("jsonschema", self._cw, entity=self.entity)
        for rtype, role, targets in rsection.relations_by_section(entity, "related", "add"):
            if role != "subject":
                continue
            rschema = self._cw.vreg.schema[rtype]
            if len(targets) > 1:
                self.warning("unable to handle multiple target type %s, %s", entity.cw_etype, rtype)
                continue
            rdef = rschema.rdef(entity.cw_etype, list(targets)[0])
            if rtype == "custom_workflow":
                continue
            defs[rtype] = self.defs_config(rtype, role, rdef)
        return defs

    def get_ancestors(self):
        entity = self.entity
        result = []
        if not hasattr(entity, "reverse_children"):
            return result
        parent = entity.reverse_children
        while parent:
            result.append(parent[0].eid)
            parent = parent[0].reverse_children
        result.reverse()
        return result


class CircularJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = JsonFormEditableAdapter.__select__ & is_instance("Circular")
    fetch_possible_targets = {"document_type", "business_field", "historical_context", "action"}

    def ui_schema(self):
        defs = super(CircularJsonFormEditableAdapter, self).ui_schema()
        for attr_date in (
            "signing_date",
            "siaf_daf_signing_date",
            "circular_modification_date",
            "abrogation_date",
        ):
            defs.update(
                {
                    attr_date: {
                        "ui:widget": "dateEditor",
                    },
                }
            )
        return defs


class OAIRepositoryJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = JsonFormEditableAdapter.__select__ & is_instance("OAIRepository")

    def ui_schema(self):
        defs = super(OAIRepositoryJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "should_normalize": {"ui:widget": "radio"},
                "context_service": {"ui:widget": "radio"},
            }
        )
        return defs


class CmsObjectJsonFormEditableAdapter(JsonFormEditableAdapter):
    __abstract__ = True

    def ui_schema(self):
        return {
            "content": {
                "ui:widget": "wysiwygEditor",
            }
        }


class RelatedTranslationMixin(object):
    rtype = "translation_of"

    def translations_edit_path(self):
        translations = {"en": "", "de": "", "es": ""}
        translations.update(
            dict(
                [
                    (lang, "{}/{}".format(etype.lower(), trad))
                    for lang, trad, etype in self._cw.execute(
                        """Any L, T, E WHERE T translation_of X,
                  T language L, T is ET, ET name E, X eid %(e)s""",
                        {"e": self.entity.eid},
                    )
                ]
            )
        )
        return translations

    def related(self):
        defs = super(RelatedTranslationMixin, self).related()
        eschema = self._cw.vreg.schema[self.entity.cw_etype]
        _ = self._cw._
        defs[self.rtype] = {
            "fetchPossibleTargets": False,
            "multiple": True,
            "etype": eschema.objrels[self.rtype].targets()[0],
            "rtype": self.rtype,
            "languages": {"en": _("english"), "de": _("german"), "es": _("spanish")},
            "pathes": self.translations_edit_path(),
        }
        return defs


class SectionTranslationJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("SectionTranslation")

    def ui_schema(self):
        defs = super(SectionTranslationJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "content": {
                    "ui:widget": "wysiwygEditor",
                },
                "language": {"ui:disabled": {}},
                "ui:order": [
                    "title",
                    "subtitle",
                    "content",
                    "short_description",
                    "language",
                ],
            }
        )
        return defs


class TopSectionJsonFormEditableAdapter(RelatedTranslationMixin, CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("Section") & score_entity(lambda x: x.cssimage)

    def related(self):
        defs = super(TopSectionJsonFormEditableAdapter, self).related()
        # add computed relation css_image on top sections
        rtype = "cssimage"
        defs[rtype] = self.defs_config(rtype, "subject", self.entity.e_schema.rdef(rtype))
        return defs


class OtherSectionJsonFormEditableAdapter(
    RelatedTranslationMixin, CmsObjectJsonFormEditableAdapter
):
    __select__ = is_instance("Section") & ~score_entity(lambda x: x.cssimage)


class NewsContentJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("NewsContent")

    def ui_schema(self):
        defs = super(NewsContentJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "start_date": {
                    "ui:widget": "dateEditor",
                },
                "stop_date": {
                    "ui:widget": "dateEditor",
                },
            }
        )
        return defs


class RelatedAuthorityMixin(object):
    def related(self):
        defs = super(RelatedAuthorityMixin, self).related()
        _ = self._cw._
        rtype = "related_authority"
        defs[rtype] = {
            "fetchPossibleTargets": False,
            "multiple": True,
            "rtype": rtype,
            "title": _(rtype),
            "titles": [_("index_location"), _("index_agent"), _("index_subject")],
            "etargets": ["LocationAuthority", "AgentAuthority", "SubjectAuthority"],
        }
        return defs


class BaseContentJsonFormEditableAdapter(
    RelatedAuthorityMixin, RelatedTranslationMixin, CmsObjectJsonFormEditableAdapter
):
    __select__ = is_instance("BaseContent")

    def ui_schema(self):
        defs = super(BaseContentJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "basecontent_service": {
                    "ui:field": "autocompleteField",
                },
                "summary": {"ui:widget": "wysiwygEditor"},
            }
        )
        defs.update(
            {
                "ui:order": [
                    "title",
                    "content",
                    "summary",
                    "summary_policy",
                    "on_homepage",
                    "order",
                    "basecontent_service",
                ]
            }
        )
        return defs


class BaseContentTranslationJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("BaseContentTranslation")

    def ui_schema(self):
        defs = super(BaseContentTranslationJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "summary": {
                    "ui:widget": "wysiwygEditor",
                },
                "language": {"ui:disabled": {}},
                "ui:order": [
                    "title",
                    "content",
                    "summary",
                    "language",
                ],
            }
        )
        return defs


class CommemorationItemJsonFormEditableAdapter(
    RelatedTranslationMixin, RelatedAuthorityMixin, CmsObjectJsonFormEditableAdapter
):
    __select__ = is_instance(
        "CommemorationItem",
    )


class CommemorationItemTranslationJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("CommemorationItemTranslation")

    def ui_schema(self):
        defs = super(CommemorationItemTranslationJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "content": {
                    "ui:widget": "wysiwygEditor",
                },
                "language": {"ui:disabled": {}},
                "ui:order": [
                    "title",
                    "subtitle",
                    "content",
                    "language",
                ],
            }
        )
        return defs


class CommemoDateJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("CommemoDate")

    def ui_schema(self):
        defs = super(CommemoDateJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "date": {
                    "ui:widget": "dateEditor",
                },
            }
        )
        return defs

    def get_ancestors(self):
        return []


class CardJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("Card")

    def ui_schema(self):
        return {
            "content": {
                "ui:widget": "wysiwygEditor",
            },
        }

    def get_ancestors(self):
        return []


class ImageJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("Image", "CssImage")

    def ui_schema(self):
        defs = {
            "caption": {
                "ui:widget": "wysiwygEditor",
            },
            "description": {
                "ui:widget": "wysiwygEditor",
            },
            "image_file": {
                "items": {
                    "data": {
                        "ui:widget": "imageEditor",
                    },
                },
                "ui:options": {
                    "removable": False,
                    "addable": False,
                },
            },
        }
        return defs

    def get_ancestors(self):
        return []


class FingingAidJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("FindingAid")

    def ui_schema(self):
        return {}

    def get_ancestors(self):
        return []


class ExternRefJsonFormEditableAdapter(RelatedAuthorityMixin, CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("ExternRef")

    def ui_schema(self):
        defs = super(ExternRefJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "exref_service": {
                    "ui:field": "autocompleteField",
                },
            }
        )
        return defs


class MapJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = JsonFormEditableAdapter.__select__ & is_instance("Map")

    def ui_schema(self):
        return {
            "top_content": {
                "ui:widget": "wysiwygEditor",
            },
            "bottom_content": {
                "ui:widget": "wysiwygEditor",
            },
        }


class ServiceJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = JsonFormEditableAdapter.__select__ & is_instance("Service")

    def ui_schema(self):
        return {
            "other": {
                "ui:widget": "wysiwygEditor",
            },
        }


class LocationAuthorityJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("LocationAuthority")

    def ui_schema(self):
        defs = super(LocationAuthorityJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "longitude": {
                    "ui:readonly": "true",
                },
                "latitude": {
                    "ui:readonly": "true",
                },
            }
        )
        return defs


class CWUserJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("CWUser")

    def get_ancestors(self):
        return []


class GlossaryTermJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("GlossaryTerm")

    def ui_schema(self):
        defs = super(GlossaryTermJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "short_description": {"ui:widget": "wysiwygEditor"},
                "description": {"ui:widget": "wysiwygEditor"},
            }
        )
        return defs


class RqTaskJsonFormEditableAdapter(JsonFormEditableAdapter):
    __select__ = is_instance("RqTask")

    def get_ancestors(self):
        return []


class ImportEadRqTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "import_ead"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
            "should_normalize": {"ui:widget": "radio"},
            "context_service": {"ui:widget": "radio"},
        }


class ImportEacRqTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "import_eac"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
        }


class ImportCSVTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "import_csv"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
            "metadata": {
                "ui:widget": "filepicker",
            },
            "should_normalize": {"ui:widget": "radio"},
            "context_service": {"ui:widget": "radio"},
        }


class ImportOaiTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "import_oai"}
    )

    def ui_schema(self):
        return {
            "should_normalize": {"ui:widget": "radio"},
            "context_service": {"ui:widget": "radio"},
        }


class ImportAuthoritiesRqTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "import_authorities"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
        }


class GroupLocAuthoritiesAlignmentRqTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "group_location_authorities"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
        }


class DeleteFindingAidsRqTaskJsonFormEditableAdapter(RqTaskJsonFormEditableAdapter):
    __select__ = RqTaskJsonFormEditableAdapter.__select__ & match_kwargs(
        {"schema_type": "delete_finding_aids"}
    )

    def ui_schema(self):
        return {
            "file": {
                "ui:widget": "filepicker",
            },
        }


class FaqItemJsonFormEditableAdapter(RelatedTranslationMixin, CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("FaqItem")

    def ui_schema(self):
        defs = super(FaqItemJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "question": {"ui:widget": "wysiwygEditor"},
                "answer": {"ui:widget": "wysiwygEditor"},
            }
        )
        return defs


class FaqItemTranslationJsonFormEditableAdapter(CmsObjectJsonFormEditableAdapter):
    __select__ = is_instance("FaqItemTranslation")

    def ui_schema(self):
        defs = super(FaqItemTranslationJsonFormEditableAdapter, self).ui_schema()
        defs.update(
            {
                "question": {"ui:widget": "wysiwygEditor"},
                "answer": {"ui:widget": "wysiwygEditor"},
                "language": {"ui:disabled": {}},
                "ui:order": [
                    "question",
                    "anwser",
                    "language",
                ],
            }
        )
        return defs

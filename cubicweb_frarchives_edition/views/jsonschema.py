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
"""cubicweb-frarchives_edition JSON schema configuration (incl. uicfg)."""

from cubicweb_francearchives.schema.cms import CMS_OBJECTS
from cubicweb_francearchives import CMS_I18N_OBJECTS
from cubicweb_jsonschema.views import jsonschema_section


jsonschema_section.tag_object_of(("*", "imported_findingaid", "FindingAid"), "hidden")
jsonschema_section.tag_subject_of(("FindingAid", "findingaid_support", "*"), "inlined")

for rtype in ("fa_header", "did", "service", "findingaid_support"):
    jsonschema_section.tag_subject_of(("FindingAid", rtype, "*"), "hidden")

jsonschema_section.tag_object_of(("*", "import_file", "File"), "hidden")
jsonschema_section.tag_object_of(("*", "image_file", "File"), "hidden")
for rel in (
    "findingaid_support",
    "alignment_result",
    "ape_ead_file",
    "additional_attachment",
    "attachment",
):
    jsonschema_section.tag_object_of(("*", rel, "File"), "hidden")

for rel in (
    "basecontent_image",
    "commemoration_image",
    "map_image",
    "section_image",
    "service_image",
    "news_image",
    "externref_image",
):
    jsonschema_section.tag_object_of(("*", rel, "Image"), "hidden")

jsonschema_section.tag_subject_of(("*", "cssimage_of", "CssImage"), "hidden")
jsonschema_section.tag_subject_of(("*", "cssimage", "CssImage"), "inlined")
jsonschema_section.tag_subject_of(("CssImage", "cssid", "*"), "hidden")

jsonschema_section.tag_subject_of(("*", "referenced_files", "*"), "hidden")
jsonschema_section.tag_object_of(("*", "referenced_files", "*"), "hidden")
jsonschema_section.tag_subject_of(("*", "fa_referenced_files", "*"), "hidden")
jsonschema_section.tag_object_of(("*", "fa_referenced_files", "*"), "hidden")
jsonschema_section.tag_subject_of(("*", "output_file", "*"), "hidden")
jsonschema_section.tag_object_of(("*", "output_file", "*"), "hidden")

jsonschema_section.tag_subject_of(("Service", "service_social_network", "*"), "inlined")

for subj in (
    CMS_OBJECTS
    + CMS_I18N_OBJECTS
    + (
        "Metadata",
        "Link",
        "File",
        "CommemoDate",
        "Image",
        "CssImage",
        "Category",
        "Circular",
        "OfficialText",
        "Service",
        "SocialNetwork",
        "Map",
    )
):
    jsonschema_section.tag_subject_of((subj, "uuid", "*"), "hidden")

for subj in CMS_I18N_OBJECTS:
    jsonschema_section.tag_subject_of((subj, "translation_of", "*"), "hidden")

jsonschema_section.tag_subject_of(("BaseContent", "description", "*"), "hidden")
jsonschema_section.tag_subject_of(("BaseContent", "keywords", "*"), "hidden")
jsonschema_section.tag_subject_of(("BaseContent", "basecontent_service", "*"), "inlined")
jsonschema_section.tag_object_of(("*", "children", "*"), "hidden")

jsonschema_section.tag_subject_of(("CommemorationItem", "collection_top", "*"), "hidden")

jsonschema_section.tag_subject_of(("Metadata", "description", "*"), "inlined")
jsonschema_section.tag_subject_of(("CWUser", "last_login_time", "*"), "hidden")
jsonschema_section.tag_subject_of(("CWUser", "use_email", "*"), "hidden")

for attr in ("status", "log", "enqueued_at", "started_at", "ended_at"):
    jsonschema_section.tag_subject_of(("RqTask", attr, "*"), "hidden")

jsonschema_section.tag_subject_of(("ExternRef", "exref_service", "*"), "inlined")

jsonschema_section.tag_subject_of(("*", "previous_info", "PreviousInfo"), "hidden")
jsonschema_section.tag_object_of(("*", "metadata", "Metadata"), "hidden")

jsonschema_section.tag_subject_of(("Card", "wikiid", "*"), "hidden")

jsonschema_section.tag_subject_of(("RqTask", "output_descr", "*"), "hidden")
jsonschema_section.tag_subject_of(("Circular", "json_values", "*"), "hidden")
jsonschema_section.tag_subject_of(("AuthorityRecord", "maintainer", "Service"), "hidden")

for attr in ("birthyear", "deathyear"):
    jsonschema_section.tag_subject_of(("AgentAuthority", attr, "*"), "hidden")

for attr in ("sort_letter", "anchor"):
    jsonschema_section.tag_subject_of(("GlossaryTerm", attr, "*"), "hidden")

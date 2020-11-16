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
from cubicweb.predicates import is_instance, objectify_predicate, has_add_permission

from cubicweb_jsonschema.views.link import EntityLink


@objectify_predicate
def in_commemocollection(cls, req, rset=None, entity=None, **kwargs):
    """Return 1 if the entity type is a CommemoCollection or
    a section inside a CommemoCollection
    """
    # import ipdb; ipdb.set_trace()
    if rset and len(rset) == 1:
        entity = rset.one()
    if entity is None:
        return 0
    if entity.cw_etype == "CommemoCollection" or (
        entity.cw_etype == "Section" and entity.is_commemo_section()
    ):
        return 1
    return 0


class has_add_target_permissions(has_add_permission):
    """checks the user has 'add' permission on cls.target_etype

    The two main changes with respect to ``has_add_permission`` are:

    - usage of ``cls.target_etype`` to indicate the entity type to
      check the 'add' permission on,

    - allow 'subjobjects' (e.g. composite) entity types to be selectable
    """

    def __call__(self, cls, req, **kwargs):
        eschema = req.vreg.schema.eschema(cls.target_type)
        if eschema.final or not eschema.has_perm(req, "add"):
            return 0
        return 1


class RelateChildrenLink(EntityLink):
    __abstract__ = True
    __regid__ = "entity.relate.children"
    __select__ = EntityLink.__select__ & has_add_target_permissions()
    target_type = None
    order = 10
    rtype = "children"

    def description_object(self, request, resource):
        _ = self._cw._
        return {
            "description": _(self.target_type),
            "etype": self.target_type,
            "method": "POST",
            "href": request.resource_path(
                resource, "relationships", self.rtype, query={"target_type": self.target_type}
            ),
            "targetSchema": {
                "$ref": request.route_path(
                    "cubicweb-jsonschema",
                    traverse=(resource.__parent__.etype, "relationships", self.rtype, "schema"),
                    _query={"target_type": self.target_type, "role": "creation"},
                ),
            },
            "rel": "related.{}".format(self.rtype),
            "title": "New Child",
            "order": self.order,
        }


class RelateChildrenNewsContentLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.newscontent"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "NewsContent"
    order = 1


class RelateChildrenSectionLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.section"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section", "CommemoCollection")
    target_type = "Section"
    order = 20


class RelateSectionTranslationLink(RelateChildrenLink):
    __regid__ = "entity.relate.translation.sectiontranslation"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    rtype = "translation_of"
    target_type = "SectionTranslation"


class RelateBaseContentTranslationLink(RelateChildrenLink):
    __regid__ = "entity.relate.translation.basecontent"
    __select__ = RelateChildrenLink.__select__ & is_instance("BaseContent")
    rtype = "translation_of"
    target_type = "BaseContentTranslation"


class RelateChildrenCommemorationitemLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.commemorationitem"
    __select__ = RelateChildrenLink.__select__ & in_commemocollection()
    target_type = "CommemorationItem"


class RelateChildrenCommemoCollectionLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.commemocollection"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "CommemoCollection"


class RelateCommemorationitemTranslationLink(RelateChildrenLink):
    __regid__ = "entity.relate.translation.commemorationitem"
    __select__ = RelateChildrenLink.__select__ & is_instance("CommemorationItem")
    rtype = "translation_of"
    target_type = "CommemorationItemTranslation"


class RelateChildrenCircularLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.circular"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "Circular"


class RelateChildrenBaseContentLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.basecontent"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "BaseContent"
    order = 0


class RelateChildrenExternRefLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.externref"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "ExternRef"


class RelateChildrenMapContentLink(RelateChildrenLink):
    __regid__ = "entity.relate.children.map"
    __select__ = RelateChildrenLink.__select__ & is_instance("Section")
    target_type = "Map"

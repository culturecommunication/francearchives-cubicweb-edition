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

from cubicweb_frarchives_edition.hooks import files_query_from_content
from rql import RQLSyntaxError


def update_editorials(cnx):
    """
    update content field of editoral etypes
    """
    etypes = (
        "Card",
        "NewsContent",
        "Map",
        "Section",
        "CommemorationItem",
        "CommemoCollection",
        "ExternRef",
        "BaseContent",
        "Map",
    )
    query = "Any X WHERE X is IN (%(etypes)s)"
    for entity in cnx.execute(query % {"etypes": ", ".join(etypes)}).entities():
        already_linked = {e.eid for e in entity.referenced_files}
        uischema = entity.cw_adapt_to("IJsonFormEditable").ui_schema()
        files = set()
        for attr, descr in uischema.items():
            if descr.get("ui:widget") == "wysiwygEditor":
                value = getattr(entity, attr)
                if not value or not value.strip():
                    continue
                queries = files_query_from_content(value)
                if not queries:
                    continue
                query = " UNION ".join("(%s)" % q for q in queries)
                try:
                    files |= {eid for eid, in cnx.execute(query)}
                except RQLSyntaxError:
                    print('fail to execute query "%r"', query)
        if not already_linked and not files:
            continue
        to_remove = already_linked - files
        if to_remove:
            print("TO REMOVE", entity, to_remove)
            cnx.execute(
                "DELETE X referenced_files Y WHERE X eid %(e)s, Y eid IN ({})".format(
                    ",".join(str(e) for e in to_remove)
                ),
                {"e": entity.eid},
            )
        to_add = files - already_linked
        if to_add:
            print("TO ADD", entity, to_add)
            cnx.execute(
                "SET X referenced_files Y WHERE X eid %(e)s, Y eid IN ({})".format(
                    ",".join(str(e) for e in to_add)
                ),
                {"e": entity.eid},
            )


update_editorials(cnx)  # noqa

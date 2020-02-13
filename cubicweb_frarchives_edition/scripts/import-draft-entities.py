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
import json

from cubicweb.utils import json_dumps
from cubicweb.predicates import is_instance, relation_possible, yes

from cubicweb_francearchives.pviews.edit import get_uuid_attr, edit_object
from cubicweb_francearchives.entities import sync
from cubicweb_francearchives import init_bfss


# remove `is_in_state('wfs_cmsobject_published')` part from ISync selectors
# so draft entities can be ISync selectable
sync.ISyncAdapter.__select__ = yes()
sync.ISyncUuidAttrAdapter.__select__ = is_instance("Concept")
sync.ISyncUuidAdapter.__select__ = relation_possible("uuid")
sync.ISyncCarddAdapter.__select__ = is_instance("Card")
sync.ISyncSectionAdapter.__select__ = sync.ISyncUuidAdapter.__select__ & is_instance(
    "Section", "CommemoCollection"
)
sync.ISyncCommemorationItemAdapter.__select__ = sync.ISyncUuidAdapter.__select__ & is_instance(
    "CommemorationItem"
)


def dump(cnx, result_file):
    rset = cnx.execute('Any X WHERE X in_state S, S name "wfs_cmsobject_draft", NOT X is Card')
    with open(result_file, "w") as fout:
        for e in rset.entities():
            s = e.cw_adapt_to("ISync")
            body = s.build_put_body()
            fout.write("%s:%s\n" % (e.cw_etype, s.uuid_value))
            fout.write(json_dumps(body))
            fout.write("\n\n")


def load(cnx, resultfile):
    init_bfss(cnx.repo)  # in case we need to save some files
    with open(resultfile) as fin:
        lines_iter = iter(fin)
        for line in lines_iter:
            line = line.strip()
            if not line:
                continue
            etype, uuid_value = line.split(":")
            etype, uuid_value = unicode(etype), unicode(uuid_value)
            print("handling {}:{}".format(etype, uuid_value))
            data = json.loads(next(lines_iter).decode("utf-8"))
            uuid_attr = get_uuid_attr(cnx.vreg, etype)
            data[uuid_attr] = uuid_value
            section_uuid = data.pop("parent-section", None)
            entity, created = edit_object(cnx, etype, data)
            if created and section_uuid:
                section = cnx.find("Section", uuid=section_uuid).one()
                section.cw_set(children=entity)
            cnx.commit()
            print("\tdone (created: %s)" % created)


if __name__ == "__main__" and "cnx" in globals():
    cmd, filepath = __args__
    if cmd == "load":
        load(cnx, filepath)
    elif cmd == "dump":
        dump(cnx, filepath)
    else:
        print("first argument should be either `load` or `dump`")

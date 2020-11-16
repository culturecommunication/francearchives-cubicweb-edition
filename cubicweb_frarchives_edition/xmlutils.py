# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
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

"""cubicweb-frarchives-edition xml utils"""

import hashlib
from lxml.builder import E
from lxml import html as lxml_html

from cubicweb_francearchives.utils import remove_html_tags
from cubicweb_francearchives.xmlutils import log as log_warning


class InvalidHTMLError(Exception):
    """HTML could not be parsed"""

    def __init__(self, errors):
        self.errors = errors


def xml_content(element):
    if element is None:
        return None
    content = lxml_html.tostring(element, encoding="unicode", with_tail=False)
    left, right = content.index(">") + 1, content.rindex("<")
    return remove_html_tags(content[left:right].strip())


def generate_summary(content, last_heading_level, skip_empty=True, root_level=2, as_string=True):
    """Generate a TOC from HTML content headings

    :param str content: HTML
    :param int last_heading_level: heading level TOC is generated up to
    :param bool skip_empty: skip empty heading in TOC
    :param int root_level: heading level the TOC is started with
    :param bool as_string: return result as string or XML elements

    :return: return summary, modified content or None is content was not modified
    """
    try:
        fragments = lxml_html.fragments_fromstring(content)
    except Exception as err:
        log_warning(err)
        raise InvalidHTMLError(err)
    headings = range(1, last_heading_level + 1)
    heading_nodes = fragments[0].xpath("|".join("//h{}".format(h) for h in headings))
    if not heading_nodes:
        if as_string:
            return ("", None)
        return None, None
    is_content_modified = False
    # summary related variables
    current_parent = E.ul({"class": "toc"})
    previous_node = None
    parent_lists = [(root_level, current_parent)]
    for i, node in enumerate(heading_nodes):
        # process headings
        node_id = node.attrib.get("id")
        if not node_id:
            value = lxml_html.tostring(node, with_tail=False)
            node_id = "{level}_{value}{i}".format(
                level=node.tag, value=hashlib.sha1(value).hexdigest(), i=i
            )
            node.set("id", node_id)
            is_content_modified = True
        # generate summary menu
        text = xml_content(node).strip()
        if not (text) and skip_empty:
            # do not generate summary for a heading without text
            continue
        link = E.li(
            E.a(text, href="#{}".format(node_id)),
        )
        current_level = int(node.tag[1])
        previous_level = int(previous_node.tag[1]) if previous_node is not None else root_level
        if current_level > previous_level:  # get down
            for j in range(1, (current_level - previous_level)):
                # add missing levels is case of invalid headings hierarchy
                new_parent_list = E.ul()
                if not current_parent.xpath("./li[last()]"):
                    current_parent.append(E.li())
                current_parent.xpath("./li[last()]")[0].append(new_parent_list)
                current_parent = new_parent_list
            if not current_parent.xpath("./li[last()]"):
                current_parent.append(E.li())
            new_parent_list = E.ul(link)
            current_parent.xpath("./li[last()]")[0].append(new_parent_list)
            current_parent = new_parent_list
            parent_lists.insert(0, [current_level, new_parent_list])
        elif current_level < previous_level:  # get up
            current_parent = [parent for level, parent in parent_lists if level == current_level]
            # if there is no parent for the current level (invalid headings
            # hierarchy), attach it to the root to avoid errors. Normaly is must not happen
            # as it is managed the previous case
            current_parent = current_parent[0] if current_parent else parent_lists[-1][1]
            current_parent.append(link)
        else:
            current_parent.append(link)
        previous_node = node
    summary = parent_lists[-1][1]
    if as_string:
        summary = lxml_html.tostring(summary).decode("utf-8").strip()
        if is_content_modified:
            fragments = "".join(lxml_html.tostring(f).decode("utf-8") for f in fragments).strip()
        else:
            fragments = None
    return summary, fragments

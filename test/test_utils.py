# -*- coding: utf-8 -*-
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


# standard library imports
from lxml import html as lxml_html

import os.path as osp

import unittest

from logilab.common import flatten

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_francearchives.testutils import XMLCompMixin

from cubicweb_frarchives_edition.xmlutils import generate_summary

from cubicweb_frarchives_edition import FILE_URL_RE


class UtilsTest(CubicWebTC):
    def test_file_url_regexpr(self):
        for url in (
            "http://test/file/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",  # noqa
            "file/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
            "/file/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
            "../file/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
            "../../file/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
        ):
            match = FILE_URL_RE.search(url)
            groups = match.groupdict()
            self.assertEqual(groups["hash"], "07957e78e792c2a1b46c212473ae03615b9406b6")
            self.assertEqual(groups["name"], "apprentissage_AD_H_Pyrenees.pdf")

        for url in (
            "http://test/totofile/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",  # noqa
            "totofile/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
            "/totofile/07957e78e792c2a1b46c212473ae03615b9406b6/apprentissage_AD_H_Pyrenees.pdf",
        ):
            self.assertIsNone(FILE_URL_RE.search(url))


class XMLUtilsTest(XMLCompMixin, CubicWebTC):
    def assertHTMLEqual(self, expected_filepath, result):
        got = lxml_html.fragments_fromstring(result)[0]
        with open(self.datapath(osp.join("xml"), expected_filepath), "rb") as f:
            expected = lxml_html.fragments_fromstring(f.read().strip())[0]
        self.assertXMLEqual(expected, got)

    def test_generate_summary(self):
        """
        Triyng: generate a toc from complexcontent.html
        Expecting: the generated toc is the expected one ("complexcontent_summary.html"),
                   headings ids are generated in the content modified by `generate_summary`
        """
        contentpath = self.datapath(osp.join("xml"), "complexcontent.html")
        with open(contentpath, "rb") as f:
            content = f.read()
            summary, content = generate_summary(content, 6)
            self.assertHTMLEqual("complexcontent_summary.html", summary)
            # the resulting content must be similar to the orignal except the
            # new ids on headings, which is not detected by assertXMLEqual
            self.assertHTMLEqual("complexcontent.html", content)
            summary, content = generate_summary(content, 6, as_string=False)
            expected_ids = {
                "h2": [
                    "h2_c418cd8851e2fbca3d47f752ef6d3235f75319a80",
                    "h2_4c428fa84cc7531b96df20d589900cbfd4f21f8b1",
                    "h2_84395488858357b07c68ec4bebf4351258d234e73",
                    "h2_7c017114f45b8257ec1280e737cb5d3f124888fc4",
                    "h2_62252209336a868e9fb4478e558276c3aefde9b85",
                    "h2_4f96c7bfe12fa13c5d40787a0e083a7f197ee0b76",
                ],
                "h3": [
                    "h3_f0b5f70ca20c26025bfd3282b4ae6ec9c43726bb7",
                    "h3_6a13a51a6ef7a304985f6013b01792da8e0f38f48",
                    "h3_d90e341f6f607673e263a0a0c7cbc8614c3ed3ff9",
                    "h3_6a13a51a6ef7a304985f6013b01792da8e0f38f410",
                    "h3_2fd75ed29260c6eacc6f6760d96b50d03ddba0a611",
                ],
                "h4": ["h4_69f6cb11e9f08857b5e1a20146e61b698059cbd62"],
            }
            # check ids are set in content
            for node in content[0].xpath("//h1|//h2|//h3|//h4"):
                self.assertIn(node.attrib["id"], expected_ids[node.tag])
            for node in summary[0].xpath("//a"):
                self.assertIn(
                    node.attrib["href"][1:],
                    flatten([expected_ids.get(k) for k in expected_ids.keys()]),
                )


if __name__ == "__main__":
    unittest.main()

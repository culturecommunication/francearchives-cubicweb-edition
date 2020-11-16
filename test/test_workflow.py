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
"""cubicweb-frarchives_edition unit tests for workflow hooks"""

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_frarchives_edition import ForbiddenPublishedTransition


from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa


class TranslationHookTests(FrACubicConfigMixIn, CubicWebTC):
    def test_publish_basecontent_translation_ok(self):
        """
        Trying: create a BaseContent, publish it, add and publish a Translation
        Expecting: the Translation is published
        """
        with self.admin_access.repo_cnx() as cnx:
            basecontent = cnx.create_entity("BaseContent", title="program", content="31 juin")
            cnx.commit()
            basecontent.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            basecontent = cnx.find("BaseContent", eid=basecontent.eid).one()
            self.assertEqual(
                "wfs_cmsobject_published", basecontent.cw_adapt_to("IWorkflowable").state
            )
            translation = cnx.create_entity(
                "BaseContentTranslation",
                language="en",
                title="program",
                content="<h1>31 june</h1>",
                translation_of=basecontent,
            )
            cnx.commit()
            translation.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()

    def test_publish_basecontent_translation_ko(self):
        """
        Trying: create a BaseContent, add and try to publish a Translation
        Expecting: the Translation is not published
        """
        with self.admin_access.repo_cnx() as cnx:
            basecontent = cnx.create_entity("BaseContent", title="program", content="31 juin")
            cnx.commit()
            translation = cnx.create_entity(
                "BaseContentTranslation",
                language="en",
                title="program",
                content="<h1>31 june</h1>",
                translation_of=basecontent,
            )
            cnx.commit()
            with self.assertRaises(ForbiddenPublishedTransition):
                translation.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
                cnx.commit()

    def test_unpublish_section(self):
        """
        Trying: create a Section, publish it, add some Translations and unpublish the Section
        Expecting: all Translations are unpublished
        """
        with self.admin_access.repo_cnx() as cnx:
            section = cnx.create_entity("Section", title="titre", content="contenu")
            cnx.commit()
            section.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            de = cnx.create_entity(
                "SectionTranslation",
                language="de",
                title="Titel",
                translation_of=section,
            )
            en = cnx.create_entity(
                "SectionTranslation",
                language="en",
                title="title",
                translation_of=section,
            )
            cnx.commit()
            de.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            de.cw_clear_all_caches()
            en.cw_clear_all_caches()
            section.cw_clear_all_caches()
            self.assertEqual("wfs_cmsobject_draft", en.cw_adapt_to("IWorkflowable").state)
            self.assertEqual("wfs_cmsobject_published", de.cw_adapt_to("IWorkflowable").state)
            self.assertEqual("wfs_cmsobject_published", section.cw_adapt_to("IWorkflowable").state)
            section.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            for translation in section.reverse_translation_of:
                translation.cw_clear_all_caches()
                self.assertEqual(
                    "wfs_cmsobject_draft", translation.cw_adapt_to("IWorkflowable").state
                )

    def test_unpublish_commemorationitem(self):
        """
        Trying: create a CommemorationItem, publish it,
        add some Translations and unpublish the SCommemorationItem
        Expecting: all Translations are unpublished
        """
        with self.admin_access.repo_cnx() as cnx:
            commemo = cnx.create_entity(
                "CommemorationItem",
                title="titre",
                content="contenu",
                alphatitle="titre",
                commemoration_year=1500,
                collection_top=cnx.create_entity("CommemoCollection", title="Moyen Age", year=1500),
            )
            cnx.commit()
            commemo.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            de = cnx.create_entity(
                "CommemorationItemTranslation",
                language="de",
                title="Titel",
                translation_of=commemo,
            )
            en = cnx.create_entity(
                "CommemorationItemTranslation",
                language="en",
                title="title",
                translation_of=commemo,
            )
            cnx.commit()
            de.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            de.cw_clear_all_caches()
            en.cw_clear_all_caches()
            commemo.cw_clear_all_caches()
            self.assertEqual("wfs_cmsobject_draft", en.cw_adapt_to("IWorkflowable").state)
            self.assertEqual("wfs_cmsobject_published", de.cw_adapt_to("IWorkflowable").state)
            self.assertEqual("wfs_cmsobject_published", commemo.cw_adapt_to("IWorkflowable").state)
            commemo.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_unpublish")
            cnx.commit()
            for translation in commemo.reverse_translation_of:
                translation.cw_clear_all_caches()
                self.assertEqual(
                    "wfs_cmsobject_draft", translation.cw_adapt_to("IWorkflowable").state
                )


if __name__ == "__main__":
    import unittest

    unittest.main()

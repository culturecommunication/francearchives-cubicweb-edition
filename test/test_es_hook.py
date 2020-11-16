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
"""cubicweb-frarchives_edition unit tests for hooks envolving es"""
import json
import mock

from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_francearchives.testutils import EsSerializableMixIn
from utils import FrACubicConfigMixIn

from esfixtures import teardown_module  # noqa


class CircularHookESTC(FrACubicConfigMixIn, EsSerializableMixIn, CubicWebTC):
    def setup_database(self):
        super().setup_database()
        with self.admin_access.cnx() as cnx:
            subject_authority = cnx.create_entity("SubjectAuthority", label="foobar")
            cnx.create_entity("Subject", label="foo", authority=subject_authority)
            cnx.create_entity(
                "Circular",
                circ_id="foo",
                status="in-effect",
                title="foo",
            )
            scheme = cnx.create_entity("ConceptScheme", title="foobar")
            concept = cnx.create_entity("Concept", in_scheme=scheme, same_as=subject_authority)
            cnx.create_entity(
                "Label",
                language_code="fr",
                kind="preferred",
                label="foobar",
                label_of=concept,
            )
            cnx.commit()

    @mock.patch("elasticsearch.client.Elasticsearch.bulk", unsafe=True)
    @mock.patch("elasticsearch.helpers.reindex", unsafe=True)
    @mock.patch("elasticsearch.client.indices.IndicesClient.create", unsafe=True)
    @mock.patch("elasticsearch.client.indices.IndicesClient.exists", unsafe=True)
    @mock.patch("elasticsearch.client.Elasticsearch.index", unsafe=True)
    def test_modify_circular(self, index, exists, create, reindex, bulk):
        """Test modifiying Circular's business_field relation.

        Trying: add/remove business_field relation
        Expecting: ElasticSearch auto-complete index is updated
        """
        with self.admin_access.cnx() as cnx:
            circular = cnx.execute("Any X WHERE X is Circular").one()
            wf = circular.cw_adapt_to("IWorkflowable")
            wf.fire_transition("wft_cmsobject_publish")
            concept = cnx.execute("Any X WHERE X is Concept").one()
            cnx.execute(
                "SET C business_field X WHERE C eid %(circular)s, X eid %(concept)s",
                {"circular": circular.eid, "concept": concept.eid},
            )
            self.assertFalse(bulk.called)
            cnx.commit()
            self.assertTrue(bulk.called)
            args, kwargs = bulk.call_args
            args = json.loads(args[0].split()[1])
            bulk.reset_mock()
            cnx.execute(
                "DELETE C business_field X WHERE C eid %(circular)s", {"circular": circular.eid}
            )
            cnx.commit()
            self.assertTrue(bulk.called)
            args, kwargs = bulk.call_args
            args = json.loads(args[0].split()[1])
            self.assertEqual(args["count"], 0)

    @mock.patch("elasticsearch.client.Elasticsearch.bulk", unsafe=True)
    @mock.patch("elasticsearch.helpers.reindex", unsafe=True)
    @mock.patch("elasticsearch.client.indices.IndicesClient.create", unsafe=True)
    @mock.patch("elasticsearch.client.indices.IndicesClient.exists", unsafe=True)
    @mock.patch("elasticsearch.client.Elasticsearch.index", unsafe=True)
    def test_related_authorities(self, index, exists, create, reindex, bulk):
        """Test modifiying related_authority relation.

        Trying: add/remove related_authority relation on a CSMObject
        Expecting: ElasticSearch auto-complete index is updated
        """
        with self.admin_access.cnx() as cnx:
            subject = cnx.create_entity("SubjectAuthority", label="foobar")
            bc = cnx.create_entity(
                "ExternRef", reftype="Virtual_exhibit", title="title", content="content"
            )
            cnx.commit()
            # publish the ExternRef to have the same count on published/unpublished index
            bc.cw_adapt_to("IWorkflowable").fire_transition("wft_cmsobject_publish")
            cnx.commit()
            bc.cw_set(related_authority=subject)
            self.assertFalse(bulk.called)
            cnx.commit()
            self.assertTrue(bulk.called)
            args, kwargs = bulk.call_args
            args = json.loads(args[0].split()[1])
            self.assertEqual(args["count"], 1)
            bulk.reset_mock()
            bc = cnx.entity_from_eid(bc.eid)
            bc.cw_set(related_authority=None)
            cnx.commit()
            self.assertTrue(bulk.called)
            args, kwargs = bulk.call_args
            args = json.loads(args[0].split()[1])
            self.assertEqual(args["count"], 0)


if __name__ == "__main__":
    import unittest

    unittest.main()

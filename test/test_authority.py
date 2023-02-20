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
import logging

from cubicweb.devtools.testlib import CubicWebTC
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.utils import json_dumps

from cubicweb_francearchives.testutils import EADImportMixin

from cubicweb_frarchives_edition.tasks.dedupe_authorities import dedupe

from cubicweb_francearchives.dataimport import load_services_map, service_infos_from_service_code

from utils import FrACubicConfigMixIn, create_findingaid
from pgfixtures import setup_module, teardown_module  # noqa


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class AutoDedupeTC(FrACubicConfigMixIn, CubicWebTC):
    configcls = PostgresApptestConfiguration

    def _run_and_assert_dedupe(self, auth1, auth2, index1, index2, strict=True):
        with self.admin_access.cnx() as cnx:
            dedupe(cnx, log=LOGGER, strict=strict)
            # auth should have been deleted
            self.assertFalse(cnx.find(auth2.cw_etype, eid=auth2.eid))
            # index2 should have been redirected on auth1
            self.assertEqual(
                {
                    x
                    for x, in cnx.execute(
                        "Any X WHERE X authority A, A eid %(a)s", {"a": auth1.eid}
                    )
                },
                {index1.eid, index2.eid},
            )

    def test_most_same_as_is_better_agent_non_strict(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            ext1 = ce("ExternalUri", uri="http://example.com/auth")
            auth1 = ce("AgentAuthority", label="example Agent")
            auth2 = ce("AgentAuthority", label="example agent")
            index1 = ce("AgentName", label="example Agent", authority=auth1)
            index2 = ce("AgentName", label="example agent", authority=auth2)
            auth1.cw_set(same_as=ext1)
            cnx.commit()
        self._run_and_assert_dedupe(auth1, auth2, index1, index2, strict=False)

    def test_most_same_as_is_better_agent(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            ext1 = ce("ExternalUri", uri="http://example.com/auth")
            auth1 = ce("AgentAuthority", label="example agent")
            auth2 = ce("AgentAuthority", label="example agent")
            index1 = ce("AgentName", label="example agent", authority=auth1)
            index2 = ce("AgentName", label="example agent", authority=auth2)
            auth1.cw_set(same_as=ext1)
            cnx.commit()
        self._run_and_assert_dedupe(auth1, auth2, index1, index2)

    def test_most_same_as_is_better_subject(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            ext1 = ce("ExternalUri", uri="http://example.com/auth")
            auth1 = ce("SubjectAuthority", label="example subject")
            auth2 = ce("SubjectAuthority", label="example subject")
            index1 = ce("Subject", label="example subject", authority=auth1)
            index2 = ce("Subject", label="example subject", authority=auth2)
            auth1.cw_set(same_as=ext1)
            cnx.commit()
        self._run_and_assert_dedupe(auth1, auth2, index1, index2)

    def test_most_same_as_is_better_location(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            ext1 = ce("ExternalUri", uri="http://example.com/auth")
            auth1 = ce("LocationAuthority", label="example location")
            auth2 = ce("LocationAuthority", label="example location")
            index1 = ce("Geogname", label="example location", authority=auth1)
            index2 = ce("Geogname", label="example location", authority=auth2)
            auth1.cw_set(same_as=ext1)
            cnx.commit()
        self._run_and_assert_dedupe(auth1, auth2, index1, index2)

    def _setup_restrict(self):
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            fa1 = create_findingaid(cnx, name="FRAD033_1")
            s1 = ce("Service", code="FRAD033", category="fake")
            fa1.cw_set(service=s1)
            fa2 = create_findingaid(cnx, name="FRAD033_2")
            fa2.cw_set(service=s1)
            fa3 = create_findingaid(cnx, name="FRAD033_3")
            s2 = ce("Service", code="FRAD040", category="fake")
            fa3.cw_set(service=s2)
            ext1 = ce("ExternalUri", uri="http://example.com/auth")
            auth1 = ce("AgentAuthority", label="example agent")
            auth2 = ce("AgentAuthority", label="example agent")
            auth3 = ce("AgentAuthority", label="example agent")
            auth4 = ce("AgentAuthority", label="example agent")
            index1 = ce("AgentName", label="example agent", authority=auth1, index=fa1)
            index2 = ce("AgentName", label="example agent", authority=auth2, index=fa2)
            index3 = ce("AgentName", label="example agent", authority=auth3, index=fa3)
            auth1.cw_set(same_as=ext1)
            ce(
                "EsDocument",
                entity=fa1,
                doc=json_dumps({"index_entries": [{"authority": auth1.eid}]}),
            )
            ce(
                "EsDocument",
                entity=fa2,
                doc=json_dumps({"index_entries": [{"authority": auth2.eid}]}),
            )
            ce(
                "EsDocument",
                entity=fa3,
                doc=json_dumps({"index_entries": [{"authority": auth3.eid}]}),
            )
            cnx.commit()
        return auth1, auth2, auth3, auth4, index1, index2, index3, fa1, fa2, fa3

    def test_no_restrict_to_service(self):
        auth1, auth2, auth3, auth4, index1, index2, index3, fa1, fa2, fa3 = self._setup_restrict()
        with self.admin_access.cnx() as cnx:
            dedupe(cnx)
            # only auth1 should exist after dedupe
            self.assertFalse(cnx.find(auth2.cw_etype, eid=auth2.eid))
            self.assertFalse(cnx.find(auth3.cw_etype, eid=auth3.eid))
            self.assertFalse(cnx.find(auth4.cw_etype, eid=auth4.eid))
            self.assertTrue(cnx.find(auth1.cw_etype, eid=auth1.eid))
            # all AgentName should have been redirected to auth1
            auth1 = cnx.entity_from_eid(auth1.eid)
            self.assertCountEqual(
                [e.eid for e in auth1.reverse_authority], [index1.eid, index2.eid, index3.eid]
            )
            # all es documents should have been rewritten
            for fa in (fa1, fa2, fa3):
                fa = cnx.entity_from_eid(fa.eid)
                index_entries = fa.reverse_entity[0].doc["index_entries"]
                self.assertTrue(
                    all(i.get("authority") == auth1.eid for i in index_entries),
                    "something wrong with index_entries `{}` (auth: {})".format(
                        index_entries, auth1.eid
                    ),
                )

    def test_restrict_to_service_and_alone(self):
        """
        if dedupe is called with service code we try to dedupe only authorities linked to
        that service (through FAComponent/Findingaid) and authorities with no
        Geogname/Subject/AgentName
        """
        auth1, auth2, auth3, auth4, index1, index2, index3, fa1, fa2, fa3 = self._setup_restrict()
        with self.admin_access.cnx() as cnx:
            dedupe(cnx, service="FRAD033")
            # only auth1 and auth3 should exist after dedupe
            # auth3 has no same_as but it is linked to another service
            self.assertFalse(cnx.find(auth2.cw_etype, eid=auth2.eid))
            self.assertFalse(cnx.find(auth4.cw_etype, eid=auth4.eid))
            self.assertTrue(cnx.find(auth1.cw_etype, eid=auth1.eid))
            self.assertTrue(cnx.find(auth3.cw_etype, eid=auth3.eid))
            # AgentName index2 should have been redirected to auth1
            auth1 = cnx.entity_from_eid(auth1.eid)
            self.assertCountEqual(
                [e.eid for e in auth1.reverse_authority], [index1.eid, index2.eid]
            )
            # only es document related to fa2 should have been rewritten
            for fa in (fa1, fa2):
                fa = cnx.entity_from_eid(fa.eid)
                index_entries = fa.reverse_entity[0].doc["index_entries"]
                self.assertTrue(
                    all(i.get("authority") == auth1.eid for i in index_entries),
                    "something wrong with index_entries `{}` (auth: {})".format(
                        index_entries, auth1.eid
                    ),
                )
            fa3 = cnx.entity_from_eid(fa3.eid)
            index_entries = fa3.reverse_entity[0].doc["index_entries"]
            self.assertTrue(
                all(i.get("authority") == auth3.eid for i in index_entries),
                "something wrong with index_entries `{}`".format(index_entries),
            )


class ReapplyAuthorityOperationsTC(FrACubicConfigMixIn, EADImportMixin, CubicWebTC):
    configcls = PostgresApptestConfiguration

    readerconfig = {
        "esonly": False,
        "index-name": "dummy",
        "appid": "data",
        "nodrop": False,
        "reimport": True,
        "force_delete": True,
    }

    def setUp(self):
        super(ReapplyAuthorityOperationsTC, self).setUp()
        with self.admin_access.cnx() as cnx:
            self.service = cnx.create_entity("Service", code="FRAD095", category="foo")
            services_map = load_services_map(cnx)
            self.service_infos = service_infos_from_service_code(self.service.code, services_map)
            cnx.commit()

    def test_auto_dedupe_without_service(self):
        """No service is set thus no service related authorities will be loaded
        during the import

        """
        with self.admin_access.cnx() as cnx:
            self.import_filepath(cnx, "reapply_auth_op.xml")
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)
        with self.admin_access.cnx() as cnx:
            self.import_filepath(
                cnx, "reapply_auth_op.xml", autodedupe_authorities="service/strict"
            )
            self.assertEqual(len(cnx.find("LocationAuthority")), 4)

    def test_auto_dedupe_with_service(self):
        """The service is set and will be taken into account while loading service related
        authorities during the import

        """
        with self.admin_access.cnx() as cnx:
            self.import_filepath(cnx, "FRAD095_00162.xml", service_infos=self.service_infos)
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)
        with self.admin_access.cnx() as cnx:
            self.import_filepath(cnx, "FRAD095_00162.xml", autodedupe_authorities="service/strict")
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)

    def test_reapply_group_op_without_service(self):
        """No service is set thus no service related authorities will be loaded
        during the import

        """
        with self.admin_access.cnx() as cnx:
            self.import_filepath(cnx, "reapply_auth_op.xml")
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)
            self.assertCountEqual(
                [1, 1], [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()]
            )
        with self.admin_access.cnx() as cnx:
            auth = cnx.find("LocationAuthority", label="Nerville-la-Forêt (Val-d'Oise)").one()
            auth.group((cnx.find("LocationAuthority", label="Nerville-la-Forêt").one().eid,))
            cnx.commit()
            self.assertCountEqual(
                [2, 0], [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()]
            )
        with self.admin_access.cnx() as cnx:
            self.import_filepath(
                cnx, "reapply_auth_op.xml", autodedupe_authorities="service/strict"
            )
            self.assertEqual(len(cnx.find("LocationAuthority")), 3)
            self.assertCountEqual(
                [1, 0, 1],
                [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()],
            )

    def test_reapply_group_op_with_service(self):
        """The service is set and will be taken into account while loading service related
        authorities during the import

        """
        with self.admin_access.cnx() as cnx:
            self.import_filepath(cnx, "FRAD095_00162.xml", service_infos=self.service_infos)
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)
            self.assertCountEqual(
                [1, 1], [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()]
            )
        with self.admin_access.cnx() as cnx:
            auth = cnx.find("LocationAuthority", label="Nerville-la-Forêt (Val-d'Oise)").one()
            auth.group((cnx.find("LocationAuthority", label="Nerville-la-Forêt").one().eid,))
            cnx.commit()
            self.assertCountEqual(
                [2, 0], [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()]
            )
        with self.admin_access.cnx() as cnx:
            self.import_filepath(
                cnx,
                "FRAD095_00162.xml",
                service_infos=self.service_infos,
                autodedupe_authorities="service/strict",
            )
            self.assertEqual(len(cnx.find("LocationAuthority")), 2)
            self.assertCountEqual(
                [2, 0], [len(a.reverse_authority) for a in cnx.find("LocationAuthority").entities()]
            )

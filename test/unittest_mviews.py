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
"""cubicweb-frarchives_edition unit tests for materialized views"""

from contextlib import contextmanager
from cubicweb import Binary
from cubicweb.devtools import testlib  # noqa
from cubicweb.devtools import PostgresApptestConfiguration

import utils
from pgfixtures import setup_module, teardown_module  # noqa


SCHEMA = 'published'


class MViewsBaseTC(utils.FrACubicConfigMixIn, testlib.CubicWebTC):
    configcls = PostgresApptestConfiguration

    @contextmanager
    def access(self, schema=False):
        with self.admin_access.cnx() as cnx:
            if schema:
                cnx.system_sql('SET SESSION search_path TO %s, public;' % schema)
            yield cnx
            cnx.system_sql('SET SESSION search_path TO public;')


class MViewsSchema(MViewsBaseTC):

    def test_setup(self):
        """Ensure SQL schema is created."""
        with self.admin_access.cnx() as cnx:
            schemas = list(cnx.system_sql(
                'select schema_name from information_schema.schemata;'))
            self.assertIn((SCHEMA,), schemas)


class MViewsCMS(MViewsBaseTC):

    def setUp(self):
        super(MViewsCMS, self).setUp()
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            for i in range(100):
                m = ce('Metadata',
                       title=u'Some Metadata Title',
                       description=u'Some decription for NewsContent %s' % i,
                       subject=u'Some subject',)
                image_file = ce('File',
                                data_name=u'hero-decouvrir.jpg',
                                data_format=u'image/jpeg',
                                data=Binary(b'toto'))
                img = ce('Image', caption=u'Décourvir %s' % i,
                         image_file=image_file)
                ce('NewsContent', title=u'NewsContent n°%d' % i,
                   content=u'Some content', metadata=m,
                   news_image=img,
                   start_date="2016/12/%s" % ((i % 31) + 1))
            cnx.commit()

    def test_notsynced_cwuser(self):
        with self.access() as cnx:
            cnx.create_entity('CWUser', login=u'toto', upassword=u'Toto1-Toto1-Toto1',
                              in_group=cnx.find('CWGroup', name='users').one())
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            rset = cnx.find('CWUser', login=u'toto')
            self.assertEqual(len(rset), 0)

    def test_synced_cwuser(self):
        with self.access() as cnx:
            anon = cnx.find('CWUser', login=u'anon').one()
            anon.cw_delete()
            cnx.commit()
            gr = cnx.find('CWGroup', name='guests').one()
            cnx.create_entity('CWUser', login=u'anon', upassword=u'Anon1-Anon1-Anon1', in_group=gr)
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            rset = cnx.find('CWUser', login=u'anon')
            self.assertEqual(len(rset), 1)
            anon = rset.one()
            self.assertEqual(anon.in_group[0].name, 'guests')

    def test_cw_properties(self):
        with self.access() as cnx:
            pkeys = [p for p, in cnx.execute('Any P WHERE X is CWProperty, X pkey P')]
            self.assertIn('system.version.frarchives_edition', pkeys)
            self.assertIn('system.version.jsonschema', pkeys)
        with self.access(SCHEMA) as cnx:
            pkeys = [p for p, in cnx.execute('Any P WHERE X is CWProperty, X pkey P')]
            self.assertNotIn('system.version.frarchives_edition', pkeys)
            self.assertNotIn('system.version.jsonschema', pkeys)

    def test_not_published(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 100)
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_draft"')), 100)
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 0)

    def test_publish_one(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 100)
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_draft"')), 100)
            for e in cnx.execute('NewsContent C WHERE C start_date "2016/12/01"').entities():
                e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access() as cnx:
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_published"')), 4)
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 4)

    def test_unpublish_one(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 100)
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_draft"')), 100)
            for e in cnx.execute('NewsContent C WHERE C start_date "2016/12/01"').entities():
                e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.find('NewsContent')), 4)
        with self.access() as cnx:
            e = cnx.execute('NewsContent C WHERE C start_date "2016/12/01"').get_entity(0, 0)
            e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_unpublish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.find('NewsContent')), 3)

    def test_delete_one(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 100)
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_draft"')), 100)
            for e in cnx.execute('NewsContent C WHERE C start_date "2016/12/01"').entities():
                e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.find('NewsContent')), 4)
            eid = cnx.find('NewsContent').rows[0][0]
        with self.access() as cnx:
            cnx.execute('DELETE NewsContent C WHERE C eid %(eid)s',
                        {'eid': eid})
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.find('NewsContent')), 3)

    def test_modify_published(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.execute('NewsContent C')), 100)
            self.assertEqual(
                len(cnx.execute('NewsContent C WHERE '
                                '  C in_state S, S name "wfs_cmsobject_draft"')), 100)
            for e in cnx.execute(u'NewsContent C WHERE '
                                 u'C title LIKE "NewsContent n°4%"').entities():
                e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.find('NewsContent', title=u'NewsContent n°42')), 1)
        with self.access() as cnx:
            e = cnx.find('NewsContent', title=u'NewsContent n°42').one()
            e.cw_set(title=u'toto')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertFalse(cnx.find('NewsContent', title=u'NewsContent n°42'))
            self.assertEqual(len(cnx.find('NewsContent', title=u'toto')), 1)

    def test_simple_relation(self):
        with self.access() as cnx:
            self.assertEqual(len(cnx.find('NewsContent')), 100)
            e = cnx.find('NewsContent').get_entity(0, 0)
            eid = e.eid
            img = e.news_image[0].eid
            meta = e.metadata[0].eid
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.execute('Any X WHERE X is NewsContent, X news_image Y')), 0)
            self.skipTest('do not handle the whole entity graph')
            # XXX do we want this? Is it possible (at acceptable costs)?
            # self.assertEqual(len(cnx.find('Image', eid=img)), 0)
            self.assertEqual(len(cnx.find('Metadata', eid=meta)), 0)

        with self.access() as cnx:
            e = cnx.execute('NewsContent C WHERE C eid %(eid)s', {'eid': eid}).one()
            e.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.execute('Any X WHERE X is NewsContent, X news_image Y')), 1)
            self.assertEqual(len(cnx.find('Image', eid=img)), 1)
            self.assertEqual(len(cnx.find('Metadata', eid=meta)), 1)


class MViewsEAD(MViewsBaseTC):
    def setUp(self):
        super(MViewsEAD, self).setUp()
        with self.admin_access.cnx() as cnx:
            ce = cnx.create_entity
            for i in range(10):
                did = ce('Did', unittitle=u'Some DID Title')
                fah = ce('FAHeader', titlestmt=u'<h1>Some title</h1>')
                fa = ce('FindingAid', name=u'FindingAid n°%d' % i,
                        eadid=u'EAD %s' % i,
                        publisher=u'Testing Unit LTD.',
                        stable_id=u'ID%d' % i,
                        fa_header=fah,
                        did=did,
                        )
                fac = ce('FAComponent',  # noqa
                         did=did,
                         finding_aid=fa,
                         stable_id=u'ID %d' % i,
                         )
            cnx.commit()

    def test_not_published(self):
        with self.access() as cnx:
            for etype in ('Did', 'FAHeader', 'FAComponent', 'FindingAid'):
                self.assertEqual(len(cnx.find(etype)), 10)
            self.assertEqual(
                len(cnx.execute('FindingAid X WHERE '
                                ' X in_state S, S name "wfs_cmsobject_draft"')), 10)
        with self.access(SCHEMA) as cnx:
            self.assertEqual(len(cnx.execute('FindingAid X')), 0)

    def test_publish_one(self):
        with self.access() as cnx:
            self.assertEqual(
                len(cnx.execute('FindingAid X WHERE '
                                ' X in_state S, S name "wfs_cmsobject_draft"')), 10)
            fa = cnx.find('FindingAid').get_entity(0, 0)
            fa.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
            eid = fa.eid
        with self.access(SCHEMA) as cnx:
            published_rset = cnx.execute('FindingAid X')
            self.assertEqual(len(published_rset), 1)
            self.assertEqual(published_rset.one().eid, eid)

    def test_published_findingaid(self):
        with self.access() as cnx:
            ce = cnx.create_entity
            fa = utils.create_findingaid(cnx)
            ce('FAComponent',
               did=ce('Did', unittitle=u'unittitle', unitid=u'unitid'),
               stable_id=u'stable',
               finding_aid=fa)
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            rset = cnx.find('FindingAid')
            self.assertEqual(len(rset), 0)
            rset = cnx.find('FAComponent')
            self.assertEqual(len(rset), 0)
        with self.access() as cnx:
            fa = cnx.find('FindingAid', eid=fa.eid).one()
            fa.cw_adapt_to('IWorkflowable').fire_transition('wft_cmsobject_publish')
            cnx.commit()
        with self.access(SCHEMA) as cnx:
            rset = cnx.find('FindingAid')
            self.assertEqual(len(rset), 1)
            rset = cnx.find('FAComponent')
            self.assertEqual(len(rset), 1)


if __name__ == '__main__':
    import unittest
    unittest.main()

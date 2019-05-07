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
from io import BytesIO
from six import text_type as unicode  # noqa
from mock import patch

from utils import FrACubicConfigMixIn
from pgfixtures import setup_module, teardown_module  # noqa

from nazca.rl.aligner import BaseAligner, PipelineAligner
from nazca.rl.blocking import MinHashingBlocking, PipelineBlocking
from nazca.utils.distances import BaseProcessing
from nazca.utils.normalize import (NormalizerPipeline, simplify,
                                   SimplifyNormalizer)
from nazca.utils.minhashing import Minlsh

from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_frarchives_edition import get_samesas_history, GEONAMES_RE
from cubicweb_frarchives_edition.alignments import geonames_align, location


class AlignTaskBaseTC(FrACubicConfigMixIn, CubicWebTC):
    """Alignment task test cases base class."""
    configcls = PostgresApptestConfiguration

    def insert(self, cnx, filename):
        """Insert contents of file into database.

        :param Connection cnx: server-side connection
        :param str filename: filename
        """
        path = self.datapath(filename)
        sql = "copy geonames from '{}' null as ''".format(path)
        cnx.system_sql(sql)


class AlignmentTC(AlignTaskBaseTC):
    """Test cases of special cases."""

    def test_paris(self):
        """Test label that does not contain context.

        Trying: Paris
        Expecting: is not aligned
        """
        rows = [
            [123, u'U', u'Paris', u'Paris', u'75', 123, 123]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "paris.txt")
            geonames = location.build_geoname_set(cnx, dpt_code=rows[0][4])
            pairs = location.alignment_geo_data(records_dptonly, geonames)
            cells = geonames_align.cells_from_pairs(
                pairs, geonames, records_dptonly
            )
            self.assertFalse(list(cells))

    def test_toulon(self):
        """Test label that contains dpt as context.

        Trying: Toulon (Var)
        Expecting: is aligned to Toulon (Provence-Alpes-Côte d'Azur, Var)
        """
        rows = [
            [123, u'U', u'Toulon (Var)', u'Var', u'83', 123, 123]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "toulon.txt")
            geonames = location.build_geoname_set(cnx)
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(
                geonames_align.cells_from_pairs(pairs, geonames, records)
            )
        self.assertEqual(
            cells[0][3], u'http://www.geonames.org/2972328'
        )

    def test_marrons(self):
        """Test label that contains dpt and city as context.

        Trying: Les Marrons (Saint-Michel-de-Chaillol, Hautes-Alpes; hameau)
        Expecting: is aligned to Les Marrons
        """
        rows = [
            [
                123, u'U',
                u'Les Marrons (Saint-Michel-de-Chaillol, Hautes-Alpes; hameau)',
                u'Hautes-Alpes', u'05', 123, 123
            ]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "marrons.txt")
            geonames = location.build_geoname_set(cnx)
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(
                geonames_align.cells_from_pairs(pairs, geonames, records)
            )
        self.assertEqual(
            cells[0][3], u'http://www.geonames.org/3000378'
        )

    def test_montaigu(self):
        """Test label that contains context that is neither dpt nor city.

        Trying: Montaigu (collège de)
        Expecting: is not aligned
        """
        rows = [
            [123, u'U', u'Montaigu (collège de)', u'Paris', u'75', 123, 123]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "montaigu.txt")
            geonames = location.build_geoname_set(cnx, dpt_code=rows[0][4])
            pairs = location.alignment_geo_data(records_dptonly, geonames)
            cells = geonames_align.cells_from_pairs(
                pairs, geonames, records_dptonly
            )
        self.assertFalse(list(cells))

    def test_breuil(self):
        """Test label that contains context that is neither dpt nor city
        and is associated with more than one service.

        Trying: Breuil (Le) associated with Yvelines, Marne, Saône-et-Loire,
        Haute-Saône, Rhône
        Expecting: is aligned to Le Breuil in Yvelines, Le Breuil
        in Marne, Le Breuil in Saône-et-Loire et Le Breuil in Rhône
        """
        finding_aids = [
            [[123, u'U', u'Breuil (Le)', u'Yvelines', u'78', 123, 123]],
            [[345, u'U', u'Breuil (Le)', u'Marne', u'51', 345, 345]],
            [[567, u'U', u'Breuil (Le)', u'Saône-et-Loire', u'71', 567, 567]],
            [[789, u'U', u'Breuil (Le)', u'Haute-Saône', u'70', 789, 789]],
            [[901, u'U', u'Breuil (Le)', u'Rhône', u'69', 901, 901]]
        ]
        urls = []
        for rows in finding_aids:
            records, records_dptonly, _ = geonames_align.build_record(rows)
            with self.admin_access.cnx() as cnx:
                self.insert(cnx, "breuil.txt")
                geonames = location.build_geoname_set(cnx, dpt_code=rows[0][4])
                pairs = location.alignment_geo_data(records_dptonly, geonames)
                cells = list(
                    geonames_align.cells_from_pairs(
                        pairs, geonames, records_dptonly
                    )
                )
                if cells:
                    urls.append(cells[0][3])
        self.assertItemsEqual(
            [
                "http://www.geonames.org/3004994",
                "http://www.geonames.org/3004993",
                "http://www.geonames.org/3005001",
                "http://www.geonames.org/3005010"
            ],
            urls
        )

    def test_bagnolet(self):
        """Bagnolet (Eure-et-Loir, France) must not be aligned.

        MinHash is not applicable at word level.

        Trying: Bagnolet (Eure-et-Loir, France)
        Expecting: is not aligned.
        """
        rows = [
            [
                123, u'U', u'Bagnolet (Eure-et-Loir, France)',
                u'Eure-et-Loir', u'28', 123, 123
            ]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "bagnolet.txt")
            geonames = location.build_geoname_set(cnx)
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(
                geonames_align.cells_from_pairs(pairs, geonames, records)
            )
            self.assertFalse(cells)

    def test_an_minutiers(self):
        """Test special case AN-Minutier.

        Trying: rue
        Expecting: is aligned to Paris
        """
        rows = [
            [139758648, u'MC/E',  u'Truffaut (rue)', u'FRAN', 93,
             139843625, 18322050]
        ]
        records, records_dptonly, _ = geonames_align.build_record(rows)
        expected = [
            [139758648, u'Paris', [u'paris', 'paris', None],
             u'Truffaut (rue)', 139843625,  18322050,
             u'Paris (paris, paris)']]
        self.assertEqual(expected, records)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "minutiers.csv")
            geonames = location.build_geoname_set(cnx)
            pairs = location.alignment_geo_data(records, geonames)
            cells = list(
                geonames_align.cells_from_pairs(pairs, geonames, records)
            )
            self.assertEqual(
                cells[0][3], 'http://www.geonames.org/6455259'
            )
        geoname_records = [
            [2968815, u'Paris', (u'paris', u'paris', None), u'Paris (paris)',
             'Paris (paris, paris)']
        ]
        pairs = location.alignment_geo_data(records, geoname_records)
        self.assertEqual(len(pairs), 1)

    def test_algerie(self):
        """Test aligning to country if no context is given.

        Trying: Algérie
        Expecting: is aligned to Algeria
        """
        rows = [
            [
                123, u'U', u'Algérie', u'Paris', u'13', 123, 123
            ]
        ]
        _, _, records_countryonly = geonames_align.build_record(rows)
        with self.admin_access.cnx() as cnx:
            self.insert(cnx, "algerie.txt")
            geonames = location.build_countries_geoname_set(
                cnx, (2589581,)
            )
            pairs = location.alignment_geo_data_countryonly(
                records_countryonly, geonames
            )
            cells = list(
                geonames_align.cells_from_pairs(pairs, geonames, records_countryonly)
            )
            expected = ['Algérie', 'Algérie', 'People’s Democratic Republic of Algeria',
                        'http://www.geonames.org/2589581',
                        'yes', '123', '123', '123', '2589581', 'Algérie']
            self.assertEqual(expected, cells[0])


class PipelineComponentsTC(AlignTaskBaseTC):
    """Alignment task pipeline components test cases."""

    def test_process_empty_csv(self):
        existing_alignment = set()
        headers = [bytes(k) for k in geonames_align.Record.headers.keys()]
        b = BytesIO()
        b.write(b'\t'.join(headers))
        result = geonames_align.process_csv(b, existing_alignment, ())
        self.assertEqual(result, ({}, {}))

    def test_build_records(self):
        rows = [
            [134877395, u'U1', u'Mesvres (canton)', u'Département de Saône-et-Loire', u'71',
             134879256, 26494078, ],
            [134877395, u'U1', u'Vanves (Hauts-de-Seine, France)',
             u'Département de Saône-et-Loire', u'71',
             134879256, 26494078]
        ]
        expected = (
            [[134877395, u'Vanves', [u'hauts de seine', None, None],
              u'Vanves (Hauts-de-Seine, France)', 134879256, 26494078,
              u'Vanves (Hauts-de-Seine, France)']],
            [[134877395, u'Mesvres', [u'saone et loire', None, None],
              u'Mesvres (canton)', 134879256, 26494078, u'Mesvres (canton) saone et loire']],
            []
        )
        self.assertEqual(expected, geonames_align.build_record(rows))

    def test_build_record_dptonly(self):
        rows = [
            [134877395, u'U1', u'Cortiambles (Givry)', u'Département de Saône-et-Loire', u'71',
             134878773, 26495096],
            [134877395, u'U2', u'Tours (Anzy-le-Duc)', u'Département de Saône-et-Loire', u'71',
             134911636, 26494709],
            [134877395, u'U3', u'Bourbon-Lancy, Bailliage de (France)',
             u'Département de Saône-et-Loire', u'71', 134912145, 26494308]
        ]
        expected = (
            [],
            [[134877395, u'Cortiambles', [u'saone et loire', u'givry', None],
              u'Cortiambles (Givry)', 134878773, 26495096,
              u'Cortiambles (Givry) saone et loire'],
             [134877395, u'Tours', [u'saone et loire', u'anzy le duc', None],
              u'Tours (Anzy-le-Duc)', 134911636, 26494709,
              u'Tours (Anzy-le-Duc) saone et loire'],
             [134877395, u'Bourbon-Lancy, Bailliage de', [u'saone et loire', None, None],
              u'Bourbon-Lancy, Bailliage de (France)', 134912145, 26494308,
              u'Bourbon-Lancy, Bailliage de (France) saone et loire']],
            []
        )
        self.assertEqual(expected, geonames_align.build_record(rows))

    def _test_minhashing_pipline(self, refset, targetset):
        """MinHash pipeline.

        :param set refset: reference set
        :param set targetset: target set

        :returns: list of aligned pairs
        :rtype: list
        """
        processing = BaseProcessing(
            ref_attr_index=0, target_attr_index=0, distance_callback=location.approx_match
        )
        place_normalizer = NormalizerPipeline((SimplifyNormalizer(attr_index=0),))
        dpt_aligner = BaseAligner(threshold=0.2, processings=(processing,))
        dpt_aligner.register_ref_normalizer(place_normalizer)
        dpt_aligner.register_target_normalizer(place_normalizer)
        blocking_2 = MinHashingBlocking(0, 0, threshold=0.4)
        dpt_blocking = PipelineBlocking((blocking_2,), collect_stats=True)
        dpt_aligner.register_blocking(dpt_blocking)
        return list(PipelineAligner((
            dpt_aligner,
        )).get_aligned_pairs(refset, targetset))

    def test_minhashing_bagnolet(self):
        """
        Test a PipelineAligner minhashing
        """
        self.skipTest('This test serves as documentation')
        refset = [['bagnolet']]
        targetset = [['baignolet']]
        pairs = self._test_minhashing_pipline(refset, targetset)
        self.assertFalse(pairs)
        with patch('random.randint', lambda a, b: (a + b) // 2):
            minlsh = Minlsh()
            sentences = (refset[0][0], targetset[0][0])
            minlsh.train((simplify(s, remove_stopwords=True) for s in sentences),)
            self.assertEqual(set([]),  minlsh.predict(0.1))
            self.assertEqual(set([]),  minlsh.predict(0.4))

    def test_minhashing_gond(self):
        """
        Test a PipelineAligner minhashing with two sentences
        """
        self.skipTest('This test serves as documentation')
        refset = [['Saint-Gond, Marais de']]
        targetset = [['Marais de Saint-Gond']]
        pairs = self._test_minhashing_pipline(refset, targetset)
        self.assertTrue(pairs)
        with patch('random.randint', lambda a, b: (a + b) // 2):
            minlsh = Minlsh()
            sentences = (refset[0][0], targetset[0][0])
            minlsh.train((simplify(s, remove_stopwords=True) for s in sentences),)
            self.assertEqual(set([(0, 1)]),  minlsh.predict(0.4))


class AlignTC(AlignTaskBaseTC):
    """Test cases of special cases."""

    def build_record(self, fa, loc, geoname_uri, keep):
        m = geonames_align.CONTEXT_RE.search(loc.label)
        label = m.group(1) if m else loc.label
        return geonames_align.Record(dict([
            ('pnia original label', loc.label),
            ('pnia name before parentheses (used for alignment)', label),
            ('geoname label (used for alignment)', label),
            ('geoname uri', geoname_uri),
            ('keep', keep),
            ('findingaid eid', fa.eid),
            ('facomponent eid', ''),
            ('pnia authority eid', loc.eid),
            ('geoname id', GEONAMES_RE.search(geoname_uri).group(1)),
            ('geoname label with admin code (used for UI display)', loc.label),
        ]))

    def create_findingaid(self, cnx, eadid):
        return cnx.create_entity(
            'FindingAid', name=eadid,
            stable_id=u'stable_id{}'.format(eadid),
            eadid=eadid,
            publisher=u'publisher',
            did=cnx.create_entity(
                'Did', unitid=u'unitid{}'.format(eadid),
                unittitle=u'title{}'.format(eadid)),
            fa_header=cnx.create_entity('FAHeader')
        )

    def test_process_empty_csv(self):
        existing_alignment = set()
        sameas_history = None
        headers = [bytes(k) for k in geonames_align.Record.headers.keys()]
        b = BytesIO()
        b.write(b'\t'.join(headers))
        result = geonames_align.process_csv(b, existing_alignment, sameas_history)
        self.assertEqual(result, ({}, {}))

    def setup_process_alignments(self, cnx, geoname_uri):
        paris = cnx.create_entity(
            'ExternalUri',
            label=u'Paris (France)',
            uri=geoname_uri,
            extid=u'2988507'
        )
        loc = cnx.create_entity(
            'LocationAuthority',
            label=u'Paris (France)',
            latitude=0.0,
            longitude=0.0,
            same_as=paris
        )
        fa = self.create_findingaid(cnx, u'eadid1')
        cnx.create_entity('Geogname',
                          label=u'index location 1',
                          index=fa, authority=loc)
        cnx.commit()
        loc = cnx.find('LocationAuthority', eid=loc.eid).one()
        self.assertTrue(loc.same_as)
        paris = cnx.find('ExternalUri', eid=paris.eid).one()
        self.assertEqual('geoname', paris.source)
        self.assertEqual([(geoname_uri, loc.eid, True), ],
                         get_samesas_history(cnx, complete=True))
        # insert here to avoid update due to hooks
        cnx.system_sql(
            '''INSERT INTO geonames (geonameid, name, latitude, longitude)
            VALUES (2988507, 'Paris', 48.85, 2.34)
            '''
        )
        cnx.commit()
        return fa, loc

    def test_create_new_alignement(self):
        """
        The alignement has been launched automatically.
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            fa = self.create_findingaid(cnx, u'eadid1')
            loc = cnx.create_entity(
                'LocationAuthority',
                label=u'Paris (France)',
            )
            cnx.create_entity('Geogname',
                              label=u'index location 1',
                              index=fa, authority=loc)
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            record = self.build_record(fa, loc, geoname_uri, 'n')
            key = (fa.eid, u'Paris (France)', geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # do not delete the existing same_as
            aligner.process_alignments(new_alignment, {})
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertTrue(loc.same_as)

    def test_dont_delete_user_alignments(self):
        """
        A same_as relation has been set by user,
        The alignement has been launched automatically.

        Trying: do not delete the existing alignement
        Expecting : same_as relation still exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            fa, loc = self.setup_process_alignments(cnx, geoname_uri)
            new_alignment = {}
            record = self.build_record(fa, loc, geoname_uri, 'n')
            key = (fa.eid, u'Paris (France)', geoname_uri)
            to_remove_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # do not delete the existing same_as
            aligner.process_alignments(new_alignment, to_remove_alignment)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertTrue(loc.same_as)

    def test_delete_user_alignments(self):
        """
        A same_as relation has been set by user.
        Process_alignments is launched from the file.

        Trying: delete the existing alignement
        Expecting : same_as relation do not exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            fa, loc = self.setup_process_alignments(cnx, geoname_uri)
            cnx.commit()
            new_alignment = {}
            record = self.build_record(fa, loc, geoname_uri, 'n')
            key = (fa.eid, u'Paris (France)', geoname_uri)
            to_remove_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # do not delete the existing same_as
            aligner.process_alignments(new_alignment, to_remove_alignment)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            # try to delete the existing same_as
            aligner.process_alignments(new_alignment,
                                       to_remove_alignment,
                                       override_alignments=True)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertFalse(loc.same_as)

    def test_dont_create_deleted_alignments(self):
        """
        A same_as relation has been set and then removed by user.
        Process_alignments is launched from the file.

        Trying: dont add the removed alignement
        Expecting : same_as relation must not exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            fa, loc = self.setup_process_alignments(cnx, geoname_uri)
            loc.cw_set(same_as=None)
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertFalse(loc.same_as)

            to_remove_alignment = {}
            record = self.build_record(fa, loc, geoname_uri, 'y')
            key = (fa.eid, u'Paris (France)', geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # the deleted alignemnt must not be recreated
            aligner.process_alignments(new_alignment, to_remove_alignment)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            self.assertEqual(1,
                             cnx.execute('Any COUNT(X) WHERE X is ExternalUri')[0][0])

    def test_create_deleted_alignments(self):
        """
        A same_as relation has been set and then removed by user.
        The alignement has been launched automatically.

        Trying: add the removed alignement
        Expecting : same_as relation must not exists
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            fa, loc = self.setup_process_alignments(cnx, geoname_uri)
            loc.cw_set(same_as=None)
            cnx.commit()
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertFalse(loc.same_as)
            to_remove_alignment = {}
            record = self.build_record(fa, loc, geoname_uri, 'y')
            key = (fa.eid, u'Paris (France)', geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            # the deleted alignemnt must not be recreated
            aligner.process_alignments(new_alignment, to_remove_alignment,
                                       override_alignments=True)
            loc = cnx.find('LocationAuthority', eid=loc.eid).one()
            self.assertTrue(loc.same_as)
            self.assertEqual(1,
                             cnx.execute('Any COUNT(X) WHERE X is ExternalUri')[0][0])

    def test_do_not_update_user_defined(self):
        """
        Test GeoNames alignment.

        Trying: update alignments when there is user-defined same_as relation
        and override_alignments toggle is off
        Expecting: user-defined latitude/longitude values are not changed
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            findingaid, locationauthority = self.setup_process_alignments(
                cnx, geoname_uri
            )
            record = self.build_record(
                findingaid, locationauthority, geoname_uri, 'y'
            )
            key = (findingaid.eid, u'Paris (France)', geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(new_alignment, {})
            locationauthority = cnx.find(
                'LocationAuthority', eid=locationauthority.eid
            ).one()
            self.assertEqual(locationauthority.latitude, 0.0)
            self.assertEqual(locationauthority.longitude, 0.0)

    def test_update_user_defined(self):
        """Test GeoNames alignment.

        Trying: update alignments when there is user-defined same_as relation
        and override_alignments toggle is on
        Expecting: user-defined latitude/longitude values are changed
        """
        with self.admin_access.cnx() as cnx:
            geoname_uri = u'http://www.geonames.org/2988507'
            findingaid, locationauthority = self.setup_process_alignments(
                cnx, geoname_uri
            )
            record = self.build_record(
                findingaid, locationauthority, geoname_uri, 'y'
            )
            key = (findingaid.eid, u'Paris (France)', geoname_uri)
            new_alignment = {key: record}
            aligner = geonames_align.GeonameAligner(cnx)
            aligner.process_alignments(
                new_alignment, {}, override_alignments=True
            )
            locationauthority = cnx.find(
                'LocationAuthority', eid=locationauthority.eid
            ).one()
            self.assertEqual(locationauthority.latitude, 48.85)
            self.assertEqual(locationauthority.longitude, 2.34)

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
from pgfixtures import setup_module, teardown_module  # noqa


from utils import FrACubicConfigMixIn
from cubicweb.devtools import PostgresApptestConfiguration
from cubicweb.devtools.testlib import CubicWebTC

from cubicweb_frarchives_edition.alignments import bano_align


class BanoAlignTC(FrACubicConfigMixIn, CubicWebTC):
    """BANO alignment task test cases."""

    configcls = PostgresApptestConfiguration

    def test_compute_existing_aligment(self):
        """Test fetching existing alignments from database.

        Trying: fetching existing alignments
        Expecting: existing alignments
        """
        with self.admin_access.cnx() as cnx:
            cnx.allow_all_hooks_but("align")
            # create ExternalId
            extid = cnx.create_entity(
                "ExternalId", label="Paris", extid="1234567890", source="bano"
            )
            # create LocationAuthority
            auth = cnx.create_entity(
                "LocationAuthority", label="Paris", latitude=0.0, longitude=0.0, same_as=extid
            )
            # create FindingAid
            fa_header = cnx.create_entity("FAHeader")
            did = cnx.create_entity("Did", unittitle="foo")
            service = cnx.create_entity("Service", category="bar")
            findingaid = cnx.create_entity(
                "FindingAid",
                name="baz",
                eadid="1234567890",
                fa_header=fa_header,
                did=did,
                stable_id="1234567890",
                service=service,
                publisher="foobar",
            )
            # create Geogname
            cnx.create_entity("Geogname", label="Paris", index=(findingaid,), authority=auth)
            cnx.commit()
            cnx.allow_all_hooks_but()
            aligner = bano_align.BanoAligner(cnx)
            expected = {(str(auth.eid), extid.extid)}
            self.assertEqual(aligner.compute_existing_alignment(), expected)

    def test_process_label_delim_voie_type(self):
        """Test processing label.

        Trying: 'Nice (Alpes-Maritimes, France) -- Corniche Fleurie (avenue)'
        Expecting: nom_comm = 'Nice' and voie = 'avenue Corniche Fleurie'
        """
        label = "Nice (Alpes-Maritimes, France) -- Corniche Fleurie (avenue)"
        processed_label = bano_align._process_label(label)
        self.assertEqual(processed_label, {"city": "Nice", "voie": "avenue Corniche Fleurie"})

    def test_process_label_delim_voie(self):
        """Test processing label.

        Trying: 'Nice (Alpes-Maritimes, France) -- Faubourg'
        Expecting: nom_comm = 'Nice' and voie = ''
        """
        label = "Nice (Alpes-Maritimes, France) -- Faubourg"
        processed_label = bano_align._process_label(label)
        self.assertEqual(processed_label, {"city": "Nice", "voie": "Faubourg"})

    def test_process_label_delim_voie_type_numero(self):
        """Test processing label.

        Trying: 'Nice (Alpes-Maritimes, France) -- Arènes de Cimiez (avenue ; 29 bis)'
        Expecting: nom_comm = 'Nice' and voie = 'avenue Arènes de Cimiez'
        """
        label = "Nice (Alpes-Maritimes, France) -- Arènes de Cimiez (avenue ; 29 bis)"
        processed_label = bano_align._process_label(label)
        self.assertEqual(processed_label, {"city": "Nice", "voie": "avenue Arènes de Cimiez"})

    def test_process_label_delim_whitespace(self):
        """Test processing label.

        Trying: '   -- foo'
        Expecting: None
        """
        label = "  -- foo"
        self.assertIsNone(bano_align._process_label(label))

    def test_process_label_an(self):
        """Test processing label.

        Trying: 'Acacias (petite rue des)'
        Expecting: nom_comm = 'Paris' and voie = 'petite rue des Acacias'
        """
        label = "Acacias (petite rue des)"
        processed_label = bano_align._process_label(label, city="Paris")
        self.assertEqual(processed_label, {"city": "Paris", "voie": "petite rue des Acacias"})

    def test_normalize_voie_sort(self):
        """Test processing label.

        Trying: 'Nice (Alpes-Maritimes, France) -- Poissonnerie, La (rue ; 2)'
        and 'Rue de la Poissonnerie' (BANO)
        Expecting: voie = 'rue Poissonnerie, La'
        and voie (normalized) = 'la poissonnerie rue' matches normalized BANO
        """
        label = "Nice (Alpes-Maritimes, France) -- Poissonnerie, La (rue ; 2)"
        actual = bano_align._process_label(label)["voie"]
        self.assertEqual(actual, "rue Poissonnerie, La")
        self.assertEqual(bano_align._normalize_voie(actual), "la poissonnerie rue")
        self.assertEqual(
            bano_align._normalize_voie(actual), bano_align._normalize_voie("Rue de la Poissonnerie")
        )

    def test_build_record_delim(self):
        """Test building record.

        Trying: ' -- ' in label
        Expecting: corresponding record
        """
        locations = [
            (
                "",
                "",
                "",
                "",
                "Nice (Alpes-Maritimes, France) -- Corniche Fleurie (avenue)",
                "foo",
                "bar",
                "",
            )
        ]
        actual = list(bano_align.build_record(locations))
        expected = [locations[0] + ({"city": "Nice", "voie": "avenue Corniche Fleurie"},)]
        self.assertEqual(actual, expected)

    def test_build_record_an(self):
        """Test building record.

        Trying: Minutiers des AN
        Expecting: corresponding record
        """
        locations = [("", "", "", "", "Acacias (petite rue des)", "MC/foo", "FRAN", "")]
        actual = list(bano_align.build_record(locations))
        expected = [locations[0] + ({"city": "Paris", "voie": "petite rue des Acacias"},)]
        self.assertEqual(actual, expected)

    def test_build_record_lyon(self):
        """Test a label without whitespaces"""
        locations = [("foo", "bar", "", "", "Lyon--Célestins--Place des", "baz", "foobar", "")]
        actual = list(bano_align.build_record(locations))
        expected = [locations[0] + ({"city": "Lyon", "voie": "Célestins--Place des"},)]
        self.assertEqual(actual, expected)

    def test_build_record_other(self):
        """Test building record.

        Trying: neither ' -- ' in label nor Minutiers des AN
        Expecting: None
        """
        locations = [
            (
                "",
                "",
                "",
                "",
                "Saint-Brice-Courcelles (Marne, Champagne-Ardenne, France)",
                "foo",
                "bar",
                "",
            )
        ]
        # neither does label contain ' -- ' nor Minutiers des AN
        # generator is empty
        self.assertEqual(list(bano_align.build_record(locations)), [])

    def test_align_delim_strict(self):
        """Test aligning to BANO dataset.

        Trying: 'Nice (Alpes-Maritimes, France) -- Arènes de Cimiez (avenue ; 29 bis)'
        Expecting: is aligned
        """
        with self.admin_access.cnx() as cnx:
            locations = [
                (
                    "",
                    "",
                    "",
                    "",
                    "Nice (Alpes-Maritimes, France) -- Arènes de Cimiez (avenue ; 29 bis)",
                    "foo",
                    "bar",
                    "",
                )
            ]
            bano_set = [("1234567890", "Avenue des Arènes de Cimiez", "Nice", 0.0, 0.0)]
            records = list(bano_align.build_record(locations))
            bano_aligner = bano_align.BanoAligner(cnx)
            pairs = bano_aligner.align(bano_set, records)
            self.assertEqual(pairs, [(0, 0)])

    def test_align_delim(self):
        """Test aligning to BANO dataset.

        Trying: Nice (Alpes-Maritimes, France) -- Cluvier (rue ; 1 bis)
        Expecting: is aligned
        """
        with self.admin_access.cnx() as cnx:
            locations = [
                (
                    "",
                    "",
                    "",
                    "",
                    "Nice (Alpes-Maritimes, France) -- Cluvier (rue ; 1 bis)",
                    "foo",
                    "bar",
                    "",
                )
            ]
            bano_set = [("3456789012", "Rue Cluvier", "Nice", 0.0, 0.0)]
            records = list(bano_align.build_record(locations))
            bano_aligner = bano_align.BanoAligner(cnx)
            pairs = bano_aligner.align(bano_set, records)
            self.assertEqual(pairs, [(0, 0)])

    def test_align_an(self):
        """Test aligning to BANO dataset.

        Trying:
        Expecting:
        """
        with self.admin_access.cnx() as cnx:
            locations = [("", "", "", "", "rue d'Austerlitz", "MC/foobar", "FRAN", "")]
            bano_set = [("2345678901", "Rue d'Austerlitz", "Paris", 0.0, 0.0)]
            records = list(bano_align.build_record(locations))
            bano_aligner = bano_align.BanoAligner(cnx)
            pairs = bano_aligner.align(bano_set, records)
            self.assertEqual(pairs, [(0, 0)])

    def test_align_an_no_match(self):
        """Test aligning to BANO dataset.

        Trying: 'Acacias (petite rue des)'
        Expecting: is not aligned
        """
        with self.admin_access.cnx() as cnx:
            locations = [("", "", "", "", "Acacias (petite rue des)", "MC/foobar", "FRAN", "")]
            bano_set = [("0987654321", "Rue des Acacias", "Paris", 0.0, 0.0)]
            records = list(bano_align.build_record(locations))
            bano_aligner = bano_align.BanoAligner(cnx)
            pairs = bano_aligner.align(bano_set, records)
            self.assertEqual(pairs, [])

    def _create_record(self, authority, geogname, externalid, keep="y"):
        """Create BanoRecord based on LocationAuthority and
        ExternalID entities.

        :param LocationAuthority authority: LocationAuthority entity
        :param Geogname geogname: Geogname entity
        :param ExternalID externalid: ExternalID entity

        :returns: record
        :rtype: BanoRecord
        """
        with self.admin_access.cnx() as cnx:
            geogname_uri = cnx.build_url(geogname.eid)
            auth_uri = cnx.build_url("location/{eid}".format(eid=authority.eid))
        record = bano_align.BanoRecord(
            {
                "pnia authority eid": authority.eid,
                "URI_Geogname": geogname_uri,
                "libellé_Geogname": geogname.label,
                "URI_LocationAuthority": auth_uri,
                "libellé_LocationAuthority": authority.label,
                "bano id": externalid.extid,
                "bano label (used for UI display)": externalid.label,
                "longitude": authority.longitude,
                "latitude": authority.latitude,
                "keep": keep,
                "fiabilité_alignement": "1",
            }
        )
        return record

    def _insert_user_defined(self):
        """Insert user-defined same_as relation modification.

        :returns: authority, externalid and record
        :rtype: LocationAuthority, ExternalId, BanoRecord
        """
        with self.admin_access.cnx() as cnx:
            authority = cnx.create_entity("LocationAuthority", label="foo")
            geogname = cnx.create_entity("Geogname", label="foo", authority=authority)
            externalid = cnx.create_entity(
                "ExternalId", extid="4567890123", label="bar", source="bano"
            )
            record = self._create_record(authority, geogname, externalid)
            # user removed alignment
            cnx.system_sql(
                """INSERT INTO sameas_history (sameas_uri, autheid, action)
                VALUES (%s, %s, false)""",
                (externalid.extid, authority.eid),
            )
            cnx.commit()
        return authority, externalid, record

    def test_do_not_update_bano_user_defined(self):
        """Test BANO alignment.

        Trying: update alignments when there are user-defined same_as relation
        modifications and override_alignments toggle is on
        Expecting: user-defined modifications are not updated
        """
        with self.admin_access.cnx() as cnx:
            authority, externalid, record = self._insert_user_defined()
            bano_aligner = bano_align.BanoAligner(cnx)
            new_alignment = {("2345678901", authority.label, externalid.extid): record}
            bano_aligner.process_alignments(new_alignment, {}, override_alignments=False)
            # do not re-insert alignment
            self.assertEqual(cnx.system_sql("SELECT * FROM same_as_relation").fetchall(), [])
            # update alignment to same BANO ID than user-defined same_as relation
            authority = cnx.create_entity("LocationAuthority", label="bar")
            geogname = cnx.create_entity("Geogname", label="foo", authority=authority)
            externalid = cnx.create_entity(
                "ExternalId", extid="5678901234", label="foobar", source="bano"
            )
            cnx.commit()
            record = self._create_record(authority, geogname, externalid)
            new_alignment = {(authority.eid, externalid.extid): record}
            bano_aligner.process_alignments(new_alignment, {}, override_alignments=False)
            self.assertEqual(
                cnx.system_sql("SELECT * FROM same_as_relation").fetchall(),
                [(authority.eid, externalid.eid)],
            )

    def test_update_bano_user_defined(self):
        """Test BANO alignment.

        Trying: update alignments when there are user-defined same_as relation
        modifications and override_alignments toggle is off
        Expecting: user-defined modifications are updated
        """
        with self.admin_access.cnx() as cnx:
            authority, externalid, record = self._insert_user_defined()
            bano_aligner = bano_align.BanoAligner(cnx)
            new_alignment = {(authority.eid, externalid.extid): record}
            bano_aligner.process_alignments(new_alignment, {}, override_alignments=True)
            # re-insert alignment
            self.assertEqual(
                cnx.system_sql("SELECT * FROM same_as_relation").fetchall(),
                [(authority.eid, externalid.eid)],
            )

    def test_update_sameas_history_new_alignment(self):
        """Test updating same-as relation history.

        Trying: adding / modifying alignment
        Expecting: same-as relation history is updated
        """
        with self.admin_access.cnx() as cnx:
            authority = cnx.create_entity("LocationAuthority", label="foo")
            geogname = cnx.create_entity("Geogname", label="foo", authority=authority)
            externalid = cnx.create_entity(
                "ExternalId", extid="4567890123", label="bar", source="bano"
            )
            record = self._create_record(authority, geogname, externalid)
            new_alignment = {(authority.eid, externalid.extid): record}
            bano_aligner = bano_align.BanoAligner(cnx)
            bano_aligner.process_alignments(new_alignment, {}, True)
            rows = cnx.system_sql(
                """SELECT * FROM sameas_history
                WHERE sameas_uri=%(extid)s AND autheid=%(autheid)s
                AND action=true""",
                {"extid": externalid.extid, "autheid": authority.eid},
            ).fetchall()
            self.assertTrue(rows)

    def test_update_sameas_history_to_remove_alignment(self):
        """Test updating same-as relation history.

        Trying: removing alignment
        Expecting: same-as relation history is updated
        """
        with self.admin_access.cnx() as cnx:
            authority = cnx.create_entity("LocationAuthority", label="foo")
            geogname = cnx.create_entity("Geogname", label="foo", authority=authority)
            externalid = cnx.create_entity(
                "ExternalId", extid="4567890123", label="bar", source="bano"
            )
            bano_aligner = bano_align.BanoAligner(cnx)
            record = self._create_record(authority, geogname, externalid)
            new_alignment = {(authority.eid, externalid.extid): record}
            bano_aligner.process_alignments(new_alignment, {}, True)
            record = self._create_record(authority, geogname, externalid, keep="n")
            to_remove_alignment = {(authority.eid, externalid.extid): record}
            bano_aligner.process_alignments({}, to_remove_alignment, True)
            rows = cnx.system_sql(
                """SELECT * FROM sameas_history
                WHERE sameas_uri=%(extid)s AND autheid=%(autheid)s
                AND action=false""",
                {"extid": externalid.extid, "autheid": authority.eid},
            ).fetchall()
            self.assertTrue(rows)

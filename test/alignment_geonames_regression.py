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

import csv
from pathlib import Path
from functools import partial

from cubicweb_frarchives_edition.alignments import location, geonames_align


def read_in(path):
    """Read CSV file in.

    :returns: list of examples and list of expected results
    :rtype: list and list
    """
    with open(path) as fp:
        yield from csv.reader(fp, delimiter="\t")


def read_in_test_data(cnx, test_data):
    """Read in test data.

    :param Connection cnx: CubicWeb database connection
    :param list labels: list of {Agent,Location,Subject}Authority labels

    :returns: test data
    :rtype: tuple
    """
    geodata = location.Geodata(cnx)
    return geonames_align.build_record(
        [
            [str(i), "", "", "", label, "", code, department, True]
            for i, (label, code, department) in enumerate(test_data)
        ],
        geodata,
    )


def run_alignment(cnx, test_data_path):
    records_fr, records_dpt, _, records_topographic = read_in_test_data(
        cnx, read_in(test_data_path)
    )
    geodata = location.Geodata(cnx)
    fclass = [
        [
            sorted(records_fr, key=lambda x: x[0]),
            sorted(location.build_geoname_set(cnx, geodata)),
            partial(location.alignment_geo_data, repeatable=True),
        ],
        [
            sorted(records_dpt, key=lambda x: x[0]),
            sorted(location.build_geoname_set(cnx, geodata, dpt_code=None)),
            partial(location.alignment_geo_data, repeatable=True),
        ],
        [
            sorted(records_topographic, key=lambda x: x[0]),
            sorted(location.build_topographic_geoname_set(cnx, geodata)),
            partial(location.alignment_geo_data_topographic, repeatable=True),
        ],
    ]

    for out, (test_data, geonames, aligner) in zip(
        ("records_fr", "records_dpt", "records_topographic"), fclass
    ):
        for row in geonames_align.cells_from_pairs(
            aligner(test_data, geonames), geonames, test_data
        ):
            _, _, _, _, FA_label, geonames_uri, geonames_label, *_, distance = row
            yield FA_label, geonames_uri, geonames_label, out, distance


def _load_saved_alignments(path):
    with open(path) as fobj:
        reader = csv.reader(fobj, delimiter="\t")
        return set((FA_label, geonames_uri) for FA_label, geonames_uri, *_ in reader)


def load_expected_alignments(datadir):
    print(f"load_expected_alignment from {datadir / 'expected_alignments.csv'}")
    return _load_saved_alignments(datadir / "expected_alignments.csv")


def load_unexpected_alignments(datadir):
    print(f"load_unexpected_alignment from {datadir / 'unexpected_alignments.csv'}")
    return _load_saved_alignments(datadir / "unexpected_alignments.csv")


def update_alignments(cnx, datadir):
    got_alignments = set(run_alignment(cnx, datadir / "sample.csv"))
    got_couples = set((FA_label, geonames_uri) for FA_label, geonames_uri, *_ in got_alignments)

    with open(datadir / "expected_alignments.csv") as fobj:
        reader = csv.reader(fobj, delimiter="\t")
        previous_expected_alignments = set(tuple(r) for r in reader)

    with open(datadir / "unexpected_alignments.csv") as fobj:
        reader = csv.reader(fobj, delimiter="\t")
        previous_unexpected_alignments = set(tuple(r) for r in reader)

    new_unexpected_alignments = previous_unexpected_alignments
    for row in previous_expected_alignments - got_alignments:
        FA_label, geonames_uri, *_ = row
        if (FA_label, geonames_uri) not in got_couples:
            new_unexpected_alignments.add(row)

    for row in set(new_unexpected_alignments):
        FA_label, geonames_uri, *_ = row
        if (FA_label, geonames_uri) in got_couples:
            new_unexpected_alignments.remove(row)

    with open(datadir / "unexpected_alignments.csv", "w") as fobj:
        writer = csv.writer(fobj, delimiter="\t")
        writer.writerows(sorted(new_unexpected_alignments))

    with open(datadir / "expected_alignments.csv", "w") as fobj:
        writer = csv.writer(fobj, delimiter="\t")
        writer.writerows(sorted(got_alignments))


def test_regression(cnx, datadir):
    print(f"compute {datadir / 'sample.csv'}")
    got_alignments = set(
        (FA_label, geonames_uri)
        for FA_label, geonames_uri, *_ in run_alignment(cnx, datadir / "sample.csv")
    )
    print(f"{len(got_alignments)} alignments found")

    expected_alignments = load_expected_alignments(datadir)
    unexpected_alignments = load_unexpected_alignments(datadir)

    ok_mark = "\033[32m" + "\u2713" + "\33[0m"

    found_alignment = expected_alignments.intersection(got_alignments)

    print(f"\n{len(found_alignment)} expected alignments found:\n")
    for item in sorted(list(found_alignment)):
        print(f"{ok_mark} {item}")

    ko_mark = "\033[91m" + "x" + "\033[0m"
    unwanted_alignments = got_alignments.intersection(unexpected_alignments)

    print(f"\n{len(unwanted_alignments)} unwanted alignments found:\n")
    for item in sorted(list(unwanted_alignments)):
        print(f"{ko_mark} {item}")

    new_mark = "\033[96m" + "\u002B" + "\033[0m"
    new_alignments = got_alignments - expected_alignments
    print(f"\n{len(new_alignments)} unexpected alignments found:\n")
    for item in sorted(list(new_alignments)):
        print(f"{new_mark} {item}")

    lost_mark = "\033[95m" + "\u02D7" + "\033[0m"
    lost_alignments = expected_alignments - got_alignments
    print(f"\n{len(lost_alignments)} expected alignments lost:\n")
    for item in sorted(list(lost_alignments)):
        print(f"{lost_mark} {item}")


if __name__ == "__main__":
    from argparse import ArgumentParser
    from cubicweb.utils import admincnx

    parser = ArgumentParser()
    parser.add_argument("APPID")
    parser.add_argument("-u", "--update-alignments", action="store_true")
    datadir = Path(__file__).parent / "data" / "alignments"
    parser.add_argument(
        "-d",
        "--datadir",
        default=datadir,
        help="datdir for test files with parent directory this file parent",
    )
    args = parser.parse_args()
    datadir = Path(__file__).parent / args.datadir
    with admincnx(args.APPID) as cnx:
        if args.update_alignments:
            print("Compute and update alignments")
            update_alignments(cnx, datadir)
        else:
            test_regression(cnx, datadir)

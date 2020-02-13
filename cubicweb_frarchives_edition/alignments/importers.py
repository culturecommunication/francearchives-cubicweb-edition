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

"""
:synopsis: Align FindingAids to
`GEONAMES> http://www.geonames.org/'_,
`BANO https://www.data.gouv.fr/fr/datasets/base-d-adresses-nationale-ouverte-bano/`_
"""


# standard library imports
import csv
import logging
import multiprocessing as mp
import os.path

from glob import glob
from itertools import chain

import shutil

# library specific imports
from cubicweb_frarchives_edition.alignments.bano_align import BanoAligner, BanoRecord

from cubicweb_frarchives_edition.alignments.geonames_align import GeonameAligner, GeonameRecord
from cubicweb_frarchives_edition.alignments.utils import split_up

from cubicweb_francearchives import admincnx


from cubicweb_francearchives.dataimport import sqlutil, FakeQueue


def align_findingaid(aligner, record, findingaid_chunk, config, log):
    lines = aligner.compute_findingaid_alignments(
        [findingaid for findingaid, _ in findingaid_chunk]
    )
    if lines:
        csv_path = "{}_{}.csv".format(id(findingaid_chunk), config["dbname"])
        with open(os.path.join(config["csv_dir"], csv_path), "w") as fp:
            writer = csv.writer(fp, delimiter="\t")
            writer.writerow(list(record.headers.keys()))
            writer.writerows(lines)


def findingaid_aligner(aligner, record, alignment_queue, config, log):
    with admincnx(config["appid"]) as cnx:
        _findingaid_aligner(cnx, aligner, record, alignment_queue, config, log)


def _findingaid_aligner(cnx, aligner, record, alignment_queue, config, log):
    aligner = aligner(cnx, log)
    while True:
        next_job = alignment_queue.get()
        # worker got None in the queue, job is finished
        if next_job is None:
            break
        findingaid_chunk = next_job
        try:
            align_findingaid(aligner, record, findingaid_chunk, config, log)
        except Exception:
            import traceback

            traceback.print_exc()
            print("failed to import")
            log.exception("failed to import")


class AlignImporter(object):
    dbname = ""

    def __init__(self, cnx, config, log=None):
        self.config = config
        self.config["dbname"] = self.dbname
        self.cnx = cnx
        self.sqlcursor = self.cnx.cnxset.cu
        self.log = log or logging.getLogger("align {}".format(self.dbname))

    def delete_existing_alignments(self):
        tables = (
            "entities",
            "created_by_relation",
            "owned_by_relation",
            "cw_source_relation",
            "is_relation",
            "is_instance_of_relation",
            "cw_externaluri",
            "same_as_relation",
        )
        with sqlutil.no_trigger(self.cnx, interactive=False, tables=tables):
            """create a temporary table with ExternalUri to be deleted"""
            self.sqlcursor.execute("CREATE TEMPORARY TABLE temp_delete (eid integer)")
            self.popupate_temp_delete()
            self.log.info("Delete existing alignments")
            sql = """
                WITH temp_update as (
                    SELECT loc.cw_eid FROM cw_locationauthority loc
                    JOIN same_as_relation same ON same.eid_from = loc.cw_eid
                    JOIN temp_delete ON same.eid_to = temp_delete.eid
                )
                UPDATE cw_locationauthority
                SET cw_latitude=NULL, cw_longitude=NULL
                FROM temp_update
                 WHERE temp_update.cw_eid = cw_locationauthority.cw_eid
            """
            self.sqlcursor.execute(sql)
            self.sqlcursor.execute(
                """
                DELETE FROM same_as_relation WHERE eid_to IN (
                    SELECT eid FROM temp_delete
                )"""
            )
            self.delete_sameas_entities()
            self.cnx.commit()

    def popupate_temp_delete(self):
        raise NotImplementedError

    def delete_sameas_entities(self):
        raise NotImplementedError

    def align(self):
        csv_dir = self.config["csv_dir"]
        # remove the directory and its content
        if os.path.exists(csv_dir):
            shutil.rmtree(csv_dir)
        os.makedirs(csv_dir)
        if not os.path.exists(csv_dir):
            os.makedirs(csv_dir)
        query = "Any X, S WHERE X is FindingAid, X stable_id S"
        services = self.config["services"]
        if services:
            query += ", X service SV, SV code IN ({})".format(
                ",".join('"%s"' % s.upper() for s in services)
            )
        findingaids = self.cnx.execute(query)
        if not findingaids:
            self.log.info("no findingaids found")
            return
        else:
            self.log.info("Found {} findingaids".format(findingaids.rowcount))
        if not self.config["nodrop"]:
            self.delete_existing_alignments()
        n = 1000
        nb_processes = max(mp.cpu_count() - 1, 1)
        if nb_processes == 1:
            fake_queue = FakeQueue(list(split_up(findingaids.rows, n)))
            fake_queue.insert(0, None)
            findingaid_aligner(self.aligner, self.record, fake_queue, self.config, self.log)
        else:
            nb_jobs = findingaids.rowcount // n + (findingaids.rowcount % n > 0)
            queue = mp.Queue(2 * nb_processes)
            workers = []
            for i in range(nb_processes):
                workers.append(
                    mp.Process(
                        target=findingaid_aligner,
                        args=(self.aligner, self.record, queue, self.config, self.log),
                    )
                )
            for w in workers:
                w.start()
            for idx, job in enumerate(chain(split_up(findingaids.rows, n), (None,) * nb_processes)):
                if job is not None:
                    print("pushing {}/{} job in queue".format(idx + 1, nb_jobs))
                queue.put(job)
            for w in workers:
                w.join()
        self.import_alignments()

    def import_alignments(self):
        csv_dir = self.config["csv_dir"]
        aligner = self.aligner(self.cnx, self.log)
        existing_alignment = aligner.compute_existing_alignment()
        new_align_global, to_remove_global = {}, {}
        for csvpath in glob(os.path.join(csv_dir, "*")):
            with open(csvpath) as f:
                new_alignment, to_remove_alignment = aligner.process_csv(f, existing_alignment)
                new_align_global.update(new_alignment)
                to_remove_global.update(to_remove_alignment)
        print(
            "import %r new alignments, remove %r alignments"
            % (len(new_align_global), len(to_remove_global))
        )
        override_alignments = self.config["force"]
        aligner.process_alignments(
            new_align_global, to_remove_global, override_alignments=override_alignments
        )


class GeonamesAlignImporter(AlignImporter):
    dbname = "geonames"
    aligner = GeonameAligner
    record = GeonameRecord

    def popupate_temp_delete(self):
        self.sqlcursor.execute(
            """
            INSERT INTO temp_delete (eid)
            SELECT ext.cw_eid
            FROM cw_externaluri ext
            LEFT JOIN sameas_history sh ON ext.cw_uri=sh.sameas_uri
            WHERE ext.cw_source='geoname' AND sh.sameas_uri is NULL"""
        )

    def delete_sameas_entities(self):
        self.sqlcursor.execute("SELECT delete_entities('cw_externaluri', 'temp_delete')")


class BanoAlignImporter(AlignImporter):
    dbname = "bano"
    aligner = BanoAligner
    record = BanoRecord

    def popupate_temp_delete(self):
        """temporary table with ExternalID to be deleted"""
        self.sqlcursor.execute(
            """
            INSERT INTO temp_delete (eid)
            SELECT cw_eid
            FROM cw_externalid ext
            LEFT JOIN sameas_history sh ON ext.cw_extid=sh.sameas_uri
            WHERE cw_source = 'bano' AND sh.sameas_uri is NULL"""
        )

    def delete_sameas_entities(self):
        self.sqlcursor.execute("SELECT delete_entities('cw_externalid', 'temp_delete')")

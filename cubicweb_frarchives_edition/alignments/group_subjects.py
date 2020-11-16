# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2020
# Contact http://www.logilab.fr -- mailto:contact@logilab.fr
#
# This software is governed by the CeCILL-C license under French law and
# abiding my the rules of distribution of free software. You can use,
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
this script groups LocationAutority which have the same normalized label:

 - lowcase
 - unaccent
 - punctuation removed

# see https://extranet.logilab.fr/ticket/69532653
"""

from datetime import datetime
import csv

import os.path as osp
from functools import partial
import tqdm

from cubicweb_frarchives_edition import update_suggest_es

NOW = datetime.now()

query = """
 WITH T as (
    SELECT
      sa.cw_eid,
      sa.cw_label as label,
      normalize_entry(sa.cw_label) as nlabel,
      sa.cw_creation_date as cd,
      count(fa.cw_eid) as fa,
      count(fac.cw_eid) as fac,
      count(rar.*) as content,
      count(fa.cw_eid) + count(fac.cw_eid) + count(rar.*) as indexcount,
      row_number() OVER (PARTITION BY normalize_entry(sa.cw_label) order by count(fa.cw_eid) + count(fac.cw_eid) + count(rar.*) DESC) rank
    FROM
      cw_subjectauthority sa
      LEFT OUTER JOIN cw_subject as index ON index.cw_authority=sa.cw_eid
      LEFT OUTER JOIN index_relation i ON i.eid_from = index.cw_eid
      LEFT OUTER JOIN cw_facomponent fac ON fac.cw_eid = i.eid_to
      LEFT OUTER JOIN cw_findingaid fa ON fa.cw_eid = i.eid_to
      LEFT OUTER join related_authority_relation rar on rar.eid_to = sa.cw_eid
      LEFT JOIN grouped_with_relation grr on sa.cw_eid=grr.eid_from
      WHERE grr.eid_from is NULL
    GROUP BY 1, sa.cw_label
  ) SELECT array_agg(ARRAY[T.cw_eid::text, T.label, T.indexcount::text])
    FROM T
    GROUP BY nlabel
    HAVING COUNT(nlabel)> 1
    ORDER BY nlabel;
"""  # noqa


def write_log(msg, log=None):
    if log:
        log.info(msg)
    else:
        print(msg)


def group_candidates(cnx, candidates, log):
    progress_bar = _tqdm(total=len(candidates))
    for items in candidates:
        eids = [item[0] for item in items]
        target = cnx.entity_from_eid(eids.pop(0))
        write_log("grouping {} with {}".format(target.absolute_url(), eids))
        target.group(eids)
        cnx.commit()
        try:
            progress_bar.update()
        except Exception:
            pass
        write_log("grouped")
        update_suggest_es(cnx, [target] + [cnx.entity_from_eid(eid) for eid in eids])
        # remove all cnx.transaction_data cache
        cnx.drop_entity_cache()


_tqdm = partial(tqdm.tqdm, disable=None)


def sort_subjects(items):
    """
    :param items list: list of items (eid, label, number of linked documents)

    authorities are orderder :

    - put all labels with the first LOWERCASE letter in the before last position
    - put all UPPERCASE labels in the last position
    """
    first_label = items[0][1]
    if first_label.isupper() or first_label[0].islower():
        items.sort(key=lambda x: x[1][0].islower())
        items.sort(key=lambda x: x[1].isupper())
    return items


def group_subject_authorities(cnx, dry_run=True, directory=None, log=None, limitdoc=10000):
    """
    group subject authorities

    :param Connection cnx: CubicWeb database connection
    :param boolean dry_run: is True do not group entities, juste write the result
    :param Logging log
    """
    rset = cnx.system_sql(query).fetchall()  # noqa
    write_log("\n-> found {} SubjectAuthorities candidates to group".format(len(rset)), log)
    candidates = [items[0] for items in rset]
    filepath = "subjects_togroup_{}{}{:02d}.csv".format(NOW.year, NOW.day, NOW.month)
    if directory:
        filepath = osp.join(directory, filepath)
    print('\n-> write subject to group in "{}"'.format(osp.abspath(filepath)))
    rejected = []
    processed_cadidates = []
    with open(filepath, "w") as fp:
        writer = csv.writer(fp)
        for idx, items in enumerate(candidates):
            row = []
            items = sort_subjects(items)
            first = items[0]
            for i, (eid, label, nbdocs) in enumerate(items):
                url = "{}subject/{}".format(cnx.base_url(), eid)
                data = "{label} ({url}) {nb}".format(label=label, url=url, nb=nbdocs)
                if i and int(nbdocs) > limitdoc:
                    first_url = "{}subject/{}".format(cnx.base_url(), first[0])
                    rejected.append((first_url, data))
                row.append(data)
            writer.writerow(row)
            processed_cadidates.append(items)
    if rejected:
        filepath = "subjects_togroup_{}{}{:02d}_rejected.csv".format(NOW.year, NOW.day, NOW.month)
        if directory:
            filepath = osp.join(directory, filepath)
            print('\n-> write subject to group in "{}"'.format(osp.abspath(filepath)))
        with open(filepath, "w") as fp:
            writer = csv.writer(fp)
            for row in rejected:
                writer.writerow(row)
    if not dry_run:
        write_log("\n-> group subjects.")
        group_candidates(cnx, processed_cadidates, log)


if __name__ == "__main__":
    group_subject_authorities(cnx)  # noqa

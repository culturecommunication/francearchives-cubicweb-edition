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
from contextlib import contextmanager

from jinja2 import Environment, PackageLoader

import logging

import psycopg2

from cubicweb_francearchives import admincnx


class LoggedCursor(object):
    def __init__(self, crs):
        self.crs = crs

    def __getattr__(self, attr):
        return getattr(self.crs, attr)

    def execute(self, query, args=None):
        logging.debug("executing %s (args=%s)", query, args)
        return self.crs.execute(query, args)


@contextmanager
def transaction(cnx):
    crs = cnx.cursor()
    try:
        yield LoggedCursor(crs)
    except Exception as exc:
        print("err", exc)
        cnx.rollback()
        raise
    cnx.commit()


def table_exists(dbparams, tablename):
    with transaction(psycopg2.connect(**dbparams)) as crs:
        try:
            query = "SELECT * FROM {0} LIMIT 1".format(tablename)
            crs.execute(query)
            crs.fetchall()
        except psycopg2.ProgrammingError:
            return False
        else:
            return True


def load_geonames_tables(appid, dbparams, allcountries_path, altnames_path, table_owner=None):
    """Create and populate Geonames table.

    :param Connection cnx: connection to CubicWeb database
    :param str allcountries_path: path to allCountries.txt file
    :param str altnames_path: path to alternateNames.txt file
    """

    env = Environment(loader=PackageLoader("cubicweb_frarchives_edition", "alignments/templates"),)
    env.filters["sqlstr"] = lambda x: "'{}'".format(x)  # noqa
    geonames_template = env.get_template("geonames.sql")
    geonames_sqlcode = geonames_template.render(
        allcountries_path=allcountries_path, altnames_path=altnames_path, owner=table_owner
    )
    with transaction(psycopg2.connect(**dbparams)) as crs:
        crs.execute(geonames_sqlcode)
    with admincnx(appid) as cnx:
        for tablename in ("geonames", "geonames_altnames"):
            rowcount = cnx.system_sql(
                "SELECT count(*) FROM {0} LIMIT 1".format(tablename)
            ).fetchall()[0][0]
            print("\n-> {0} rows created in {1} table".format(rowcount, tablename))


def load_bano_tables(appid, dbparams, path, table_owner=None):
    """Create and populate BANO table.

    :param Connection cnx: connection to CubicWeb database
    :param str path: path to data file
    """
    env = Environment(loader=PackageLoader("cubicweb_frarchives_edition", "alignments/templates"),)
    template = env.get_template("bano_whitelisted.sql")
    with transaction(psycopg2.connect(**dbparams)) as crs:
        crs.execute(template.render(path=path, owner=table_owner))
    with admincnx(appid) as cnx:
        for tablename in ("bano_whitelisted",):
            rowcount = cnx.system_sql(
                "SELECT count(*) FROM {0} LIMIT 1".format(tablename)
            ).fetchall()[0][0]
        print("\n-> {0} rows created in {1} table".format(rowcount, tablename))

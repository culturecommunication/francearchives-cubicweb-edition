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
from cubicweb_frarchives_edition import GEONAMES_RE
from cubicweb_frarchives_edition.alignments.location import (
    code_dpt,
    code_regions,
    country_code_to_name
)


def create_label(cnx, url):
    """Create ExternalUri label.

    :param Connection cnx: CubicWeb database connection
    :param str url: GeoNames URL

    :return: label
    :rtype: str
    """
    if u'geonames' in url.lower():
        return _create_geonames_label(cnx, url)
    else:
        return u''


def _create_geonames_label(cnx, url):
    """Create GeoNames label ('ville (region, departement)').

    :param Connection cnx: CubicWeb database connection
    :param str url: GeoNames URL

    :returns: label
    :rtype: str
    """
    # GeoNames URL is either e.g. https://www.geonames.org/2988507/paris.html
    # or e.g. https://www.geonames.org/2988507
    match = GEONAMES_RE.search(url)
    if not match:
        return u''
    geonameid = match.group(1)
    res = cnx.system_sql(
        '''
        SELECT name, country_code, admin1_code, admin2_code
        FROM geonames WHERE geonameid = %(gid)s
        ''', {'gid': geonameid}
    ).fetchall()
    if not res:
        return u''
    label, country_code, admin1_code, admin2_code = res[0]
    if country_code == u'FR':
        admin1_name = code_regions.get(admin1_code, "")
        admin2_name = code_dpt.get(admin2_code, "")
        if admin1_name or admin2_name:
            label = u'{} ({})'.format(
                label, ', '.join(v for v in (admin1_name, admin2_name) if v)
            )
    else:
        # for other countries only retrieve the country name
        res = cnx.system_sql(
            '''
            SELECT geonameid FROM geonames WHERE country_code=%s
            AND fcode IN ('PCL','PCLI')
            ''', (country_code,)
        ).fetchall()
        if res:
            country_name = country_code_to_name.get(res[0][0], '')
            if country_name:
                label = u'{} ({})'.format(label, country_name)
    return label


def includeme(config):
    config.include('.routes')
    config.include('.pviews')

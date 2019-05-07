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
import re
import os.path as osp
import os

import logging
import zipfile

from cubicweb_frarchives_edition.api import jsonapi_error, JSONBadRequest
from cubicweb_francearchives.dataimport import RELFILES_DIR

LOG = logging.getLogger(__name__)


def bad_request(error):
    return JSONBadRequest(*[
        jsonapi_error(status=422,
                      details=error, pointer='file')])


def xml_re_match(startswith, filename):
    return re.match(r'%s_.*.xml' % startswith, filename)


def pdf_re_match(startswith, filename):
    return re.match(r'%s_.*.pdf' % startswith, filename)


def check_zipfiles(zipf, service):
    filenames = zipf.namelist()
    res = {'wrong_files': [], 'empty_files': [],
           'empty': False, 'missing_csv': False,
           'missing_xml': False}
    # directories always end this '/'
    directories_only = all(f.endswith('/') for f in filenames)
    if not filenames or directories_only:
        res['empty'] = True
        return res
    params = {'code': service}
    # PDF
    pdf_dir = '{code}/PDF/'.format(**params)
    metafile = osp.join(service, 'PDF', 'metadata.csv')
    contains_pdf = pdf_dir in filenames
    if contains_pdf and metafile not in filenames:
        res['missing_csv'] = True
    service_file = r'{code}\/{code}'.format(**params)
    pdf_file = r'{code}\/PDF\/{code}'.format(**params)
    if not contains_pdf and not any(f for f in filenames if f.endswith('xml')):
        res['missing_xml'] = True
        return res
    pdf_file = r'{code}\/PDF\/{code}'.format(**params)
    for info in zipf.infolist():
        filename = info.filename
        # the filename is a directory
        if filename.endswith('/'):
            continue
        # skip all files in RELFILES_DIR
        if RELFILES_DIR in filename:
            continue
        # any xml file must start with the service code
        if xml_re_match(service_file, filename):
            continue
        # if PDF directory is present the file must be:
        #   - a metadatafile or
        #   - a pdf file starting with the service code
        if contains_pdf and filename == metafile:
            continue
        if contains_pdf and not pdf_re_match(pdf_file, filename):
            res['wrong_files'].append(filename)
        if not info.file_size:
            res['empty_files'].append(filename)
    return res


def process_faimport_zip(cnx, fileobj):
    """ the zip file may contain :
    <code_service directory>
    |__<code_service>_XXX.xml
    |__<code_service>_XXX.xml

    or
    |__PDF
        |__ <code_service>_XXX.pdf
        |__ <code_service>_XXX.pdf
        |__ ...
        |__ metadata.csv
    or
    |__RELFILES
        |__file1.xxx
        |__ ...
        |__fileN.xxx

    """
    _ = cnx._
    if fileobj is None:
        raise bad_request(_('This file is empty'))
    if not zipfile.is_zipfile(fileobj.file):
        raise bad_request(_('This file in not a zip file'))
    # check the related service
    code, ext = osp.splitext(fileobj.filename)
    rset = cnx.find('Service', code=code)
    if not rset:
        raise bad_request(
            _(u'Service "{}" does not exist. '
              u'Check the ziped file name "{}" or create a '
              u'new service before uploading '
              u'the file again').format(code, fileobj.filename))
    # check the zip file structure
    zf = zipfile.ZipFile(fileobj.file, mode='r')
    zip_errors = check_zipfiles(zf, code)
    if any(zip_errors.values()):
        errors = []
        if zip_errors['empty']:
            error = _('This archive contains no files.')
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        empty_files = zip_errors['empty_files']
        if empty_files:
            error = _('Following files are empty : {}'.
                      format(', '.join(empty_files)))
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        wrong_files = zip_errors['wrong_files']
        if wrong_files:
            error = _('Following files does not start with "{}" : {}'.
                      format(code, ', '.join(wrong_files)))
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        if zip_errors['missing_csv']:
            error = _('PDF/metadata.csv is missing from zip')
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        if zip_errors['missing_xml']:
            error = _('no XML files found in zip')
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        raise JSONBadRequest(*errors)
    # catch errors ?
    ead_dir = cnx.vreg.config.get('ead-services-dir')
    if ead_dir is None:
        raise bad_request(_('missing "ead-services-dir" parameter in all-in-one.conf'))
    zf.extractall(ead_dir)
    return [osp.join(ead_dir, filepath) for filepath in zf.namelist()
            if osp.splitext(filepath.lower())[1] in ('.pdf', '.xml')]


def check_csv_zipfiles(zf):
    errors = {'empty_files': [], 'empty': False,
              'missing_csv': False}
    csv_files = [f for f in zf.namelist() if osp.splitext(f.lower())[1] == '.csv']
    if not csv_files:
        errors['empty'] = True
    for info in zf.infolist():
        filename = info.filename
        code, ext = osp.splitext(filename)
        if ext == '.csv' and not info.file_size:
            errors['empty_files'].append(info.filename)
    return errors


def process_csvimport_zip(cnx, fileobj):
    """ the zip file may contain :
    <directory>
    |__data1.csv
    |__....csv
    |__dataX.csv
    |__ metadata.csv (optional)
    """
    _ = cnx._
    if fileobj is None:
        raise bad_request(_('This file is empty'))
    if not zipfile.is_zipfile(fileobj.file):
        raise bad_request(_('This file in not a zip file'))
    # check the zip file structure
    zf = zipfile.ZipFile(fileobj.file, mode='r')
    # check the related service
    code, ext = osp.splitext(fileobj.filename)
    rset = cnx.find('Service', code=code)
    if not rset:
        raise bad_request(
            _(u'Service "{}" does not exist. '
              u'Check the ziped file name "{}" or create a '
              u'new service before uploading '
              u'the file again').format(code, fileobj.filename))

    files_errors = check_csv_zipfiles(zf)
    if any(files_errors.values()):
        errors = []
        if files_errors['empty']:
            error = _('This archive contains no csv files.')
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        empty_files = files_errors['empty_files']
        if empty_files:
            error = _('Following files are empty : {}'.
                      format(', '.join(empty_files)))
            errors.append(jsonapi_error(status=422,
                          details=error, pointer='file'))
        raise JSONBadRequest(*errors)
    ead_dir = cnx.vreg.config.get('ead-services-dir')
    if ead_dir is None:
        raise bad_request(_('missing "ead-services-dir" parameter in all-in-one.conf'))
    zf.extractall(ead_dir)
    csv_files = [f for f in zf.namelist() if osp.splitext(f.lower())[1] == '.csv']
    res = {'filepaths': [], 'metadata': None}
    for filepath in csv_files:
        if filepath.endswith('metadata.csv'):
            res['metadata'] = osp.join(ead_dir, filepath)
        else:
            res['filepaths'].append(osp.join(ead_dir, filepath))
    return res


def process_faimport_xml(cnx, fileobj, servicecode):
    _ = cnx._
    ead_dir = cnx.vreg.config.get('ead-services-dir')
    if ead_dir is None:
        raise bad_request(_('missing "ead-services-dir" parameter in all-in-one.conf'))
    directory = osp.join(ead_dir, servicecode)
    filepath = osp.join(ead_dir, servicecode, fileobj.filename)
    if not osp.exists(directory):
        os.makedirs(directory)
    with open(filepath, 'wb') as f:
        f.write(fileobj.value)
    return [filepath]

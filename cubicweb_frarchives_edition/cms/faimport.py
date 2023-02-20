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
from io import StringIO

import re
import os.path as osp
import os

import zipfile

from cubicweb_francearchives.dataimport import RELFILES_DIR
from cubicweb_francearchives.dataimport.csv_nomina import check_document_fieldnames

from cubicweb_frarchives_edition.api import jsonapi_error, JSONBadRequest
from cubicweb_frarchives_edition.tasks.qualify_authorities import FIELDNAMES, KIBANA_FIELDNAMES


def bad_request(error):
    return JSONBadRequest(*[jsonapi_error(status=422, details=error, pointer="file")])


def xml_re_match(startswith, filename):
    return re.match(r"%s_.*.xml" % startswith, filename)


def pdf_re_match(startswith, filename):
    return re.match(r"%s_.*.pdf" % startswith, filename)


def check_zipfiles(zipf, service):
    filenames = zipf.namelist()
    res = {
        "wrong_files": [],
        "empty_files": [],
        "empty": False,
        "missing_csv": False,
        "missing_xml": False,
    }
    # directories always end this '/'
    directories_only = all(f.endswith("/") for f in filenames)
    if not filenames or directories_only:
        res["empty"] = True
        return res
    params = {"code": service}
    # PDF
    pdf_dir = "{code}/PDF/".format(**params)
    metafile = osp.join(service, "PDF", "metadata.csv")
    contains_pdf = pdf_dir in filenames
    if contains_pdf and metafile not in filenames:
        res["missing_csv"] = True
    service_file = r"{code}\/{code}".format(**params)
    pdf_file = r"{code}\/PDF\/{code}".format(**params)
    if not contains_pdf and not any(f for f in filenames if f.endswith("xml")):
        res["missing_xml"] = True
        return res
    pdf_file = r"{code}\/PDF\/{code}".format(**params)
    for info in zipf.infolist():
        filename = info.filename
        # the filename is a directory
        if filename.endswith("/"):
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
            res["wrong_files"].append(filename)
        if not info.file_size:
            res["empty_files"].append(filename)
    return res


def get_dir_or_raise_bad_request(cnx, config_dir_name):
    doc_dir = cnx.vreg.config.get(config_dir_name)
    if doc_dir is None:
        raise bad_request(cnx._(f'missing "{config_dir_name}" parameter in all-in-one.conf'))
    return doc_dir


def process_faimport_zip(cnx, fileobj, write_zip_func):
    """the zip file may contain :
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
        raise bad_request(_("This file is empty"))
    if not zipfile.is_zipfile(fileobj.file):
        raise bad_request(_("This file in not a zip file"))
    # check the related service
    code, ext = osp.splitext(fileobj.filename)
    rset = cnx.find("Service", code=code)
    if not rset:
        raise bad_request(
            _(
                'Service "{}" does not exist. '
                'Check the ziped file name "{}" or create a '
                "new service before uploading "
                "the file again"
            ).format(code, fileobj.filename)
        )
    # check the zip file structure
    zf = zipfile.ZipFile(fileobj.file, mode="r")
    zip_errors = check_zipfiles(zf, code)
    if any(zip_errors.values()):
        errors = []
        if zip_errors["empty"]:
            error = _("This archive contains no files.")
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        empty_files = zip_errors["empty_files"]
        if empty_files:
            error = _("Following files are empty : {}".format(", ".join(empty_files)))
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        wrong_files = zip_errors["wrong_files"]
        if wrong_files:
            error = _(
                'Following files does not start with "{}" : {}'.format(code, ", ".join(wrong_files))
            )
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        if zip_errors["missing_csv"]:
            error = _("PDF/metadata.csv is missing from zip")
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        if zip_errors["missing_xml"]:
            error = _("no XML files found in zip")
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        raise JSONBadRequest(*errors)
    # catch errors ?
    ead_dir = get_dir_or_raise_bad_request(cnx, "ead-services-dir")
    return write_zip_func(zf, ead_dir, exts=(".pdf", ".xml", ".csv"))


def check_csv_zipfiles(zf):
    errors = {"empty_files": [], "empty": False, "missing_csv": False}
    csv_files = [f for f in zf.namelist() if osp.splitext(f.lower())[1] == ".csv"]
    if not csv_files:
        errors["empty"] = True
    for info in zf.infolist():
        filename = info.filename
        code, ext = osp.splitext(filename)
        if ext == ".csv" and not info.file_size:
            errors["empty_files"].append(info.filename)
    return errors


def process_csvimport_zip(cnx, fileobj, write_zip_func):
    """the zip file may contain :
    <directory>
    |__data1.csv
    |__....csv
    |__dataX.csv
    |__ metadata.csv (optional)
    """
    _ = cnx._
    if fileobj is None:
        raise bad_request(_("This file is empty"))
    if not zipfile.is_zipfile(fileobj.file):
        raise bad_request(_("This file in not a zip file"))
    # check the zip file structure
    zf = zipfile.ZipFile(fileobj.file, mode="r")
    # check the related service
    code, ext = osp.splitext(fileobj.filename)
    rset = cnx.find("Service", code=code)
    if not rset:
        raise bad_request(
            _(
                'Service "{}" does not exist. '
                'Check the ziped file name "{}" or create a '
                "new service before uploading "
                "the file again"
            ).format(code, fileobj.filename)
        )

    files_errors = check_csv_zipfiles(zf)
    if any(files_errors.values()):
        errors = []
        if files_errors["empty"]:
            error = _("This archive contains no csv files.")
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        empty_files = files_errors["empty_files"]
        if empty_files:
            error = _("Following files are empty : {}".format(", ".join(empty_files)))
            errors.append(jsonapi_error(status=422, details=error, pointer="file"))
        raise JSONBadRequest(*errors)
    ead_dir = get_dir_or_raise_bad_request(cnx, "ead-services-dir")
    csv_files = write_zip_func(zf, ead_dir, exts=(".csv",))
    res = {"filepaths": [], "metadata": None}
    for filepath in csv_files:
        if filepath.endswith("metadata.csv"):
            res["metadata"] = filepath
        else:
            res["filepaths"].append(filepath)
    return res


def process_faimport_xml(cnx, fileobj, servicecode, write_func):
    ead_dir = get_dir_or_raise_bad_request(cnx, "ead-services-dir")
    return write_func(fileobj.filename, fileobj.value, subdirectories=[ead_dir, servicecode])


def get_eac_dir(cnx):
    eac_dir = cnx.vreg.config.get("eac-services-dir")
    if eac_dir is None:
        raise bad_request(cnx._('missing "eac-services-dir" parameter in all-in-one.conf'))
    if not osp.exists(eac_dir):
        os.makedirs(eac_dir)
    return eac_dir


def process_authorityrecords_zip(cnx, fileobj, write_zip_func):
    """store zip files directly in the eac_dir"""
    eac_dir = get_eac_dir(cnx)
    _ = cnx._
    if fileobj is None:
        raise bad_request(_("This file is empty"))
    if not zipfile.is_zipfile(fileobj.file):
        raise bad_request(_("This file in not a zip file"))
    zf = zipfile.ZipFile(fileobj.file, mode="r")
    return write_zip_func(zf, eac_dir, exts=(".xml",))


def process_authorityrecord_xml(cnx, fileobj, servicecode, write_func):
    """store the file in the eac_dir sub-directory (servicecode)"""
    eac_dir = get_dir_or_raise_bad_request(cnx, "eac-services-dir")
    return write_func(fileobj.filename, fileobj.value, subdirectories=[eac_dir, servicecode])


def check_quality_csv(cnx, fileobj, st):
    try:
        stream = StringIO(fileobj.value.decode())
    except Exception as exception:
        raise bad_request(cnx._(f'Unable to read "{fileobj.filename}": {exception}'))
    try:
        headers = csv.DictReader(stream, delimiter="\t").fieldnames
    except Exception as exception:
        raise bad_request(cnx._(f'Unable to process "{fileobj.filename}": {exception}'))
    if headers is None:
        raise bad_request(cnx._(f'Unable to process "{fileobj.filename}": no headers found'))
    for variantes in (FIELDNAMES, KIBANA_FIELDNAMES):
        if not set(variantes.keys()).difference(headers):
            return variantes
    raise bad_request(
        cnx._(f'''"{fileobj.filename}" file contains invalid headers "{", ".join(headers)}"''')
    )  # noqa


def process_nomina_csv(cnx, fileobj, servicecode, write_func):
    """store the file in the nomina_dir sub-directory (servicecode)"""
    nomina_dir = get_dir_or_raise_bad_request(cnx, "nomina-services-dir")
    return write_func(fileobj.filename, fileobj.value, subdirectories=[nomina_dir, servicecode])


def validate_import_nomina_csv(cnx, fileobj, service_code, doctype, delimiter, write_func):
    """store the file in the nomina_dir sub-directory (servicecode)"""
    try:
        stream = StringIO(fileobj.value.decode())
    except Exception as exception:
        raise bad_request(cnx._(f'Unable to read "{fileobj.filename}": {exception}'))
    try:
        fieldnames = csv.DictReader(stream, delimiter=delimiter).fieldnames
    except Exception as exception:
        raise bad_request(cnx._(f'Unable to process "{fileobj.filename}": {exception}'))
    if fieldnames is None:
        raise bad_request(cnx._(f'Unable to process "{fileobj.filename}": no fieldnames found'))
    try:
        errors = check_document_fieldnames(cnx, doctype, fieldnames)
    except Exception as err:
        raise bad_request(cnx._('Unable to process "%s": %s') % (fileobj.filename, err))
    if errors:
        raise bad_request(
            cnx._('Unable to process "%s": %s') % (fileobj.filename, "  ".join(errors))
        )
    return process_nomina_csv(cnx, fileobj, service_code, write_func)

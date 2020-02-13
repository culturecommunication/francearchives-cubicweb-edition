# -*- coding: utf-8 -*-
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


# standard library imports
import io
import csv
import logging
import zipfile
from uuid import uuid4
from tempfile import NamedTemporaryFile
from functools import wraps

# third party imports

# CubicWeb specific imports
from cubicweb import Binary

# library specific imports


def zip_files(files, archive=""):
    """Create Zip archive. If archive is not set,
    a named temporary file is created.

    :param str archive: Zip archive
    :param list files: list of filename-arcname tuples

    :returns: Zip archive
    :rtype: str
    """
    log = logging.getLogger("rq.task")
    if not archive:
        fp = NamedTemporaryFile(delete=False)
        archive = fp.name
        fp.close
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fp:
        for filename, arcname in files:
            try:
                fp.write(filename, arcname=arcname)
            except Exception:
                log.warning("failed to add %s to Zip archive %s", filename, archive)
        log.info("Zip archive contains %r files", len(fp.namelist()))
    return archive


def serve(data_format):
    def decorator(func):
        @wraps(func)
        def wrapper(cnx, eid, title, *args, **kwargs):
            cw_binary = func(cnx, eid, title, *args, **kwargs)
            output_file = cnx.create_entity(
                "File",
                data=cw_binary,
                data_format=data_format,
                data_name=title,
                title=title,
                uuid=str(uuid4().hex),
            )
            rq_task = cnx.entity_from_eid(eid)
            rq_task.cw_set(output_file=output_file)
            cnx.commit()
            return output_file

        return wrapper

    return decorator


@serve("application/zip")
def serve_zip(cnx, eid, title, path):
    """Serve Zip archive.

    :param Connection cnx: CubicWeb database connection
    :param int eid: RqTask eid
    :param str title: output file title
    :param str path: path to Zip archive

    :returns: output file
    :rtype: File
    """
    cw_binary = Binary()
    cw_binary.write(open(path, "rb").read())
    return cw_binary


@serve("text/csv")
def serve_csv(cnx, eid, title, rows, delimiter=","):
    """Serve CSV file.

    :param Connection cnx: CubicWeb database connection
    :param in eid: RqTask eid
    :param str title: output file title
    :param list rows: list of rows
    :param str delimiter: delimiter

    :returns: output file
    :rtype: File
    """
    return write_binary_csv(rows, delimiter=delimiter)


def _encode(rows):
    """Encode rows.

    :returns: list of encoded rows
    :rtype: list
    """
    log = logging.getLogger("rq.task")
    encoded = []
    for row in rows:
        try:
            encoded.append(
                [column.encode("utf-8") if isinstance(column, str) else column for column in row]
            )
        except UnicodeEncodeError as exception:
            log.debug("failed to encode row : UnicodeEncodeError (%s)", exception)
            continue
    return encoded


def write_binary_csv(rows, delimiter=","):
    """Write rows to Binary.

    :param list rows: rows
    """
    cw_binary = Binary()
    fp = io.TextIOWrapper(cw_binary, encoding="utf-8", newline="")
    writer = csv.writer(fp, delimiter=delimiter)
    writer.writerows(rows)
    # https://bugs.python.org/issue21363
    fp.detach()
    return cw_binary


def write_csv(rows, headers=[], path="", delimiter=","):
    """Write rows to CSV file. If path is not set,
    a named temporary file is created.

    :param list rows: rows
    :param tuple headers: column headers
    :param str path: CSV file path

    :returns: path
    :rtype: str
    """
    if not path:
        # create named temporary file
        fp = NamedTemporaryFile(delete=False)
        path = fp.name
        fp.close()
    with open(path, "w") as fp:
        writer = csv.writer(fp, delimiter=delimiter)
        if headers:
            rows.insert(0, headers)
        writer.writerows(rows)
    return path

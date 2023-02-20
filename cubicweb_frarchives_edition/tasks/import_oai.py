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

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import email.utils

import logging
import os
import rq

from uuid import uuid4

from logilab.mtconverter import xml_escape, html_unescape
from cubicweb import Binary

from cubicweb_francearchives import POSTGRESQL_SUPERUSER
from cubicweb_francearchives.dataimport.oai import import_delta
from cubicweb_francearchives.utils import remove_html_tags
from cubicweb_frarchives_edition.rq import rqjob

from cubicweb_frarchives_edition.tasks.compute_alignments import compute_alignments
from cubicweb_frarchives_edition.tasks.import_csv_nomina import import_csv_nomina
from cubicweb_frarchives_edition.tasks.publish import publish_findingaid


def import_oai_ead(
    cnx,
    repo_eid,
    auto_dedupe=True,
    context_service=True,
    ignore_last_import=False,
    auto_import=False,
    dry_run=False,
    records_limit=None,
    publish=False,
):
    log = logging.getLogger("rq.task")
    log.info("Start the task with  %r", "superuser" if POSTGRESQL_SUPERUSER else "no superuser")
    log.info(
        "Harvest all: ignore last import date"
        if ignore_last_import
        else "Harvest data from the last import"
    )
    oairepo = cnx.entity_from_eid(repo_eid)
    job = rq.get_current_job()
    taskeid = int(job.id)
    index_policy = {
        "autodedupe_authorities": "{context}/{normalize}".format(
            context="service" if context_service else "global",
            normalize="normalize" if auto_dedupe else "strict",
        )
    }
    import_delta(
        cnx,
        repo_eid,
        ignore_last_import,
        index_policy=index_policy,
        dry_run=dry_run,
        records_limit=records_limit,
        log=log,
        rqtask_eid=taskeid,
    )
    # retrieve task's logs and send them
    rqtask = cnx.entity_from_eid(taskeid)
    send_logs(cnx, rqtask)
    # process task
    oaitask = oairepo.tasks[-1]
    wf = oaitask.cw_adapt_to("IWorkflowable")
    start = oaitask.creation_date
    stop = wf.latest_trinfo().creation_date
    # set last_successful_import on OAIRepository on the task start date
    if wf.state == "wfs_oaiimport_completed":
        oairepo.cw_set(last_successful_import=start)
        cnx.commit()
    # ead
    rset = cnx.execute(
        """Any X WHERE X is FindingAid, X modification_date >= %(start)s,
           X modification_date <= %(stop)s""",
        {"start": start, "stop": stop},
    )
    imported_findingaids = [fa for fa, in rset]
    log.info(f"{len(imported_findingaids)} imported findingaid(s)")
    if imported_findingaids:
        rqtask.cw_set(fatask_findingaid=imported_findingaids)
        cnx.commit()
    # remove published findingaid that was deleted in current task
    cnx.system_sql(
        "SELECT published.unpublish_findingaid(fa.cw_eid) "
        "FROM published.cw_findingaid fa LEFT OUTER JOIN entities e ON "
        "(e.eid=fa.cw_eid) WHERE e.eid IS NULL"
    )
    # insert intial state for all FindingAid with no current state
    rset = cnx.execute(
        'Any S WHERE S is State, S state_of WF, X default_workflow WF, X name "FindingAid", '
        "WF initial_state S"
    )
    cnx.system_sql(
        "INSERT INTO in_state_relation (eid_from, eid_to) "
        "SELECT cw_eid, %(eid_to)s FROM cw_findingaid WHERE "
        "NOT EXISTS (SELECT 1 FROM in_state_relation i "
        "WHERE i.eid_from = cw_eid)",
        {"eid_to": rset[0][0]},
    )
    if not imported_findingaids:
        return
    # publish harvested findingaids
    if publish:
        publish_task = cnx.create_entity(
            "RqTask", name="publish_findingaid", title="publish IR harvested in {}".format(job.id)
        )
        publish_task.cw_adapt_to("IRqJob").enqueue(publish_findingaid, rqtask.eid)
        rqtask.cw_set(subtasks=publish_task.eid)
        cnx.commit()
    # launch compute alignment
    aligntask = cnx.create_entity(
        "RqTask",
        name="compute_alignments",
        title="automatic compute_alignments for {}".format(job.id),
    )
    aligntask.cw_adapt_to("IRqJob").enqueue(compute_alignments, imported_findingaids, auto_import)
    rqtask.cw_set(subtasks=aligntask.eid)
    cnx.commit()


def import_oai_nomina(
    cnx, repo_eid, ignore_last_import=False, dry_run=False, records_limit=None, csv_rows_limit=None
):
    log = logging.getLogger("rq.task")
    log.info("Start the task with  %r", "superuser" if POSTGRESQL_SUPERUSER else "no superuser")
    log.info(
        "Harvest all: ignore last import date"
        if ignore_last_import
        else "Harvest data from the last import"
    )
    oairepo = cnx.entity_from_eid(repo_eid)
    job = rq.get_current_job()
    taskeid = int(job.id)
    # harvest oai
    filepaths = import_delta(
        cnx,
        repo_eid,
        ignore_last_import,
        index_policy=None,
        dry_run=dry_run,
        records_limit=records_limit,
        csv_rows_limit=csv_rows_limit or 100000,
        log=log,
        rqtask_eid=taskeid,
    )
    # retrieve task's logs and send them
    rqtask = cnx.entity_from_eid(taskeid)
    # process task
    send_logs(cnx, rqtask)
    # process task
    oaitask = oairepo.tasks[-1]
    wf = oaitask.cw_adapt_to("IWorkflowable")
    # set last_successful_import on OAIRepository
    if dry_run is False and records_limit is None and wf.state == "wfs_oaiimport_completed":
        oairepo.cw_set(last_successful_import=wf.latest_trinfo().creation_date)
        cnx.commit()
    # create output files
    output_files = []
    cnx.transaction_data["fs_importing"] = True
    if filepaths:
        for filepath in filepaths:
            _, basepath = os.path.split(filepath)
            cwfile = cnx.create_entity(
                "File",
                data=Binary(filepath.encode("utf-8")),
                data_format="text/csv",
                data_name=basepath,
                title=basepath,
                uuid=str(uuid4().hex),
            )
            output_files.append(cwfile)
    if not output_files:
        return
    rqtask.cw_set(output_file=output_files)
    cnx.commit()
    if not dry_run:
        entity = cnx.entity_from_eid(taskeid)
        for cwfile in rqtask.output_file:
            filepath = cnx.execute("Any FSPATH(D) WHERE X eid %(e)s, X data D", {"e": cwfile.eid})[
                0
            ][0].getvalue()
            import_csv_task = cnx.create_entity(
                "RqTask",
                name="import_csv_nomina",
                title=f'import Nomina Records from "{cwfile.data_name}" (for job {job.id})',
            )
            import_csv_task.cw_adapt_to("IRqJob").enqueue(
                import_csv_nomina, filepath, oairepo.service[0].code, doctype="OAI"
            )
            entity.cw_set(subtasks=import_csv_task.eid)
            cnx.commit()


@rqjob
def import_oai(
    cnx,
    repo_eid,
    auto_dedupe=True,
    context_service=True,
    ignore_last_import=False,
    auto_import=False,
    dry_run=False,
    records_limit=None,
    csv_rows_limit=None,
    publish=False,
):
    oairepo = cnx.entity_from_eid(repo_eid)
    qs = oairepo.oai_params
    # nomina
    if qs.get("metadataPrefix") == ["nomina"]:
        import_oai_nomina(
            cnx,
            repo_eid,
            ignore_last_import,
            dry_run=dry_run,
            records_limit=records_limit,
            csv_rows_limit=csv_rows_limit,
        )
        return
    import_oai_ead(
        cnx,
        repo_eid,
        auto_dedupe=auto_dedupe,
        context_service=context_service,
        ignore_last_import=ignore_last_import,
        auto_import=auto_import,
        dry_run=dry_run,
        records_limit=records_limit,
        publish=publish,
    )


def build_email(cnx, rqtask, recipients, logs):
    """Build email

    :param Connection cnx: CubicWeb database connection
    :param RqTask: rqtask: redis task for l'import
    :param Str: logs recipients
    :param List: logs to be send
    """
    _ = cnx._
    oai_task = rqtask.oaiimport_task[0]
    oai_repo = oai_task.oai_repository[0]
    service = oai_repo.service[0]
    task_completed = oai_task.cw_adapt_to("IWorkflowable").state == "wfs_oaiimport_completed"
    content = [
        _(
            """Following errors/warnings occurred while harvesting {} ({}) OAI repository {}. For the complete log visit {}."""  # noqa
        ).format(service.dc_title(), service.code or "", oai_repo.url, rqtask.absolute_url())
    ]

    if task_completed:
        status = _("harvesting completed with errors/warnings")
    else:
        status = _("harvesting failed")
        content.append(status)
    content.append("\n".join("  ".join(line) for line in logs))
    subject = _(f"""[FranceArchives]: {service.dc_title()} ({service.code}) - {status}""")
    content = "\n\n".join(content)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["Date"] = f"{email.utils.formatdate()}"
    msg["From"] = cnx.vreg.config["sender-addr"]
    msg["To"] = recipients
    part1 = MIMEText(f"{remove_html_tags(content)}", "plain", "UTF-8")
    msg.attach(part1)
    html = content.replace("\n", "<br />")
    # attach html in the last position
    part2 = MIMEText(html, "html", "UTF-8")
    msg.attach(part2)
    return msg


def process_logs(cnx, logs):
    """
    Process RqTask logs

    :param Connection cnx: CubicWeb database connection
    :param Str: logs to be send

    """
    loglines = [line for line in xml_escape(logs).splitlines() if line]
    rows = []
    for line in loglines:
        line = line.strip()
        if not line:
            continue
        try:
            severity, date, time, info = line.split(None, 3)
            try:
                hour, time = time.split(",")
                date = "{} {}".format(date, hour)
            except Exception:
                pass
            if severity not in ("INFO", "DEBUG"):
                rows.append([date, severity, html_unescape(info)])
        except (ValueError, KeyError):
            pass
    return rows


def send_logs(cnx, rqtask):
    """ """
    recipients = cnx.vreg.config.get("admin-emails")
    if not recipients:
        # do not send emails
        return
    logs = rqtask.cw_adapt_to("IRqJob").log
    if not logs:
        return
    content = process_logs(cnx, logs)
    if content:
        recipients = cnx.vreg.config["admin-emails"]
        msg = build_email(cnx, rqtask, recipients, content)
        logger = logging.getLogger("logs_by_mail")
        try:
            logger.info(f"sending e-mail to {recipients}")
            cnx.vreg.config.sendmails([(msg, recipients)])
        except Exception as exception:
            logging.error(f"Email for {rqtask} could not be send to {recipients}: {exception}")
            logging.error(exception)

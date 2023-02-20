# -*- coding: utf-8 -*-
#
# Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2022
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

from pyramid.response import Response
from pyramid.view import view_config
from cubicweb import ValidationError
from cubicweb_francearchives.pviews.cwroutes import startup_view_factory


def display_view(request, vid):
    cwreq = request.cw_request
    viewsreg = cwreq.vreg["views"]
    view = viewsreg.select(vid, cwreq, rset=None)
    return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))


@view_config(route_name="captcha")
def captcha_view(request):
    """Default view for the 'captcha' route."""
    cwreq = request.cw_request
    viewsreg = cwreq.vreg["views"]
    view = viewsreg.select("captcha", cwreq, rset=None)
    return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))


@view_config(route_name="forgottenpassword_sendmail")
def sendmail_controllers(request):
    """Default view for the 'forgottenpassword_sendmail' route."""
    cwreq = request.cw_request
    viewsreg = cwreq.vreg["views"]
    view = viewsreg.select("forgottenpassword", cwreq, rset=None)
    # clean the old form
    for attr in ("_ctrl", "_sent"):
        cwreq.form.pop(attr, None)
    controller = cwreq.vreg["controllers"].select("forgottenpassword_sendmail", cwreq)
    try:
        data = controller.checked_data()
    except ValidationError as exc:
        cwreq.form["_ctrl"] = {"errors": exc.errors}
        return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))
    try:
        with cwreq.cnx.repo.internal_cnx() as cnx:
            cnx.call_service("forgotpwd_send_email", use_email=data["use_email"])
            cnx.commit()
    except ValidationError as exc:
        cwreq.form["_ctrl"] = {"errors": exc.errors}
        return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))
    except Exception as exc:
        cwreq.form["_ctrl"] = {"errors": exc}
        return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))
    # email sent with success
    cwreq.form["_sent"] = 1
    return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))


@view_config(route_name="forgottenpassword-requestconfirm")
def requestconfirm_controllers(request):
    """Default view for the 'forgottenpassword-requestconfirm' route."""
    cwreq = request.cw_request
    viewsreg = cwreq.vreg["views"]
    view = viewsreg.select("forgottenpasswordrequest", cwreq, rset=None)
    # clean the old form
    for attr in ("_ctrl", "_sent", "errors"):
        cwreq.form.pop(attr, None)
    controller = cwreq.vreg["controllers"].select("forgottenpassword-requestconfirm", cwreq)
    try:
        data = controller.checked_data()
    except ValidationError as exc:
        cwreq.form["_ctrl"] = {"errors": exc.errors}
        return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))
    try:
        with cwreq.cnx.repo.internal_cnx() as cnx:
            msg = cnx.call_service(
                "forgotpwd_change_passwd",
                use_email=data["use_email"],
                revocation_id=data["revocation_id"],
                upassword=data["upassword"],
            )
            cnx.commit()
        cwreq.form["_ctrl"] = {"errors": {None: msg}}
    except ValidationError as exc:
        cwreq.form["_ctrl"] = {"errors": exc.errors}
    except Exception as exc:
        cwreq.form["_ctrl"] = {"errors": exc}
    # XXX there is no way to tell error messages from success notifications
    # unless to rewrite ForgotPwdChangePwdService
    # thus both will be displayed as errors
    return Response(viewsreg.main_template(cwreq, "main-template", rset=None, view=view))


def includeme(config):
    for vid in (
        "forgottenpassword",
        "forgottenpasswordrequest",
        "forgottenpassword_sendmail",
        "forgottenpassword-requestconfirm",
    ):
        config.add_route(vid, "/" + vid)
        config.add_view(startup_view_factory(vid), route_name=vid, request_method=("GET", "HEAD"))

    config.add_route("captcha", "/view", request_param=("captchakey", "t", "vid"))
    config.scan(__name__)

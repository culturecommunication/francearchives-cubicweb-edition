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
from time import time
from cwtags import tag as T

from cubicweb.web.form import FieldNotFound

from cubicweb.crypto import decrypt

from cubicweb_forgotpwd.views import (
    ForgottenPasswordForm,
    ForgottenPasswordFormView,
    ForgottenPasswordRequestForm,
    ForgottenPasswordRequestView,
)

from cubicweb_frarchives_edition.views import get_template

from cubicweb_francearchives.views.forms import AbstractPniaStaticFormRenderer


ForgottenPasswordForm.form_renderer_id = "forgotpwd_render"
ForgottenPasswordForm.form_buttons = []
ForgottenPasswordForm.domid = "forgotpwdForm"


class PniaForgottenPasswordFormView(ForgottenPasswordFormView):
    def call(self):
        with T.section(self.w, Klass="document-view"):
            self.w(T.h1(self._cw._("Forgot your password?")))
            if self._cw.form.get("_sent", None):
                msg = self._cw._(
                    "An email has been sent, follow instructions in there to "
                    "change your password."
                )
                self.w('<p class="alert alert-success">{msg}</p>'.format(msg=msg))
            else:
                submitted_values = self._cw.form.get("_ctrl")
                form = self._cw.vreg["forms"].select("forgottenpassword", self._cw)
                form.render(w=self.w, submitted=submitted_values)


class ForgottenPasswordFormRenderer(AbstractPniaStaticFormRenderer):
    __regid__ = "forgotpwd_render"
    template = get_template("forgotpwd_fields.jinja2")

    def render_content(self, w, form, values):
        """pnia customization: rgaa remove useless fieldset without label"""
        self.render_fields(w, form, values)
        self.render_buttons(w, form)

    def template_attrs(self):
        _ = self._cw._
        captcha_src = self._cw.build_url(
            "view",
            vid="captcha",
            t=int(time() * 100),
            captchakey="captcha",
        )
        return {
            "_": _,
            "submit_value": _("Send"),
            "required_info": _("This field is required"),
            "email_label": _("Your email"),
            "email_value": self._cw.form.get("use_email", ""),
            "captcha_label": _("Captcha"),
            "captcha_help": _("Please copy letters for the image"),
            "captcha_src": captcha_src,
            "captcha_value": self._cw.form.get("captcha", ""),
            "email_error": _("Please enter a valid email address. For exemple: name@domain.fr"),
        }

    def process_errors(self, form):
        processed = {}
        _ = self._cw._
        for key, value in form.ctl_errors.items():
            try:
                field = form.field_by_name(key)
                if field.required:
                    processed["forgotpwd_%s_error" % key] = _(value)
            except FieldNotFound:
                processed["message_error"] = _(value)
        return processed


ForgottenPasswordRequestForm.form_renderer_id = "forgottenpasswordrequest_render"
ForgottenPasswordRequestForm.form_buttons = []
ForgottenPasswordRequestForm.domid = "forgotpwdRequestForm"


class PniaForgottenPasswordRequestView(ForgottenPasswordRequestView):
    def check_key(self):
        try:
            return decrypt(self._cw.form["key"], self._cw.vreg.config["forgotpwd-cypher-seed"])
        except Exception:
            return None

    def call(self):
        self._cw.add_js("bundle-edition.js")
        with T.section(self.w, Klass="document-view"):
            submitted_values = self._cw.form.get("_ctrl")
            self.w(T.h1(self._cw._("Forgot your password?")))
            self.w(T.p(self._cw._("Update your password:")))
            key = self.check_key()
            if key is None:
                self.w(
                    T.div(self._cw._("Invalid link. Please try again."), Class="alert alert-danger")
                )
                return
            form = self._cw.vreg["forms"].select("forgottenpasswordrequest", self._cw)
            form.add_hidden("use_email", key["use_email"])
            form.add_hidden("revocation_id", key["revocation_id"])
            form.add_hidden("key", self._cw.form["key"])
            form.render(w=self.w, submitted=submitted_values)


class ForgottenPasswordRequestFormRenderer(AbstractPniaStaticFormRenderer):
    __regid__ = "forgottenpasswordrequest_render"
    template = get_template("forgotpwd_request_fields.jinja2")

    def render_content(self, w, form, values):
        """pnia customization: rgaa remove useless fieldset without label"""
        self.render_fields(w, form, values)
        self.render_buttons(w, form)

    def template_attrs(self):
        _ = self._cw._
        return {
            "_": _,
            "required_info": _("This field is required"),
            "upassword_label": _("Password"),
            "upassword_confirm_label": _("Please, confirm the password"),
        }

    def process_errors(self, form):
        processed = {}
        _ = self._cw._
        for key, value in form.ctl_errors.items():
            try:
                field = form.field_by_name(key)
                if field.required:
                    processed["forgotpwd_%s_error" % key] = _(value)
            except FieldNotFound:
                processed["message_error"] = _(value)
        return processed


def registration_callback(vreg):
    vreg.register_all(
        list(globals().values()),
        __name__,
        (PniaForgottenPasswordFormView, PniaForgottenPasswordRequestView),
    )
    vreg.register_and_replace(PniaForgottenPasswordFormView, ForgottenPasswordFormView)
    vreg.register_and_replace(PniaForgottenPasswordRequestView, ForgottenPasswordRequestView)

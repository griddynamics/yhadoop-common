/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APPLICATION_ID;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._EVEN;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._INFO_WRAP;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._ODD;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;


import java.util.Collection;

import com.google.inject.Inject;

import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppAttemptInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;

public class AppBlock extends HtmlBlock {

  private ApplicationACLsManager aclsManager;

  @Inject
  AppBlock(ResourceManager rm, ViewContext ctx, ApplicationACLsManager aclsManager) {
    super(ctx);
    this.aclsManager = aclsManager;
  }

  @Override
  protected void render(Block html) {
    String aid = $(APPLICATION_ID);
    if (aid.isEmpty()) {
      puts("Bad request: requires application ID");
      return;
    }

    ApplicationId appID = null;
    try {
      appID = Apps.toAppID(aid);
    } catch (Exception e) {
      puts("Invalid Application ID: " + aid);
      return;
    }

    RMContext context = getInstance(RMContext.class);
    RMApp rmApp = context.getRMApps().get(appID);
    if (rmApp == null) {
      puts("Application not found: "+ aid);
      return;
    }
    AppInfo app = new AppInfo(rmApp, true);

    // Check for the authorization.
    String remoteUser = request().getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null
        && !this.aclsManager.checkAccess(callerUGI,
            ApplicationAccessType.VIEW_APP, app.getUser(), appID)) {
      puts("You (User " + remoteUser
          + ") are not authorized to view application " + appID);
      return;
    }

    setTitle(join("Application ", aid));

    info("Application Overview").
      _("User:", app.getUser()).
      _("Name:", app.getName()).
      _("Application Type:", app.getApplicationType()).
      _("State:", app.getState()).
      _("FinalStatus:", app.getFinalStatus()).
      _("Started:", Times.format(app.getStartTime())).
      _("Elapsed:", StringUtils.formatTime(
        Times.elapsed(app.getStartTime(), app.getFinishTime()))).
      _("Tracking URL:", !app.isTrackingUrlReady() ?
        "#" : app.getTrackingUrlPretty(), app.getTrackingUI()).
      _("Diagnostics:", app.getNote());

    Collection<RMAppAttempt> attempts = rmApp.getAppAttempts().values();
    String amString =
        attempts.size() == 1 ? "ApplicationMaster" : "ApplicationMasters";

    DIV<Hamlet> div = html.
        _(InfoBlock.class).
        div(_INFO_WRAP);
    // MRAppMasters Table
    TABLE<DIV<Hamlet>> table = div.table("#app");
    table.
      tr().
        th(amString).
      _().
      tr().
        th(_TH, "Attempt Number").
        th(_TH, "Start Time").
        th(_TH, "Node").
        th(_TH, "Logs").
      _();

    boolean odd = false;
    for (RMAppAttempt attempt : attempts) {
      AppAttemptInfo attemptInfo = new AppAttemptInfo(attempt, app.getUser());
      table.tr((odd = !odd) ? _ODD : _EVEN).
        td(String.valueOf(attemptInfo.getAttemptId())).
        td(Times.format(attemptInfo.getStartTime())).
        td().a(".nodelink", url(HttpConfig.getSchemePrefix(),
            attemptInfo.getNodeHttpAddress()),
            attemptInfo.getNodeHttpAddress())._().
        td().a(".logslink", url(attemptInfo.getLogsLink()), "logs")._().
      _();
    }

    table._();
    div._();
  }
}

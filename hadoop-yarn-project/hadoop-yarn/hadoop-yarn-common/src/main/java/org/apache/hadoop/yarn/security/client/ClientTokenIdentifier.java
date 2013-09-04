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

package org.apache.hadoop.yarn.security.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class ClientTokenIdentifier extends TokenIdentifier {

  public static final Text KIND_NAME = new Text("YARN_CLIENT_TOKEN");

  private ApplicationId appId;
  private Text clientName;

  // TODO: Add more information in the tokenID such that it is not
  // transferrable, more secure etc.

  public ClientTokenIdentifier(ApplicationId id, String clientName) {
    this.appId = id;
    this.clientName = new Text(clientName);
  }

  public ClientTokenIdentifier() {
    this.clientName = new Text();
  }

  public ApplicationId getApplicationID() {
    return appId;
  }

  public Text getClientName() {
    return clientName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(this.appId.getClusterTimestamp());
    out.writeInt(this.appId.getId());
    clientName.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.appId = BuilderUtils.newApplicationId(in.readLong(), in.readInt());
    clientName.readFields(in);
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public UserGroupInformation getUser() {
    String userName = clientName.toString();
    if (userName.isEmpty()) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(userName);
  }

  @InterfaceAudience.Private
  public static class Renewer extends Token.TrivialRenewer {
    @Override
    protected Text getKind() {
      return KIND_NAME;
    }
  }
}

package org.apache.hadoop.hdfs.server.common;
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
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;

public class UpgradeObjectImpl extends UpgradeObject {
  private int version;
  private NodeType nodeType = NodeType.DATA_NODE;
  private short startStatus = 0;
  private short stopStatus = 100;

  UpgradeObjectImpl() {}
  
  UpgradeObjectImpl(int version, NodeType nodeType) {
    this.status = 0;
    this.version = version;
    this.nodeType = nodeType;
  }

  @Override
  public UpgradeCommand startUpgrade() throws IOException {
    return new UpgradeCommand(UpgradeCommand.UC_ACTION_REPORT_STATUS, 100,
        startStatus);
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public NodeType getType() {
    return nodeType;
  }

  @Override
  public UpgradeCommand completeUpgrade() throws IOException {
    return new UpgradeCommand(UpgradeCommand.UC_ACTION_REPORT_STATUS, 100,
        stopStatus);
  }
}
